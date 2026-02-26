"""Tests for robust anomaly detection (median/MAD, drift, multi-dim)."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from aws_cost_anomalies.analysis.anomalies import (
    _classify_drift_severity,
    _theil_sen_slope,
    classify_severity,
    detect_anomalies,
)
from aws_cost_anomalies.storage.schema import rebuild_daily_summary

_INSERT_SQL = (
    "INSERT INTO cost_line_items VALUES ("
    + ", ".join(["?"] * 24)
    + ")"
)


def _make_row(day_offset, cost, base_date=date(2025, 1, 1),
              account="111111111111", service="AmazonEC2",
              region="us-east-1", row_id_prefix="id"):
    """Helper to build a cost_line_items row tuple."""
    usage_date = base_date + timedelta(days=day_offset)
    return (
        f"{row_id_prefix}-{day_offset}-{account}-{service}",
        datetime.combine(usage_date, datetime.min.time()),
        datetime.combine(usage_date + timedelta(days=1), datetime.min.time()),
        base_date,
        base_date + timedelta(days=31),
        "999999999999",
        account,
        service,
        service,
        region,
        None,
        "BoxUsage",
        "RunInstances",
        None,
        "Usage",
        cost,
        cost,
        cost,
        cost * 10,
        "USD",
        "test",
        "AWS",
        datetime.now(),
        "test.parquet",
    )


def _insert_and_rebuild(db, rows):
    """Insert rows into cost_line_items and rebuild daily summary."""
    db.executemany(_INSERT_SQL, rows)
    rebuild_daily_summary(db)


def _patch_today(target_date):
    """Context manager to mock datetime.now(tz).date() in anomalies module."""
    fake_now = MagicMock()
    fake_now.date.return_value = target_date
    return patch("aws_cost_anomalies.analysis.anomalies.datetime",
                 wraps=datetime, **{"now.return_value": fake_now})


# ── Severity classification ──────────────────────────────────────────

def test_classify_severity():
    assert classify_severity(5.0) == "critical"
    assert classify_severity(-4.5) == "critical"
    assert classify_severity(3.5) == "warning"
    assert classify_severity(-3.1) == "warning"
    assert classify_severity(2.5) == "info"
    assert classify_severity(-2.0) == "info"


# ── Drift severity classification ────────────────────────────────────

def test_classify_drift_severity_critical():
    assert _classify_drift_severity(1.5) == "critical"
    assert _classify_drift_severity(-1.1) == "critical"


def test_classify_drift_severity_warning():
    assert _classify_drift_severity(0.6) == "warning"
    assert _classify_drift_severity(-0.8) == "warning"


def test_classify_drift_severity_info():
    assert _classify_drift_severity(0.3) == "info"
    assert _classify_drift_severity(-0.1) == "info"


def test_classify_drift_severity_boundaries():
    assert _classify_drift_severity(1.0) == "warning"   # boundary: not > 1.0
    assert _classify_drift_severity(0.5) == "info"       # boundary: not > 0.5


# ── Theil-Sen slope ─────────────────────────────────────────────────

def test_theil_sen_flat_line():
    costs = np.array([100.0, 100.0, 100.0, 100.0, 100.0])
    assert _theil_sen_slope(costs) == pytest.approx(0.0)


def test_theil_sen_linear_increase():
    costs = np.array([10.0, 20.0, 30.0, 40.0, 50.0])
    assert _theil_sen_slope(costs) == pytest.approx(10.0)


def test_theil_sen_linear_decrease():
    costs = np.array([50.0, 40.0, 30.0, 20.0, 10.0])
    assert _theil_sen_slope(costs) == pytest.approx(-10.0)


def test_theil_sen_robust_to_outlier():
    # Linear trend of slope=1, but one outlier at index 2
    costs = np.array([0.0, 1.0, 100.0, 3.0, 4.0])
    slope = _theil_sen_slope(costs)
    # Median of pairwise slopes should be close to 1.0, not pulled to ~25
    assert 0.5 < slope < 2.0


def test_theil_sen_two_points():
    costs = np.array([5.0, 15.0])
    assert _theil_sen_slope(costs) == pytest.approx(10.0)


# ── Backward-compatible aliases ─────────────────────────────────────

def test_anomaly_backward_compat_aliases(db):
    """mean_cost and std_cost properties return median_cost and mad."""
    rows = [_make_row(d, 100.0 if d < 13 else 500.0) for d in range(14)]
    _insert_and_rebuild(db, rows)

    with _patch_today(date(2025, 1, 15)):
        anomalies = detect_anomalies(
            db, days=14, group_by="product_code", sensitivity="medium"
        )

    point = [a for a in anomalies if a.kind == "point"]
    assert len(point) >= 1
    a = point[0]
    assert a.mean_cost == a.median_cost
    assert a.std_cost == a.mad


# ── Basic edge cases ─────────────────────────────────────────────────

def test_detect_anomalies_no_data(db):
    rebuild_daily_summary(db)
    results = detect_anomalies(db, days=14, group_by="product_code")
    assert results == []


def test_detect_anomalies_invalid_group(db):
    with pytest.raises(ValueError, match="group_by must be one of"):
        detect_anomalies(db, group_by="invalid")


# ── Point anomaly detection (median + MAD) ───────────────────────────

def test_detect_anomalies_with_spike(db):
    """Steady $100/day for 13 days, spike to $500 on day 14."""
    rows = [_make_row(d, 100.0 if d < 13 else 500.0) for d in range(14)]
    _insert_and_rebuild(db, rows)

    with _patch_today(date(2025, 1, 15)):
        anomalies = detect_anomalies(
            db, days=14, group_by="product_code", sensitivity="medium"
        )

    point_anomalies = [a for a in anomalies if a.kind == "point"]
    assert len(point_anomalies) >= 1
    ec2 = next(a for a in point_anomalies if a.group_value == "AmazonEC2")
    assert ec2.direction == "spike"
    assert ec2.z_score > 2.5
    assert ec2.current_cost == 500.0
    assert ec2.median_cost == 100.0


def test_detect_anomalies_with_drop(db):
    """Steady $500/day for 13 days, drop to $50 on day 14."""
    rows = [_make_row(d, 500.0 if d < 13 else 50.0) for d in range(14)]
    _insert_and_rebuild(db, rows)

    with _patch_today(date(2025, 1, 15)):
        anomalies = detect_anomalies(
            db, days=14, group_by="product_code", sensitivity="medium"
        )

    point_anomalies = [a for a in anomalies if a.kind == "point"]
    assert len(point_anomalies) >= 1
    ec2 = next(a for a in point_anomalies if a.group_value == "AmazonEC2")
    assert ec2.direction == "drop"
    assert ec2.z_score < -2.5


def test_detect_anomalies_below_min_cost(db):
    """Low cost items should be filtered out."""
    rows = [_make_row(d, 0.01 if d < 13 else 0.50) for d in range(14)]
    _insert_and_rebuild(db, rows)

    with _patch_today(date(2025, 1, 15)):
        anomalies = detect_anomalies(
            db, days=14, group_by="product_code", min_daily_cost=1.0
        )

    assert len(anomalies) == 0


# ── MAD robustness ───────────────────────────────────────────────────

def test_mad_robust_to_outlier_in_baseline(db):
    """One outlier in baseline shouldn't suppress detection of a real anomaly.

    12 days at $100, 1 outlier at $500, then current day at $300.
    With mean/std the outlier inflates std. With median/MAD, median≈$100,
    MAD≈$0, so $300 is clearly anomalous.
    """
    rows = [_make_row(d,
                       500.0 if d == 10 else (300.0 if d == 13 else 100.0))
            for d in range(14)]
    _insert_and_rebuild(db, rows)

    with _patch_today(date(2025, 1, 15)):
        anomalies = detect_anomalies(
            db, days=14, group_by="product_code", sensitivity="medium"
        )

    point_anomalies = [a for a in anomalies if a.kind == "point"]
    assert len(point_anomalies) >= 1
    ec2 = next(a for a in point_anomalies if a.group_value == "AmazonEC2")
    assert ec2.direction == "spike"
    assert ec2.median_cost == 100.0


def test_zero_mad_triggers_anomaly(db):
    """All baseline days identical ($100), current day $200 → should flag."""
    rows = [_make_row(d, 100.0 if d < 13 else 200.0) for d in range(14)]
    _insert_and_rebuild(db, rows)

    with _patch_today(date(2025, 1, 15)):
        anomalies = detect_anomalies(
            db, days=14, group_by="product_code", sensitivity="medium"
        )

    point_anomalies = [a for a in anomalies if a.kind == "point"]
    assert len(point_anomalies) >= 1
    ec2 = next(a for a in point_anomalies if a.group_value == "AmazonEC2")
    assert ec2.z_score == 10.0
    assert ec2.mad == 0.0


# ── Trend/drift detection ───────────────────────────────────────────

def test_detects_gradual_upward_drift(db):
    """Linear increase from $100 to $140 over 14 days (~40% drift)."""
    rows = [_make_row(d, 100.0 + d * (40.0 / 13)) for d in range(14)]
    _insert_and_rebuild(db, rows)

    with _patch_today(date(2025, 1, 15)):
        anomalies = detect_anomalies(
            db, days=14, group_by="product_code",
            sensitivity="medium", drift_threshold=0.20,
        )

    trend_anomalies = [a for a in anomalies if a.kind == "trend"]
    assert len(trend_anomalies) >= 1
    ec2 = next(a for a in trend_anomalies if a.group_value == "AmazonEC2")
    assert ec2.direction == "drift_up"
    assert ec2.z_score > 0.20


def test_no_drift_on_flat_costs(db):
    """Flat $100/day should produce no trend anomalies."""
    rows = [_make_row(d, 100.0) for d in range(14)]
    _insert_and_rebuild(db, rows)

    with _patch_today(date(2025, 1, 15)):
        anomalies = detect_anomalies(
            db, days=14, group_by="product_code",
            sensitivity="medium", drift_threshold=0.20,
        )

    trend_anomalies = [a for a in anomalies if a.kind == "trend"]
    assert len(trend_anomalies) == 0


def test_detects_downward_drift(db):
    """Linear decrease from $200 to $100 over 14 days."""
    rows = [_make_row(d, 200.0 - d * (100.0 / 13)) for d in range(14)]
    _insert_and_rebuild(db, rows)

    with _patch_today(date(2025, 1, 15)):
        anomalies = detect_anomalies(
            db, days=14, group_by="product_code",
            sensitivity="medium", drift_threshold=0.20,
        )

    trend_anomalies = [a for a in anomalies if a.kind == "trend"]
    assert len(trend_anomalies) >= 1
    ec2 = next(a for a in trend_anomalies if a.group_value == "AmazonEC2")
    assert ec2.direction == "drift_down"
    assert ec2.z_score < 0


def test_drift_below_threshold_not_flagged(db):
    """Only 10% total drift (below default 20%) should not flag."""
    rows = [_make_row(d, 100.0 + d * (10.0 / 13)) for d in range(14)]
    _insert_and_rebuild(db, rows)

    with _patch_today(date(2025, 1, 15)):
        anomalies = detect_anomalies(
            db, days=14, group_by="product_code",
            sensitivity="medium", drift_threshold=0.20,
        )

    trend_anomalies = [a for a in anomalies if a.kind == "trend"]
    assert len(trend_anomalies) == 0


# ── Multi-dimensional grouping ──────────────────────────────────────

def test_multi_dim_service_account(db):
    """Spike in one (service, account) pair detected with multi-dim grouping."""
    rows = []
    for d in range(14):
        # EC2 account-111: spike on last day
        cost_ec2_111 = 100.0 if d < 13 else 500.0
        rows.append(_make_row(d, cost_ec2_111,
                              account="111111111111", service="AmazonEC2",
                              row_id_prefix="a"))
        # EC2 account-222: steady
        rows.append(_make_row(d, 100.0,
                              account="222222222222", service="AmazonEC2",
                              row_id_prefix="b"))
        # S3 account-111: steady
        rows.append(_make_row(d, 50.0,
                              account="111111111111", service="AmazonS3",
                              row_id_prefix="c"))
    _insert_and_rebuild(db, rows)

    with _patch_today(date(2025, 1, 15)):
        anomalies = detect_anomalies(
            db, days=14,
            group_by=["product_code", "usage_account_id"],
            sensitivity="medium",
        )

    point_anomalies = [a for a in anomalies if a.kind == "point"]
    assert len(point_anomalies) >= 1
    match = next(a for a in point_anomalies
                 if "AmazonEC2" in a.group_value and "111111111111" in a.group_value)
    assert match.direction == "spike"
    assert match.group_by == "product_code+usage_account_id"


def test_single_dim_masks_multi_dim_anomaly(db):
    """A spike in one account offset by drop in another is invisible at service level."""
    rows = []
    for d in range(14):
        # Account 111: spike on last day
        cost_111 = 100.0 if d < 13 else 200.0
        rows.append(_make_row(d, cost_111,
                              account="111111111111", service="AmazonEC2",
                              row_id_prefix="a"))
        # Account 222: drop on last day (offsetting)
        cost_222 = 100.0 if d < 13 else 0.01
        rows.append(_make_row(d, cost_222,
                              account="222222222222", service="AmazonEC2",
                              row_id_prefix="b"))
    _insert_and_rebuild(db, rows)

    with _patch_today(date(2025, 1, 15)):
        # Single-dim: aggregate hides the anomaly
        single_anomalies = detect_anomalies(
            db, days=14, group_by="product_code", sensitivity="medium",
        )
        single_point = [a for a in single_anomalies if a.kind == "point"]

        # Multi-dim: reveals per-account anomalies
        multi_anomalies = detect_anomalies(
            db, days=14,
            group_by=["product_code", "usage_account_id"],
            sensitivity="medium",
        )
        multi_point = [a for a in multi_anomalies if a.kind == "point"]

    assert len(single_point) == 0
    assert len(multi_point) >= 1


def test_multi_dim_invalid_column_rejected(db):
    """Passing an invalid column in multi-dim should raise ValueError."""
    with pytest.raises(ValueError, match="group_by must be one of"):
        detect_anomalies(db, group_by=["product_code", "invalid"])


# ── Combined point + trend ───────────────────────────────────────────

def test_spike_and_drift_together(db):
    """Gradually rising cost plus a spike on last day produces both anomaly types."""
    # 13 days: $100 → $126 (linear rise), day 14: spike to $300
    rows = []
    for d in range(14):
        if d < 13:
            cost = 100.0 + d * 2.0  # $100, $102, ..., $124
        else:
            cost = 300.0  # spike
        rows.append(_make_row(d, cost))
    _insert_and_rebuild(db, rows)

    with _patch_today(date(2025, 1, 15)):
        anomalies = detect_anomalies(
            db, days=14, group_by="product_code",
            sensitivity="medium", drift_threshold=0.20,
        )

    point_anomalies = [a for a in anomalies if a.kind == "point"]
    trend_anomalies = [a for a in anomalies if a.kind == "trend"]
    assert len(point_anomalies) >= 1
    assert len(trend_anomalies) >= 1
    assert point_anomalies[0].direction == "spike"
    assert trend_anomalies[0].direction == "drift_up"
