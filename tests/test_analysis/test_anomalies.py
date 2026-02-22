"""Tests for z-score anomaly detection."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from unittest.mock import patch

import pytest

from aws_cost_anomalies.analysis.anomalies import (
    classify_severity,
    detect_anomalies,
)
from aws_cost_anomalies.storage.schema import rebuild_daily_summary

_INSERT_SQL = (
    "INSERT INTO cost_line_items VALUES ("
    + ", ".join(["?"] * 23)
    + ")"
)


def test_classify_severity():
    assert classify_severity(5.0) == "critical"
    assert classify_severity(-4.5) == "critical"
    assert classify_severity(3.5) == "warning"
    assert classify_severity(-3.1) == "warning"
    assert classify_severity(2.5) == "info"
    assert classify_severity(-2.0) == "info"


def test_detect_anomalies_no_data(db):
    rebuild_daily_summary(db)
    results = detect_anomalies(db, days=14, group_by="product_code")
    assert results == []


def test_detect_anomalies_invalid_group(db):
    with pytest.raises(ValueError, match="group_by must be one of"):
        detect_anomalies(db, group_by="invalid")


def test_detect_anomalies_with_spike(db):
    """Insert data with a deliberate spike on the last day."""
    base_date = date(2025, 1, 1)
    rows = []
    for day_offset in range(14):
        usage_date = base_date + timedelta(days=day_offset)
        # Steady $100/day for first 13 days, then spike to $500
        cost = 100.0 if day_offset < 13 else 500.0
        rows.append(
            (
                f"id-{day_offset}",
                datetime.combine(usage_date, datetime.min.time()),
                datetime.combine(usage_date + timedelta(days=1), datetime.min.time()),
                date(2025, 1, 1),
                date(2025, 2, 1),
                "999999999999",
                "111111111111",
                "AmazonEC2",
                "EC2",
                "us-east-1",
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
                datetime.now(),
                "test.parquet",
            )
        )

    db.executemany(
        _INSERT_SQL,
        rows,
    )
    rebuild_daily_summary(db)

    with patch("aws_cost_anomalies.analysis.anomalies.date") as mock_date:
        mock_date.today.return_value = date(2025, 1, 15)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

        anomalies = detect_anomalies(
            db, days=14, group_by="product_code", sensitivity="medium"
        )

    assert len(anomalies) >= 1
    ec2_anomaly = next(a for a in anomalies if a.group_value == "AmazonEC2")
    assert ec2_anomaly.direction == "spike"
    assert ec2_anomaly.z_score > 2.5
    assert ec2_anomaly.current_cost == 500.0


def test_detect_anomalies_with_drop(db):
    """Insert data with a deliberate drop on the last day."""
    base_date = date(2025, 1, 1)
    rows = []
    for day_offset in range(14):
        usage_date = base_date + timedelta(days=day_offset)
        cost = 500.0 if day_offset < 13 else 50.0
        rows.append(
            (
                f"id-{day_offset}",
                datetime.combine(usage_date, datetime.min.time()),
                datetime.combine(usage_date + timedelta(days=1), datetime.min.time()),
                date(2025, 1, 1),
                date(2025, 2, 1),
                "999999999999",
                "111111111111",
                "AmazonEC2",
                "EC2",
                "us-east-1",
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
                datetime.now(),
                "test.parquet",
            )
        )

    db.executemany(
        _INSERT_SQL,
        rows,
    )
    rebuild_daily_summary(db)

    with patch("aws_cost_anomalies.analysis.anomalies.date") as mock_date:
        mock_date.today.return_value = date(2025, 1, 15)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

        anomalies = detect_anomalies(
            db, days=14, group_by="product_code", sensitivity="medium"
        )

    assert len(anomalies) >= 1
    ec2_anomaly = next(a for a in anomalies if a.group_value == "AmazonEC2")
    assert ec2_anomaly.direction == "drop"
    assert ec2_anomaly.z_score < -2.5


def test_detect_anomalies_below_min_cost(db):
    """Low cost items should be filtered out."""
    base_date = date(2025, 1, 1)
    rows = []
    for day_offset in range(14):
        usage_date = base_date + timedelta(days=day_offset)
        cost = 0.01 if day_offset < 13 else 0.50  # Always below $1 min
        rows.append(
            (
                f"id-{day_offset}",
                datetime.combine(usage_date, datetime.min.time()),
                datetime.combine(usage_date + timedelta(days=1), datetime.min.time()),
                date(2025, 1, 1),
                date(2025, 2, 1),
                "999",
                "111",
                "AmazonEC2",
                "EC2",
                "us-east-1",
                None,
                "BoxUsage",
                "RunInstances",
                None,
                "Usage",
                cost,
                cost,
                cost,
                1.0,
                "USD",
                "test",
                datetime.now(),
                "test.parquet",
            )
        )

    db.executemany(
        _INSERT_SQL,
        rows,
    )
    rebuild_daily_summary(db)

    with patch("aws_cost_anomalies.analysis.anomalies.date") as mock_date:
        mock_date.today.return_value = date(2025, 1, 15)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

        anomalies = detect_anomalies(
            db, days=14, group_by="product_code", min_daily_cost=1.0
        )

    assert len(anomalies) == 0
