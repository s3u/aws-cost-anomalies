"""Robust anomaly detection: median/MAD z-scores, Theil-Sen drift, multi-dim grouping."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import duckdb
import numpy as np

SENSITIVITY_THRESHOLDS = {
    "low": 3.0,
    "medium": 2.5,
    "high": 2.0,
}


@dataclass
class Anomaly:
    usage_date: date
    group_by: str  # "product_code" or "product_code+usage_account_id"
    group_value: str  # "AmazonEC2" or "AmazonEC2 / 111111111111"
    current_cost: float
    median_cost: float
    mad: float  # Median Absolute Deviation
    z_score: float  # modified z-score (point) or drift pct (trend)
    severity: str  # "critical", "warning", "info"
    direction: str  # "spike", "drop", "drift_up", "drift_down"
    kind: str = "point"  # "point" or "trend"

    # Backward-compatible aliases for renamed fields
    @property
    def mean_cost(self) -> float:
        return self.median_cost

    @property
    def std_cost(self) -> float:
        return self.mad


def classify_severity(z_score: float) -> str:
    """Classify anomaly severity based on absolute z-score."""
    abs_z = abs(z_score)
    if abs_z > 4.0:
        return "critical"
    elif abs_z > 3.0:
        return "warning"
    return "info"


def _classify_drift_severity(drift_fraction: float) -> str:
    """Classify drift severity based on absolute drift fraction (1.0 = 100%)."""
    abs_drift = abs(drift_fraction)
    if abs_drift > 1.0:
        return "critical"
    elif abs_drift > 0.5:
        return "warning"
    return "info"


def _theil_sen_slope(costs: np.ndarray) -> float:
    """Robust slope estimator: median of all pairwise slopes.

    Vectorized to avoid O(n^2) Python loop — uses numpy index grids
    so the inner work runs in C.
    """
    n = len(costs)
    j_idx, i_idx = np.triu_indices(n, k=1)
    slopes = (costs[j_idx] - costs[i_idx]) / (j_idx - i_idx)
    return float(np.median(slopes))


def detect_anomalies(
    conn: duckdb.DuckDBPyConnection,
    days: int = 14,
    group_by: str | list[str] = "product_code",
    sensitivity: str = "medium",
    min_daily_cost: float = 1.0,
    drift_threshold: float = 0.20,
    data_source: str | None = None,
    reference_date: date | None = None,
) -> list[Anomaly]:
    """Detect cost anomalies using robust statistics over a rolling window.

    Point anomalies use median + MAD (modified z-score).
    Trend anomalies use Theil-Sen slope to detect gradual drift.

    Args:
        conn: DuckDB connection
        days: Rolling window size (includes the current day)
        group_by: Dimension(s) to group by — single string or list
        sensitivity: "low" (z>3), "medium" (z>2.5), "high" (z>2)
        min_daily_cost: Minimum daily cost to consider
        drift_threshold: Fractional drift over window to flag (0.20 = 20%)
        data_source: Filter by data source ('cur' or 'cost_explorer'). None = all.
        reference_date: The "current" date for detection. Defaults to today (UTC).

    Returns list of detected anomalies, sorted by |z_score| descending.
    """
    valid_groups = {"product_code", "usage_account_id", "region"}

    # Normalize group_by to list
    if isinstance(group_by, str):
        group_cols = [group_by]
    else:
        group_cols = list(group_by)

    for col in group_cols:
        if col not in valid_groups:
            raise ValueError(f"group_by must be one of {valid_groups}")

    threshold = SENSITIVITY_THRESHOLDS.get(sensitivity, 2.5)
    ref_date = reference_date or datetime.now(timezone.utc).date()
    cutoff = ref_date - timedelta(days=days)

    select_cols = ", ".join(group_cols)
    group_clause = ", ".join(group_cols)
    group_by_label = "+".join(group_cols)

    source_filter = ""
    params: list = [cutoff, ref_date]
    if data_source:
        source_filter = " AND data_source = ?"
        params.append(data_source)

    rows = conn.execute(
        f"""
        SELECT {select_cols}, usage_date, SUM(total_net_amortized_cost) AS daily_cost
        FROM daily_cost_summary
        WHERE usage_date >= ? AND usage_date <= ?{source_filter}
        GROUP BY {group_clause}, usage_date
        ORDER BY {group_clause}, usage_date
        """,
        params,
    ).fetchall()

    # Organize by group
    groups: dict[str, list[tuple[date, float]]] = {}
    num_group_cols = len(group_cols)
    for row in rows:
        key = " / ".join(str(row[i]) or "unknown" for i in range(num_group_cols))
        usage_date = row[num_group_cols]
        daily_cost = row[num_group_cols + 1]
        if key not in groups:
            groups[key] = []
        groups[key].append((usage_date, daily_cost))

    anomalies: list[Anomaly] = []

    for group_value, daily_data in groups.items():
        if len(daily_data) < 3:
            continue

        daily_data.sort(key=lambda x: x[0])
        baseline_costs = np.array([cost for _, cost in daily_data[:-1]])
        current_date, current_cost = daily_data[-1]

        if current_cost < min_daily_cost:
            continue

        # --- Point anomaly detection (median + MAD) ---
        median = float(np.median(baseline_costs))
        mad = float(np.median(np.abs(baseline_costs - median)))

        if mad < 1e-10:
            if abs(current_cost - median) > min_daily_cost:
                z_score = 10.0 if current_cost > median else -10.0
            else:
                z_score = 0.0
        else:
            z_score = 0.6745 * (current_cost - median) / mad

        if abs(z_score) >= threshold:
            anomalies.append(
                Anomaly(
                    usage_date=current_date,
                    group_by=group_by_label,
                    group_value=group_value,
                    current_cost=current_cost,
                    median_cost=median,
                    mad=mad,
                    z_score=z_score,
                    severity=classify_severity(z_score),
                    direction="spike" if z_score > 0 else "drop",
                    kind="point",
                )
            )

        # --- Trend/drift detection (Theil-Sen slope) ---
        all_costs = np.array([cost for _, cost in daily_data])
        if len(all_costs) >= 5 and median > min_daily_cost:
            slope = _theil_sen_slope(all_costs)
            num_days = len(all_costs)
            drift_pct = (slope * num_days) / median

            if abs(drift_pct) >= drift_threshold:
                anomalies.append(
                    Anomaly(
                        usage_date=current_date,
                        group_by=group_by_label,
                        group_value=group_value,
                        current_cost=current_cost,
                        median_cost=median,
                        mad=mad,
                        z_score=drift_pct,
                        severity=_classify_drift_severity(drift_pct),
                        direction="drift_up" if drift_pct > 0 else "drift_down",
                        kind="trend",
                    )
                )

    # Sort by severity (critical > warning > info), then by magnitude within severity.
    # This avoids comparing z_scores across kinds (point z-scores ~2-10+ vs drift pct ~0.2-1.0).
    _severity_rank = {"critical": 0, "warning": 1, "info": 2}
    anomalies.sort(key=lambda a: (_severity_rank.get(a.severity, 9), -abs(a.z_score)))
    return anomalies


# ---------------------------------------------------------------------------
# Historical anomaly scanning
# ---------------------------------------------------------------------------


@dataclass
class ScanResult:
    scan_start: date
    scan_end: date
    anomalies: list[Anomaly]
    days_scanned: int


def scan_anomalies(
    conn: duckdb.DuckDBPyConnection,
    scan_start: date,
    scan_end: date,
    days: int = 14,
    group_by: str | list[str] = "product_code",
    sensitivity: str = "medium",
    min_daily_cost: float = 1.0,
    drift_threshold: float = 0.20,
    data_source: str | None = None,
) -> ScanResult:
    """Scan a date range for anomalies by running detection on each day.

    Iterates day-by-day from scan_start to scan_end, calling
    detect_anomalies with reference_date=day. Deduplicates
    consecutive-day anomalies for the same (group_value, kind),
    keeping the one with the highest |z_score|.

    Args:
        conn: DuckDB connection
        scan_start: First day to scan (inclusive)
        scan_end: Last day to scan (inclusive)
        days: Rolling window size for each detection
        group_by: Dimension(s) to group by
        sensitivity: Detection sensitivity
        min_daily_cost: Minimum daily cost to consider
        drift_threshold: Fractional drift threshold
        data_source: Filter by data source

    Returns:
        ScanResult with deduplicated anomalies and scan metadata.

    Raises:
        ValueError: If scan_start > scan_end.
    """
    if scan_start > scan_end:
        raise ValueError(
            f"scan_start ({scan_start}) must be <= scan_end ({scan_end})"
        )

    # Track best anomaly per (group_value, kind) for consecutive runs.
    # Key: (group_value, kind) → (Anomaly, last_date_seen)
    active: dict[tuple[str, str], tuple[Anomaly, date]] = {}
    finished: list[Anomaly] = []

    days_scanned = 0
    current = scan_start
    while current <= scan_end:
        days_scanned += 1
        day_anomalies = detect_anomalies(
            conn,
            days=days,
            group_by=group_by,
            sensitivity=sensitivity,
            min_daily_cost=min_daily_cost,
            drift_threshold=drift_threshold,
            data_source=data_source,
            reference_date=current,
        )

        seen_today: set[tuple[str, str]] = set()
        for a in day_anomalies:
            key = (a.group_value, a.kind)
            seen_today.add(key)

            if key in active:
                prev_anomaly, _ = active[key]
                if abs(a.z_score) > abs(prev_anomaly.z_score):
                    active[key] = (a, current)
                else:
                    active[key] = (prev_anomaly, current)
            else:
                active[key] = (a, current)

        # Flush keys not seen today (streak broken)
        for key in list(active):
            if key not in seen_today:
                finished.append(active.pop(key)[0])

        current += timedelta(days=1)

    # Flush remaining active streaks
    for anomaly, _ in active.values():
        finished.append(anomaly)

    _severity_rank = {"critical": 0, "warning": 1, "info": 2}
    finished.sort(key=lambda a: (_severity_rank.get(a.severity, 9), -abs(a.z_score)))

    return ScanResult(
        scan_start=scan_start,
        scan_end=scan_end,
        anomalies=finished,
        days_scanned=days_scanned,
    )
