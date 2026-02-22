"""Z-score anomaly detection over rolling windows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

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
    group_by: str
    group_value: str
    current_cost: float
    mean_cost: float
    std_cost: float
    z_score: float
    severity: str  # "critical", "warning", "info"
    direction: str  # "spike" or "drop"


def classify_severity(z_score: float) -> str:
    """Classify anomaly severity based on absolute z-score."""
    abs_z = abs(z_score)
    if abs_z > 4.0:
        return "critical"
    elif abs_z > 3.0:
        return "warning"
    return "info"


def detect_anomalies(
    conn: duckdb.DuckDBPyConnection,
    days: int = 14,
    group_by: str = "product_code",
    sensitivity: str = "medium",
    min_daily_cost: float = 1.0,
) -> list[Anomaly]:
    """Detect cost anomalies using modified z-score with rolling window.

    For each group:
    1. Fetch daily costs for the last N days
    2. Use all days except the most recent as baseline
    3. Compute z-score of the most recent day vs baseline
    4. Flag if |z-score| exceeds threshold and cost > min_daily_cost

    Args:
        conn: DuckDB connection
        days: Rolling window size (includes the current day)
        group_by: Dimension to group by
        sensitivity: "low" (z>3), "medium" (z>2.5), "high" (z>2)
        min_daily_cost: Minimum daily cost to consider

    Returns list of detected anomalies, sorted by |z_score| descending.
    """
    valid_groups = {"product_code", "usage_account_id", "region"}
    if group_by not in valid_groups:
        raise ValueError(f"group_by must be one of {valid_groups}")

    threshold = SENSITIVITY_THRESHOLDS.get(sensitivity, 2.5)
    cutoff = date.today() - timedelta(days=days)

    # Get daily costs per group
    rows = conn.execute(
        f"""
        SELECT
            {group_by} AS group_value,
            usage_date,
            SUM(total_unblended_cost) AS daily_cost
        FROM daily_cost_summary
        WHERE usage_date >= ?
        GROUP BY {group_by}, usage_date
        ORDER BY {group_by}, usage_date
        """,
        [cutoff],
    ).fetchall()

    # Organize by group
    groups: dict[str, list[tuple[date, float]]] = {}
    for group_value, usage_date, daily_cost in rows:
        key = group_value or "unknown"
        if key not in groups:
            groups[key] = []
        groups[key].append((usage_date, daily_cost))

    anomalies: list[Anomaly] = []

    for group_value, daily_data in groups.items():
        if len(daily_data) < 3:
            # Not enough data for meaningful z-score
            continue

        # Sort by date, split into baseline (all but last) and current (last)
        daily_data.sort(key=lambda x: x[0])
        baseline_costs = np.array([cost for _, cost in daily_data[:-1]])
        current_date, current_cost = daily_data[-1]

        if current_cost < min_daily_cost:
            continue

        mean_cost = float(np.mean(baseline_costs))
        std_cost = float(np.std(baseline_costs, ddof=1))

        if std_cost < 1e-10:
            # Zero variance — flag if current differs significantly from mean
            if abs(current_cost - mean_cost) > min_daily_cost:
                z_score = 10.0 if current_cost > mean_cost else -10.0
            else:
                continue
        else:
            z_score = (current_cost - mean_cost) / std_cost

        if abs(z_score) >= threshold:
            anomalies.append(
                Anomaly(
                    usage_date=current_date,
                    group_by=group_by,
                    group_value=group_value,
                    current_cost=current_cost,
                    mean_cost=mean_cost,
                    std_cost=std_cost,
                    z_score=z_score,
                    severity=classify_severity(z_score),
                    direction="spike" if z_score > 0 else "drop",
                )
            )

    # Sort by absolute z-score descending
    anomalies.sort(key=lambda a: abs(a.z_score), reverse=True)
    return anomalies
