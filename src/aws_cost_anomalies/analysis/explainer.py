"""Comprehensive anomaly explanation with baseline stats and attribution."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from statistics import median

import duckdb


@dataclass
class UsageTypeChange:
    usage_type: str
    baseline_cost: float
    anomaly_cost: float
    absolute_change: float
    pct_change: float | None


@dataclass
class AnomalyExplanation:
    service: str
    anomaly_date: date
    account_id: str | None
    baseline_median: float
    baseline_min: float
    baseline_max: float
    anomaly_cost: float
    cost_vs_median: float
    cost_multiple: float
    is_ongoing: bool
    days_after_checked: int
    elevated_days_after: int
    has_cur_data: bool
    has_baseline: bool
    top_usage_type_changes: list[UsageTypeChange] = field(default_factory=list)


def explain_anomaly(
    conn: duckdb.DuckDBPyConnection,
    service: str,
    anomaly_date: date,
    account_id: str | None = None,
    baseline_days: int = 14,
) -> AnomalyExplanation:
    """Build a comprehensive anomaly explanation.

    Computes baseline statistics, anomaly magnitude, ongoing status,
    and usage-type attribution (if CUR data is available).

    Args:
        conn: DuckDB connection
        service: AWS product_code (e.g. "AmazonEC2")
        anomaly_date: Date of the anomaly
        account_id: Optional account filter
        baseline_days: Number of days before anomaly_date for baseline

    Returns:
        AnomalyExplanation with full narrative data.

    Raises:
        ValueError: If no data exists for the service/date.
    """
    # Build optional account filter
    acct_filter = ""
    params: list = [service]
    if account_id:
        acct_filter = " AND usage_account_id = ?"
        params.append(account_id)

    baseline_start = anomaly_date - timedelta(days=baseline_days)
    baseline_end = anomaly_date - timedelta(days=1)

    # Get baseline costs
    baseline_rows = conn.execute(
        f"""
        SELECT usage_date, SUM(total_net_amortized_cost) AS cost
        FROM daily_cost_summary
        WHERE product_code = ?
          {acct_filter}
          AND usage_date >= ? AND usage_date <= ?
        GROUP BY usage_date
        ORDER BY usage_date
        """,
        [*params, baseline_start, baseline_end],
    ).fetchall()

    # Get anomaly day cost
    anomaly_row = conn.execute(
        f"""
        SELECT SUM(total_net_amortized_cost) AS cost
        FROM daily_cost_summary
        WHERE product_code = ?
          {acct_filter}
          AND usage_date = ?
        """,
        [*params, anomaly_date],
    ).fetchone()

    anomaly_cost = anomaly_row[0] if anomaly_row and anomaly_row[0] is not None else None

    if anomaly_cost is None and not baseline_rows:
        raise ValueError(
            f"No data found for {service} on {anomaly_date} or in the "
            f"baseline period ({baseline_start} to {baseline_end})."
        )

    if anomaly_cost is None:
        raise ValueError(
            f"No data found for {service} on {anomaly_date}."
        )

    baseline_costs = [row[1] for row in baseline_rows]
    has_baseline = len(baseline_costs) > 0
    if not baseline_costs:
        # No baseline data — use anomaly day as its own baseline.
        # has_baseline=False signals that comparisons are unreliable.
        baseline_costs = [anomaly_cost]

    baseline_med = round(median(baseline_costs), 2)
    baseline_min_val = round(min(baseline_costs), 2)
    baseline_max_val = round(max(baseline_costs), 2)
    anomaly_cost = round(anomaly_cost, 2)
    cost_vs_median = round(anomaly_cost - baseline_med, 2)
    cost_multiple = round(anomaly_cost / baseline_med, 1) if baseline_med else 0.0

    # Check if anomaly is ongoing (up to 7 days after)
    after_start = anomaly_date + timedelta(days=1)
    after_end = anomaly_date + timedelta(days=7)
    elevated_threshold = baseline_med * 1.5

    after_rows = conn.execute(
        f"""
        SELECT usage_date, SUM(total_net_amortized_cost) AS cost
        FROM daily_cost_summary
        WHERE product_code = ?
          {acct_filter}
          AND usage_date >= ? AND usage_date <= ?
        GROUP BY usage_date
        ORDER BY usage_date
        """,
        [*params, after_start, after_end],
    ).fetchall()

    days_after_checked = len(after_rows)
    elevated_days = sum(1 for _, cost in after_rows if cost > elevated_threshold)
    is_ongoing = elevated_days > 0

    # Usage type attribution (optional — requires CUR data)
    has_cur_data = False
    top_changes: list[UsageTypeChange] = []

    cur_check = conn.execute(
        f"""
        SELECT COUNT(*) FROM cost_line_items
        WHERE product_code = ?
          {acct_filter}
          AND CAST(usage_start_date AS DATE) = ?
        """,
        [*params, anomaly_date],
    ).fetchone()

    if cur_check and cur_check[0] > 0:
        has_cur_data = True

        # Baseline avg per usage_type
        # Anomaly day per usage_type
        change_rows = conn.execute(
            f"""
            WITH baseline AS (
                SELECT usage_type,
                       SUM(net_unblended_cost) / ? AS avg_cost
                FROM cost_line_items
                WHERE product_code = ?
                  {acct_filter}
                  AND CAST(usage_start_date AS DATE) >= ?
                  AND CAST(usage_start_date AS DATE) <= ?
                GROUP BY usage_type
            ),
            anomaly AS (
                SELECT usage_type,
                       SUM(net_unblended_cost) AS cost
                FROM cost_line_items
                WHERE product_code = ?
                  {acct_filter}
                  AND CAST(usage_start_date AS DATE) = ?
                GROUP BY usage_type
            )
            SELECT
                COALESCE(b.usage_type, a.usage_type) AS usage_type,
                COALESCE(b.avg_cost, 0) AS baseline_cost,
                COALESCE(a.cost, 0) AS anomaly_cost
            FROM baseline b
            FULL OUTER JOIN anomaly a ON b.usage_type = a.usage_type
            ORDER BY ABS(COALESCE(a.cost, 0) - COALESCE(b.avg_cost, 0)) DESC
            LIMIT 10
            """,
            [
                max(len(baseline_costs), 1),
                *params, baseline_start, baseline_end,
                *params, anomaly_date,
            ],
        ).fetchall()

        for usage_type, base_cost, anom_cost in change_rows:
            abs_change = anom_cost - base_cost
            pct = (abs_change / base_cost * 100) if base_cost else None
            top_changes.append(UsageTypeChange(
                usage_type=usage_type or "unknown",
                baseline_cost=round(base_cost, 2),
                anomaly_cost=round(anom_cost, 2),
                absolute_change=round(abs_change, 2),
                pct_change=round(pct, 1) if pct is not None else None,
            ))

    return AnomalyExplanation(
        service=service,
        anomaly_date=anomaly_date,
        account_id=account_id,
        baseline_median=baseline_med,
        baseline_min=baseline_min_val,
        baseline_max=baseline_max_val,
        anomaly_cost=anomaly_cost,
        cost_vs_median=cost_vs_median,
        cost_multiple=cost_multiple,
        is_ongoing=is_ongoing,
        days_after_checked=days_after_checked,
        elevated_days_after=elevated_days,
        has_cur_data=has_cur_data,
        has_baseline=has_baseline,
        top_usage_type_changes=top_changes,
    )
