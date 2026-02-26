"""Drill-down analysis for cost spikes — break down by usage_type, operation, resource."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import duckdb


@dataclass
class DrillDownResult:
    service: str
    date_start: date
    date_end: date
    account_id: str | None
    total_cost: float
    breakdown_by_usage_type: list[dict] = field(default_factory=list)
    breakdown_by_operation: list[dict] = field(default_factory=list)
    top_resources: list[dict] = field(default_factory=list)


def drill_down_cost_spike(
    conn: duckdb.DuckDBPyConnection,
    service: str,
    date_start: date,
    date_end: date,
    account_id: str | None = None,
    top_n: int = 10,
) -> DrillDownResult:
    """Break down costs for a service by usage_type, operation, and resource_id.

    Requires CUR data in the cost_line_items table (Cost Explorer data
    does not include usage_type, operation, or resource_id).

    Args:
        conn: DuckDB connection
        service: AWS product_code (e.g. "AmazonEC2")
        date_start: Start date (inclusive)
        date_end: End date (inclusive)
        account_id: Optional account filter
        top_n: Number of top items per breakdown

    Returns:
        DrillDownResult with breakdowns by usage_type, operation, and resource.

    Raises:
        ValueError: If dates are invalid or no CUR data is available.
    """
    if date_start > date_end:
        raise ValueError(
            f"date_start ({date_start}) must be <= date_end ({date_end})"
        )

    # Build optional account filter
    acct_filter = ""
    params: list = [date_start, date_end, service]
    if account_id:
        acct_filter = " AND usage_account_id = ?"
        params.append(account_id)

    # Check CUR data exists for this range/service
    count_row = conn.execute(
        f"""
        SELECT COUNT(*) AS cnt
        FROM cost_line_items
        WHERE CAST(usage_start_date AS DATE) >= ?
          AND CAST(usage_start_date AS DATE) <= ?
          AND product_code = ?
          {acct_filter}
        """,
        params,
    ).fetchone()

    if not count_row or count_row[0] == 0:
        raise ValueError(
            f"No CUR data found for {service} between {date_start} and "
            f"{date_end}. Drill-down requires CUR (Cost & Usage Report) "
            f"data in the cost_line_items table."
        )

    # Total cost
    total_row = conn.execute(
        f"""
        SELECT SUM(unblended_cost) AS total
        FROM cost_line_items
        WHERE CAST(usage_start_date AS DATE) >= ?
          AND CAST(usage_start_date AS DATE) <= ?
          AND product_code = ?
          {acct_filter}
        """,
        params,
    ).fetchone()
    total_cost = total_row[0] if total_row and total_row[0] else 0.0

    # Breakdown by usage_type
    usage_type_rows = conn.execute(
        f"""
        SELECT usage_type,
               SUM(unblended_cost) AS cost,
               SUM(usage_amount) AS usage_amount
        FROM cost_line_items
        WHERE CAST(usage_start_date AS DATE) >= ?
          AND CAST(usage_start_date AS DATE) <= ?
          AND product_code = ?
          {acct_filter}
        GROUP BY usage_type
        ORDER BY cost DESC
        LIMIT ?
        """,
        [*params, top_n],
    ).fetchall()

    breakdown_by_usage_type = [
        {
            "usage_type": row[0] or "unknown",
            "cost": round(row[1], 2),
            "pct_of_total": round(row[1] / total_cost * 100, 1) if total_cost else 0.0,
            "usage_amount": round(row[2], 2) if row[2] else 0.0,
        }
        for row in usage_type_rows
    ]

    # Breakdown by operation
    operation_rows = conn.execute(
        f"""
        SELECT operation,
               SUM(unblended_cost) AS cost
        FROM cost_line_items
        WHERE CAST(usage_start_date AS DATE) >= ?
          AND CAST(usage_start_date AS DATE) <= ?
          AND product_code = ?
          {acct_filter}
        GROUP BY operation
        ORDER BY cost DESC
        LIMIT ?
        """,
        [*params, top_n],
    ).fetchall()

    breakdown_by_operation = [
        {
            "operation": row[0] or "unknown",
            "cost": round(row[1], 2),
            "pct_of_total": round(row[1] / total_cost * 100, 1) if total_cost else 0.0,
        }
        for row in operation_rows
    ]

    # Top resources (exclude NULL/empty resource_ids)
    resource_rows = conn.execute(
        f"""
        SELECT resource_id,
               SUM(unblended_cost) AS cost
        FROM cost_line_items
        WHERE CAST(usage_start_date AS DATE) >= ?
          AND CAST(usage_start_date AS DATE) <= ?
          AND product_code = ?
          AND resource_id IS NOT NULL
          AND resource_id != ''
          {acct_filter}
        GROUP BY resource_id
        ORDER BY cost DESC
        LIMIT ?
        """,
        [*params, top_n],
    ).fetchall()

    top_resources = [
        {
            "resource_id": row[0],
            "cost": round(row[1], 2),
            "pct_of_total": round(row[1] / total_cost * 100, 1) if total_cost else 0.0,
        }
        for row in resource_rows
    ]

    return DrillDownResult(
        service=service,
        date_start=date_start,
        date_end=date_end,
        account_id=account_id,
        total_cost=round(total_cost, 2),
        breakdown_by_usage_type=breakdown_by_usage_type,
        breakdown_by_operation=breakdown_by_operation,
        top_resources=top_resources,
    )
