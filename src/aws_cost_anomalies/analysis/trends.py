"""Daily trend aggregation queries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import duckdb


@dataclass
class TrendRow:
    usage_date: date
    group_value: str
    total_cost: float
    cost_change: float | None
    pct_change: float | None


def get_daily_trends(
    conn: duckdb.DuckDBPyConnection,
    days: int = 14,
    group_by: str = "product_code",
    top_n: int = 10,
) -> list[TrendRow]:
    """Get daily cost trends grouped by a dimension.

    Args:
        conn: DuckDB connection
        days: Number of days to look back
        group_by: Column to group by (product_code, usage_account_id, region)
        top_n: Limit to top N groups by total cost

    Returns list of TrendRow sorted by date and group.
    """
    valid_groups = {"product_code", "usage_account_id", "region"}
    if group_by not in valid_groups:
        raise ValueError(f"group_by must be one of {valid_groups}")

    cutoff = date.today() - timedelta(days=days)

    # First get top N groups by total cost in the window
    top_groups = conn.execute(
        f"""
        SELECT {group_by}, SUM(total_unblended_cost) as total
        FROM daily_cost_summary
        WHERE usage_date >= ?
        GROUP BY {group_by}
        ORDER BY total DESC
        LIMIT ?
        """,
        [cutoff, top_n],
    ).fetchall()

    if not top_groups:
        return []

    group_values = [row[0] for row in top_groups]
    placeholders = ", ".join(["?"] * len(group_values))

    rows = conn.execute(
        f"""
        WITH daily AS (
            SELECT
                usage_date,
                {group_by} AS group_value,
                SUM(total_unblended_cost) AS total_cost
            FROM daily_cost_summary
            WHERE usage_date >= ?
              AND {group_by} IN ({placeholders})
            GROUP BY usage_date, {group_by}
        ),
        with_lag AS (
            SELECT
                usage_date,
                group_value,
                total_cost,
                LAG(total_cost) OVER (PARTITION BY group_value ORDER BY usage_date) AS prev_cost
            FROM daily
        )
        SELECT
            usage_date,
            group_value,
            total_cost,
            total_cost - prev_cost AS cost_change,
            CASE WHEN prev_cost > 0
                THEN (total_cost - prev_cost) / prev_cost * 100
                ELSE NULL END AS pct_change
        FROM with_lag
        ORDER BY usage_date, group_value
        """,
        [cutoff, *group_values],
    ).fetchall()

    return [
        TrendRow(
            usage_date=row[0],
            group_value=row[1] or "unknown",
            total_cost=row[2],
            cost_change=row[3],
            pct_change=row[4],
        )
        for row in rows
    ]


def get_total_daily_costs(
    conn: duckdb.DuckDBPyConnection,
    days: int = 14,
) -> list[tuple[date, float]]:
    """Get total daily cost across all dimensions."""
    cutoff = date.today() - timedelta(days=days)
    rows = conn.execute(
        """
        SELECT usage_date, SUM(total_unblended_cost) AS total_cost
        FROM daily_cost_summary
        WHERE usage_date >= ?
        GROUP BY usage_date
        ORDER BY usage_date
        """,
        [cutoff],
    ).fetchall()
    return [(row[0], row[1]) for row in rows]
