"""Daily trend aggregation queries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

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
    data_source: str | None = None,
) -> list[TrendRow]:
    """Get daily cost trends grouped by a dimension.

    Args:
        conn: DuckDB connection
        days: Number of days to look back
        group_by: Column to group by (product_code, usage_account_id, region)
        top_n: Limit to top N groups by total cost
        data_source: Filter by data source ('cur' or 'cost_explorer'). None = all.

    Returns list of TrendRow sorted by date and group.
    """
    valid_groups = {"product_code", "usage_account_id", "region"}
    if group_by not in valid_groups:
        raise ValueError(f"group_by must be one of {valid_groups}")

    cutoff = datetime.now(timezone.utc).date() - timedelta(days=days)

    source_filter = ""
    params: list = [cutoff]
    if data_source:
        source_filter = " AND data_source = ?"
        params.append(data_source)

    # First get top N groups by total cost in the window
    top_groups = conn.execute(
        f"""
        SELECT {group_by}, SUM(total_unblended_cost) as total
        FROM daily_cost_summary
        WHERE usage_date >= ?{source_filter}
        GROUP BY {group_by}
        ORDER BY total DESC
        LIMIT ?
        """,
        [*params, top_n],
    ).fetchall()

    if not top_groups:
        return []

    group_values = [row[0] for row in top_groups]
    placeholders = ", ".join(["?"] * len(group_values))

    detail_params: list = [cutoff]
    detail_source_filter = ""
    if data_source:
        detail_source_filter = " AND data_source = ?"
        detail_params.append(data_source)

    rows = conn.execute(
        f"""
        WITH daily AS (
            SELECT
                usage_date,
                {group_by} AS group_value,
                SUM(total_unblended_cost) AS total_cost
            FROM daily_cost_summary
            WHERE usage_date >= ?{detail_source_filter}
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
        [*detail_params, *group_values],
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


_SAFE_GROUP_COLUMNS = frozenset({"product_code", "usage_account_id", "region"})
_VALID_GRANULARITIES = frozenset({"daily", "weekly", "monthly"})

_GRANULARITY_TRUNC = {
    "daily": "day",
    "weekly": "week",
    "monthly": "month",
}


@dataclass
class CostTrendPoint:
    usage_date: date
    cost: float
    group_value: str | None = None


@dataclass
class CostTrendResult:
    date_start: date
    date_end: date
    granularity: str
    group_by: str | None
    filter_value: str | None
    points: list[CostTrendPoint]
    total: float
    average: float
    min_cost: float
    max_cost: float


def get_cost_trend(
    conn: duckdb.DuckDBPyConnection,
    date_start: date,
    date_end: date,
    group_by: str | None = None,
    filter_value: str | None = None,
    granularity: str = "daily",
) -> CostTrendResult:
    """Get a cost time series with optional grouping and filtering.

    Args:
        conn: DuckDB connection
        date_start: Start date (inclusive)
        date_end: End date (inclusive)
        group_by: Optional column to group by
        filter_value: Filter to a specific value of group_by
        granularity: "daily", "weekly", or "monthly"

    Returns:
        CostTrendResult with data points and summary statistics.

    Raises:
        ValueError: If parameters are invalid.
    """
    if date_start > date_end:
        raise ValueError(
            f"date_start ({date_start}) must be <= date_end ({date_end})"
        )

    if granularity not in _VALID_GRANULARITIES:
        raise ValueError(
            f"granularity must be one of {sorted(_VALID_GRANULARITIES)}, "
            f"got '{granularity}'"
        )

    if filter_value and not group_by:
        raise ValueError(
            "filter_value requires group_by to be specified."
        )

    if group_by and group_by not in _SAFE_GROUP_COLUMNS:
        raise ValueError(
            f"group_by must be one of {sorted(_SAFE_GROUP_COLUMNS)}, "
            f"got '{group_by}'"
        )

    trunc_unit = _GRANULARITY_TRUNC[granularity]
    params: list = [date_start, date_end]

    filter_clause = ""
    if filter_value and group_by:
        filter_clause = f" AND {group_by} = ?"
        params.append(filter_value)

    if group_by:
        column = group_by  # validated above
        sql = f"""
        SELECT DATE_TRUNC('{trunc_unit}', usage_date) AS period_date,
               {column} AS group_value,
               SUM(total_unblended_cost) AS cost
        FROM daily_cost_summary
        WHERE usage_date >= ? AND usage_date <= ?
          {filter_clause}
        GROUP BY period_date, {column}
        ORDER BY period_date, {column}
        """  # noqa: S608
    else:
        sql = f"""
        SELECT DATE_TRUNC('{trunc_unit}', usage_date) AS period_date,
               SUM(total_unblended_cost) AS cost
        FROM daily_cost_summary
        WHERE usage_date >= ? AND usage_date <= ?
          {filter_clause}
        GROUP BY period_date
        ORDER BY period_date
        """  # noqa: S608

    rows = conn.execute(sql, params).fetchall()

    points: list[CostTrendPoint] = []
    costs: list[float] = []

    for row in rows:
        if group_by:
            period_date, group_value, cost = row
        else:
            period_date, cost = row
            group_value = None

        cost_val = round(cost, 2)
        # DuckDB DATE_TRUNC returns a timestamp; convert to date
        if hasattr(period_date, "date"):
            period_date = period_date.date()
        points.append(CostTrendPoint(
            usage_date=period_date,
            cost=cost_val,
            group_value=group_value,
        ))
        costs.append(cost_val)

    total = round(sum(costs), 2) if costs else 0.0
    average = round(total / len(costs), 2) if costs else 0.0
    min_cost = round(min(costs), 2) if costs else 0.0
    max_cost = round(max(costs), 2) if costs else 0.0

    return CostTrendResult(
        date_start=date_start,
        date_end=date_end,
        granularity=granularity,
        group_by=group_by,
        filter_value=filter_value,
        points=points,
        total=total,
        average=average,
        min_cost=min_cost,
        max_cost=max_cost,
    )


def get_total_daily_costs(
    conn: duckdb.DuckDBPyConnection,
    days: int = 14,
    data_source: str | None = None,
) -> list[tuple[date, float]]:
    """Get total daily cost across all dimensions."""
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=days)

    source_filter = ""
    params: list = [cutoff]
    if data_source:
        source_filter = " AND data_source = ?"
        params.append(data_source)

    rows = conn.execute(
        f"""
        SELECT usage_date, SUM(total_unblended_cost) AS total_cost
        FROM daily_cost_summary
        WHERE usage_date >= ?{source_filter}
        GROUP BY usage_date
        ORDER BY usage_date
        """,
        params,
    ).fetchall()
    return [(row[0], row[1]) for row in rows]
