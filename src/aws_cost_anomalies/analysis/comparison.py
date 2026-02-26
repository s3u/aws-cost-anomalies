"""Period-over-period cost comparison."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import duckdb

_SAFE_GROUP_COLUMNS = frozenset({"product_code", "usage_account_id", "region"})


@dataclass
class PeriodComparison:
    period_a: tuple[date, date]  # (start, end)
    period_b: tuple[date, date]
    period_a_total: float
    period_b_total: float
    movers: list[dict]  # sorted by |absolute_change| desc
    new_in_b: list[dict]  # items in B but not A
    disappeared_from_a: list[dict]  # items in A but not B


def compare_periods(
    conn: duckdb.DuckDBPyConnection,
    period_a_start: date,
    period_a_end: date,
    period_b_start: date,
    period_b_end: date,
    group_by: str = "product_code",
    top_n: int = 10,
) -> PeriodComparison:
    """Compare costs between two date ranges grouped by a dimension.

    Args:
        conn: DuckDB connection
        period_a_start: Start of period A (inclusive)
        period_a_end: End of period A (inclusive)
        period_b_start: Start of period B (inclusive)
        period_b_end: End of period B (inclusive)
        group_by: Grouping dimension
        top_n: Number of top movers to return

    Returns:
        PeriodComparison with totals, movers, and new/disappeared items.

    Raises:
        ValueError: If group_by is invalid or dates are inconsistent.
    """
    if period_a_start > period_a_end:
        raise ValueError(
            f"period_a_start ({period_a_start}) must be <= "
            f"period_a_end ({period_a_end})"
        )
    if period_b_start > period_b_end:
        raise ValueError(
            f"period_b_start ({period_b_start}) must be <= "
            f"period_b_end ({period_b_end})"
        )

    # Validate group_by at the point of SQL interpolation to make the
    # safety coupling explicit — only hardcoded column names are allowed.
    if group_by not in _SAFE_GROUP_COLUMNS:
        raise ValueError(
            f"group_by must be one of {sorted(_SAFE_GROUP_COLUMNS)}, "
            f"got '{group_by}'"
        )
    column = group_by  # now guaranteed to be a safe column name

    sql = f"""
    WITH period_a AS (
        SELECT {column} AS group_value, SUM(total_net_amortized_cost) AS cost
        FROM daily_cost_summary
        WHERE usage_date >= ? AND usage_date <= ?
        GROUP BY {column}
    ),
    period_b AS (
        SELECT {column} AS group_value, SUM(total_net_amortized_cost) AS cost
        FROM daily_cost_summary
        WHERE usage_date >= ? AND usage_date <= ?
        GROUP BY {column}
    )
    SELECT
        COALESCE(a.group_value, b.group_value) AS group_value,
        COALESCE(a.cost, 0) AS period_a_cost,
        COALESCE(b.cost, 0) AS period_b_cost
    FROM period_a a
    FULL OUTER JOIN period_b b ON a.group_value = b.group_value
    ORDER BY ABS(COALESCE(b.cost, 0) - COALESCE(a.cost, 0)) DESC
    """  # noqa: S608

    rows = conn.execute(
        sql, [period_a_start, period_a_end, period_b_start, period_b_end]
    ).fetchall()

    movers: list[dict] = []
    new_in_b: list[dict] = []
    disappeared: list[dict] = []
    period_a_total = 0.0
    period_b_total = 0.0

    for group_value, a_cost, b_cost in rows:
        period_a_total += a_cost
        period_b_total += b_cost
        abs_change = b_cost - a_cost
        pct_change = (abs_change / a_cost * 100) if a_cost else None

        entry = {
            "group_value": group_value,
            "period_a_cost": round(a_cost, 2),
            "period_b_cost": round(b_cost, 2),
            "absolute_change": round(abs_change, 2),
            "percentage_change": round(pct_change, 1) if pct_change is not None else None,
        }

        if a_cost == 0:
            new_in_b.append(entry)
        elif b_cost == 0:
            disappeared.append(entry)
        else:
            movers.append(entry)

    return PeriodComparison(
        period_a=(period_a_start, period_a_end),
        period_b=(period_b_start, period_b_end),
        period_a_total=round(period_a_total, 2),
        period_b_total=round(period_b_total, 2),
        movers=movers[:top_n],
        new_in_b=new_in_b,
        disappeared_from_a=disappeared,
    )
