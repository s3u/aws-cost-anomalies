"""Line-item attribution for cost changes between two periods."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import duckdb


@dataclass
class AttributionItem:
    key: str
    period_a_cost: float
    period_b_cost: float
    absolute_change: float
    pct_change: float | None


@dataclass
class CostAttribution:
    service: str
    period_a: tuple[date, date]
    period_b: tuple[date, date]
    account_id: str | None
    period_a_total: float
    period_b_total: float
    movers_by_usage_type: list[AttributionItem] = field(default_factory=list)
    new_by_usage_type: list[AttributionItem] = field(default_factory=list)
    disappeared_by_usage_type: list[AttributionItem] = field(default_factory=list)
    movers_by_resource: list[AttributionItem] = field(default_factory=list)
    new_by_resource: list[AttributionItem] = field(default_factory=list)
    disappeared_by_resource: list[AttributionItem] = field(default_factory=list)


def attribute_cost_change(
    conn: duckdb.DuckDBPyConnection,
    service: str,
    period_a_start: date,
    period_a_end: date,
    period_b_start: date,
    period_b_end: date,
    account_id: str | None = None,
    top_n: int = 10,
) -> CostAttribution:
    """Compare two periods at line-item level for a service.

    Shows which usage types and resources are new, gone, or changed.
    Requires CUR data in the cost_line_items table.

    Args:
        conn: DuckDB connection
        service: AWS product_code (e.g. "AmazonEC2")
        period_a_start: Start of period A (inclusive)
        period_a_end: End of period A (inclusive)
        period_b_start: Start of period B (inclusive)
        period_b_end: End of period B (inclusive)
        account_id: Optional account filter
        top_n: Number of top items per category

    Returns:
        CostAttribution with breakdowns by usage_type and resource_id.

    Raises:
        ValueError: If dates are invalid or no CUR data is available.
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

    # Build optional account filter
    acct_filter = ""
    base_params: list = [service]
    if account_id:
        acct_filter = " AND usage_account_id = ?"
        base_params.append(account_id)

    # Check CUR data exists for either period
    count_row = conn.execute(
        f"""
        SELECT COUNT(*) FROM cost_line_items
        WHERE product_code = ?
          {acct_filter}
          AND (
            (CAST(usage_start_date AS DATE) >= ? AND CAST(usage_start_date AS DATE) <= ?)
            OR
            (CAST(usage_start_date AS DATE) >= ? AND CAST(usage_start_date AS DATE) <= ?)
          )
        """,
        [*base_params, period_a_start, period_a_end, period_b_start, period_b_end],
    ).fetchone()

    if not count_row or count_row[0] == 0:
        raise ValueError(
            f"No CUR data found for {service} in either period. "
            f"Attribution requires CUR (Cost & Usage Report) data "
            f"in the cost_line_items table."
        )

    _safe_dimensions = frozenset({"usage_type", "resource_id"})

    def _query_dimension(dimension: str) -> tuple[list[AttributionItem], list[AttributionItem], list[AttributionItem], float, float]:
        """Query a dimension and categorize into movers/new/disappeared."""
        if dimension not in _safe_dimensions:
            raise ValueError(
                f"dimension must be one of {sorted(_safe_dimensions)}, "
                f"got '{dimension}'"
            )
        sql = f"""
        WITH period_a AS (
            SELECT {dimension} AS key, SUM(unblended_cost) AS cost
            FROM cost_line_items
            WHERE product_code = ?
              {acct_filter}
              AND CAST(usage_start_date AS DATE) >= ?
              AND CAST(usage_start_date AS DATE) <= ?
            GROUP BY {dimension}
        ),
        period_b AS (
            SELECT {dimension} AS key, SUM(unblended_cost) AS cost
            FROM cost_line_items
            WHERE product_code = ?
              {acct_filter}
              AND CAST(usage_start_date AS DATE) >= ?
              AND CAST(usage_start_date AS DATE) <= ?
            GROUP BY {dimension}
        )
        SELECT
            COALESCE(a.key, b.key) AS key,
            COALESCE(a.cost, 0) AS a_cost,
            COALESCE(b.cost, 0) AS b_cost
        FROM period_a a
        FULL OUTER JOIN period_b b ON a.key = b.key
        ORDER BY ABS(COALESCE(b.cost, 0) - COALESCE(a.cost, 0)) DESC
        """  # noqa: S608

        params_a = [*base_params, period_a_start, period_a_end]
        params_b = [*base_params, period_b_start, period_b_end]
        rows = conn.execute(sql, [*params_a, *params_b]).fetchall()

        movers: list[AttributionItem] = []
        new: list[AttributionItem] = []
        disappeared: list[AttributionItem] = []
        total_a = 0.0
        total_b = 0.0

        for key, a_cost, b_cost in rows:
            total_a += a_cost
            total_b += b_cost
            abs_change = b_cost - a_cost
            pct_change = (abs_change / a_cost * 100) if a_cost else None

            item = AttributionItem(
                key=key or "unknown",
                period_a_cost=round(a_cost, 2),
                period_b_cost=round(b_cost, 2),
                absolute_change=round(abs_change, 2),
                pct_change=round(pct_change, 1) if pct_change is not None else None,
            )

            if a_cost == 0:
                new.append(item)
            elif b_cost == 0:
                disappeared.append(item)
            else:
                movers.append(item)

        return movers[:top_n], new[:top_n], disappeared[:top_n], total_a, total_b

    ut_movers, ut_new, ut_disappeared, total_a, total_b = _query_dimension("usage_type")
    # Resource totals are the same as usage_type totals (same underlying rows)
    res_movers, res_new, res_disappeared, _, _ = _query_dimension("resource_id")

    return CostAttribution(
        service=service,
        period_a=(period_a_start, period_a_end),
        period_b=(period_b_start, period_b_end),
        account_id=account_id,
        period_a_total=round(total_a, 2),
        period_b_total=round(total_b, 2),
        movers_by_usage_type=ut_movers,
        new_by_usage_type=ut_new,
        disappeared_by_usage_type=ut_disappeared,
        movers_by_resource=res_movers,
        new_by_resource=res_new,
        disappeared_by_resource=res_disappeared,
    )
