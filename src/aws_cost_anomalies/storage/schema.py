"""DDL for cost tables and daily summary rebuild."""

from __future__ import annotations

import duckdb

COST_LINE_ITEMS_DDL = """
CREATE TABLE IF NOT EXISTS cost_line_items (
    line_item_id VARCHAR,
    usage_start_date TIMESTAMP,
    usage_end_date TIMESTAMP,
    billing_period_start DATE,
    billing_period_end DATE,
    payer_account_id VARCHAR,
    usage_account_id VARCHAR,
    product_code VARCHAR,
    product_name VARCHAR,
    region VARCHAR,
    availability_zone VARCHAR,
    usage_type VARCHAR,
    operation VARCHAR,
    resource_id VARCHAR,
    line_item_type VARCHAR,
    unblended_cost DOUBLE,
    blended_cost DOUBLE,
    net_unblended_cost DOUBLE,
    usage_amount DOUBLE,
    currency_code VARCHAR,
    line_item_description VARCHAR,
    _ingested_at TIMESTAMP DEFAULT current_timestamp,
    _source_file VARCHAR
)
"""

DAILY_COST_SUMMARY_DDL = """
CREATE TABLE IF NOT EXISTS daily_cost_summary (
    usage_date DATE,
    usage_account_id VARCHAR,
    product_code VARCHAR,
    region VARCHAR,
    total_unblended_cost DOUBLE,
    total_blended_cost DOUBLE,
    total_usage_amount DOUBLE,
    line_item_count BIGINT
)
"""

INGESTION_LOG_DDL = """
CREATE TABLE IF NOT EXISTS ingestion_log (
    assembly_id VARCHAR,
    billing_period VARCHAR,
    s3_key VARCHAR,
    rows_loaded BIGINT,
    ingested_at TIMESTAMP DEFAULT current_timestamp
)
"""

_INDEXES = [
    # Speed up daily summary rebuild and date-range queries
    (
        "idx_cli_usage_date",
        "cost_line_items(usage_start_date)",
    ),
    (
        "idx_cli_billing_period",
        "cost_line_items(billing_period_start)",
    ),
    # Speed up trend/anomaly queries on daily_cost_summary
    (
        "idx_dcs_date",
        "daily_cost_summary(usage_date)",
    ),
    (
        "idx_dcs_product",
        "daily_cost_summary(product_code)",
    ),
    (
        "idx_dcs_account",
        "daily_cost_summary(usage_account_id)",
    ),
    # Speed up incremental ingestion checks
    (
        "idx_il_period",
        "ingestion_log(billing_period)",
    ),
]

_EXCLUDED_LINE_ITEM_TYPES = (
    "'Tax', 'Fee', 'Credit', 'Refund', 'BundledDiscount'"
)


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all tables and indexes if they don't exist."""
    conn.execute(COST_LINE_ITEMS_DDL)
    conn.execute(DAILY_COST_SUMMARY_DDL)
    conn.execute(INGESTION_LOG_DDL)
    for idx_name, idx_def in _INDEXES:
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS {idx_name} "
            f"ON {idx_def}"
        )


def rebuild_daily_summary(
    conn: duckdb.DuckDBPyConnection,
) -> int:
    """Rebuild daily_cost_summary from cost_line_items.

    Excludes non-usage line item types.
    Returns the number of rows in the rebuilt summary.
    """
    conn.execute("DELETE FROM daily_cost_summary")
    conn.execute(f"""
        INSERT INTO daily_cost_summary
        SELECT
            CAST(usage_start_date AS DATE) AS usage_date,
            usage_account_id,
            product_code,
            region,
            SUM(unblended_cost) AS total_unblended_cost,
            SUM(blended_cost) AS total_blended_cost,
            SUM(usage_amount) AS total_usage_amount,
            COUNT(*) AS line_item_count
        FROM cost_line_items
        WHERE line_item_type NOT IN ({_EXCLUDED_LINE_ITEM_TYPES})
        GROUP BY
            CAST(usage_start_date AS DATE),
            usage_account_id,
            product_code,
            region
    """)
    result = conn.execute(
        "SELECT COUNT(*) FROM daily_cost_summary"
    ).fetchone()
    return result[0] if result else 0
