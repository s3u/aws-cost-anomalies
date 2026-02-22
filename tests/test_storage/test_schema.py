"""Tests for schema DDL and daily summary rebuild."""

from __future__ import annotations

import duckdb

from aws_cost_anomalies.storage.schema import create_tables, rebuild_daily_summary


def test_create_tables():
    conn = duckdb.connect(":memory:")
    create_tables(conn)
    tables = conn.execute("SHOW TABLES").fetchall()
    table_names = {t[0] for t in tables}
    assert "cost_line_items" in table_names
    assert "daily_cost_summary" in table_names
    assert "ingestion_log" in table_names


def test_create_tables_idempotent():
    conn = duckdb.connect(":memory:")
    create_tables(conn)
    create_tables(conn)  # should not raise


def test_rebuild_daily_summary(db_with_data):
    row_count = rebuild_daily_summary(db_with_data)
    assert row_count > 0

    # Check that Tax items would be excluded
    summary = db_with_data.execute(
        "SELECT COUNT(*) FROM daily_cost_summary"
    ).fetchone()
    assert summary[0] == row_count


def test_rebuild_daily_summary_excludes_tax(db):
    """Verify Tax line items are excluded from daily summary."""

    db.execute(
        """INSERT INTO cost_line_items
        (line_item_id, usage_start_date, usage_end_date, billing_period_start,
         billing_period_end, payer_account_id, usage_account_id, product_code,
         product_name, region, line_item_type, unblended_cost, blended_cost,
         usage_amount, currency_code, _source_file)
        VALUES
        ('usage-1', '2025-01-01', '2025-01-02', '2025-01-01', '2025-02-01',
         '999', '111', 'AmazonEC2', 'EC2', 'us-east-1', 'Usage', 100.0, 95.0,
         10.0, 'USD', 'test.parquet'),
        ('tax-1', '2025-01-01', '2025-01-02', '2025-01-01', '2025-02-01',
         '999', '111', 'AmazonEC2', 'EC2', 'us-east-1', 'Tax', 10.0, 10.0,
         0.0, 'USD', 'test.parquet')
        """
    )
    row_count = rebuild_daily_summary(db)
    assert row_count == 1
    cost = db.execute(
        "SELECT total_unblended_cost FROM daily_cost_summary"
    ).fetchone()
    assert cost[0] == 100.0
