"""Tests for schema DDL and daily summary rebuild."""

from __future__ import annotations

from datetime import date

import duckdb

from aws_cost_anomalies.storage.schema import (
    create_tables,
    insert_cost_explorer_summary,
    rebuild_daily_summary,
)


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


def test_rebuild_preserves_cost_explorer_data(db):
    """Rebuild of CUR data must not delete Cost Explorer rows."""
    # Insert CE data first
    ce_rows = [
        (date(2025, 1, 15), "111", "AmazonEC2", "", 200.0, 190.0, 0.0, 0),
    ]
    insert_cost_explorer_summary(db, ce_rows)

    # Insert CUR line items and rebuild
    db.execute(
        """INSERT INTO cost_line_items
        (line_item_id, usage_start_date, usage_end_date, billing_period_start,
         billing_period_end, payer_account_id, usage_account_id, product_code,
         product_name, region, line_item_type, unblended_cost, blended_cost,
         usage_amount, currency_code, _source_file)
        VALUES
        ('u-1', '2025-01-10', '2025-01-11', '2025-01-01', '2025-02-01',
         '999', '111', 'AmazonEC2', 'EC2', 'us-east-1', 'Usage', 50.0, 48.0,
         5.0, 'USD', 'test.parquet')
        """
    )
    rebuild_daily_summary(db)

    # Both rows should exist
    total = db.execute(
        "SELECT COUNT(*) FROM daily_cost_summary"
    ).fetchone()[0]
    assert total == 2

    ce_count = db.execute(
        "SELECT COUNT(*) FROM daily_cost_summary "
        "WHERE data_source = 'cost_explorer'"
    ).fetchone()[0]
    assert ce_count == 1

    cur_count = db.execute(
        "SELECT COUNT(*) FROM daily_cost_summary "
        "WHERE data_source = 'cur'"
    ).fetchone()[0]
    assert cur_count == 1


def test_insert_cost_explorer_summary_basic(db):
    """Insert CE rows and verify they appear with correct data_source."""
    rows = [
        (date(2025, 1, 1), "111", "AmazonEC2", "", 100.0, 95.0, 0.0, 0),
        (date(2025, 1, 2), "111", "AmazonS3", "", 20.0, 19.0, 0.0, 0),
    ]
    count = insert_cost_explorer_summary(db, rows)
    assert count == 2

    result = db.execute(
        "SELECT COUNT(*) FROM daily_cost_summary "
        "WHERE data_source = 'cost_explorer'"
    ).fetchone()
    assert result[0] == 2


def test_insert_cost_explorer_summary_replaces_previous(db):
    """Calling insert twice replaces old CE rows (no duplicates)."""
    rows_v1 = [
        (date(2025, 1, 1), "111", "AmazonEC2", "", 100.0, 95.0, 0.0, 0),
    ]
    insert_cost_explorer_summary(db, rows_v1)

    rows_v2 = [
        (date(2025, 1, 1), "111", "AmazonEC2", "", 110.0, 105.0, 0.0, 0),
        (date(2025, 1, 2), "111", "AmazonEC2", "", 50.0, 48.0, 0.0, 0),
    ]
    count = insert_cost_explorer_summary(db, rows_v2)
    assert count == 2

    # Only the v2 rows should exist
    total = db.execute(
        "SELECT COUNT(*) FROM daily_cost_summary "
        "WHERE data_source = 'cost_explorer'"
    ).fetchone()[0]
    assert total == 2

    cost = db.execute(
        "SELECT total_unblended_cost FROM daily_cost_summary "
        "WHERE data_source = 'cost_explorer' AND usage_date = '2025-01-01'"
    ).fetchone()[0]
    assert cost == 110.0


def test_insert_cost_explorer_empty_rows(db):
    """Inserting empty list is a no-op and returns 0."""
    rows = [
        (date(2025, 1, 1), "111", "AmazonEC2", "", 100.0, 95.0, 0.0, 0),
    ]
    insert_cost_explorer_summary(db, rows)
    count = insert_cost_explorer_summary(db, [])
    assert count == 0

    # Existing CE data should be preserved (empty insert is no-op)
    total = db.execute(
        "SELECT COUNT(*) FROM daily_cost_summary "
        "WHERE data_source = 'cost_explorer'"
    ).fetchone()[0]
    assert total == 1


def test_insert_cost_explorer_preserves_outside_date_range(db):
    """Importing a narrow date range preserves CE data outside that range."""
    # Import a wide range: Jan 1 and Jan 15
    wide_rows = [
        (date(2025, 1, 1), "111", "AmazonEC2", "", 100.0, 95.0, 0.0, 0),
        (date(2025, 1, 15), "111", "AmazonS3", "", 50.0, 48.0, 0.0, 0),
    ]
    insert_cost_explorer_summary(db, wide_rows)

    # Now import just Jan 15 with updated cost
    narrow_rows = [
        (date(2025, 1, 15), "111", "AmazonS3", "", 55.0, 52.0, 0.0, 0),
    ]
    insert_cost_explorer_summary(db, narrow_rows)

    # Jan 1 data should still exist
    jan1 = db.execute(
        "SELECT total_unblended_cost FROM daily_cost_summary "
        "WHERE data_source = 'cost_explorer' AND usage_date = '2025-01-01'"
    ).fetchone()
    assert jan1 is not None
    assert jan1[0] == 100.0

    # Jan 15 data should be the updated value
    jan15 = db.execute(
        "SELECT total_unblended_cost FROM daily_cost_summary "
        "WHERE data_source = 'cost_explorer' AND usage_date = '2025-01-15'"
    ).fetchone()
    assert jan15[0] == 55.0


def test_insert_cost_explorer_preserves_cur_data(db):
    """Inserting CE data must not touch CUR rows."""
    # Insert CUR line items and rebuild
    db.execute(
        """INSERT INTO cost_line_items
        (line_item_id, usage_start_date, usage_end_date, billing_period_start,
         billing_period_end, payer_account_id, usage_account_id, product_code,
         product_name, region, line_item_type, unblended_cost, blended_cost,
         usage_amount, currency_code, _source_file)
        VALUES
        ('u-1', '2025-01-10', '2025-01-11', '2025-01-01', '2025-02-01',
         '999', '111', 'AmazonEC2', 'EC2', 'us-east-1', 'Usage', 50.0, 48.0,
         5.0, 'USD', 'test.parquet')
        """
    )
    rebuild_daily_summary(db)

    # Now insert CE data
    ce_rows = [
        (date(2025, 1, 15), "111", "AmazonEC2", "", 200.0, 190.0, 0.0, 0),
    ]
    insert_cost_explorer_summary(db, ce_rows)

    cur_count = db.execute(
        "SELECT COUNT(*) FROM daily_cost_summary "
        "WHERE data_source = 'cur'"
    ).fetchone()[0]
    assert cur_count == 1
