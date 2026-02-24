"""Tests for parquet loading and column mapping."""

from __future__ import annotations

import tempfile
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from aws_cost_anomalies.ingestion.loader import (
    build_select_clause,
    delete_billing_period_data,
    detect_cur_version,
    get_ingested_assemblies,
    load_parquet_file,
    record_ingestion,
)
from aws_cost_anomalies.storage.schema import create_tables


def _make_v2_parquet(path: Path, num_rows: int = 5) -> Path:
    """Create a minimal CUR v2 parquet file."""
    table = pa.table(
        {
            "identity_line_item_id": [f"id-{i}" for i in range(num_rows)],
            "line_item_usage_start_date": ["2025-01-01T00:00:00Z"] * num_rows,
            "line_item_usage_end_date": ["2025-01-02T00:00:00Z"] * num_rows,
            "bill_billing_period_start_date": ["2025-01-01"] * num_rows,
            "bill_billing_period_end_date": ["2025-02-01"] * num_rows,
            "bill_payer_account_id": ["999999999999"] * num_rows,
            "line_item_usage_account_id": ["111111111111"] * num_rows,
            "product_product_code": ["AmazonEC2"] * num_rows,
            "product_product_name": ["Amazon Elastic Compute Cloud"] * num_rows,
            "product_region": ["us-east-1"] * num_rows,
            "line_item_availability_zone": ["us-east-1a"] * num_rows,
            "line_item_usage_type": ["BoxUsage:m5.xlarge"] * num_rows,
            "line_item_operation": ["RunInstances"] * num_rows,
            "line_item_resource_id": [f"i-{i:08d}" for i in range(num_rows)],
            "line_item_line_item_type": ["Usage"] * num_rows,
            "line_item_unblended_cost": [10.0 + i for i in range(num_rows)],
            "line_item_blended_cost": [9.5 + i for i in range(num_rows)],
            "line_item_net_unblended_cost": [9.0 + i for i in range(num_rows)],
            "line_item_usage_amount": [100.0] * num_rows,
            "line_item_currency_code": ["USD"] * num_rows,
            "line_item_line_item_description": ["EC2 usage"] * num_rows,
        }
    )
    parquet_path = path / "test_cur_v2.parquet"
    pq.write_table(table, parquet_path)
    return parquet_path


def _make_v1_parquet(path: Path, num_rows: int = 3) -> Path:
    """Create a minimal CUR v1 parquet file with slash-separated column names."""
    table = pa.table(
        {
            "identity/LineItemId": [f"id-{i}" for i in range(num_rows)],
            "lineItem/UsageStartDate": ["2025-01-01T00:00:00Z"] * num_rows,
            "lineItem/UsageEndDate": ["2025-01-02T00:00:00Z"] * num_rows,
            "bill/BillingPeriodStartDate": ["2025-01-01"] * num_rows,
            "bill/BillingPeriodEndDate": ["2025-02-01"] * num_rows,
            "bill/PayerAccountId": ["999999999999"] * num_rows,
            "lineItem/UsageAccountId": ["111111111111"] * num_rows,
            "product/ProductCode": ["AmazonS3"] * num_rows,
            "product/ProductName": ["Amazon Simple Storage Service"] * num_rows,
            "product/region": ["us-west-2"] * num_rows,
            "lineItem/LineItemType": ["Usage"] * num_rows,
            "lineItem/UnblendedCost": [5.0 + i for i in range(num_rows)],
            "lineItem/BlendedCost": [4.5 + i for i in range(num_rows)],
            "lineItem/UsageAmount": [50.0] * num_rows,
            "lineItem/CurrencyCode": ["USD"] * num_rows,
        }
    )
    parquet_path = path / "test_cur_v1.parquet"
    pq.write_table(table, parquet_path)
    return parquet_path


def test_detect_cur_version_v1():
    assert detect_cur_version(["identity/LineItemId", "lineItem/UnblendedCost"]) == "v1"


def test_detect_cur_version_v2():
    assert detect_cur_version(["identity_line_item_id", "line_item_unblended_cost"]) == "v2"


def test_build_select_clause_v2():
    columns = ["identity_line_item_id", "line_item_unblended_cost", "line_item_usage_account_id"]
    clause = build_select_clause(columns)
    assert "identity_line_item_id" in clause
    assert "line_item_unblended_cost" in clause
    # _source_file is now a parameterized placeholder
    assert "? AS _source_file" in clause


def test_load_parquet_v2():
    conn = duckdb.connect(":memory:")
    create_tables(conn)

    with tempfile.TemporaryDirectory() as tmpdir:
        parquet_path = _make_v2_parquet(Path(tmpdir))
        rows = load_parquet_file(conn, parquet_path, source_file="s3://bucket/test.parquet")

    assert rows == 5
    result = conn.execute("SELECT COUNT(*) FROM cost_line_items").fetchone()
    assert result[0] == 5

    # Check column mapping worked
    row = conn.execute(
        "SELECT line_item_id, product_code, unblended_cost FROM cost_line_items LIMIT 1"
    ).fetchone()
    assert row[0] == "id-0"
    assert row[1] == "AmazonEC2"
    assert row[2] == 10.0


def test_load_parquet_v1():
    conn = duckdb.connect(":memory:")
    create_tables(conn)

    with tempfile.TemporaryDirectory() as tmpdir:
        parquet_path = _make_v1_parquet(Path(tmpdir))
        rows = load_parquet_file(conn, parquet_path)

    assert rows == 3
    row = conn.execute(
        "SELECT product_code, region, unblended_cost FROM cost_line_items LIMIT 1"
    ).fetchone()
    assert row[0] == "AmazonS3"
    assert row[1] == "us-west-2"
    assert row[2] == 5.0


def test_delete_billing_period_data(db_with_data):
    initial = db_with_data.execute("SELECT COUNT(*) FROM cost_line_items").fetchone()[0]
    assert initial > 0

    # Record some ingestion data
    record_ingestion(db_with_data, "asm-1", "20250101-20250201", "key1", 100)

    delete_billing_period_data(db_with_data, "20250101-20250201")

    remaining = db_with_data.execute("SELECT COUNT(*) FROM cost_line_items").fetchone()[0]
    assert remaining == 0

    log_remaining = db_with_data.execute("SELECT COUNT(*) FROM ingestion_log").fetchone()[0]
    assert log_remaining == 0


def test_ingestion_log_tracking(db):
    record_ingestion(db, "asm-1", "20250101-20250201", "key1.parquet", 100)
    record_ingestion(db, "asm-1", "20250101-20250201", "key2.parquet", 200)

    assemblies = get_ingested_assemblies(db)
    assert assemblies["20250101-20250201"] == "asm-1"
