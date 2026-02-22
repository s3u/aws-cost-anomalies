"""Shared fixtures: in-memory DuckDB, synthetic cost data."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import duckdb
import pytest

from aws_cost_anomalies.storage.database import get_connection
from aws_cost_anomalies.storage.schema import create_tables


@pytest.fixture
def db() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with schema created."""
    conn = get_connection(":memory:")
    create_tables(conn)
    return conn


@pytest.fixture
def db_with_data(db: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    """DuckDB with 14 days of synthetic cost data across 2 accounts and 3 services."""
    base_date = date(2025, 1, 1)
    accounts = ["111111111111", "222222222222"]
    services = [
        ("AmazonEC2", "Amazon Elastic Compute Cloud"),
        ("AmazonS3", "Amazon Simple Storage Service"),
        ("AmazonRDS", "Amazon Relational Database Service"),
    ]
    regions = ["us-east-1", "us-west-2"]

    rows = []
    row_id = 0
    for day_offset in range(14):
        usage_date = base_date + timedelta(days=day_offset)
        for acct in accounts:
            for svc_code, svc_name in services:
                for region in regions:
                    row_id += 1
                    # Base cost with some variance per service
                    base_cost = {"AmazonEC2": 100.0, "AmazonS3": 20.0, "AmazonRDS": 50.0}[
                        svc_code
                    ]
                    # Add small daily variation
                    cost = base_cost + (day_offset % 3) * 2.0
                    rows.append(
                        (
                            f"line-{row_id}",
                            datetime.combine(usage_date, datetime.min.time()),
                            datetime.combine(
                                usage_date + timedelta(days=1), datetime.min.time()
                            ),
                            date(2025, 1, 1),
                            date(2025, 2, 1),
                            "999999999999",
                            acct,
                            svc_code,
                            svc_name,
                            region,
                            f"{region}a",
                            f"{svc_code}-usage",
                            "RunInstances",
                            f"arn:aws:{svc_code.lower()}:{region}:{acct}:resource-{row_id}",
                            "Usage",
                            cost,
                            cost * 0.95,
                            cost * 0.9,
                            cost * 10,
                            "USD",
                            f"{svc_name} usage",
                            datetime.now(),
                            "test-file.parquet",
                        )
                    )

    db.executemany(
        """INSERT INTO cost_line_items VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )""",
        rows,
    )
    return db
