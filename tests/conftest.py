"""Shared fixtures: in-memory DuckDB, synthetic cost data."""

from __future__ import annotations

import math
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


def _cost(base: float, day: int, *, noise_amp: float = 2.0) -> float:
    """Deterministic daily cost with small sinusoidal noise."""
    return base + noise_amp * math.sin(day * 0.8)


@pytest.fixture
def db_with_data(db: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    """DuckDB with 31 days of realistic synthetic cost data.

    Covers Jan 1–31 2025, 2 accounts × 3 services × 2 regions.

    Built-in anomaly patterns:
      - EC2 / acct-111 / us-east-1: gradual upward drift ($100 → ~$150)
      - EC2 / acct-222 / us-east-1: steady $100, spike to $400 on day 28
      - RDS / acct-111 / us-east-1: steady $50, drop to $10 on day 30
      - Everything else: stable baselines with small noise
    """
    base_date = date(2025, 1, 1)
    accounts = ["111111111111", "222222222222"]
    services = [
        ("AmazonEC2", "Amazon Elastic Compute Cloud"),
        ("AmazonS3", "Amazon Simple Storage Service"),
        ("AmazonRDS", "Amazon Relational Database Service"),
    ]
    regions = ["us-east-1", "us-west-2"]

    # Base costs per (service, account, region) — steady state
    base_costs = {
        ("AmazonEC2", "111111111111", "us-east-1"): 100.0,
        ("AmazonEC2", "111111111111", "us-west-2"): 80.0,
        ("AmazonEC2", "222222222222", "us-east-1"): 100.0,
        ("AmazonEC2", "222222222222", "us-west-2"): 90.0,
        ("AmazonS3", "111111111111", "us-east-1"): 20.0,
        ("AmazonS3", "111111111111", "us-west-2"): 15.0,
        ("AmazonS3", "222222222222", "us-east-1"): 25.0,
        ("AmazonS3", "222222222222", "us-west-2"): 18.0,
        ("AmazonRDS", "111111111111", "us-east-1"): 50.0,
        ("AmazonRDS", "111111111111", "us-west-2"): 40.0,
        ("AmazonRDS", "222222222222", "us-east-1"): 45.0,
        ("AmazonRDS", "222222222222", "us-west-2"): 35.0,
    }

    rows = []
    row_id = 0
    for day_offset in range(31):
        usage_date = base_date + timedelta(days=day_offset)
        for acct in accounts:
            for svc_code, svc_name in services:
                for region in regions:
                    row_id += 1
                    base = base_costs[(svc_code, acct, region)]
                    cost = _cost(base, day_offset)

                    # --- Anomaly patterns ---

                    # Drift: EC2 / acct-111 / us-east-1 rises ~$1.6/day
                    if (svc_code, acct, region) == (
                        "AmazonEC2", "111111111111", "us-east-1"
                    ):
                        cost = _cost(100.0 + day_offset * 1.6, day_offset)

                    # Spike: EC2 / acct-222 / us-east-1 on day 28
                    if (svc_code, acct, region) == (
                        "AmazonEC2", "222222222222", "us-east-1"
                    ) and day_offset == 27:
                        cost = 400.0

                    # Drop: RDS / acct-111 / us-east-1 on day 30
                    if (svc_code, acct, region) == (
                        "AmazonRDS", "111111111111", "us-east-1"
                    ) and day_offset == 29:
                        cost = 10.0

                    rows.append(
                        (
                            f"line-{row_id}",
                            datetime.combine(
                                usage_date, datetime.min.time()
                            ),
                            datetime.combine(
                                usage_date + timedelta(days=1),
                                datetime.min.time(),
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
