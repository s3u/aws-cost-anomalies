#!/usr/bin/env python3
"""Generate realistic sample CUR v2 Parquet files for testing.

Creates ~90 days of AWS cost data across multiple accounts and services,
with a deliberate cost anomaly in the final 2 days for anomaly detection.

Usage:
    python scripts/generate_sample_data.py [--output-dir ./data/sample]

The script then loads the generated data into DuckDB and rebuilds
the daily summary table.
"""

from __future__ import annotations

import argparse
import hashlib
import random
import sys
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PAYER_ACCOUNT = "123456789012"

LINKED_ACCOUNTS = {
    "111111111111": "Production",
    "222222222222": "Staging",
    "333333333333": "Development",
    "444444444444": "Data-Analytics",
    "555555555555": "Security",
}

# Service configs: (product_code, product_name, regions, typical daily cost range per account)
SERVICES = [
    ("AmazonEC2", "Amazon Elastic Compute Cloud", ["us-east-1", "us-west-2", "eu-west-1"], (800, 3500)),
    ("AmazonRDS", "Amazon Relational Database Service", ["us-east-1", "us-west-2"], (400, 1200)),
    ("AmazonS3", "Amazon Simple Storage Service", ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"], (100, 600)),
    ("AWSLambda", "AWS Lambda", ["us-east-1", "us-west-2", "eu-west-1"], (50, 300)),
    ("AmazonECS", "Amazon Elastic Container Service", ["us-east-1", "us-west-2"], (200, 800)),
    ("AmazonCloudFront", "Amazon CloudFront", ["us-east-1"], (150, 500)),
    ("AmazonDynamoDB", "Amazon DynamoDB", ["us-east-1", "us-west-2"], (100, 400)),
    ("AmazonElastiCache", "Amazon ElastiCache", ["us-east-1"], (200, 600)),
    ("AmazonKinesis", "Amazon Kinesis", ["us-east-1", "us-west-2"], (50, 200)),
    ("AWSCloudTrail", "AWS CloudTrail", ["us-east-1"], (10, 50)),
]

# Usage type templates per service
USAGE_TYPES = {
    "AmazonEC2": [
        "{region}:BoxUsage:m5.2xlarge",
        "{region}:BoxUsage:c5.4xlarge",
        "{region}:BoxUsage:r5.xlarge",
        "{region}:EBS:VolumeUsage.gp3",
        "{region}:NatGateway-Hours",
        "{region}:DataTransfer-Out-Bytes",
    ],
    "AmazonRDS": [
        "{region}:InstanceUsage:db.r5.xlarge",
        "{region}:InstanceUsage:db.m5.large",
        "{region}:RDS:GP2-Storage",
        "{region}:RDS:BackupUsage",
    ],
    "AmazonS3": [
        "{region}:TimedStorage-ByteHrs",
        "{region}:Requests-Tier1",
        "{region}:Requests-Tier2",
        "{region}:DataTransfer-Out-Bytes",
    ],
    "AWSLambda": [
        "{region}:Lambda-GB-Second",
        "{region}:Request",
        "{region}:Lambda-Provisioned-GB-Second",
    ],
    "AmazonECS": [
        "{region}:Fargate-vCPU-Hours:perCPU",
        "{region}:Fargate-GB-Hours",
    ],
    "AmazonCloudFront": [
        "US:DataTransfer-Out-Bytes",
        "EU:DataTransfer-Out-Bytes",
        "US:Requests-Tier1",
    ],
    "AmazonDynamoDB": [
        "{region}:PayPerRequest-ReadCapacityUnit",
        "{region}:PayPerRequest-WriteCapacityUnit",
        "{region}:TimedStorage-ByteHrs",
    ],
    "AmazonElastiCache": [
        "{region}:NodeUsage:cache.r6g.large",
        "{region}:NodeUsage:cache.m5.xlarge",
    ],
    "AmazonKinesis": [
        "{region}:shardHour",
        "{region}:PutRecord-Bytes",
    ],
    "AWSCloudTrail": [
        "{region}:EventsRecorded",
    ],
}

OPERATIONS = {
    "AmazonEC2": ["RunInstances", "CreateVolume", "NatGateway"],
    "AmazonRDS": ["CreateDBInstance", "CreateDBSnapshot"],
    "AmazonS3": ["PutObject", "GetObject", "ListBucket"],
    "AWSLambda": ["Invoke", "GetFunction"],
    "AmazonECS": ["RunTask", "FargateUsage"],
    "AmazonCloudFront": ["GET", "POST"],
    "AmazonDynamoDB": ["GetItem", "PutItem", "Query"],
    "AmazonElastiCache": ["CreateCacheCluster"],
    "AmazonKinesis": ["PutRecord", "GetRecords"],
    "AWSCloudTrail": ["LookupEvents"],
}

AZ_SUFFIXES = ["a", "b", "c"]


def _make_resource_id(service: str, region: str, account: str) -> str:
    """Generate a realistic-looking resource ARN."""
    stub = hashlib.md5(f"{service}{region}{account}{random.random()}".encode()).hexdigest()[:12]
    resource_map = {
        "AmazonEC2": f"arn:aws:ec2:{region}:{account}:instance/i-{stub}",
        "AmazonRDS": f"arn:aws:rds:{region}:{account}:db:prod-db-{stub[:8]}",
        "AmazonS3": f"arn:aws:s3:::bucket-{stub}",
        "AWSLambda": f"arn:aws:lambda:{region}:{account}:function:fn-{stub[:8]}",
        "AmazonECS": f"arn:aws:ecs:{region}:{account}:task/cluster/{stub}",
        "AmazonCloudFront": f"arn:aws:cloudfront::{account}:distribution/E{stub[:13].upper()}",
        "AmazonDynamoDB": f"arn:aws:dynamodb:{region}:{account}:table/tbl-{stub[:8]}",
        "AmazonElastiCache": f"arn:aws:elasticache:{region}:{account}:cluster:cache-{stub[:8]}",
        "AmazonKinesis": f"arn:aws:kinesis:{region}:{account}:stream/stream-{stub[:8]}",
        "AWSCloudTrail": f"arn:aws:cloudtrail:{region}:{account}:trail/mgmt",
    }
    return resource_map.get(service, f"arn:aws:{service.lower()}:{region}:{account}:{stub}")


def generate_cur_data(
    start_date: date,
    end_date: date,
    anomaly_start: date | None = None,
    anomaly_account: str = "111111111111",
    anomaly_service: str = "AmazonEC2",
    anomaly_multiplier: float = 4.0,
    seed: int = 42,
) -> list[dict]:
    """Generate CUR v2 line items.

    Args:
        start_date: First day of data.
        end_date: Last day of data (inclusive).
        anomaly_start: Date from which to inject cost anomaly.
        anomaly_account: Account to spike.
        anomaly_service: Service to spike.
        anomaly_multiplier: How much to multiply costs.
        seed: Random seed for reproducibility.

    Returns:
        List of dicts, each a CUR v2 row.
    """
    rng = random.Random(seed)
    np_rng = np.random.default_rng(seed)

    rows: list[dict] = []
    current = start_date

    # Pre-generate stable resource IDs per (account, service, region)
    resource_cache: dict[tuple, list[str]] = {}

    while current <= end_date:
        billing_period_start = current.replace(day=1)
        if billing_period_start.month == 12:
            billing_period_end = billing_period_start.replace(year=billing_period_start.year + 1, month=1)
        else:
            billing_period_end = billing_period_start.replace(month=billing_period_start.month + 1)

        for account_id, account_name in LINKED_ACCOUNTS.items():
            # Each account has a cost profile multiplier
            account_scale = {
                "111111111111": 1.5,  # Production spends more
                "222222222222": 0.8,
                "333333333333": 0.5,
                "444444444444": 1.2,
                "555555555555": 0.3,
            }.get(account_id, 1.0)

            for svc_code, svc_name, regions, (cost_low, cost_high) in SERVICES:
                for region in regions:
                    # Daily cost for this (account, service, region) combo
                    base_cost = np_rng.uniform(cost_low, cost_high) * account_scale

                    # Add day-of-week variation (weekends 20% lower)
                    if current.weekday() >= 5:
                        base_cost *= 0.8

                    # Add slight daily noise
                    base_cost *= np_rng.normal(1.0, 0.05)
                    base_cost = max(base_cost, 0.01)

                    # Apply anomaly multiplier
                    is_anomaly = (
                        anomaly_start
                        and current >= anomaly_start
                        and account_id == anomaly_account
                        and svc_code == anomaly_service
                    )
                    if is_anomaly:
                        base_cost *= anomaly_multiplier

                    # Generate 3-8 line items per (account, service, region, day)
                    usage_types = USAGE_TYPES.get(svc_code, ["{region}:Usage"])
                    operations = OPERATIONS.get(svc_code, ["Usage"])

                    n_items = rng.randint(3, 8)
                    # Split the daily cost across line items
                    splits = np_rng.dirichlet(np.ones(n_items))

                    cache_key = (account_id, svc_code, region)
                    if cache_key not in resource_cache:
                        resource_cache[cache_key] = [
                            _make_resource_id(svc_code, region, account_id)
                            for _ in range(rng.randint(2, 5))
                        ]

                    for i in range(n_items):
                        item_cost = round(base_cost * splits[i], 10)
                        usage_amount = round(abs(np_rng.normal(100, 50)) * splits[i], 6)
                        blended_cost = round(item_cost * np_rng.uniform(0.92, 0.98), 10)

                        usage_type_template = rng.choice(usage_types)
                        usage_type = usage_type_template.format(region=region)

                        hour = rng.randint(0, 23)
                        start_dt = datetime(current.year, current.month, current.day, hour, 0, 0)
                        end_dt = start_dt + timedelta(hours=1)

                        az = f"{region}{rng.choice(AZ_SUFFIXES)}" if region != "us-east-1" or rng.random() > 0.3 else ""

                        row = {
                            "identity_line_item_id": str(uuid.uuid4()),
                            "line_item_usage_start_date": start_dt.isoformat() + "Z",
                            "line_item_usage_end_date": end_dt.isoformat() + "Z",
                            "bill_billing_period_start_date": billing_period_start.isoformat() + "T00:00:00Z",
                            "bill_billing_period_end_date": billing_period_end.isoformat() + "T00:00:00Z",
                            "bill_payer_account_id": PAYER_ACCOUNT,
                            "line_item_usage_account_id": account_id,
                            "product_product_code": svc_code,
                            "product_product_name": svc_name,
                            "product_region": region,
                            "line_item_availability_zone": az,
                            "line_item_usage_type": usage_type,
                            "line_item_operation": rng.choice(operations),
                            "line_item_resource_id": rng.choice(resource_cache[cache_key]),
                            "line_item_line_item_type": "Usage",
                            "line_item_unblended_cost": item_cost,
                            "line_item_blended_cost": blended_cost,
                            "line_item_net_unblended_cost": round(item_cost * 0.95, 10),
                            "line_item_usage_amount": usage_amount,
                            "line_item_currency_code": "USD",
                            "line_item_line_item_description": (
                                f"{svc_name} {usage_type} in {region}"
                            ),
                        }
                        rows.append(row)

        current += timedelta(days=1)

    return rows


def write_parquet_files(
    rows: list[dict],
    output_dir: Path,
    rows_per_file: int = 200_000,
) -> list[Path]:
    """Write CUR rows as snappy-compressed Parquet files.

    Splits into multiple files if needed.
    Returns list of written file paths.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Build PyArrow schema (all strings except cost/usage which are doubles)
    double_cols = {
        "line_item_unblended_cost",
        "line_item_blended_cost",
        "line_item_net_unblended_cost",
        "line_item_usage_amount",
    }

    if not rows:
        print("No rows generated!")
        return []

    col_names = list(rows[0].keys())
    fields = []
    for col in col_names:
        if col in double_cols:
            fields.append(pa.field(col, pa.float64()))
        else:
            fields.append(pa.field(col, pa.string()))

    schema = pa.schema(fields)

    files = []
    for chunk_idx in range(0, len(rows), rows_per_file):
        chunk = rows[chunk_idx : chunk_idx + rows_per_file]

        arrays = {}
        for col in col_names:
            values = [row[col] for row in chunk]
            if col in double_cols:
                arrays[col] = pa.array(values, type=pa.float64())
            else:
                arrays[col] = pa.array(values, type=pa.string())

        table = pa.table(arrays, schema=schema)
        file_num = chunk_idx // rows_per_file + 1
        filename = f"sample-cur-{file_num:05d}.snappy.parquet"
        filepath = output_dir / filename
        pq.write_table(table, filepath, compression="snappy")
        files.append(filepath)
        print(f"  Wrote {filepath.name} ({len(chunk):,} rows)")

    return files


def load_into_duckdb(
    parquet_files: list[Path],
    db_path: str,
) -> None:
    """Load parquet files into DuckDB and rebuild daily summary."""
    # Import here to allow script to show --help without project installed
    from aws_cost_anomalies.ingestion.loader import load_parquet_file
    from aws_cost_anomalies.storage.database import get_connection
    from aws_cost_anomalies.storage.schema import (
        create_tables,
        rebuild_daily_summary,
    )

    print(f"\nLoading into DuckDB at {db_path}...")
    conn = get_connection(db_path)
    create_tables(conn)

    # Clear existing data for clean load
    conn.execute("DELETE FROM cost_line_items")
    conn.execute("DELETE FROM daily_cost_summary")
    conn.execute("DELETE FROM ingestion_log")

    total_rows = 0
    for filepath in parquet_files:
        n = load_parquet_file(conn, filepath, source_file=filepath.name)
        total_rows += n
        print(f"  Loaded {filepath.name}: {n:,} rows")

    print(f"\nTotal line items loaded: {total_rows:,}")

    print("Rebuilding daily summary...")
    summary_rows = rebuild_daily_summary(conn)
    print(f"Daily summary rows: {summary_rows:,}")

    # Quick stats
    result = conn.execute("""
        SELECT
            MIN(usage_date) AS first_date,
            MAX(usage_date) AS last_date,
            COUNT(DISTINCT usage_account_id) AS accounts,
            COUNT(DISTINCT product_code) AS services,
            ROUND(SUM(total_unblended_cost), 2) AS total_cost
        FROM daily_cost_summary
    """).fetchone()
    print(f"\nData summary:")
    print(f"  Date range: {result[0]} to {result[1]}")
    print(f"  Accounts:   {result[2]}")
    print(f"  Services:   {result[3]}")
    print(f"  Total cost: ${result[4]:,.2f}")

    # Show daily cost range
    daily = conn.execute("""
        SELECT
            ROUND(MIN(daily_total), 2),
            ROUND(AVG(daily_total), 2),
            ROUND(MAX(daily_total), 2)
        FROM (
            SELECT usage_date, SUM(total_unblended_cost) AS daily_total
            FROM daily_cost_summary
            GROUP BY usage_date
        )
    """).fetchone()
    print(f"  Daily cost: min=${daily[0]:,.2f}  avg=${daily[1]:,.2f}  max=${daily[2]:,.2f}")

    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Generate sample CUR v2 Parquet data and load into DuckDB."
    )
    parser.add_argument(
        "--output-dir",
        default="./data/sample",
        help="Directory for generated Parquet files (default: ./data/sample)",
    )
    parser.add_argument(
        "--db-path",
        default="./data/costs.duckdb",
        help="DuckDB database path (default: ./data/costs.duckdb)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Number of days of data to generate (default: 90)",
    )
    parser.add_argument(
        "--anomaly-days",
        type=int,
        default=2,
        help="Number of recent days with anomaly (default: 2)",
    )
    parser.add_argument(
        "--anomaly-multiplier",
        type=float,
        default=4.0,
        help="Cost multiplier for anomaly (default: 4.0x)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed (default: 42)",
    )
    parser.add_argument(
        "--no-load",
        action="store_true",
        help="Generate Parquet files only, don't load into DuckDB",
    )

    args = parser.parse_args()

    end_date = date.today() - timedelta(days=1)  # yesterday
    start_date = end_date - timedelta(days=args.days - 1)
    anomaly_start = end_date - timedelta(days=args.anomaly_days - 1)

    print(f"Generating {args.days} days of CUR data...")
    print(f"  Date range: {start_date} to {end_date}")
    print(f"  Accounts:   {len(LINKED_ACCOUNTS)} linked + 1 payer")
    print(f"  Services:   {len(SERVICES)}")
    print(f"  Anomaly:    {args.anomaly_multiplier}x EC2 spike in account 111111111111")
    print(f"              starting {anomaly_start}")
    print()

    rows = generate_cur_data(
        start_date=start_date,
        end_date=end_date,
        anomaly_start=anomaly_start,
        anomaly_multiplier=args.anomaly_multiplier,
        seed=args.seed,
    )
    print(f"Generated {len(rows):,} line items")

    output_dir = Path(args.output_dir)
    print(f"\nWriting Parquet files to {output_dir}/...")
    files = write_parquet_files(rows, output_dir)

    if not args.no_load:
        load_into_duckdb(files, args.db_path)
    else:
        print("\nSkipping DuckDB load (--no-load).")
        print(f"Files ready at: {output_dir}")

    print("\nDone!")


if __name__ == "__main__":
    main()
