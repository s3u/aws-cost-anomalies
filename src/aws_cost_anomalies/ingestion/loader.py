"""Load parquet/CSV CUR files into DuckDB with column mapping."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

# CUR v1 (legacy) -> normalized column mapping
# CUR v1 uses slash-separated names like "lineItem/UnblendedCost"
CUR_V1_COLUMN_MAP = {
    "identity/LineItemId": "line_item_id",
    "lineItem/UsageStartDate": "usage_start_date",
    "lineItem/UsageEndDate": "usage_end_date",
    "bill/BillingPeriodStartDate": "billing_period_start",
    "bill/BillingPeriodEndDate": "billing_period_end",
    "bill/PayerAccountId": "payer_account_id",
    "lineItem/UsageAccountId": "usage_account_id",
    "product/ProductCode": "product_code",
    "product/ProductName": "product_name",
    "product/region": "region",
    "lineItem/AvailabilityZone": "availability_zone",
    "lineItem/UsageType": "usage_type",
    "lineItem/Operation": "operation",
    "lineItem/ResourceId": "resource_id",
    "lineItem/LineItemType": "line_item_type",
    "lineItem/UnblendedCost": "unblended_cost",
    "lineItem/BlendedCost": "blended_cost",
    "lineItem/NetUnblendedCost": "net_unblended_cost",
    "lineItem/UsageAmount": "usage_amount",
    "lineItem/CurrencyCode": "currency_code",
    "lineItem/LineItemDescription": "line_item_description",
}

# CUR v2 uses underscore-separated names
CUR_V2_COLUMN_MAP = {
    "identity_line_item_id": "line_item_id",
    "line_item_usage_start_date": "usage_start_date",
    "line_item_usage_end_date": "usage_end_date",
    "bill_billing_period_start_date": "billing_period_start",
    "bill_billing_period_end_date": "billing_period_end",
    "bill_payer_account_id": "payer_account_id",
    "line_item_usage_account_id": "usage_account_id",
    "product_product_code": "product_code",
    "product_product_name": "product_name",
    "product_region": "region",
    "line_item_availability_zone": "availability_zone",
    "line_item_usage_type": "usage_type",
    "line_item_operation": "operation",
    "line_item_resource_id": "resource_id",
    "line_item_line_item_type": "line_item_type",
    "line_item_unblended_cost": "unblended_cost",
    "line_item_blended_cost": "blended_cost",
    "line_item_net_unblended_cost": "net_unblended_cost",
    "line_item_usage_amount": "usage_amount",
    "line_item_currency_code": "currency_code",
    "line_item_line_item_description": "line_item_description",
}

# Columns that must be present for meaningful data
_REQUIRED_TARGETS = {
    "usage_start_date",
    "line_item_type",
    "unblended_cost",
}


def detect_cur_version(parquet_columns: list[str]) -> str:
    """Detect CUR version from parquet column names.

    Returns 'v1' or 'v2'.
    """
    if any("/" in col for col in parquet_columns):
        return "v1"
    return "v2"


def _validate_mapped_columns(
    parquet_columns: list[str],
    col_map: dict[str, str],
) -> list[str]:
    """Check that critical columns can be mapped.

    Returns list of warnings for missing optional columns.
    """
    parquet_col_set = set(parquet_columns)
    reverse = {v: k for k, v in col_map.items()}
    warnings = []

    for target in _REQUIRED_TARGETS:
        source = reverse.get(target)
        if not source or source not in parquet_col_set:
            raise ValueError(
                f"Required column '{target}' not found "
                f"in parquet file. Cannot load this CUR file."
            )

    # Warn about missing optional columns
    all_targets = set(col_map.values())
    for target in all_targets - _REQUIRED_TARGETS:
        source = reverse.get(target)
        if not source or source not in parquet_col_set:
            warnings.append(target)

    return warnings


def build_select_clause(
    parquet_columns: list[str], source_file: str
) -> str:
    """Build a SELECT clause mapping parquet to normalized names.

    Columns not found in the source are filled with NULL.
    """
    version = detect_cur_version(parquet_columns)
    col_map = (
        CUR_V1_COLUMN_MAP if version == "v1" else CUR_V2_COLUMN_MAP
    )

    parquet_col_set = set(parquet_columns)
    select_parts = []
    target_columns = [
        "line_item_id",
        "usage_start_date",
        "usage_end_date",
        "billing_period_start",
        "billing_period_end",
        "payer_account_id",
        "usage_account_id",
        "product_code",
        "product_name",
        "region",
        "availability_zone",
        "usage_type",
        "operation",
        "resource_id",
        "line_item_type",
        "unblended_cost",
        "blended_cost",
        "net_unblended_cost",
        "usage_amount",
        "currency_code",
        "line_item_description",
    ]

    reverse_map = {v: k for k, v in col_map.items()}

    for target_col in target_columns:
        source_col = reverse_map.get(target_col)
        if source_col and source_col in parquet_col_set:
            select_parts.append(
                f'"{source_col}" AS {target_col}'
            )
        else:
            select_parts.append(f"NULL AS {target_col}")

    select_parts.append("current_timestamp AS _ingested_at")
    escaped_source = source_file.replace("'", "''")
    select_parts.append(f"'{escaped_source}' AS _source_file")

    return ", ".join(select_parts)


def load_parquet_file(
    conn: duckdb.DuckDBPyConnection,
    file_path: str | Path,
    source_file: str | None = None,
) -> int:
    """Load a single parquet file into cost_line_items.

    Validates that required columns exist before loading.
    Returns the number of rows loaded.
    Raises ValueError if the file is missing critical columns.
    """
    file_path = Path(file_path)
    source_file = source_file or file_path.name

    if not file_path.exists():
        raise FileNotFoundError(
            f"Parquet file not found: {file_path}"
        )

    # Read parquet column names (skip schema root row)
    try:
        schema_result = conn.execute(
            "SELECT name FROM parquet_schema(?)"
            " WHERE num_children IS NULL"
            " OR num_children = 0",
            [str(file_path)],
        ).fetchall()
    except Exception as e:
        raise ValueError(
            f"Cannot read parquet schema from "
            f"{file_path.name}: {e}"
        ) from e

    parquet_columns = [row[0] for row in schema_result]

    if not parquet_columns:
        raise ValueError(
            f"No columns found in {file_path.name}. "
            "File may be corrupted."
        )

    # Validate columns
    version = detect_cur_version(parquet_columns)
    col_map = (
        CUR_V1_COLUMN_MAP if version == "v1" else CUR_V2_COLUMN_MAP
    )
    missing = _validate_mapped_columns(
        parquet_columns, col_map
    )
    if missing:
        logger.warning(
            "File %s missing optional columns: %s "
            "(will be NULL)",
            source_file,
            ", ".join(sorted(missing)),
        )

    select_clause = build_select_clause(
        parquet_columns, source_file
    )

    file_str = str(file_path)
    result = conn.execute(
        f"INSERT INTO cost_line_items"
        f" SELECT {select_clause}"
        f" FROM read_parquet(?)",
        [file_str],
    )

    row_count = result.fetchone()
    return row_count[0] if row_count else 0


def delete_billing_period_data(
    conn: duckdb.DuckDBPyConnection, billing_period: str
) -> None:
    """Delete all line items for a billing period."""
    start_str = billing_period[:8]
    start_date = (
        f"{start_str[:4]}-{start_str[4:6]}-{start_str[6:8]}"
    )
    conn.execute(
        "DELETE FROM cost_line_items "
        "WHERE billing_period_start = ?",
        [start_date],
    )
    conn.execute(
        "DELETE FROM ingestion_log WHERE billing_period = ?",
        [billing_period],
    )


def record_ingestion(
    conn: duckdb.DuckDBPyConnection,
    assembly_id: str,
    billing_period: str,
    s3_key: str,
    rows_loaded: int,
) -> None:
    """Record a file ingestion in the ingestion log."""
    conn.execute(
        "INSERT INTO ingestion_log "
        "(assembly_id, billing_period, s3_key, "
        "rows_loaded, ingested_at) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            assembly_id,
            billing_period,
            s3_key,
            rows_loaded,
            datetime.now(timezone.utc),
        ],
    )


def get_ingested_assemblies(
    conn: duckdb.DuckDBPyConnection,
) -> dict[str, str]:
    """Return mapping of billing_period -> assembly_id."""
    rows = conn.execute(
        "SELECT billing_period, "
        "MAX_BY(assembly_id, ingested_at) AS assembly_id "
        "FROM ingestion_log "
        "GROUP BY billing_period"
    ).fetchall()
    return {row[0]: row[1] for row in rows}
