"""Parse CUR manifest.json files."""

from __future__ import annotations

import json
from dataclasses import dataclass


@dataclass
class CURManifest:
    assembly_id: str
    account: str
    report_name: str
    billing_period_start: str
    billing_period_end: str
    report_keys: list[str]
    compression: str
    bucket: str
    columns: list[dict]

    @property
    def billing_period(self) -> str:
        """Return billing period as YYYYMMDD-YYYYMMDD."""
        start = self.billing_period_start.split("T")[0]
        end = self.billing_period_end.split("T")[0]
        return f"{start.replace('-', '')}-{end.replace('-', '')}"

    @property
    def is_parquet(self) -> bool:
        return self.compression.lower() == "parquet"


def parse_manifest(content: str | bytes) -> CURManifest:
    """Parse a CUR manifest.json into a CURManifest.

    Raises ValueError if the manifest is malformed or missing
    required fields.
    """
    if isinstance(content, bytes):
        content = content.decode("utf-8")

    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Invalid CUR manifest JSON: {e}"
        ) from e

    if not isinstance(data, dict):
        raise ValueError(
            "CUR manifest must be a JSON object"
        )

    if "assemblyId" not in data:
        raise ValueError(
            "CUR manifest missing required field 'assemblyId'"
        )

    billing = data.get("billingPeriod", {})

    report_keys = data.get("reportKeys", [])
    if not report_keys:
        raise ValueError(
            "CUR manifest has no reportKeys — nothing to ingest"
        )

    return CURManifest(
        assembly_id=data["assemblyId"],
        account=data.get("account", ""),
        report_name=data.get("reportName", ""),
        billing_period_start=billing.get("start", ""),
        billing_period_end=billing.get("end", ""),
        report_keys=report_keys,
        compression=data.get("compression", ""),
        bucket=data.get("bucket", ""),
        columns=data.get("columns", []),
    )
