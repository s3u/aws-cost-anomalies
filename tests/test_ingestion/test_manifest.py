"""Tests for CUR manifest parsing."""

from __future__ import annotations

import json
from pathlib import Path

from aws_cost_anomalies.ingestion.manifest import parse_manifest

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


def test_parse_sample_manifest():
    content = (FIXTURES_DIR / "sample_manifest.json").read_text()
    manifest = parse_manifest(content)

    assert manifest.assembly_id == "20250101-abcdef12"
    assert manifest.account == "999999999999"
    assert manifest.report_name == "my-cur-report"
    assert len(manifest.report_keys) == 2
    assert manifest.is_parquet
    assert manifest.billing_period == "20250101-20250201"


def test_parse_manifest_from_bytes():
    content = (FIXTURES_DIR / "sample_manifest.json").read_bytes()
    manifest = parse_manifest(content)
    assert manifest.assembly_id == "20250101-abcdef12"


def test_parse_manifest_minimal():
    data = {
        "assemblyId": "test-assembly",
        "billingPeriod": {
            "start": "20250301T000000.000Z",
            "end": "20250401T000000.000Z",
        },
        "reportKeys": ["file1.parquet"],
        "compression": "Parquet",
    }
    manifest = parse_manifest(json.dumps(data))
    assert manifest.assembly_id == "test-assembly"
    assert manifest.billing_period == "20250301-20250401"
    assert manifest.is_parquet
