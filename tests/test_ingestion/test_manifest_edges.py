"""Tests for manifest parsing edge cases."""

from __future__ import annotations

import json

import pytest

from aws_cost_anomalies.ingestion.manifest import (
    parse_manifest,
)


def test_malformed_json():
    with pytest.raises(ValueError, match="Invalid CUR manifest"):
        parse_manifest("{not valid json}")


def test_non_dict_json():
    with pytest.raises(ValueError, match="JSON object"):
        parse_manifest('"just a string"')


def test_missing_assembly_id():
    data = {"reportKeys": ["file.parquet"]}
    with pytest.raises(ValueError, match="assemblyId"):
        parse_manifest(json.dumps(data))


def test_empty_report_keys():
    data = {"assemblyId": "asm-1", "reportKeys": []}
    with pytest.raises(ValueError, match="no reportKeys"):
        parse_manifest(json.dumps(data))


def test_missing_report_keys():
    data = {"assemblyId": "asm-1"}
    with pytest.raises(ValueError, match="no reportKeys"):
        parse_manifest(json.dumps(data))


def test_valid_minimal():
    data = {
        "assemblyId": "asm-1",
        "reportKeys": ["file.parquet"],
        "billingPeriod": {
            "start": "20250101T000000.000Z",
            "end": "20250201T000000.000Z",
        },
        "compression": "Parquet",
    }
    m = parse_manifest(json.dumps(data))
    assert m.assembly_id == "asm-1"
    assert m.is_parquet
    assert m.billing_period == "20250101-20250201"


def test_non_parquet_compression():
    data = {
        "assemblyId": "asm-1",
        "reportKeys": ["file.csv.gz"],
        "compression": "GZIP",
    }
    m = parse_manifest(json.dumps(data))
    assert not m.is_parquet
