"""Tests for DuckDB connection management."""

from __future__ import annotations

import tempfile
from pathlib import Path

from aws_cost_anomalies.storage.database import get_connection


def test_in_memory_connection():
    conn = get_connection(":memory:")
    result = conn.execute("SELECT 1").fetchone()
    assert result == (1,)


def test_file_connection_creates_dirs():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "sub" / "dir" / "test.duckdb")
        conn = get_connection(db_path)
        result = conn.execute("SELECT 1").fetchone()
        assert result == (1,)
        assert Path(db_path).exists()
