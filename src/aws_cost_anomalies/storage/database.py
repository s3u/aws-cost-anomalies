"""DuckDB connection management."""

from __future__ import annotations

from pathlib import Path

import duckdb


def get_connection(db_path: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection, creating parent directories if needed."""
    if db_path != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(db_path)
