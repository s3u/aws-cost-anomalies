"""Safe SQL execution — validate and run queries against DuckDB."""

from __future__ import annotations

import re

import duckdb

# Forbidden SQL patterns — block anything that modifies data or schema
FORBIDDEN_PATTERNS = [
    r"\bINSERT\b",
    r"\bUPDATE\b",
    r"\bDELETE\b",
    r"\bDROP\b",
    r"\bCREATE\b",
    r"\bALTER\b",
    r"\bTRUNCATE\b",
    r"\bREPLACE\b",
    r"\bMERGE\b",
    r"\bGRANT\b",
    r"\bREVOKE\b",
    r"\bEXEC\b",
    r"\bEXECUTE\b",
    r"\bCALL\b",
    r"\bCOPY\b",
    r"\bATTACH\b",
    r"\bDETACH\b",
    r"\bPRAGMA\b",
    r"\bSET\b",
    r"\bLOAD\b",
    r"\bINSTALL\b",
]


class UnsafeSQLError(Exception):
    """Raised when a SQL query contains forbidden operations."""

    pass


def validate_sql(sql: str) -> str:
    """Validate that a SQL query is safe to execute (read-only).

    Returns the cleaned SQL string.
    Raises UnsafeSQLError if the query contains forbidden patterns.
    """
    cleaned = sql.strip().rstrip(";")

    # Block multi-statement queries (embedded semicolons)
    if ";" in cleaned:
        raise UnsafeSQLError("Multi-statement queries are not allowed.")

    # Strip leading SQL comments before checking the first keyword.
    # Models sometimes prefix queries with "-- label" comments.
    prefix = cleaned
    while prefix:
        if prefix.startswith("--"):
            # Line comment — skip to next line
            newline = prefix.find("\n")
            prefix = prefix[newline + 1:].lstrip() if newline != -1 else ""
        elif prefix.startswith("/*"):
            # Block comment — skip to closing */
            end = prefix.find("*/")
            prefix = prefix[end + 2:].lstrip() if end != -1 else ""
        else:
            break

    # Must start with SELECT or WITH (CTEs)
    upper = prefix.upper()
    if not (upper.startswith("SELECT") or upper.startswith("WITH")):
        raise UnsafeSQLError(
            "Only SELECT and WITH (CTE) queries are allowed. "
            f"Query starts with: {upper[:20]}"
        )

    # Check for forbidden patterns
    for pattern in FORBIDDEN_PATTERNS:
        if re.search(pattern, cleaned, re.IGNORECASE):
            keyword = pattern.replace(r"\b", "").strip()
            raise UnsafeSQLError(f"Forbidden SQL keyword detected: {keyword}")

    return cleaned


def execute_query(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
) -> tuple[list[str], list[tuple]]:
    """Validate and execute a SQL query.

    Returns (column_names, rows).
    Raises UnsafeSQLError if the query is not safe.
    """
    safe_sql = validate_sql(sql)
    result = conn.execute(safe_sql)
    columns = [desc[0] for desc in result.description]
    rows = result.fetchall()
    return columns, rows
