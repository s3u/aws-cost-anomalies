"""Tests for safe SQL execution."""

from __future__ import annotations

import pytest

from aws_cost_anomalies.agent.executor import UnsafeSQLError, execute_query, validate_sql


class TestValidateSQL:
    def test_valid_select(self):
        assert validate_sql("SELECT 1") == "SELECT 1"

    def test_valid_select_with_semicolon(self):
        assert validate_sql("SELECT 1;") == "SELECT 1"

    def test_valid_cte(self):
        sql = "WITH cte AS (SELECT 1 AS x) SELECT * FROM cte"
        assert validate_sql(sql) == sql

    def test_rejects_insert(self):
        with pytest.raises(UnsafeSQLError):
            validate_sql("INSERT INTO cost_line_items VALUES (1)")

    def test_rejects_update(self):
        with pytest.raises(UnsafeSQLError):
            validate_sql("UPDATE cost_line_items SET unblended_cost = 0")

    def test_rejects_delete(self):
        with pytest.raises(UnsafeSQLError):
            validate_sql("DELETE FROM cost_line_items")

    def test_rejects_drop(self):
        with pytest.raises(UnsafeSQLError):
            validate_sql("DROP TABLE cost_line_items")

    def test_rejects_create(self):
        with pytest.raises(UnsafeSQLError):
            validate_sql("CREATE TABLE evil (x INT)")

    def test_rejects_non_select(self):
        with pytest.raises(UnsafeSQLError, match="Only SELECT and WITH"):
            validate_sql("SHOW TABLES")

    def test_rejects_attach(self):
        with pytest.raises(UnsafeSQLError):
            validate_sql("ATTACH '/tmp/evil.db'")

    def test_rejects_copy(self):
        with pytest.raises(UnsafeSQLError):
            validate_sql("COPY cost_line_items TO '/tmp/data.csv'")

    # Test subquery injection: queries that start with SELECT but embed forbidden keywords
    def test_rejects_insert_in_subquery(self):
        with pytest.raises(UnsafeSQLError, match="Forbidden SQL keyword"):
            validate_sql("SELECT 1; INSERT INTO cost_line_items VALUES (1)")

    def test_rejects_drop_in_subquery(self):
        with pytest.raises(UnsafeSQLError, match="Forbidden SQL keyword"):
            validate_sql("SELECT * FROM (DROP TABLE cost_line_items)")

    def test_rejects_delete_embedded(self):
        with pytest.raises(UnsafeSQLError, match="Forbidden SQL keyword"):
            validate_sql("WITH x AS (DELETE FROM cost_line_items RETURNING *) SELECT * FROM x")


class TestExecuteQuery:
    def test_execute_simple_select(self, db):
        columns, rows = execute_query(db, "SELECT 1 AS num, 'hello' AS msg")
        assert columns == ["num", "msg"]
        assert rows == [(1, "hello")]

    def test_execute_against_real_table(self, db_with_data):
        columns, rows = execute_query(
            db_with_data,
            "SELECT COUNT(*) AS cnt FROM cost_line_items",
        )
        assert columns == ["cnt"]
        assert rows[0][0] > 0

    def test_execute_rejects_unsafe(self, db):
        with pytest.raises(UnsafeSQLError):
            execute_query(db, "DROP TABLE cost_line_items")
