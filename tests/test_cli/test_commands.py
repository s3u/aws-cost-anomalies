"""Integration tests for CLI commands with mock data."""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

# Import commands to register them
from aws_cost_anomalies.cli import anomalies as _a  # noqa: F401
from aws_cost_anomalies.cli import ingest as _i  # noqa: F401
from aws_cost_anomalies.cli import query as _q  # noqa: F401
from aws_cost_anomalies.cli import trends as _t  # noqa: F401
from aws_cost_anomalies.cli.app import app
from aws_cost_anomalies.storage.database import get_connection
from aws_cost_anomalies.storage.schema import create_tables

runner = CliRunner()


def _make_config(tmpdir: str, db_path: str) -> str:
    """Write a minimal config file pointing to temp DB."""
    config_path = str(Path(tmpdir) / "config.yaml")
    with open(config_path, "w") as f:
        f.write(
            f"database:\n"
            f"  path: {db_path}\n"
            f"  cache_dir: {tmpdir}/cache\n"
        )
    return config_path


class TestIngestCommand:
    def test_missing_s3_config(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = _make_config(
                tmpdir, f"{tmpdir}/costs.duckdb"
            )
            result = runner.invoke(
                app, ["ingest", "--config", cfg]
            )
            assert result.exit_code == 1
            assert "bucket" in result.output.lower()

    def test_bad_date_format(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg_path = str(Path(tmpdir) / "config.yaml")
            with open(cfg_path, "w") as f:
                f.write(
                    "s3:\n"
                    "  bucket: test\n"
                    "  report_name: test\n"
                    f"database:\n"
                    f"  path: {tmpdir}/c.duckdb\n"
                )
            result = runner.invoke(
                app,
                ["ingest", "--config", cfg_path, "--date", "bad"],
            )
            assert result.exit_code != 0


class TestTrendsCommand:
    def test_empty_db(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = f"{tmpdir}/costs.duckdb"
            cfg = _make_config(tmpdir, db_path)
            # Initialize empty DB
            conn = get_connection(db_path)
            create_tables(conn)
            conn.close()
            result = runner.invoke(
                app, ["trends", "--config", cfg]
            )
            assert result.exit_code == 1
            assert "ingest" in result.output.lower()

    def test_bad_group_by(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = f"{tmpdir}/costs.duckdb"
            cfg = _make_config(tmpdir, db_path)
            conn = get_connection(db_path)
            create_tables(conn)
            conn.close()
            result = runner.invoke(
                app,
                [
                    "trends",
                    "--config",
                    cfg,
                    "--group-by",
                    "invalid",
                ],
            )
            assert result.exit_code == 1


class TestAnomaliesCommand:
    def test_empty_db(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = f"{tmpdir}/costs.duckdb"
            cfg = _make_config(tmpdir, db_path)
            conn = get_connection(db_path)
            create_tables(conn)
            conn.close()
            result = runner.invoke(
                app, ["anomalies", "--config", cfg]
            )
            assert result.exit_code == 1
            assert "ingest" in result.output.lower()

    def test_bad_sensitivity(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = f"{tmpdir}/costs.duckdb"
            cfg = _make_config(tmpdir, db_path)
            conn = get_connection(db_path)
            create_tables(conn)
            conn.close()
            result = runner.invoke(
                app,
                [
                    "anomalies",
                    "--config",
                    cfg,
                    "--sensitivity",
                    "extreme",
                ],
            )
            assert result.exit_code == 1


class TestQueryCommand:
    def test_empty_db(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = f"{tmpdir}/costs.duckdb"
            cfg = _make_config(tmpdir, db_path)
            conn = get_connection(db_path)
            create_tables(conn)
            conn.close()
            result = runner.invoke(
                app,
                ["query", "--config", cfg, "top services"],
            )
            assert result.exit_code == 1
            assert "ingest" in result.output.lower()

    def test_no_question_no_interactive(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = f"{tmpdir}/costs.duckdb"
            cfg = _make_config(tmpdir, db_path)
            conn = get_connection(db_path)
            create_tables(conn)
            # Insert dummy data so it doesn't exit early
            conn.execute(
                "INSERT INTO daily_cost_summary VALUES "
                "('2025-01-01', '111', 'EC2', 'us-east-1', "
                "100, 95, 10, 1)"
            )
            conn.close()
            result = runner.invoke(
                app, ["query", "--config", cfg]
            )
            assert result.exit_code == 1
            assert "question" in result.output.lower()

    @patch("aws_cost_anomalies.cli.query.run_agent")
    def test_successful_query(self, mock_run_agent):
        from aws_cost_anomalies.agent import AgentResponse

        mock_run_agent.return_value = AgentResponse(
            answer="EC2 costs $1,500.",
            steps=[],
            input_tokens=100,
            output_tokens=50,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = f"{tmpdir}/costs.duckdb"
            cfg = _make_config(tmpdir, db_path)
            conn = get_connection(db_path)
            create_tables(conn)
            conn.execute(
                "INSERT INTO daily_cost_summary VALUES "
                "('2025-01-01', '111', 'EC2', 'us-east-1', "
                "100, 95, 10, 1)"
            )
            conn.close()
            result = runner.invoke(
                app,
                ["query", "--config", cfg, "top services"],
            )
            assert result.exit_code == 0
            assert "EC2 costs $1,500" in result.output
            mock_run_agent.assert_called_once()

    @patch("aws_cost_anomalies.cli.query.run_agent")
    def test_agent_error_displayed(self, mock_run_agent):
        from aws_cost_anomalies.agent import AgentError

        mock_run_agent.side_effect = AgentError(
            "AWS credentials not found."
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = f"{tmpdir}/costs.duckdb"
            cfg = _make_config(tmpdir, db_path)
            conn = get_connection(db_path)
            create_tables(conn)
            conn.execute(
                "INSERT INTO daily_cost_summary VALUES "
                "('2025-01-01', '111', 'EC2', 'us-east-1', "
                "100, 95, 10, 1)"
            )
            conn.close()
            result = runner.invoke(
                app,
                ["query", "--config", cfg, "top services"],
            )
            assert "credentials" in result.output.lower()
