"""Tests for CLI app commands."""

from __future__ import annotations

from typer.testing import CliRunner

# Import commands to register them
from aws_cost_anomalies.cli import anomalies as _anomalies  # noqa: F401
from aws_cost_anomalies.cli import ingest as _ingest  # noqa: F401
from aws_cost_anomalies.cli import query as _query  # noqa: F401
from aws_cost_anomalies.cli import trends as _trends  # noqa: F401
from aws_cost_anomalies.cli.app import app

runner = CliRunner()


def test_app_help():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "Detect AWS cost anomalies" in result.output


def test_ingest_help():
    result = runner.invoke(app, ["ingest", "--help"])
    assert result.exit_code == 0
    assert "--config" in result.output
    assert "--full-refresh" in result.output


def test_trends_help():
    result = runner.invoke(app, ["trends", "--help"])
    assert result.exit_code == 0
    assert "--days" in result.output
    assert "--group-by" in result.output


def test_anomalies_help():
    result = runner.invoke(app, ["anomalies", "--help"])
    assert result.exit_code == 0
    assert "--sensitivity" in result.output


def test_query_help():
    result = runner.invoke(app, ["query", "--help"])
    assert result.exit_code == 0
    assert "--interactive" in result.output
