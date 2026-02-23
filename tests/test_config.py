"""Tests for config loading and validation."""

from __future__ import annotations

import tempfile

import pytest

from aws_cost_anomalies.config.settings import (
    ConfigError,
    Settings,
    load_settings,
)


def test_load_nonexistent_config_raises():
    """Explicit config path that doesn't exist raises."""
    with pytest.raises(ConfigError, match="not found"):
        load_settings("/nonexistent/path/config.yaml")


def test_load_valid_yaml():
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as f:
        f.write(
            "s3:\n"
            "  bucket: test-bucket\n"
            "  report_name: my-report\n"
            "anomaly:\n"
            "  rolling_window_days: 7\n"
            "  z_score_threshold: 3.0\n"
        )
        f.flush()
        settings = load_settings(f.name)

    assert settings.s3.bucket == "test-bucket"
    assert settings.s3.report_name == "my-report"
    assert settings.anomaly.rolling_window_days == 7
    assert settings.anomaly.z_score_threshold == 3.0


def test_load_invalid_yaml():
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as f:
        f.write("- just\n- a\n- list\n")
        f.flush()
        with pytest.raises(ConfigError, match="YAML mapping"):
            load_settings(f.name)


def test_bad_int_value():
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as f:
        f.write("anomaly:\n  rolling_window_days: abc\n")
        f.flush()
        with pytest.raises(ConfigError, match="integer"):
            load_settings(f.name)


def test_negative_window():
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as f:
        f.write("anomaly:\n  rolling_window_days: -5\n")
        f.flush()
        with pytest.raises(ConfigError, match=">= 1"):
            load_settings(f.name)


def test_bad_float_value():
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as f:
        f.write("anomaly:\n  z_score_threshold: not_a_number\n")
        f.flush()
        with pytest.raises(ConfigError, match="number"):
            load_settings(f.name)


def test_env_var_override(monkeypatch):
    monkeypatch.setenv("AWS_COST_DB_PATH", "/tmp/override.db")
    settings = load_settings(None)
    assert settings.database.path == "/tmp/override.db"


def test_default_settings():
    settings = Settings()
    assert settings.s3.region == "us-east-1"
    assert settings.database.path == "./data/costs.duckdb"
    assert settings.anomaly.rolling_window_days == 14
    assert settings.agent.max_tokens == 4096
    assert settings.agent.region == "us-east-1"
    assert settings.agent.max_agent_iterations == 10
    assert "claude-sonnet-4" in settings.agent.model
    assert settings.cost_explorer.region == "us-east-1"
    assert settings.cost_explorer.lookback_days == 14


def test_cost_explorer_config_from_yaml():
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as f:
        f.write(
            "cost_explorer:\n"
            "  region: eu-west-1\n"
            "  lookback_days: 30\n"
        )
        f.flush()
        settings = load_settings(f.name)
    assert settings.cost_explorer.region == "eu-west-1"
    assert settings.cost_explorer.lookback_days == 30


def test_cost_explorer_lookback_too_large():
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as f:
        f.write("cost_explorer:\n  lookback_days: 400\n")
        f.flush()
        with pytest.raises(ConfigError, match="<= 365"):
            load_settings(f.name)


def test_cost_explorer_region_env_override(monkeypatch):
    monkeypatch.setenv("AWS_COST_EXPLORER_REGION", "ap-southeast-1")
    settings = load_settings(None)
    assert settings.cost_explorer.region == "ap-southeast-1"
