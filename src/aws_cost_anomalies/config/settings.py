"""YAML config loading with defaults and environment variable overrides."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


class ConfigError(Exception):
    """Raised for invalid configuration."""

    pass


@dataclass
class S3Config:
    bucket: str = ""
    prefix: str = ""
    report_name: str = ""
    region: str = "us-east-1"


@dataclass
class DatabaseConfig:
    path: str = "./data/costs.duckdb"
    cache_dir: str = "./data/cache"


@dataclass
class AnomalyConfig:
    rolling_window_days: int = 14
    z_score_threshold: float = 2.5
    min_daily_cost: float = 1.0
    drift_threshold_pct: float = 20.0


@dataclass
class MCPServerConfigEntry:
    """Configuration for a single MCP server."""

    name: str
    command: str
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    env_passthrough: list[str] = field(default_factory=list)


@dataclass
class CostExplorerConfig:
    region: str = "us-east-1"
    lookback_days: int = 14


@dataclass
class AgentConfig:
    model: str = "us.anthropic.claude-sonnet-4-20250514-v1:0"
    max_tokens: int = 4096
    region: str = "us-east-1"
    max_agent_iterations: int = 10
    mcp_servers: list[MCPServerConfigEntry] = field(default_factory=list)


@dataclass
class Settings:
    s3: S3Config = field(default_factory=S3Config)
    database: DatabaseConfig = field(
        default_factory=DatabaseConfig
    )
    anomaly: AnomalyConfig = field(
        default_factory=AnomalyConfig
    )
    agent: AgentConfig = field(default_factory=AgentConfig)
    cost_explorer: CostExplorerConfig = field(
        default_factory=CostExplorerConfig
    )


def _safe_int(value, name: str, default: int) -> int:
    """Convert value to int with helpful error."""
    try:
        result = int(value)
    except (TypeError, ValueError):
        raise ConfigError(
            f"Config '{name}' must be an integer, "
            f"got: {value!r}"
        )
    if result < 1:
        raise ConfigError(
            f"Config '{name}' must be >= 1, got: {result}"
        )
    return result


def _safe_float(
    value, name: str, default: float, min_val: float = 0.0
) -> float:
    """Convert value to float with helpful error."""
    try:
        result = float(value)
    except (TypeError, ValueError):
        raise ConfigError(
            f"Config '{name}' must be a number, "
            f"got: {value!r}"
        )
    if result < min_val:
        raise ConfigError(
            f"Config '{name}' must be >= {min_val}, "
            f"got: {result}"
        )
    return result


def load_settings(
    config_path: str | Path | None = None,
) -> Settings:
    """Load settings from YAML config with env var overrides.

    Raises ConfigError for invalid config values.
    """
    raw: dict = {}
    if config_path:
        path = Path(config_path)
        if not path.exists():
            raise ConfigError(
                f"Config file not found: {config_path}"
            )
        try:
            raw = yaml.safe_load(path.read_text()) or {}
        except yaml.YAMLError as e:
            raise ConfigError(
                f"Invalid YAML in {config_path}: {e}"
            )
    elif Path("config.yaml").exists():
        try:
            text = Path("config.yaml").read_text()
            raw = yaml.safe_load(text) or {}
        except yaml.YAMLError as e:
            raise ConfigError(
                f"Invalid YAML in config.yaml: {e}"
            )

    if not isinstance(raw, dict):
        raise ConfigError(
            "Config file must contain a YAML mapping"
        )

    s3_raw = raw.get("s3", {})
    s3 = S3Config(
        bucket=str(s3_raw.get("bucket", "")),
        prefix=str(s3_raw.get("prefix", "")),
        report_name=str(s3_raw.get("report_name", "")),
        region=str(s3_raw.get("region", "us-east-1")),
    )

    db_raw = raw.get("database", {})
    db = DatabaseConfig(
        path=os.environ.get(
            "AWS_COST_DB_PATH",
            str(db_raw.get("path", "./data/costs.duckdb")),
        ),
        cache_dir=os.environ.get(
            "AWS_COST_CACHE_DIR",
            str(db_raw.get("cache_dir", "./data/cache")),
        ),
    )

    anom_raw = raw.get("anomaly", {})
    anomaly = AnomalyConfig(
        rolling_window_days=_safe_int(
            anom_raw.get("rolling_window_days", 14),
            "anomaly.rolling_window_days",
            14,
        ),
        z_score_threshold=_safe_float(
            anom_raw.get("z_score_threshold", 2.5),
            "anomaly.z_score_threshold",
            2.5,
            min_val=0.1,
        ),
        min_daily_cost=_safe_float(
            anom_raw.get("min_daily_cost", 1.0),
            "anomaly.min_daily_cost",
            1.0,
            min_val=0.0,
        ),
        drift_threshold_pct=_safe_float(
            anom_raw.get("drift_threshold_pct", 20.0),
            "anomaly.drift_threshold_pct",
            20.0,
            min_val=1.0,
        ),
    )

    agent_raw = raw.get("agent", {})

    mcp_servers: list[MCPServerConfigEntry] = []
    for entry in agent_raw.get("mcp_servers", []):
        if not isinstance(entry, dict):
            raise ConfigError("Each agent.mcp_servers entry must be a mapping")
        name = entry.get("name")
        command = entry.get("command")
        if not name or not command:
            raise ConfigError(
                "Each agent.mcp_servers entry requires 'name' and 'command'"
            )
        mcp_servers.append(
            MCPServerConfigEntry(
                name=str(name),
                command=str(command),
                args=[str(a) for a in entry.get("args", [])],
                env={str(k): str(v) for k, v in entry.get("env", {}).items()},
                env_passthrough=[str(v) for v in entry.get("env_passthrough", [])],
            )
        )

    agent_cfg = AgentConfig(
        model=str(
            agent_raw.get(
                "model",
                "us.anthropic.claude-sonnet-4-20250514-v1:0",
            )
        ),
        max_tokens=_safe_int(
            agent_raw.get("max_tokens", 4096),
            "agent.max_tokens",
            4096,
        ),
        region=os.environ.get(
            "AWS_BEDROCK_REGION",
            str(agent_raw.get("region", "us-east-1")),
        ),
        max_agent_iterations=_safe_int(
            agent_raw.get("max_agent_iterations", 10),
            "agent.max_agent_iterations",
            10,
        ),
        mcp_servers=mcp_servers,
    )

    ce_raw = raw.get("cost_explorer", {})
    ce_lookback = _safe_int(
        ce_raw.get("lookback_days", 14),
        "cost_explorer.lookback_days",
        14,
    )
    if ce_lookback > 365:
        raise ConfigError(
            "cost_explorer.lookback_days must be <= 365"
        )
    cost_explorer = CostExplorerConfig(
        region=os.environ.get(
            "AWS_COST_EXPLORER_REGION",
            str(ce_raw.get("region", "us-east-1")),
        ),
        lookback_days=ce_lookback,
    )

    return Settings(
        s3=s3,
        database=db,
        anomaly=anomaly,
        agent=agent_cfg,
        cost_explorer=cost_explorer,
    )
