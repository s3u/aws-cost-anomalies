"""Anomalies command — detect cost anomalies."""

from __future__ import annotations

from typing import Optional

import typer
from rich.console import Console

from aws_cost_anomalies.analysis.anomalies import (
    SENSITIVITY_THRESHOLDS,
    detect_anomalies,
)
from aws_cost_anomalies.cli.app import app
from aws_cost_anomalies.cli.formatting import (
    format_currency,
    print_anomalies_table,
)
from aws_cost_anomalies.config.settings import load_settings
from aws_cost_anomalies.storage.database import get_connection
from aws_cost_anomalies.storage.schema import create_tables

console = Console()

GROUP_BY_MAP = {
    "service": ["product_code"],
    "account": ["usage_account_id"],
    "region": ["region"],
    "service+account": ["product_code", "usage_account_id"],
    "service+region": ["product_code", "region"],
    "account+region": ["usage_account_id", "region"],
}


def _check_has_data(conn) -> bool:
    result = conn.execute(
        "SELECT COUNT(*) FROM daily_cost_summary"
    ).fetchone()
    return result[0] > 0 if result else False


@app.command()
def anomalies(
    config: Optional[str] = typer.Option(
        None, "--config", help="Path to config YAML file"
    ),
    days: int = typer.Option(
        14, "--days", help="Rolling window size in days"
    ),
    sensitivity: str = typer.Option(
        "medium",
        "--sensitivity",
        help="Sensitivity: low, medium, or high",
    ),
    group_by: str = typer.Option(
        "service",
        "--group-by",
        help="Group by: service, account, region, "
        "service+account, service+region, account+region",
    ),
    drift_threshold: Optional[int] = typer.Option(
        None,
        "--drift-threshold",
        help="Drift threshold in percent (default: from config, or 20)",
    ),
) -> None:
    """Detect cost anomalies using z-score analysis."""
    settings = load_settings(config)
    conn = get_connection(settings.database.path)
    create_tables(conn)

    if sensitivity not in SENSITIVITY_THRESHOLDS:
        console.print(
            "[red]Error:[/red] --sensitivity must be "
            "one of: low, medium, high"
        )
        raise typer.Exit(1)

    if group_by not in GROUP_BY_MAP:
        choices = ", ".join(GROUP_BY_MAP)
        console.print(
            f"[red]Error:[/red] --group-by must be "
            f"one of: {choices}"
        )
        raise typer.Exit(1)

    if not _check_has_data(conn):
        console.print(
            "[yellow]No cost data found.[/yellow] "
            "Run [bold]ingest[/bold] first to load CUR data."
        )
        raise typer.Exit(1)

    columns = GROUP_BY_MAP[group_by]
    min_cost = settings.anomaly.min_daily_cost
    threshold = SENSITIVITY_THRESHOLDS[sensitivity]

    effective_drift = drift_threshold if drift_threshold is not None else settings.anomaly.drift_threshold_pct

    results = detect_anomalies(
        conn,
        days=days,
        group_by=columns,
        sensitivity=sensitivity,
        min_daily_cost=min_cost,
        drift_threshold=effective_drift / 100,
    )

    print_anomalies_table(results)

    # Show filter info
    console.print(
        f"\n[dim]Settings: {days}-day window, "
        f"sensitivity={sensitivity} (z>{threshold}), "
        f"drift threshold={effective_drift:.0f}%, "
        f"min cost={format_currency(min_cost)}/day[/dim]"
    )
