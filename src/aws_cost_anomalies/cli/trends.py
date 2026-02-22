"""Trends command — show daily cost trends."""

from __future__ import annotations

from typing import Optional

import typer
from rich.console import Console

from aws_cost_anomalies.analysis.trends import (
    get_daily_trends,
    get_total_daily_costs,
)
from aws_cost_anomalies.cli.app import app
from aws_cost_anomalies.cli.formatting import (
    format_currency,
    print_trends_table,
)
from aws_cost_anomalies.config.settings import load_settings
from aws_cost_anomalies.storage.database import get_connection
from aws_cost_anomalies.storage.schema import create_tables

console = Console()

GROUP_BY_LABELS = {
    "service": ("product_code", "Service"),
    "account": ("usage_account_id", "Account"),
    "region": ("region", "Region"),
}


def _check_has_data(conn) -> bool:
    result = conn.execute(
        "SELECT COUNT(*) FROM daily_cost_summary"
    ).fetchone()
    return result[0] > 0 if result else False


@app.command()
def trends(
    config: Optional[str] = typer.Option(
        None, "--config", help="Path to config YAML file"
    ),
    days: int = typer.Option(
        14, "--days", help="Number of days to look back"
    ),
    group_by: str = typer.Option(
        "service",
        "--group-by",
        help="Group by: service, account, or region",
    ),
    top: int = typer.Option(
        10, "--top", help="Show top N groups by cost"
    ),
) -> None:
    """Show daily cost trends by dimension."""
    settings = load_settings(config)
    conn = get_connection(settings.database.path)
    create_tables(conn)

    if group_by not in GROUP_BY_LABELS:
        choices = ", ".join(GROUP_BY_LABELS)
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

    column, label = GROUP_BY_LABELS[group_by]

    # Show total daily costs
    totals = get_total_daily_costs(conn, days=days)
    if totals:
        console.print(
            f"\n[bold]Total Daily Costs "
            f"(last {days} days):[/bold]"
        )
        for usage_date, total in totals:
            console.print(
                f"  {usage_date}  {format_currency(total)}"
            )
        console.print()

    # Show grouped trends
    trend_rows = get_daily_trends(
        conn, days=days, group_by=column, top_n=top
    )

    if trend_rows:
        print_trends_table(trend_rows, label)
    else:
        console.print(
            "[yellow]No trend data for the selected "
            "time range.[/yellow]"
        )
