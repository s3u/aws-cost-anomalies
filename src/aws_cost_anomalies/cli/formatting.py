"""Rich tables and output formatting."""

from __future__ import annotations

from rich.console import Console
from rich.table import Table

from aws_cost_anomalies.analysis.anomalies import Anomaly
from aws_cost_anomalies.analysis.trends import TrendRow

console = Console()

SEVERITY_COLORS = {
    "critical": "bold red",
    "warning": "yellow",
    "info": "cyan",
}


def format_currency(value: float | None) -> str:
    """Format a cost value as currency."""
    if value is None:
        return "—"
    return f"${value:,.2f}"


def format_pct(value: float | None) -> str:
    """Format a percentage value."""
    if value is None:
        return "—"
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.1f}%"


def print_trends_table(trends: list[TrendRow], group_label: str) -> None:
    """Print a Rich table of cost trends."""
    table = Table(title="Daily Cost Trends", show_lines=False)
    table.add_column("Date", style="dim")
    table.add_column(group_label)
    table.add_column("Cost", justify="right")
    table.add_column("Change", justify="right")
    table.add_column("% Change", justify="right")

    for row in trends:
        change_style = ""
        if row.cost_change is not None:
            change_style = "red" if row.cost_change > 0 else "green"

        table.add_row(
            str(row.usage_date),
            row.group_value,
            format_currency(row.total_cost),
            f"[{change_style}]{format_currency(row.cost_change)}[/{change_style}]"
            if change_style
            else format_currency(row.cost_change),
            f"[{change_style}]{format_pct(row.pct_change)}[/{change_style}]"
            if change_style
            else format_pct(row.pct_change),
        )

    console.print(table)


def print_anomalies_table(anomalies: list[Anomaly]) -> None:
    """Print a Rich table of detected anomalies."""
    if not anomalies:
        console.print("[green]No anomalies detected.[/green]")
        return

    table = Table(title="Cost Anomalies Detected", show_lines=True)
    table.add_column("Severity", justify="center")
    table.add_column("Date", style="dim")
    table.add_column("Dimension")
    table.add_column("Current Cost", justify="right")
    table.add_column("Avg Cost", justify="right")
    table.add_column("Z-Score", justify="right")
    table.add_column("Direction")

    for a in anomalies:
        severity_style = SEVERITY_COLORS.get(a.severity, "")
        direction_icon = "^ spike" if a.direction == "spike" else "v drop"

        table.add_row(
            f"[{severity_style}]{a.severity.upper()}[/{severity_style}]",
            str(a.usage_date),
            f"{a.group_by}={a.group_value}",
            format_currency(a.current_cost),
            format_currency(a.mean_cost),
            f"{a.z_score:+.2f}",
            direction_icon,
        )

    console.print(table)
    console.print(f"\n[dim]{len(anomalies)} anomaly(ies) found.[/dim]")


def print_query_results(columns: list[str], rows: list[tuple]) -> None:
    """Print SQL query results as a Rich table."""
    if not rows:
        console.print("[yellow]No results.[/yellow]")
        return

    table = Table(show_lines=False)
    for col in columns:
        table.add_column(col)

    for row in rows:
        table.add_row(*[_format_cell(v) for v in row])

    console.print(table)
    console.print(f"\n[dim]{len(rows)} row(s)[/dim]")


def _format_cell(value: object) -> str:
    """Format a single cell value for display."""
    if value is None:
        return "NULL"
    if isinstance(value, float):
        return f"{value:,.2f}"
    return str(value)
