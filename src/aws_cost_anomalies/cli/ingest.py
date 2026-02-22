"""Ingest command — download CUR data from S3 and load into DuckDB."""

from __future__ import annotations

import re
from typing import Optional

import typer
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
)

from aws_cost_anomalies.cli.app import app
from aws_cost_anomalies.config.settings import load_settings
from aws_cost_anomalies.ingestion.loader import (
    delete_billing_period_data,
    get_ingested_assemblies,
    load_parquet_file,
    record_ingestion,
)
from aws_cost_anomalies.ingestion.s3_client import (
    CURBrowser,
    S3Error,
)
from aws_cost_anomalies.storage.database import get_connection
from aws_cost_anomalies.storage.schema import (
    create_tables,
    rebuild_daily_summary,
)

console = Console()


def _parse_date_option(date_str: str) -> str:
    """Parse --date YYYY-MM into billing period format.

    Raises typer.BadParameter if the format is invalid.
    """
    if not re.match(r"^\d{4}-\d{2}$", date_str):
        raise typer.BadParameter(
            f"Invalid date format '{date_str}'. "
            "Use YYYY-MM (e.g., 2025-01)."
        )
    year, month = int(date_str[:4]), int(date_str[5:7])
    if month < 1 or month > 12:
        raise typer.BadParameter(
            f"Invalid month {month}. Must be 01-12."
        )
    if month == 12:
        end_year, end_month = year + 1, 1
    else:
        end_year, end_month = year, month + 1
    return f"{year}{month:02d}01-{end_year}{end_month:02d}01"


@app.command()
def ingest(
    config: Optional[str] = typer.Option(
        None, "--config", help="Path to config YAML file"
    ),
    date: Optional[str] = typer.Option(
        None,
        "--date",
        help="Specific month to ingest (YYYY-MM, e.g. 2025-01)",
    ),
    full_refresh: bool = typer.Option(
        False,
        "--full-refresh",
        help="Re-ingest all billing periods",
    ),
) -> None:
    """Download CUR data from S3 and load into DuckDB."""
    settings = load_settings(config)

    if not settings.s3.bucket or not settings.s3.report_name:
        console.print(
            "[red]Error:[/red] S3 bucket and report_name "
            "must be configured.\n"
            "Set them in config.yaml or see config.example.yaml."
        )
        raise typer.Exit(1)

    conn = get_connection(settings.database.path)
    create_tables(conn)

    try:
        browser = CURBrowser(
            bucket=settings.s3.bucket,
            prefix=settings.s3.prefix,
            report_name=settings.s3.report_name,
            region=settings.s3.region,
        )
    except S3Error as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    file_progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        console=console,
    )
    spinner = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    )

    # List billing periods
    with spinner:
        task = spinner.add_task(
            "Listing billing periods...", total=None
        )
        try:
            if date:
                bp = _parse_date_option(date)
                periods = [bp]
            else:
                periods = browser.list_billing_periods()
        except S3Error as e:
            console.print(f"\n[red]Error:[/red] {e}")
            raise typer.Exit(1)
        spinner.update(
            task,
            description=f"Found {len(periods)} billing period(s)",
        )

    if not periods:
        console.print("[yellow]No billing periods found.[/yellow]")
        raise typer.Exit(0)

    # Check what's already ingested
    ingested = (
        get_ingested_assemblies(conn) if not full_refresh else {}
    )

    total_rows = 0
    skipped = 0

    with file_progress:
        for period in periods:
            try:
                manifest = browser.get_manifest(period)
            except (S3Error, FileNotFoundError, ValueError) as e:
                console.print(
                    f"[yellow]Warning:[/yellow] "
                    f"Skipping {period}: {e}"
                )
                continue

            # Skip if already ingested with same assembly
            if not full_refresh and period in ingested:
                if ingested[period] == manifest.assembly_id:
                    skipped += 1
                    continue
                delete_billing_period_data(conn, period)

            if full_refresh:
                delete_billing_period_data(conn, period)

            n_files = len(manifest.report_keys)
            task = file_progress.add_task(
                f"{period}", total=n_files
            )

            for s3_key in manifest.report_keys:
                file_progress.update(
                    task, description=f"{period}: downloading..."
                )
                try:
                    local_path = browser.download_file(
                        s3_key, settings.database.cache_dir
                    )
                except S3Error as e:
                    console.print(
                        f"\n[red]Error downloading "
                        f"{s3_key}:[/red] {e}"
                    )
                    file_progress.advance(task)
                    continue

                file_progress.update(
                    task, description=f"{period}: loading..."
                )
                try:
                    rows = load_parquet_file(
                        conn, local_path, source_file=s3_key
                    )
                except Exception as e:
                    console.print(
                        f"\n[red]Error loading "
                        f"{s3_key}:[/red] {e}"
                    )
                    file_progress.advance(task)
                    continue

                record_ingestion(
                    conn,
                    manifest.assembly_id,
                    period,
                    s3_key,
                    rows,
                )
                total_rows += rows
                file_progress.advance(task)

            file_progress.update(
                task, description=f"{period}: done"
            )

    # Rebuild daily summary
    with spinner:
        task = spinner.add_task(
            "Rebuilding daily summary...", total=None
        )
        summary_rows = rebuild_daily_summary(conn)
        spinner.update(
            task,
            description=f"Daily summary: {summary_rows:,} rows",
        )

    console.print(
        f"\n[green]Ingestion complete.[/green] "
        f"{total_rows:,} line items loaded."
    )
    if skipped:
        console.print(
            f"[dim]{skipped} period(s) already up to date.[/dim]"
        )
