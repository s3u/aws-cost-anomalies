"""Query command — natural language queries via Bedrock agent."""

from __future__ import annotations

import json
from typing import Optional

import typer
from rich.console import Console
from rich.syntax import Syntax

from aws_cost_anomalies.agent import (
    AgentError,
    AgentStep,
    run_agent,
)
from aws_cost_anomalies.cli.app import app
from aws_cost_anomalies.config.settings import Settings, load_settings
from aws_cost_anomalies.storage.database import get_connection
from aws_cost_anomalies.storage.schema import create_tables

console = Console()

# Human-readable labels for tool progress messages.
_TOOL_LABELS: dict[str, str] = {
    "query_cost_database": "Querying cost database",
    "get_cost_explorer_data": "Fetching Cost Explorer data",
    "get_cloudwatch_metrics": "Checking CloudWatch metrics",
    "get_budget_info": "Retrieving budget info",
    "get_organization_info": "Looking up organization accounts",
    "ingest_cost_explorer_data": "Importing Cost Explorer data",
    "ingest_cur_data": "Importing CUR data from S3",
}


def _make_step_callback(verbose: bool):
    """Create an on_step callback.

    Always shows brief progress (tool name) before execution.
    In verbose mode, also shows tool input and result details.

    on_step is called twice per tool:
      1. Before execution (step.tool_result is None)
      2. After execution (step.tool_result is set)
    """

    def _on_step(step: AgentStep) -> None:
        if step.tool_result is None:
            # --- Pre-execution: show what tool is running ---
            label = _TOOL_LABELS.get(
                step.tool_name, f"Using {step.tool_name}"
            )
            console.print(f"\n[bold blue]{label}...[/bold blue]")

            # In verbose mode, also show input details
            if verbose:
                if step.tool_name == "query_cost_database":
                    sql = step.tool_input.get("sql", "")
                    if sql:
                        console.print(Syntax(sql, "sql", theme="monokai"))
                else:
                    input_str = json.dumps(step.tool_input, indent=2)
                    console.print(f"[dim]{input_str}[/dim]")
        else:
            # --- Post-execution: show result summary ---
            if "error" in step.tool_result:
                console.print(
                    f"[red]Error:[/red] "
                    f"{step.tool_result['error']}"
                )
            elif step.tool_name == "query_cost_database":
                count = step.tool_result.get("row_count", 0)
                console.print(f"[dim]{count} rows returned[/dim]")
            elif step.tool_name in (
                "ingest_cost_explorer_data",
                "ingest_cur_data",
            ):
                rows = step.tool_result.get("rows_loaded", 0)
                source = step.tool_result.get("source", "")
                console.print(
                    f"[green]{rows:,} rows loaded[/green]"
                    f"[dim] (source: {source})[/dim]"
                )
            elif verbose:
                preview = json.dumps(step.tool_result, indent=2)
                if len(preview) > 500:
                    preview = preview[:500] + "\n..."
                console.print(f"[dim]{preview}[/dim]")

    return _on_step


def _run_question(
    conn,
    question: str,
    model: str,
    region: str,
    max_tokens: int,
    max_iterations: int,
    settings: Settings,
    history: list[dict] | None = None,
    mcp_bridge=None,
    verbose: bool = False,
) -> list[dict] | None:
    """Send a question to the agent and display the response.

    Returns the updated conversation messages for multi-turn
    context, or None on error.
    """
    console.print(f"\n[dim]Thinking...[/dim]")
    try:
        response = run_agent(
            question=question,
            db_conn=conn,
            model=model,
            region=region,
            max_tokens=max_tokens,
            max_iterations=max_iterations,
            on_step=_make_step_callback(verbose),
            history=history,
            mcp_bridge=mcp_bridge,
            settings=settings,
        )
    except AgentError as e:
        console.print(f"[red]Error:[/red] {e}")
        return history

    console.print("\n[bold]Answer:[/bold]")
    console.print(response.answer)

    if response.steps:
        console.print(
            f"\n[dim]({len(response.steps)} tool calls, "
            f"{response.input_tokens + response.output_tokens} "
            f"tokens used)[/dim]"
        )

    return response.messages


@app.command()
def query(
    question: Optional[str] = typer.Argument(
        None, help="Question in natural language"
    ),
    config: Optional[str] = typer.Option(
        None, "--config", help="Path to config YAML file"
    ),
    interactive: bool = typer.Option(
        False,
        "--interactive",
        "-i",
        help="Interactive REPL mode",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Show tool calls and SQL as the agent runs",
    ),
) -> None:
    """Ask questions about your AWS costs in natural language."""
    settings = load_settings(config)
    conn = get_connection(settings.database.path)
    create_tables(conn)

    model = settings.agent.model
    region = settings.agent.region
    max_tokens = settings.agent.max_tokens
    max_iterations = settings.agent.max_agent_iterations

    # Set up MCP bridge if servers are configured
    bridge = None
    if settings.agent.mcp_servers:
        try:
            from aws_cost_anomalies.agent.mcp_bridge import MCPBridge
        except ImportError:
            console.print(
                "[yellow]MCP servers configured but 'mcp' package "
                "not installed.[/yellow]\n"
                "Install with: [bold]pip install 'aws-cost-anomalies[mcp]'[/bold]"
            )
            raise typer.Exit(1)

        bridge = MCPBridge(settings.agent.mcp_servers)
        try:
            tool_count = bridge.connect()
            console.print(
                f"[dim]MCP: {tool_count} tools from "
                f"{len(settings.agent.mcp_servers)} server(s)[/dim]"
            )
        except Exception as e:
            console.print(f"[yellow]MCP connection error: {e}[/yellow]")
            bridge = None

    try:
        if interactive:
            console.print("\n[bold]AWS Cost Query Agent[/bold]")
            console.print(
                "Ask questions about your AWS costs in plain English. "
                "The agent can query local cost data, import from Cost "
                "Explorer or CUR, and call AWS APIs.\n"
            )
            console.print("[dim]Example queries:[/dim]")
            console.print("[dim]  What are my top 5 most expensive services this month?[/dim]")
            console.print("[dim]  Import Cost Explorer data for the last 30 days[/dim]")
            console.print("[dim]  Which accounts had the biggest cost increase?[/dim]")
            console.print("[dim]  Break down last week's spend by service and region[/dim]")
            console.print(
                "\n[dim]Type 'exit' or 'quit' to leave.[/dim]\n"
            )

            history: list[dict] | None = None
            while True:
                try:
                    q = console.input("[bold cyan]> [/bold cyan]")
                except (EOFError, KeyboardInterrupt):
                    console.print("\nBye!")
                    break

                q = q.strip()
                if not q:
                    continue
                if q.lower() in ("exit", "quit", "q"):
                    console.print("Bye!")
                    break

                history = _run_question(
                    conn, q, model, region,
                    max_tokens, max_iterations,
                    settings=settings,
                    history=history,
                    mcp_bridge=bridge,
                    verbose=verbose,
                )
                console.print()

        elif question:
            _run_question(
                conn, question, model, region,
                max_tokens, max_iterations,
                settings=settings,
                mcp_bridge=bridge,
                verbose=verbose,
            )

        else:
            console.print(
                "[yellow]Provide a question or use "
                "--interactive for REPL mode.[/yellow]\n"
                "Example: aws-cost-anomalies query "
                "'What are my top 5 most expensive services?'"
            )
            raise typer.Exit(1)
    finally:
        if bridge is not None:
            bridge.close()
