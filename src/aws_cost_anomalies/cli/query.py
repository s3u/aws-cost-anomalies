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
from aws_cost_anomalies.config.settings import load_settings
from aws_cost_anomalies.storage.database import get_connection
from aws_cost_anomalies.storage.schema import create_tables

console = Console()


def _check_has_data(conn) -> bool:
    """Check if the database has any cost data."""
    result = conn.execute(
        "SELECT COUNT(*) FROM daily_cost_summary"
    ).fetchone()
    return result[0] > 0 if result else False


def _on_agent_step(step: AgentStep) -> None:
    """Display agent tool calls inline as they happen."""
    console.print(
        f"\n[bold blue]Using tool:[/bold blue] {step.tool_name}"
    )

    # Show tool input
    if step.tool_name == "query_cost_database":
        sql = step.tool_input.get("sql", "")
        if sql:
            console.print(Syntax(sql, "sql", theme="monokai"))
    else:
        input_str = json.dumps(step.tool_input, indent=2)
        console.print(f"[dim]{input_str}[/dim]")

    # Show result preview
    if step.tool_result:
        if "error" in step.tool_result:
            console.print(
                f"[red]Tool error:[/red] "
                f"{step.tool_result['error']}"
            )
        elif step.tool_name == "query_cost_database":
            count = step.tool_result.get("row_count", 0)
            console.print(f"[dim]{count} rows returned[/dim]")
        else:
            preview = json.dumps(step.tool_result, indent=2)
            # Truncate long previews
            if len(preview) > 500:
                preview = preview[:500] + "\n..."
            console.print(f"[dim]{preview}[/dim]")


def _run_question(
    conn,
    question: str,
    model: str,
    region: str,
    max_tokens: int,
    max_iterations: int,
    history: list[dict] | None = None,
    mcp_bridge=None,
) -> list[dict] | None:
    """Send a question to the agent and display the response.

    Returns the updated conversation messages for multi-turn
    context, or None on error.
    """
    console.print(f"\n[dim]Thinking: {question}[/dim]")
    try:
        response = run_agent(
            question=question,
            db_conn=conn,
            model=model,
            region=region,
            max_tokens=max_tokens,
            max_iterations=max_iterations,
            on_step=_on_agent_step,
            history=history,
            mcp_bridge=mcp_bridge,
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
) -> None:
    """Ask questions about your AWS costs in natural language."""
    settings = load_settings(config)
    conn = get_connection(settings.database.path)
    create_tables(conn)

    if not _check_has_data(conn):
        console.print(
            "[yellow]No cost data found.[/yellow] "
            "Run [bold]ingest[/bold] first to load CUR data."
        )
        raise typer.Exit(1)

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
            console.print("[bold]AWS Cost Query Agent[/bold]")
            console.print(
                "Ask questions about your AWS costs in plain "
                "English. Type 'exit' or 'quit' to leave.\n"
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
                    history=history,
                    mcp_bridge=bridge,
                )
                console.print()

        elif question:
            _run_question(
                conn, question, model, region,
                max_tokens, max_iterations,
                mcp_bridge=bridge,
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
