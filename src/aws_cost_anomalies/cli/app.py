"""Typer app root — registers all CLI subcommands."""

from __future__ import annotations

import typer

app = typer.Typer(
    name="aws-cost-anomalies",
    help="Detect AWS cost anomalies across root and linked accounts.",
    no_args_is_help=True,
)


def main() -> None:
    # Import commands to register them
    from aws_cost_anomalies.cli import anomalies as _anomalies  # noqa: F401
    from aws_cost_anomalies.cli import ingest as _ingest  # noqa: F401
    from aws_cost_anomalies.cli import query as _query  # noqa: F401
    from aws_cost_anomalies.cli import trends as _trends  # noqa: F401

    app()


if __name__ == "__main__":
    main()
