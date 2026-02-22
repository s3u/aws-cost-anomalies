"""Tool definitions and executors for the NLQ agent."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from aws_cost_anomalies.nlq.mcp_bridge import MCPBridge

import boto3
import duckdb
from botocore.exceptions import ClientError, NoCredentialsError

from aws_cost_anomalies.nlq.executor import (
    UnsafeSQLError,
    execute_query,
)


@dataclass
class ToolContext:
    """Shared context passed to every tool executor."""

    db_conn: duckdb.DuckDBPyConnection
    aws_region: str = "us-east-1"


# ---------------------------------------------------------------------------
# Tool spec definitions (Bedrock Converse toolSpec format)
# ---------------------------------------------------------------------------

QUERY_COST_DATABASE_SPEC: dict = {
    "toolSpec": {
        "name": "query_cost_database",
        "description": (
            "Query the local DuckDB cost database using SQL. "
            "The database contains AWS Cost and Usage Report data "
            "with tables: daily_cost_summary (pre-aggregated daily "
            "totals by account, service, region) and cost_line_items "
            "(raw CUR line items). Use DuckDB SQL syntax. Only "
            "SELECT queries are allowed."
        ),
        "inputSchema": {
            "json": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": (
                            "A read-only DuckDB SQL query. Must start "
                            "with SELECT or WITH."
                        ),
                    }
                },
                "required": ["sql"],
            }
        },
    }
}

COST_EXPLORER_SPEC: dict = {
    "toolSpec": {
        "name": "get_cost_explorer_data",
        "description": (
            "Get real-time cost data from AWS Cost Explorer API. "
            "Useful for recent costs not yet in the CUR database, "
            "or for cost forecasts. Returns cost and usage grouped "
            "by the specified dimension."
        ),
        "inputSchema": {
            "json": {
                "type": "object",
                "properties": {
                    "start_date": {
                        "type": "string",
                        "description": "Start date in YYYY-MM-DD format.",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date in YYYY-MM-DD format (exclusive).",
                    },
                    "granularity": {
                        "type": "string",
                        "enum": ["DAILY", "MONTHLY"],
                        "description": "Time granularity for results.",
                    },
                    "group_by": {
                        "type": "string",
                        "enum": [
                            "SERVICE",
                            "LINKED_ACCOUNT",
                            "REGION",
                            "USAGE_TYPE",
                        ],
                        "description": "Dimension to group costs by. Optional.",
                    },
                },
                "required": ["start_date", "end_date", "granularity"],
            }
        },
    }
}

CLOUDWATCH_SPEC: dict = {
    "toolSpec": {
        "name": "get_cloudwatch_metrics",
        "description": (
            "Get CloudWatch metrics or billing alarms. Use action "
            "'get_metric' to retrieve metric statistics, or "
            "'describe_alarms' to list active billing/cost alarms."
        ),
        "inputSchema": {
            "json": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["get_metric", "describe_alarms"],
                        "description": "Which CloudWatch operation to perform.",
                    },
                    "namespace": {
                        "type": "string",
                        "description": (
                            "CloudWatch namespace (e.g. 'AWS/Billing'). "
                            "Required for get_metric."
                        ),
                    },
                    "metric_name": {
                        "type": "string",
                        "description": (
                            "Metric name (e.g. 'EstimatedCharges'). "
                            "Required for get_metric."
                        ),
                    },
                    "period_hours": {
                        "type": "integer",
                        "description": "How many hours of data to fetch. Default 24.",
                    },
                },
                "required": ["action"],
            }
        },
    }
}

BUDGET_SPEC: dict = {
    "toolSpec": {
        "name": "get_budget_info",
        "description": (
            "Get AWS Budgets information — configured budgets, "
            "their limits, and actual vs forecasted spend."
        ),
        "inputSchema": {
            "json": {
                "type": "object",
                "properties": {
                    "account_id": {
                        "type": "string",
                        "description": (
                            "AWS account ID to query budgets for. "
                            "Uses STS caller identity if not provided."
                        ),
                    }
                },
                "required": [],
            }
        },
    }
}

ORGANIZATION_SPEC: dict = {
    "toolSpec": {
        "name": "get_organization_info",
        "description": (
            "Get AWS Organizations info — list accounts in the "
            "organization with names, IDs, and status."
        ),
        "inputSchema": {
            "json": {
                "type": "object",
                "properties": {
                    "account_id": {
                        "type": "string",
                        "description": (
                            "Specific account ID to describe. "
                            "If omitted, lists all accounts."
                        ),
                    }
                },
                "required": [],
            }
        },
    }
}


# ---------------------------------------------------------------------------
# Tool registry
# ---------------------------------------------------------------------------

TOOL_DEFINITIONS: list[dict] = [
    QUERY_COST_DATABASE_SPEC,
    COST_EXPLORER_SPEC,
    CLOUDWATCH_SPEC,
    BUDGET_SPEC,
    ORGANIZATION_SPEC,
]


# ---------------------------------------------------------------------------
# Tool executors — each returns a dict (success or error)
# ---------------------------------------------------------------------------

def _make_serializable(value: Any) -> Any:
    """Convert non-JSON-serializable types to strings/floats.

    Bedrock Converse toolResult JSON documents only accept
    str, int, bool, float, list, and dict.
    """
    if value is None:
        return None
    if isinstance(value, (str, int, bool, float)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [_make_serializable(v) for v in value]
    if isinstance(value, dict):
        return {k: _make_serializable(v) for k, v in value.items()}
    return str(value)


def _execute_query_cost_database(
    tool_input: dict, context: ToolContext
) -> dict:
    """Execute a SQL query against the local DuckDB database."""
    sql = tool_input.get("sql", "")
    if not sql.strip():
        return {"error": "No SQL query provided."}

    try:
        columns, rows = execute_query(context.db_conn, sql)
    except UnsafeSQLError as e:
        return {"error": f"Unsafe query blocked: {e}"}
    except duckdb.Error as e:
        return {"error": f"SQL execution error: {e}"}

    # Format results as list of dicts, ensuring JSON-safe types
    results = [
        {col: _make_serializable(val) for col, val in zip(columns, row)}
        for row in rows
    ]
    return {
        "columns": columns,
        "row_count": len(rows),
        "results": results,
    }


def _execute_cost_explorer(
    tool_input: dict, context: ToolContext
) -> dict:
    """Fetch data from AWS Cost Explorer."""
    try:
        client = boto3.client("ce", region_name=context.aws_region)

        kwargs: dict[str, Any] = {
            "TimePeriod": {
                "Start": tool_input["start_date"],
                "End": tool_input["end_date"],
            },
            "Granularity": tool_input.get("granularity", "DAILY"),
            "Metrics": ["UnblendedCost", "BlendedCost"],
        }

        group_by = tool_input.get("group_by")
        if group_by:
            kwargs["GroupBy"] = [
                {"Type": "DIMENSION", "Key": group_by}
            ]

        response = client.get_cost_and_usage(**kwargs)
        results = []
        for period in response.get("ResultsByTime", []):
            entry: dict[str, Any] = {
                "start": period["TimePeriod"]["Start"],
                "end": period["TimePeriod"]["End"],
            }
            if period.get("Groups"):
                entry["groups"] = [
                    {
                        "key": g["Keys"][0],
                        "unblended_cost": g["Metrics"]["UnblendedCost"]["Amount"],
                    }
                    for g in period["Groups"]
                ]
            elif period.get("Total"):
                entry["total_unblended_cost"] = period["Total"][
                    "UnblendedCost"
                ]["Amount"]
            results.append(entry)

        return {"results": results}

    except NoCredentialsError:
        return {
            "error": "AWS credentials not found for Cost Explorer."
        }
    except ClientError as e:
        return {"error": f"Cost Explorer error: {e}"}


def _execute_cloudwatch(
    tool_input: dict, context: ToolContext
) -> dict:
    """Fetch CloudWatch metrics or alarms."""
    action = tool_input.get("action", "")

    try:
        client = boto3.client(
            "cloudwatch", region_name=context.aws_region
        )

        if action == "describe_alarms":
            response = client.describe_alarms(
                StateValue="ALARM",
                MaxRecords=50,
            )
            alarms = [
                {
                    "name": a["AlarmName"],
                    "state": a["StateValue"],
                    "metric": a.get("MetricName", ""),
                    "threshold": a.get("Threshold"),
                    "description": a.get("AlarmDescription", ""),
                }
                for a in response.get("MetricAlarms", [])
            ]
            return {"alarms": alarms, "count": len(alarms)}

        if action == "get_metric":
            namespace = tool_input.get("namespace", "AWS/Billing")
            metric_name = tool_input.get(
                "metric_name", "EstimatedCharges"
            )
            period_hours = tool_input.get("period_hours", 24)
            end_time = date.today()
            start_time = end_time - timedelta(
                hours=max(period_hours, 1)
            )

            response = client.get_metric_statistics(
                Namespace=namespace,
                MetricName=metric_name,
                StartTime=start_time.isoformat(),
                EndTime=end_time.isoformat(),
                Period=3600,
                Statistics=["Maximum", "Average"],
            )
            datapoints = sorted(
                response.get("Datapoints", []),
                key=lambda d: d["Timestamp"].isoformat(),
            )
            results = [
                {
                    "timestamp": dp["Timestamp"].isoformat(),
                    "maximum": dp.get("Maximum"),
                    "average": dp.get("Average"),
                }
                for dp in datapoints
            ]
            return {"metric": metric_name, "datapoints": results}

        return {"error": f"Unknown action: {action}"}

    except NoCredentialsError:
        return {
            "error": "AWS credentials not found for CloudWatch."
        }
    except ClientError as e:
        return {"error": f"CloudWatch error: {e}"}


def _execute_budget_info(
    tool_input: dict, context: ToolContext
) -> dict:
    """Fetch AWS Budgets information."""
    try:
        account_id = tool_input.get("account_id")
        if not account_id:
            sts = boto3.client(
                "sts", region_name=context.aws_region
            )
            account_id = sts.get_caller_identity()["Account"]

        client = boto3.client(
            "budgets", region_name=context.aws_region
        )
        response = client.describe_budgets(AccountId=account_id)
        budgets = []
        for b in response.get("Budgets", []):
            limit = b.get("BudgetLimit", {})
            actual = b.get("CalculatedSpend", {}).get(
                "ActualSpend", {}
            )
            forecasted = b.get("CalculatedSpend", {}).get(
                "ForecastedSpend", {}
            )
            budgets.append(
                {
                    "name": b["BudgetName"],
                    "type": b["BudgetType"],
                    "limit": f"{limit.get('Amount', '?')} {limit.get('Unit', '')}",
                    "actual_spend": actual.get("Amount", "?"),
                    "forecasted_spend": forecasted.get(
                        "Amount", "?"
                    ),
                    "time_unit": b.get("TimeUnit", ""),
                }
            )
        return {"budgets": budgets, "account_id": account_id}

    except NoCredentialsError:
        return {
            "error": "AWS credentials not found for Budgets."
        }
    except ClientError as e:
        return {"error": f"Budgets error: {e}"}


def _execute_organization_info(
    tool_input: dict, context: ToolContext
) -> dict:
    """Fetch AWS Organizations account info."""
    try:
        client = boto3.client(
            "organizations", region_name=context.aws_region
        )

        specific_id = tool_input.get("account_id")
        if specific_id:
            response = client.describe_account(
                AccountId=specific_id
            )
            acct = response["Account"]
            return {
                "account": {
                    "id": acct["Id"],
                    "name": acct["Name"],
                    "email": acct.get("Email", ""),
                    "status": acct["Status"],
                }
            }

        # List all accounts
        paginator = client.get_paginator("list_accounts")
        accounts = []
        for page in paginator.paginate():
            for acct in page["Accounts"]:
                accounts.append(
                    {
                        "id": acct["Id"],
                        "name": acct["Name"],
                        "email": acct.get("Email", ""),
                        "status": acct["Status"],
                    }
                )
        return {"accounts": accounts, "count": len(accounts)}

    except NoCredentialsError:
        return {
            "error": "AWS credentials not found for Organizations."
        }
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "AWSOrganizationsNotInUseException":
            return {
                "error": (
                    "AWS Organizations is not enabled for "
                    "this account."
                )
            }
        return {"error": f"Organizations error: {e}"}


# ---------------------------------------------------------------------------
# Executor registry and dispatch
# ---------------------------------------------------------------------------

_EXECUTORS: dict[str, Callable[[dict, ToolContext], dict]] = {
    "query_cost_database": _execute_query_cost_database,
    "get_cost_explorer_data": _execute_cost_explorer,
    "get_cloudwatch_metrics": _execute_cloudwatch,
    "get_budget_info": _execute_budget_info,
    "get_organization_info": _execute_organization_info,
}


def execute_tool(
    tool_name: str,
    tool_input: dict,
    context: ToolContext,
    mcp_bridge: MCPBridge | None = None,
) -> dict:
    """Dispatch a tool call to the appropriate executor.

    Always returns a dict — errors are returned as
    {"error": "..."} rather than raised, so the agent can adapt.
    """
    executor = _EXECUTORS.get(tool_name)
    if executor:
        try:
            return executor(tool_input, context)
        except Exception as e:
            return {"error": f"Tool execution failed: {e}"}

    if mcp_bridge is not None and mcp_bridge.is_mcp_tool(tool_name):
        try:
            return mcp_bridge.call_tool(tool_name, tool_input)
        except Exception as e:
            return {"error": f"MCP tool execution failed: {e}"}

    return {"error": f"Unknown tool: {tool_name}"}
