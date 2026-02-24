"""Tool definitions and executors for the agent."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from aws_cost_anomalies.agent.mcp_bridge import MCPBridge
    from aws_cost_anomalies.config.settings import Settings

import duckdb
from botocore.exceptions import ClientError, NoCredentialsError

from aws_cost_anomalies.utils.aws import aws_session

from aws_cost_anomalies.agent.executor import (
    UnsafeSQLError,
    execute_query,
)


@dataclass
class ToolContext:
    """Shared context passed to every tool executor."""

    db_conn: duckdb.DuckDBPyConnection
    aws_region: str = "us-east-1"
    aws_profile: str = ""
    settings: Settings | None = None


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

INGEST_COST_EXPLORER_SPEC: dict = {
    "toolSpec": {
        "name": "ingest_cost_explorer_data",
        "description": (
            "Import daily cost data from the AWS Cost Explorer API "
            "into the local database. Use this when the database is "
            "empty or when the user asks to refresh/import Cost "
            "Explorer data."
        ),
        "inputSchema": {
            "json": {
                "type": "object",
                "properties": {
                    "start_date": {
                        "type": "string",
                        "description": (
                            "Start date in YYYY-MM-DD format (inclusive)."
                        ),
                    },
                    "end_date": {
                        "type": "string",
                        "description": (
                            "End date in YYYY-MM-DD format (exclusive)."
                        ),
                    },
                },
                "required": ["start_date", "end_date"],
            }
        },
    }
}

INGEST_CUR_DATA_SPEC: dict = {
    "toolSpec": {
        "name": "ingest_cur_data",
        "description": (
            "Import CUR (Cost & Usage Report) data from S3 into the "
            "local database. Requires S3 configuration in config.yaml. "
            "Use when the user asks to import or refresh CUR data."
        ),
        "inputSchema": {
            "json": {
                "type": "object",
                "properties": {
                    "month": {
                        "type": "string",
                        "description": (
                            "Specific month to ingest as YYYY-MM "
                            "(e.g. 2025-01). If omitted, ingests "
                            "all available billing periods."
                        ),
                    },
                    "full_refresh": {
                        "type": "boolean",
                        "description": (
                            "Re-ingest even if data already exists "
                            "for the period. Default false."
                        ),
                    },
                },
                "required": [],
            }
        },
    }
}


DETECT_COST_ANOMALIES_SPEC: dict = {
    "toolSpec": {
        "name": "detect_cost_anomalies",
        "description": (
            "Detect cost anomalies in the local database using "
            "robust statistical methods (median/MAD z-scores for "
            "point anomalies, Theil-Sen slope for gradual drift). "
            "Returns detected anomalies with severity, direction, "
            "and statistical details. Use this when the user asks "
            "about unusual spending, cost spikes, or anomalies."
        ),
        "inputSchema": {
            "json": {
                "type": "object",
                "properties": {
                    "days": {
                        "type": "integer",
                        "description": (
                            "Rolling window size in days. Default 14."
                        ),
                    },
                    "sensitivity": {
                        "type": "string",
                        "enum": ["low", "medium", "high"],
                        "description": (
                            "Detection sensitivity: low (z>3), "
                            "medium (z>2.5), high (z>2). Default medium."
                        ),
                    },
                    "group_by": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": [
                                "product_code",
                                "usage_account_id",
                                "region",
                            ],
                        },
                        "description": (
                            "Dimensions to group by. Default "
                            "['product_code']. Use multiple for "
                            "drill-down (e.g. service + account)."
                        ),
                    },
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
    DETECT_COST_ANOMALIES_SPEC,
    INGEST_COST_EXPLORER_SPEC,
    INGEST_CUR_DATA_SPEC,
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
        client = aws_session(context.aws_profile).client(
            "ce", region_name=context.aws_region
        )

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
        client = aws_session(context.aws_profile).client(
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
            end_time = datetime.utcnow()
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
        session = aws_session(context.aws_profile)
        if not account_id:
            sts = session.client(
                "sts", region_name=context.aws_region
            )
            account_id = sts.get_caller_identity()["Account"]

        client = session.client(
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
        client = aws_session(context.aws_profile).client(
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


def _execute_ingest_cost_explorer(
    tool_input: dict, context: ToolContext
) -> dict:
    """Import Cost Explorer data into the local database."""
    from aws_cost_anomalies.ingestion.cost_explorer import (
        CostExplorerError,
        fetch_cost_explorer_data,
    )
    from aws_cost_anomalies.storage.schema import (
        insert_cost_explorer_summary,
    )

    start = tool_input.get("start_date", "")
    end = tool_input.get("end_date", "")
    if not start or not end:
        return {"error": "start_date and end_date are required."}

    ce_region = "us-east-1"
    if context.settings and context.settings.cost_explorer:
        ce_region = context.settings.cost_explorer.region

    pages_fetched = 0

    def on_page(page_num: int, rows_so_far: int) -> None:
        nonlocal pages_fetched
        pages_fetched = page_num

    try:
        rows = fetch_cost_explorer_data(
            start_date=start,
            end_date=end,
            region=ce_region,
            on_page=on_page,
            profile=context.aws_profile,
        )
    except CostExplorerError as e:
        return {"error": str(e)}

    # Transform to tuples for insert
    tuples = [
        (
            r.usage_date,
            r.usage_account_id,
            r.product_code,
            "",  # region — CE 2-dim GroupBy limit
            r.total_unblended_cost,
            r.total_blended_cost,
            0.0,  # usage_amount
            0,  # line_item_count
        )
        for r in rows
    ]

    inserted = insert_cost_explorer_summary(context.db_conn, tuples)
    return {
        "rows_loaded": inserted,
        "date_range": f"{start} to {end}",
        "pages_fetched": pages_fetched,
        "source": "cost_explorer",
    }


def _execute_ingest_cur_data(
    tool_input: dict, context: ToolContext
) -> dict:
    """Import CUR data from S3 into the local database."""
    import re

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
    from aws_cost_anomalies.storage.schema import (
        rebuild_daily_summary,
    )

    if not context.settings:
        return {"error": "Settings not available."}

    s3_cfg = context.settings.s3
    if not s3_cfg.bucket or not s3_cfg.report_name:
        return {
            "error": (
                "S3 bucket and report_name must be configured in "
                "config.yaml to use CUR ingestion."
            )
        }

    month = tool_input.get("month")
    full_refresh = tool_input.get("full_refresh", False)

    try:
        browser = CURBrowser(
            bucket=s3_cfg.bucket,
            prefix=s3_cfg.prefix,
            report_name=s3_cfg.report_name,
            region=s3_cfg.region,
            profile=context.aws_profile,
        )
    except S3Error as e:
        return {"error": f"S3 connection error: {e}"}

    # Determine billing periods
    try:
        if month:
            if not re.match(r"^\d{4}-\d{2}$", month):
                return {
                    "error": (
                        f"Invalid month format '{month}'. "
                        "Use YYYY-MM."
                    )
                }
            y, m = int(month[:4]), int(month[5:7])
            if m == 12:
                end_y, end_m = y + 1, 1
            else:
                end_y, end_m = y, m + 1
            periods = [
                f"{y}{m:02d}01-{end_y}{end_m:02d}01"
            ]
        else:
            periods = browser.list_billing_periods()
    except S3Error as e:
        return {"error": f"Error listing periods: {e}"}

    if not periods:
        return {"error": "No billing periods found in S3."}

    ingested = (
        get_ingested_assemblies(context.db_conn)
        if not full_refresh
        else {}
    )

    total_rows = 0
    errors: list[dict] = []
    cache_dir = context.settings.database.cache_dir

    for period in periods:
        try:
            manifest = browser.get_manifest(period)
        except (S3Error, FileNotFoundError, ValueError) as e:
            errors.append({"period": period, "error": str(e)})
            continue

        if not full_refresh and period in ingested:
            if ingested[period] == manifest.assembly_id:
                continue
            delete_billing_period_data(context.db_conn, period)

        if full_refresh:
            delete_billing_period_data(context.db_conn, period)

        for s3_key in manifest.report_keys:
            try:
                local_path = browser.download_file(
                    s3_key, cache_dir
                )
                rows = load_parquet_file(
                    context.db_conn,
                    local_path,
                    source_file=s3_key,
                )
                record_ingestion(
                    context.db_conn,
                    manifest.assembly_id,
                    period,
                    s3_key,
                    rows,
                )
                total_rows += rows
            except Exception as e:
                errors.append(
                    {"key": s3_key, "error": str(e)}
                )
                continue

    summary_rows = rebuild_daily_summary(context.db_conn)

    result: dict = {
        "rows_loaded": total_rows,
        "summary_rows": summary_rows,
        "periods": periods,
        "source": "cur",
    }
    if errors:
        result["errors"] = errors
    return result


def _execute_detect_cost_anomalies(
    tool_input: dict, context: ToolContext
) -> dict:
    """Run anomaly detection on the local database."""
    from aws_cost_anomalies.analysis.anomalies import detect_anomalies

    # Use config defaults if available, allow tool_input to override
    default_days = 14
    default_sensitivity = "medium"
    drift_threshold = 0.20
    if context.settings:
        default_days = context.settings.anomaly.rolling_window_days
        drift_threshold = context.settings.anomaly.drift_threshold_pct / 100.0
        # Map z_score_threshold to sensitivity label
        z_thresh = context.settings.anomaly.z_score_threshold
        if z_thresh >= 3.0:
            default_sensitivity = "low"
        elif z_thresh >= 2.5:
            default_sensitivity = "medium"
        else:
            default_sensitivity = "high"

    days = tool_input.get("days", default_days)
    sensitivity = tool_input.get("sensitivity", default_sensitivity)
    group_by = tool_input.get("group_by", ["product_code"])

    try:
        anomalies = detect_anomalies(
            context.db_conn,
            days=days,
            group_by=group_by,
            sensitivity=sensitivity,
            drift_threshold=drift_threshold,
        )
    except ValueError as e:
        return {"error": str(e)}

    results = []
    for a in anomalies:
        entry: dict[str, Any] = {
            "usage_date": str(a.usage_date),
            "group_by": a.group_by,
            "group_value": a.group_value,
            "current_cost": round(a.current_cost, 2),
            "median_cost": round(a.median_cost, 2),
            "mad": round(a.mad, 4),
            "z_score": round(a.z_score, 2),
            "severity": a.severity,
            "direction": a.direction,
            "kind": a.kind,
        }
        if a.kind == "trend":
            entry["drift_pct"] = round(a.z_score * 100, 1)
        results.append(entry)

    return {
        "anomaly_count": len(results),
        "anomalies": results,
        "parameters": {
            "days": days,
            "sensitivity": sensitivity,
            "group_by": group_by,
        },
        "summary": (
            f"{len(results)} anomalies detected over the last "
            f"{days} days (sensitivity={sensitivity})."
        ),
    }


# ---------------------------------------------------------------------------
# Executor registry and dispatch
# ---------------------------------------------------------------------------

_EXECUTORS: dict[str, Callable[[dict, ToolContext], dict]] = {
    "query_cost_database": _execute_query_cost_database,
    "get_cost_explorer_data": _execute_cost_explorer,
    "get_cloudwatch_metrics": _execute_cloudwatch,
    "get_budget_info": _execute_budget_info,
    "get_organization_info": _execute_organization_info,
    "detect_cost_anomalies": _execute_detect_cost_anomalies,
    "ingest_cost_explorer_data": _execute_ingest_cost_explorer,
    "ingest_cur_data": _execute_ingest_cur_data,
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
