"""Tests for tool executors."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import duckdb
import pytest

from aws_cost_anomalies.agent.tools import (
    TOOL_DEFINITIONS,
    ToolContext,
    execute_tool,
)
from aws_cost_anomalies.storage.schema import create_tables


@pytest.fixture
def db_conn():
    """In-memory DuckDB with schema and sample data."""
    conn = duckdb.connect(":memory:")
    create_tables(conn)
    conn.execute(
        "INSERT INTO daily_cost_summary VALUES "
        "('2025-01-15', '111111111111', 'AmazonEC2', "
        "'us-east-1', 1500.50, 1400.00, 100, 50, 'cur')"
    )
    conn.execute(
        "INSERT INTO daily_cost_summary VALUES "
        "('2025-01-15', '111111111111', 'AmazonS3', "
        "'us-east-1', 250.75, 240.00, 5000, 20, 'cur')"
    )
    conn.execute(
        "INSERT INTO daily_cost_summary VALUES "
        "('2025-01-16', '222222222222', 'AmazonEC2', "
        "'us-west-2', 800.00, 750.00, 80, 30, 'cur')"
    )
    return conn


@pytest.fixture
def context(db_conn):
    return ToolContext(db_conn=db_conn, aws_region="us-east-1")


class TestToolDefinitions:
    def test_all_tools_have_spec(self):
        assert len(TOOL_DEFINITIONS) == 7
        for defn in TOOL_DEFINITIONS:
            assert "toolSpec" in defn
            spec = defn["toolSpec"]
            assert "name" in spec
            assert "description" in spec
            assert "inputSchema" in spec

    def test_tool_names(self):
        names = {d["toolSpec"]["name"] for d in TOOL_DEFINITIONS}
        assert names == {
            "query_cost_database",
            "get_cost_explorer_data",
            "get_cloudwatch_metrics",
            "get_budget_info",
            "get_organization_info",
            "ingest_cost_explorer_data",
            "ingest_cur_data",
        }


class TestQueryCostDatabase:
    def test_simple_select(self, context):
        result = execute_tool(
            "query_cost_database",
            {"sql": "SELECT COUNT(*) AS cnt FROM daily_cost_summary"},
            context,
        )
        assert "error" not in result
        assert result["row_count"] == 1
        assert result["results"][0]["cnt"] == 3

    def test_aggregation_query(self, context):
        result = execute_tool(
            "query_cost_database",
            {
                "sql": (
                    "SELECT product_code, "
                    "ROUND(SUM(total_unblended_cost), 2) AS total "
                    "FROM daily_cost_summary "
                    "GROUP BY product_code "
                    "ORDER BY total DESC"
                )
            },
            context,
        )
        assert "error" not in result
        assert result["row_count"] == 2
        assert result["results"][0]["product_code"] == "AmazonEC2"

    def test_unsafe_query_blocked(self, context):
        result = execute_tool(
            "query_cost_database",
            {"sql": "DROP TABLE daily_cost_summary"},
            context,
        )
        assert "error" in result
        assert "Unsafe" in result["error"]

    def test_empty_sql(self, context):
        result = execute_tool(
            "query_cost_database",
            {"sql": ""},
            context,
        )
        assert "error" in result

    def test_invalid_sql(self, context):
        result = execute_tool(
            "query_cost_database",
            {"sql": "SELECT * FROM nonexistent_table"},
            context,
        )
        assert "error" in result

    def test_returns_columns(self, context):
        result = execute_tool(
            "query_cost_database",
            {"sql": "SELECT usage_date, product_code FROM daily_cost_summary LIMIT 1"},
            context,
        )
        assert result["columns"] == ["usage_date", "product_code"]


class TestCostExplorer:
    @patch("aws_cost_anomalies.agent.tools.boto3.client")
    def test_basic_cost_query(self, mock_boto_client, context):
        mock_ce = MagicMock()
        mock_ce.get_cost_and_usage.return_value = {
            "ResultsByTime": [
                {
                    "TimePeriod": {
                        "Start": "2025-01-01",
                        "End": "2025-01-02",
                    },
                    "Total": {
                        "UnblendedCost": {
                            "Amount": "1234.56",
                            "Unit": "USD",
                        },
                        "BlendedCost": {
                            "Amount": "1200.00",
                            "Unit": "USD",
                        },
                    },
                }
            ]
        }
        mock_boto_client.return_value = mock_ce

        result = execute_tool(
            "get_cost_explorer_data",
            {
                "start_date": "2025-01-01",
                "end_date": "2025-01-02",
                "granularity": "DAILY",
            },
            context,
        )

        assert "error" not in result
        assert len(result["results"]) == 1
        assert result["results"][0]["total_unblended_cost"] == "1234.56"

    @patch("aws_cost_anomalies.agent.tools.boto3.client")
    def test_grouped_results(self, mock_boto_client, context):
        mock_ce = MagicMock()
        mock_ce.get_cost_and_usage.return_value = {
            "ResultsByTime": [
                {
                    "TimePeriod": {
                        "Start": "2025-01-01",
                        "End": "2025-02-01",
                    },
                    "Groups": [
                        {
                            "Keys": ["AmazonEC2"],
                            "Metrics": {
                                "UnblendedCost": {"Amount": "500"},
                                "BlendedCost": {"Amount": "480"},
                            },
                        },
                        {
                            "Keys": ["AmazonS3"],
                            "Metrics": {
                                "UnblendedCost": {"Amount": "100"},
                                "BlendedCost": {"Amount": "95"},
                            },
                        },
                    ],
                }
            ]
        }
        mock_boto_client.return_value = mock_ce

        result = execute_tool(
            "get_cost_explorer_data",
            {
                "start_date": "2025-01-01",
                "end_date": "2025-02-01",
                "granularity": "MONTHLY",
                "group_by": "SERVICE",
            },
            context,
        )

        assert "error" not in result
        groups = result["results"][0]["groups"]
        assert len(groups) == 2
        assert groups[0]["key"] == "AmazonEC2"


class TestCloudWatch:
    @patch("aws_cost_anomalies.agent.tools.boto3.client")
    def test_describe_alarms(self, mock_boto_client, context):
        mock_cw = MagicMock()
        mock_cw.describe_alarms.return_value = {
            "MetricAlarms": [
                {
                    "AlarmName": "HighBilling",
                    "StateValue": "ALARM",
                    "MetricName": "EstimatedCharges",
                    "Threshold": 1000.0,
                    "AlarmDescription": "Billing over $1000",
                }
            ]
        }
        mock_boto_client.return_value = mock_cw

        result = execute_tool(
            "get_cloudwatch_metrics",
            {"action": "describe_alarms"},
            context,
        )

        assert "error" not in result
        assert result["count"] == 1
        assert result["alarms"][0]["name"] == "HighBilling"

    @patch("aws_cost_anomalies.agent.tools.boto3.client")
    def test_unknown_action(self, mock_boto_client, context):
        mock_boto_client.return_value = MagicMock()
        result = execute_tool(
            "get_cloudwatch_metrics",
            {"action": "bad_action"},
            context,
        )
        assert "error" in result


class TestBudgetInfo:
    @patch("aws_cost_anomalies.agent.tools.boto3.client")
    def test_describe_budgets(self, mock_boto_client, context):
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {
            "Account": "111111111111"
        }
        mock_budgets = MagicMock()
        mock_budgets.describe_budgets.return_value = {
            "Budgets": [
                {
                    "BudgetName": "Monthly",
                    "BudgetType": "COST",
                    "BudgetLimit": {
                        "Amount": "10000",
                        "Unit": "USD",
                    },
                    "CalculatedSpend": {
                        "ActualSpend": {"Amount": "7500"},
                        "ForecastedSpend": {"Amount": "9800"},
                    },
                    "TimeUnit": "MONTHLY",
                }
            ]
        }

        def _make_client(service, **kwargs):
            if service == "sts":
                return mock_sts
            return mock_budgets

        mock_boto_client.side_effect = _make_client

        result = execute_tool(
            "get_budget_info", {}, context
        )

        assert "error" not in result
        assert len(result["budgets"]) == 1
        assert result["budgets"][0]["name"] == "Monthly"
        assert result["budgets"][0]["actual_spend"] == "7500"


class TestOrganizationInfo:
    @patch("aws_cost_anomalies.agent.tools.boto3.client")
    def test_list_accounts(self, mock_boto_client, context):
        mock_org = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Accounts": [
                    {
                        "Id": "111111111111",
                        "Name": "Production",
                        "Email": "prod@example.com",
                        "Status": "ACTIVE",
                    },
                    {
                        "Id": "222222222222",
                        "Name": "Staging",
                        "Email": "stage@example.com",
                        "Status": "ACTIVE",
                    },
                ]
            }
        ]
        mock_org.get_paginator.return_value = paginator
        mock_boto_client.return_value = mock_org

        result = execute_tool(
            "get_organization_info", {}, context
        )

        assert "error" not in result
        assert result["count"] == 2
        assert result["accounts"][0]["name"] == "Production"

    @patch("aws_cost_anomalies.agent.tools.boto3.client")
    def test_describe_single_account(self, mock_boto_client, context):
        mock_org = MagicMock()
        mock_org.describe_account.return_value = {
            "Account": {
                "Id": "111111111111",
                "Name": "Production",
                "Email": "prod@example.com",
                "Status": "ACTIVE",
            }
        }
        mock_boto_client.return_value = mock_org

        result = execute_tool(
            "get_organization_info",
            {"account_id": "111111111111"},
            context,
        )

        assert "error" not in result
        assert result["account"]["name"] == "Production"


class TestIngestCostExplorerData:
    def test_missing_dates_returns_error(self, context):
        result = execute_tool(
            "ingest_cost_explorer_data", {}, context
        )
        assert "error" in result
        assert "required" in result["error"].lower()

    def test_missing_end_date_returns_error(self, context):
        result = execute_tool(
            "ingest_cost_explorer_data",
            {"start_date": "2025-01-01"},
            context,
        )
        assert "error" in result

    @patch(
        "aws_cost_anomalies.ingestion.cost_explorer."
        "fetch_cost_explorer_data"
    )
    def test_successful_import(self, mock_fetch, db_conn):
        from datetime import date as d

        from aws_cost_anomalies.config.settings import Settings
        from aws_cost_anomalies.ingestion.cost_explorer import (
            CostExplorerRow,
        )

        mock_fetch.return_value = [
            CostExplorerRow(
                usage_date=d(2025, 1, 1),
                usage_account_id="111",
                product_code="AmazonEC2",
                total_unblended_cost=100.0,
                total_blended_cost=95.0,
            ),
        ]
        ctx = ToolContext(
            db_conn=db_conn,
            aws_region="us-east-1",
            settings=Settings(),
        )
        result = execute_tool(
            "ingest_cost_explorer_data",
            {"start_date": "2025-01-01", "end_date": "2025-01-02"},
            ctx,
        )
        assert "error" not in result
        assert result["rows_loaded"] == 1
        assert result["source"] == "cost_explorer"

    @patch(
        "aws_cost_anomalies.ingestion.cost_explorer."
        "fetch_cost_explorer_data"
    )
    def test_api_error_returned(self, mock_fetch, context):
        from aws_cost_anomalies.ingestion.cost_explorer import (
            CostExplorerError,
        )

        mock_fetch.side_effect = CostExplorerError("no creds")
        result = execute_tool(
            "ingest_cost_explorer_data",
            {"start_date": "2025-01-01", "end_date": "2025-01-02"},
            context,
        )
        assert "error" in result
        assert "no creds" in result["error"]


class TestIngestCurData:
    def test_no_settings_returns_error(self, db_conn):
        ctx = ToolContext(db_conn=db_conn, settings=None)
        result = execute_tool("ingest_cur_data", {}, ctx)
        assert "error" in result
        assert "Settings" in result["error"]

    def test_no_s3_config_returns_error(self, db_conn):
        from aws_cost_anomalies.config.settings import Settings

        ctx = ToolContext(
            db_conn=db_conn, settings=Settings()
        )
        result = execute_tool("ingest_cur_data", {}, ctx)
        assert "error" in result
        assert "bucket" in result["error"].lower()

    def test_invalid_month_format(self, db_conn):
        from aws_cost_anomalies.config.settings import (
            S3Config,
            Settings,
        )

        settings = Settings(
            s3=S3Config(bucket="b", report_name="r")
        )
        ctx = ToolContext(db_conn=db_conn, settings=settings)
        result = execute_tool(
            "ingest_cur_data", {"month": "bad"}, ctx
        )
        assert "error" in result
        assert "YYYY-MM" in result["error"]


class TestUnknownTool:
    def test_unknown_tool_returns_error(self, context):
        result = execute_tool("nonexistent_tool", {}, context)
        assert "error" in result
        assert "Unknown tool" in result["error"]
