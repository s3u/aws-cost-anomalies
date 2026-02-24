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
        "'us-east-1', 1500.50, 1400.00, 1320.44, 100, 50, 'cur')"
    )
    conn.execute(
        "INSERT INTO daily_cost_summary VALUES "
        "('2025-01-15', '111111111111', 'AmazonS3', "
        "'us-east-1', 250.75, 240.00, 220.66, 5000, 20, 'cur')"
    )
    conn.execute(
        "INSERT INTO daily_cost_summary VALUES "
        "('2025-01-16', '222222222222', 'AmazonEC2', "
        "'us-west-2', 800.00, 750.00, 704.00, 80, 30, 'cur')"
    )
    return conn


@pytest.fixture
def context(db_conn):
    return ToolContext(db_conn=db_conn, aws_region="us-east-1")


class TestToolDefinitions:
    def test_all_tools_have_spec(self):
        assert len(TOOL_DEFINITIONS) == 8
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
            "detect_cost_anomalies",
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
    @patch("aws_cost_anomalies.utils.aws.boto3.Session")
    def test_basic_cost_query(self, mock_session_cls, context):
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
                        "NetAmortizedCost": {
                            "Amount": "1100.00",
                            "Unit": "USD",
                        },
                    },
                }
            ]
        }
        mock_session_cls.return_value.client.return_value = mock_ce

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

    @patch("aws_cost_anomalies.utils.aws.boto3.Session")
    def test_grouped_results(self, mock_session_cls, context):
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
                                "NetAmortizedCost": {"Amount": "440"},
                            },
                        },
                        {
                            "Keys": ["AmazonS3"],
                            "Metrics": {
                                "UnblendedCost": {"Amount": "100"},
                                "BlendedCost": {"Amount": "95"},
                                "NetAmortizedCost": {"Amount": "88"},
                            },
                        },
                    ],
                }
            ]
        }
        mock_session_cls.return_value.client.return_value = mock_ce

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

    @patch("aws_cost_anomalies.utils.aws.boto3.Session")
    def test_profile_forwarded_to_session(self, mock_session_cls, db_conn):
        mock_ce = MagicMock()
        mock_ce.get_cost_and_usage.return_value = {
            "ResultsByTime": [
                {
                    "TimePeriod": {
                        "Start": "2025-01-01",
                        "End": "2025-01-02",
                    },
                    "Total": {
                        "UnblendedCost": {"Amount": "10", "Unit": "USD"},
                        "BlendedCost": {"Amount": "10", "Unit": "USD"},
                        "NetAmortizedCost": {"Amount": "9", "Unit": "USD"},
                    },
                }
            ]
        }
        mock_session_cls.return_value.client.return_value = mock_ce

        ctx = ToolContext(
            db_conn=db_conn, aws_region="us-east-1",
            aws_profile="root-readonly",
        )
        result = execute_tool(
            "get_cost_explorer_data",
            {
                "start_date": "2025-01-01",
                "end_date": "2025-01-02",
                "granularity": "DAILY",
            },
            ctx,
        )

        assert "error" not in result
        mock_session_cls.assert_called_once_with(
            profile_name="root-readonly"
        )


class TestCloudWatch:
    @patch("aws_cost_anomalies.utils.aws.boto3.Session")
    def test_describe_alarms(self, mock_session_cls, context):
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
        mock_session_cls.return_value.client.return_value = mock_cw

        result = execute_tool(
            "get_cloudwatch_metrics",
            {"action": "describe_alarms"},
            context,
        )

        assert "error" not in result
        assert result["count"] == 1
        assert result["alarms"][0]["name"] == "HighBilling"

    @patch("aws_cost_anomalies.utils.aws.boto3.Session")
    def test_unknown_action(self, mock_session_cls, context):
        mock_session_cls.return_value.client.return_value = MagicMock()
        result = execute_tool(
            "get_cloudwatch_metrics",
            {"action": "bad_action"},
            context,
        )
        assert "error" in result


class TestBudgetInfo:
    @patch("aws_cost_anomalies.utils.aws.boto3.Session")
    def test_describe_budgets(self, mock_session_cls, context):
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

        mock_session_cls.return_value.client.side_effect = _make_client

        result = execute_tool(
            "get_budget_info", {}, context
        )

        assert "error" not in result
        assert len(result["budgets"]) == 1
        assert result["budgets"][0]["name"] == "Monthly"
        assert result["budgets"][0]["actual_spend"] == "7500"


class TestOrganizationInfo:
    @patch("aws_cost_anomalies.utils.aws.boto3.Session")
    def test_list_accounts(self, mock_session_cls, context):
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
        mock_session_cls.return_value.client.return_value = mock_org

        result = execute_tool(
            "get_organization_info", {}, context
        )

        assert "error" not in result
        assert result["count"] == 2
        assert result["accounts"][0]["name"] == "Production"

    @patch("aws_cost_anomalies.utils.aws.boto3.Session")
    def test_describe_single_account(self, mock_session_cls, context):
        mock_org = MagicMock()
        mock_org.describe_account.return_value = {
            "Account": {
                "Id": "111111111111",
                "Name": "Production",
                "Email": "prod@example.com",
                "Status": "ACTIVE",
            }
        }
        mock_session_cls.return_value.client.return_value = mock_org

        result = execute_tool(
            "get_organization_info",
            {"account_id": "111111111111"},
            context,
        )

        assert "error" not in result
        assert result["account"]["name"] == "Production"


class TestDetectCostAnomalies:
    """Tests for the detect_cost_anomalies tool executor."""

    @pytest.fixture
    def anomaly_db(self):
        """DB with enough data to trigger anomaly detection."""
        from datetime import datetime, timedelta, timezone

        conn = duckdb.connect(":memory:")
        create_tables(conn)

        # 13 days of stable EC2 costs (~100/day), then a spike on day 14
        today = datetime.now(timezone.utc).date()
        for i in range(13, 0, -1):
            d = today - timedelta(days=i)
            conn.execute(
                "INSERT INTO daily_cost_summary VALUES "
                "(?, '111111111111', 'AmazonEC2', 'us-east-1', "
                "100.0, 95.0, 88.0, 50, 10, 'cur')",
                [d],
            )
        # Today: spike to 500
        conn.execute(
            "INSERT INTO daily_cost_summary VALUES "
            "(?, '111111111111', 'AmazonEC2', 'us-east-1', "
            "500.0, 480.0, 440.0, 200, 40, 'cur')",
            [today],
        )

        # 14 days of stable S3 costs (~20/day), no anomaly
        for i in range(13, -1, -1):
            d = today - timedelta(days=i)
            conn.execute(
                "INSERT INTO daily_cost_summary VALUES "
                "(?, '111111111111', 'AmazonS3', 'us-east-1', "
                "20.0, 19.0, 17.6, 1000, 5, 'cur')",
                [d],
            )
        return conn

    @pytest.fixture
    def anomaly_context(self, anomaly_db):
        return ToolContext(db_conn=anomaly_db, aws_region="us-east-1")

    def test_detects_spike(self, anomaly_context):
        result = execute_tool(
            "detect_cost_anomalies",
            {"days": 14, "sensitivity": "medium"},
            anomaly_context,
        )
        assert "error" not in result
        assert result["anomaly_count"] >= 1
        # The EC2 spike should be detected
        ec2_anomalies = [
            a for a in result["anomalies"]
            if a["group_value"] == "AmazonEC2"
            and a["kind"] == "point"
        ]
        assert len(ec2_anomalies) == 1
        assert ec2_anomalies[0]["direction"] == "spike"
        assert ec2_anomalies[0]["current_cost"] == 500.0

    def test_returns_summary(self, anomaly_context):
        result = execute_tool(
            "detect_cost_anomalies", {}, anomaly_context
        )
        assert "summary" in result
        assert "anomalies detected" in result["summary"]

    def test_returns_parameters(self, anomaly_context):
        result = execute_tool(
            "detect_cost_anomalies",
            {"days": 7, "sensitivity": "high", "group_by": ["product_code"]},
            anomaly_context,
        )
        assert result["parameters"]["days"] == 7
        assert result["parameters"]["sensitivity"] == "high"
        assert result["parameters"]["group_by"] == ["product_code"]

    def test_no_anomalies_in_stable_data(self):
        """Stable costs should produce no point anomalies."""
        from datetime import datetime, timedelta, timezone

        conn = duckdb.connect(":memory:")
        create_tables(conn)
        today = datetime.now(timezone.utc).date()
        for i in range(13, -1, -1):
            d = today - timedelta(days=i)
            conn.execute(
                "INSERT INTO daily_cost_summary VALUES "
                "(?, '111', 'AmazonEC2', 'us-east-1', "
                "100.0, 95.0, 88.0, 50, 10, 'cur')",
                [d],
            )
        ctx = ToolContext(db_conn=conn, aws_region="us-east-1")
        result = execute_tool("detect_cost_anomalies", {}, ctx)
        point_anomalies = [
            a for a in result["anomalies"] if a["kind"] == "point"
        ]
        assert len(point_anomalies) == 0

    def test_insufficient_data(self):
        """Fewer than 3 days should produce no anomalies."""
        from datetime import datetime, timezone

        conn = duckdb.connect(":memory:")
        create_tables(conn)
        conn.execute(
            "INSERT INTO daily_cost_summary VALUES "
            "(?, '111', 'AmazonEC2', 'us-east-1', "
            "100.0, 95.0, 88.0, 50, 10, 'cur')",
            [datetime.now(timezone.utc).date()],
        )
        ctx = ToolContext(db_conn=conn, aws_region="us-east-1")
        result = execute_tool("detect_cost_anomalies", {}, ctx)
        assert result["anomaly_count"] == 0

    def test_invalid_group_by(self, anomaly_context):
        result = execute_tool(
            "detect_cost_anomalies",
            {"group_by": ["invalid_column"]},
            anomaly_context,
        )
        assert "error" in result

    def test_multi_group_by(self, anomaly_context):
        result = execute_tool(
            "detect_cost_anomalies",
            {"group_by": ["product_code", "usage_account_id"]},
            anomaly_context,
        )
        assert "error" not in result
        assert result["parameters"]["group_by"] == [
            "product_code",
            "usage_account_id",
        ]

    def test_honours_config_defaults(self, anomaly_db):
        from aws_cost_anomalies.config.settings import (
            AnomalyConfig,
            Settings,
        )

        settings = Settings(
            anomaly=AnomalyConfig(
                rolling_window_days=7,
                z_score_threshold=3.0,
                min_daily_cost=5.0,
                drift_threshold_pct=30.0,
            )
        )
        ctx = ToolContext(
            db_conn=anomaly_db,
            aws_region="us-east-1",
            settings=settings,
        )
        # No explicit params — should use config defaults
        result = execute_tool("detect_cost_anomalies", {}, ctx)
        assert result["parameters"]["days"] == 7
        assert result["parameters"]["sensitivity"] == "low"


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
                total_net_amortized_cost=88.0,
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
