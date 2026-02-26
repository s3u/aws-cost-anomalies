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
        assert len(TOOL_DEFINITIONS) == 14
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
            "compare_periods",
            "drill_down_cost_spike",
            "scan_anomalies_over_range",
            "attribute_cost_change",
            "get_cost_trend",
            "explain_anomaly",
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
        assert ec2_anomalies[0]["current_cost"] == 440.0

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


class TestComparePeriods:
    """Tests for the compare_periods tool executor."""

    @pytest.fixture
    def comparison_db(self):
        """DB with data across two periods for comparison tests."""
        conn = duckdb.connect(":memory:")
        create_tables(conn)

        # Period A: Jan 1-7 — EC2 $100/day, S3 $20/day, RDS $50/day
        for day in range(1, 8):
            conn.execute(
                "INSERT INTO daily_cost_summary VALUES "
                "(?, '111111111111', 'AmazonEC2', 'us-east-1', "
                "100.0, 95.0, 88.0, 50, 10, 'cur')",
                [f"2025-01-{day:02d}"],
            )
            conn.execute(
                "INSERT INTO daily_cost_summary VALUES "
                "(?, '111111111111', 'AmazonS3', 'us-east-1', "
                "20.0, 19.0, 17.6, 1000, 5, 'cur')",
                [f"2025-01-{day:02d}"],
            )
            conn.execute(
                "INSERT INTO daily_cost_summary VALUES "
                "(?, '111111111111', 'AmazonRDS', 'us-east-1', "
                "50.0, 47.0, 44.0, 10, 3, 'cur')",
                [f"2025-01-{day:02d}"],
            )

        # Period B: Jan 8-14 — EC2 $200/day (doubled), S3 $20/day (same)
        # RDS gone, Lambda new at $30/day
        for day in range(8, 15):
            conn.execute(
                "INSERT INTO daily_cost_summary VALUES "
                "(?, '111111111111', 'AmazonEC2', 'us-east-1', "
                "200.0, 190.0, 176.0, 100, 20, 'cur')",
                [f"2025-01-{day:02d}"],
            )
            conn.execute(
                "INSERT INTO daily_cost_summary VALUES "
                "(?, '111111111111', 'AmazonS3', 'us-east-1', "
                "20.0, 19.0, 17.6, 1000, 5, 'cur')",
                [f"2025-01-{day:02d}"],
            )
            conn.execute(
                "INSERT INTO daily_cost_summary VALUES "
                "(?, '111111111111', 'AWSLambda', 'us-east-1', "
                "30.0, 28.0, 26.4, 5000, 15, 'cur')",
                [f"2025-01-{day:02d}"],
            )

        return conn

    @pytest.fixture
    def comparison_context(self, comparison_db):
        return ToolContext(db_conn=comparison_db, aws_region="us-east-1")

    def test_compare_periods_basic(self, comparison_context):
        result = execute_tool(
            "compare_periods",
            {
                "period_a_start": "2025-01-01",
                "period_a_end": "2025-01-07",
                "period_b_start": "2025-01-08",
                "period_b_end": "2025-01-14",
            },
            comparison_context,
        )
        assert "error" not in result

        # Period A: EC2 616 + S3 123.2 + RDS 308 = 1047.2 (net amortized)
        assert result["period_a"]["total_cost"] == 1047.2
        # Period B: EC2 1232 + S3 123.2 + Lambda 184.8 = 1540.0 (net amortized)
        assert result["period_b"]["total_cost"] == 1540.0

        # Total change
        assert result["total_change"]["absolute"] == 492.8
        assert result["total_change"]["percentage"] is not None

        # Movers should include EC2 (present in both, changed)
        mover_names = [m["group_value"] for m in result["top_movers"]]
        assert "AmazonEC2" in mover_names

        # Summary string present
        assert "summary" in result

    def test_compare_periods_new_and_disappeared(self, comparison_context):
        result = execute_tool(
            "compare_periods",
            {
                "period_a_start": "2025-01-01",
                "period_a_end": "2025-01-07",
                "period_b_start": "2025-01-08",
                "period_b_end": "2025-01-14",
            },
            comparison_context,
        )
        assert "error" not in result

        new_names = [n["group_value"] for n in result["new_in_period_b"]]
        assert "AWSLambda" in new_names

        gone_names = [g["group_value"] for g in result["gone_from_period_a"]]
        assert "AmazonRDS" in gone_names

    def test_compare_periods_invalid_dates(self, comparison_context):
        result = execute_tool(
            "compare_periods",
            {
                "period_a_start": "not-a-date",
                "period_a_end": "2025-01-07",
                "period_b_start": "2025-01-08",
                "period_b_end": "2025-01-14",
            },
            comparison_context,
        )
        assert "error" in result

    def test_compare_periods_empty_data(self):
        """No data in range returns zeros gracefully."""
        conn = duckdb.connect(":memory:")
        create_tables(conn)
        ctx = ToolContext(db_conn=conn, aws_region="us-east-1")
        result = execute_tool(
            "compare_periods",
            {
                "period_a_start": "2025-06-01",
                "period_a_end": "2025-06-07",
                "period_b_start": "2025-06-08",
                "period_b_end": "2025-06-14",
            },
            ctx,
        )
        assert "error" not in result
        assert result["period_a"]["total_cost"] == 0.0
        assert result["period_b"]["total_cost"] == 0.0
        assert result["top_movers"] == []


class TestDrillDownCostSpike:
    """Tests for the drill_down_cost_spike tool executor."""

    @pytest.fixture
    def drilldown_db(self):
        """DB with cost_line_items data for drill-down tests."""
        from datetime import date as d

        conn = duckdb.connect(":memory:")
        create_tables(conn)

        # Insert varied cost_line_items for AmazonEC2
        items = [
            # (usage_start, account, product, usage_type, operation, resource_id, cost, usage_amount)
            (d(2025, 1, 15), "111111111111", "AmazonEC2", "BoxUsage:m5.xlarge", "RunInstances", "i-abc123", 80.0, 24.0),
            (d(2025, 1, 15), "111111111111", "AmazonEC2", "BoxUsage:m5.xlarge", "RunInstances", "i-abc456", 60.0, 24.0),
            (d(2025, 1, 15), "111111111111", "AmazonEC2", "BoxUsage:c5.2xlarge", "RunInstances", "i-def789", 120.0, 24.0),
            (d(2025, 1, 15), "111111111111", "AmazonEC2", "EBS:VolumeUsage.gp3", "CreateVolume", "", 15.0, 500.0),
            (d(2025, 1, 15), "111111111111", "AmazonEC2", "DataTransfer-Out-Bytes", "InterZone-Out", "", 25.0, 100.0),
            # Different account
            (d(2025, 1, 15), "222222222222", "AmazonEC2", "BoxUsage:t3.micro", "RunInstances", "i-xyz111", 5.0, 24.0),
            # Different service (S3) — should not appear in EC2 drill-down
            (d(2025, 1, 15), "111111111111", "AmazonS3", "TimedStorage-ByteHrs", "StandardStorage", "", 10.0, 50000.0),
        ]
        for item in items:
            conn.execute(
                "INSERT INTO cost_line_items "
                "(usage_start_date, usage_account_id, product_code, "
                "usage_type, operation, resource_id, unblended_cost, "
                "net_unblended_cost, usage_amount, line_item_type) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'Usage')",
                [*item[:-1], item[-2], item[-1]],
            )
        return conn

    @pytest.fixture
    def drilldown_context(self, drilldown_db):
        return ToolContext(db_conn=drilldown_db, aws_region="us-east-1")

    def test_basic_drill_down(self, drilldown_context):
        result = execute_tool(
            "drill_down_cost_spike",
            {
                "service": "AmazonEC2",
                "date_start": "2025-01-15",
                "date_end": "2025-01-15",
            },
            drilldown_context,
        )
        assert "error" not in result
        assert result["service"] == "AmazonEC2"
        assert result["total_cost"] > 0

        # Should have usage_type breakdown
        assert len(result["breakdown_by_usage_type"]) > 0
        # BoxUsage types should dominate
        usage_types = [b["usage_type"] for b in result["breakdown_by_usage_type"]]
        assert any("BoxUsage" in ut for ut in usage_types)

        # Should have operation breakdown
        assert len(result["breakdown_by_operation"]) > 0

        # Should have top resources (only non-empty resource_ids)
        assert len(result["top_resources"]) > 0
        for r in result["top_resources"]:
            assert r["resource_id"] != ""

        # pct_of_total should add up reasonably
        total_pct = sum(b["pct_of_total"] for b in result["breakdown_by_usage_type"])
        assert 99.0 <= total_pct <= 101.0

        # Summary string present
        assert "summary" in result

    def test_no_cur_data_error(self, drilldown_context):
        result = execute_tool(
            "drill_down_cost_spike",
            {
                "service": "AmazonRDS",
                "date_start": "2025-01-15",
                "date_end": "2025-01-15",
            },
            drilldown_context,
        )
        assert "error" in result
        assert "No CUR data" in result["error"]

    def test_account_filter(self, drilldown_context):
        result = execute_tool(
            "drill_down_cost_spike",
            {
                "service": "AmazonEC2",
                "date_start": "2025-01-15",
                "date_end": "2025-01-15",
                "account_id": "111111111111",
            },
            drilldown_context,
        )
        assert "error" not in result
        assert result["account_id"] == "111111111111"
        # Total should be 80+60+120+15+25 = 300 (only account 111)
        assert result["total_cost"] == 300.0

    def test_invalid_dates(self, drilldown_context):
        result = execute_tool(
            "drill_down_cost_spike",
            {
                "service": "AmazonEC2",
                "date_start": "2025-01-20",
                "date_end": "2025-01-15",
            },
            drilldown_context,
        )
        assert "error" in result

    def test_invalid_date_format(self, drilldown_context):
        result = execute_tool(
            "drill_down_cost_spike",
            {
                "service": "AmazonEC2",
                "date_start": "not-a-date",
                "date_end": "2025-01-15",
            },
            drilldown_context,
        )
        assert "error" in result

    def test_missing_service(self, drilldown_context):
        result = execute_tool(
            "drill_down_cost_spike",
            {
                "date_start": "2025-01-15",
                "date_end": "2025-01-15",
            },
            drilldown_context,
        )
        assert "error" in result


class TestScanAnomaliesOverRange:
    """Tests for the scan_anomalies_over_range tool executor."""

    @pytest.fixture
    def scan_db(self):
        """DB with 30 days of data and a spike on day 15."""
        from datetime import date as d, timedelta

        conn = duckdb.connect(":memory:")
        create_tables(conn)

        base_date = d(2025, 1, 1)
        for day_offset in range(30):
            usage_date = base_date + timedelta(days=day_offset)
            # Stable EC2 at $100/day, spike to $500 on Jan 15
            is_spike = usage_date == d(2025, 1, 15)
            ec2_cost = 500.0 if is_spike else 100.0
            ec2_amortized = 440.0 if is_spike else 88.0
            conn.execute(
                "INSERT INTO daily_cost_summary VALUES "
                "(?, '111111111111', 'AmazonEC2', 'us-east-1', "
                "?, 95.0, ?, 50, 10, 'cur')",
                [usage_date, ec2_cost, ec2_amortized],
            )
            # Stable S3 at $20/day
            conn.execute(
                "INSERT INTO daily_cost_summary VALUES "
                "(?, '111111111111', 'AmazonS3', 'us-east-1', "
                "20.0, 19.0, 17.6, 1000, 5, 'cur')",
                [usage_date],
            )
        return conn

    @pytest.fixture
    def scan_context(self, scan_db):
        return ToolContext(db_conn=scan_db, aws_region="us-east-1")

    def test_finds_historical_spike(self, scan_context):
        result = execute_tool(
            "scan_anomalies_over_range",
            {
                "scan_start": "2025-01-10",
                "scan_end": "2025-01-20",
            },
            scan_context,
        )
        assert "error" not in result
        assert result["anomaly_count"] >= 1

        # Should find the EC2 spike
        ec2_anomalies = [
            a for a in result["anomalies"]
            if a["group_value"] == "AmazonEC2"
            and a["kind"] == "point"
        ]
        assert len(ec2_anomalies) >= 1
        assert ec2_anomalies[0]["direction"] == "spike"

        # Should report scan metadata
        assert result["days_scanned"] == 11
        assert "summary" in result

    def test_no_anomalies_in_flat_range(self, scan_context):
        result = execute_tool(
            "scan_anomalies_over_range",
            {
                "scan_start": "2025-01-20",
                "scan_end": "2025-01-28",
            },
            scan_context,
        )
        assert "error" not in result
        # No spike in this range, should find no point anomalies
        point_anomalies = [
            a for a in result["anomalies"] if a["kind"] == "point"
        ]
        assert len(point_anomalies) == 0

    def test_invalid_dates(self, scan_context):
        result = execute_tool(
            "scan_anomalies_over_range",
            {
                "scan_start": "2025-01-20",
                "scan_end": "2025-01-10",
            },
            scan_context,
        )
        assert "error" in result

    def test_invalid_date_format(self, scan_context):
        result = execute_tool(
            "scan_anomalies_over_range",
            {
                "scan_start": "bad-date",
                "scan_end": "2025-01-20",
            },
            scan_context,
        )
        assert "error" in result

    def test_summary_structure(self, scan_context):
        result = execute_tool(
            "scan_anomalies_over_range",
            {
                "scan_start": "2025-01-15",
                "scan_end": "2025-01-15",
                "sensitivity": "high",
                "group_by": ["product_code"],
            },
            scan_context,
        )
        assert "error" not in result
        assert result["scan_start"] == "2025-01-15"
        assert result["scan_end"] == "2025-01-15"
        assert result["days_scanned"] == 1
        assert result["parameters"]["sensitivity"] == "high"
        assert result["parameters"]["group_by"] == ["product_code"]


class TestAttributeCostChange:
    """Tests for the attribute_cost_change tool."""

    @pytest.fixture
    def attribution_db(self):
        """DB with CUR data across two periods for attribution tests.

        Period A (Jan 1-7): BoxUsage $80/day, EBS $15/day
        Period B (Jan 8-14): BoxUsage $160/day (doubled), SpotUsage $40/day (new), EBS gone
        """
        from datetime import date as d

        conn = duckdb.connect(":memory:")
        create_tables(conn)

        # Period A: Jan 1-7
        for day in range(1, 8):
            dt = d(2025, 1, day)
            conn.execute(
                "INSERT INTO cost_line_items "
                "(usage_start_date, usage_account_id, product_code, "
                "usage_type, operation, resource_id, unblended_cost, "
                "net_unblended_cost, usage_amount, line_item_type) "
                "VALUES (?, '111111111111', 'AmazonEC2', "
                "'BoxUsage:m5.xlarge', 'RunInstances', 'i-abc123', "
                "80.0, 80.0, 24.0, 'Usage')",
                [dt],
            )
            conn.execute(
                "INSERT INTO cost_line_items "
                "(usage_start_date, usage_account_id, product_code, "
                "usage_type, operation, resource_id, unblended_cost, "
                "net_unblended_cost, usage_amount, line_item_type) "
                "VALUES (?, '111111111111', 'AmazonEC2', "
                "'EBS:VolumeUsage.gp3', 'CreateVolume', 'vol-001', "
                "15.0, 15.0, 500.0, 'Usage')",
                [dt],
            )

        # Period B: Jan 8-14
        for day in range(8, 15):
            dt = d(2025, 1, day)
            # BoxUsage doubled
            conn.execute(
                "INSERT INTO cost_line_items "
                "(usage_start_date, usage_account_id, product_code, "
                "usage_type, operation, resource_id, unblended_cost, "
                "net_unblended_cost, usage_amount, line_item_type) "
                "VALUES (?, '111111111111', 'AmazonEC2', "
                "'BoxUsage:m5.xlarge', 'RunInstances', 'i-abc123', "
                "160.0, 160.0, 48.0, 'Usage')",
                [dt],
            )
            # New SpotUsage
            conn.execute(
                "INSERT INTO cost_line_items "
                "(usage_start_date, usage_account_id, product_code, "
                "usage_type, operation, resource_id, unblended_cost, "
                "net_unblended_cost, usage_amount, line_item_type) "
                "VALUES (?, '111111111111', 'AmazonEC2', "
                "'SpotUsage:c5.xlarge', 'RunInstances', 'i-spot001', "
                "40.0, 40.0, 24.0, 'Usage')",
                [dt],
            )
            # EBS gone — no rows in period B

        # Add data for account 222 (small, for filter test)
        conn.execute(
            "INSERT INTO cost_line_items "
            "(usage_start_date, usage_account_id, product_code, "
            "usage_type, operation, resource_id, unblended_cost, "
            "net_unblended_cost, usage_amount, line_item_type) "
            "VALUES ('2025-01-01', '222222222222', 'AmazonEC2', "
            "'BoxUsage:t3.micro', 'RunInstances', 'i-dev001', "
            "5.0, 5.0, 24.0, 'Usage')"
        )

        return conn

    @pytest.fixture
    def attribution_context(self, attribution_db):
        return ToolContext(db_conn=attribution_db, aws_region="us-east-1")

    def test_basic_attribution(self, attribution_context):
        result = execute_tool(
            "attribute_cost_change",
            {
                "service": "AmazonEC2",
                "period_a_start": "2025-01-01",
                "period_a_end": "2025-01-07",
                "period_b_start": "2025-01-08",
                "period_b_end": "2025-01-14",
            },
            attribution_context,
        )
        assert "error" not in result
        assert result["service"] == "AmazonEC2"
        assert result["period_a"]["total_cost"] > 0
        assert result["period_b"]["total_cost"] > result["period_a"]["total_cost"]
        assert result["total_change"]["absolute"] > 0
        assert "summary" in result

    def test_new_and_disappeared(self, attribution_context):
        result = execute_tool(
            "attribute_cost_change",
            {
                "service": "AmazonEC2",
                "period_a_start": "2025-01-01",
                "period_a_end": "2025-01-07",
                "period_b_start": "2025-01-08",
                "period_b_end": "2025-01-14",
            },
            attribution_context,
        )
        assert "error" not in result
        by_ut = result["by_usage_type"]

        # SpotUsage should be new in period B
        new_keys = [item["key"] for item in by_ut["new"]]
        assert any("Spot" in k for k in new_keys)

        # EBS should have disappeared from period A
        gone_keys = [item["key"] for item in by_ut["disappeared"]]
        assert any("EBS" in k for k in gone_keys)

        # BoxUsage should be a mover
        mover_keys = [item["key"] for item in by_ut["movers"]]
        assert any("Box" in k for k in mover_keys)

    def test_no_cur_data_error(self, attribution_context):
        result = execute_tool(
            "attribute_cost_change",
            {
                "service": "AmazonRDS",
                "period_a_start": "2025-01-01",
                "period_a_end": "2025-01-07",
                "period_b_start": "2025-01-08",
                "period_b_end": "2025-01-14",
            },
            attribution_context,
        )
        assert "error" in result
        assert "No CUR data" in result["error"]

    def test_account_filter(self, attribution_context):
        result = execute_tool(
            "attribute_cost_change",
            {
                "service": "AmazonEC2",
                "period_a_start": "2025-01-01",
                "period_a_end": "2025-01-07",
                "period_b_start": "2025-01-08",
                "period_b_end": "2025-01-14",
                "account_id": "111111111111",
            },
            attribution_context,
        )
        assert "error" not in result
        assert result["period_a"]["total_cost"] > 0

    def test_invalid_dates(self, attribution_context):
        result = execute_tool(
            "attribute_cost_change",
            {
                "service": "AmazonEC2",
                "period_a_start": "not-a-date",
                "period_a_end": "2025-01-07",
                "period_b_start": "2025-01-08",
                "period_b_end": "2025-01-14",
            },
            attribution_context,
        )
        assert "error" in result

    def test_missing_service(self, attribution_context):
        result = execute_tool(
            "attribute_cost_change",
            {
                "service": "",
                "period_a_start": "2025-01-01",
                "period_a_end": "2025-01-07",
                "period_b_start": "2025-01-08",
                "period_b_end": "2025-01-14",
            },
            attribution_context,
        )
        assert "error" in result
        assert "service is required" in result["error"]


class TestGetCostTrend:
    """Tests for the get_cost_trend tool — reuses comparison_db (Jan 1-14)."""

    @pytest.fixture
    def comparison_db(self):
        """DB with data across two periods for trend tests."""
        conn = duckdb.connect(":memory:")
        create_tables(conn)

        # Jan 1-7: EC2 $100/day, S3 $20/day
        for day in range(1, 8):
            conn.execute(
                "INSERT INTO daily_cost_summary VALUES "
                "(?, '111111111111', 'AmazonEC2', 'us-east-1', "
                "100.0, 95.0, 88.0, 50, 10, 'cur')",
                [f"2025-01-{day:02d}"],
            )
            conn.execute(
                "INSERT INTO daily_cost_summary VALUES "
                "(?, '111111111111', 'AmazonS3', 'us-east-1', "
                "20.0, 19.0, 17.6, 1000, 5, 'cur')",
                [f"2025-01-{day:02d}"],
            )

        # Jan 8-14: EC2 $200/day, S3 $20/day
        for day in range(8, 15):
            conn.execute(
                "INSERT INTO daily_cost_summary VALUES "
                "(?, '111111111111', 'AmazonEC2', 'us-east-1', "
                "200.0, 190.0, 176.0, 100, 20, 'cur')",
                [f"2025-01-{day:02d}"],
            )
            conn.execute(
                "INSERT INTO daily_cost_summary VALUES "
                "(?, '111111111111', 'AmazonS3', 'us-east-1', "
                "20.0, 19.0, 17.6, 1000, 5, 'cur')",
                [f"2025-01-{day:02d}"],
            )

        return conn

    @pytest.fixture
    def trend_context(self, comparison_db):
        return ToolContext(db_conn=comparison_db, aws_region="us-east-1")

    def test_basic_daily_trend(self, trend_context):
        result = execute_tool(
            "get_cost_trend",
            {"date_start": "2025-01-01", "date_end": "2025-01-14"},
            trend_context,
        )
        assert "error" not in result
        assert result["granularity"] == "daily"
        assert len(result["points"]) == 14
        assert result["stats"]["total"] > 0
        assert result["stats"]["min"] <= result["stats"]["max"]
        assert "summary" in result

    def test_grouped_trend(self, trend_context):
        result = execute_tool(
            "get_cost_trend",
            {
                "date_start": "2025-01-01",
                "date_end": "2025-01-14",
                "group_by": "product_code",
            },
            trend_context,
        )
        assert "error" not in result
        # Should have points for both EC2 and S3
        groups = {p["group"] for p in result["points"]}
        assert "AmazonEC2" in groups
        assert "AmazonS3" in groups

    def test_filtered_trend(self, trend_context):
        result = execute_tool(
            "get_cost_trend",
            {
                "date_start": "2025-01-01",
                "date_end": "2025-01-14",
                "group_by": "product_code",
                "filter_value": "AmazonEC2",
            },
            trend_context,
        )
        assert "error" not in result
        # All points should be EC2
        for p in result["points"]:
            assert p["group"] == "AmazonEC2"
        assert result["filter_value"] == "AmazonEC2"

    def test_weekly_granularity(self, trend_context):
        result = execute_tool(
            "get_cost_trend",
            {
                "date_start": "2025-01-01",
                "date_end": "2025-01-14",
                "granularity": "weekly",
            },
            trend_context,
        )
        assert "error" not in result
        assert result["granularity"] == "weekly"
        # Should have fewer points than daily
        assert len(result["points"]) < 14

    def test_empty_range(self, trend_context):
        result = execute_tool(
            "get_cost_trend",
            {"date_start": "2024-06-01", "date_end": "2024-06-30"},
            trend_context,
        )
        assert "error" not in result
        assert len(result["points"]) == 0
        assert result["stats"]["total"] == 0

    def test_filter_without_group_by_error(self, trend_context):
        result = execute_tool(
            "get_cost_trend",
            {
                "date_start": "2025-01-01",
                "date_end": "2025-01-14",
                "filter_value": "AmazonEC2",
            },
            trend_context,
        )
        assert "error" in result
        assert "group_by" in result["error"]

    def test_invalid_dates(self, trend_context):
        result = execute_tool(
            "get_cost_trend",
            {"date_start": "bad-date", "date_end": "2025-01-14"},
            trend_context,
        )
        assert "error" in result

    def test_invalid_granularity(self, trend_context):
        result = execute_tool(
            "get_cost_trend",
            {
                "date_start": "2025-01-01",
                "date_end": "2025-01-14",
                "granularity": "hourly",
            },
            trend_context,
        )
        assert "error" in result


class TestExplainAnomaly:
    """Tests for the explain_anomaly tool."""

    @pytest.fixture
    def explainer_db(self):
        """DB with 14 days stable, spike on Jan 15, then 3 days normal.

        Daily cost summary: $100/day for Jan 1-14, $500 on Jan 15, $100 for Jan 16-18.
        CUR data on Jan 15: BoxUsage $350, EBS $100, DataTransfer $50.
        CUR data on Jan 1-14: BoxUsage $70/day, EBS $20/day, DataTransfer $10/day.
        """
        from datetime import date as d

        conn = duckdb.connect(":memory:")
        create_tables(conn)

        # Baseline: Jan 1-14 at $100/day
        for day in range(1, 15):
            dt = d(2025, 1, day)
            conn.execute(
                "INSERT INTO daily_cost_summary VALUES "
                "(?, '111111111111', 'AmazonEC2', 'us-east-1', "
                "100.0, 95.0, 88.0, 50, 10, 'cur')",
                [dt],
            )
            # CUR line items for baseline
            conn.execute(
                "INSERT INTO cost_line_items "
                "(usage_start_date, usage_account_id, product_code, "
                "usage_type, operation, resource_id, unblended_cost, "
                "net_unblended_cost, usage_amount, line_item_type) "
                "VALUES (?, '111111111111', 'AmazonEC2', "
                "'BoxUsage:m5.xlarge', 'RunInstances', 'i-abc123', "
                "70.0, 70.0, 24.0, 'Usage')",
                [dt],
            )
            conn.execute(
                "INSERT INTO cost_line_items "
                "(usage_start_date, usage_account_id, product_code, "
                "usage_type, operation, resource_id, unblended_cost, "
                "net_unblended_cost, usage_amount, line_item_type) "
                "VALUES (?, '111111111111', 'AmazonEC2', "
                "'EBS:VolumeUsage.gp3', 'CreateVolume', 'vol-001', "
                "20.0, 20.0, 500.0, 'Usage')",
                [dt],
            )
            conn.execute(
                "INSERT INTO cost_line_items "
                "(usage_start_date, usage_account_id, product_code, "
                "usage_type, operation, resource_id, unblended_cost, "
                "net_unblended_cost, usage_amount, line_item_type) "
                "VALUES (?, '111111111111', 'AmazonEC2', "
                "'DataTransfer-Out-Bytes', 'InterZone-Out', '', "
                "10.0, 10.0, 100.0, 'Usage')",
                [dt],
            )

        # Spike day: Jan 15 at $500
        conn.execute(
            "INSERT INTO daily_cost_summary VALUES "
            "('2025-01-15', '111111111111', 'AmazonEC2', 'us-east-1', "
            "500.0, 475.0, 440.0, 250, 50, 'cur')"
        )
        # CUR line items for spike day
        conn.execute(
            "INSERT INTO cost_line_items "
            "(usage_start_date, usage_account_id, product_code, "
            "usage_type, operation, resource_id, unblended_cost, "
            "net_unblended_cost, usage_amount, line_item_type) "
            "VALUES ('2025-01-15', '111111111111', 'AmazonEC2', "
            "'BoxUsage:m5.xlarge', 'RunInstances', 'i-abc123', "
            "350.0, 350.0, 120.0, 'Usage')"
        )
        conn.execute(
            "INSERT INTO cost_line_items "
            "(usage_start_date, usage_account_id, product_code, "
            "usage_type, operation, resource_id, unblended_cost, "
            "net_unblended_cost, usage_amount, line_item_type) "
            "VALUES ('2025-01-15', '111111111111', 'AmazonEC2', "
            "'EBS:VolumeUsage.gp3', 'CreateVolume', 'vol-001', "
            "100.0, 100.0, 2500.0, 'Usage')"
        )
        conn.execute(
            "INSERT INTO cost_line_items "
            "(usage_start_date, usage_account_id, product_code, "
            "usage_type, operation, resource_id, unblended_cost, "
            "net_unblended_cost, usage_amount, line_item_type) "
            "VALUES ('2025-01-15', '111111111111', 'AmazonEC2', "
            "'DataTransfer-Out-Bytes', 'InterZone-Out', '', "
            "50.0, 50.0, 500.0, 'Usage')"
        )

        # After spike: Jan 16-18 back to normal $100
        for day in range(16, 19):
            dt = d(2025, 1, day)
            conn.execute(
                "INSERT INTO daily_cost_summary VALUES "
                "(?, '111111111111', 'AmazonEC2', 'us-east-1', "
                "100.0, 95.0, 88.0, 50, 10, 'cur')",
                [dt],
            )

        return conn

    @pytest.fixture
    def explainer_context(self, explainer_db):
        return ToolContext(db_conn=explainer_db, aws_region="us-east-1")

    def test_basic_explanation(self, explainer_context):
        result = execute_tool(
            "explain_anomaly",
            {"service": "AmazonEC2", "anomaly_date": "2025-01-15"},
            explainer_context,
        )
        assert "error" not in result
        assert result["service"] == "AmazonEC2"
        assert result["anomaly_cost"] == 440.0
        assert result["baseline"]["median_cost"] == 88.0
        assert result["cost_multiple"] == 5.0
        assert result["cost_vs_median"] == 352.0
        assert result["has_baseline"] is True
        assert "summary" in result

    def test_ongoing_detection(self, explainer_context):
        """Costs returned to normal, so is_ongoing should be False."""
        result = execute_tool(
            "explain_anomaly",
            {"service": "AmazonEC2", "anomaly_date": "2025-01-15"},
            explainer_context,
        )
        assert "error" not in result
        assert result["is_ongoing"] is False
        assert result["days_after_checked"] == 3
        assert result["elevated_days_after"] == 0

    def test_cur_usage_type_changes(self, explainer_context):
        result = execute_tool(
            "explain_anomaly",
            {"service": "AmazonEC2", "anomaly_date": "2025-01-15"},
            explainer_context,
        )
        assert "error" not in result
        assert result["has_cur_data"] is True
        changes = result["top_usage_type_changes"]
        assert len(changes) > 0
        # BoxUsage should be the biggest change
        ut_names = [c["usage_type"] for c in changes]
        assert any("Box" in ut for ut in ut_names)

    def test_no_cur_graceful(self, explainer_context):
        """explain_anomaly should work without CUR data (summary only)."""
        # Jan 1 has both summary and CUR data, but we test a service
        # that only has summary data
        conn = explainer_context.db_conn
        conn.execute(
            "INSERT INTO daily_cost_summary VALUES "
            "('2025-01-15', '111111111111', 'AmazonRDS', 'us-east-1', "
            "200.0, 190.0, 176.0, 50, 10, 'cur')"
        )
        # Add baseline for RDS
        from datetime import date as d
        for day in range(1, 15):
            conn.execute(
                "INSERT INTO daily_cost_summary VALUES "
                "(?, '111111111111', 'AmazonRDS', 'us-east-1', "
                "50.0, 47.5, 44.0, 10, 3, 'cur')",
                [d(2025, 1, day)],
            )

        result = execute_tool(
            "explain_anomaly",
            {"service": "AmazonRDS", "anomaly_date": "2025-01-15"},
            explainer_context,
        )
        assert "error" not in result
        assert result["has_cur_data"] is False
        assert result["top_usage_type_changes"] == []
        assert result["anomaly_cost"] == 176.0

    def test_no_data_error(self, explainer_context):
        result = execute_tool(
            "explain_anomaly",
            {"service": "AWSLambda", "anomaly_date": "2025-01-15"},
            explainer_context,
        )
        assert "error" in result
        assert "No data" in result["error"]

    def test_account_filter(self, explainer_context):
        result = execute_tool(
            "explain_anomaly",
            {
                "service": "AmazonEC2",
                "anomaly_date": "2025-01-15",
                "account_id": "111111111111",
            },
            explainer_context,
        )
        assert "error" not in result
        assert result["account_id"] == "111111111111"

    def test_invalid_date(self, explainer_context):
        result = execute_tool(
            "explain_anomaly",
            {"service": "AmazonEC2", "anomaly_date": "not-a-date"},
            explainer_context,
        )
        assert "error" in result


class TestUnknownTool:
    def test_unknown_tool_returns_error(self, context):
        result = execute_tool("nonexistent_tool", {}, context)
        assert "error" in result
        assert "Unknown tool" in result["error"]
