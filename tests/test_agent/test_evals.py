"""Agent evals — correctness tests against real Bedrock.

These evals verify the agent produces **numerically correct** answers
by asserting against pre-computed golden values from the deterministic
cost fixture (30 days × 3 accounts × 5 services × 2 regions).

Run with:  pytest -m evals -v
Skip by default when running plain `pytest`.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from aws_cost_anomalies.agent import AgentError, run_agent

from .eval_fixtures import (
    ACCOUNT_SERVICE_TOTALS,
    ACCOUNT_TOTALS,
    ACCOUNTS,
    DAILY_AVG,
    EC2_ACCOUNT_REGION_TOTALS,
    REGION_TOTALS,
    SERVICE_TOTALS,
    SERVICES,
    SPIKE_ACCOUNT,
    SPIKE_SERVICE,
    TOTAL_COST,
    assert_answer_contains,
    assert_cost_in_answer,
    assert_ranking,
    assert_used_tool,
    assert_valid_response,
    eval_db,  # noqa: F401 — pytest fixture import
    eval_db_recent,  # noqa: F401
    eval_db_with_cur_data,  # noqa: F401
    eval_db_with_historical_spike,  # noqa: F401
    eval_db_with_spike,  # noqa: F401
)

# Re-export fixtures so pytest discovers them in this module
__all__ = [
    "eval_db",
    "eval_db_recent",
    "eval_db_with_cur_data",
    "eval_db_with_historical_spike",
    "eval_db_with_spike",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Mock out AWS API tools so evals only hit Bedrock + local DuckDB.
_AWS_TOOL_ERROR = {
    "error": "AWS API unavailable in eval mode. Use query_cost_database instead."
}


def _mock_aws_tool(tool_input: dict, context: object) -> dict:
    return _AWS_TOOL_ERROR


@pytest.fixture(autouse=True)
def _patch_aws_tools():
    """Prevent evals from calling real AWS APIs (Cost Explorer, ingestion, etc.)."""
    with (
        patch(
            "aws_cost_anomalies.agent.tools._execute_cost_explorer",
            side_effect=_mock_aws_tool,
        ),
        patch(
            "aws_cost_anomalies.agent.tools._execute_cloudwatch",
            side_effect=_mock_aws_tool,
        ),
        patch(
            "aws_cost_anomalies.agent.tools._execute_budget_info",
            side_effect=_mock_aws_tool,
        ),
        patch(
            "aws_cost_anomalies.agent.tools._execute_organization_info",
            side_effect=_mock_aws_tool,
        ),
        patch(
            "aws_cost_anomalies.agent.tools._execute_ingest_cost_explorer",
            side_effect=_mock_aws_tool,
        ),
        patch(
            "aws_cost_anomalies.agent.tools._execute_ingest_cur_data",
            side_effect=_mock_aws_tool,
        ),
    ):
        yield


def _run(question: str, eval_db):  # noqa: F811
    """Run the agent, skipping if Bedrock credentials are unavailable."""
    try:
        return run_agent(question, eval_db)
    except AgentError as e:
        msg = str(e).lower()
        if "credential" in msg or "security token" in msg or "not authorized" in msg:
            pytest.skip(f"Bedrock credentials unavailable: {e}")
        raise


# ============================================================================
# Eval 1: Total Spend
# ============================================================================


@pytest.mark.evals
class TestTotalSpend:
    """Verify the agent reports the correct total spend."""

    def test_total_spend(self, eval_db):  # noqa: F811
        """Total spend for January 2025 must be ≈ $14,460."""
        response = _run("What is my total spend for January 2025?", eval_db)

        assert_valid_response(response)
        assert_used_tool(response, "query_cost_database")
        assert_cost_in_answer(response, TOTAL_COST)


# ============================================================================
# Eval 2: Top Services (Ranking)
# ============================================================================


@pytest.mark.evals
class TestTopServices:
    """Verify service ranking and amounts."""

    def test_top_services(self, eval_db):  # noqa: F811
        """Top 5 services: EC2 #1, RDS #2, correct amounts."""
        response = _run(
            "What are my top 5 most expensive services?", eval_db
        )

        assert_valid_response(response)
        assert_used_tool(response, "query_cost_database")


        # All 5 services should be mentioned
        mentioned = sum(
            1
            for s in SERVICES
            if s.lower() in response.answer.lower()
            or s.replace("Amazon", "").lower() in response.answer.lower()
            or s.replace("AWS", "").lower() in response.answer.lower()
        )
        assert mentioned >= 5, (
            f"Expected all 5 services mentioned, found {mentioned}.\n"
            f"Answer: {response.answer[:500]}"
        )

        # EC2 must be ranked above RDS
        assert_ranking(response, "EC2", "RDS")

        # Verify top two dollar amounts
        assert_cost_in_answer(response, SERVICE_TOTALS["AmazonEC2"])
        assert_cost_in_answer(response, SERVICE_TOTALS["AmazonRDS"])


# ============================================================================
# Eval 3: Cost by Account
# ============================================================================


@pytest.mark.evals
class TestCostByAccount:
    """Verify per-account costs and ranking."""

    def test_cost_by_account(self, eval_db):  # noqa: F811
        """All 3 accounts present; 111... highest, 333... lowest."""
        response = _run("Show me costs broken down by account", eval_db)

        assert_valid_response(response)
        assert_used_tool(response, "query_cost_database")


        # All 3 account IDs should appear
        for acct in ACCOUNTS:
            assert acct in response.answer, (
                f"Expected account {acct} in answer.\n"
                f"Answer: {response.answer[:500]}"
            )

        # Prod > Staging > Dev ordering
        assert_ranking(response, "111111111111", "222222222222", "333333333333")

        # Verify dollar amounts for each account
        for acct in ACCOUNTS:
            assert_cost_in_answer(response, ACCOUNT_TOTALS[acct])


# ============================================================================
# Eval 4: Region Comparison
# ============================================================================


@pytest.mark.evals
class TestRegionComparison:
    """Verify region costs and relative ordering."""

    def test_region_comparison(self, eval_db):  # noqa: F811
        """us-east-1 ≈ $8,676, us-west-2 ≈ $5,784; east higher."""
        response = _run(
            "Compare costs between us-east-1 and us-west-2", eval_db
        )

        assert_valid_response(response)
        assert_used_tool(response, "query_cost_database")

        assert_answer_contains(response, "us-east-1", "us-west-2")

        # us-east-1 must be mentioned first (higher cost)
        assert_ranking(response, "us-east-1", "us-west-2")

        # Verify both region totals
        assert_cost_in_answer(response, REGION_TOTALS["us-east-1"])
        assert_cost_in_answer(response, REGION_TOTALS["us-west-2"])


# ============================================================================
# Eval 5: Service Drilldown (EC2 by Account × Region)
# ============================================================================


@pytest.mark.evals
class TestServiceDrilldown:
    """Verify EC2 breakdown by account and region."""

    def test_ec2_drilldown(self, eval_db):  # noqa: F811
        """6 cells (3 accounts × 2 regions); 111.../us-east-1 ≈ $2,700."""
        response = _run(
            "Break down EC2 costs by account and region", eval_db
        )

        assert_valid_response(response)
        assert_used_tool(response, "query_cost_database")

        assert_answer_contains(response, "EC2")

        # All 3 accounts should be mentioned
        for acct in ACCOUNTS:
            assert acct in response.answer, (
                f"Expected account {acct} in EC2 drilldown.\n"
                f"Answer: {response.answer[:500]}"
            )

        # Largest cell: 111.../us-east-1
        assert_cost_in_answer(
            response, EC2_ACCOUNT_REGION_TOTALS[("111111111111", "us-east-1")]
        )


# ============================================================================
# Eval 6: Specific Account Services
# ============================================================================


@pytest.mark.evals
class TestSpecificAccount:
    """Verify service breakdown for a specific account."""

    def test_account_222_services(self, eval_db):  # noqa: F811
        """Account 222: all 5 services, EC2 highest ($1,800), Lambda lowest ($240)."""
        response = _run(
            "What services does account 222222222222 use and how much does each cost?",
            eval_db,
        )

        assert_valid_response(response)
        assert_used_tool(response, "query_cost_database")

        assert_answer_contains(response, "222222222222")

        # EC2 should be ranked above Lambda for this account
        assert_ranking(response, "EC2", "Lambda")

        # Verify top service amount (EC2 = $1,800) and bottom (Lambda = $240)
        assert_cost_in_answer(
            response, ACCOUNT_SERVICE_TOTALS[("222222222222", "AmazonEC2")]
        )
        assert_cost_in_answer(
            response, ACCOUNT_SERVICE_TOTALS[("222222222222", "AWSLambda")]
        )


# ============================================================================
# Eval 7: Daily Trend
# ============================================================================


@pytest.mark.evals
class TestDailyTrend:
    """Verify the agent recognizes flat/stable daily costs."""

    def test_daily_trend(self, eval_db):  # noqa: F811
        """Daily costs are flat at ≈ $482/day; no trend should be reported."""
        response = _run(
            "Show me the daily cost trend for January 2025", eval_db
        )

        assert_valid_response(response)
        assert_used_tool(response, "query_cost_database")


        # Daily amount should be approximately $482
        assert_cost_in_answer(response, DAILY_AVG)

        # Agent should NOT report an increasing or decreasing trend.
        # Negated uses ("no spike", "without any surge") are fine.
        answer_lower = response.answer.lower()
        trend_words = ["increasing", "decreasing", "spike", "surge", "drop", "jumped"]
        negation_prefixes = ["no ", "no\n", "zero ", "without ", "any ", "absent ", "not ", "n't "]
        found_trend = []
        for word in trend_words:
            pos = answer_lower.find(word)
            if pos == -1:
                continue
            # Check if preceded by a negation within 40 chars
            preceding = answer_lower[max(0, pos - 40):pos]
            if any(neg in preceding for neg in negation_prefixes):
                continue
            found_trend.append(word)
        assert not found_trend, (
            f"Agent reported a trend ({found_trend}) but costs are flat.\n"
            f"Answer: {response.answer[:500]}"
        )


# ============================================================================
# Eval 8: No Data Range
# ============================================================================


@pytest.mark.evals
class TestNoDataRange:
    """Verify the agent does not hallucinate costs for empty date ranges."""

    def test_no_data_march_2024(self, eval_db):  # noqa: F811
        """March 2024 has no data; agent must not invent dollar amounts."""
        response = _run("What were my costs in March 2024?", eval_db)

        assert_valid_response(response)
        assert_used_tool(response, "query_cost_database")

        # Should acknowledge no data
        answer_lower = response.answer.lower()
        no_data_indicators = [
            "no data",
            "no cost",
            "no record",
            "no result",
            "not available",
            "not found",
            "no spend",
            "no information",
            "$0",
            "zero",
            "0.00",
            "don't have",
            "do not have",
            "doesn't",
            "empty",
            "unavailable",
            "cannot retrieve",
            "can't retrieve",
            "only contains",
            "beyond",
            "outside",
            "no entries",
        ]
        has_no_data = any(ind in answer_lower for ind in no_data_indicators)
        assert has_no_data, (
            f"Expected agent to report no data for March 2024.\n"
            f"Answer: {response.answer[:500]}"
        )


# ============================================================================
# Eval 9: Day-over-Day Change
# ============================================================================


@pytest.mark.evals
class TestDayOverDayChange:
    """Verify the agent recognizes zero day-over-day change."""

    def test_no_change(self, eval_db):  # noqa: F811
        """Costs are flat; agent should not name a specific account with a big increase."""
        response = _run(
            "Which account had the biggest day-over-day cost increase?",
            eval_db,
        )

        assert_valid_response(response)
        assert_used_tool(response, "query_cost_database")


        # Agent should indicate no significant change
        answer_lower = response.answer.lower()
        stable_indicators = [
            "no significant",
            "no change",
            "zero change",
            "no increase",
            "stable",
            "consistent",
            "flat",
            "constant",
            "same",
            "identical",
            "no variation",
            "unchanged",
            "no difference",
            "none of the accounts",
            "did not change",
            "didn't change",
            "no day-over-day",
            "0%",
            "$0",
            "0.00",
            "no notable",
            "no major",
            "no substantial",
        ]
        has_stable = any(ind in answer_lower for ind in stable_indicators)
        assert has_stable, (
            f"Expected agent to report no significant day-over-day change "
            f"(costs are flat), but it didn't use any stability indicator.\n"
            f"Answer: {response.answer[:500]}"
        )


# ============================================================================
# Eval 10: Cross-check (Total = Sum of Parts)
# ============================================================================


@pytest.mark.evals
class TestCrossCheck:
    """Verify service totals add up to the grand total."""

    def test_service_totals_sum(self, eval_db):  # noqa: F811
        """5 service totals should each be correct and sum to ≈ $14,460."""
        response = _run(
            "What is the total cost per service, and what do they add up to?",
            eval_db,
        )

        assert_valid_response(response)
        assert_used_tool(response, "query_cost_database")


        # Grand total must appear
        assert_cost_in_answer(response, TOTAL_COST)

        # At least the top 2 individual service totals should be correct
        assert_cost_in_answer(response, SERVICE_TOTALS["AmazonEC2"])
        assert_cost_in_answer(response, SERVICE_TOTALS["AmazonRDS"])


# ============================================================================
# Eval 11: No Anomalies (flat recent data)
# ============================================================================


@pytest.mark.evals
class TestNoAnomalies:
    """Verify the agent uses detect_cost_anomalies and reports nothing on flat data."""

    def test_no_anomalies(self, eval_db_recent):  # noqa: F811
        """Flat costs should produce no anomalies; agent must not invent any."""
        response = _run("Are there any cost anomalies?", eval_db_recent)

        assert_valid_response(response)
        assert_used_tool(response, "detect_cost_anomalies")

        # Agent should indicate no anomalies found
        answer_lower = response.answer.lower()
        no_anomaly_indicators = [
            "no anomal",
            "no cost anomal",
            "no significant",
            "no unusual",
            "not detect",
            "not find",
            "not identify",
            "didn't detect",
            "didn't find",
            "didn't identify",
            "none detected",
            "none found",
            "none identified",
            "no spike",
            "no drift",
            "0 anomal",
            "zero anomal",
            "stable",
            "normal",
            "no issues",
        ]
        has_no_anomaly = any(ind in answer_lower for ind in no_anomaly_indicators)
        assert has_no_anomaly, (
            f"Expected agent to report no anomalies on flat data.\n"
            f"Answer: {response.answer[:500]}"
        )


# ============================================================================
# Eval 12: Spike Detection
# ============================================================================


@pytest.mark.evals
class TestSpikeDetection:
    """Verify the agent detects a cost spike and names the correct service."""

    def test_detects_ec2_spike(self, eval_db_with_spike):  # noqa: F811
        """EC2 spike on the last day should be detected as a critical anomaly."""
        response = _run(
            "Are there any cost anomalies?", eval_db_with_spike
        )

        assert_valid_response(response)
        assert_used_tool(response, "detect_cost_anomalies")

        # Must mention EC2 as the anomalous service
        answer_lower = response.answer.lower()
        assert "ec2" in answer_lower, (
            f"Expected 'EC2' in anomaly answer.\n"
            f"Answer: {response.answer[:500]}"
        )

        # Must describe it as a spike, increase, or anomaly
        spike_indicators = [
            "spike",
            "spik",
            "surge",
            "increase",
            "jump",
            "anomal",
            "unusual",
            "critical",
            "higher",
            "elevated",
            "above",
        ]
        has_spike_language = any(ind in answer_lower for ind in spike_indicators)
        assert has_spike_language, (
            f"Expected spike/anomaly language about EC2.\n"
            f"Answer: {response.answer[:500]}"
        )


# ============================================================================
# Eval 13: Anomaly Drilldown (identify responsible account)
# ============================================================================


@pytest.mark.evals
class TestAnomalyDrilldown:
    """Verify the agent can identify which account caused the anomaly."""

    def test_identifies_responsible_account(self, eval_db_with_spike):  # noqa: F811
        """Agent should detect the EC2 spike and attribute it to account 111."""
        response = _run(
            "Are there any cost anomalies? If so, which account is responsible?",
            eval_db_with_spike,
        )

        assert_valid_response(response)
        assert_used_tool(response, "detect_cost_anomalies")

        # Must mention the spiking service
        assert_answer_contains(response, "EC2")

        # Must identify the responsible account
        assert SPIKE_ACCOUNT in response.answer, (
            f"Expected account {SPIKE_ACCOUNT} identified as responsible.\n"
            f"Answer: {response.answer[:500]}"
        )


# ============================================================================
# Eval 14: Drill-Down Cost Spike
# ============================================================================


@pytest.mark.evals
class TestDrillDownCostSpike:
    """Verify the agent drills into a cost spike using usage_type/resource detail."""

    def test_drill_down_ec2_spike(self, eval_db_with_cur_data):  # noqa: F811
        """Agent should use drill_down_cost_spike and mention usage type details."""
        response = _run(
            "EC2 costs spiked today. Drill down to show what usage types "
            "or resources caused it.",
            eval_db_with_cur_data,
        )

        assert_valid_response(response)
        assert_used_tool(response, "drill_down_cost_spike")

        # Must mention EC2
        assert_answer_contains(response, "EC2")

        # Should mention usage type details (BoxUsage, EBS, DataTransfer, etc.)
        answer_lower = response.answer.lower()
        usage_indicators = [
            "boxusage", "usage type", "usage_type",
            "ebs", "datatransfer", "runinstances",
            "m5", "c5", "volume",
        ]
        has_detail = any(ind in answer_lower for ind in usage_indicators)
        assert has_detail, (
            f"Expected usage type or resource detail in drill-down answer.\n"
            f"Answer: {response.answer[:500]}"
        )


# ============================================================================
# Eval 15: Scan Anomalies Over Range
# ============================================================================


@pytest.mark.evals
class TestScanAnomaliesOverRange:
    """Verify the agent can find historical anomalies using scan."""

    def test_finds_january_anomaly(self, eval_db_with_historical_spike):  # noqa: F811
        """Agent should use scan_anomalies_over_range and find the Jan 15 spike."""
        response = _run(
            "Were there any cost anomalies during January 2025?",
            eval_db_with_historical_spike,
        )

        assert_valid_response(response)
        assert_used_tool(response, "scan_anomalies_over_range")

        # Must mention EC2
        assert_answer_contains(response, "EC2")

        # Should reference the spike around Jan 15
        answer_lower = response.answer.lower()
        date_indicators = [
            "jan 15", "january 15", "2025-01-15", "15th",
            "mid-jan", "mid jan", "middle of jan",
        ]
        has_date = any(ind in answer_lower for ind in date_indicators)
        assert has_date, (
            f"Expected reference to the spike around Jan 15.\n"
            f"Answer: {response.answer[:500]}"
        )


# ============================================================================
# Eval 16: Attribute Cost Change
# ============================================================================


@pytest.mark.evals
class TestAttributeCostChange:
    """Verify the agent uses attribute_cost_change to explain what changed."""

    def test_attribute_ec2_spike(self, eval_db_with_cur_data):  # noqa: F811
        """Agent should attribute EC2 cost changes between periods."""
        response = _run(
            "EC2 costs spiked today compared to the previous week. "
            "What specific usage types or resources changed?",
            eval_db_with_cur_data,
        )

        assert_valid_response(response)

        # Should use attribution or drill-down tool
        tool_names = [s.tool_name for s in response.steps]
        used_attribution = "attribute_cost_change" in tool_names
        used_drilldown = "drill_down_cost_spike" in tool_names
        assert used_attribution or used_drilldown, (
            f"Expected attribute_cost_change or drill_down_cost_spike, "
            f"but agent used: {tool_names}"
        )

        # Must mention EC2
        assert_answer_contains(response, "EC2")

        # Should mention usage type details
        answer_lower = response.answer.lower()
        detail_indicators = [
            "boxusage", "usage type", "usage_type",
            "ebs", "datatransfer", "spot",
            "new", "changed", "increased", "doubled",
        ]
        has_detail = any(ind in answer_lower for ind in detail_indicators)
        assert has_detail, (
            f"Expected usage type change details in attribution answer.\n"
            f"Answer: {response.answer[:500]}"
        )


# ============================================================================
# Eval 17: Get Cost Trend
# ============================================================================


@pytest.mark.evals
class TestGetCostTrend:
    """Verify the agent uses get_cost_trend for time-series questions."""

    def test_ec2_daily_trend(self, eval_db):  # noqa: F811
        """Agent should show the daily cost trend for EC2 in January."""
        response = _run(
            "Show me the daily cost trend for EC2 in January 2025.",
            eval_db,
        )

        assert_valid_response(response)

        # Should use trend tool or query_cost_database
        tool_names = [s.tool_name for s in response.steps]
        used_trend = "get_cost_trend" in tool_names
        used_query = "query_cost_database" in tool_names
        assert used_trend or used_query, (
            f"Expected get_cost_trend or query_cost_database, "
            f"but agent used: {tool_names}"
        )

        # Must mention EC2
        assert_answer_contains(response, "EC2")

        # Should have dollar amounts
        assert_cost_in_answer(
            response, SERVICE_TOTALS["AmazonEC2"] / 30, tolerance=0.15
        )


# ============================================================================
# Eval 18: Explain Anomaly
# ============================================================================


@pytest.mark.evals
class TestExplainAnomaly:
    """Verify the agent uses explain_anomaly to build a narrative."""

    def test_explain_ec2_anomaly(self, eval_db_with_spike):  # noqa: F811
        """Agent should explain the EC2 anomaly with baseline comparison."""
        response = _run(
            "There's an EC2 cost anomaly today. Explain what happened — "
            "how does today compare to normal, and is it ongoing?",
            eval_db_with_spike,
        )

        assert_valid_response(response)

        # Should use explain_anomaly or detect_cost_anomalies
        tool_names = [s.tool_name for s in response.steps]
        used_explain = "explain_anomaly" in tool_names
        used_detect = "detect_cost_anomalies" in tool_names
        assert used_explain or used_detect, (
            f"Expected explain_anomaly or detect_cost_anomalies, "
            f"but agent used: {tool_names}"
        )

        # Must mention EC2
        assert_answer_contains(response, "EC2")

        # Should describe the magnitude (spike, multiple, higher, etc.)
        answer_lower = response.answer.lower()
        magnitude_indicators = [
            "spike", "increase", "higher", "above", "elevated",
            "times", "multiple", "5x", "5×", "500%",
            "compared to", "baseline", "normal", "median",
        ]
        has_magnitude = any(ind in answer_lower for ind in magnitude_indicators)
        assert has_magnitude, (
            f"Expected magnitude/comparison language in anomaly explanation.\n"
            f"Answer: {response.answer[:500]}"
        )
