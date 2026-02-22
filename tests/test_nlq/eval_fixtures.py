"""Shared fixtures and helpers for NLQ agent evals.

Provides an in-memory DuckDB loaded with deterministic cost data
(30 days × 3 accounts × 5 services × 2 regions) and pre-computed
reference values for assertions.
"""

from __future__ import annotations

import re
from datetime import date, timedelta

import duckdb
import pytest

from aws_cost_anomalies.nlq.agent import AgentResponse
from aws_cost_anomalies.storage.database import get_connection
from aws_cost_anomalies.storage.schema import create_tables

# ---------------------------------------------------------------------------
# Deterministic cost schedule (account, service, region → daily base cost)
# ---------------------------------------------------------------------------

ACCOUNTS = ["111111111111", "222222222222", "333333333333"]
SERVICES = ["AmazonEC2", "AmazonS3", "AmazonRDS", "AWSLambda", "AmazonCloudFront"]
REGIONS = ["us-east-1", "us-west-2"]

# Base daily cost per (account_index, service)
# Production (111...) is most expensive, Development (333...) is cheapest
_BASE_COSTS: dict[tuple[str, str], float] = {
    # Production — heavy EC2/RDS
    ("111111111111", "AmazonEC2"): 150.00,
    ("111111111111", "AmazonS3"): 30.00,
    ("111111111111", "AmazonRDS"): 80.00,
    ("111111111111", "AWSLambda"): 10.00,
    ("111111111111", "AmazonCloudFront"): 25.00,
    # Staging — moderate
    ("222222222222", "AmazonEC2"): 60.00,
    ("222222222222", "AmazonS3"): 15.00,
    ("222222222222", "AmazonRDS"): 40.00,
    ("222222222222", "AWSLambda"): 8.00,
    ("222222222222", "AmazonCloudFront"): 12.00,
    # Development — light
    ("333333333333", "AmazonEC2"): 25.00,
    ("333333333333", "AmazonS3"): 5.00,
    ("333333333333", "AmazonRDS"): 15.00,
    ("333333333333", "AWSLambda"): 3.00,
    ("333333333333", "AmazonCloudFront"): 4.00,
}

# us-east-1 gets 60% of cost, us-west-2 gets 40%
_REGION_WEIGHTS = {"us-east-1": 0.6, "us-west-2": 0.4}

START_DATE = date(2025, 1, 1)
END_DATE = date(2025, 1, 30)
NUM_DAYS = 30

# ---------------------------------------------------------------------------
# Pre-compute reference values
# ---------------------------------------------------------------------------


def _compute_all_costs() -> list[tuple[date, str, str, str, float]]:
    """Generate all (usage_date, account, service, region, cost) tuples."""
    rows = []
    for day_offset in range(NUM_DAYS):
        usage_date = START_DATE + timedelta(days=day_offset)
        for acct in ACCOUNTS:
            for svc in SERVICES:
                base = _BASE_COSTS[(acct, svc)]
                for region in REGIONS:
                    cost = round(base * _REGION_WEIGHTS[region], 2)
                    rows.append((usage_date, acct, svc, region, cost))
    return rows


_ALL_COSTS = _compute_all_costs()

# Expected reference values
TOTAL_COST = sum(r[4] for r in _ALL_COSTS)

# Per-service totals (exported as public dict)
SERVICE_TOTALS: dict[str, float] = {}
for _, _, svc, _, cost in _ALL_COSTS:
    SERVICE_TOTALS[svc] = SERVICE_TOTALS.get(svc, 0.0) + cost
TOP_SERVICE = max(SERVICE_TOTALS, key=lambda s: SERVICE_TOTALS[s])  # AmazonEC2

# Per-account totals (exported as public dict)
ACCOUNT_TOTALS: dict[str, float] = {}
for _, acct, _, _, cost in _ALL_COSTS:
    ACCOUNT_TOTALS[acct] = ACCOUNT_TOTALS.get(acct, 0.0) + cost
TOP_ACCOUNT = max(ACCOUNT_TOTALS, key=lambda a: ACCOUNT_TOTALS[a])  # 111111111111

DAILY_AVG = TOTAL_COST / NUM_DAYS

# Per-region totals (exported as public dict)
REGION_TOTALS: dict[str, float] = {}
for _, _, _, region, cost in _ALL_COSTS:
    REGION_TOTALS[region] = REGION_TOTALS.get(region, 0.0) + cost

# Per-account per-service totals
ACCOUNT_SERVICE_TOTALS: dict[tuple[str, str], float] = {}
for _, acct, svc, _, cost in _ALL_COSTS:
    key = (acct, svc)
    ACCOUNT_SERVICE_TOTALS[key] = ACCOUNT_SERVICE_TOTALS.get(key, 0.0) + cost

# EC2 by account × region
EC2_ACCOUNT_REGION_TOTALS: dict[tuple[str, str], float] = {}
for _, acct, svc, region, cost in _ALL_COSTS:
    if svc == "AmazonEC2":
        key = (acct, region)
        EC2_ACCOUNT_REGION_TOTALS[key] = (
            EC2_ACCOUNT_REGION_TOTALS.get(key, 0.0) + cost
        )


# ---------------------------------------------------------------------------
# Pytest fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def eval_db() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with 30 days of deterministic cost data.

    Data goes directly into daily_cost_summary (no line items needed).
    """
    conn = get_connection(":memory:")
    create_tables(conn)

    rows = [
        (usage_date, acct, svc, region, cost, cost * 0.95, cost * 10, 1)
        for usage_date, acct, svc, region, cost in _ALL_COSTS
    ]

    conn.executemany(
        """INSERT INTO daily_cost_summary
           (usage_date, usage_account_id, product_code, region,
            total_unblended_cost, total_blended_cost,
            total_usage_amount, line_item_count)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    return conn


# ---------------------------------------------------------------------------
# Assertion helpers
# ---------------------------------------------------------------------------


def assert_used_tool(response: AgentResponse, tool_name: str) -> None:
    """Assert the agent called a specific tool at least once."""
    tool_names = [s.tool_name for s in response.steps]
    assert tool_name in tool_names, (
        f"Expected tool '{tool_name}' to be used, "
        f"but agent used: {tool_names}"
    )


def assert_answer_contains(response: AgentResponse, *keywords: str) -> None:
    """Assert the answer contains all given keywords (case-insensitive)."""
    lower_answer = response.answer.lower()
    for kw in keywords:
        assert kw.lower() in lower_answer, (
            f"Expected keyword '{kw}' in answer, got:\n{response.answer[:500]}"
        )


def assert_no_tool_errors(response: AgentResponse) -> None:
    """Assert no tool steps returned an error dict."""
    for step in response.steps:
        if step.tool_result and "error" in step.tool_result:
            raise AssertionError(
                f"Tool '{step.tool_name}' returned error: "
                f"{step.tool_result['error']}"
            )


def assert_valid_response(response: AgentResponse) -> None:
    """Assert basic response validity: non-empty answer, token usage > 0."""
    assert response.answer, "Agent returned an empty answer"
    assert response.input_tokens > 0, "Expected input_tokens > 0"
    assert response.output_tokens > 0, "Expected output_tokens > 0"


def _extract_dollar_amounts(text: str) -> list[float]:
    """Extract all dollar amounts from text.

    Handles formats like: $14,460.00, $14460, $14,460, 14460.00, etc.
    """
    # Match $-prefixed amounts: $14,460.00 or $14460
    dollar_pattern = r"\$\s*([\d,]+(?:\.\d{1,2})?)"
    matches = re.findall(dollar_pattern, text)
    amounts = []
    for m in matches:
        cleaned = m.replace(",", "")
        try:
            amounts.append(float(cleaned))
        except ValueError:
            continue
    return amounts


def assert_cost_in_answer(
    response: AgentResponse,
    expected: float,
    tolerance: float = 0.05,
) -> None:
    """Assert the answer contains a dollar amount within ±tolerance of expected."""
    amounts = _extract_dollar_amounts(response.answer)
    lo = expected * (1 - tolerance)
    hi = expected * (1 + tolerance)
    match = any(lo <= a <= hi for a in amounts)
    assert match, (
        f"Expected a dollar amount within ±{tolerance:.0%} of ${expected:,.2f} "
        f"(range ${lo:,.2f}–${hi:,.2f}), "
        f"but found amounts: {['${:,.2f}'.format(a) for a in amounts]}\n"
        f"Answer: {response.answer[:500]}"
    )


def assert_ranking(response: AgentResponse, *ordered_items: str) -> None:
    """Assert items appear in order in the answer (first = highest ranked).

    Finds the first occurrence of each item and verifies they appear
    in ascending positional order (i.e. earlier in text = higher ranked).
    """
    lower_answer = response.answer.lower()
    positions: list[tuple[str, int]] = []
    for item in ordered_items:
        pos = lower_answer.find(item.lower())
        assert pos != -1, (
            f"Expected '{item}' to appear in answer, but it was not found.\n"
            f"Answer: {response.answer[:500]}"
        )
        positions.append((item, pos))
    for i in range(len(positions) - 1):
        item_a, pos_a = positions[i]
        item_b, pos_b = positions[i + 1]
        assert pos_a < pos_b, (
            f"Expected '{item_a}' (pos {pos_a}) to appear before "
            f"'{item_b}' (pos {pos_b}) in the answer.\n"
            f"Answer: {response.answer[:500]}"
        )
