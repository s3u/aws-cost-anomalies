"""Tests for formatting helpers."""

from __future__ import annotations

from datetime import date

from aws_cost_anomalies.analysis.anomalies import Anomaly
from aws_cost_anomalies.analysis.trends import TrendRow
from aws_cost_anomalies.cli.formatting import (
    format_currency,
    format_pct,
    print_anomalies_table,
    print_query_results,
    print_trends_table,
)


def test_format_currency_positive():
    assert format_currency(1234.56) == "$1,234.56"


def test_format_currency_zero():
    assert format_currency(0.0) == "$0.00"


def test_format_currency_negative():
    assert format_currency(-50.0) == "$-50.00"


def test_format_currency_none():
    assert format_currency(None) == "\u2014"  # em-dash


def test_format_pct_positive():
    assert format_pct(25.0) == "+25.0%"


def test_format_pct_negative():
    assert format_pct(-10.5) == "-10.5%"


def test_format_pct_zero():
    assert format_pct(0.0) == "0.0%"


def test_format_pct_none():
    assert format_pct(None) == "\u2014"


def test_print_anomalies_table_empty(capsys):
    # Should print "No anomalies" message, not crash
    print_anomalies_table([])


def test_print_anomalies_table_with_data():
    anomalies = [
        Anomaly(
            usage_date=date(2025, 1, 14),
            group_by="product_code",
            group_value="AmazonEC2",
            current_cost=500.0,
            median_cost=100.0,
            mad=5.0,
            z_score=80.0,
            severity="critical",
            direction="spike",
            kind="point",
        ),
        Anomaly(
            usage_date=date(2025, 1, 14),
            group_by="product_code",
            group_value="AmazonEC2",
            current_cost=140.0,
            median_cost=100.0,
            mad=2.0,
            z_score=0.40,
            severity="info",
            direction="drift_up",
            kind="trend",
        ),
    ]
    # Just ensure it doesn't crash
    print_anomalies_table(anomalies)


def test_print_trends_table():
    rows = [
        TrendRow(
            usage_date=date(2025, 1, 1),
            group_value="AmazonEC2",
            total_cost=100.0,
            cost_change=None,
            pct_change=None,
        ),
        TrendRow(
            usage_date=date(2025, 1, 2),
            group_value="AmazonEC2",
            total_cost=120.0,
            cost_change=20.0,
            pct_change=20.0,
        ),
    ]
    # Just ensure it doesn't crash
    print_trends_table(rows, "Service")


def test_print_query_results_empty():
    print_query_results(["col1"], [])


def test_print_query_results_with_data():
    columns = ["service", "cost"]
    rows = [("AmazonEC2", 100.5), ("AmazonS3", None)]
    print_query_results(columns, rows)
