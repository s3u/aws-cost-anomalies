"""Tests for date utility functions."""

from __future__ import annotations

from datetime import date

from aws_cost_anomalies.utils.dates import (
    billing_period_str,
    date_range,
    month_start,
)


def test_date_range_single_day():
    result = date_range(date(2025, 1, 1), date(2025, 1, 1))
    assert result == [date(2025, 1, 1)]


def test_date_range_multiple_days():
    result = date_range(date(2025, 1, 1), date(2025, 1, 3))
    assert result == [
        date(2025, 1, 1),
        date(2025, 1, 2),
        date(2025, 1, 3),
    ]


def test_date_range_across_months():
    result = date_range(date(2025, 1, 30), date(2025, 2, 2))
    assert len(result) == 4
    assert result[0] == date(2025, 1, 30)
    assert result[-1] == date(2025, 2, 2)


def test_month_start():
    assert month_start(date(2025, 3, 15)) == date(2025, 3, 1)
    assert month_start(date(2025, 1, 1)) == date(2025, 1, 1)
    assert month_start(date(2025, 12, 31)) == date(2025, 12, 1)


def test_billing_period_str_regular():
    result = billing_period_str(date(2025, 3, 15))
    assert result == "20250301-20250401"


def test_billing_period_str_january():
    result = billing_period_str(date(2025, 1, 10))
    assert result == "20250101-20250201"


def test_billing_period_str_december():
    result = billing_period_str(date(2025, 12, 5))
    assert result == "20251201-20260101"
