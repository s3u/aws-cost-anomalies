"""Tests for daily trend aggregation."""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

from aws_cost_anomalies.analysis.trends import get_daily_trends, get_total_daily_costs
from aws_cost_anomalies.storage.schema import rebuild_daily_summary


def test_get_daily_trends_by_service(db_with_data):
    rebuild_daily_summary(db_with_data)

    # Patch date.today() to be within our test data range
    with patch("aws_cost_anomalies.analysis.trends.date") as mock_date:
        mock_date.today.return_value = date(2025, 1, 14)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

        trends = get_daily_trends(db_with_data, days=14, group_by="product_code", top_n=3)

    assert len(trends) > 0
    # Should have entries for EC2, S3, and RDS
    services = {t.group_value for t in trends}
    assert "AmazonEC2" in services
    assert "AmazonS3" in services


def test_get_daily_trends_by_account(db_with_data):
    rebuild_daily_summary(db_with_data)

    with patch("aws_cost_anomalies.analysis.trends.date") as mock_date:
        mock_date.today.return_value = date(2025, 1, 14)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

        trends = get_daily_trends(db_with_data, days=14, group_by="usage_account_id", top_n=5)

    accounts = {t.group_value for t in trends}
    assert "111111111111" in accounts
    assert "222222222222" in accounts


def test_get_daily_trends_empty(db):
    rebuild_daily_summary(db)

    trends = get_daily_trends(db, days=14, group_by="product_code")
    assert trends == []


def test_get_daily_trends_invalid_group(db):
    import pytest

    with pytest.raises(ValueError, match="group_by must be one of"):
        get_daily_trends(db, group_by="invalid_column")


def test_get_total_daily_costs(db_with_data):
    rebuild_daily_summary(db_with_data)

    with patch("aws_cost_anomalies.analysis.trends.date") as mock_date:
        mock_date.today.return_value = date(2025, 1, 14)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

        totals = get_total_daily_costs(db_with_data, days=14)

    assert len(totals) > 0
    # Each entry is (date, total_cost)
    for d, cost in totals:
        assert isinstance(d, date)
        assert cost > 0


def test_trend_rows_have_change_values(db_with_data):
    rebuild_daily_summary(db_with_data)

    with patch("aws_cost_anomalies.analysis.trends.date") as mock_date:
        mock_date.today.return_value = date(2025, 1, 14)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

        trends = get_daily_trends(db_with_data, days=14, group_by="product_code", top_n=1)

    # First row for each group should have None change (no previous day)
    # Subsequent rows should have numeric change values
    has_change = any(t.cost_change is not None for t in trends)
    assert has_change
