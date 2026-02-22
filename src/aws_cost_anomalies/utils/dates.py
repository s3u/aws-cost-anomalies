"""Date range helpers."""

from __future__ import annotations

from datetime import date, timedelta


def date_range(start: date, end: date) -> list[date]:
    """Return a list of dates from start to end inclusive."""
    days = (end - start).days
    return [start + timedelta(days=i) for i in range(days + 1)]


def month_start(d: date) -> date:
    """Return the first day of the month for the given date."""
    return d.replace(day=1)


def billing_period_str(d: date) -> str:
    """Format date as CUR billing period string YYYYMM01-YYYYMM01."""
    start = month_start(d)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return f"{start.strftime('%Y%m%d')}-{end.strftime('%Y%m%d')}"
