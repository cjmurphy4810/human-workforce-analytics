import pandas as pd
import pytest

import projections


def _frame(rows):
    return pd.DataFrame(rows, columns=[
        "metric_date", "views", "estimated_minutes_watched",
        "subscribers_gained", "subscribers_lost",
    ])


def test_linear_daily_rates_averages_last_n_days():
    df = _frame([
        ("2026-04-01", 100, 60, 10, 2),   # outside 30-day window
        ("2026-04-15", 200, 120, 20, 5),  # inside
        ("2026-04-30", 300, 180, 30, 5),  # inside
    ])
    rates = projections.linear_daily_rates(df, lookback_days=30)
    # cutoff = 2026-04-30 - 30d = 2026-03-31; all three rows are after that.
    # Adjust: only inside-window rows are 2026-04-15 and 2026-04-30 if cutoff drops earliest.
    # With 30-day window from max=2026-04-30, cutoff=2026-03-31 strict>; all three rows kept.
    assert rates["views_per_day"] == pytest.approx((100 + 200 + 300) / 3)
    assert rates["hours_per_day"] == pytest.approx((60 + 120 + 180) / 3 / 60)
    assert rates["net_subs_per_day"] == pytest.approx(((10-2) + (20-5) + (30-5)) / 3)


def test_linear_daily_rates_excludes_old_rows():
    df = _frame([
        ("2026-01-01", 9999, 9999, 9999, 0),  # very old, must be excluded
        ("2026-04-20", 100, 60, 10, 0),
        ("2026-04-25", 200, 120, 20, 0),
    ])
    rates = projections.linear_daily_rates(df, lookback_days=30)
    assert rates["views_per_day"] == pytest.approx(150.0)


def test_linear_daily_rates_empty_frame():
    rates = projections.linear_daily_rates(pd.DataFrame(), lookback_days=30)
    assert rates == {"views_per_day": 0.0, "net_subs_per_day": 0.0, "hours_per_day": 0.0}


def test_project_adds_delta_to_current():
    current = {"subscribers": 1000, "views": 50_000, "hours": 800}
    rates = {"views_per_day": 100.0, "net_subs_per_day": 5.0, "hours_per_day": 10.0}
    out = projections.project(current, rates, horizon_days=30)
    assert out["delta_subscribers"] == 150
    assert out["delta_views"] == 3000
    assert out["delta_hours"] == 300
    assert out["projected_subscribers"] == 1150
    assert out["projected_views"] == 53_000
    assert out["projected_hours"] == 1100


def test_project_zero_rates_returns_current():
    current = {"subscribers": 100, "views": 1000, "hours": 50}
    rates = {"views_per_day": 0.0, "net_subs_per_day": 0.0, "hours_per_day": 0.0}
    out = projections.project(current, rates, horizon_days=365)
    assert out["projected_subscribers"] == 100
    assert out["projected_views"] == 1000
    assert out["projected_hours"] == 50
    assert out["delta_subscribers"] == 0
