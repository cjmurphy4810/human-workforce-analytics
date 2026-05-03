"""Linear growth projections from recent daily metrics.

Pure functions — no DB, no Streamlit, no API. Mirrors retention.py for testability.
"""

import pandas as pd


def linear_daily_rates(daily_channel: pd.DataFrame, lookback_days: int = 30) -> dict:
    """Average daily rate over the most recent `lookback_days` of daily_channel.

    Returns views_per_day, net_subs_per_day, hours_per_day (all floats).
    Falls back to whatever rows exist if fewer than `lookback_days` available.
    """
    if daily_channel.empty:
        return {"views_per_day": 0.0, "net_subs_per_day": 0.0, "hours_per_day": 0.0}

    df = daily_channel.copy()
    df["metric_date"] = pd.to_datetime(df["metric_date"])
    cutoff = df["metric_date"].max() - pd.Timedelta(days=lookback_days)
    recent = df[df["metric_date"] > cutoff]

    if recent.empty:
        return {"views_per_day": 0.0, "net_subs_per_day": 0.0, "hours_per_day": 0.0}

    return {
        "views_per_day": float(recent["views"].mean()),
        "net_subs_per_day": float(
            (recent["subscribers_gained"] - recent["subscribers_lost"]).mean()
        ),
        "hours_per_day": float((recent["estimated_minutes_watched"] / 60).mean()),
    }


def project(current: dict, rates: dict, horizon_days: int) -> dict:
    """Linear projection: total at horizon = current + rate * horizon.

    `current` keys: subscribers, views, hours.
    `rates`   keys: views_per_day, net_subs_per_day, hours_per_day.
    Returns projected_* and delta_* for each metric.
    """
    delta_subs = rates["net_subs_per_day"] * horizon_days
    delta_views = rates["views_per_day"] * horizon_days
    delta_hours = rates["hours_per_day"] * horizon_days
    return {
        "projected_subscribers": int(round(current["subscribers"] + delta_subs)),
        "projected_views": int(round(current["views"] + delta_views)),
        "projected_hours": int(round(current["hours"] + delta_hours)),
        "delta_subscribers": int(round(delta_subs)),
        "delta_views": int(round(delta_views)),
        "delta_hours": int(round(delta_hours)),
    }
