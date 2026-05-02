"""Aggregation logic for audience retention buckets.

Pure functions — no DB, no Streamlit, no API. Easy to test in isolation.
"""

from datetime import date, timedelta

import pandas as pd

# (rolling_window_days, snapshot_days) per toggle.
# rolling_window_days = which retention_buckets row to read (7/90/365).
# snapshot_days = how far back to scope view counts.
_TOGGLE_WINDOWS = {
    "Last week":    (7, 7),
    "Last month":   (90, 30),
    "Last quarter": (90, 90),
    "Last year":    (365, 365),
}


def bucket_shares(r25: float, r75: float) -> tuple[float, float, float]:
    """Convert retention ratios into (b1, b2, b3) shares that sum to <=1.

    b1 = viewers who dropped before 25%      = 1 - r25
    b2 = viewers who reached 25% but not 75% = r25 - r75
    b3 = viewers who reached 75%             = r75
    """
    r25 = min(max(r25, 0.0), 1.0)
    r75 = min(max(r75, 0.0), 1.0)
    if r75 > r25:
        r75 = r25
    return (1.0 - r25, r25 - r75, r75)


def aggregate_snapshot(rows: pd.DataFrame) -> dict:
    """Aggregate per-video retention rows into channel-wide bucket totals.

    `rows` must have columns: video_id, views, retention_at_25, retention_at_75.
    Returns counts and percentages for the three buckets.
    """
    if rows.empty:
        return {
            "total_views": 0,
            "b1_count": 0.0, "b2_count": 0.0, "b3_count": 0.0,
            "b1_pct": 0.0, "b2_pct": 0.0, "b3_pct": 0.0,
        }

    shares = rows.apply(
        lambda r: bucket_shares(r["retention_at_25"], r["retention_at_75"]),
        axis=1, result_type="expand",
    )
    shares.columns = ["s1", "s2", "s3"]
    counts = shares.multiply(rows["views"].values, axis=0)
    total = int(rows["views"].sum())
    b1, b2, b3 = float(counts["s1"].sum()), float(counts["s2"].sum()), float(counts["s3"].sum())
    return {
        "total_views": total,
        "b1_count": b1, "b2_count": b2, "b3_count": b3,
        "b1_pct": b1 / total if total else 0.0,
        "b2_pct": b2 / total if total else 0.0,
        "b3_pct": b3 / total if total else 0.0,
    }


def window_bounds_for_toggle(toggle: str, today: date) -> tuple[date, date, int]:
    """Resolve a range_picker label to (snapshot_start, snapshot_end, rolling_window_days).

    snapshot_start..snapshot_end is the range we scope view counts to.
    rolling_window_days picks which retention_buckets row to read (7/90/365).
    """
    if toggle not in _TOGGLE_WINDOWS:
        raise ValueError(f"Unknown toggle: {toggle}")
    rolling_days, snapshot_days = _TOGGLE_WINDOWS[toggle]
    snapshot_start = today - timedelta(days=snapshot_days)
    return snapshot_start, today, rolling_days
