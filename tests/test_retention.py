import pandas as pd
import pytest

import retention


def test_bucket_shares_basic():
    """r25=0.6, r75=0.3 → b1=0.4, b2=0.3, b3=0.3."""
    shares = retention.bucket_shares(r25=0.6, r75=0.3)
    assert shares == pytest.approx((0.4, 0.3, 0.3))


def test_bucket_shares_caps_above_one():
    """A r25 > 1.0 (shouldn't happen, but defensive) is capped."""
    shares = retention.bucket_shares(r25=1.2, r75=0.5)
    assert shares == pytest.approx((0.0, 0.5, 0.5))


def test_bucket_shares_floors_negative():
    """If r75 > r25 (data anomaly), b2 cannot be negative — clamp r75 down to r25."""
    shares = retention.bucket_shares(r25=0.4, r75=0.5)
    b1, b2, b3 = shares
    assert b1 == pytest.approx(0.6)
    assert b2 == 0.0
    assert b3 == pytest.approx(0.4)
    assert sum(shares) == pytest.approx(1.0)


def test_aggregate_snapshot_sums_across_videos():
    """Two videos contribute their bucket counts; totals sum."""
    rows = pd.DataFrame([
        {"video_id": "v1", "views": 1000, "retention_at_25": 0.6, "retention_at_75": 0.3},
        {"video_id": "v2", "views": 500,  "retention_at_25": 0.8, "retention_at_75": 0.5},
    ])
    snap = retention.aggregate_snapshot(rows)
    assert snap["b1_count"] == pytest.approx(500.0)
    assert snap["b2_count"] == pytest.approx(450.0)
    assert snap["b3_count"] == pytest.approx(550.0)
    assert snap["total_views"] == 1500
    assert snap["b1_pct"] == pytest.approx(500 / 1500)
    assert snap["b2_pct"] == pytest.approx(450 / 1500)
    assert snap["b3_pct"] == pytest.approx(550 / 1500)


def test_aggregate_snapshot_empty_returns_zeros():
    """Empty input returns all zeros, not a divide-by-zero."""
    snap = retention.aggregate_snapshot(pd.DataFrame(columns=[
        "video_id", "views", "retention_at_25", "retention_at_75"
    ]))
    assert snap["total_views"] == 0
    assert snap["b1_count"] == 0
    assert snap["b1_pct"] == 0.0
    assert snap["b2_pct"] == 0.0
    assert snap["b3_pct"] == 0.0


def test_window_bounds_for_toggle():
    """Each toggle resolves to (window_start, window_end, days_back) for the snapshot."""
    today = pd.Timestamp("2026-05-02").date()
    assert retention.window_bounds_for_toggle("Last week", today=today) == (
        pd.Timestamp("2026-04-25").date(), today, 7,
    )
    assert retention.window_bounds_for_toggle("Last month", today=today) == (
        pd.Timestamp("2026-04-02").date(), today, 90,
    )
    assert retention.window_bounds_for_toggle("Last quarter", today=today) == (
        pd.Timestamp("2026-02-01").date(), today, 90,
    )
    assert retention.window_bounds_for_toggle("Last year", today=today) == (
        pd.Timestamp("2025-05-02").date(), today, 365,
    )
