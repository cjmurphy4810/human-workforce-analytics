import sqlite3

import pytest

from db import SCHEMA
from pages import promotion_intelligence as report_page
from promotion_intelligence.promotion_roi import ROICalculator, format_projected_cost


def test_build_real_features_takes_channel_param():
    source = open("pages/promotion_intelligence.py").read()
    assert "def _build_real_features(db: Path, cpv: float, channel: str)" in source


def test_build_real_features_queries_are_channel_scoped():
    source = open("pages/promotion_intelligence.py").read()
    assert "from channel_state import render_channel_selector" in source
    assert "_active_channel = render_channel_selector()" in source
    assert "_build_real_features(_DB, cpv, channel=_active_channel)" in source


def test_real_advertising_data_does_not_restore_qualifying_hours_for_a_short(tmp_path):
    db_path = tmp_path / "analytics.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(SCHEMA)
        conn.execute(
            "INSERT INTO videos(channel, video_id, title, published_at, duration_seconds) "
            "VALUES (?, ?, ?, ?, ?)",
            ("channel-a", "short-1", "Promoted Short", "2026-07-01", 120),
        )
        conn.execute(
            "INSERT INTO video_snapshots(channel, video_id, captured_at, view_count) "
            "VALUES (?, ?, ?, ?)",
            ("channel-a", "short-1", "2026-08-14", 1_000),
        )
        conn.execute(
            "INSERT INTO daily_video_metrics("
            "channel, video_id, metric_date, views, estimated_minutes_watched, "
            "average_view_duration, likes, subscribers_gained) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("channel-a", "short-1", "2026-08-14", 1_000, 600, 36, 50, 10),
        )
        conn.execute(
            "INSERT INTO video_traffic_source_metrics("
            "channel, video_id, metric_date, traffic_source_type, views, "
            "estimated_minutes_watched, average_view_duration) "
            "VALUES (?, ?, ?, 'ADVERTISING', ?, ?, ?)",
            ("channel-a", "short-1", "2026-08-14", 200, 20, 6),
        )

    [feature] = report_page._build_real_features(db_path, 0.025, "channel-a")

    assert feature.total_watch_hours == pytest.approx(10)
    assert feature.organic_watch_hours == pytest.approx(29 / 3)
    assert feature.avg_view_duration_seconds == pytest.approx(36)
    assert feature.avg_organic_view_duration_seconds == pytest.approx(43.5)
    assert feature.qualifying_hours == 0
    assert feature.cost_per_qualified_hour == 0
    estimate = ROICalculator().estimate_roi(
        report_page.RecommendationEngine([feature]).rank_all()[0],
        10.0,
    )
    assert estimate.cost_per_qualified_hour_projected is None
    assert format_projected_cost(estimate.cost_per_qualified_hour_projected) == "N/A"
