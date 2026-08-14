import sqlite3
from datetime import date

import pytest

from demo.config import DEMO_CHANNEL_KEY, DEMO_DB_PATH
from demo.db import connect_demo_db
from demo.report_data import query_frame
from qualifying_watch_hours import _build_real_metrics


def test_overview_inputs_are_non_empty():
    with sqlite3.connect(DEMO_DB_PATH) as conn:
        for table in (
            "channel_snapshots",
            "daily_channel_metrics",
            "videos",
            "daily_geo_metrics",
            "playlists",
            "retention_buckets",
            "channel_traffic_sources",
            "publishing_queue",
        ):
            count = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE channel = ?",
                (DEMO_CHANNEL_KEY,),
            ).fetchone()[0]
            assert count > 0, table


def test_qualifying_hours_have_paid_and_organic_inputs():
    with sqlite3.connect(DEMO_DB_PATH) as conn:
        types = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT traffic_source_type "
                "FROM video_traffic_source_metrics WHERE channel = ?",
                (DEMO_CHANNEL_KEY,),
            )
        }
    assert "ADVERTISING" in types
    assert len(types - {"ADVERTISING"}) >= 3


def test_query_frame_returns_an_empty_frame_for_invalid_sql():
    result = query_frame(
        "SELECT missing_column FROM daily_channel_metrics WHERE channel = :channel",
        {"channel": DEMO_CHANNEL_KEY},
    )
    assert result.empty


def test_qualifying_metrics_sum_daily_increments_through_as_of(tmp_path):
    path = tmp_path / "metrics.db"
    with connect_demo_db(path) as conn:
        conn.execute(
            "INSERT INTO videos(channel, video_id, title, published_at, duration_seconds) "
            "VALUES (?, 'video_1', 'Fixture', '2026-08-01', 600)",
            (DEMO_CHANNEL_KEY,),
        )
        conn.executemany(
            "INSERT INTO daily_video_metrics "
            "(metric_date, channel, video_id, views, estimated_minutes_watched, "
            "average_view_duration, likes, subscribers_gained) "
            "VALUES (?, ?, 'video_1', ?, ?, ?, 0, 0)",
            [
                ("2026-08-12", DEMO_CHANNEL_KEY, 10, 10.0, 60.0),
                ("2026-08-13", DEMO_CHANNEL_KEY, 30, 60.0, 120.0),
                ("2026-08-14", DEMO_CHANNEL_KEY, 100, 300.0, 180.0),
            ],
        )
        conn.executemany(
            "INSERT INTO video_traffic_source_metrics "
            "(metric_date, channel, video_id, traffic_source_type, views, "
            "estimated_minutes_watched, average_view_duration) "
            "VALUES (?, ?, 'video_1', ?, ?, ?, ?)",
            [
                ("2026-08-12", DEMO_CHANNEL_KEY, "YT_SEARCH", 10, 10.0, 60.0),
                ("2026-08-13", DEMO_CHANNEL_KEY, "YT_SEARCH", 20, 40.0, 120.0),
                ("2026-08-13", DEMO_CHANNEL_KEY, "ADVERTISING", 10, 20.0, 120.0),
                ("2026-08-14", DEMO_CHANNEL_KEY, "ADVERTISING", 100, 300.0, 180.0),
            ],
        )
        conn.commit()

    metrics = _build_real_metrics(path, DEMO_CHANNEL_KEY, as_of=date(2026, 8, 13))

    assert len(metrics) == 1
    metric = metrics[0]
    assert metric.total_views == 40
    assert metric.total_watch_hours == pytest.approx(70 / 60)
    assert metric.promotion_views == 10
    assert metric.promotion_watch_hours == pytest.approx(20 / 60)
    assert metric.avg_view_duration_seconds == pytest.approx(105.0)

