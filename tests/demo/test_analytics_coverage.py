import sqlite3
from datetime import date

import pytest

from demo.config import DEMO_CHANNEL_KEY, DEMO_DB_PATH
from demo.db import connect_demo_db
from demo.report_data import query_frame
from qualifying_watch_hours import (
    _build_real_metrics,
    _build_real_timeseries,
    _get_advertising_watch_hours,
    _get_db_date_range,
    _get_shorts_watch_hours,
)


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


def test_content_intelligence_inputs_cover_every_ui_tier_and_asset_variety():
    expected_tiers = {
        "top_episode",
        "subscriber_magnet",
        "hidden_gem",
        "average",
        "underperformer",
    }
    with sqlite3.connect(DEMO_DB_PATH) as conn:
        tiers = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT tier FROM ci_video_scores WHERE channel = ?",
                (DEMO_CHANNEL_KEY,),
            )
        }
        asset_types = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT asset_type FROM ci_content_assets WHERE channel = ?",
                (DEMO_CHANNEL_KEY,),
            )
        }

    assert tiers == expected_tiers
    assert len(asset_types) >= 2


def test_render_comparison_playlists_have_enough_videos():
    expected_playlists = {
        "Engineering Shorts",
        "Visual Engineering Briefings",
        "Deep-Dive Workshops",
        "Studio Originals",
    }
    with sqlite3.connect(DEMO_DB_PATH) as conn:
        counts = dict(
            conn.execute(
                "SELECT p.title, COUNT(*) "
                "FROM playlists p "
                "JOIN playlist_videos pv "
                "ON pv.channel = p.channel AND pv.playlist_id = p.playlist_id "
                "WHERE p.channel = ? AND p.title IN (?, ?, ?, ?) "
                "GROUP BY p.title",
                (DEMO_CHANNEL_KEY, *sorted(expected_playlists)),
            )
        )

    assert set(counts) == expected_playlists
    assert all(count >= 4 for count in counts.values())


def test_promotion_inputs_include_advertised_and_organic_only_videos():
    with sqlite3.connect(DEMO_DB_PATH) as conn:
        promotion_flags = {
            row[0]
            for row in conn.execute(
                "SELECT CASE WHEN EXISTS ("
                "SELECT 1 FROM video_traffic_source_metrics t "
                "WHERE t.channel = v.channel AND t.video_id = v.video_id "
                "AND t.traffic_source_type = 'ADVERTISING'"
                ") THEN 1 ELSE 0 END AS advertised "
                "FROM videos v WHERE v.channel = ?",
                (DEMO_CHANNEL_KEY,),
            )
        }

    assert promotion_flags == {0, 1}


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

    metrics = _build_real_metrics(
        path,
        DEMO_CHANNEL_KEY,
        as_of=date(2026, 8, 13),
        daily_metrics_are_increments=True,
    )

    assert len(metrics) == 1
    metric = metrics[0]
    assert metric.total_views == 40
    assert metric.total_watch_hours == pytest.approx(70 / 60)
    assert metric.promotion_views == 10
    assert metric.promotion_watch_hours == pytest.approx(20 / 60)
    assert metric.avg_view_duration_seconds == pytest.approx(105.0)


def test_production_metrics_use_latest_cumulative_snapshot_at_as_of(tmp_path):
    path = tmp_path / "production.db"
    with connect_demo_db(path) as conn:
        conn.execute(
            "INSERT INTO videos(channel, video_id, title, published_at, duration_seconds) "
            "VALUES (?, 'video_1', 'Fixture', '2026-01-01', 600)",
            (DEMO_CHANNEL_KEY,),
        )
        conn.executemany(
            "INSERT INTO video_snapshots(captured_at, channel, video_id, view_count, "
            "like_count, comment_count) VALUES (?, ?, 'video_1', ?, 0, 0)",
            [
                ("2026-08-01T00:00:00", DEMO_CHANNEL_KEY, 100),
                ("2026-08-10T00:00:00", DEMO_CHANNEL_KEY, 150),
            ],
        )
        conn.executemany(
            "INSERT INTO daily_video_metrics "
            "(metric_date, channel, video_id, views, estimated_minutes_watched, "
            "average_view_duration, likes, subscribers_gained) "
            "VALUES (?, ?, 'video_1', ?, ?, ?, 0, 0)",
            [
                ("2026-08-01", DEMO_CHANNEL_KEY, 100, 600.0, 360.0),
                ("2026-08-10", DEMO_CHANNEL_KEY, 150, 900.0, 360.0),
            ],
        )
        conn.executemany(
            "INSERT INTO video_traffic_source_metrics "
            "(metric_date, channel, video_id, traffic_source_type, views, "
            "estimated_minutes_watched, average_view_duration) "
            "VALUES (?, ?, 'video_1', 'ADVERTISING', ?, ?, 180)",
            [
                ("2026-08-01", DEMO_CHANNEL_KEY, 20, 60.0),
                ("2026-08-10", DEMO_CHANNEL_KEY, 30, 90.0),
            ],
        )
        conn.commit()

    metrics = _build_real_metrics(
        path,
        DEMO_CHANNEL_KEY,
        as_of=date(2026, 8, 10),
    )

    assert len(metrics) == 1
    assert metrics[0].total_views == 150
    assert metrics[0].total_watch_hours == pytest.approx(15.0)
    assert metrics[0].promotion_views == 30
    assert metrics[0].promotion_watch_hours == pytest.approx(1.5)


def test_incremental_timeseries_matches_each_window_and_excludes_shorts(tmp_path):
    path = tmp_path / "changing_mix.db"
    with connect_demo_db(path) as conn:
        conn.executemany(
            "INSERT INTO videos(channel, video_id, title, duration_seconds) "
            "VALUES (?, ?, ?, ?)",
            [
                (DEMO_CHANNEL_KEY, "long", "Long", 600),
                (DEMO_CHANNEL_KEY, "short", "Short", 120),
            ],
        )
        conn.executemany(
            "INSERT INTO daily_channel_metrics VALUES (?, ?, ?, ?, 0, 0)",
            [
                ("2026-08-07", DEMO_CHANNEL_KEY, 600, 600.0),
                ("2026-08-14", DEMO_CHANNEL_KEY, 600, 600.0),
            ],
        )
        conn.executemany(
            "INSERT INTO daily_video_metrics "
            "(metric_date, channel, video_id, views, estimated_minutes_watched, "
            "average_view_duration, likes, subscribers_gained) "
            "VALUES (?, ?, ?, ?, ?, 60, 0, 0)",
            [
                ("2026-08-07", DEMO_CHANNEL_KEY, "long", 400, 400.0),
                ("2026-08-07", DEMO_CHANNEL_KEY, "short", 200, 200.0),
                ("2026-08-14", DEMO_CHANNEL_KEY, "long", 600, 600.0),
            ],
        )
        conn.executemany(
            "INSERT INTO video_traffic_source_metrics VALUES (?, ?, ?, ?, ?, ?, 60)",
            [
                ("2026-08-07", DEMO_CHANNEL_KEY, "long", "ADVERTISING", 300, 300.0),
                ("2026-08-07", DEMO_CHANNEL_KEY, "long", "YT_SEARCH", 100, 100.0),
                ("2026-08-07", DEMO_CHANNEL_KEY, "short", "YT_SEARCH", 200, 200.0),
                ("2026-08-14", DEMO_CHANNEL_KEY, "long", "YT_SEARCH", 600, 600.0),
            ],
        )
        conn.commit()

    result = _build_real_timeseries(
        path,
        DEMO_CHANNEL_KEY,
        as_of=date(2026, 8, 14),
        daily_metrics_are_increments=True,
    )

    assert result["promotion_hours"].tolist() == pytest.approx([5.0, 0.0])
    assert result["organic_hours"].tolist() == pytest.approx([5.0, 10.0])
    assert result["qualifying_hours"].tolist() == pytest.approx([100 / 60, 10.0])


def _build_unbaselined_production_window(path):
    with connect_demo_db(path) as conn:
        conn.executemany(
            "INSERT INTO videos(channel, video_id, title, duration_seconds) "
            "VALUES (?, ?, ?, ?)",
            [
                (DEMO_CHANNEL_KEY, "long", "Long", 600),
                (DEMO_CHANNEL_KEY, "short", "Short", 120),
            ],
        )
        conn.executemany(
            "INSERT INTO daily_channel_metrics VALUES (?, ?, 600, 600, 0, 0)",
            [
                ("2026-08-07", DEMO_CHANNEL_KEY),
                ("2026-08-14", DEMO_CHANNEL_KEY),
            ],
        )
        conn.executemany(
            "INSERT INTO daily_video_metrics "
            "(metric_date, channel, video_id, views, estimated_minutes_watched, "
            "average_view_duration, likes, subscribers_gained) "
            "VALUES (?, ?, 'short', ?, ?, 60, 0, 0)",
            [
                ("2026-08-07", DEMO_CHANNEL_KEY, 200, 200.0),
                ("2026-08-14", DEMO_CHANNEL_KEY, 260, 260.0),
            ],
        )
        conn.executemany(
            "INSERT INTO video_traffic_source_metrics "
            "(metric_date, channel, video_id, traffic_source_type, views, "
            "estimated_minutes_watched, average_view_duration) VALUES "
            "(?, ?, ?, 'ADVERTISING', ?, ?, 60)",
            [
                ("2026-08-07", DEMO_CHANNEL_KEY, "long", 300, 300.0),
                ("2026-08-14", DEMO_CHANNEL_KEY, "long", 360, 360.0),
                ("2026-08-07", DEMO_CHANNEL_KEY, "short", 20, 20.0),
                ("2026-08-14", DEMO_CHANNEL_KEY, "short", 30, 30.0),
            ],
        )
        conn.commit()


def test_production_ypp_excludes_unbaselined_lifetime_snapshot(tmp_path):
    path = tmp_path / "unbaselined_ypp.db"
    _build_unbaselined_production_window(path)

    advertising, has_data = _get_advertising_watch_hours(
        path, DEMO_CHANNEL_KEY, as_of=date(2026, 8, 14)
    )
    shorts = _get_shorts_watch_hours(
        path, DEMO_CHANNEL_KEY, as_of=date(2026, 8, 14)
    )

    assert has_data is True
    assert advertising == pytest.approx(70 / 60)
    assert shorts == pytest.approx(50 / 60)


def test_production_weekly_series_excludes_unbaselined_lifetime_snapshot(tmp_path):
    path = tmp_path / "unbaselined_weekly.db"
    _build_unbaselined_production_window(path)

    result = _build_real_timeseries(
        path, DEMO_CHANNEL_KEY, as_of=date(2026, 8, 14)
    )

    assert result["promotion_hours"].tolist() == pytest.approx([0.0, 70 / 60])
    assert result["organic_hours"].tolist() == pytest.approx([10.0, 10 - 70 / 60])
    assert result["qualifying_hours"].tolist() == pytest.approx([10.0, 8.0])


def test_db_date_range_matches_trailing_year_window(tmp_path):
    path = tmp_path / "date_range.db"
    with connect_demo_db(path) as conn:
        conn.executemany(
            "INSERT INTO daily_channel_metrics VALUES (?, ?, 1, 60, 0, 0)",
            [
                ("2025-08-14", DEMO_CHANNEL_KEY),
                ("2025-08-15", DEMO_CHANNEL_KEY),
                ("2026-08-14", DEMO_CHANNEL_KEY),
            ],
        )
        conn.commit()

    assert _get_db_date_range(
        path, DEMO_CHANNEL_KEY, as_of=date(2026, 8, 14)
    ) == ("2025-08-15", "2026-08-14")


def test_video_metadata_without_metrics_is_not_real_data(tmp_path):
    path = tmp_path / "metadata_only.db"
    with connect_demo_db(path) as conn:
        conn.execute(
            "INSERT INTO videos(channel, video_id, title, duration_seconds) "
            "VALUES (?, 'video_1', 'Metadata only', 600)",
            (DEMO_CHANNEL_KEY,),
        )
        conn.commit()

    assert _build_real_metrics(
        path,
        DEMO_CHANNEL_KEY,
        as_of=date(2026, 8, 14),
        daily_metrics_are_increments=True,
    ) == []
