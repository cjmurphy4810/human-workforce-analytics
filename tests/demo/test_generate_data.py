import hashlib
import json
import sqlite3
from datetime import date

from demo.config import DEMO_CHANNEL_KEY
from demo.db import connect_demo_db
from demo.generate_data import build_demo_database, validate_demo_database
import pytest


def _digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_generation_is_deterministic(tmp_path):
    first = tmp_path / "first.db"
    second = tmp_path / "second.db"
    build_demo_database(first, seed=8142026)
    build_demo_database(second, seed=8142026)
    assert _digest(first) == _digest(second)


def test_fixture_has_six_months_and_one_channel(tmp_path):
    path = tmp_path / "demo.db"
    build_demo_database(path)
    with sqlite3.connect(path) as conn:
        start, end, days = conn.execute(
            "SELECT MIN(metric_date), MAX(metric_date), COUNT(*) "
            "FROM daily_channel_metrics"
        ).fetchone()
        channels = conn.execute(
            "SELECT DISTINCT channel FROM daily_channel_metrics"
        ).fetchall()
    assert (start, end, days) == ("2026-02-12", "2026-08-14", 184)
    assert channels == [(DEMO_CHANNEL_KEY,)]


def test_fixture_integrity_validator_passes(tmp_path):
    path = tmp_path / "demo.db"
    build_demo_database(path)
    assert validate_demo_database(path) == []


def test_retention_has_all_report_windows_with_reconciled_views(tmp_path):
    path = tmp_path / "demo.db"
    build_demo_database(path)
    with sqlite3.connect(path) as conn:
        rows = conn.execute(
            "SELECT video_id, window_start, window_end, window_kind, views "
            "FROM retention_buckets ORDER BY video_id, window_kind"
        ).fetchall()
        video_count = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
        mismatches = conn.execute(
            "SELECT COUNT(*) FROM retention_buckets r WHERE r.views != ("
            "SELECT COALESCE(SUM(d.views), 0) FROM daily_video_metrics d "
            "WHERE d.channel=r.channel AND d.video_id=r.video_id "
            "AND d.metric_date BETWEEN r.window_start AND r.window_end)"
        ).fetchone()[0]

    assert len(rows) == video_count * 4
    assert {row[3] for row in rows} == {
        "rolling7",
        "rolling30",
        "rolling90",
        "rolling365",
    }
    assert mismatches == 0


def test_publishing_queue_separates_unpublished_candidates_from_history(tmp_path):
    path = tmp_path / "demo.db"
    build_demo_database(path)
    with sqlite3.connect(path) as conn:
        analyzed, news_count, raw = conn.execute(
            "SELECT videos_analyzed, news_stories_count, result_json FROM publishing_queue"
        ).fetchone()
        historical_ids = {row[0] for row in conn.execute("SELECT video_id FROM videos")}
        recommendation_offsets = [
            row[0]
            for row in conn.execute(
                "SELECT (julianday(v.published_at) - "
                "julianday(q.recommended_publish_date)) * 24 "
                "FROM queue_recommendations q JOIN videos v "
                "ON v.channel=q.channel AND v.video_id=q.video_id"
            )
        ]

    payload = json.loads(raw)
    ranked = payload["ranked_videos"]
    headlines = payload["news_headlines"]
    assert payload["news_available"] is True
    assert analyzed == len(ranked) > 0
    assert news_count == len(headlines) > 0
    assert {item["video_id"] for item in ranked}.isdisjoint(historical_ids)
    assert [item["rank"] for item in ranked] == list(range(1, len(ranked) + 1))
    assert all(-72 <= offset <= 96 for offset in recommendation_offsets)


def test_video_window_aggregation_uses_sums_and_weighted_durations(tmp_path):
    from demo.analytics import aggregate_video_window

    path = tmp_path / "metrics.db"
    with connect_demo_db(path) as conn:
        conn.execute(
            "INSERT INTO videos(channel, video_id, title) VALUES (?, 'video_1', 'Fixture')",
            (DEMO_CHANNEL_KEY,),
        )
        conn.executemany(
            "INSERT INTO daily_video_metrics "
            "(metric_date, channel, video_id, views, estimated_minutes_watched, "
            "average_view_duration, likes, subscribers_gained) "
            "VALUES (?, ?, 'video_1', ?, ?, ?, ?, ?)",
            [
                ("2026-08-12", DEMO_CHANNEL_KEY, 10, 10.0, 60.0, 1, 0),
                ("2026-08-13", DEMO_CHANNEL_KEY, 30, 60.0, 120.0, 3, 2),
                ("2026-08-14", DEMO_CHANNEL_KEY, 100, 300.0, 180.0, 10, 5),
            ],
        )
        conn.executemany(
            "INSERT INTO video_traffic_source_metrics VALUES (?, ?, 'video_1', ?, ?, ?, ?)",
            [
                ("2026-08-12", DEMO_CHANNEL_KEY, "YT_SEARCH", 6, 6.0, 60.0),
                ("2026-08-12", DEMO_CHANNEL_KEY, "EXTERNAL", 4, 4.0, 60.0),
                ("2026-08-13", DEMO_CHANNEL_KEY, "YT_SEARCH", 20, 40.0, 120.0),
                ("2026-08-13", DEMO_CHANNEL_KEY, "ADVERTISING", 10, 20.0, 120.0),
                ("2026-08-14", DEMO_CHANNEL_KEY, "YT_SEARCH", 100, 300.0, 180.0),
            ],
        )
        conn.commit()

    result = aggregate_video_window(
        path,
        start=date(2026, 8, 12),
        end=date(2026, 8, 13),
    )

    assert result == [
        {
            "video_id": "video_1",
            "views": 40,
            "estimated_minutes_watched": 70.0,
            "average_view_duration": 105.0,
            "likes": 4,
            "subscribers_gained": 2,
            "traffic_sources": {
                "ADVERTISING": {
                    "views": 10,
                    "estimated_minutes_watched": 20.0,
                    "average_view_duration": 120.0,
                },
                "EXTERNAL": {
                    "views": 4,
                    "estimated_minutes_watched": 4.0,
                    "average_view_duration": 60.0,
                },
                "YT_SEARCH": {
                    "views": 26,
                    "estimated_minutes_watched": 46.0,
                    "average_view_duration": pytest.approx(106.1538461538),
                },
            },
        }
    ]


def test_advertising_is_limited_to_a_subset_and_traffic_reconciles(tmp_path):
    path = tmp_path / "demo.db"
    build_demo_database(path)
    with sqlite3.connect(path) as conn:
        videos = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
        promoted = conn.execute(
            "SELECT COUNT(DISTINCT video_id) FROM video_traffic_source_metrics "
            "WHERE traffic_source_type='ADVERTISING'"
        ).fetchone()[0]
        promoted_ad_share = conn.execute(
            "SELECT 100.0 * SUM(CASE WHEN traffic_source_type='ADVERTISING' "
            "THEN views ELSE 0 END) / SUM(views) "
            "FROM video_traffic_source_metrics WHERE video_id IN ("
            "SELECT DISTINCT video_id FROM video_traffic_source_metrics "
            "WHERE traffic_source_type='ADVERTISING')"
        ).fetchone()[0]
        mismatches = conn.execute(
            "SELECT COUNT(*) FROM daily_video_metrics d "
            "LEFT JOIN (SELECT channel, metric_date, video_id, SUM(views) views "
            "FROM video_traffic_source_metrics GROUP BY channel, metric_date, video_id) t "
            "ON t.channel=d.channel AND t.metric_date=d.metric_date AND t.video_id=d.video_id "
            "WHERE d.views != COALESCE(t.views, -1)"
        ).fetchone()[0]

    assert 0 < promoted < videos
    assert 4.5 <= promoted_ad_share <= 5.5
    assert mismatches == 0


def test_ci_rows_cover_canonical_tiers_and_multiple_draft_asset_types(tmp_path):
    path = tmp_path / "demo.db"
    build_demo_database(path)
    with sqlite3.connect(path) as conn:
        tiers = {row[0] for row in conn.execute("SELECT DISTINCT tier FROM ci_video_scores")}
        asset_types = {
            row[0] for row in conn.execute("SELECT DISTINCT asset_type FROM ci_content_assets")
        }
        statuses = {row[0] for row in conn.execute("SELECT DISTINCT status FROM ci_content_assets")}

    assert tiers == {
        "top_episode",
        "subscriber_magnet",
        "hidden_gem",
        "average",
        "underperformer",
    }
    assert asset_types >= {"community_post", "quote_card"}
    assert statuses == {"draft"}


@pytest.mark.parametrize(
    ("sql", "expected_error"),
    [
        (
            "UPDATE daily_channel_metrics SET metric_date='2026-02-11' "
            "WHERE metric_date='2026-02-12'",
            "daily channel date coverage mismatch",
        ),
        (
            "DELETE FROM videos WHERE video_id IN "
            "(SELECT video_id FROM videos ORDER BY video_id LIMIT 10)",
            "insufficient video population",
        ),
        (
            "DELETE FROM daily_geo_metrics "
            "WHERE rowid=(SELECT rowid FROM daily_geo_metrics LIMIT 1)",
            "geography coverage mismatch",
        ),
        (
            "UPDATE channel_snapshots SET view_count=view_count+1 "
            "WHERE id=(SELECT MAX(id) FROM channel_snapshots)",
            "channel snapshot cumulative mismatch",
        ),
        (
            "DELETE FROM channel_snapshots",
            "channel snapshot coverage mismatch",
        ),
        (
            "DELETE FROM video_snapshots",
            "video snapshot coverage mismatch",
        ),
        (
            "UPDATE video_traffic_source_metrics SET views=views+1 "
            "WHERE rowid=(SELECT rowid FROM video_traffic_source_metrics LIMIT 1)",
            "video traffic reconciliation mismatch",
        ),
        (
            "UPDATE video_traffic_source_metrics SET views=views+10000 "
            "WHERE rowid=(SELECT rowid FROM video_traffic_source_metrics "
            "WHERE traffic_source_type='ADVERTISING' LIMIT 1)",
            "advertising share mismatch",
        ),
        (
            "DELETE FROM retention_buckets "
            "WHERE rowid=(SELECT rowid FROM retention_buckets LIMIT 1)",
            "retention window coverage mismatch",
        ),
        (
            "UPDATE publishing_queue SET result_json='{}'",
            "publishing queue payload mismatch",
        ),
        (
            "UPDATE ci_video_scores SET tier='PROMOTE' "
            "WHERE rowid=(SELECT rowid FROM ci_video_scores LIMIT 1)",
            "CI tier coverage mismatch",
        ),
        (
            "UPDATE ci_video_scores SET engagement_score=NULL "
            "WHERE rowid=(SELECT rowid FROM ci_video_scores LIMIT 1)",
            "CI numeric values mismatch",
        ),
        (
            "UPDATE ci_video_scores SET total_views=NULL "
            "WHERE rowid=(SELECT rowid FROM ci_video_scores LIMIT 1)",
            "CI numeric values mismatch",
        ),
        (
            "UPDATE ci_video_scores SET overall_score=1e999 "
            "WHERE rowid=(SELECT rowid FROM ci_video_scores LIMIT 1)",
            "CI numeric values mismatch",
        ),
        (
            "UPDATE ci_content_assets SET asset_type='community_post'",
            "CI asset coverage mismatch",
        ),
    ],
)
def test_validator_reports_corrupted_contracts(tmp_path, sql, expected_error):
    path = tmp_path / "demo.db"
    build_demo_database(path)
    with sqlite3.connect(path) as conn:
        conn.execute(sql)
        conn.commit()

    assert expected_error in validate_demo_database(path)


@pytest.mark.parametrize(
    "field",
    ["news_headlines", "ranked_videos"],
)
def test_validator_returns_queue_error_for_non_object_elements(tmp_path, field):
    path = tmp_path / "demo.db"
    build_demo_database(path)
    with sqlite3.connect(path) as conn:
        payload = json.loads(
            conn.execute("SELECT result_json FROM publishing_queue").fetchone()[0]
        )
        payload[field] = ["bad"] * len(payload[field])
        conn.execute(
            "UPDATE publishing_queue SET result_json=?",
            (json.dumps(payload),),
        )
        conn.commit()

    assert "publishing queue payload mismatch" in validate_demo_database(path)
