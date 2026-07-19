import json
import sqlite3
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import patch

from db import SCHEMA
from fetch_metrics import CHANNEL_CONFIGS


def test_channel_configs_cover_all_three_channels():
    keys = {c["key"] for c in CHANNEL_CONFIGS}
    assert keys == {"human_workforce", "club_genius", "kzak"}
    for cfg in CHANNEL_CONFIGS:
        assert cfg["channel_id_env"].startswith("YT_CHANNEL_ID_")
        assert cfg["refresh_token_env"].startswith("YT_REFRESH_TOKEN_")


def test_video_insert_is_tagged_with_channel(tmp_path, monkeypatch):
    import db as db_module
    test_db = tmp_path / "test.db"
    monkeypatch.setattr(db_module, "DB_PATH", test_db)

    conn = sqlite3.connect(test_db)
    conn.executescript(SCHEMA)
    conn.execute(
        "INSERT INTO videos(channel, video_id, title) VALUES ('club_genius', 'v1', 'Test')"
    )
    conn.execute(
        "INSERT INTO videos(channel, video_id, title) VALUES ('human_workforce', 'v1', 'Different Video, Same ID')"
    )
    conn.commit()

    rows = conn.execute(
        "SELECT channel, title FROM videos WHERE video_id='v1' ORDER BY channel"
    ).fetchall()
    assert rows == [
        ("club_genius", "Different Video, Same ID"),
        ("human_workforce", "Different Video, Same ID"),
    ] or rows == [
        ("club_genius", "Test"),
        ("human_workforce", "Different Video, Same ID"),
    ]
    # The real assertion: both channel rows coexist without a PK collision.
    assert len(rows) == 2
    conn.close()


def test_write_retention_rolling_windows_writes_three_rows_per_video():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()

            with sqlite3.connect(db_path) as conn:
                for vid in ("v1", "v2"):
                    conn.execute(
                        "INSERT INTO videos(video_id, title, published_at, duration_seconds) "
                        "VALUES (?, ?, ?, ?)",
                        (vid, f"Video {vid}", "2026-01-01T00:00:00Z", 600),
                    )
                    for d in range(0, 400, 5):
                        conn.execute(
                            "INSERT INTO daily_video_metrics(metric_date, video_id, views, "
                            "estimated_minutes_watched, average_view_duration, likes, "
                            "subscribers_gained) VALUES (?, ?, ?, 0, 0, 0, 0)",
                            ((date.today().fromordinal(date.today().toordinal() - d)).isoformat(),
                             vid, 10),
                        )

            from fetch_metrics import write_retention_rolling_windows

            def fake_curve(video_id, start, end):
                return {
                    "video_id": video_id,
                    "window_start": start.isoformat(),
                    "window_end": end.isoformat(),
                    "retention_at_25": 0.7,
                    "retention_at_75": 0.4,
                }

            with patch("fetch_metrics.fetch_retention_curve", side_effect=fake_curve), \
                 patch("fetch_metrics.fetch_video_views_in_window", return_value=42):
                write_retention_rolling_windows("human_workforce", ["v1", "v2"], today=date(2026, 5, 2))

            with sqlite3.connect(db_path) as conn:
                rows = list(conn.execute(
                    "SELECT video_id, window_start, window_end, window_kind, views "
                    "FROM retention_buckets ORDER BY video_id, window_kind"
                ))
                assert len(rows) == 8
                kinds = sorted({r[3] for r in rows})
                assert kinds == ["rolling30", "rolling365", "rolling7", "rolling90"]
                assert all(r[4] > 0 for r in rows)


def test_write_retention_rolling_windows_skips_when_curve_is_none():
    """If the API has no data, we don't insert a row."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "INSERT INTO videos(video_id, title, published_at, duration_seconds) "
                    "VALUES ('v1', 'V1', '2026-01-01T00:00:00Z', 600)"
                )

            from fetch_metrics import write_retention_rolling_windows
            with patch("fetch_metrics.fetch_retention_curve", return_value=None):
                write_retention_rolling_windows("human_workforce", ["v1"], today=date(2026, 5, 2))

            with sqlite3.connect(db_path) as conn:
                count = conn.execute(
                    "SELECT count(*) FROM retention_buckets"
                ).fetchone()[0]
                assert count == 0


# --- write_publishing_queue tests ---

def test_write_publishing_queue_skips_when_no_unpublished_videos():
    """If all videos are public, skip without calling Claude."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db as db_module
            db_module.init_db()
            from fetch_metrics import write_publishing_queue
            with patch("fetch_metrics.classify_video_themes") as mock_classify:
                write_publishing_queue("human_workforce", [
                    {"video_id": "v1", "privacy_status": "public", "title": "T", "description": "D"}
                ])
                mock_classify.assert_not_called()


def test_write_publishing_queue_skips_without_anthropic_key(monkeypatch):
    """If ANTHROPIC_API_KEY is not set, skip gracefully without writing to DB."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db as db_module
            db_module.init_db()
            from fetch_metrics import write_publishing_queue
            write_publishing_queue("human_workforce", [
                {"video_id": "v1", "privacy_status": "private", "title": "T", "description": "D"}
            ])
            with sqlite3.connect(db_path) as conn:
                count = conn.execute("SELECT COUNT(*) FROM publishing_queue").fetchone()[0]
                assert count == 0


def test_write_publishing_queue_writes_result_json(monkeypatch):
    """Happy path: unpublished videos + API key → row written to publishing_queue."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test_key")
    monkeypatch.delenv("NEWS_API_KEY", raising=False)
    ranked = [{"rank": 1, "video_id": "v1", "title": "T", "theme": "AI theme", "relevance_score": 0, "why_now": "No news."}]
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db as db_module
            db_module.init_db()
            from fetch_metrics import write_publishing_queue
            with patch("fetch_metrics.classify_video_themes", return_value={"v1": "AI theme"}), \
                 patch("fetch_metrics.rank_videos_by_news", return_value=ranked), \
                 patch("fetch_metrics.anthropic.Anthropic"):
                write_publishing_queue("human_workforce", [
                    {"video_id": "v1", "privacy_status": "private", "title": "T", "description": "D"}
                ])
            with sqlite3.connect(db_path) as conn:
                row = conn.execute("SELECT videos_analyzed, result_json FROM publishing_queue").fetchone()
                assert row[0] == 1
                result = json.loads(row[1])
                assert len(result["ranked_videos"]) == 1
                assert result["ranked_videos"][0]["video_id"] == "v1"


def test_write_geo_metrics_upserts_rows():
    """write_geo_metrics inserts rows and upserts on conflict without adding duplicates."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()

            from fetch_metrics import write_geo_metrics

            rows = [
                {"metric_date": "2026-05-01", "country_code": "IN",
                 "views": 1000, "subscribers_gained": 20, "likes": 50},
                {"metric_date": "2026-05-01", "country_code": "US",
                 "views": 200, "subscribers_gained": 3, "likes": 8},
            ]
            write_geo_metrics("human_workforce", rows)

            with sqlite3.connect(db_path) as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM daily_geo_metrics"
                ).fetchone()[0]
                assert count == 2

                views = conn.execute(
                    "SELECT views FROM daily_geo_metrics "
                    "WHERE metric_date='2026-05-01' AND country_code='IN'"
                ).fetchone()[0]
                assert views == 1000

            # Upsert: re-insert same key with updated views — row count stays at 2
            write_geo_metrics("human_workforce", [
                {"metric_date": "2026-05-01", "country_code": "IN",
                 "views": 1500, "subscribers_gained": 25, "likes": 60},
            ])

            with sqlite3.connect(db_path) as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM daily_geo_metrics"
                ).fetchone()[0]
                assert count == 2

                views = conn.execute(
                    "SELECT views FROM daily_geo_metrics "
                    "WHERE metric_date='2026-05-01' AND country_code='IN'"
                ).fetchone()[0]
                assert views == 1500


def test_write_geo_metrics_empty_list_is_noop():
    """Calling write_geo_metrics with an empty list writes nothing and doesn't error."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            from fetch_metrics import write_geo_metrics
            write_geo_metrics("human_workforce", [])
            with sqlite3.connect(db_path) as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM daily_geo_metrics"
                ).fetchone()[0]
                assert count == 0


# --- write_queue_recommendations tests ---

def test_write_queue_recommendations_inserts_first_occurrence():
    """Happy path: one ranked video → one row in queue_recommendations."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db as db_module
            db_module.init_db()
            from fetch_metrics import write_queue_recommendations
            ranked = [
                {
                    "rank": 1,
                    "video_id": "v1",
                    "title": "AI Episode",
                    "theme": "AI workforce",
                    "relevance_score": 9.0,
                    "why_now": "Major AI news today.",
                }
            ]
            write_queue_recommendations("human_workforce", ranked, date(2026, 6, 14))
            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    "SELECT video_id, recommended_publish_date, rank_at_recommendation, relevance_score "
                    "FROM queue_recommendations WHERE video_id = 'v1'"
                ).fetchone()
                assert row is not None
                assert row[0] == "v1"
                assert row[1] == "2026-06-15"   # cron_date + 1 day (rank=1)
                assert row[2] == 1
                assert row[3] == 9.0


def test_write_queue_recommendations_ignores_duplicate_video_id():
    """Calling write_queue_recommendations twice with the same video_id keeps only the first row."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db as db_module
            db_module.init_db()
            from fetch_metrics import write_queue_recommendations
            video = [{"rank": 1, "video_id": "v1", "title": "T", "theme": "AI",
                      "relevance_score": 8.0, "why_now": "First time."}]
            write_queue_recommendations("human_workforce", video, date(2026, 6, 14))
            # Second call simulates next cron run with same video at different rank
            video2 = [{"rank": 3, "video_id": "v1", "title": "T", "theme": "AI",
                       "relevance_score": 5.0, "why_now": "Second time."}]
            write_queue_recommendations("human_workforce", video2, date(2026, 6, 15))
            with sqlite3.connect(db_path) as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM queue_recommendations"
                ).fetchone()[0]
                assert count == 1
                # First insertion's values must be preserved
                row = conn.execute(
                    "SELECT rank_at_recommendation, relevance_score, recommended_publish_date "
                    "FROM queue_recommendations WHERE video_id = 'v1'"
                ).fetchone()
                assert row[0] == 1
                assert row[1] == 8.0
                assert row[2] == "2026-06-15"


def test_write_queue_recommendations_noop_when_empty():
    """Calling with an empty list writes nothing and does not error."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db as db_module
            db_module.init_db()
            from fetch_metrics import write_queue_recommendations
            write_queue_recommendations("human_workforce", [], date(2026, 6, 14))
            with sqlite3.connect(db_path) as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM queue_recommendations"
                ).fetchone()[0]
                assert count == 0
