import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

from db import migrate_add_channel_column, migrate_rebuild_composite_keys


def test_retention_buckets_table_created():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            with sqlite3.connect(db_path) as conn:
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='retention_buckets'"
                )
                assert cursor.fetchone() is not None


def test_retention_buckets_init_is_idempotent():
    """Running init_db twice must not error."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            db.init_db()


def test_retention_buckets_primary_key():
    """Inserting a duplicate (video_id, window_start, window_end, window_kind) must conflict."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "INSERT INTO retention_buckets (video_id, window_start, window_end, "
                    "window_kind, views, retention_at_25, retention_at_75, fetched_at) "
                    "VALUES ('v1', '2026-01-01', '2026-01-08', 'weekly', 100, 0.6, 0.3, "
                    "'2026-01-08T00:00:00Z')"
                )
                try:
                    conn.execute(
                        "INSERT INTO retention_buckets (video_id, window_start, window_end, "
                        "window_kind, views, retention_at_25, retention_at_75, fetched_at) "
                        "VALUES ('v1', '2026-01-01', '2026-01-08', 'weekly', 200, 0.7, 0.4, "
                        "'2026-01-08T00:00:00Z')"
                    )
                    raised = False
                except sqlite3.IntegrityError:
                    raised = True
                assert raised


def test_retention_buckets_window_kind_disambiguates():
    """A rolling7 row and a weekly row can share start/end if their kinds differ."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "INSERT INTO retention_buckets (video_id, window_start, window_end, "
                    "window_kind, views, retention_at_25, retention_at_75, fetched_at) "
                    "VALUES ('v1', '2026-01-01', '2026-01-08', 'rolling7', 100, 0.6, 0.3, "
                    "'2026-01-08T00:00:00Z')"
                )
                conn.execute(
                    "INSERT INTO retention_buckets (video_id, window_start, window_end, "
                    "window_kind, views, retention_at_25, retention_at_75, fetched_at) "
                    "VALUES ('v1', '2026-01-01', '2026-01-08', 'weekly', 100, 0.6, 0.3, "
                    "'2026-01-08T00:00:00Z')"
                )
                count = conn.execute(
                    "SELECT COUNT(*) FROM retention_buckets"
                ).fetchone()[0]
                assert count == 2


def test_publishing_queue_table_created():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            with sqlite3.connect(db_path) as conn:
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='publishing_queue'"
                )
                assert cursor.fetchone() is not None


def test_publishing_queue_autoincrement_and_columns():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "INSERT INTO publishing_queue(analyzed_at, videos_analyzed, news_stories_count, result_json) "
                    "VALUES ('2026-05-13T10:00:00Z', 3, 20, '{\"ranked_videos\": []}')"
                )
                row = conn.execute("SELECT * FROM publishing_queue").fetchone()
                assert row[0] == 1          # id
                assert row[1] == "2026-05-13T10:00:00Z"  # analyzed_at
                assert row[2] == "human_workforce"  # channel
                assert row[3] == 3          # videos_analyzed
                assert row[4] == 20         # news_stories_count
                assert "ranked_videos" in row[5]  # result_json


def test_daily_geo_metrics_table_created():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            with sqlite3.connect(db_path) as conn:
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='daily_geo_metrics'"
                )
                assert cursor.fetchone() is not None


def test_daily_geo_metrics_primary_key_constraint():
    """Duplicate (metric_date, country_code) must raise IntegrityError."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "INSERT INTO daily_geo_metrics(metric_date, country_code, views, "
                    "subscribers_gained, likes) VALUES ('2026-05-01', 'IN', 100, 5, 10)"
                )
                raised = False
                try:
                    conn.execute(
                        "INSERT INTO daily_geo_metrics(metric_date, country_code, views, "
                        "subscribers_gained, likes) VALUES ('2026-05-01', 'IN', 200, 10, 20)"
                    )
                except sqlite3.IntegrityError:
                    raised = True
                assert raised


def test_queue_recommendations_table_created():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            with sqlite3.connect(db_path) as conn:
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='queue_recommendations'"
                )
                assert cursor.fetchone() is not None


def test_queue_recommendations_insert_or_ignore():
    """Inserting the same video_id twice must result in exactly one row."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            with sqlite3.connect(db_path) as conn:
                for _ in range(2):
                    conn.execute(
                        "INSERT OR IGNORE INTO queue_recommendations "
                        "(video_id, first_recommended_at, recommended_publish_date, "
                        "rank_at_recommendation, relevance_score, theme, why_now) "
                        "VALUES ('v1', '2026-06-14T10:00:00Z', '2026-06-15', 1, 8.5, 'AI', 'Timely.')"
                    )
                count = conn.execute(
                    "SELECT COUNT(*) FROM queue_recommendations"
                ).fetchone()[0]
                assert count == 1


def test_migrate_add_channel_column_backfills_existing_rows(tmp_path):
    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_path)
    # Simulate the pre-migration (single-channel) schema for one representative table.
    conn.execute(
        "CREATE TABLE videos (video_id TEXT PRIMARY KEY, title TEXT, description TEXT, "
        "published_at TEXT, duration_seconds INTEGER, thumbnail_url TEXT)"
    )
    conn.execute("INSERT INTO videos(video_id, title) VALUES ('vid1', 'Old Video')")
    conn.commit()

    from db import DEFAULT_CHANNEL, migrate_add_channel_column
    migrate_add_channel_column(conn)

    row = conn.execute("SELECT channel, video_id FROM videos WHERE video_id='vid1'").fetchone()
    assert row == (DEFAULT_CHANNEL, "vid1")
    conn.close()


def test_migrate_add_channel_column_is_idempotent(tmp_path):
    db_path = tmp_path / "legacy2.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE videos (video_id TEXT PRIMARY KEY, title TEXT, description TEXT, "
        "published_at TEXT, duration_seconds INTEGER, thumbnail_url TEXT)"
    )
    conn.commit()
    from db import migrate_add_channel_column
    migrate_add_channel_column(conn)
    migrate_add_channel_column(conn)  # must not raise on second run
    cols = [r[1] for r in conn.execute("PRAGMA table_info(videos)").fetchall()]
    assert cols.count("channel") == 1
    conn.close()


def test_schema_creates_channel_columns(tmp_path):
    db_path = tmp_path / "fresh.db"
    conn = sqlite3.connect(db_path)
    from db import SCHEMA
    conn.executescript(SCHEMA)
    for table in [
        "channel_snapshots", "videos", "video_snapshots", "daily_video_metrics",
        "daily_channel_metrics", "retention_buckets", "daily_geo_metrics",
        "publishing_queue", "playlists", "playlist_videos", "queue_recommendations",
        "video_traffic_source_metrics", "channel_traffic_sources",
        "ci_video_scores", "ci_content_assets",
    ]:
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        assert "channel" in cols, f"{table} missing channel column"
    conn.close()


def test_migrate_rebuild_composite_keys_makes_on_conflict_work_on_legacy_table(tmp_path):
    """Reproduces the production bug: ALTER TABLE ADD COLUMN adds `channel` but
    cannot rebuild the PRIMARY KEY, so pre-existing installations kept their old
    single-column key and every ON CONFLICT(channel, ...) upsert in
    fetch_metrics.py raised OperationalError against real (pre-migration) data.
    """
    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_path)
    # Simulate a pre-multi-channel `videos` table (old single-column PK).
    conn.execute(
        "CREATE TABLE videos (video_id TEXT PRIMARY KEY, title TEXT, description TEXT, "
        "published_at TEXT, duration_seconds INTEGER, thumbnail_url TEXT)"
    )
    conn.execute("INSERT INTO videos(video_id, title) VALUES ('vid1', 'Old Title')")
    conn.commit()

    migrate_add_channel_column(conn)  # adds `channel`, but NOT a composite PK

    # Before the fix, this raises: OperationalError: ON CONFLICT clause does
    # not match any PRIMARY KEY or UNIQUE constraint.
    migrate_rebuild_composite_keys(conn)

    conn.execute(
        "INSERT INTO videos(channel, video_id, title) VALUES ('human_workforce', 'vid1', 'Updated Title') "
        "ON CONFLICT(channel, video_id) DO UPDATE SET title=excluded.title"
    )
    conn.execute(
        "INSERT INTO videos(channel, video_id, title) VALUES ('club_genius', 'vid1', 'CGS Title') "
        "ON CONFLICT(channel, video_id) DO UPDATE SET title=excluded.title"
    )
    conn.commit()

    rows = conn.execute(
        "SELECT channel, title FROM videos WHERE video_id='vid1' ORDER BY channel"
    ).fetchall()
    assert rows == [("club_genius", "CGS Title"), ("human_workforce", "Updated Title")]
    conn.close()


def test_migrate_rebuild_composite_keys_is_idempotent(tmp_path):
    db_path = tmp_path / "legacy2.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE videos (video_id TEXT PRIMARY KEY, title TEXT, description TEXT, "
        "published_at TEXT, duration_seconds INTEGER, thumbnail_url TEXT)"
    )
    conn.commit()
    migrate_add_channel_column(conn)
    migrate_rebuild_composite_keys(conn)
    migrate_rebuild_composite_keys(conn)  # must not raise on second run
    cols = [r[1] for r in conn.execute("PRAGMA table_info(videos)").fetchall()]
    assert sorted(cols) == sorted(
        ["channel", "video_id", "title", "description", "published_at",
         "duration_seconds", "thumbnail_url"]
    )
    conn.close()
