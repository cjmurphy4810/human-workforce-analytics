import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch


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
                assert row[2] == 3          # videos_analyzed
                assert row[3] == 20         # news_stories_count
                assert "ranked_videos" in row[4]  # result_json
