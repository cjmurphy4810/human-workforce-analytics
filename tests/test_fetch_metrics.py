import sqlite3
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import patch


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
                write_retention_rolling_windows(["v1", "v2"], today=date(2026, 5, 2))

            with sqlite3.connect(db_path) as conn:
                rows = list(conn.execute(
                    "SELECT video_id, window_start, window_end, window_kind, views "
                    "FROM retention_buckets ORDER BY video_id, window_kind"
                ))
                assert len(rows) == 6
                kinds = sorted({r[3] for r in rows})
                assert kinds == ["rolling365", "rolling7", "rolling90"]
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
                write_retention_rolling_windows(["v1"], today=date(2026, 5, 2))

            with sqlite3.connect(db_path) as conn:
                count = conn.execute(
                    "SELECT count(*) FROM retention_buckets"
                ).fetchone()[0]
                assert count == 0
