import sqlite3
from datetime import date

import pytest

from database_maintenance import check_database, maintain_database


SCHEMA = """
CREATE TABLE retention_buckets (
    channel TEXT NOT NULL,
    video_id TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    window_kind TEXT NOT NULL,
    views INTEGER NOT NULL,
    retention_at_25 REAL NOT NULL,
    retention_at_75 REAL NOT NULL,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (channel, video_id, window_start, window_end, window_kind)
);
CREATE TABLE publishing_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    analyzed_at TEXT NOT NULL,
    channel TEXT NOT NULL,
    result_json TEXT NOT NULL
);
CREATE TABLE ci_video_scores (
    scored_at TEXT NOT NULL,
    channel TEXT NOT NULL,
    video_id TEXT NOT NULL,
    overall_score REAL,
    PRIMARY KEY (channel, scored_at, video_id)
);
CREATE TABLE channel_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    channel TEXT NOT NULL
);
CREATE TABLE video_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    channel TEXT NOT NULL,
    video_id TEXT NOT NULL
);
"""


def _database(tmp_path):
    path = tmp_path / "metrics.db"
    with sqlite3.connect(path) as conn:
        conn.executescript(SCHEMA)
    return path


def _retention(channel, video, start, end, kind="rolling30", fetched="2026-10-01T00:00:00Z"):
    return (channel, video, start, end, kind, 10, 0.8, 0.5, fetched)


def test_maintenance_keeps_only_current_rows_within_each_channel(tmp_path):
    path = _database(tmp_path)
    with sqlite3.connect(path) as conn:
        conn.executemany(
            "INSERT INTO retention_buckets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                _retention("alpha", "v1", "2026-10-01", "2026-10-31"),
                _retention("alpha", "v1", "2026-10-02", "2026-11-01"),
                _retention("alpha", "v1", "2026-10-03", "2026-11-01", fetched="2026-11-02T00:00:00Z"),
                _retention("alpha", "v1", "2026-08-01", "2026-11-01", kind="rolling90"),
                _retention("alpha", "v1", "2026-08-02", "2026-11-02", kind="rolling90"),
                _retention("beta", "v1", "2026-09-01", "2026-10-01"),
                _retention("beta", "v1", "2026-10-01", "2026-11-01"),
            ],
        )
        conn.executemany(
            "INSERT INTO publishing_queue(analyzed_at, channel, result_json) VALUES (?, ?, ?)",
            [
                ("2026-11-01T00:00:00Z", "alpha", '{"version":"old"}'),
                ("2026-11-02T00:00:00Z", "alpha", '{"version":"tie-old"}'),
                ("2026-11-02T00:00:00Z", "alpha", '{"version":"tie-new"}'),
                ("2026-11-03T00:00:00Z", "beta", '{"version":"beta"}'),
            ],
        )
        conn.executemany(
            "INSERT INTO ci_video_scores VALUES (?, ?, ?, ?)",
            [
                ("2026-11-01", "alpha", "v1", 1.0),
                ("2026-11-02", "alpha", "v1", 2.0),
                ("2026-11-02", "alpha", "v2", 3.0),
                ("2026-11-01", "beta", "v1", 4.0),
            ],
        )

    deleted = maintain_database(path, today=date(2026, 11, 22))

    with sqlite3.connect(path) as conn:
        retention = conn.execute(
            "SELECT channel, window_kind, window_start, window_end "
            "FROM retention_buckets ORDER BY channel, window_kind"
        ).fetchall()
        queues = conn.execute(
            "SELECT channel, analyzed_at, result_json FROM publishing_queue ORDER BY channel"
        ).fetchall()
        scores = conn.execute(
            "SELECT channel, scored_at, video_id FROM ci_video_scores ORDER BY channel, video_id"
        ).fetchall()

    assert retention == [
        ("alpha", "rolling30", "2026-10-03", "2026-11-01"),
        ("alpha", "rolling90", "2026-08-02", "2026-11-02"),
        ("beta", "rolling30", "2026-10-01", "2026-11-01"),
    ]
    assert queues == [
        ("alpha", "2026-11-02T00:00:00Z", '{"version":"tie-new"}'),
        ("beta", "2026-11-03T00:00:00Z", '{"version":"beta"}'),
    ]
    assert scores == [
        ("alpha", "2026-11-02", "v1"),
        ("alpha", "2026-11-02", "v2"),
        ("beta", "2026-11-01", "v1"),
    ]
    assert deleted["retention_buckets"] == 4
    assert deleted["publishing_queue"] == 2
    assert deleted["ci_video_scores"] == 1


def test_maintenance_preserves_370_day_boundary_and_removes_older_history(tmp_path):
    path = _database(tmp_path)
    with sqlite3.connect(path) as conn:
        for table in ("channel_snapshots", "video_snapshots"):
            extra = ", video_id" if table == "video_snapshots" else ""
            values = ", 'v1'" if table == "video_snapshots" else ""
            conn.execute(
                f"INSERT INTO {table}(captured_at, channel{extra}) VALUES ('2025-11-16T23:59:59Z', 'alpha'{values})"
            )
            conn.execute(
                f"INSERT INTO {table}(captured_at, channel{extra}) VALUES ('2025-11-17T00:00:00Z', 'alpha'{values})"
            )
            conn.execute(
                f"INSERT INTO {table}(captured_at, channel{extra}) VALUES ('2026-11-22T00:00:00Z', 'beta'{values})"
            )

    maintain_database(path, today=date(2026, 11, 22))

    with sqlite3.connect(path) as conn:
        for table in ("channel_snapshots", "video_snapshots"):
            rows = conn.execute(
                f"SELECT channel, captured_at FROM {table} ORDER BY channel, captured_at"
            ).fetchall()
            assert rows == [
                ("alpha", "2025-11-17T00:00:00Z"),
                ("beta", "2026-11-22T00:00:00Z"),
            ]


def test_maintenance_handles_empty_tables_and_compacts_deleted_pages(tmp_path):
    path = _database(tmp_path)
    with sqlite3.connect(path) as conn:
        for i in range(2_000):
            conn.execute(
                "INSERT INTO publishing_queue(analyzed_at, channel, result_json) VALUES (?, 'alpha', ?)",
                (f"2026-11-01T00:{i:04d}:00Z", "x" * 2_000),
            )
    before = path.stat().st_size

    deleted = maintain_database(path, today=date(2026, 11, 22))

    assert deleted["publishing_queue"] == 1_999
    assert deleted["retention_buckets"] == 0
    assert path.stat().st_size < before / 2


def test_check_database_accepts_healthy_file_and_rejects_size_limit(tmp_path):
    path = _database(tmp_path)
    size = path.stat().st_size

    result = check_database(path, max_bytes=size)
    assert result == {"integrity": "ok", "size_bytes": size, "max_bytes": size}

    with pytest.raises(ValueError, match="exceeds publish limit"):
        check_database(path, max_bytes=size - 1)


def test_check_database_rejects_missing_and_zero_byte_files(tmp_path):
    missing = tmp_path / "missing.db"
    with pytest.raises(ValueError, match="does not exist"):
        check_database(missing, max_bytes=1_000_000)
    assert not missing.exists()

    empty = tmp_path / "empty.db"
    empty.touch()
    with pytest.raises(ValueError, match="empty"):
        check_database(empty, max_bytes=1_000_000)
