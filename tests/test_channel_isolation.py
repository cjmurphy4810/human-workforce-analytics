# tests/test_channel_isolation.py
import sqlite3

from db import SCHEMA


def _seed(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    for channel, video_id, views in [
        ("human_workforce", "vidA", 1000),
        ("club_genius", "vidA", 5000),  # same video_id, different channel
        ("kzak", "vidB", 250),
    ]:
        conn.execute(
            "INSERT INTO videos(channel, video_id, title) VALUES (?, ?, ?)",
            (channel, video_id, f"{channel}-{video_id}"),
        )
        conn.execute(
            "INSERT INTO daily_video_metrics(metric_date, channel, video_id, views) "
            "VALUES ('2026-07-18', ?, ?, ?)",
            (channel, video_id, views),
        )
    conn.commit()


def test_videos_with_same_id_coexist_across_channels(tmp_path):
    conn = sqlite3.connect(tmp_path / "iso.db")
    _seed(conn)
    rows = conn.execute(
        "SELECT channel, title FROM videos WHERE video_id = 'vidA' ORDER BY channel"
    ).fetchall()
    assert rows == [("club_genius", "club_genius-vidA"), ("human_workforce", "human_workforce-vidA")]
    conn.close()


def test_channel_filtered_query_never_returns_other_channels(tmp_path):
    conn = sqlite3.connect(tmp_path / "iso2.db")
    _seed(conn)
    for channel, expected_views in [
        ("human_workforce", 1000),
        ("club_genius", 5000),
        ("kzak", 250),
    ]:
        rows = conn.execute(
            "SELECT SUM(views) FROM daily_video_metrics WHERE channel = ?", (channel,)
        ).fetchone()
        assert rows[0] == expected_views
    conn.close()


def test_unfiltered_query_would_wrongly_sum_across_channels(tmp_path):
    """Documents the failure mode every task's `WHERE channel = ...` guards against."""
    conn = sqlite3.connect(tmp_path / "iso3.db")
    _seed(conn)
    total = conn.execute("SELECT SUM(views) FROM daily_video_metrics").fetchone()[0]
    assert total == 1000 + 5000 + 250  # would be wrong if shown as any single channel's total
    conn.close()
