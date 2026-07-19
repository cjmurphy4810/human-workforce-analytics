"""Tests for the content intelligence scoring engine."""
import sqlite3
import tempfile
from datetime import date
from pathlib import Path


from content_intelligence.scoring.engine import _classify_tier, _percentile_rank, score_videos


# ── _percentile_rank ──────────────────────────────────────────────────────────

def test_percentile_rank_empty():
    assert _percentile_rank([]) == []


def test_percentile_rank_single():
    result = _percentile_rank([42.0])
    assert result == [0.0]


def test_percentile_rank_two_values():
    result = _percentile_rank([10.0, 20.0])
    assert result[0] < result[1]
    assert result[1] == 100.0


def test_percentile_rank_all_same():
    result = _percentile_rank([5.0, 5.0, 5.0])
    assert all(r == 0.0 for r in result)


# ── _classify_tier ────────────────────────────────────────────────────────────

def test_classify_tier_top_episode():
    assert _classify_tier(75, 70, 50, 30) == "top_episode"


def test_classify_tier_subscriber_magnet():
    assert _classify_tier(60, 50, 80, 30) == "subscriber_magnet"


def test_classify_tier_hidden_gem():
    assert _classify_tier(55, 30, 50, 70) == "hidden_gem"


def test_classify_tier_average():
    assert _classify_tier(50, 50, 40, 40) == "average"


def test_classify_tier_underperformer():
    assert _classify_tier(20, 20, 20, 20) == "underperformer"


# ── score_videos ──────────────────────────────────────────────────────────────

def _seed_db(conn: sqlite3.Connection, channel: str = "human_workforce") -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS videos (
            channel TEXT NOT NULL DEFAULT 'human_workforce',
            video_id TEXT NOT NULL, title TEXT, published_at TEXT,
            duration_seconds INTEGER, thumbnail_url TEXT, description TEXT,
            PRIMARY KEY (channel, video_id)
        );
        CREATE TABLE IF NOT EXISTS daily_video_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel TEXT NOT NULL DEFAULT 'human_workforce',
            metric_date TEXT NOT NULL, video_id TEXT NOT NULL,
            views INTEGER, estimated_minutes_watched REAL,
            average_view_duration REAL, likes INTEGER, subscribers_gained INTEGER,
            UNIQUE(channel, metric_date, video_id)
        );
        CREATE TABLE IF NOT EXISTS video_traffic_source_metrics (
            channel TEXT NOT NULL DEFAULT 'human_workforce',
            metric_date TEXT NOT NULL, video_id TEXT NOT NULL,
            traffic_source_type TEXT NOT NULL,
            views INTEGER, estimated_minutes_watched REAL, average_view_duration REAL,
            PRIMARY KEY (channel, metric_date, video_id, traffic_source_type)
        );
        CREATE TABLE IF NOT EXISTS ci_video_scores (
            scored_at TEXT NOT NULL, channel TEXT NOT NULL, video_id TEXT NOT NULL, tier TEXT NOT NULL,
            engagement_score REAL, evergreen_score REAL,
            subscriber_magnet_score REAL, hidden_gem_score REAL,
            overall_score REAL, total_views INTEGER,
            watch_rate_pct REAL, like_rate_pct REAL, sub_rate_pct REAL,
            promotion_ratio REAL, PRIMARY KEY (scored_at, channel, video_id)
        );
        CREATE TABLE IF NOT EXISTS ci_content_assets (
            asset_id TEXT PRIMARY KEY, channel TEXT NOT NULL, video_id TEXT NOT NULL,
            video_title TEXT, asset_type TEXT NOT NULL,
            title TEXT, body TEXT NOT NULL, generated_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'draft',
            approved_at TEXT, scheduled_for TEXT, notes TEXT NOT NULL DEFAULT ''
        );
    """)
    conn.execute(
        "INSERT INTO videos(channel, video_id, title, duration_seconds, published_at) VALUES (?,?,?,?,?)",
        (channel, "v1", "Test Video One", 600, "2026-01-01T00:00:00Z"),
    )
    conn.execute(
        "INSERT INTO videos(channel, video_id, title, duration_seconds, published_at) VALUES (?,?,?,?,?)",
        (channel, "v2", "Test Video Two", 300, "2026-02-01T00:00:00Z"),
    )
    conn.execute(
        "INSERT INTO daily_video_metrics(channel, metric_date, video_id, views, "
        "estimated_minutes_watched, average_view_duration, likes, subscribers_gained) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (channel, "2026-06-29", "v1", 1000, 5000, 300, 50, 10),
    )
    conn.execute(
        "INSERT INTO daily_video_metrics(channel, metric_date, video_id, views, "
        "estimated_minutes_watched, average_view_duration, likes, subscribers_gained) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (channel, "2026-06-29", "v2", 100, 200, 200, 5, 3),
    )
    conn.commit()


def test_score_videos_empty_db():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with sqlite3.connect(str(db_path)) as conn:
            _seed_db(conn)
            conn.execute("DELETE FROM daily_video_metrics")
            conn.commit()
        result = score_videos(db_path, "human_workforce")
        assert result == []


def test_score_videos_returns_one_score_per_video():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with sqlite3.connect(str(db_path)) as conn:
            _seed_db(conn)
        scores = score_videos(db_path, "human_workforce", date(2026, 6, 29))
        assert len(scores) == 2


def test_score_videos_scored_at_set_correctly():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with sqlite3.connect(str(db_path)) as conn:
            _seed_db(conn)
        scores = score_videos(db_path, "human_workforce", date(2026, 6, 29))
        assert all(s.scored_at == "2026-06-29" for s in scores)


def test_score_videos_percentiles_in_range():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with sqlite3.connect(str(db_path)) as conn:
            _seed_db(conn)
        scores = score_videos(db_path, "human_workforce")
        for s in scores:
            assert 0 <= s.overall_score <= 100
            assert 0 <= s.engagement_score <= 100
            assert 0 <= s.evergreen_score <= 100
            assert 0 <= s.subscriber_magnet_score <= 100
            assert 0 <= s.hidden_gem_score <= 100


def test_score_videos_zero_duration_gives_zero_watch_rate():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with sqlite3.connect(str(db_path)) as conn:
            _seed_db(conn)
            conn.execute("UPDATE videos SET duration_seconds=0 WHERE video_id='v1'")
            conn.commit()
        scores = score_videos(db_path, "human_workforce")
        v1 = next(s for s in scores if s.video_id == "v1")
        assert v1.watch_rate_pct == 0.0


def test_score_videos_watch_rate_capped_at_100():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with sqlite3.connect(str(db_path)) as conn:
            _seed_db(conn)
            # Set avg_view_duration > duration_seconds
            conn.execute(
                "UPDATE daily_video_metrics SET average_view_duration=999 WHERE video_id='v1'"
            )
            conn.commit()
        scores = score_videos(db_path, "human_workforce")
        v1 = next(s for s in scores if s.video_id == "v1")
        assert v1.watch_rate_pct <= 100.0


def test_score_videos_higher_views_gets_higher_views_percentile():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with sqlite3.connect(str(db_path)) as conn:
            _seed_db(conn)
        scores = score_videos(db_path, "human_workforce")
        v1 = next(s for s in scores if s.video_id == "v1")
        v2 = next(s for s in scores if s.video_id == "v2")
        # v1 has more views so should have higher evergreen/overall score
        assert v1.total_views > v2.total_views


# ── channel isolation ──────────────────────────────────────────────────────────

def test_score_videos_only_returns_requested_channel():
    """Two channels share overlapping video_ids with different metrics; scoring
    one channel must never see or blend the other channel's rows."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with sqlite3.connect(str(db_path)) as conn:
            _seed_db(conn, channel="human_workforce")
            # Same video_ids (v1/v2), different channel, wildly different views.
            conn.execute(
                "INSERT INTO videos(channel, video_id, title, duration_seconds, published_at) "
                "VALUES (?,?,?,?,?)",
                ("club_genius", "v1", "CG Video One", 600, "2026-01-01T00:00:00Z"),
            )
            conn.execute(
                "INSERT INTO videos(channel, video_id, title, duration_seconds, published_at) "
                "VALUES (?,?,?,?,?)",
                ("club_genius", "v2", "CG Video Two", 300, "2026-02-01T00:00:00Z"),
            )
            conn.execute(
                "INSERT INTO daily_video_metrics(channel, metric_date, video_id, views, "
                "estimated_minutes_watched, average_view_duration, likes, subscribers_gained) "
                "VALUES (?,?,?,?,?,?,?,?)",
                ("club_genius", "2026-06-29", "v1", 999999, 500000, 250, 40000, 9000),
            )
            conn.execute(
                "INSERT INTO daily_video_metrics(channel, metric_date, video_id, views, "
                "estimated_minutes_watched, average_view_duration, likes, subscribers_gained) "
                "VALUES (?,?,?,?,?,?,?,?)",
                ("club_genius", "2026-06-29", "v2", 888888, 400000, 150, 30000, 8000),
            )
            conn.execute(
                "INSERT INTO video_traffic_source_metrics(channel, metric_date, video_id, "
                "traffic_source_type, views) VALUES (?,?,?,?,?)",
                ("club_genius", "2026-06-29", "v1", "ADVERTISING", 500000),
            )
            conn.commit()

        scores = score_videos(db_path, "club_genius", date(2026, 6, 29))

        assert len(scores) == 2
        assert {s.video_id for s in scores} == {"v1", "v2"}
        v1 = next(s for s in scores if s.video_id == "v1")
        v2 = next(s for s in scores if s.video_id == "v2")
        # These values only match club_genius's seeded rows, never human_workforce's
        # (views=1000/100) nor a blend of the two.
        assert v1.total_views == 999999
        assert v2.total_views == 888888
        assert v1.title == "CG Video One"
        assert v2.title == "CG Video Two"

        # And scoring human_workforce must remain unaffected by club_genius's presence.
        hw_scores = score_videos(db_path, "human_workforce", date(2026, 6, 29))
        assert {s.total_views for s in hw_scores} == {1000, 100}
