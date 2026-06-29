"""Tests for content_intelligence.service persistence functions."""
import sqlite3
import tempfile
from datetime import date
from pathlib import Path

from content_intelligence.models import ContentAsset, VideoScore
from content_intelligence.service import (
    load_assets,
    load_scores,
    run_scoring,
    save_asset,
    update_asset_status,
)


def _make_db(tmp: str) -> Path:
    db_path = Path(tmp) / "test.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS videos (
                video_id TEXT PRIMARY KEY, title TEXT, published_at TEXT,
                duration_seconds INTEGER, thumbnail_url TEXT, description TEXT
            );
            CREATE TABLE IF NOT EXISTS daily_video_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_date TEXT NOT NULL, video_id TEXT NOT NULL,
                views INTEGER, estimated_minutes_watched REAL,
                average_view_duration REAL, likes INTEGER, subscribers_gained INTEGER,
                UNIQUE(metric_date, video_id)
            );
            CREATE TABLE IF NOT EXISTS video_traffic_source_metrics (
                metric_date TEXT NOT NULL, video_id TEXT NOT NULL,
                traffic_source_type TEXT NOT NULL,
                views INTEGER, estimated_minutes_watched REAL, average_view_duration REAL,
                PRIMARY KEY (metric_date, video_id, traffic_source_type)
            );
            CREATE TABLE IF NOT EXISTS ci_video_scores (
                scored_at TEXT NOT NULL, video_id TEXT NOT NULL, tier TEXT NOT NULL,
                engagement_score REAL, evergreen_score REAL,
                subscriber_magnet_score REAL, hidden_gem_score REAL,
                overall_score REAL, total_views INTEGER,
                watch_rate_pct REAL, like_rate_pct REAL,
                sub_rate_pct REAL, promotion_ratio REAL,
                PRIMARY KEY (scored_at, video_id)
            );
            CREATE TABLE IF NOT EXISTS ci_content_assets (
                asset_id TEXT PRIMARY KEY, video_id TEXT NOT NULL,
                video_title TEXT, asset_type TEXT NOT NULL,
                title TEXT, body TEXT NOT NULL, generated_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'draft',
                approved_at TEXT, scheduled_for TEXT,
                notes TEXT NOT NULL DEFAULT ''
            );
        """)
        conn.execute(
            "INSERT INTO videos(video_id, title, duration_seconds) VALUES (?,?,?)",
            ("v1", "Test Video", 600),
        )
        conn.execute(
            "INSERT INTO daily_video_metrics(metric_date, video_id, views, "
            "estimated_minutes_watched, average_view_duration, likes, subscribers_gained) "
            "VALUES (?,?,?,?,?,?,?)",
            ("2026-06-29", "v1", 500, 2000, 240, 25, 5),
        )
        conn.commit()
    return db_path


def test_run_scoring_persists_to_db():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        scores = run_scoring(db_path, date(2026, 6, 29))
        assert len(scores) == 1
        rows = load_scores(db_path, date(2026, 6, 29))
        assert len(rows) == 1
        assert rows[0]["video_id"] == "v1"


def test_run_scoring_upsert_idempotent():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        run_scoring(db_path, date(2026, 6, 29))
        run_scoring(db_path, date(2026, 6, 29))
        rows = load_scores(db_path, date(2026, 6, 29))
        assert len(rows) == 1


def test_save_asset_and_load():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        asset = ContentAsset(
            asset_id="test123", video_id="v1", video_title="Test Video",
            asset_type="community_post", title="Post: Test Video",
            body="Hello world", generated_at="2026-06-29T00:00:00Z",
        )
        save_asset(db_path, asset)
        rows = load_assets(db_path)
        assert len(rows) == 1
        assert rows[0]["asset_id"] == "test123"
        assert rows[0]["status"] == "draft"


def test_update_asset_status_changes_status():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        asset = ContentAsset(
            asset_id="test456", video_id="v1", video_title="Test",
            asset_type="poll", title="Poll: Test",
            body='{"question": "Q?", "options": ["A", "B", "C", "D"]}',
            generated_at="2026-06-29T00:00:00Z",
        )
        save_asset(db_path, asset)
        update_asset_status(db_path, "test456", "approved", approved_at="2026-06-29T01:00:00Z")
        rows = load_assets(db_path)
        assert rows[0]["status"] == "approved"
        assert rows[0]["approved_at"] == "2026-06-29T01:00:00Z"


def test_load_assets_filter_by_type():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        for i, atype in enumerate(("community_post", "poll", "quote_card")):
            save_asset(db_path, ContentAsset(
                asset_id=f"a{i}", video_id="v1", video_title="T",
                asset_type=atype, title=f"{atype} title",
                body="body", generated_at="2026-06-29T00:00:00Z",
            ))
        polls = load_assets(db_path, asset_type="poll")
        assert len(polls) == 1
        assert polls[0]["asset_type"] == "poll"


def test_load_assets_filter_by_status():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        save_asset(db_path, ContentAsset(
            asset_id="x1", video_id="v1", video_title="T",
            asset_type="community_post", title="P",
            body="body", generated_at="2026-06-29T00:00:00Z",
        ))
        update_asset_status(db_path, "x1", "approved")
        drafts = load_assets(db_path, status="draft")
        approved = load_assets(db_path, status="approved")
        assert len(drafts) == 0
        assert len(approved) == 1


def test_update_asset_notes():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        save_asset(db_path, ContentAsset(
            asset_id="n1", video_id="v1", video_title="T",
            asset_type="quote_card", title="Q",
            body="body", generated_at="2026-06-29T00:00:00Z",
        ))
        update_asset_status(db_path, "n1", "draft", notes="Needs revision")
        rows = load_assets(db_path)
        assert rows[0]["notes"] == "Needs revision"
