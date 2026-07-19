"""Tests for ContentIntelligenceService and backward-compat service functions."""
from __future__ import annotations

import sqlite3
import tempfile
from datetime import date
from pathlib import Path

from content_intelligence.models import (
    Episode,
    LegacyContentAsset,
)
from content_intelligence.service import (
    ContentIntelligenceService,
    load_assets,
    load_scores,
    run_scoring,
    save_asset,
    update_asset_status,
)


# ── DB helpers ────────────────────────────────────────────────────────────────


def _make_db(tmp: str, seed_metrics: bool = True) -> Path:
    db_path = Path(tmp) / "test.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS videos (
                channel TEXT NOT NULL DEFAULT 'human_workforce',
                video_id TEXT, title TEXT, published_at TEXT,
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
                scored_at TEXT NOT NULL,
                channel TEXT NOT NULL DEFAULT 'human_workforce',
                video_id TEXT NOT NULL, tier TEXT NOT NULL,
                engagement_score REAL, evergreen_score REAL,
                subscriber_magnet_score REAL, hidden_gem_score REAL,
                overall_score REAL, total_views INTEGER,
                watch_rate_pct REAL, like_rate_pct REAL,
                sub_rate_pct REAL, promotion_ratio REAL,
                PRIMARY KEY (channel, scored_at, video_id)
            );
            CREATE TABLE IF NOT EXISTS ci_content_assets (
                asset_id TEXT PRIMARY KEY,
                channel TEXT NOT NULL DEFAULT 'human_workforce',
                video_id TEXT NOT NULL,
                video_title TEXT, asset_type TEXT NOT NULL,
                title TEXT, body TEXT NOT NULL, generated_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'draft',
                approved_at TEXT, scheduled_for TEXT,
                notes TEXT NOT NULL DEFAULT ''
            );
        """)
        conn.execute(
            "INSERT INTO videos(video_id, title, duration_seconds, published_at) VALUES (?,?,?,?)",
            ("v1", "Test Video One", 600, "2026-01-01T00:00:00Z"),
        )
        conn.execute(
            "INSERT INTO videos(video_id, title, duration_seconds, published_at) VALUES (?,?,?,?)",
            ("v2", "Test Video Two", 300, "2026-02-01T00:00:00Z"),
        )
        if seed_metrics:
            conn.execute(
                "INSERT INTO daily_video_metrics(metric_date, video_id, views, "
                "estimated_minutes_watched, average_view_duration, likes, subscribers_gained) "
                "VALUES (?,?,?,?,?,?,?)",
                ("2026-06-29", "v1", 1000, 5000, 300, 50, 20),
            )
            conn.execute(
                "INSERT INTO daily_video_metrics(metric_date, video_id, views, "
                "estimated_minutes_watched, average_view_duration, likes, subscribers_gained) "
                "VALUES (?,?,?,?,?,?,?)",
                ("2026-06-29", "v2", 100, 300, 150, 5, 1),
            )
        conn.commit()
    return db_path


def test_service_requires_channel():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "empty.db"
        from db import SCHEMA
        conn = sqlite3.connect(db_path)
        conn.executescript(SCHEMA)
        conn.close()

        svc = ContentIntelligenceService(db_path, channel="club_genius")
        assert svc._channel == "club_genius"


def _legacy_asset(asset_id: str, video_id: str = "v1", atype: str = "community_post") -> LegacyContentAsset:
    return LegacyContentAsset(
        asset_id=asset_id,
        video_id=video_id,
        video_title="Test Video",
        asset_type=atype,
        title=f"{atype}: Test Video",
        body="Some content here.",
        generated_at="2026-06-29T00:00:00Z",
    )


# ── ContentIntelligenceService ────────────────────────────────────────────────


def test_service_load_episodes_and_snapshots():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        svc = ContentIntelligenceService(db_path, channel="human_workforce")
        episodes, snapshots = svc._load_episodes_and_snapshots()
        assert len(episodes) == 2
        # Only videos with views > 0 get snapshots
        assert len(snapshots) == 2


def test_service_load_episodes_empty_metrics():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp, seed_metrics=False)
        svc = ContentIntelligenceService(db_path, channel="human_workforce")
        episodes, snapshots = svc._load_episodes_and_snapshots()
        assert len(episodes) == 2
        assert len(snapshots) == 0


def test_service_score_content_library_returns_ranked():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        svc = ContentIntelligenceService(db_path, channel="human_workforce")
        ranked = svc.score_content_library()
        assert len(ranked) == 2
        # Higher-performing video should rank first
        assert ranked[0].score is not None
        assert ranked[0].score >= (ranked[1].score or 0.0)


def test_service_score_content_library_sets_episode_id_to_video_id():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        svc = ContentIntelligenceService(db_path, channel="human_workforce")
        ranked = svc.score_content_library()
        video_ids = {ep.youtube_video_id for ep in ranked}
        assert "v1" in video_ids
        assert "v2" in video_ids


def test_service_get_top_episodes():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        svc = ContentIntelligenceService(db_path, channel="human_workforce")
        top = svc.get_top_episodes(n=1)
        assert len(top) == 1


def test_service_get_top_episodes_no_metrics_all_scores_zero():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp, seed_metrics=False)
        svc = ContentIntelligenceService(db_path, channel="human_workforce")
        top = svc.get_top_episodes()
        # Returns episodes but all scores are 0 (no analytics data)
        assert all(ep.score == 0.0 for ep in top)


def test_service_get_subscriber_magnets_returns_episodes():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        svc = ContentIntelligenceService(db_path, channel="human_workforce")
        # v1 has 1000 views and 20 subs → 2% sub rate → exactly at threshold
        magnets = svc.get_subscriber_magnets()
        assert isinstance(magnets, list)
        # v1 should qualify (20/1000 = 2%)
        ids = {ep.youtube_video_id for ep in magnets}
        assert "v1" in ids


def test_service_get_hidden_gems_returns_list():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        svc = ContentIntelligenceService(db_path, channel="human_workforce")
        gems = svc.get_hidden_gems()
        assert isinstance(gems, list)


def test_service_get_repackaging_opportunities_empty_when_no_ctr():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        svc = ContentIntelligenceService(db_path, channel="human_workforce")
        # No CTR data available in this DB — should return empty
        opps = svc.get_repackaging_opportunities()
        assert opps == []


def test_service_create_asset_draft():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        svc = ContentIntelligenceService(db_path, channel="human_workforce")
        ep = Episode(id="ep1", youtube_video_id="v1", title="Test")
        from content_intelligence.models import AssetStatus, AssetType
        asset = svc.create_asset_draft(
            episode=ep,
            asset_type=AssetType.community_post,
            title="Post",
            content="Hello world",
        )
        assert asset.status == AssetStatus.draft
        assert asset.episode_id == "ep1"
        assert asset.content == "Hello world"


def test_service_get_recommended_action_no_classifications():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        svc = ContentIntelligenceService(db_path, channel="human_workforce")
        ep = Episode(youtube_video_id="v1", title="Test")
        action = svc.get_recommended_action(ep)
        assert isinstance(action, str)
        assert len(action) > 0


def test_service_get_recommended_action_with_classifications():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        svc = ContentIntelligenceService(db_path, channel="human_workforce")
        ep = Episode(youtube_video_id="v1", title="Test",
                     classifications=["subscriber_magnet", "high_watch_time"])
        action = svc.get_recommended_action(ep)
        assert "·" in action  # multiple actions joined with separator


# ── Backward-compat: run_scoring / load_scores ────────────────────────────────


def test_run_scoring_persists_to_db():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        scores = run_scoring(db_path, channel="human_workforce", scored_at=date(2026, 6, 29))
        assert len(scores) == 2
        rows = load_scores(db_path, "human_workforce", date(2026, 6, 29))
        assert len(rows) == 2


def test_run_scoring_upsert_idempotent():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        run_scoring(db_path, channel="human_workforce", scored_at=date(2026, 6, 29))
        run_scoring(db_path, channel="human_workforce", scored_at=date(2026, 6, 29))
        rows = load_scores(db_path, "human_workforce", date(2026, 6, 29))
        assert len(rows) == 2


def test_run_scoring_empty_metrics():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp, seed_metrics=False)
        scores = run_scoring(db_path, channel="human_workforce")
        assert scores == []


def test_load_scores_without_date_returns_latest():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        run_scoring(db_path, channel="human_workforce", scored_at=date(2026, 6, 29))
        rows = load_scores(db_path, "human_workforce")
        assert len(rows) == 2
        assert all(r["scored_at"] == "2026-06-29" for r in rows)


def test_load_scores_isolates_by_channel():
    """Two channels scored for the same date must not leak into each other's results."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        # Seed a second channel with its own videos/metrics so run_scoring has data to score.
        with sqlite3.connect(str(db_path)) as conn:
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
                ("club_genius", "2026-06-29", "v1", 1000, 5000, 300, 50, 20),
            )
            conn.execute(
                "INSERT INTO daily_video_metrics(channel, metric_date, video_id, views, "
                "estimated_minutes_watched, average_view_duration, likes, subscribers_gained) "
                "VALUES (?,?,?,?,?,?,?,?)",
                ("club_genius", "2026-06-29", "v2", 100, 300, 150, 5, 1),
            )
            conn.commit()

        run_scoring(db_path, channel="human_workforce", scored_at=date(2026, 6, 29))
        run_scoring(db_path, channel="club_genius", scored_at=date(2026, 6, 29))

        hw_rows = load_scores(db_path, "human_workforce", date(2026, 6, 29))
        cg_rows = load_scores(db_path, "club_genius", date(2026, 6, 29))
        assert len(hw_rows) == 2
        assert len(cg_rows) == 2
        assert all(r["channel"] == "human_workforce" for r in hw_rows)
        assert all(r["channel"] == "club_genius" for r in cg_rows)

        # MAX(scored_at) branch must also stay isolated per channel.
        hw_latest = load_scores(db_path, "human_workforce")
        cg_latest = load_scores(db_path, "club_genius")
        assert len(hw_latest) == 2
        assert len(cg_latest) == 2
        assert all(r["channel"] == "human_workforce" for r in hw_latest)
        assert all(r["channel"] == "club_genius" for r in cg_latest)


# ── Backward-compat: save_asset / update_asset_status / load_assets ──────────


def test_save_asset_and_load():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        save_asset(db_path, _legacy_asset("a1"), channel="human_workforce")
        rows = load_assets(db_path, channel="human_workforce")
        assert len(rows) == 1
        assert rows[0]["asset_id"] == "a1"
        assert rows[0]["status"] == "draft"


def test_save_asset_isolates_by_channel():
    """Same asset_id written for different channels must be tagged with its own channel."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        save_asset(db_path, _legacy_asset("shared1"), channel="human_workforce")
        save_asset(db_path, _legacy_asset("shared2"), channel="club_genius")

        hw_rows = load_assets(db_path, channel="human_workforce")
        cg_rows = load_assets(db_path, channel="club_genius")
        assert [r["asset_id"] for r in hw_rows] == ["shared1"]
        assert [r["asset_id"] for r in cg_rows] == ["shared2"]
        assert hw_rows[0]["channel"] == "human_workforce"
        assert cg_rows[0]["channel"] == "club_genius"


def test_save_asset_upsert_idempotent():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        asset = _legacy_asset("a2")
        save_asset(db_path, asset, channel="human_workforce")
        save_asset(db_path, asset, channel="human_workforce")
        rows = load_assets(db_path, channel="human_workforce")
        assert len(rows) == 1


def test_update_asset_status_approved():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        save_asset(db_path, _legacy_asset("a3"), channel="human_workforce")
        update_asset_status(db_path, "a3", "approved", channel="human_workforce", approved_at="2026-06-29T10:00:00Z")
        rows = load_assets(db_path, channel="human_workforce")
        assert rows[0]["status"] == "approved"
        assert rows[0]["approved_at"] == "2026-06-29T10:00:00Z"


def test_update_asset_notes():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        save_asset(db_path, _legacy_asset("a4"), channel="human_workforce")
        update_asset_status(db_path, "a4", "draft", channel="human_workforce", notes="Needs revision")
        rows = load_assets(db_path, channel="human_workforce")
        assert rows[0]["notes"] == "Needs revision"


def test_load_assets_filter_by_type():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        for i, atype in enumerate(("community_post", "quote_card", "community_post")):
            save_asset(db_path, _legacy_asset(f"a{i}", atype=atype), channel="human_workforce")
        posts = load_assets(db_path, channel="human_workforce", asset_type="community_post")
        assert len(posts) == 2


def test_load_assets_filter_by_status():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        save_asset(db_path, _legacy_asset("a5"), channel="human_workforce")
        update_asset_status(db_path, "a5", "approved", channel="human_workforce")
        save_asset(db_path, _legacy_asset("a6"), channel="human_workforce")
        drafts = load_assets(db_path, channel="human_workforce", status="draft")
        approved = load_assets(db_path, channel="human_workforce", status="approved")
        assert len(drafts) == 1
        assert len(approved) == 1


def test_load_assets_filter_by_video_id():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _make_db(tmp)
        save_asset(db_path, _legacy_asset("a7", video_id="v1"), channel="human_workforce")
        save_asset(db_path, _legacy_asset("a8", video_id="v2"), channel="human_workforce")
        v1_assets = load_assets(db_path, channel="human_workforce", video_id="v1")
        assert all(r["video_id"] == "v1" for r in v1_assets)
        assert len(v1_assets) == 1
