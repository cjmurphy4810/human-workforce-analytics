"""Tests for Phase 1 Pydantic models: Episode, AnalyticsSnapshot, ContentAsset."""
from __future__ import annotations

import dataclasses
from datetime import date, datetime

import pytest
from pydantic import ValidationError

from content_intelligence.models import (
    ASSET_TYPE_LABELS,
    CLASSIFICATION_ACTIONS,
    TIER_LABELS,
    AnalyticsSnapshot,
    AssetStatus,
    AssetType,
    ContentAsset,
    Episode,
    LegacyContentAsset,
    VideoScore,
)


# ── Episode ───────────────────────────────────────────────────────────────────


def test_episode_defaults():
    ep = Episode(youtube_video_id="abc123", title="My Episode")
    assert ep.description == ""
    assert ep.duration_seconds == 0
    assert ep.language == "en"
    assert ep.categories == []
    assert ep.tags == []
    assert ep.score is None
    assert ep.classifications == []
    assert len(ep.id) == 32  # uuid4().hex


def test_episode_id_auto_generated():
    e1 = Episode(youtube_video_id="v1", title="A")
    e2 = Episode(youtube_video_id="v2", title="B")
    assert e1.id != e2.id


def test_episode_published_date_optional():
    ep = Episode(youtube_video_id="v1", title="A")
    assert ep.published_date is None
    ep2 = Episode(youtube_video_id="v1", title="A", published_date=date(2026, 1, 1))
    assert ep2.published_date == date(2026, 1, 1)


def test_episode_score_settable():
    ep = Episode(youtube_video_id="v1", title="A")
    ep.score = 73.5
    assert ep.score == 73.5


def test_episode_classifications_settable():
    ep = Episode(youtube_video_id="v1", title="A")
    ep.classifications = ["subscriber_magnet", "high_engagement"]
    assert "subscriber_magnet" in ep.classifications


def test_episode_requires_youtube_video_id():
    with pytest.raises(ValidationError):
        Episode(title="Missing video id")  # type: ignore[call-arg]


# ── AnalyticsSnapshot ─────────────────────────────────────────────────────────


def test_snapshot_defaults():
    snap = AnalyticsSnapshot(episode_id="ep1", snapshot_date=date(2026, 6, 29))
    assert snap.views == 0
    assert snap.watch_hours == 0.0
    assert snap.ctr == 0.0
    assert snap.subscribers_gained == 0
    assert snap.comments == 0
    assert snap.likes == 0
    assert snap.shares == 0
    assert snap.impressions == 0
    assert snap.returning_viewers == 0


def test_snapshot_all_fields():
    snap = AnalyticsSnapshot(
        episode_id="ep1",
        snapshot_date=date(2026, 6, 29),
        views=1000,
        watch_hours=120.5,
        average_view_duration_seconds=432.0,
        average_percentage_viewed=72.0,
        ctr=4.5,
        subscribers_gained=20,
        comments=15,
        likes=80,
        shares=12,
        impressions=5000,
        returning_viewers=200,
    )
    assert snap.views == 1000
    assert snap.watch_hours == 120.5
    assert snap.ctr == 4.5


def test_snapshot_requires_episode_id():
    with pytest.raises(ValidationError):
        AnalyticsSnapshot(snapshot_date=date(2026, 6, 29))  # type: ignore[call-arg]


# ── ContentAsset (Pydantic) ───────────────────────────────────────────────────


def test_content_asset_defaults():
    asset = ContentAsset(
        episode_id="ep1",
        asset_type=AssetType.community_post,
        title="Post: My Episode",
        content="Some post content.",
    )
    assert asset.status == AssetStatus.draft
    assert asset.platform == ""
    assert asset.scheduled_time is None
    assert asset.published_time is None
    assert asset.metadata == {}
    assert len(asset.id) == 32


def test_content_asset_id_unique():
    a1 = ContentAsset(episode_id="e", asset_type=AssetType.quote_card, title="T", content="C")
    a2 = ContentAsset(episode_id="e", asset_type=AssetType.quote_card, title="T", content="C")
    assert a1.id != a2.id


def test_content_asset_status_enum():
    asset = ContentAsset(
        episode_id="ep1",
        asset_type=AssetType.linkedin_post,
        title="Post",
        content="Content",
        status=AssetStatus.approved,
    )
    assert asset.status == AssetStatus.approved


def test_content_asset_scheduled_time():
    dt = datetime(2026, 7, 1, 12, 0, 0)
    asset = ContentAsset(
        episode_id="ep1",
        asset_type=AssetType.community_post,
        title="T",
        content="C",
        scheduled_time=dt,
    )
    assert asset.scheduled_time == dt


def test_content_asset_metadata():
    asset = ContentAsset(
        episode_id="ep1",
        asset_type=AssetType.image_prompt,
        title="T",
        content="C",
        metadata={"source_timestamp": 42.0},
    )
    assert asset.metadata["source_timestamp"] == 42.0


# ── AssetType enum ────────────────────────────────────────────────────────────


def test_asset_type_all_values_in_labels():
    for t in AssetType:
        assert t.value in ASSET_TYPE_LABELS


def test_asset_type_includes_expected_types():
    expected = {
        "community_post", "executive_poll", "quote_card", "linkedin_post",
        "blog_outline", "newsletter_summary", "short_video_hook",
    }
    values = {t.value for t in AssetType}
    assert expected <= values


# ── AssetStatus enum ──────────────────────────────────────────────────────────


def test_asset_status_values():
    assert AssetStatus.draft == "draft"
    assert AssetStatus.approved == "approved"
    assert AssetStatus.published == "published"
    assert AssetStatus.failed == "failed"
    assert AssetStatus.archived == "archived"


# ── Display helpers ───────────────────────────────────────────────────────────


def test_tier_labels_coverage():
    for t in ("top_episode", "subscriber_magnet", "hidden_gem", "average", "underperformer"):
        assert t in TIER_LABELS


def test_classification_actions_coverage():
    expected_keys = {
        "subscriber_magnet", "hidden_gem", "high_engagement",
        "evergreen_candidate", "needs_repackaging",
        "high_watch_time", "low_ctr_opportunity",
    }
    assert expected_keys <= set(CLASSIFICATION_ACTIONS.keys())


# ── Backward-compat: LegacyContentAsset and VideoScore ───────────────────────


def test_legacy_content_asset_is_dataclass():
    assert dataclasses.is_dataclass(LegacyContentAsset)


def test_legacy_content_asset_fields():
    a = LegacyContentAsset(
        asset_id="x", video_id="v1", video_title="T",
        asset_type="community_post", title="Post",
        body="Body text", generated_at="2026-06-29T00:00:00Z",
    )
    assert a.status == "draft"
    assert a.approved_at is None
    assert a.notes == ""


def test_video_score_is_dataclass():
    assert dataclasses.is_dataclass(VideoScore)


def test_video_score_fields():
    vs = VideoScore(
        video_id="v1", title="Test", scored_at="2026-06-29",
        total_views=500, watch_rate_pct=65.0, like_rate_pct=2.0,
        sub_rate_pct=0.4, promotion_ratio=0.1,
        engagement_score=60.0, evergreen_score=55.0,
        subscriber_magnet_score=45.0, hidden_gem_score=40.0,
        overall_score=52.0, tier="average",
    )
    assert vs.published_at is None
    assert vs.duration_seconds == 0
    assert vs.estimated_hours == 0.0
