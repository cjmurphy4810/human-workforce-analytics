"""Tests for ContentAsset and VideoScore dataclasses."""
import dataclasses

from content_intelligence.models import (
    ContentAsset,
    VideoScore,
    ASSET_TYPE_LABELS,
    TIER_LABELS,
)


def test_video_score_defaults():
    vs = VideoScore(
        video_id="v1", title="Test", scored_at="2026-06-29",
        total_views=100, watch_rate_pct=50.0, like_rate_pct=2.0,
        sub_rate_pct=0.5, promotion_ratio=0.3,
        engagement_score=60.0, evergreen_score=55.0,
        subscriber_magnet_score=45.0, hidden_gem_score=40.0,
        overall_score=55.0, tier="average",
    )
    assert vs.published_at is None
    assert vs.duration_seconds == 0
    assert vs.estimated_hours == 0.0


def test_content_asset_defaults():
    a = ContentAsset(
        asset_id="abc", video_id="v1", video_title="T",
        asset_type="community_post", title="Post: T",
        body="Hello", generated_at="2026-06-29T00:00:00Z",
    )
    assert a.status == "draft"
    assert a.approved_at is None
    assert a.notes == ""


def test_asset_type_labels_coverage():
    for k in ("community_post", "poll", "quote_card", "short_hook", "linkedin_post", "course_idea"):
        assert k in ASSET_TYPE_LABELS


def test_tier_labels_coverage():
    for t in ("top_episode", "subscriber_magnet", "hidden_gem", "average", "underperformer"):
        assert t in TIER_LABELS


def test_video_score_is_dataclass():
    assert dataclasses.is_dataclass(VideoScore)


def test_content_asset_is_dataclass():
    assert dataclasses.is_dataclass(ContentAsset)
