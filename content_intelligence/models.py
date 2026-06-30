"""Pydantic data models for the Content Intelligence Engine.

Phase 1: structured, validated models for Episode, AnalyticsSnapshot, and
ContentAsset. Replaces the Phase 0 dataclass-based models.

Backward-compat aliases (VideoScore, ASSET_TYPE_LABELS, TIER_LABELS) are
retained so existing imports in service.py and the Streamlit page do not break.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime
from enum import Enum
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


# ── Enumerations ──────────────────────────────────────────────────────────────

class AssetStatus(str, Enum):
    draft = "draft"
    approved = "approved"
    scheduled = "scheduled"
    published = "published"
    failed = "failed"
    archived = "archived"


class AssetType(str, Enum):
    community_post = "community_post"
    executive_poll = "executive_poll"
    quote_card = "quote_card"
    executive_tip = "executive_tip"
    discussion_question = "discussion_question"
    linkedin_post = "linkedin_post"
    blog_outline = "blog_outline"
    newsletter_summary = "newsletter_summary"
    course_lesson = "course_lesson"
    assessment_question = "assessment_question"
    infographic_text = "infographic_text"
    image_prompt = "image_prompt"
    short_video_hook = "short_video_hook"


# ── Display helpers ───────────────────────────────────────────────────────────

ASSET_TYPE_LABELS: dict[str, str] = {
    t.value: t.value.replace("_", " ").title() for t in AssetType
}

TIER_LABELS: dict[str, str] = {
    "top_episode": "Top Episode",
    "subscriber_magnet": "Subscriber Magnet",
    "hidden_gem": "Hidden Gem",
    "average": "Average",
    "underperformer": "Underperformer",
}

CLASSIFICATION_ACTIONS: dict[str, str] = {
    "subscriber_magnet": "Use as end-screen destination · Pin in playlists",
    "hidden_gem": "Promote in community posts · Boost organic reach",
    "high_engagement": "Create shorts from this episode · Feature in newsletter",
    "evergreen_candidate": "Convert into course module · Build LinkedIn series",
    "needs_repackaging": "Repackage thumbnail and title to improve CTR",
    "high_watch_time": "Feature in end-screens · Use as deep-dive reference",
    "low_ctr_opportunity": "A/B test new thumbnail · Rewrite title for discoverability",
}


# ── Core Pydantic models ──────────────────────────────────────────────────────

class Episode(BaseModel):
    """A single podcast episode / YouTube video."""

    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    youtube_video_id: str
    title: str
    description: str = ""
    published_date: Optional[date] = None
    duration_seconds: int = 0
    thumbnail_url: str = ""
    transcript: Optional[str] = None
    language: str = "en"
    categories: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    score: Optional[float] = None
    classifications: list[str] = Field(default_factory=list)


class AnalyticsSnapshot(BaseModel):
    """Point-in-time analytics for one episode."""

    episode_id: str
    snapshot_date: date
    views: int = 0
    watch_hours: float = 0.0
    average_view_duration_seconds: float = 0.0
    average_percentage_viewed: float = 0.0
    ctr: float = 0.0
    subscribers_gained: int = 0
    comments: int = 0
    likes: int = 0
    shares: int = 0
    impressions: int = 0
    returning_viewers: int = 0


class ContentAsset(BaseModel):
    """A generated or drafted content asset derived from an episode."""

    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    episode_id: str
    asset_type: AssetType
    title: str
    content: str
    platform: str = ""
    status: AssetStatus = AssetStatus.draft
    scheduled_time: Optional[datetime] = None
    published_time: Optional[datetime] = None
    metadata: dict[str, Any] = Field(default_factory=dict)


# ── Backward-compat: Phase 0 dataclasses ─────────────────────────────────────
# These are kept alive so Phase 0 code (generation/drafts.py, fetch_metrics.py,
# service.save_asset) does not break during the Phase 1 transition.
# They will be removed in Phase 2 once the generation pipeline is rewritten.

import dataclasses  # noqa: E402

VideoTier = Literal["top_episode", "subscriber_magnet", "hidden_gem", "average", "underperformer"]
ApprovalStatus = Literal["draft", "approved", "rejected", "scheduled", "published"]


@dataclasses.dataclass
class LegacyContentAsset:
    """Phase 0 ContentAsset dataclass — used by generation/drafts.py and save_asset."""

    asset_id: str
    video_id: str
    video_title: str
    asset_type: str
    title: str
    body: str
    generated_at: str
    status: str = "draft"
    approved_at: Optional[str] = None
    scheduled_for: Optional[str] = None
    notes: str = ""


@dataclasses.dataclass
class VideoScore:
    video_id: str
    title: str
    scored_at: str

    total_views: int
    watch_rate_pct: float
    like_rate_pct: float
    sub_rate_pct: float
    promotion_ratio: float

    engagement_score: float
    evergreen_score: float
    subscriber_magnet_score: float
    hidden_gem_score: float
    overall_score: float

    tier: VideoTier

    published_at: Optional[str] = None
    duration_seconds: int = 0
    estimated_hours: float = 0.0
