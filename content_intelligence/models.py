"""Typed data models for the Content Intelligence Engine."""
from __future__ import annotations

import dataclasses
from typing import Literal, Optional

VideoTier = Literal["top_episode", "subscriber_magnet", "hidden_gem", "average", "underperformer"]
AssetType = Literal["community_post", "poll", "quote_card", "short_hook", "linkedin_post", "course_idea"]
ApprovalStatus = Literal["draft", "approved", "rejected", "scheduled", "published"]

ASSET_TYPE_LABELS: dict[str, str] = {
    "community_post": "Community Post",
    "poll": "Poll",
    "quote_card": "Quote Card",
    "short_hook": "Short Hook",
    "linkedin_post": "LinkedIn Post",
    "course_idea": "Course Idea",
}

TIER_LABELS: dict[str, str] = {
    "top_episode": "Top Episode",
    "subscriber_magnet": "Subscriber Magnet",
    "hidden_gem": "Hidden Gem",
    "average": "Average",
    "underperformer": "Underperformer",
}


@dataclasses.dataclass
class VideoScore:
    video_id: str
    title: str
    scored_at: str  # ISO date YYYY-MM-DD

    # Raw metrics (from DB)
    total_views: int
    watch_rate_pct: float
    like_rate_pct: float
    sub_rate_pct: float
    promotion_ratio: float  # 0.0–1.0

    # Composite scores 0–100 (percentile-based within catalog)
    engagement_score: float
    evergreen_score: float
    subscriber_magnet_score: float
    hidden_gem_score: float
    overall_score: float

    tier: VideoTier

    published_at: Optional[str] = None
    duration_seconds: int = 0
    estimated_hours: float = 0.0


@dataclasses.dataclass
class ContentAsset:
    asset_id: str        # uuid4 hex
    video_id: str
    video_title: str
    asset_type: AssetType
    title: str           # short human-readable label
    body: str            # generated content (text or JSON string)
    generated_at: str    # ISO datetime
    status: ApprovalStatus = "draft"
    approved_at: Optional[str] = None
    scheduled_for: Optional[str] = None
    notes: str = ""
