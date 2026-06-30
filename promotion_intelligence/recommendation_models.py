"""Pydantic-free data models for Promotion Intelligence.

Uses dataclasses throughout so no extra dependency beyond the stdlib is needed.
"""
from __future__ import annotations

import dataclasses
from enum import Enum
from typing import Optional


# ── Enumerations ──────────────────────────────────────────────────────────────


class PromotionClass(str, Enum):
    promote_immediately = "Promote Immediately"
    watch_organically = "Watch Organically"
    do_not_promote = "Do Not Promote"
    needs_more_data = "Needs More Organic Data"
    already_saturated = "Already Saturated"


PROMOTION_CLASS_COLOR: dict[PromotionClass, str] = {
    PromotionClass.promote_immediately: "#22c55e",
    PromotionClass.watch_organically: "#3b82f6",
    PromotionClass.do_not_promote: "#ef4444",
    PromotionClass.needs_more_data: "#f59e0b",
    PromotionClass.already_saturated: "#8b5cf6",
}

PROMOTION_CLASS_ICON: dict[PromotionClass, str] = {
    PromotionClass.promote_immediately: "🚀",
    PromotionClass.watch_organically: "👀",
    PromotionClass.do_not_promote: "🚫",
    PromotionClass.needs_more_data: "⏳",
    PromotionClass.already_saturated: "📊",
}


# ── Video features ────────────────────────────────────────────────────────────


@dataclasses.dataclass
class VideoFeatures:
    """All input features consumed by the promotion scoring engine."""

    video_id: str
    title: str

    # ── Volume ─────────────────────────────────────────────────────────────
    total_views: int
    organic_views: int
    promotion_views: int
    subscribers_gained: int
    follow_on_views: int
    likes: int

    # ── Watch time ─────────────────────────────────────────────────────────
    total_watch_hours: float
    organic_watch_hours: float
    qualifying_hours: float
    avg_view_duration_seconds: float
    avg_promotion_view_duration_seconds: float

    # ── Derived rates ───────────────────────────────────────────────────────
    audience_retention_pct: float           # avg_view_duration / length_s × 100
    subscriber_conversion_per_1k: float     # subs / organic_views × 1000
    views_per_day: float
    follow_on_rate_pct: float               # follow_on_views / total_views × 100
    promotion_ratio_pct: float              # promo_views / total_views × 100
    organic_multiplier: float               # organic_views / max(promo_views, 1)

    # ── Existing scores ─────────────────────────────────────────────────────
    promotion_efficiency_score: float       # 0-100 from analytics.promotion_efficiency
    ci_overall_score: float                 # 0-100 from content intelligence

    # ── Cost estimates ──────────────────────────────────────────────────────
    cpv: float                              # cost-per-view used for this video
    promotion_cost_estimated: float         # cpv × promotion_views
    cost_per_qualified_hour: float
    cost_per_subscriber: float
    cost_per_follow_on_view: float

    # ── Metadata ────────────────────────────────────────────────────────────
    video_age_days: int
    length_seconds: int
    language: str = "en"
    series: str = ""
    book: str = ""
    topic: str = ""

    # ── Data quality ────────────────────────────────────────────────────────
    data_source: str = "NONE"              # API_ACTUAL | ESTIMATED | NONE
    has_sufficient_data: bool = True


# ── Score breakdown ───────────────────────────────────────────────────────────


@dataclasses.dataclass
class ScoreBreakdown:
    """Component scores that sum to the composite Promotion Opportunity Score."""

    retention: float            # max 25 pts
    subscriber_conversion: float  # max 20 pts
    organic_hours: float        # max 20 pts
    views_per_day: float        # max 15 pts
    follow_on_rate: float       # max 10 pts
    promotion_efficiency: float  # max 10 pts

    @property
    def total(self) -> float:
        return round(
            self.retention
            + self.subscriber_conversion
            + self.organic_hours
            + self.views_per_day
            + self.follow_on_rate
            + self.promotion_efficiency,
            1,
        )


# ── Promotion opportunity ─────────────────────────────────────────────────────


@dataclasses.dataclass
class PromotionOpportunity:
    features: VideoFeatures
    score: float                    # 0-100 composite
    breakdown: ScoreBreakdown
    classification: PromotionClass
    explanation: str
    rank: int = 0

    @property
    def video_id(self) -> str:
        return self.features.video_id

    @property
    def title(self) -> str:
        return self.features.title


# ── ROI estimate ──────────────────────────────────────────────────────────────


@dataclasses.dataclass
class ROIEstimate:
    video_id: str
    budget: float
    cpv: float

    estimated_views: int
    estimated_subscribers: int
    estimated_organic_lift: int
    estimated_follow_on_views: int
    estimated_qualifying_hours: float
    expected_promotion_efficiency: float

    cost_per_qualified_hour_projected: float
    cost_per_subscriber_projected: float
    cost_per_follow_on_projected: float

    confidence: str           # "high" | "medium" | "low"
    confidence_reason: str


# ── Recommendation cards ──────────────────────────────────────────────────────


@dataclasses.dataclass
class RecommendationCards:
    top_10_to_promote: list[PromotionOpportunity]
    top_10_to_stop: list[PromotionOpportunity]
    most_efficient: Optional[PromotionOpportunity]
    least_efficient: Optional[PromotionOpportunity]
    highest_organic_multiplier: Optional[PromotionOpportunity]
    highest_subscriber_generator: Optional[PromotionOpportunity]
    highest_qualifying_hour_generator: Optional[PromotionOpportunity]
