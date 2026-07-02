"""Data model for Organic Momentum scoring."""
from __future__ import annotations

import dataclasses
from enum import Enum
from typing import Optional


class MomentumClass(str, Enum):
    breakout = "Breakout Momentum"
    promising = "Promising Momentum"
    paid_spike = "Paid Spike Only"
    organic_sleeper = "Organic Sleeper"
    needs_packaging = "Needs Better Packaging"
    retention_problem = "Retention Problem"
    do_not_promote = "Do Not Promote"
    insufficient_data = "Insufficient Data"


class PromotionStatus(str, Enum):
    promoted = "Promoted"
    not_promoted = "Not Promoted"
    unknown = "Unknown"


MOMENTUM_CLASS_COLOR: dict[MomentumClass, str] = {
    MomentumClass.breakout: "#22c55e",
    MomentumClass.promising: "#84cc16",
    MomentumClass.paid_spike: "#f59e0b",
    MomentumClass.organic_sleeper: "#3b82f6",
    MomentumClass.needs_packaging: "#8b5cf6",
    MomentumClass.retention_problem: "#f97316",
    MomentumClass.do_not_promote: "#ef4444",
    MomentumClass.insufficient_data: "#6b7280",
}

MOMENTUM_CLASS_ICON: dict[MomentumClass, str] = {
    MomentumClass.breakout: "🚀",
    MomentumClass.promising: "📈",
    MomentumClass.paid_spike: "💸",
    MomentumClass.organic_sleeper: "😴",
    MomentumClass.needs_packaging: "🎁",
    MomentumClass.retention_problem: "⏱️",
    MomentumClass.do_not_promote: "🚫",
    MomentumClass.insufficient_data: "⏳",
}

MOMENTUM_CLASS_ACTION: dict[MomentumClass, str] = {
    MomentumClass.breakout: "Scale Promotion",
    MomentumClass.promising: "Monitor — Test Small Budget",
    MomentumClass.paid_spike: "Pause Promotion",
    MomentumClass.organic_sleeper: "Add to Playlist · Create Follow-Up",
    MomentumClass.needs_packaging: "Refresh Thumbnail · Rewrite Title",
    MomentumClass.retention_problem: "Review Script · Do Not Invest",
    MomentumClass.do_not_promote: "Do Not Invest",
    MomentumClass.insufficient_data: "Gather More Data",
}


@dataclasses.dataclass
class ScoreWeights:
    """Configurable weights for the Organic Momentum Score. Must sum to 1.0."""

    organic_views_growth: float = 0.20
    organic_wh_growth: float = 0.20
    organic_ratio: float = 0.15
    completion_rate: float = 0.10
    avg_pct_viewed: float = 0.10
    subscriber_conversion: float = 0.10
    returning_proxy: float = 0.05
    follow_on_proxy: float = 0.05
    ctr_proxy: float = 0.05

    def validate(self) -> None:
        total = (
            self.organic_views_growth
            + self.organic_wh_growth
            + self.organic_ratio
            + self.completion_rate
            + self.avg_pct_viewed
            + self.subscriber_conversion
            + self.returning_proxy
            + self.follow_on_proxy
            + self.ctr_proxy
        )
        if abs(total - 1.0) > 0.01:
            raise ValueError(f"Weights must sum to 1.0, got {total:.3f}")

    def as_dict(self) -> dict[str, float]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class ScoreBreakdown:
    """Per-component scores (each already weighted, so they sum to total)."""

    organic_views_growth: float = 0.0   # max 20 pts
    organic_wh_growth: float = 0.0       # max 20 pts
    organic_ratio: float = 0.0           # max 15 pts
    completion_rate: float = 0.0         # max 10 pts
    avg_pct_viewed: float = 0.0          # max 10 pts
    subscriber_conversion: float = 0.0  # max 10 pts
    returning_proxy: float = 0.0         # max 5 pts
    follow_on_proxy: float = 0.0         # max 5 pts
    ctr_proxy: float = 0.0               # max 5 pts

    @property
    def total(self) -> float:
        return round(
            self.organic_views_growth
            + self.organic_wh_growth
            + self.organic_ratio
            + self.completion_rate
            + self.avg_pct_viewed
            + self.subscriber_conversion
            + self.returning_proxy
            + self.follow_on_proxy
            + self.ctr_proxy,
            1,
        )


@dataclasses.dataclass
class OrganicMomentumMetrics:
    """All metrics for one video's Organic Momentum assessment."""

    video_id: str
    title: str
    published_date: str
    video_length_seconds: int

    # Promotion
    promotion_status: PromotionStatus
    promotion_start_date: Optional[str]
    promotion_end_date: Optional[str]
    promotion_cost: float

    # Volume
    total_views: int
    organic_views: int
    promotion_views: int
    post_promotion_organic_views: int      # estimated when exact dates unknown

    # Watch time
    total_watch_hours: float
    estimated_qualifying_watch_hours: float
    post_promotion_organic_watch_hours: float  # estimated

    # Quality
    average_view_duration_seconds: float
    average_percentage_viewed: float
    ctr: float                             # 0.0 when unavailable
    impressions: int                       # 0 when unavailable
    engaged_views: int                     # 0 when unavailable
    returning_viewers: int                 # 0 when unavailable

    # Conversion & discovery
    subscribers: int
    follow_on_views: int                   # 0 when unavailable

    # Discovery breakdown
    browse_views: int                      # 0 when unavailable
    suggested_views: int                   # 0 when unavailable
    search_views: int                      # 0 when unavailable

    # Lift metrics
    organic_lift: float                    # organic_views relative to promotion
    organic_watch_hour_lift: float
    organic_momentum_per_dollar: float     # organic_wh_lift / promotion_cost

    # Momentum trend
    view_growth_rate: float               # (recent_daily_avg - early_daily_avg) / early
    wh_growth_rate: float
    recent_daily_views: float
    peak_daily_views: float
    data_points: int                       # number of fetch runs available

    # Scores
    organic_momentum_score: float
    score_breakdown: ScoreBreakdown
    classification: MomentumClass
    recommended_action: str
    data_quality_flag: str                 # "actual" | "estimated" | "partial"
