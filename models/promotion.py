"""Data model for per-video promotion metrics used in qualifying watch hours calculations."""
from __future__ import annotations

import dataclasses
from datetime import datetime
from typing import Optional


@dataclasses.dataclass
class VideoPromotionMetrics:
    video_id: str
    title: str
    published: Optional[datetime]
    length_seconds: int

    total_views: int
    promotion_views: int
    organic_views: int

    total_watch_hours: float
    promotion_watch_hours: float
    organic_watch_hours: float
    estimated_qualifying_hours: float

    promotion_cost: float
    subscribers: int
    follow_on_views: int

    promotion_percentage: float
    cost_per_qualified_hour: float
    cost_per_subscriber: float
    cost_per_follow_on_view: float

    avg_view_duration_seconds: float = 0.0
    avg_promotion_view_duration_seconds: float = 0.0
    promotion_duration_estimated: bool = False

    ctr: float = 0.0
    status: str = "active"
    campaign: str = ""
    country: str = ""
    language: str = "en"
    playlist: str = ""
    series: str = ""

    promotion_efficiency_score: float = 0.0
    # "API_ACTUAL"  — promotion_watch_hours from insightTrafficSourceType=ADVERTISING
    # "ESTIMATED"   — promotion_watch_hours from promotion_views × avg_view_duration / 3600
    # "NONE"        — no promotion data available; qualifying hours equal total hours
    data_source: str = "NONE"


def make_metrics(
    *,
    video_id: str,
    title: str,
    published: Optional[datetime],
    length_seconds: int,
    total_views: int,
    promotion_views: int,
    total_watch_hours: float,
    avg_promotion_view_duration_seconds: float,
    promotion_cost: float,
    subscribers: int,
    follow_on_views: int,
    avg_view_duration_seconds: float = 0.0,
    ctr: float = 0.0,
    status: str = "active",
    campaign: str = "",
    country: str = "",
    language: str = "en",
    playlist: str = "",
    series: str = "",
    promotion_duration_estimated: bool = False,
    data_source: str = "NONE",
) -> VideoPromotionMetrics:
    """Factory that derives all computed fields from primary inputs."""
    organic_views = max(total_views - promotion_views, 0)
    promo_watch_hours = promotion_views * avg_promotion_view_duration_seconds / 3600
    organic_watch_hours = max(total_watch_hours - promo_watch_hours, 0)
    qualifying_hours = organic_watch_hours
    promo_pct = (promotion_views / max(total_views, 1)) * 100
    cost_per_qual = promotion_cost / qualifying_hours if qualifying_hours > 0 and promotion_cost > 0 else 0.0
    cost_per_sub = promotion_cost / subscribers if subscribers > 0 and promotion_cost > 0 else 0.0
    cost_per_follow = promotion_cost / follow_on_views if follow_on_views > 0 and promotion_cost > 0 else 0.0

    return VideoPromotionMetrics(
        video_id=video_id,
        title=title,
        published=published,
        length_seconds=length_seconds,
        total_views=total_views,
        promotion_views=promotion_views,
        organic_views=organic_views,
        total_watch_hours=total_watch_hours,
        promotion_watch_hours=promo_watch_hours,
        organic_watch_hours=organic_watch_hours,
        estimated_qualifying_hours=qualifying_hours,
        promotion_cost=promotion_cost,
        subscribers=subscribers,
        follow_on_views=follow_on_views,
        promotion_percentage=promo_pct,
        cost_per_qualified_hour=cost_per_qual,
        cost_per_subscriber=cost_per_sub,
        cost_per_follow_on_view=cost_per_follow,
        avg_view_duration_seconds=avg_view_duration_seconds,
        avg_promotion_view_duration_seconds=avg_promotion_view_duration_seconds,
        promotion_duration_estimated=promotion_duration_estimated,
        ctr=ctr,
        status=status,
        campaign=campaign,
        country=country,
        language=language,
        playlist=playlist,
        series=series,
        data_source=data_source,
    )
