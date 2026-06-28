"""Per-video promotion impact calculations."""
from __future__ import annotations

from models.promotion import VideoPromotionMetrics


def compute_return_on_promotion(m: VideoPromotionMetrics) -> float:
    """
    Return on promotion as a percentage.

    Uses organic hours generated as a value proxy (1 qualifying hour = $1 baseline).
    A positive value means organic value exceeded promotion spend.
    """
    if m.promotion_cost <= 0:
        return 0.0
    organic_value = m.estimated_qualifying_hours * 1.0
    return ((organic_value - m.promotion_cost) / m.promotion_cost) * 100


def compute_promotion_impact(m: VideoPromotionMetrics) -> dict:
    """Return a dict of promotion impact metrics for a single video."""
    organic_wh = m.estimated_qualifying_hours
    return {
        "video_id": m.video_id,
        "title": m.title,
        "promotion_views": m.promotion_views,
        "promotion_subscribers": m.subscribers,
        "promotion_cost": m.promotion_cost,
        "organic_views_after_promotion": m.organic_views,
        "organic_watch_hours_after_promotion": organic_wh,
        "follow_on_views": m.follow_on_views,
        "estimated_qualifying_hours_generated": m.estimated_qualifying_hours,
        "net_gain_hours": organic_wh - m.promotion_watch_hours,
        "return_on_promotion_pct": compute_return_on_promotion(m),
        "cost_per_organic_hour": m.cost_per_qualified_hour,
        "cost_per_qualified_hour": m.cost_per_qualified_hour,
        "cost_per_subscriber": m.cost_per_subscriber,
    }
