"""
Qualifying watch hours calculations.

Core formula:
  Organic Watch Hours = Total Watch Hours - Promotion Watch Hours
  Estimated Qualifying Hours = Organic Watch Hours, excluding Shorts
  Promotion Watch Hours = Promotion Views × Avg Promotion View Duration (seconds) / 3600

When avg promotion view duration is unavailable, estimate using the overall average
view duration and mark the result as estimated.
"""
from __future__ import annotations

import dataclasses
from datetime import date
from typing import Optional

from models.promotion import VideoPromotionMetrics
from models.qualifying_hours import QualifyingHoursReport


def compute_qualifying_hours(
    metrics: list[VideoPromotionMetrics],
    as_of: Optional[date] = None,
) -> QualifyingHoursReport:
    total_wh = sum(m.total_watch_hours for m in metrics)
    promo_wh = sum(m.promotion_watch_hours for m in metrics)
    organic_wh = sum(m.organic_watch_hours for m in metrics)
    qualifying_wh = sum(m.estimated_qualifying_hours for m in metrics)
    total_organic_views = sum(m.organic_views for m in metrics)

    avg_organic_dur = 0.0
    if total_organic_views > 0:
        avg_organic_dur = sum(
            m.avg_view_duration_seconds * m.organic_views
            for m in metrics
            if m.organic_views > 0
        ) / total_organic_views

    return QualifyingHoursReport(
        estimated_qualifying_hours=qualifying_wh,
        promotion_watch_hours=promo_wh,
        organic_watch_hours=organic_wh,
        promotion_pct=(promo_wh / max(total_wh, 1)) * 100,
        avg_organic_view_duration_seconds=avg_organic_dur,
        hours_lost_to_promotion=promo_wh,
        as_of_date=as_of or date.today(),
    )


def recompute_with_sim_duration(
    metrics: list[VideoPromotionMetrics],
    sim_duration_seconds: float,
) -> list[VideoPromotionMetrics]:
    """Return a new list with promotion_watch_hours recomputed using sim_duration_seconds."""
    result = []
    for m in metrics:
        promo_wh = m.promotion_views * sim_duration_seconds / 3600
        organic_wh = max(m.total_watch_hours - promo_wh, 0)
        qualifying_wh = 0.0 if 0 < m.length_seconds <= 180 else organic_wh
        cost_per_qual = (
            m.promotion_cost / qualifying_wh
            if qualifying_wh > 0 and m.promotion_cost > 0
            else 0.0
        )
        result.append(dataclasses.replace(
            m,
            promotion_watch_hours=promo_wh,
            organic_watch_hours=organic_wh,
            estimated_qualifying_hours=qualifying_wh,
            cost_per_qualified_hour=cost_per_qual,
            avg_promotion_view_duration_seconds=sim_duration_seconds,
            promotion_duration_estimated=True,
        ))
    return result
