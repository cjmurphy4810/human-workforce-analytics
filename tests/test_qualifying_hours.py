import dataclasses

import pytest

from analytics.qualifying_hours import (
    compute_qualifying_hours,
    recompute_with_sim_duration,
)
from models.promotion import make_metrics


def _metric(video_id: str, *, length: int, total_hours: float, promo_hours: float):
    metric = make_metrics(
        video_id=video_id,
        title=video_id,
        published=None,
        length_seconds=length,
        total_views=100,
        promotion_views=20,
        total_watch_hours=total_hours,
        avg_promotion_view_duration_seconds=promo_hours * 3600 / 20,
        promotion_cost=0,
        subscribers=0,
        follow_on_views=0,
    )
    return metric


def test_report_distinguishes_organic_from_monetization_qualifying_hours():
    long_video = _metric("long", length=600, total_hours=10, promo_hours=2)
    short_video = dataclasses.replace(
        _metric("short", length=120, total_hours=4, promo_hours=1),
        estimated_qualifying_hours=0,
    )

    report = compute_qualifying_hours([long_video, short_video])

    assert report.promotion_watch_hours == pytest.approx(3)
    assert report.organic_watch_hours == pytest.approx(11)
    assert report.estimated_qualifying_hours == pytest.approx(8)


def test_simulation_keeps_shorts_excluded_from_qualifying_hours():
    short_video = dataclasses.replace(
        _metric("short", length=120, total_hours=4, promo_hours=1),
        estimated_qualifying_hours=0,
    )

    [result] = recompute_with_sim_duration([short_video], 90)

    assert result.organic_watch_hours == pytest.approx(3.5)
    assert result.estimated_qualifying_hours == 0
