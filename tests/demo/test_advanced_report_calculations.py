from datetime import date

import pytest

from demo.analytics import (
    aggregate_video_window,
    content_repackaging_rows,
    content_tier_rows,
    eligible_organic_watch_hours,
    filter_promotion_opportunities,
    organic_window_totals,
    rank_persisted_content_rows,
)
from demo.config import DEMO_CHANNEL_KEY
from demo.db import connect_demo_db
from promotion_intelligence.promotion_prediction import PromotionPredictor
from promotion_intelligence.promotion_roi import ROICalculator
from promotion_intelligence.recommendation_models import (
    PromotionClass,
    PromotionOpportunity,
    ScoreBreakdown,
    VideoFeatures,
)


def _opportunity(
    video_id: str,
    *,
    length_seconds: int = 600,
    topic: str = "Agents",
    score: float = 50.0,
) -> PromotionOpportunity:
    features = VideoFeatures(
        video_id=video_id,
        title=video_id,
        total_views=1_000,
        organic_views=1_000,
        promotion_views=0,
        subscribers_gained=20,
        follow_on_views=100,
        likes=50,
        total_watch_hours=50.0,
        organic_watch_hours=50.0,
        qualifying_hours=50.0 if length_seconds > 180 else 0.0,
        avg_view_duration_seconds=180.0,
        avg_promotion_view_duration_seconds=60.0,
        audience_retention_pct=30.0,
        subscriber_conversion_per_1k=20.0,
        views_per_day=10.0,
        follow_on_rate_pct=10.0,
        promotion_ratio_pct=0.0,
        organic_multiplier=0.0,
        promotion_efficiency_score=50.0,
        ci_overall_score=50.0,
        cpv=0.025,
        promotion_cost_estimated=0.0,
        cost_per_qualified_hour=0.0,
        cost_per_subscriber=0.0,
        cost_per_follow_on_view=0.0,
        video_age_days=100,
        length_seconds=length_seconds,
        topic=topic,
    )
    return PromotionOpportunity(
        features=features,
        score=score,
        breakdown=ScoreBreakdown(0, 0, 0, 0, 0, 0),
        classification=PromotionClass.watch_organically,
        explanation="fixture",
    )


def test_eligible_organic_watch_hours_zeroes_shorts_and_subtracts_advertising():
    assert eligible_organic_watch_hours(180, 20.0, 5.0) == 0.0
    assert eligible_organic_watch_hours(181, 20.0, 5.0) == 15.0
    assert eligible_organic_watch_hours(600, 5.0, 20.0) == 0.0


def test_organic_window_totals_remove_same_window_advertising():
    result = organic_window_totals(
        {
            "views": 100,
            "estimated_minutes_watched": 1_200.0,
            "traffic_sources": {
                "ADVERTISING": {
                    "views": 70,
                    "estimated_minutes_watched": 900.0,
                }
            },
        }
    )

    assert result == {"views": 30, "watch_hours": 5.0}


def test_organic_window_growth_inputs_ignore_a_paid_campaign_spike():
    baseline = organic_window_totals(
        {
            "views": 100,
            "estimated_minutes_watched": 600.0,
            "traffic_sources": {},
        }
    )
    recent = organic_window_totals(
        {
            "views": 300,
            "estimated_minutes_watched": 1_800.0,
            "traffic_sources": {
                "ADVERTISING": {
                    "views": 200,
                    "estimated_minutes_watched": 1_200.0,
                }
            },
        }
    )

    assert recent == baseline == {"views": 100, "watch_hours": 10.0}


def test_render_group_qualifying_total_sums_per_video_eligibility():
    rows = [
        (120, 8.0, 2.0),
        (600, 20.0, 5.0),
        (900, 4.0, 10.0),
    ]

    total = sum(
        eligible_organic_watch_hours(duration, watch, advertising)
        for duration, watch, advertising in rows
    )

    assert total == 15.0


def test_aggregate_video_window_counts_actual_observed_days(tmp_path):
    path = tmp_path / "observations.db"
    with connect_demo_db(path) as conn:
        conn.execute(
            "INSERT INTO videos(channel, video_id, title) VALUES (?, 'video_1', 'Fixture')",
            (DEMO_CHANNEL_KEY,),
        )
        conn.executemany(
            "INSERT INTO daily_video_metrics "
            "(metric_date, channel, video_id, views, estimated_minutes_watched, "
            "average_view_duration, likes, subscribers_gained) "
            "VALUES (?, ?, 'video_1', 10, 10, 60, 0, 0)",
            [
                ("2026-08-12", DEMO_CHANNEL_KEY),
                ("2026-08-14", DEMO_CHANNEL_KEY),
            ],
        )
        conn.commit()

    rows = aggregate_video_window(
        path,
        start=date(2026, 8, 10),
        end=date(2026, 8, 14),
        channel=DEMO_CHANNEL_KEY,
        include_observed_days=True,
    )

    assert rows[0]["observed_days"] == 2


def test_roi_uses_only_eligible_organic_lift_watch_hours():
    long_form = _opportunity("long")
    short = _opportunity("short", length_seconds=180)

    long_estimate = ROICalculator(cpv=0.025).estimate_roi(long_form, 10.0)
    short_estimate = ROICalculator(cpv=0.025).estimate_roi(short, 10.0)

    assert long_estimate.estimated_views == 400
    assert long_estimate.estimated_organic_lift == 80
    assert long_estimate.estimated_qualifying_hours == pytest.approx(4.0)
    assert short_estimate.estimated_organic_lift == 80
    assert short_estimate.estimated_qualifying_hours == 0.0


def test_predictor_uses_organic_lift_and_short_eligibility_for_qualifying_hours():
    predictor = PromotionPredictor(cpv=0.025)

    long_result = predictor.predict_all(_opportunity("long"), 10.0)
    short_result = predictor.predict_all(
        _opportunity("short", length_seconds=180), 10.0
    )

    assert long_result["organic_lift"] == 80
    assert long_result["qualifying_hours"] == pytest.approx(4.0)
    assert short_result["qualifying_hours"] == 0.0


def test_promotion_filter_output_is_the_card_input_population():
    agents = _opportunity("agents", topic="Agents", score=60.0)
    retrieval = _opportunity("retrieval", topic="Retrieval", score=90.0)
    low_score = _opportunity("low", topic="Agents", score=20.0)

    filtered = filter_promotion_opportunities(
        [agents, retrieval, low_score],
        topics={"Agents"},
        minimum_score=50.0,
    )

    assert filtered == [agents]
    assert filter_promotion_opportunities(
        [agents, retrieval, low_score], topics=set(), minimum_score=50.0
    ) == [agents, retrieval]


def test_content_rank_and_tab_membership_use_only_persisted_score_fields():
    rows = [
        {
            "video_id": "live-label-only",
            "tier": "average",
            "overall_score": 99.0,
            "watch_rate_pct": 80.0,
            "classifications": ["subscriber_magnet"],
        },
        {
            "video_id": "persisted-magnet",
            "tier": "subscriber_magnet",
            "overall_score": 40.0,
            "watch_rate_pct": 30.0,
            "classifications": [],
        },
        {
            "video_id": "persisted-repackage",
            "tier": "underperformer",
            "overall_score": 10.0,
            "watch_rate_pct": 50.0,
            "classifications": [],
        },
    ]

    ranked = rank_persisted_content_rows(rows)

    assert [row["video_id"] for row in ranked] == [
        "live-label-only",
        "persisted-magnet",
        "persisted-repackage",
    ]
    assert [
        row["video_id"] for row in content_tier_rows(ranked, "subscriber_magnet")
    ] == ["persisted-magnet"]
    assert [row["video_id"] for row in content_repackaging_rows(ranked)] == [
        "persisted-repackage"
    ]
