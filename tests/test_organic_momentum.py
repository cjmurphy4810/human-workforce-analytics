"""Unit tests for analytics/organic_momentum pure functions."""
from __future__ import annotations

import pytest

from analytics.organic_momentum import (
    MomentumScorer,
    ScoreWeights,
    _percentile_rank,
    _PopStats,
    calculate_growth_rate,
    calculate_organic_momentum_score,
    calculate_post_promotion_lift,
    classify_momentum,
    normalize_metric,
    recommend_action,
)
from models.organic_momentum import (
    MomentumClass,
    OrganicMomentumMetrics,
    PromotionStatus,
    ScoreBreakdown,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────


def _make_metric(
    video_id: str = "v1",
    total_views: int = 5000,
    organic_views: int = 4000,
    promotion_views: int = 1000,
    subscribers: int = 20,
    avg_dur: float = 180.0,
    length_s: int = 360,
    view_growth_rate: float = 0.10,
    wh_growth_rate: float = 0.08,
    recent_daily: float = 50.0,
    peak_daily: float = 200.0,
    data_points: int = 30,
    promotion_status: PromotionStatus = PromotionStatus.promoted,
) -> OrganicMomentumMetrics:
    return OrganicMomentumMetrics(
        video_id=video_id,
        title=f"Test Video {video_id}",
        published_date="2026-05-01",
        video_length_seconds=length_s,
        promotion_status=promotion_status,
        promotion_start_date=None,
        promotion_end_date=None,
        promotion_cost=0.0,
        total_views=total_views,
        organic_views=organic_views,
        promotion_views=promotion_views,
        post_promotion_organic_views=int(organic_views * 0.7),
        total_watch_hours=round(total_views * avg_dur / 3600.0, 2),
        estimated_qualifying_watch_hours=round(organic_views * avg_dur / 3600.0, 2),
        post_promotion_organic_watch_hours=round(organic_views * avg_dur / 3600.0 * 0.7, 2),
        average_view_duration_seconds=avg_dur,
        average_percentage_viewed=round(avg_dur / length_s * 100, 1),
        ctr=0.0,
        impressions=0,
        engaged_views=0,
        returning_viewers=0,
        subscribers=subscribers,
        follow_on_views=0,
        browse_views=0,
        suggested_views=0,
        search_views=0,
        organic_lift=organic_views / max(promotion_views, 1),
        organic_watch_hour_lift=0.0,
        organic_momentum_per_dollar=0.0,
        view_growth_rate=view_growth_rate,
        wh_growth_rate=wh_growth_rate,
        recent_daily_views=recent_daily,
        peak_daily_views=peak_daily,
        data_points=data_points,
        organic_momentum_score=0.0,
        score_breakdown=ScoreBreakdown(),
        classification=MomentumClass.insufficient_data,
        recommended_action="",
        data_quality_flag="actual",
    )


# ── normalize_metric ──────────────────────────────────────────────────────────


def test_normalize_metric_midpoint() -> None:
    assert normalize_metric(50.0, 0.0, 100.0) == pytest.approx(50.0)


def test_normalize_metric_clamps_high() -> None:
    assert normalize_metric(150.0, 0.0, 100.0) == 100.0


def test_normalize_metric_clamps_low() -> None:
    assert normalize_metric(-10.0, 0.0, 100.0) == 0.0


def test_normalize_metric_degenerate_range() -> None:
    assert normalize_metric(5.0, 5.0, 5.0) == 50.0


# ── calculate_growth_rate ────────────────────────────────────────────────────


def test_growth_rate_positive() -> None:
    assert calculate_growth_rate(120.0, 100.0) == pytest.approx(0.20)


def test_growth_rate_negative() -> None:
    assert calculate_growth_rate(80.0, 100.0) == pytest.approx(-0.20)


def test_growth_rate_zero_baseline() -> None:
    assert calculate_growth_rate(50.0, 0.0) == 0.0


def test_growth_rate_no_change() -> None:
    assert calculate_growth_rate(100.0, 100.0) == pytest.approx(0.0)


# ── calculate_post_promotion_lift ─────────────────────────────────────────────


def test_lift_positive() -> None:
    assert calculate_post_promotion_lift(30.0, 50.0) == pytest.approx(20.0)


def test_lift_negative() -> None:
    assert calculate_post_promotion_lift(50.0, 20.0) == pytest.approx(-30.0)


# ── _percentile_rank ──────────────────────────────────────────────────────────


def test_percentile_rank_minimum() -> None:
    assert _percentile_rank(0.0, [0.0, 50.0, 100.0]) == pytest.approx(0.0)


def test_percentile_rank_maximum() -> None:
    assert _percentile_rank(100.0, [0.0, 50.0, 100.0]) == pytest.approx(1.0)


def test_percentile_rank_midpoint() -> None:
    assert _percentile_rank(50.0, [0.0, 50.0, 100.0]) == pytest.approx(0.5)


def test_percentile_rank_single_element() -> None:
    assert _percentile_rank(42.0, [42.0]) == 0.5


def test_percentile_rank_empty() -> None:
    assert _percentile_rank(5.0, []) == 0.5


def test_percentile_rank_degenerate_range() -> None:
    assert _percentile_rank(5.0, [5.0, 5.0, 5.0]) == 0.5


# ── ScoreWeights validation ───────────────────────────────────────────────────


def test_score_weights_default_valid() -> None:
    w = ScoreWeights()
    w.validate()  # should not raise


def test_score_weights_invalid_raises() -> None:
    w = ScoreWeights(organic_views_growth=0.99)
    with pytest.raises(ValueError, match="sum to 1.0"):
        w.validate()


# ── calculate_organic_momentum_score ─────────────────────────────────────────


def test_score_returns_in_range() -> None:
    m1 = _make_metric("v1", view_growth_rate=0.50, wh_growth_rate=0.40)
    m2 = _make_metric("v2", view_growth_rate=-0.20, wh_growth_rate=-0.15)
    m3 = _make_metric("v3", view_growth_rate=0.05, wh_growth_rate=0.03)
    pop = _PopStats([m1, m2, m3])
    weights = ScoreWeights()
    for m in [m1, m2, m3]:
        score, bd = calculate_organic_momentum_score(m, pop, weights)
        assert 0.0 <= score <= 100.0
        assert 0.0 <= bd.total <= 100.0


def test_high_growth_scores_higher() -> None:
    high = _make_metric("vh", view_growth_rate=0.80, wh_growth_rate=0.70)
    low = _make_metric("vl", view_growth_rate=-0.50, wh_growth_rate=-0.40)
    pop = _PopStats([high, low])
    weights = ScoreWeights()
    score_high, _ = calculate_organic_momentum_score(high, pop, weights)
    score_low, _ = calculate_organic_momentum_score(low, pop, weights)
    assert score_high > score_low


# ── classify_momentum ────────────────────────────────────────────────────────


def test_classify_insufficient_data_low_views() -> None:
    m = _make_metric(total_views=50)
    assert classify_momentum(80.0, m, 5000.0) == MomentumClass.insufficient_data


def test_classify_insufficient_data_few_points() -> None:
    m = _make_metric(data_points=1)
    assert classify_momentum(80.0, m, 5000.0) == MomentumClass.insufficient_data


def test_classify_breakout() -> None:
    m = _make_metric(total_views=10000, organic_views=9000)
    assert classify_momentum(85.0, m, 5000.0) == MomentumClass.breakout


def test_classify_paid_spike() -> None:
    # Mostly paid traffic, low score
    m = _make_metric(
        total_views=10000,
        organic_views=3000,
        promotion_views=7000,
        promotion_status=PromotionStatus.promoted,
    )
    cls = classify_momentum(40.0, m, 5000.0)
    assert cls == MomentumClass.paid_spike


def test_classify_organic_sleeper() -> None:
    m = _make_metric(
        total_views=500,
        organic_views=500,
        promotion_views=0,
        view_growth_rate=0.30,
        promotion_status=PromotionStatus.not_promoted,
    )
    cls = classify_momentum(50.0, m, 5000.0)
    assert cls == MomentumClass.organic_sleeper


def test_classify_retention_problem_low_completion() -> None:
    m = _make_metric(avg_dur=30.0, length_s=360, total_views=5000)
    cls = classify_momentum(35.0, m, 5000.0)
    assert cls == MomentumClass.retention_problem


# ── recommend_action ─────────────────────────────────────────────────────────


def test_recommend_action_breakout() -> None:
    m = _make_metric()
    action = recommend_action(MomentumClass.breakout, m)
    assert "Scale" in action or "strong" in action.lower()


def test_recommend_action_paid_spike() -> None:
    m = _make_metric()
    action = recommend_action(MomentumClass.paid_spike, m)
    assert "Pause" in action


def test_recommend_action_retention_problem() -> None:
    m = _make_metric()
    action = recommend_action(MomentumClass.retention_problem, m)
    assert "first 30" in action or "Review" in action


# ── MomentumScorer integration ────────────────────────────────────────────────


def test_scorer_populates_scores() -> None:
    metrics = [_make_metric(f"v{i}") for i in range(5)]
    scored = MomentumScorer().score_all(metrics)
    assert all(m.organic_momentum_score > 0 for m in scored)


def test_scorer_ranks_descending() -> None:
    metrics = [_make_metric(f"v{i}") for i in range(5)]
    scored = MomentumScorer().score_all(metrics)
    scores = [m.organic_momentum_score for m in scored]
    assert scores == sorted(scores, reverse=True)


def test_scorer_sets_classification() -> None:
    metrics = [_make_metric(f"v{i}") for i in range(5)]
    scored = MomentumScorer().score_all(metrics)
    assert all(m.classification != MomentumClass.insufficient_data for m in scored
               if m.total_views >= 100 and m.data_points >= 3)


def test_scorer_empty_input() -> None:
    assert MomentumScorer().score_all([]) == []
