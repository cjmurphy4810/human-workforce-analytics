"""Pure organic-momentum scoring shared by production and the public demo."""

from __future__ import annotations

from typing import Optional

from models.organic_momentum import (
    MOMENTUM_CLASS_ACTION,
    MomentumClass,
    OrganicMomentumMetrics,
    PromotionStatus,
    ScoreBreakdown,
    ScoreWeights,
)


_MIN_VIEWS = 100
_MIN_DATA_POINTS = 3


def normalize_metric(value: float, min_value: float, max_value: float) -> float:
    """Normalize *value* to [0, 100] within [min_value, max_value]."""
    if max_value <= min_value:
        return 50.0
    return max(
        0.0,
        min(100.0, (value - min_value) / (max_value - min_value) * 100.0),
    )


def calculate_growth_rate(current: float, baseline: float) -> float:
    """Fractional change from *baseline* to *current* (0.0 when baseline is 0)."""
    if baseline <= 0:
        return 0.0
    return (current - baseline) / baseline


def calculate_post_promotion_lift(pre_daily: float, post_daily: float) -> float:
    """Absolute lift in daily metric after promotion vs before."""
    return post_daily - pre_daily


def _percentile_rank(value: float, sorted_vals: list[float]) -> float:
    """Return [0, 1] rank of *value* within *sorted_vals*."""
    n = len(sorted_vals)
    if n <= 1:
        return 0.5
    lo, hi = sorted_vals[0], sorted_vals[-1]
    if hi <= lo:
        return 0.5
    clamped = max(lo, min(hi, value))
    lo_idx, hi_idx = 0, n - 1
    while lo_idx < hi_idx:
        mid = (lo_idx + hi_idx) // 2
        if sorted_vals[mid] < clamped:
            lo_idx = mid + 1
        else:
            hi_idx = mid
    return lo_idx / (n - 1)


class _PopStats:
    def __init__(self, records: list[OrganicMomentumMetrics]) -> None:
        self._view_gr = sorted(record.view_growth_rate for record in records)
        self._wh_gr = sorted(record.wh_growth_rate for record in records)
        self._org_ratio = sorted(
            record.organic_views / max(record.total_views, 1) for record in records
        )
        self._completion = sorted(
            record.average_view_duration_seconds
            / max(record.video_length_seconds, 1)
            for record in records
        )
        self._sub_per_1k = sorted(
            record.subscribers / max(record.organic_views, 1) * 1000
            for record in records
        )
        self._ret_proxy = sorted(
            record.subscribers / max(record.total_views, 1) for record in records
        )

    def prank(self, field: str, value: float) -> float:
        return _percentile_rank(value, getattr(self, f"_{field}"))


def calculate_organic_momentum_score(
    metric: OrganicMomentumMetrics,
    population: _PopStats,
    weights: ScoreWeights,
) -> tuple[float, ScoreBreakdown]:
    """Return (score_0_to_100, ScoreBreakdown) for one video."""
    organic_ratio = metric.organic_views / max(metric.total_views, 1)
    completion = metric.average_view_duration_seconds / max(
        metric.video_length_seconds, 1
    )
    subscribers_per_1k = metric.subscribers / max(metric.organic_views, 1) * 1000
    returning_proxy = metric.subscribers / max(metric.total_views, 1)

    view_growth = population.prank("view_gr", metric.view_growth_rate) * 100
    watch_growth = population.prank("wh_gr", metric.wh_growth_rate) * 100
    organic = population.prank("org_ratio", organic_ratio) * 100
    completion_rank = population.prank("completion", completion) * 100
    average_percentage = min(completion * 100.0, 100.0)
    subscribers = population.prank("sub_per_1k", subscribers_per_1k) * 100
    returning = population.prank("ret_proxy", returning_proxy) * 100

    breakdown = ScoreBreakdown(
        organic_views_growth=round(weights.organic_views_growth * view_growth, 2),
        organic_wh_growth=round(weights.organic_wh_growth * watch_growth, 2),
        organic_ratio=round(weights.organic_ratio * organic, 2),
        completion_rate=round(weights.completion_rate * completion_rank, 2),
        avg_pct_viewed=round(weights.avg_pct_viewed * average_percentage, 2),
        subscriber_conversion=round(
            weights.subscriber_conversion * subscribers, 2
        ),
        returning_proxy=round(weights.returning_proxy * returning, 2),
        follow_on_proxy=round(weights.follow_on_proxy * 50.0, 2),
        ctr_proxy=round(weights.ctr_proxy * 50.0, 2),
    )
    return breakdown.total, breakdown


def classify_momentum(
    score: float,
    metric: OrganicMomentumMetrics,
    median_views: float,
) -> MomentumClass:
    """Classify one scored video with deterministic priority rules."""
    if metric.total_views < _MIN_VIEWS or metric.data_points < _MIN_DATA_POINTS:
        return MomentumClass.insufficient_data

    organic_ratio = metric.organic_views / max(metric.total_views, 1)
    completion = metric.average_view_duration_seconds / max(
        metric.video_length_seconds, 1
    )
    if (
        metric.promotion_status == PromotionStatus.promoted
        and organic_ratio < 0.45
        and score < 55
    ):
        return MomentumClass.paid_spike
    if completion < 0.25 and score < 50:
        return MomentumClass.retention_problem
    if completion > 0.55 and metric.total_views < median_views * 0.7 and score < 60:
        return MomentumClass.needs_packaging
    if metric.total_views < median_views and metric.view_growth_rate > 0.05 and score >= 35:
        return MomentumClass.organic_sleeper
    if score >= 80:
        return MomentumClass.breakout
    if score >= 60:
        return MomentumClass.promising
    return MomentumClass.do_not_promote


def recommend_action(
    classification: MomentumClass,
    metric: OrganicMomentumMetrics,
) -> str:
    """Return the action attached to one classification."""
    base = MOMENTUM_CLASS_ACTION.get(classification, "Monitor")
    if classification == MomentumClass.breakout:
        if metric.promotion_cost > 0:
            return (
                "Scale Promotion — strong organic ROI at "
                f"${metric.organic_momentum_per_dollar:.2f} organic WH/$"
            )
        return "Scale Promotion — strong organic growth, test a $10–$20 budget"
    if classification == MomentumClass.promising:
        return "Monitor organically 2–3 more weeks, then test a $5 promotion"
    if classification == MomentumClass.paid_spike:
        return (
            "Pause Promotion — organic uplift weak; refresh thumbnail/title "
            "before re-promoting"
        )
    if classification == MomentumClass.organic_sleeper:
        return "Add to Playlist + Create Follow-Up — growing without paid spend"
    if classification == MomentumClass.needs_packaging:
        if metric.average_percentage_viewed > 60:
            return "Refresh Thumbnail — retention strong but discovery weak"
        return "Rewrite Title + Refresh Thumbnail — good watch time, poor click-through"
    if classification == MomentumClass.retention_problem:
        return (
            "Review first 30 seconds — viewers leaving early; do not promote until fixed"
        )
    if classification == MomentumClass.do_not_promote:
        return "Do Not Invest — weak across major indicators"
    return base


class MomentumScorer:
    """Score, classify, and rank a list of organic-momentum metrics."""

    def __init__(self, weights: Optional[ScoreWeights] = None) -> None:
        self._weights = weights or ScoreWeights()
        self._weights.validate()

    def score_all(
        self, metrics: list[OrganicMomentumMetrics]
    ) -> list[OrganicMomentumMetrics]:
        if not metrics:
            return []

        population = _PopStats(metrics)
        views = sorted(metric.total_views for metric in metrics)
        median_views = float(views[len(views) // 2]) if views else 0.0
        for metric in metrics:
            score, breakdown = calculate_organic_momentum_score(
                metric, population, self._weights
            )
            classification = classify_momentum(score, metric, median_views)
            metric.organic_momentum_score = score
            metric.score_breakdown = breakdown
            metric.classification = classification
            metric.recommended_action = recommend_action(classification, metric)

        metrics.sort(
            key=lambda metric: metric.organic_momentum_score,
            reverse=True,
        )
        return metrics
