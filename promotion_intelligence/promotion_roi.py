"""ROI estimation for promotion budgets.

Produces ROIEstimate objects for $5 / $10 / $20 / $50 budget tiers.

All projections assume a cost-per-view (CPV) model:
    estimated_views = budget / CPV

The organic_lift_ratio is derived from historical data where available:
    lift_ratio = clamp(organic_views / promo_views × 0.30, 0.10, 0.45)
When no promotion history exists, the ratio is inferred from the
Promotion Opportunity Score (10–30% range).
"""
from __future__ import annotations

from promotion_intelligence.recommendation_models import (
    PromotionOpportunity,
    ROIEstimate,
    VideoFeatures,
)

BUDGET_TIERS: list[float] = [5.0, 10.0, 20.0, 50.0]


class ROICalculator:
    """Estimates expected ROI for each budget tier given a PromotionOpportunity."""

    def __init__(self, cpv: float = 0.025) -> None:
        self.cpv = max(cpv, 0.001)  # guard against zero CPV

    def estimate_roi(self, opp: PromotionOpportunity, budget: float) -> ROIEstimate:
        feat = opp.features

        # ── View projection ────────────────────────────────────────────────
        projected_views = max(1, int(budget / self.cpv))

        # ── Subscribers ────────────────────────────────────────────────────
        sub_rate = feat.subscriber_conversion_per_1k / 1000.0
        projected_subs = max(0, int(projected_views * sub_rate))

        # ── Organic lift ───────────────────────────────────────────────────
        if feat.promotion_views > 0:
            raw_multiplier = feat.organic_views / max(feat.promotion_views, 1)
            lift_ratio = min(max(raw_multiplier * 0.30, 0.10), 0.45)
        else:
            # No history — score-proxy: 10–30% range
            lift_ratio = 0.10 + (opp.score / 100.0) * 0.20
        organic_lift = max(0, int(projected_views * lift_ratio))

        # ── Follow-on views ────────────────────────────────────────────────
        follow_on = max(0, int(projected_views * feat.follow_on_rate_pct / 100.0))

        # ── Qualifying hours ───────────────────────────────────────────────
        # Promoted viewers watch less than organic — use historical avg promo duration
        promo_dur = feat.avg_promotion_view_duration_seconds
        if promo_dur <= 0:
            promo_dur = feat.avg_view_duration_seconds * 0.40  # estimate: 40% of avg
        qual_hours = round(projected_views * promo_dur / 3600.0, 2)

        # ── Expected PES ───────────────────────────────────────────────────
        if feat.promotion_efficiency_score > 0:
            expected_pes = round(
                feat.promotion_efficiency_score * 0.80 + opp.score * 0.20, 1
            )
        else:
            expected_pes = round(opp.score * 0.60, 1)

        # ── Cost metrics ───────────────────────────────────────────────────
        cost_per_qh = round(budget / max(qual_hours, 0.01), 3)
        cost_per_sub = round(budget / max(projected_subs, 1), 3)
        cost_per_fo = round(budget / max(follow_on, 1), 3)

        confidence, reason = _confidence(feat, projected_views)

        return ROIEstimate(
            video_id=feat.video_id,
            budget=budget,
            cpv=self.cpv,
            estimated_views=projected_views,
            estimated_subscribers=projected_subs,
            estimated_organic_lift=organic_lift,
            estimated_follow_on_views=follow_on,
            estimated_qualifying_hours=qual_hours,
            expected_promotion_efficiency=expected_pes,
            cost_per_qualified_hour_projected=cost_per_qh,
            cost_per_subscriber_projected=cost_per_sub,
            cost_per_follow_on_projected=cost_per_fo,
            confidence=confidence,
            confidence_reason=reason,
        )

    def estimate_all_tiers(self, opp: PromotionOpportunity) -> list[ROIEstimate]:
        return [self.estimate_roi(opp, b) for b in BUDGET_TIERS]


def _confidence(feat: VideoFeatures, projected_views: int) -> tuple[str, str]:
    if feat.promotion_views > 200 and feat.data_source == "API_ACTUAL":
        return (
            "high",
            "Historical promotion data available with sufficient volume.",
        )
    if feat.promotion_views > 0:
        return (
            "medium",
            "Limited promotion history available — treat larger budgets as directional.",
        )
    if feat.organic_views >= 500:
        return (
            "medium",
            "Strong organic history; projecting from video performance patterns only.",
        )
    return (
        "low",
        "Insufficient data — estimate is directional only. Recommend a $5 test run first.",
    )
