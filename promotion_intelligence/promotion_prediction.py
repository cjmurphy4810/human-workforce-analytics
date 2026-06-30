"""Forward-looking promotion outcome prediction.

Separate from ROICalculator so callers can invoke individual predictions
(e.g. just predict_views for quick what-ifs) without constructing a full
ROIEstimate.
"""
from __future__ import annotations

from promotion_intelligence.recommendation_models import PromotionOpportunity


class PromotionPredictor:
    """Predict individual promotion outcome metrics for a given budget.

    Default CPV = $0.025 (YouTube in-stream mid-range).
    Callers should pass a CPV calibrated from actual channel spend history
    when available.
    """

    def __init__(self, cpv: float = 0.025) -> None:
        self.cpv = max(cpv, 0.001)

    # ── Individual predictors ──────────────────────────────────────────────────

    def predict_views(self, budget: float) -> int:
        return max(1, int(budget / self.cpv))

    def predict_subscribers(self, projected_views: int, sub_rate_per_1k: float) -> int:
        return max(0, int(projected_views * sub_rate_per_1k / 1000.0))

    def predict_organic_lift(
        self,
        projected_views: int,
        organic_multiplier: float,
        score: float,
    ) -> int:
        """Estimate organic views gained as a side-effect of promotion."""
        if organic_multiplier > 0:
            lift_ratio = min(organic_multiplier * 0.30, 0.45)
        else:
            lift_ratio = 0.10 + (score / 100.0) * 0.20
        return max(0, int(projected_views * lift_ratio))

    def predict_follow_on(
        self,
        projected_views: int,
        follow_on_rate_pct: float,
    ) -> int:
        return max(0, int(projected_views * follow_on_rate_pct / 100.0))

    def predict_qualifying_hours(
        self,
        projected_views: int,
        avg_view_duration_s: float,
        avg_promo_duration_s: float = 0.0,
    ) -> float:
        """Qualifying hours = promoted views × avg promotional view duration."""
        effective_dur = avg_promo_duration_s if avg_promo_duration_s > 0 else avg_view_duration_s * 0.40
        return round(projected_views * effective_dur / 3600.0, 2)

    def predict_efficiency_score(
        self,
        opp: PromotionOpportunity,
        organic_lift: int,
        projected_views: int,
    ) -> float:
        if projected_views == 0:
            return 0.0
        lift_signal = min(organic_lift / projected_views * 100.0, 100.0)
        historical = opp.features.promotion_efficiency_score
        if historical > 0:
            return round(historical * 0.70 + lift_signal * 0.30, 1)
        return round(min(lift_signal + opp.score * 0.40, 100.0), 1)

    # ── Convenience: predict all metrics for one budget ───────────────────────

    def predict_all(
        self,
        opp: PromotionOpportunity,
        budget: float,
    ) -> dict[str, float | int]:
        feat = opp.features
        views = self.predict_views(budget)
        subs = self.predict_subscribers(views, feat.subscriber_conversion_per_1k)
        lift = self.predict_organic_lift(views, feat.organic_multiplier, opp.score)
        follow_on = self.predict_follow_on(views, feat.follow_on_rate_pct)
        qual_h = self.predict_qualifying_hours(
            views,
            feat.avg_view_duration_seconds,
            feat.avg_promotion_view_duration_seconds,
        )
        pes = self.predict_efficiency_score(opp, lift, views)
        return {
            "budget": budget,
            "views": views,
            "subscribers": subs,
            "organic_lift": lift,
            "follow_on_views": follow_on,
            "qualifying_hours": qual_h,
            "promotion_efficiency_score": pes,
        }

    def explain_prediction(
        self,
        budget: float,
        opp: PromotionOpportunity,
        projected_views: int,
        qualifying_hours: float,
    ) -> str:
        feat = opp.features
        parts: list[str] = [
            f"At a ${budget:.0f} budget with estimated CPV of ${self.cpv:.3f},"
            f" expect ~{projected_views:,} ad impressions."
        ]
        if feat.audience_retention_pct >= 60:
            parts.append(
                f"Strong retention ({feat.audience_retention_pct:.0f}%) suggests"
                " viewers will engage meaningfully with the content."
            )
        elif feat.audience_retention_pct < 40:
            parts.append("Low audience retention may limit qualifying hours generated.")

        if feat.subscriber_conversion_per_1k >= 5:
            parts.append(
                f"This video converts ~{feat.subscriber_conversion_per_1k:.1f} subscribers"
                " per 1,000 views — promotion should generate measurable subscriber growth."
            )
        if qualifying_hours >= 5:
            parts.append(
                f"Projected {qualifying_hours:.1f} qualifying hours contributes"
                " directly to YPP eligibility."
            )
        return " ".join(parts)
