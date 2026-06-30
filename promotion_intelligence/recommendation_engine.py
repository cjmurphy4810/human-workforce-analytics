"""Promotion Opportunity Scoring and Classification Engine.

Scoring weights (sum to 100):
    Audience Retention        25 pts  — core content-quality signal
    Subscriber Conversion     20 pts  — long-term channel-growth value
    Organic Watch Hours       20 pts  — direct YPP contribution (population-relative)
    Views Per Day             15 pts  — organic momentum to amplify (population-relative)
    Follow-on Rate            10 pts  — discovery multiplier effect
    Promotion Efficiency      10 pts  — historical promotion ROI

Classification thresholds (applied in priority order):
    1. Needs More Organic Data — organic_views < MIN or age < MIN_DAYS
    2. Already Saturated       — promo_ratio > SAT_RATIO and PES < SAT_PES
    3. Promote Immediately     — score ≥ 60
    4. Watch Organically       — score ≥ 35
    5. Do Not Promote          — else
"""
from __future__ import annotations

from typing import Any, Callable, Optional

from promotion_intelligence.recommendation_models import (
    PromotionClass,
    PromotionOpportunity,
    RecommendationCards,
    ScoreBreakdown,
    VideoFeatures,
)


# ── Internal helpers ──────────────────────────────────────────────────────────


def _percentile_rank(value: float, sorted_vals: list[float]) -> float:
    """Return [0, 1] rank of value within sorted_vals (0.0 = lowest)."""
    n = len(sorted_vals)
    if n == 0:
        return 0.5
    if n == 1:
        return 0.5
    lo, hi = sorted_vals[0], sorted_vals[-1]
    if hi <= lo:
        return 0.5
    clamped = max(lo, min(hi, value))
    # Linear interpolation within the sorted list
    lo_idx = 0
    hi_idx = n - 1
    while lo_idx < hi_idx:
        mid = (lo_idx + hi_idx) // 2
        if sorted_vals[mid] < clamped:
            lo_idx = mid + 1
        else:
            hi_idx = mid
    return lo_idx / (n - 1)


def _clip_norm(value: float, ceiling: float) -> float:
    """Normalize value to [0, 1] capped at ceiling."""
    if ceiling <= 0:
        return 0.0
    return min(value / ceiling, 1.0)


class _PopStats:
    """Population-level statistics for relative normalization."""

    def __init__(self, features: list[VideoFeatures]) -> None:
        self._organic_hours = sorted(f.organic_watch_hours for f in features)
        self._vpds = sorted(f.views_per_day for f in features)

    def organic_hours_rank(self, value: float) -> float:
        return _percentile_rank(value, self._organic_hours)

    def vpd_rank(self, value: float) -> float:
        return _percentile_rank(value, self._vpds)


# ── Recommendation engine ─────────────────────────────────────────────────────


class RecommendationEngine:
    """Score, classify, explain, and rank all videos for promotion potential."""

    # Absolute normalization ceilings
    _RETENTION_CEIL = 100.0
    _SUB_CEIL_PER_1K = 30.0     # 30 subs per 1,000 organic views = exceptional
    _FOLLOW_ON_CEIL = 30.0       # 30% follow-on rate = excellent

    # Classification thresholds (configurable via constructor)
    _MIN_ORGANIC_VIEWS = 50
    _MIN_AGE_DAYS = 14
    _SAT_PROMO_RATIO = 65.0     # % — already saturated above this
    _SAT_MIN_PES = 40.0         # PES below this confirms saturation
    _PROMOTE_THRESHOLD = 60.0
    _WATCH_THRESHOLD = 35.0

    def __init__(
        self,
        all_features: list[VideoFeatures],
        min_organic_views: int = 50,
        min_age_days: int = 14,
        saturation_promo_ratio: float = 65.0,
    ) -> None:
        self._all = all_features
        self._pop = _PopStats(all_features)
        self._MIN_ORGANIC_VIEWS = min_organic_views
        self._MIN_AGE_DAYS = min_age_days
        self._SAT_PROMO_RATIO = saturation_promo_ratio

    # ── Scoring ───────────────────────────────────────────────────────────────

    def score_video(self, feat: VideoFeatures) -> tuple[float, ScoreBreakdown]:
        """Return (composite_score in [0, 100], ScoreBreakdown)."""
        retention_pts = 25.0 * _clip_norm(feat.audience_retention_pct, self._RETENTION_CEIL)
        sub_pts = 20.0 * _clip_norm(feat.subscriber_conversion_per_1k, self._SUB_CEIL_PER_1K)
        organic_pts = 20.0 * self._pop.organic_hours_rank(feat.organic_watch_hours)
        vpd_pts = 15.0 * self._pop.vpd_rank(feat.views_per_day)
        follow_on_pts = 10.0 * _clip_norm(feat.follow_on_rate_pct, self._FOLLOW_ON_CEIL)

        # PES: 0 means never promoted → neutral 0.5 to not penalize new content
        pes_norm = (
            feat.promotion_efficiency_score / 100.0
            if feat.promotion_efficiency_score > 0
            else 0.5
        )
        pes_pts = 10.0 * pes_norm

        breakdown = ScoreBreakdown(
            retention=round(retention_pts, 2),
            subscriber_conversion=round(sub_pts, 2),
            organic_hours=round(organic_pts, 2),
            views_per_day=round(vpd_pts, 2),
            follow_on_rate=round(follow_on_pts, 2),
            promotion_efficiency=round(pes_pts, 2),
        )
        return breakdown.total, breakdown

    # ── Classification ────────────────────────────────────────────────────────

    def classify_video(self, feat: VideoFeatures, score: float) -> PromotionClass:
        """Rule-based classification applied in priority order."""
        if feat.organic_views < self._MIN_ORGANIC_VIEWS or feat.video_age_days < self._MIN_AGE_DAYS:
            return PromotionClass.needs_more_data

        if (
            feat.promotion_ratio_pct >= self._SAT_PROMO_RATIO
            and feat.promotion_efficiency_score > 0
            and feat.promotion_efficiency_score < self._SAT_MIN_PES
        ):
            return PromotionClass.already_saturated

        if score >= self._PROMOTE_THRESHOLD:
            return PromotionClass.promote_immediately

        if score >= self._WATCH_THRESHOLD:
            return PromotionClass.watch_organically

        return PromotionClass.do_not_promote

    # ── Explainability ────────────────────────────────────────────────────────

    def explain(self, opp: PromotionOpportunity, pop_avgs: dict[str, float]) -> str:
        """Return a natural-language explanation for the recommendation."""
        feat = opp.features
        cls = opp.classification

        avg_ret = pop_avgs.get("retention", 50.0)
        avg_sub = pop_avgs.get("sub_per_1k", 5.0)
        avg_qh = pop_avgs.get("qualifying_hours", 50.0)
        avg_vpd = pop_avgs.get("vpd", 10.0)

        facts: list[str] = []

        # Retention
        if feat.audience_retention_pct >= avg_ret * 1.15:
            facts.append(
                f"above-average audience retention "
                f"({feat.audience_retention_pct:.0f}% vs {avg_ret:.0f}% avg)"
            )
        elif feat.audience_retention_pct < avg_ret * 0.70:
            facts.append(
                f"below-average audience retention "
                f"({feat.audience_retention_pct:.0f}% vs {avg_ret:.0f}% avg)"
            )

        # Subscriber conversion
        if feat.subscriber_conversion_per_1k >= avg_sub * 1.25:
            facts.append(
                f"high subscriber conversion "
                f"({feat.subscriber_conversion_per_1k:.1f} per 1,000 organic views "
                f"vs {avg_sub:.1f} avg)"
            )
        elif feat.subscriber_conversion_per_1k < avg_sub * 0.50 and feat.organic_views >= 100:
            facts.append(
                f"low subscriber conversion rate "
                f"({feat.subscriber_conversion_per_1k:.1f} per 1,000 organic views)"
            )

        # Qualifying hours vs average
        if feat.qualifying_hours >= avg_qh * 1.30 and avg_qh > 0:
            facts.append(
                f"generated {feat.qualifying_hours / avg_qh:.1f}× more qualifying hours "
                f"than average ({feat.qualifying_hours:.0f} h vs {avg_qh:.0f} h avg)"
            )
        elif feat.qualifying_hours < avg_qh * 0.40 and avg_qh > 0:
            facts.append(
                f"low qualifying hours output "
                f"({feat.qualifying_hours:.0f} h vs {avg_qh:.0f} h avg)"
            )

        # Views per day momentum
        if feat.views_per_day >= avg_vpd * 1.20 and avg_vpd > 0:
            facts.append(f"strong organic momentum ({feat.views_per_day:.0f} views/day)")

        # Promotion efficiency (only if ever promoted)
        if feat.promotion_efficiency_score >= 70:
            facts.append(
                f"historically efficient promotion (PES {feat.promotion_efficiency_score:.0f}/100)"
            )
        elif 0 < feat.promotion_efficiency_score < 35:
            facts.append(
                f"historically poor promotion efficiency (PES {feat.promotion_efficiency_score:.0f}/100)"
            )

        # Follow-on discovery
        if feat.follow_on_rate_pct >= 10:
            facts.append(
                f"high follow-on discovery rate "
                f"({feat.follow_on_rate_pct:.0f}% of views are organic referrals)"
            )

        # Saturation signal
        if feat.promotion_ratio_pct >= self._SAT_PROMO_RATIO:
            facts.append(
                f"{feat.promotion_ratio_pct:.0f}% of views are paid — "
                "organic ceiling may be reached"
            )

        # Data quality
        if not feat.has_sufficient_data:
            if feat.organic_views < self._MIN_ORGANIC_VIEWS:
                facts.append(
                    f"only {feat.organic_views:,} organic views — needs at least "
                    f"{self._MIN_ORGANIC_VIEWS:,} before promotion decisions are reliable"
                )
            else:
                facts.append(
                    f"published {feat.video_age_days} days ago — allow at least "
                    f"{self._MIN_AGE_DAYS} days for organic momentum to develop"
                )

        # Build sentence
        prefix_map: dict[PromotionClass, str] = {
            PromotionClass.promote_immediately: "Recommend promoting",
            PromotionClass.watch_organically: "Recommend monitoring organically",
            PromotionClass.do_not_promote: "Do not promote",
            PromotionClass.needs_more_data: "Awaiting organic data",
            PromotionClass.already_saturated: "Saturation detected",
        }
        prefix = prefix_map.get(cls, "Assessment")

        if facts:
            return f"{prefix}: {'; '.join(facts[:5])}."

        label = "strong" if opp.score >= 60 else ("moderate" if opp.score >= 35 else "low")
        return (
            f"{prefix}: overall Promotion Opportunity Score is {label} "
            f"({opp.score:.0f}/100)."
        )

    # ── Ranking ───────────────────────────────────────────────────────────────

    def rank_all(self) -> list[PromotionOpportunity]:
        """Score, classify, explain, and rank all loaded videos."""
        pop_avgs = self._population_averages()
        opps: list[PromotionOpportunity] = []
        for feat in self._all:
            score, breakdown = self.score_video(feat)
            cls = self.classify_video(feat, score)
            opp = PromotionOpportunity(
                features=feat,
                score=score,
                breakdown=breakdown,
                classification=cls,
                explanation="",
            )
            opp.explanation = self.explain(opp, pop_avgs)
            opps.append(opp)

        opps.sort(key=lambda o: o.score, reverse=True)
        for i, opp in enumerate(opps):
            opp.rank = i + 1
        return opps

    # ── Recommendation cards ──────────────────────────────────────────────────

    def get_cards(self, opps: list[PromotionOpportunity]) -> RecommendationCards:
        """Derive the seven recommendation card categories."""
        promotable = [o for o in opps if o.classification == PromotionClass.promote_immediately]

        overinvested = sorted(
            [
                o for o in opps
                if o.classification in (
                    PromotionClass.already_saturated,
                    PromotionClass.do_not_promote,
                )
                and o.features.promotion_views > 0
            ],
            key=lambda o: o.features.promotion_ratio_pct,
            reverse=True,
        )

        promoted = [o for o in opps if o.features.promotion_views > 0]

        def _best(
            lst: list[PromotionOpportunity],
            key: Callable[[PromotionOpportunity], Any],
        ) -> Optional[PromotionOpportunity]:
            return max(lst, key=key) if lst else None

        return RecommendationCards(
            top_10_to_promote=promotable[:10],
            top_10_to_stop=overinvested[:10],
            most_efficient=_best(promoted, lambda o: o.features.promotion_efficiency_score),
            least_efficient=(
                min(promoted, key=lambda o: o.features.promotion_efficiency_score)
                if promoted else None
            ),
            highest_organic_multiplier=_best(
                promoted, lambda o: o.features.organic_multiplier
            ),
            highest_subscriber_generator=_best(opps, lambda o: o.features.subscribers_gained),
            highest_qualifying_hour_generator=_best(opps, lambda o: o.features.qualifying_hours),
        )

    # ── Internal ──────────────────────────────────────────────────────────────

    def _population_averages(self) -> dict[str, float]:
        if not self._all:
            return {}
        n = len(self._all)
        return {
            "retention": sum(f.audience_retention_pct for f in self._all) / n,
            "sub_per_1k": sum(f.subscriber_conversion_per_1k for f in self._all) / n,
            "vpd": sum(f.views_per_day for f in self._all) / n,
            "qualifying_hours": sum(f.qualifying_hours for f in self._all) / n,
        }
