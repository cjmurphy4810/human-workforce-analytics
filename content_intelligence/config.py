"""Scoring configuration for the Content Intelligence Engine.

All weights and thresholds are Pydantic models so they can be validated,
serialized, and overridden from config files or environment variables.
"""
from __future__ import annotations

from pydantic import BaseModel, Field, model_validator


class ScoringWeights(BaseModel):
    """Relative weights for each metric when computing a composite score.

    Weights must sum to 1.0 (±0.01 tolerance for floating-point rounding).
    """

    ctr: float = 0.20
    average_percentage_viewed: float = 0.20
    watch_hours: float = 0.20
    subscribers_gained: float = 0.20
    comments: float = 0.08
    shares: float = 0.07
    returning_viewers: float = 0.05

    @model_validator(mode="after")
    def _weights_sum_to_one(self) -> "ScoringWeights":
        total = (
            self.ctr
            + self.average_percentage_viewed
            + self.watch_hours
            + self.subscribers_gained
            + self.comments
            + self.shares
            + self.returning_viewers
        )
        if abs(total - 1.0) > 0.01:
            raise ValueError(
                f"ScoringWeights must sum to 1.0, got {total:.4f}. "
                "Adjust weights so they total 1.0."
            )
        return self


class NormalizationThresholds(BaseModel):
    """Values treated as the 'perfect score' ceiling for each metric.

    Metrics above these ceilings are capped at 1.0 before weighting.
    These represent realistic upper bounds for a growing YouTube channel.
    """

    ctr_max: float = Field(default=10.0, gt=0, description="10% CTR = perfect score")
    avg_pct_viewed_max: float = Field(default=100.0, gt=0)
    watch_hours_max: float = Field(default=500.0, gt=0)
    subscribers_gained_max: float = Field(default=500.0, gt=0)
    comments_max: float = Field(default=100.0, gt=0)
    shares_max: float = Field(default=50.0, gt=0)
    returning_viewers_max: float = Field(default=500.0, gt=0)


class ClassificationThresholds(BaseModel):
    """Thresholds for the rule-based episode classification system."""

    # subscriber_magnet: high subscriber rate relative to views
    subscriber_magnet_min_sub_rate: float = 0.02  # 2% subs per view

    # hidden_gem: high average percentage viewed but low impressions
    hidden_gem_min_avg_pct_viewed: float = 50.0
    hidden_gem_max_impressions: int = 10_000

    # high_engagement: meaningful comment + like rate
    high_engagement_min_engagement_rate: float = 0.04  # (comments+likes)/views

    # evergreen_candidate: consistently watched all the way through
    evergreen_candidate_min_avg_pct_viewed: float = 60.0
    evergreen_candidate_min_views: int = 100

    # needs_repackaging: content is good but low CTR stops discovery
    needs_repackaging_min_avg_pct_viewed: float = 50.0
    needs_repackaging_max_ctr: float = 3.0

    # high_watch_time: accumulating significant total watch hours
    high_watch_time_min_hours: float = 100.0

    # low_ctr_opportunity: content is engaging but title/thumbnail underperforms
    low_ctr_opportunity_max_ctr: float = 2.0
    low_ctr_opportunity_min_avg_pct_viewed: float = 40.0


class ScoringConfig(BaseModel):
    """Top-level scoring configuration bundling weights and thresholds."""

    weights: ScoringWeights = Field(default_factory=ScoringWeights)
    thresholds: NormalizationThresholds = Field(default_factory=NormalizationThresholds)
    classification: ClassificationThresholds = Field(
        default_factory=ClassificationThresholds
    )


# Module-level default — importable directly for convenience
DEFAULT_CONFIG = ScoringConfig()
