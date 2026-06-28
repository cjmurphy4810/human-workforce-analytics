"""
Promotion Efficiency Score (0–100) using normalized weighted scoring.

Weights:
  organic_hours          0.30  (higher is better)
  follow_on_views        0.20  (higher is better)
  subscribers            0.20  (higher is better)
  cost (inverse)         0.15  (lower cost is better)
  promo_pct (inverse)    0.10  (lower promo % is better)
  cost_per_hour (inv)    0.05  (lower cost/hour is better)
"""
from __future__ import annotations

import dataclasses

import pandas as pd

from models.promotion import VideoPromotionMetrics

_WEIGHTS = {
    "organic_hours": 0.30,
    "follow_on_views": 0.20,
    "subscribers": 0.20,
    "cost_inv": 0.15,
    "promo_pct_inv": 0.10,
    "cost_per_hour_inv": 0.05,
}


def _minmax(series: pd.Series) -> pd.Series:
    lo, hi = series.min(), series.max()
    if hi == lo:
        return pd.Series([0.5] * len(series), index=series.index)
    return (series - lo) / (hi - lo)


def compute_efficiency_scores(
    metrics: list[VideoPromotionMetrics],
) -> list[VideoPromotionMetrics]:
    """Return a new list with promotion_efficiency_score filled in (0–100)."""
    if not metrics:
        return metrics

    df = pd.DataFrame([
        {
            "idx": i,
            "organic_hours": m.estimated_qualifying_hours,
            "follow_on_views": m.follow_on_views,
            "subscribers": m.subscribers,
            "cost": m.promotion_cost,
            "promo_pct": m.promotion_percentage,
            "cost_per_hour": m.cost_per_qualified_hour,
        }
        for i, m in enumerate(metrics)
    ]).set_index("idx")

    scored = pd.DataFrame(index=df.index)
    scored["organic_hours"] = _minmax(df["organic_hours"]) * _WEIGHTS["organic_hours"]
    scored["follow_on_views"] = _minmax(df["follow_on_views"]) * _WEIGHTS["follow_on_views"]
    scored["subscribers"] = _minmax(df["subscribers"]) * _WEIGHTS["subscribers"]
    # Inverse metrics: high value → low score, so invert after normalizing
    scored["cost_inv"] = (1 - _minmax(df["cost"])) * _WEIGHTS["cost_inv"]
    scored["promo_pct_inv"] = (1 - _minmax(df["promo_pct"])) * _WEIGHTS["promo_pct_inv"]
    scored["cost_per_hour_inv"] = (1 - _minmax(df["cost_per_hour"])) * _WEIGHTS["cost_per_hour_inv"]

    raw_scores = scored.sum(axis=1) * 100  # 0–100

    result = []
    for i, m in enumerate(metrics):
        result.append(dataclasses.replace(m, promotion_efficiency_score=round(raw_scores.iloc[i], 1)))
    return result
