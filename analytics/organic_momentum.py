"""Organic Momentum scoring, classification, and data building.

Score components (default weights sum to 1.0):
    organic_views_growth   0.20  — early-vs-late daily view trend
    organic_wh_growth      0.20  — early-vs-late daily watch-hours trend
    organic_ratio          0.15  — (total - promo) / total (organic discovery quality)
    completion_rate        0.10  — avg_view_duration / video_length
    avg_pct_viewed         0.10  — completion_rate, population-relative
    subscriber_conversion  0.10  — subs / organic_views × 1000, population-relative
    returning_proxy        0.05  — subs / total_views (proxy when returning data unavailable)
    follow_on_proxy        0.05  — neutral 0.5 when RELATED_VIDEO unavailable
    ctr_proxy              0.05  — neutral 0.5 when impressions unavailable

All population-relative components use percentile rank so the scoring is
self-calibrating regardless of channel scale.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

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


# ── Pure scoring functions ────────────────────────────────────────────────────


def normalize_metric(value: float, min_value: float, max_value: float) -> float:
    """Normalize *value* to [0, 100] within [min_value, max_value]."""
    if max_value <= min_value:
        return 50.0
    return max(0.0, min(100.0, (value - min_value) / (max_value - min_value) * 100.0))


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


# ── Population statistics ─────────────────────────────────────────────────────


class _PopStats:
    def __init__(self, records: list[OrganicMomentumMetrics]) -> None:
        self._view_gr = sorted(r.view_growth_rate for r in records)
        self._wh_gr = sorted(r.wh_growth_rate for r in records)
        self._org_ratio = sorted(
            r.organic_views / max(r.total_views, 1) for r in records
        )
        self._completion = sorted(
            r.average_view_duration_seconds / max(r.video_length_seconds, 1)
            for r in records
        )
        self._sub_per_1k = sorted(
            r.subscribers / max(r.organic_views, 1) * 1000 for r in records
        )
        self._ret_proxy = sorted(
            r.subscribers / max(r.total_views, 1) for r in records
        )

    def prank(self, field: str, value: float) -> float:
        return _percentile_rank(value, getattr(self, f"_{field}"))


# ── Scorer ────────────────────────────────────────────────────────────────────


def calculate_organic_momentum_score(
    m: OrganicMomentumMetrics,
    pop: _PopStats,
    weights: ScoreWeights,
) -> tuple[float, ScoreBreakdown]:
    """Return (score_0_to_100, ScoreBreakdown) for one video."""
    org_ratio = m.organic_views / max(m.total_views, 1)
    completion = m.average_view_duration_seconds / max(m.video_length_seconds, 1)
    sub_per_1k = m.subscribers / max(m.organic_views, 1) * 1000
    ret_proxy = m.subscribers / max(m.total_views, 1)

    # Percentile ranks → [0, 100]
    vgr_pct = pop.prank("view_gr", m.view_growth_rate) * 100
    whgr_pct = pop.prank("wh_gr", m.wh_growth_rate) * 100
    org_pct = pop.prank("org_ratio", org_ratio) * 100
    comp_pct = pop.prank("completion", completion) * 100
    # avg_pct_viewed: absolute completion capped at 100%
    avg_pct_abs = min(completion * 100.0, 100.0)
    sub_pct = pop.prank("sub_per_1k", sub_per_1k) * 100
    ret_pct = pop.prank("ret_proxy", ret_proxy) * 100

    # Components without DB data get neutral 50/100
    follow_on_norm = 50.0
    ctr_norm = 50.0

    bd = ScoreBreakdown(
        organic_views_growth=round(weights.organic_views_growth * vgr_pct, 2),
        organic_wh_growth=round(weights.organic_wh_growth * whgr_pct, 2),
        organic_ratio=round(weights.organic_ratio * org_pct, 2),
        completion_rate=round(weights.completion_rate * comp_pct, 2),
        avg_pct_viewed=round(weights.avg_pct_viewed * avg_pct_abs, 2),
        subscriber_conversion=round(weights.subscriber_conversion * sub_pct, 2),
        returning_proxy=round(weights.returning_proxy * ret_pct, 2),
        follow_on_proxy=round(weights.follow_on_proxy * follow_on_norm, 2),
        ctr_proxy=round(weights.ctr_proxy * ctr_norm, 2),
    )
    return bd.total, bd


def classify_momentum(
    score: float,
    m: OrganicMomentumMetrics,
    median_views: float,
) -> MomentumClass:
    """Rule-based classification applied in priority order."""
    if m.total_views < _MIN_VIEWS or m.data_points < _MIN_DATA_POINTS:
        return MomentumClass.insufficient_data

    org_ratio = m.organic_views / max(m.total_views, 1)
    completion = m.average_view_duration_seconds / max(m.video_length_seconds, 1)

    # Paid spike: mostly paid traffic and weak organic retention
    if m.promotion_status == PromotionStatus.promoted and org_ratio < 0.45 and score < 55:
        return MomentumClass.paid_spike

    # Retention problem: people bail very early regardless of traffic
    if completion < 0.25 and score < 50:
        return MomentumClass.retention_problem

    # Needs packaging: good retention but stuck below median views (poor discovery)
    if completion > 0.55 and m.total_views < median_views * 0.7 and score < 60:
        return MomentumClass.needs_packaging

    # Organic sleeper: below-median views but still growing
    if m.total_views < median_views and m.view_growth_rate > 0.05 and score >= 35:
        return MomentumClass.organic_sleeper

    if score >= 80:
        return MomentumClass.breakout

    if score >= 60:
        return MomentumClass.promising

    if score < 30:
        return MomentumClass.do_not_promote

    return MomentumClass.do_not_promote


def recommend_action(cls: MomentumClass, m: OrganicMomentumMetrics) -> str:
    """Return a specific recommended action string."""
    base = MOMENTUM_CLASS_ACTION.get(cls, "Monitor")

    if cls == MomentumClass.breakout:
        if m.promotion_cost > 0:
            return f"Scale Promotion — strong organic ROI at ${m.organic_momentum_per_dollar:.2f} organic WH/$"
        return "Scale Promotion — strong organic growth, test a $10–$20 budget"

    if cls == MomentumClass.promising:
        return "Monitor organically 2–3 more weeks, then test a $5 promotion"

    if cls == MomentumClass.paid_spike:
        return "Pause Promotion — organic uplift weak; refresh thumbnail/title before re-promoting"

    if cls == MomentumClass.organic_sleeper:
        return "Add to Playlist + Create Follow-Up — growing without paid spend"

    if cls == MomentumClass.needs_packaging:
        completion_pct = m.average_percentage_viewed
        if completion_pct > 60:
            return "Refresh Thumbnail — retention strong but discovery weak"
        return "Rewrite Title + Refresh Thumbnail — good watch time, poor click-through"

    if cls == MomentumClass.retention_problem:
        return "Review first 30 seconds — viewers leaving early; do not promote until fixed"

    if cls == MomentumClass.do_not_promote:
        return "Do Not Invest — weak across major indicators"

    return base


class MomentumScorer:
    """Score, classify, and rank a list of OrganicMomentumMetrics."""

    def __init__(self, weights: Optional[ScoreWeights] = None) -> None:
        self._weights = weights or ScoreWeights()
        self._weights.validate()

    def score_all(
        self, metrics: list[OrganicMomentumMetrics]
    ) -> list[OrganicMomentumMetrics]:
        """Return the list with score/classification/recommended_action filled in, ranked."""
        if not metrics:
            return []

        pop = _PopStats(metrics)
        views_list = sorted(m.total_views for m in metrics)
        n = len(views_list)
        median_views = float(views_list[n // 2]) if n > 0 else 0.0

        for m in metrics:
            score, breakdown = calculate_organic_momentum_score(m, pop, self._weights)
            cls = classify_momentum(score, m, median_views)
            action = recommend_action(cls, m)
            m.organic_momentum_score = score
            m.score_breakdown = breakdown
            m.classification = cls
            m.recommended_action = action

        metrics.sort(key=lambda m: m.organic_momentum_score, reverse=True)
        return metrics


# ── DB data builder ───────────────────────────────────────────────────────────


def _db_query(db_path: str, sql: str) -> pd.DataFrame:
    p = Path(db_path)
    if not p.exists():
        return pd.DataFrame()
    with sqlite3.connect(str(p)) as conn:
        try:
            return pd.read_sql_query(sql, conn)
        except Exception:
            return pd.DataFrame()


def _compute_growth_stats(db_path: str) -> pd.DataFrame:
    """Derive daily view-increment trends from the daily_video_metrics time series."""
    df = _db_query(db_path, """
        SELECT video_id, metric_date,
               CAST(views AS REAL) AS views,
               estimated_minutes_watched / 60.0 AS watch_hours
        FROM daily_video_metrics
        ORDER BY video_id, metric_date
    """)
    if df.empty:
        return pd.DataFrame()

    df["metric_date"] = pd.to_datetime(df["metric_date"])
    df = df.sort_values(["video_id", "metric_date"])
    df["view_delta"] = df.groupby("video_id")["views"].diff()
    df["wh_delta"] = df.groupby("video_id")["watch_hours"].diff()
    df = df.dropna(subset=["view_delta"])

    # Cap extreme deltas at 99th percentile per video to suppress data-reset spikes
    df["view_delta"] = df.groupby("video_id")["view_delta"].transform(
        lambda x: x.clip(lower=0, upper=x.quantile(0.99) * 3 if len(x) > 3 else x.clip(lower=0))
    )
    df["wh_delta"] = df.groupby("video_id")["wh_delta"].transform(
        lambda x: x.clip(lower=0, upper=x.quantile(0.99) * 3 if len(x) > 3 else x.clip(lower=0))
    )

    records: list[dict] = []
    for vid, grp in df.groupby("video_id"):
        n = len(grp)
        split = max(n // 2, 2)
        first = grp.iloc[:split]
        second = grp.iloc[split:]

        f_mean = float(first["view_delta"].mean()) if not first.empty else 0.0
        s_mean = float(second["view_delta"].mean()) if not second.empty else 0.0
        f_wh = float(first["wh_delta"].mean()) if not first.empty else 0.0
        s_wh = float(second["wh_delta"].mean()) if not second.empty else 0.0

        view_gr = calculate_growth_rate(s_mean, f_mean)
        wh_gr = calculate_growth_rate(s_wh, f_wh)

        records.append({
            "video_id": str(vid),
            "view_growth_rate": view_gr,
            "wh_growth_rate": wh_gr,
            "recent_daily_views": round(s_mean, 1),
            "peak_daily_views": round(float(grp["view_delta"].max()), 1),
            "data_points": n,
        })

    return pd.DataFrame(records) if records else pd.DataFrame()


def build_momentum_data(db_path: str) -> list[OrganicMomentumMetrics]:
    """Load all available data from the DB and return un-scored metrics."""
    vids = _db_query(db_path, """
        SELECT video_id, title, published_at, duration_seconds
        FROM videos
    """)
    if vids.empty:
        return []

    latest = _db_query(db_path, """
        SELECT d.video_id,
               d.views AS total_views,
               d.estimated_minutes_watched / 60.0 AS total_watch_hours,
               COALESCE(d.average_view_duration, 0) AS average_view_duration,
               COALESCE(d.subscribers_gained, 0) AS subscribers_gained
        FROM daily_video_metrics d
        INNER JOIN (
            SELECT video_id, MAX(metric_date) AS latest_date
            FROM daily_video_metrics GROUP BY video_id
        ) lx ON d.video_id = lx.video_id AND d.metric_date = lx.latest_date
    """)

    adv = _db_query(db_path, """
        SELECT video_id, SUM(views) AS adv_views
        FROM video_traffic_source_metrics
        WHERE traffic_source_type = 'ADVERTISING'
        GROUP BY video_id
    """)

    growth = _compute_growth_stats(db_path)

    df = vids.copy()
    for side, defaults in [
        (latest, {"total_views": 0, "total_watch_hours": 0.0,
                  "average_view_duration": 0.0, "subscribers_gained": 0}),
        (adv, {"adv_views": 0}),
        (growth, {"view_growth_rate": 0.0, "wh_growth_rate": 0.0,
                  "recent_daily_views": 0.0, "peak_daily_views": 0.0,
                  "data_points": 0}),
    ]:
        if not side.empty:
            df = df.merge(side, on="video_id", how="left")
        for col, val in defaults.items():
            if col not in df.columns:
                df[col] = val
            df[col] = df[col].fillna(val)

    now = datetime.now(tz=timezone.utc)
    metrics: list[OrganicMomentumMetrics] = []

    for _, row in df.iterrows():
        vid = str(row["video_id"])
        title = str(row["title"])
        pub_str = str(row.get("published_at", ""))
        length_s = int(row.get("duration_seconds") or 0)

        try:
            pub_dt = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pub_dt = now

        total_views = int(row["total_views"])
        total_wh = float(row["total_watch_hours"])
        avg_dur = float(row["average_view_duration"])
        subs = int(row["subscribers_gained"])
        adv_views = int(row["adv_views"])
        vgr = float(row["view_growth_rate"])
        whgr = float(row["wh_growth_rate"])
        recent_dv = float(row["recent_daily_views"])
        peak_dv = float(row["peak_daily_views"])
        dp = int(row["data_points"])

        organic_views = max(total_views - adv_views, 0)
        promo_status = PromotionStatus.promoted if adv_views > 0 else PromotionStatus.not_promoted
        promo_cost = 0.0  # cost data not in DB; would need Google Ads integration

        # Estimate qualifying hours: organic watch hours (subtract promo fraction)
        promo_wh_frac = adv_views / max(total_views, 1)
        promo_wh = total_wh * promo_wh_frac
        qualifying_wh = max(total_wh - promo_wh, 0.0)

        # Post-promotion organic: estimated as total organic × growth momentum
        # If growing (vgr > 0), more of the recent views are organic
        organic_retention_factor = max(0.5, min(1.0, 0.7 + vgr * 0.3))
        post_promo_organic = int(organic_views * organic_retention_factor)
        post_promo_wh = qualifying_wh * organic_retention_factor

        avg_pct = (avg_dur / max(length_s, 1)) * 100.0 if length_s > 0 else 0.0

        # Lift metrics relative to promotion cost (unavailable without Ads data)
        organic_lift = float(organic_views) / max(adv_views, 1) if adv_views > 0 else 0.0
        wh_lift = qualifying_wh / max(total_wh * promo_wh_frac + 0.01, 0.01)
        mom_per_dollar = 0.0  # no cost data

        quality_flag = "actual" if dp >= _MIN_DATA_POINTS else "insufficient"
        if adv_views == 0 and total_views > 0:
            quality_flag = "partial"  # no traffic source breakdown

        m = OrganicMomentumMetrics(
            video_id=vid,
            title=title,
            published_date=pub_dt.strftime("%Y-%m-%d"),
            video_length_seconds=length_s,
            promotion_status=promo_status,
            promotion_start_date=None,
            promotion_end_date=None,
            promotion_cost=promo_cost,
            total_views=total_views,
            organic_views=organic_views,
            promotion_views=adv_views,
            post_promotion_organic_views=post_promo_organic,
            total_watch_hours=round(total_wh, 2),
            estimated_qualifying_watch_hours=round(qualifying_wh, 2),
            post_promotion_organic_watch_hours=round(post_promo_wh, 2),
            average_view_duration_seconds=avg_dur,
            average_percentage_viewed=round(avg_pct, 1),
            ctr=0.0,
            impressions=0,
            engaged_views=0,
            returning_viewers=0,
            subscribers=subs,
            follow_on_views=0,
            browse_views=0,
            suggested_views=0,
            search_views=0,
            organic_lift=round(organic_lift, 2),
            organic_watch_hour_lift=round(wh_lift, 2),
            organic_momentum_per_dollar=mom_per_dollar,
            view_growth_rate=round(vgr, 4),
            wh_growth_rate=round(whgr, 4),
            recent_daily_views=recent_dv,
            peak_daily_views=peak_dv,
            data_points=dp,
            organic_momentum_score=0.0,
            score_breakdown=ScoreBreakdown(),
            classification=MomentumClass.insufficient_data,
            recommended_action="",
            data_quality_flag=quality_flag,
        )
        metrics.append(m)

    return metrics
