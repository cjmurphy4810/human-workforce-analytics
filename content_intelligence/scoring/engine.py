"""Score published videos using existing analytics data from the DB.

No new API calls — reads daily_video_metrics, videos, and
video_traffic_source_metrics that fetch_metrics.py already populates.
"""
from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

from content_intelligence.models import VideoScore, VideoTier


def _percentile_rank(values: list[float]) -> list[float]:
    """Return percentile rank 0-100 for each value. Higher value = higher rank."""
    n = len(values)
    if n == 0:
        return []
    sorted_vals = sorted(values)
    ranks: list[float] = []
    for v in values:
        idx = sorted_vals.index(v)
        ranks.append(idx / max(n - 1, 1) * 100)
    return ranks


def score_videos(db_path: Path, channel: str, scored_at: date | None = None) -> list[VideoScore]:
    """
    Load the latest per-video snapshot from the DB and compute content scores.

    Inputs:
        daily_video_metrics — views, avg_view_duration, likes, subscribers_gained
        videos              — duration_seconds, title, published_at
        video_traffic_source_metrics (ADVERTISING) — advertising views per video

    All queries are scoped to `channel` so scores are never blended across
    channels.
    """
    if scored_at is None:
        scored_at = date.today()

    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT
                d.video_id,
                COALESCE(v.title, d.video_id)           AS title,
                v.published_at,
                COALESCE(v.duration_seconds, 0)          AS duration_seconds,
                d.views,
                COALESCE(d.estimated_minutes_watched, 0) / 60.0 AS estimated_hours,
                COALESCE(d.average_view_duration, 0)     AS average_view_duration,
                COALESCE(d.likes, 0)                     AS likes,
                COALESCE(d.subscribers_gained, 0)        AS subscribers_gained,
                COALESCE(adv.adv_views, 0)               AS adv_views
            FROM daily_video_metrics d
            INNER JOIN (
                SELECT video_id, MAX(metric_date) AS latest_date
                FROM daily_video_metrics
                WHERE channel = :channel
                GROUP BY video_id
            ) latest ON d.video_id = latest.video_id
                     AND d.metric_date = latest.latest_date
            LEFT JOIN videos v ON d.video_id = v.video_id
                     AND v.channel = :channel
            LEFT JOIN (
                SELECT video_id, SUM(views) AS adv_views
                FROM video_traffic_source_metrics
                WHERE traffic_source_type = 'ADVERTISING'
                  AND channel = :channel
                GROUP BY video_id
            ) adv ON d.video_id = adv.video_id
            WHERE d.views > 0
              AND d.channel = :channel
        """, {"channel": channel}).fetchall()

    if not rows:
        return []

    # Collect raw lists for vectorised percentile computation
    video_ids = [r["video_id"] for r in rows]
    titles = [r["title"] for r in rows]
    published_ats = [r["published_at"] for r in rows]
    duration_secs = [int(r["duration_seconds"]) for r in rows]
    views_list = [int(r["views"]) for r in rows]
    hours_list = [float(r["estimated_hours"]) for r in rows]
    adv_views_list = [int(r["adv_views"]) for r in rows]

    watch_rates: list[float] = []
    like_rates: list[float] = []
    sub_rates: list[float] = []
    promo_ratios: list[float] = []

    for r, dur, adv_v, v in zip(rows, duration_secs, adv_views_list, views_list):
        avg_dur = float(r["average_view_duration"])
        watch_r = min(avg_dur / dur * 100.0, 100.0) if dur > 0 else 0.0
        watch_rates.append(round(watch_r, 1))
        like_rates.append(round(float(r["likes"]) / max(v, 1) * 100.0, 4))
        sub_rates.append(round(float(r["subscribers_gained"]) / max(v, 1) * 100.0, 5))
        promo_ratios.append(round(min(float(adv_v) / max(v, 1), 1.0), 4))

    views_pct = _percentile_rank([float(x) for x in views_list])
    watch_pct = _percentile_rank(watch_rates)
    like_pct = _percentile_rank(like_rates)
    sub_pct = _percentile_rank(sub_rates)
    organic_pct = _percentile_rank([1.0 - p for p in promo_ratios])

    scored_str = scored_at.isoformat()
    result: list[VideoScore] = []

    for i in range(len(rows)):
        engagement = (
            0.40 * watch_pct[i]
            + 0.40 * like_pct[i]
            + 0.20 * sub_pct[i]
        )
        evergreen = (
            0.50 * watch_pct[i]
            + 0.30 * views_pct[i]
            + 0.20 * organic_pct[i]
        )
        sub_magnet = (
            0.60 * sub_pct[i]
            + 0.20 * organic_pct[i]
            + 0.20 * watch_pct[i]
        )
        hidden_gem = (
            0.60 * engagement
            + 0.40 * (100.0 - views_pct[i])
        )
        overall = (
            0.40 * engagement
            + 0.30 * evergreen
            + 0.20 * sub_magnet
            + 0.10 * views_pct[i]
        )
        tier = _classify_tier(overall, views_pct[i], sub_magnet, hidden_gem)

        result.append(VideoScore(
            video_id=video_ids[i],
            title=titles[i],
            scored_at=scored_str,
            total_views=views_list[i],
            watch_rate_pct=watch_rates[i],
            like_rate_pct=like_rates[i],
            sub_rate_pct=sub_rates[i],
            promotion_ratio=promo_ratios[i],
            engagement_score=round(engagement, 1),
            evergreen_score=round(evergreen, 1),
            subscriber_magnet_score=round(sub_magnet, 1),
            hidden_gem_score=round(hidden_gem, 1),
            overall_score=round(overall, 1),
            tier=tier,
            published_at=published_ats[i],
            duration_seconds=duration_secs[i],
            estimated_hours=round(hours_list[i], 2),
        ))

    return result


def _classify_tier(
    overall: float,
    views_pct: float,
    sub_magnet: float,
    hidden_gem: float,
) -> VideoTier:
    if overall >= 70 and views_pct >= 60:
        return "top_episode"
    if sub_magnet >= 70:
        return "subscriber_magnet"
    if hidden_gem >= 65 and views_pct < 40:
        return "hidden_gem"
    if overall >= 30:
        return "average"
    return "underperformer"
