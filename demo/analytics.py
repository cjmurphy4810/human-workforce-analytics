"""Windowed analytics contract for demo report loaders.

Rows in ``daily_video_metrics`` and ``video_traffic_source_metrics`` are daily
increments, never lifetime snapshots.  Report code must select an explicit inclusive
date window and aggregate it through :func:`aggregate_video_window`.  Durations are
weighted by views; count and watch-time fields are summed over the same window.
"""

from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping

from demo.config import DEMO_CHANNEL_KEY


def eligible_organic_watch_hours(
    duration_seconds: int,
    total_watch_hours: float,
    advertising_watch_hours: float,
) -> float:
    """Return YPP-eligible organic hours for one video and one matching window."""
    if 0 < duration_seconds <= 180:
        return 0.0
    return max(float(total_watch_hours) - float(advertising_watch_hours), 0.0)


def organic_window_totals(row: Mapping[str, Any]) -> dict[str, float | int]:
    """Remove same-window advertising increments from a video aggregate."""
    traffic = row.get("traffic_sources") or {}
    advertising = traffic.get("ADVERTISING") or {}
    return {
        "views": max(int(row.get("views", 0)) - int(advertising.get("views", 0)), 0),
        "watch_hours": max(
            (
                float(row.get("estimated_minutes_watched", 0.0))
                - float(advertising.get("estimated_minutes_watched", 0.0))
            )
            / 60.0,
            0.0,
        ),
    }


def filter_promotion_opportunities(
    opportunities: Iterable[Any],
    *,
    topics: set[str],
    minimum_score: float,
) -> list[Any]:
    """Return the single filtered opportunity population used by every report section."""
    return [
        item
        for item in opportunities
        if (not topics or item.features.topic in topics) and item.score >= minimum_score
    ]


def rank_persisted_content_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Rank CI rows exclusively by their persisted overall score."""
    return sorted(rows, key=lambda row: float(row["overall_score"]), reverse=True)


def content_tier_rows(
    rows: Iterable[dict[str, Any]], tier: str
) -> list[dict[str, Any]]:
    """Select a CI tab population exclusively from the persisted tier."""
    return [row for row in rows if row["tier"] == tier]


def content_repackaging_rows(
    rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Select persisted underperformers whose persisted watch rate supports repackaging."""
    return [
        row
        for row in rows
        if row["tier"] == "underperformer"
        and float(row.get("watch_rate_pct", 0.0)) >= 45.0
    ]


def aggregate_video_window(
    path: Path,
    *,
    start: date,
    end: date,
    channel: str = DEMO_CHANNEL_KEY,
    include_observed_days: bool = False,
) -> list[dict[str, object]]:
    """Return per-video and per-source aggregates for an inclusive date window."""
    if start > end:
        raise ValueError("start must be on or before end")

    with sqlite3.connect(path) as conn:
        metric_rows = conn.execute(
            "SELECT video_id, COALESCE(SUM(views), 0), "
            "COALESCE(SUM(estimated_minutes_watched), 0), "
            "COALESCE(SUM(average_view_duration * views) / NULLIF(SUM(views), 0), 0), "
            "COALESCE(SUM(likes), 0), COALESCE(SUM(subscribers_gained), 0), "
            "COUNT(DISTINCT metric_date) "
            "FROM daily_video_metrics WHERE channel=? AND metric_date BETWEEN ? AND ? "
            "GROUP BY video_id ORDER BY video_id",
            (channel, start.isoformat(), end.isoformat()),
        ).fetchall()
        traffic_rows = conn.execute(
            "SELECT video_id, traffic_source_type, COALESCE(SUM(views), 0), "
            "COALESCE(SUM(estimated_minutes_watched), 0), "
            "COALESCE(SUM(average_view_duration * views) / NULLIF(SUM(views), 0), 0) "
            "FROM video_traffic_source_metrics "
            "WHERE channel=? AND metric_date BETWEEN ? AND ? "
            "GROUP BY video_id, traffic_source_type "
            "ORDER BY video_id, traffic_source_type",
            (channel, start.isoformat(), end.isoformat()),
        ).fetchall()

    traffic_by_video: dict[str, dict[str, dict[str, float | int]]] = {}
    for video_id, source, views, minutes, average_duration in traffic_rows:
        traffic_by_video.setdefault(video_id, {})[source] = {
            "views": int(views),
            "estimated_minutes_watched": float(minutes),
            "average_view_duration": float(average_duration),
        }

    return [
        {
            "video_id": video_id,
            "views": int(views),
            "estimated_minutes_watched": float(minutes),
            "average_view_duration": float(average_duration),
            "likes": int(likes),
            "subscribers_gained": int(subscribers),
            **({"observed_days": int(observed_days)} if include_observed_days else {}),
            "traffic_sources": traffic_by_video.get(video_id, {}),
        }
        for (
            video_id,
            views,
            minutes,
            average_duration,
            likes,
            subscribers,
            observed_days,
        ) in metric_rows
    ]
