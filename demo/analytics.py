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

from demo.config import DEMO_CHANNEL_KEY


def aggregate_video_window(
    path: Path,
    *,
    start: date,
    end: date,
    channel: str = DEMO_CHANNEL_KEY,
) -> list[dict[str, object]]:
    """Return per-video and per-source aggregates for an inclusive date window."""
    if start > end:
        raise ValueError("start must be on or before end")

    with sqlite3.connect(path) as conn:
        metric_rows = conn.execute(
            "SELECT video_id, COALESCE(SUM(views), 0), "
            "COALESCE(SUM(estimated_minutes_watched), 0), "
            "COALESCE(SUM(average_view_duration * views) / NULLIF(SUM(views), 0), 0), "
            "COALESCE(SUM(likes), 0), COALESCE(SUM(subscribers_gained), 0) "
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
            "traffic_sources": traffic_by_video.get(video_id, {}),
        }
        for video_id, views, minutes, average_duration, likes, subscribers in metric_rows
    ]
