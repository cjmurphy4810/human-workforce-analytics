"""Pull current YouTube stats and append to SQLite.

Run twice daily via GitHub Actions. Stores:
- Channel snapshot (subs, total views, video count) at run time
- Video snapshots (views/likes/comments per video) at run time
- Daily metrics from the YouTube Analytics API (last 7 days, refreshed each run)
"""

import os
from datetime import date, datetime, timedelta, timezone

from db import get_conn, init_db
from youtube_client import (
    fetch_all_video_ids,
    fetch_channel_stats,
    fetch_daily_channel_metrics,
    fetch_retention_curve,
    fetch_video_details,
    fetch_video_period_metrics,
    fetch_video_views_in_window,
    parse_iso8601_duration,
    resolve_channel_id,
)


ROLLING_WINDOWS = (
    (7, "rolling7"),
    (90, "rolling90"),
    (365, "rolling365"),
)


def write_retention_rolling_windows(video_ids: list[str], today: date | None = None) -> None:
    """Fetch retention curves for three rolling windows per video and persist them.

    Views are fetched directly from YouTube Analytics for each window — the
    authoritative count, not derivable from daily_video_metrics (which stores
    trailing-90-day snapshots, not actual daily views).
    """
    today = today or date.today()
    fetched_at = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        for vid in video_ids:
            for days, kind in ROLLING_WINDOWS:
                start = today - timedelta(days=days)
                curve = fetch_retention_curve(vid, start, today)
                if curve is None:
                    continue
                views = fetch_video_views_in_window(vid, start, today)
                conn.execute(
                    "INSERT INTO retention_buckets(video_id, window_start, window_end, "
                    "window_kind, views, retention_at_25, retention_at_75, fetched_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(video_id, window_start, window_end, window_kind) DO UPDATE SET "
                    "views=excluded.views, retention_at_25=excluded.retention_at_25, "
                    "retention_at_75=excluded.retention_at_75, fetched_at=excluded.fetched_at",
                    (vid, start.isoformat(), today.isoformat(), kind,
                     int(views), curve["retention_at_25"], curve["retention_at_75"],
                     fetched_at),
                )


def main() -> None:
    init_db()
    requested = os.environ.get("YT_CHANNEL_ID") or None
    captured_at = datetime.now(timezone.utc).isoformat()

    print(f"[{captured_at}] Resolving channel...")
    requested_channel_id = resolve_channel_id(requested)

    print("Fetching channel stats...")
    channel = fetch_channel_stats(requested_channel_id)
    channel_id = channel["channel_id"]
    print(f"Channel: {channel['channel_title']} ({channel_id})")

    print("Fetching all video IDs from uploads playlist...")
    video_ids = fetch_all_video_ids(channel["uploads_playlist_id"])
    print(f"Found {len(video_ids)} videos.")

    print("Fetching video details...")
    videos = fetch_video_details(video_ids)

    end = date.today()
    start = end - timedelta(days=90)
    print(f"Fetching daily channel metrics {start} -> {end}...")
    daily_channel = fetch_daily_channel_metrics(start, end, channel_id)

    print(f"Fetching per-video totals {start} -> {end}...")
    daily_video = fetch_video_period_metrics(start, end, channel_id)

    with get_conn() as conn:
        conn.execute(
            "INSERT INTO channel_snapshots(captured_at, channel_id, subscriber_count, view_count, video_count) "
            "VALUES (?, ?, ?, ?, ?)",
            (captured_at, channel_id, channel["subscriber_count"],
             channel["view_count"], channel["video_count"]),
        )

        for v in videos:
            conn.execute(
                "INSERT INTO videos(video_id, title, description, published_at, duration_seconds, thumbnail_url) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(video_id) DO UPDATE SET title=excluded.title, "
                "description=excluded.description, thumbnail_url=excluded.thumbnail_url",
                (v["video_id"], v["title"], v["description"], v["published_at"],
                 parse_iso8601_duration(v["duration"]), v["thumbnail_url"]),
            )
            conn.execute(
                "INSERT INTO video_snapshots(captured_at, video_id, view_count, like_count, comment_count) "
                "VALUES (?, ?, ?, ?, ?)",
                (captured_at, v["video_id"], v["view_count"], v["like_count"], v["comment_count"]),
            )

        for d in daily_channel:
            conn.execute(
                "INSERT INTO daily_channel_metrics(metric_date, views, estimated_minutes_watched, "
                "subscribers_gained, subscribers_lost) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(metric_date) DO UPDATE SET views=excluded.views, "
                "estimated_minutes_watched=excluded.estimated_minutes_watched, "
                "subscribers_gained=excluded.subscribers_gained, "
                "subscribers_lost=excluded.subscribers_lost",
                (d["metric_date"], d["views"], d["estimated_minutes_watched"],
                 d["subscribers_gained"], d["subscribers_lost"]),
            )

        for d in daily_video:
            conn.execute(
                "INSERT INTO daily_video_metrics(metric_date, video_id, views, "
                "estimated_minutes_watched, average_view_duration, likes, subscribers_gained) "
                "VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(metric_date, video_id) DO UPDATE SET views=excluded.views, "
                "estimated_minutes_watched=excluded.estimated_minutes_watched, "
                "average_view_duration=excluded.average_view_duration, "
                "likes=excluded.likes, subscribers_gained=excluded.subscribers_gained",
                (d["metric_date"], d["video_id"], d["views"], d["estimated_minutes_watched"],
                 d["average_view_duration"], d["likes"], d["subscribers_gained"]),
            )

    print("Fetching retention curves for rolling windows (7/90/365 days)...")
    write_retention_rolling_windows([v["video_id"] for v in videos])

    print("Done.")


if __name__ == "__main__":
    main()
