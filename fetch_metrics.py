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
    fetch_daily_video_metrics,
    fetch_video_details,
    parse_iso8601_duration,
)


def main() -> None:
    init_db()
    channel_id = os.environ["YT_CHANNEL_ID"]
    captured_at = datetime.now(timezone.utc).isoformat()

    print(f"[{captured_at}] Fetching channel stats for {channel_id}...")
    channel = fetch_channel_stats(channel_id)

    print("Fetching all video IDs from uploads playlist...")
    video_ids = fetch_all_video_ids(channel["uploads_playlist_id"])
    print(f"Found {len(video_ids)} videos.")

    print("Fetching video details...")
    videos = fetch_video_details(video_ids)

    end = date.today()
    start = end - timedelta(days=7)
    print(f"Fetching daily channel metrics {start} -> {end}...")
    daily_channel = fetch_daily_channel_metrics(start, end)

    print(f"Fetching daily per-video metrics {start} -> {end}...")
    daily_video = fetch_daily_video_metrics(start, end)

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

    print("Done.")


if __name__ == "__main__":
    main()
