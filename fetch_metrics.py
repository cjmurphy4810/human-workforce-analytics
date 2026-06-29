"""Pull current YouTube stats and append to SQLite.

Run twice daily via GitHub Actions. Stores:
- Channel snapshot (subs, total views, video count) at run time
- Video snapshots (views/likes/comments per video) at run time
- Daily metrics from the YouTube Analytics API (last 7 days, refreshed each run)
"""

import json
import os
from datetime import date, datetime, timedelta, timezone

import anthropic

from ai_client import classify_video_themes, fetch_news_headlines, rank_videos_by_news
from db import get_conn, init_db
from youtube_client import (
    fetch_all_video_ids,
    fetch_channel_playlists,
    fetch_channel_stats,
    fetch_daily_channel_metrics,
    fetch_daily_ctr_metrics,
    fetch_daily_geo_metrics,
    fetch_playlist_video_ids,
    fetch_retention_curve,
    fetch_video_ctr_metrics,
    fetch_video_details,
    fetch_video_period_metrics,
    fetch_video_traffic_source_metrics,
    fetch_video_views_in_window,
    parse_iso8601_duration,
    resolve_channel_id,
)


ROLLING_WINDOWS = (
    (7, "rolling7"),
    (30, "rolling30"),
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
                try:
                    curve = fetch_retention_curve(vid, start, today)
                    if curve is None:
                        continue
                    views = fetch_video_views_in_window(vid, start, today)
                except Exception as e:
                    # YouTube Analytics returns transient 500s for individual videos; skip and continue
                    # so one bad video doesn't roll back the whole batch.
                    print(f"  skip {vid} {kind}: {e.__class__.__name__}")
                    continue
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


def write_publishing_queue(videos: list[dict]) -> dict | None:
    """Classify unpublished video themes, rank by news relevance, persist to DB."""
    unpublished = [v for v in videos if v.get("privacy_status") != "public"]
    if not unpublished:
        print("  No unpublished videos, skipping publishing queue.")
        return

    print(f"  Found {len(unpublished)} unpublished videos.")
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY")
    if not anthropic_key:
        print("  ANTHROPIC_API_KEY not set, skipping publishing queue.")
        return

    ai = anthropic.Anthropic(api_key=anthropic_key)
    analyzed_at = datetime.now(timezone.utc).isoformat()

    print("  Classifying video themes...")
    themes = classify_video_themes(ai, unpublished)
    videos_with_themes = [
        {**v, "theme": themes.get(v["video_id"], "General workforce topics")}
        for v in unpublished
    ]

    headlines: list[dict] = []
    news_key = os.environ.get("NEWS_API_KEY")
    if news_key:
        print("  Fetching news headlines...")
        headlines = fetch_news_headlines(news_key)
    else:
        print("  NEWS_API_KEY not set, skipping news fetch.")

    print("  Ranking videos by news relevance...")
    ranked = rank_videos_by_news(ai, videos_with_themes, headlines)

    result = {
        "news_available": bool(headlines),
        "ranked_videos": ranked,
        "news_headlines": headlines,
    }

    with get_conn() as conn:
        conn.execute(
            "INSERT INTO publishing_queue(analyzed_at, videos_analyzed, news_stories_count, result_json) "
            "VALUES (?, ?, ?, ?)",
            (analyzed_at, len(unpublished), len(headlines), json.dumps(result)),
        )
    print(f"  Publishing queue written: {len(ranked)} videos ranked against {len(headlines)} headlines.")
    return result


def write_queue_recommendations(ranked_videos: list[dict], cron_date: date) -> None:
    """Persist first-time queue appearances to queue_recommendations (INSERT OR IGNORE)."""
    if not ranked_videos:
        return
    first_recommended_at = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        for item in ranked_videos:
            rank = int(item.get("rank") or 0)
            recommended_publish_date = (cron_date + timedelta(days=rank)).isoformat()
            conn.execute(
                "INSERT OR IGNORE INTO queue_recommendations "
                "(video_id, first_recommended_at, recommended_publish_date, "
                "rank_at_recommendation, relevance_score, theme, why_now) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    item.get("video_id"),
                    first_recommended_at,
                    recommended_publish_date,
                    rank,
                    float(item.get("relevance_score", 0)),
                    item.get("theme"),
                    item.get("why_now"),
                ),
            )
    print(f"  Queue recommendations: {len(ranked_videos)} videos processed (INSERT OR IGNORE).")


def write_geo_metrics(rows: list[dict]) -> None:
    """Persist geographic metrics to daily_geo_metrics table with upsert."""
    with get_conn() as conn:
        for d in rows:
            conn.execute(
                "INSERT INTO daily_geo_metrics(metric_date, country_code, views, "
                "subscribers_gained, likes) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(metric_date, country_code) DO UPDATE SET "
                "views=excluded.views, "
                "subscribers_gained=excluded.subscribers_gained, "
                "likes=excluded.likes",
                (d["metric_date"], d["country_code"], d["views"],
                 d["subscribers_gained"], d["likes"]),
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
    try:
        daily_channel = fetch_daily_channel_metrics(start, end, channel_id)
    except Exception as e:
        print(f"  daily channel metrics failed ({e.__class__.__name__}), skipping.")
        daily_channel = []

    print(f"Fetching per-video totals {start} -> {end}...")
    try:
        daily_video = fetch_video_period_metrics(start, end, channel_id)
    except Exception as e:
        print(f"  per-video totals failed ({e.__class__.__name__}), skipping.")
        daily_video = []

    print(f"Fetching daily geo metrics {start} -> {end}...")
    try:
        daily_geo = fetch_daily_geo_metrics(start, end, channel_id)
    except Exception as e:
        print(f"  daily geo metrics failed ({e.__class__.__name__}: {e}), skipping.")
        daily_geo = []

    print(f"Fetching video CTR metrics {start} -> {end}...")
    try:
        video_ctr = fetch_video_ctr_metrics(start, end, channel_id)
        print(f"  {len(video_ctr)} video CTR rows.")
    except Exception as e:
        print(f"  video CTR metrics failed ({e.__class__.__name__}: {e}), skipping.")
        video_ctr = []

    print(f"Fetching daily channel CTR metrics {start} -> {end}...")
    try:
        daily_ctr = fetch_daily_ctr_metrics(start, end, channel_id)
        print(f"  {len(daily_ctr)} daily CTR rows.")
    except Exception as e:
        print(f"  daily CTR metrics failed ({e.__class__.__name__}: {e}), skipping.")
        daily_ctr = []

    print(f"Fetching ADVERTISING traffic source metrics for {len(video_ids)} videos {start} -> {end}...")
    try:
        traffic_source = fetch_video_traffic_source_metrics(video_ids, start, end, channel_id)
        print(f"  {len(traffic_source)} videos had ADVERTISING traffic.")
    except Exception as e:
        print(f"  traffic source metrics failed ({e.__class__.__name__}: {e}), skipping.")
        traffic_source = []

    print("Fetching channel playlists...")
    try:
        playlists = fetch_channel_playlists(channel_id)
        print(f"Found {len(playlists)} playlists.")
    except Exception as e:
        print(f"  playlist fetch failed ({e.__class__.__name__}: {e}), skipping.")
        playlists = []

    print("Fetching playlist video memberships...")
    playlist_video_memberships = []
    for p in playlists:
        try:
            vids = fetch_playlist_video_ids(p["playlist_id"])
            for pos, vid in enumerate(vids):
                playlist_video_memberships.append({
                    "playlist_id": p["playlist_id"],
                    "video_id": vid,
                    "position": pos,
                })
        except Exception as e:
            print(f"  skip playlist items {p['playlist_id']}: {e.__class__.__name__}")
    print(f"Fetched {len(playlist_video_memberships)} playlist-video memberships.")

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

        for d in video_ctr:
            conn.execute(
                "INSERT INTO video_ctr_metrics(metric_date, video_id, impressions, views, ctr) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(metric_date, video_id) DO UPDATE SET "
                "impressions=excluded.impressions, views=excluded.views, ctr=excluded.ctr",
                (d["metric_date"], d["video_id"], d["impressions"], d["views"], d["ctr"]),
            )

        for d in daily_ctr:
            conn.execute(
                "INSERT INTO daily_channel_ctr(metric_date, impressions, views, ctr) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT(metric_date) DO UPDATE SET "
                "impressions=excluded.impressions, views=excluded.views, ctr=excluded.ctr",
                (d["metric_date"], d["impressions"], d["views"], d["ctr"]),
            )

        for d in traffic_source:
            conn.execute(
                "INSERT INTO video_traffic_source_metrics("
                "metric_date, video_id, traffic_source_type, "
                "views, estimated_minutes_watched, average_view_duration) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(metric_date, video_id, traffic_source_type) DO UPDATE SET "
                "views=excluded.views, "
                "estimated_minutes_watched=excluded.estimated_minutes_watched, "
                "average_view_duration=excluded.average_view_duration",
                (d["metric_date"], d["video_id"], d["traffic_source_type"],
                 d["views"], d["estimated_minutes_watched"], d["average_view_duration"]),
            )

        for p in playlists:
            conn.execute(
                "INSERT INTO playlists(playlist_id, title, description, published_at, item_count, thumbnail_url) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(playlist_id) DO UPDATE SET title=excluded.title, "
                "description=excluded.description, item_count=excluded.item_count, "
                "thumbnail_url=excluded.thumbnail_url",
                (p["playlist_id"], p["title"], p["description"], p["published_at"],
                 p["item_count"], p["thumbnail_url"]),
            )

        for m in playlist_video_memberships:
            conn.execute(
                "INSERT INTO playlist_videos(playlist_id, video_id, position) "
                "VALUES (?, ?, ?) "
                "ON CONFLICT(playlist_id, video_id) DO UPDATE SET position=excluded.position",
                (m["playlist_id"], m["video_id"], m["position"]),
            )

    try:
        write_geo_metrics(daily_geo)
    except Exception as e:
        print(f"  geo metrics write failed ({e.__class__.__name__}), skipping.")

    print("Fetching retention curves for rolling windows (7/90/365 days)...")
    write_retention_rolling_windows([v["video_id"] for v in videos])

    print("Analyzing publishing queue...")
    pq_result = None
    try:
        pq_result = write_publishing_queue(videos)
    except Exception as e:
        print(f"  Publishing queue failed ({e.__class__.__name__}), skipping.")

    print("Writing queue recommendations...")
    try:
        ranked_for_recs = pq_result.get("ranked_videos", []) if pq_result else []
        write_queue_recommendations(ranked_for_recs, date.today())
    except Exception as e:
        print(f"  Queue recommendations write failed ({e.__class__.__name__}), skipping.")

    print("Done.")


if __name__ == "__main__":
    main()
