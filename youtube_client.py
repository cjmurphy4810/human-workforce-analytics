"""YouTube Data + Analytics API client.

Authentication uses an OAuth 2.0 refresh token stored in env vars / Streamlit secrets:
  YT_CLIENT_ID, YT_CLIENT_SECRET, YT_REFRESH_TOKEN, YT_CHANNEL_ID
"""

import os
import re
from datetime import date

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
]


def _credentials() -> Credentials:
    creds = Credentials(
        token=None,
        refresh_token=os.environ["YT_REFRESH_TOKEN"],
        client_id=os.environ["YT_CLIENT_ID"],
        client_secret=os.environ["YT_CLIENT_SECRET"],
        token_uri="https://oauth2.googleapis.com/token",
        scopes=SCOPES,
    )
    creds.refresh(Request())
    return creds


def data_service():
    return build("youtube", "v3", credentials=_credentials(), cache_discovery=False)


def analytics_service():
    return build("youtubeAnalytics", "v2", credentials=_credentials(), cache_discovery=False)


def resolve_channel_id(channel_id_or_handle: str | None) -> str | None:
    """Accept a channel ID (UC...), a handle (@TheHumanWorkforce), or None."""
    if not channel_id_or_handle:
        return None
    val = channel_id_or_handle.strip()
    if val.startswith("UC") and len(val) == 24:
        return val
    if val.startswith("@") or not val.startswith("UC"):
        handle = val if val.startswith("@") else f"@{val}"
        yt = data_service()
        resp = yt.channels().list(part="id", forHandle=handle).execute()
        items = resp.get("items", [])
        if not items:
            raise ValueError(f"No channel found for handle {handle}")
        return items[0]["id"]
    return val


def fetch_channel_stats(channel_id: str | None = None) -> dict:
    """Fetch channel stats. If channel_id is None, uses the authenticated user's own channel."""
    yt = data_service()
    if channel_id:
        resp = yt.channels().list(part="snippet,statistics,contentDetails", id=channel_id).execute()
    else:
        resp = yt.channels().list(part="snippet,statistics,contentDetails", mine=True).execute()
    items = resp.get("items", [])
    if not items:
        raise ValueError(f"No channel returned. Response: {resp}")
    item = items[0]
    return {
        "channel_id": item["id"],
        "channel_title": item["snippet"]["title"],
        "subscriber_count": int(item["statistics"].get("subscriberCount", 0)),
        "view_count": int(item["statistics"].get("viewCount", 0)),
        "video_count": int(item["statistics"].get("videoCount", 0)),
        "uploads_playlist_id": item["contentDetails"]["relatedPlaylists"]["uploads"],
    }


def fetch_all_video_ids(uploads_playlist_id: str) -> list[str]:
    yt = data_service()
    video_ids = []
    page_token = None
    while True:
        resp = yt.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=50,
            pageToken=page_token,
        ).execute()
        video_ids.extend(item["contentDetails"]["videoId"] for item in resp["items"])
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return video_ids


def fetch_video_details(video_ids: list[str]) -> list[dict]:
    yt = data_service()
    details = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i : i + 50]
        resp = yt.videos().list(part="snippet,statistics,contentDetails", id=",".join(batch)).execute()
        for item in resp["items"]:
            details.append({
                "video_id": item["id"],
                "title": item["snippet"]["title"],
                "description": item["snippet"].get("description", ""),
                "published_at": item["snippet"]["publishedAt"],
                "thumbnail_url": item["snippet"]["thumbnails"].get("high", {}).get("url", ""),
                "duration": item["contentDetails"]["duration"],
                "view_count": int(item["statistics"].get("viewCount", 0)),
                "like_count": int(item["statistics"].get("likeCount", 0)),
                "comment_count": int(item["statistics"].get("commentCount", 0)),
            })
    return details


def fetch_daily_channel_metrics(start: date, end: date, channel_id: str | None = None) -> list[dict]:
    yt = analytics_service()
    ids = f"channel=={channel_id}" if channel_id else "channel==MINE"
    resp = yt.reports().query(
        ids=ids,
        startDate=start.isoformat(),
        endDate=end.isoformat(),
        metrics="views,estimatedMinutesWatched,subscribersGained,subscribersLost",
        dimensions="day",
    ).execute()
    rows = resp.get("rows", [])
    return [
        {
            "metric_date": r[0],
            "views": int(r[1]),
            "estimated_minutes_watched": float(r[2]),
            "subscribers_gained": int(r[3]),
            "subscribers_lost": int(r[4]),
        }
        for r in rows
    ]


def fetch_video_period_metrics(start: date, end: date, channel_id: str | None = None) -> list[dict]:
    """Aggregate metrics per video for the date range (not per day)."""
    yt = analytics_service()
    ids = f"channel=={channel_id}" if channel_id else "channel==MINE"
    resp = yt.reports().query(
        ids=ids,
        startDate=start.isoformat(),
        endDate=end.isoformat(),
        metrics="views,estimatedMinutesWatched,averageViewDuration,likes,subscribersGained",
        dimensions="video",
        maxResults=200,
        sort="-views",
    ).execute()
    rows = resp.get("rows", [])
    return [
        {
            "metric_date": end.isoformat(),
            "video_id": r[0],
            "views": int(r[1]),
            "estimated_minutes_watched": float(r[2]),
            "average_view_duration": float(r[3]),
            "likes": int(r[4]),
            "subscribers_gained": int(r[5]),
        }
        for r in rows
    ]


def fetch_retention_curve(video_id: str, start: date, end: date) -> dict | None:
    """Fetch audienceWatchRatio at 25% and 75% elapsed time for one video.

    Returns None if the API has no data for this (video, window) — typically
    because the video has too few views to clear YouTube's privacy threshold.
    """
    yt = analytics_service()
    resp = yt.reports().query(
        ids="channel==MINE",
        startDate=start.isoformat(),
        endDate=end.isoformat(),
        metrics="audienceWatchRatio",
        dimensions="elapsedVideoTimeRatio",
        filters=f"video=={video_id};audienceType==ORGANIC",
    ).execute()
    rows = resp.get("rows", [])
    if not rows:
        return None

    points = sorted((float(r[0]), float(r[1])) for r in rows)

    def at(target: float) -> float:
        for i, (t, _) in enumerate(points):
            if abs(t - target) < 1e-6:
                return min(points[i][1], 1.0)
            if t > target:
                if i == 0:
                    return min(points[0][1], 1.0)
                t0, v0 = points[i - 1]
                t1, v1 = points[i]
                ratio = (target - t0) / (t1 - t0)
                return min(v0 + ratio * (v1 - v0), 1.0)
        return min(points[-1][1], 1.0)

    return {
        "video_id": video_id,
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "retention_at_25": at(0.25),
        "retention_at_75": at(0.75),
    }


def fetch_video_views_in_window(video_id: str, start: date, end: date) -> int:
    """Fetch the actual view count for one video over a date range.

    Authoritative — comes straight from YouTube Analytics. Returns 0 if no data.
    """
    yt = analytics_service()
    resp = yt.reports().query(
        ids="channel==MINE",
        startDate=start.isoformat(),
        endDate=end.isoformat(),
        metrics="views",
        filters=f"video=={video_id}",
    ).execute()
    rows = resp.get("rows", [])
    if not rows:
        return 0
    return int(rows[0][0])


def parse_iso8601_duration(duration: str) -> int:
    """Convert ISO 8601 duration (PT1H2M3S) to seconds."""
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration)
    if not m:
        return 0
    h, mn, s = (int(g) if g else 0 for g in m.groups())
    return h * 3600 + mn * 60 + s
