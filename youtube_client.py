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


def fetch_daily_channel_metrics(start: date, end: date) -> list[dict]:
    yt = analytics_service()
    resp = yt.reports().query(
        ids="channel==MINE",
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


def fetch_daily_video_metrics(start: date, end: date) -> list[dict]:
    yt = analytics_service()
    resp = yt.reports().query(
        ids="channel==MINE",
        startDate=start.isoformat(),
        endDate=end.isoformat(),
        metrics="views,estimatedMinutesWatched,averageViewDuration,likes,subscribersGained",
        dimensions="day,video",
        maxResults=200,
        sort="-views",
    ).execute()
    rows = resp.get("rows", [])
    return [
        {
            "metric_date": r[0],
            "video_id": r[1],
            "views": int(r[2]),
            "estimated_minutes_watched": float(r[3]),
            "average_view_duration": float(r[4]),
            "likes": int(r[5]),
            "subscribers_gained": int(r[6]),
        }
        for r in rows
    ]


def parse_iso8601_duration(duration: str) -> int:
    """Convert ISO 8601 duration (PT1H2M3S) to seconds."""
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration)
    if not m:
        return 0
    h, mn, s = (int(g) if g else 0 for g in m.groups())
    return h * 3600 + mn * 60 + s
