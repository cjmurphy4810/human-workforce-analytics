"""
YouTube Analytics adapter interfaces.

Implementations:
  LocalDBAnalyticsAdapter  – reads from the local SQLite database (default)
  YouTubeAnalyticsAPIAdapter – calls the YouTube Analytics API (requires OAuth)

Add new providers by implementing the YouTubeAnalyticsProvider protocol.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class YouTubeAnalyticsProvider(Protocol):
    def fetch_video_watch_time(
        self,
        video_ids: list[str],
        date_range: tuple[str, str],
    ) -> pd.DataFrame:
        """Return DataFrame with columns: video_id, metric_date, views, estimated_minutes_watched, average_view_duration."""
        ...

    def fetch_channel_watch_time(
        self,
        date_range: tuple[str, str],
    ) -> pd.DataFrame:
        """Return DataFrame with columns: metric_date, views, estimated_minutes_watched."""
        ...

    def fetch_video_metadata(self, video_ids: list[str]) -> pd.DataFrame:
        """Return DataFrame with columns: video_id, title, published_at, duration_seconds."""
        ...


class LocalDBAnalyticsAdapter:
    """Reads analytics data from the project's local SQLite database."""

    def __init__(self, db_path: str | Path) -> None:
        self._db = Path(db_path)

    def _query(self, sql: str) -> pd.DataFrame:
        if not self._db.exists():
            return pd.DataFrame()
        with sqlite3.connect(self._db) as conn:
            try:
                return pd.read_sql_query(sql, conn)
            except Exception:
                return pd.DataFrame()

    def fetch_video_watch_time(
        self,
        video_ids: list[str],
        date_range: tuple[str, str],
    ) -> pd.DataFrame:
        ids = ", ".join(f"'{v}'" for v in video_ids)
        sql = (
            f"SELECT metric_date, video_id, views, estimated_minutes_watched, average_view_duration "
            f"FROM daily_video_metrics "
            f"WHERE video_id IN ({ids}) "
            f"  AND metric_date BETWEEN '{date_range[0]}' AND '{date_range[1]}' "
            f"ORDER BY metric_date"
        )
        return self._query(sql)

    def fetch_channel_watch_time(self, date_range: tuple[str, str]) -> pd.DataFrame:
        sql = (
            f"SELECT metric_date, views, estimated_minutes_watched "
            f"FROM daily_channel_metrics "
            f"WHERE metric_date BETWEEN '{date_range[0]}' AND '{date_range[1]}' "
            f"ORDER BY metric_date"
        )
        return self._query(sql)

    def fetch_video_metadata(self, video_ids: list[str]) -> pd.DataFrame:
        ids = ", ".join(f"'{v}'" for v in video_ids)
        sql = (
            f"SELECT video_id, title, published_at, duration_seconds "
            f"FROM videos WHERE video_id IN ({ids})"
        )
        return self._query(sql)


class YouTubeAnalyticsAPIAdapter:
    """
    Calls the YouTube Analytics API v2.

    Requires an authenticated google-auth credentials object.
    Not yet implemented — wire up when OAuth credentials are available.
    """

    def __init__(self, credentials) -> None:
        self._credentials = credentials

    def fetch_video_watch_time(
        self,
        video_ids: list[str],
        date_range: tuple[str, str],
    ) -> pd.DataFrame:
        raise NotImplementedError("YouTube Analytics API adapter not yet wired up.")

    def fetch_channel_watch_time(self, date_range: tuple[str, str]) -> pd.DataFrame:
        raise NotImplementedError("YouTube Analytics API adapter not yet wired up.")

    def fetch_video_metadata(self, video_ids: list[str]) -> pd.DataFrame:
        raise NotImplementedError("YouTube Analytics API adapter not yet wired up.")
