"""Typed, read-only access to the packaged public-demo database."""

from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
import sqlite3
from typing import Generic, Mapping, TypeVar

import pandas as pd

from demo.config import DEMO_DB_PATH


T = TypeVar("T")


class DatabaseFailureReason(str, Enum):
    """Non-sensitive failure categories safe for runtime control flow."""

    MISSING = "missing"
    UNREADABLE = "unreadable"
    MALFORMED = "malformed"
    SCHEMA = "schema"
    QUERY = "query"


@dataclass(frozen=True)
class DatabaseFailure:
    reason: DatabaseFailureReason


@dataclass(frozen=True)
class DatabaseResult(Generic[T]):
    """A value or a deliberately detail-free database failure."""

    value: T | None = None
    failure: DatabaseFailure | None = None

    @property
    def is_available(self) -> bool:
        return self.failure is None


class DemoDatabaseUnavailable(RuntimeError):
    """Internal signal caught by the public entry point before rendering details."""

    def __init__(self, failure: DatabaseFailure) -> None:
        super().__init__("public demo database unavailable")
        self.failure = failure


REQUIRED_SCHEMA: Mapping[str, frozenset[str]] = {
    "channel_snapshots": frozenset(
        {"captured_at", "channel", "channel_id", "subscriber_count", "view_count"}
    ),
    "channel_traffic_sources": frozenset(
        {"metric_date", "channel", "traffic_source_type", "views"}
    ),
    "ci_content_assets": frozenset(
        {"asset_id", "channel", "video_id", "title", "body", "status"}
    ),
    "ci_video_scores": frozenset(
        {"scored_at", "channel", "video_id", "tier", "overall_score"}
    ),
    "daily_channel_metrics": frozenset(
        {"metric_date", "channel", "views", "estimated_minutes_watched"}
    ),
    "daily_geo_metrics": frozenset(
        {"metric_date", "channel", "country_code", "views"}
    ),
    "daily_video_metrics": frozenset(
        {
            "metric_date",
            "channel",
            "video_id",
            "views",
            "estimated_minutes_watched",
        }
    ),
    "playlists": frozenset({"channel", "playlist_id", "title"}),
    "playlist_videos": frozenset({"channel", "playlist_id", "video_id"}),
    "publishing_queue": frozenset({"analyzed_at", "channel", "result_json"}),
    "queue_recommendations": frozenset(
        {"channel", "video_id", "recommended_publish_date", "theme", "why_now"}
    ),
    "retention_buckets": frozenset(
        {"channel", "video_id", "window_start", "window_end", "window_kind"}
    ),
    "video_snapshots": frozenset(
        {"captured_at", "channel", "video_id", "view_count"}
    ),
    "video_traffic_source_metrics": frozenset(
        {
            "metric_date",
            "channel",
            "video_id",
            "traffic_source_type",
            "views",
            "estimated_minutes_watched",
        }
    ),
    "videos": frozenset(
        {"channel", "video_id", "title", "published_at", "duration_seconds"}
    ),
}


def _failure(reason: DatabaseFailureReason) -> DatabaseResult[T]:
    return DatabaseResult(failure=DatabaseFailure(reason))


def _path_failure(path: Path) -> DatabaseFailureReason | None:
    try:
        if not path.exists() or not path.is_file():
            return DatabaseFailureReason.MISSING
        if path.stat().st_mode & 0o444 == 0:
            return DatabaseFailureReason.UNREADABLE
    except OSError:
        return DatabaseFailureReason.UNREADABLE
    return None


def _connect_readonly(path: Path) -> sqlite3.Connection:
    uri = f"{path.resolve().as_uri()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.execute("PRAGMA query_only = ON")
    return connection


def inspect_demo_database(path: Path = DEMO_DB_PATH) -> DatabaseResult[bool]:
    """Validate that the fixture is readable, healthy, and has the runtime schema."""
    path = Path(path)
    reason = _path_failure(path)
    if reason is not None:
        return _failure(reason)

    try:
        connection = _connect_readonly(path)
    except (OSError, sqlite3.Error):
        return _failure(DatabaseFailureReason.UNREADABLE)

    with closing(connection):
        try:
            integrity = connection.execute("PRAGMA quick_check(1)").fetchone()
            if not integrity or integrity[0] != "ok":
                return _failure(DatabaseFailureReason.MALFORMED)
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            if not set(REQUIRED_SCHEMA).issubset(tables):
                return _failure(DatabaseFailureReason.SCHEMA)
            for table, expected_columns in REQUIRED_SCHEMA.items():
                columns = {
                    row[1]
                    for row in connection.execute(
                        f'PRAGMA table_info("{table}")'
                    ).fetchall()
                }
                if not expected_columns.issubset(columns):
                    return _failure(DatabaseFailureReason.SCHEMA)
        except sqlite3.DatabaseError:
            return _failure(DatabaseFailureReason.MALFORMED)
        except sqlite3.Error:
            return _failure(DatabaseFailureReason.SCHEMA)
    return DatabaseResult(value=True)


def query_frame(
    sql: str,
    params: Mapping[str, object] | tuple[object, ...],
    *,
    path: Path = DEMO_DB_PATH,
) -> DatabaseResult[pd.DataFrame]:
    """Execute one read-only query and return data or a typed failure."""
    path = Path(path)
    reason = _path_failure(path)
    if reason is not None:
        return _failure(reason)
    try:
        connection = _connect_readonly(path)
    except (OSError, sqlite3.Error):
        return _failure(DatabaseFailureReason.UNREADABLE)
    with closing(connection):
        try:
            return DatabaseResult(
                value=pd.read_sql_query(sql, connection, params=params)
            )
        except (sqlite3.DatabaseError, pd.errors.DatabaseError):
            return _failure(DatabaseFailureReason.QUERY)


def query_rows(
    sql: str,
    params: Mapping[str, object] | tuple[object, ...],
    *,
    path: Path = DEMO_DB_PATH,
) -> DatabaseResult[list[tuple]]:
    """Execute one read-only tuple query and return data or a typed failure."""
    path = Path(path)
    reason = _path_failure(path)
    if reason is not None:
        return _failure(reason)
    try:
        connection = _connect_readonly(path)
    except (OSError, sqlite3.Error):
        return _failure(DatabaseFailureReason.UNREADABLE)
    with closing(connection):
        try:
            return DatabaseResult(value=connection.execute(sql, params).fetchall())
        except sqlite3.Error:
            return _failure(DatabaseFailureReason.QUERY)


def require_frame(
    sql: str,
    params: Mapping[str, object] | tuple[object, ...],
    *,
    path: Path = DEMO_DB_PATH,
) -> pd.DataFrame:
    """Return a frame or raise the internal route-level unavailable signal."""
    result = query_frame(sql, params, path=path)
    if result.failure is not None:
        raise DemoDatabaseUnavailable(result.failure)
    assert result.value is not None
    return result.value


def require_rows(
    sql: str,
    params: Mapping[str, object] | tuple[object, ...],
    *,
    path: Path = DEMO_DB_PATH,
) -> list[tuple]:
    """Return tuple rows or raise the internal route-level unavailable signal."""
    result = query_rows(sql, params, path=path)
    if result.failure is not None:
        raise DemoDatabaseUnavailable(result.failure)
    assert result.value is not None
    return result.value
