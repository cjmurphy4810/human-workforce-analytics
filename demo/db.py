"""SQLite boundary for the public synthetic analytics demo.

This schema is deliberately local to the demo.  It must never inherit production
channel defaults or migration state from the application's primary database.
"""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from demo.config import DEMO_DB_PATH


SCHEMA = """
CREATE TABLE IF NOT EXISTS channel_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT 'ai_engineering_genius',
    channel_id TEXT NOT NULL,
    subscriber_count INTEGER,
    view_count INTEGER,
    video_count INTEGER
);

CREATE TABLE IF NOT EXISTS videos (
    channel TEXT NOT NULL DEFAULT 'ai_engineering_genius',
    video_id TEXT NOT NULL,
    title TEXT,
    description TEXT,
    published_at TEXT,
    duration_seconds INTEGER,
    thumbnail_url TEXT,
    PRIMARY KEY (channel, video_id)
);

CREATE TABLE IF NOT EXISTS video_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT 'ai_engineering_genius',
    video_id TEXT NOT NULL,
    view_count INTEGER,
    like_count INTEGER,
    comment_count INTEGER,
    FOREIGN KEY (channel, video_id) REFERENCES videos(channel, video_id)
);

CREATE TABLE IF NOT EXISTS daily_video_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_date TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT 'ai_engineering_genius',
    video_id TEXT NOT NULL,
    views INTEGER,
    estimated_minutes_watched REAL,
    average_view_duration REAL,
    likes INTEGER,
    subscribers_gained INTEGER,
    UNIQUE(channel, metric_date, video_id),
    FOREIGN KEY (channel, video_id) REFERENCES videos(channel, video_id)
);

CREATE TABLE IF NOT EXISTS daily_channel_metrics (
    metric_date TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT 'ai_engineering_genius',
    views INTEGER,
    estimated_minutes_watched REAL,
    subscribers_gained INTEGER,
    subscribers_lost INTEGER,
    PRIMARY KEY (channel, metric_date)
);

CREATE TABLE IF NOT EXISTS retention_buckets (
    channel TEXT NOT NULL DEFAULT 'ai_engineering_genius',
    video_id TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    window_kind TEXT NOT NULL,
    views INTEGER NOT NULL,
    retention_at_25 REAL NOT NULL,
    retention_at_75 REAL NOT NULL,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (channel, video_id, window_start, window_end, window_kind),
    FOREIGN KEY (channel, video_id) REFERENCES videos(channel, video_id)
);

CREATE INDEX IF NOT EXISTS idx_video_snapshots_video_time
    ON video_snapshots(channel, video_id, captured_at);
CREATE INDEX IF NOT EXISTS idx_channel_snapshots_time
    ON channel_snapshots(channel, captured_at);
CREATE INDEX IF NOT EXISTS idx_retention_buckets_kind_end
    ON retention_buckets(channel, window_kind, window_end);

CREATE TABLE IF NOT EXISTS daily_geo_metrics (
    metric_date TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT 'ai_engineering_genius',
    country_code TEXT NOT NULL,
    views INTEGER,
    subscribers_gained INTEGER,
    likes INTEGER,
    PRIMARY KEY (channel, metric_date, country_code)
);

CREATE INDEX IF NOT EXISTS idx_daily_geo_metrics_date
    ON daily_geo_metrics(channel, metric_date);

CREATE TABLE IF NOT EXISTS publishing_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    analyzed_at TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT 'ai_engineering_genius',
    videos_analyzed INTEGER NOT NULL DEFAULT 0,
    news_stories_count INTEGER NOT NULL DEFAULT 0,
    result_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS playlists (
    channel TEXT NOT NULL DEFAULT 'ai_engineering_genius',
    playlist_id TEXT NOT NULL,
    title TEXT,
    description TEXT,
    published_at TEXT,
    item_count INTEGER,
    thumbnail_url TEXT,
    PRIMARY KEY (channel, playlist_id)
);

CREATE TABLE IF NOT EXISTS playlist_videos (
    channel TEXT NOT NULL DEFAULT 'ai_engineering_genius',
    playlist_id TEXT NOT NULL,
    video_id TEXT NOT NULL,
    position INTEGER,
    PRIMARY KEY (channel, playlist_id, video_id),
    FOREIGN KEY (channel, playlist_id) REFERENCES playlists(channel, playlist_id),
    FOREIGN KEY (channel, video_id) REFERENCES videos(channel, video_id)
);

CREATE TABLE IF NOT EXISTS queue_recommendations (
    channel TEXT NOT NULL DEFAULT 'ai_engineering_genius',
    video_id TEXT NOT NULL,
    first_recommended_at TEXT NOT NULL,
    recommended_publish_date TEXT NOT NULL,
    rank_at_recommendation INTEGER NOT NULL,
    relevance_score REAL NOT NULL,
    theme TEXT,
    why_now TEXT,
    PRIMARY KEY (channel, video_id),
    FOREIGN KEY (channel, video_id) REFERENCES videos(channel, video_id)
);

CREATE TABLE IF NOT EXISTS video_traffic_source_metrics (
    metric_date TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT 'ai_engineering_genius',
    video_id TEXT NOT NULL,
    traffic_source_type TEXT NOT NULL,
    views INTEGER,
    estimated_minutes_watched REAL,
    average_view_duration REAL,
    PRIMARY KEY (channel, metric_date, video_id, traffic_source_type),
    FOREIGN KEY (channel, video_id) REFERENCES videos(channel, video_id)
);

CREATE INDEX IF NOT EXISTS idx_vtsm_video_date
    ON video_traffic_source_metrics(channel, video_id, metric_date);

CREATE TABLE IF NOT EXISTS channel_traffic_sources (
    metric_date TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT 'ai_engineering_genius',
    traffic_source_type TEXT NOT NULL,
    views INTEGER,
    estimated_minutes_watched REAL,
    PRIMARY KEY (channel, metric_date, traffic_source_type)
);

CREATE INDEX IF NOT EXISTS idx_channel_traffic_date
    ON channel_traffic_sources(channel, metric_date);

CREATE TABLE IF NOT EXISTS ci_video_scores (
    scored_at TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT 'ai_engineering_genius',
    video_id TEXT NOT NULL,
    tier TEXT NOT NULL,
    engagement_score REAL,
    evergreen_score REAL,
    subscriber_magnet_score REAL,
    hidden_gem_score REAL,
    overall_score REAL,
    total_views INTEGER,
    watch_rate_pct REAL,
    like_rate_pct REAL,
    sub_rate_pct REAL,
    promotion_ratio REAL,
    PRIMARY KEY (channel, scored_at, video_id),
    FOREIGN KEY (channel, video_id) REFERENCES videos(channel, video_id)
);

CREATE INDEX IF NOT EXISTS idx_ci_scores_date
    ON ci_video_scores(channel, scored_at);

CREATE TABLE IF NOT EXISTS ci_content_assets (
    asset_id TEXT PRIMARY KEY,
    channel TEXT NOT NULL DEFAULT 'ai_engineering_genius',
    video_id TEXT NOT NULL,
    video_title TEXT,
    asset_type TEXT NOT NULL,
    title TEXT,
    body TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    approved_at TEXT,
    scheduled_for TEXT,
    notes TEXT NOT NULL DEFAULT '',
    FOREIGN KEY (channel, video_id) REFERENCES videos(channel, video_id)
);

CREATE INDEX IF NOT EXISTS idx_ci_assets_video
    ON ci_content_assets(channel, video_id);
CREATE INDEX IF NOT EXISTS idx_ci_assets_status
    ON ci_content_assets(channel, status, asset_type);
"""


@contextmanager
def connect_demo_db(path: Path = DEMO_DB_PATH) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA page_size = 4096")
    conn.execute("PRAGMA auto_vacuum = NONE")
    conn.execute("PRAGMA journal_mode = DELETE")
    conn.execute("PRAGMA synchronous = FULL")
    conn.executescript(SCHEMA)
    try:
        yield conn
    finally:
        conn.close()
