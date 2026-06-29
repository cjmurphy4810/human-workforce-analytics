import sqlite3
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path(__file__).parent / "data.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS channel_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    subscriber_count INTEGER,
    view_count INTEGER,
    video_count INTEGER
);

CREATE TABLE IF NOT EXISTS videos (
    video_id TEXT PRIMARY KEY,
    title TEXT,
    description TEXT,
    published_at TEXT,
    duration_seconds INTEGER,
    thumbnail_url TEXT
);

CREATE TABLE IF NOT EXISTS video_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    video_id TEXT NOT NULL,
    view_count INTEGER,
    like_count INTEGER,
    comment_count INTEGER,
    FOREIGN KEY (video_id) REFERENCES videos(video_id)
);

CREATE TABLE IF NOT EXISTS daily_video_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_date TEXT NOT NULL,
    video_id TEXT NOT NULL,
    views INTEGER,
    estimated_minutes_watched REAL,
    average_view_duration REAL,
    likes INTEGER,
    subscribers_gained INTEGER,
    UNIQUE(metric_date, video_id)
);

CREATE TABLE IF NOT EXISTS daily_channel_metrics (
    metric_date TEXT PRIMARY KEY,
    views INTEGER,
    estimated_minutes_watched REAL,
    subscribers_gained INTEGER,
    subscribers_lost INTEGER
);

CREATE TABLE IF NOT EXISTS retention_buckets (
    video_id TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    window_kind TEXT NOT NULL,
    views INTEGER NOT NULL,
    retention_at_25 REAL NOT NULL,
    retention_at_75 REAL NOT NULL,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (video_id, window_start, window_end, window_kind)
);

CREATE INDEX IF NOT EXISTS idx_video_snapshots_video_time
    ON video_snapshots(video_id, captured_at);
CREATE INDEX IF NOT EXISTS idx_channel_snapshots_time
    ON channel_snapshots(captured_at);
CREATE INDEX IF NOT EXISTS idx_retention_buckets_kind_end
    ON retention_buckets(window_kind, window_end);
CREATE TABLE IF NOT EXISTS daily_geo_metrics (
    metric_date        TEXT NOT NULL,
    country_code       TEXT NOT NULL,
    views              INTEGER,
    subscribers_gained INTEGER,
    likes              INTEGER,
    PRIMARY KEY (metric_date, country_code)
);

CREATE INDEX IF NOT EXISTS idx_daily_geo_metrics_date
    ON daily_geo_metrics(metric_date);
CREATE TABLE IF NOT EXISTS publishing_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    analyzed_at TEXT NOT NULL,
    videos_analyzed INTEGER NOT NULL DEFAULT 0,
    news_stories_count INTEGER NOT NULL DEFAULT 0,
    result_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS playlists (
    playlist_id TEXT PRIMARY KEY,
    title TEXT,
    description TEXT,
    published_at TEXT,
    item_count INTEGER,
    thumbnail_url TEXT
);

CREATE TABLE IF NOT EXISTS playlist_videos (
    playlist_id TEXT NOT NULL,
    video_id TEXT NOT NULL,
    position INTEGER,
    PRIMARY KEY (playlist_id, video_id)
);

CREATE TABLE IF NOT EXISTS queue_recommendations (
    video_id TEXT PRIMARY KEY,
    first_recommended_at TEXT NOT NULL,
    recommended_publish_date TEXT NOT NULL,
    rank_at_recommendation INTEGER NOT NULL,
    relevance_score REAL NOT NULL,
    theme TEXT,
    why_now TEXT
);

CREATE TABLE IF NOT EXISTS video_traffic_source_metrics (
    metric_date              TEXT NOT NULL,
    video_id                 TEXT NOT NULL,
    traffic_source_type      TEXT NOT NULL,
    views                    INTEGER,
    estimated_minutes_watched REAL,
    average_view_duration    REAL,
    PRIMARY KEY (metric_date, video_id, traffic_source_type)
);
CREATE INDEX IF NOT EXISTS idx_vtsm_video_date
    ON video_traffic_source_metrics(video_id, metric_date);

CREATE TABLE IF NOT EXISTS video_ctr_metrics (
    metric_date  TEXT NOT NULL,
    video_id     TEXT NOT NULL,
    impressions  INTEGER,
    views        INTEGER,
    ctr          REAL,
    PRIMARY KEY (metric_date, video_id)
);
CREATE INDEX IF NOT EXISTS idx_vctr_video_date
    ON video_ctr_metrics(video_id, metric_date);

CREATE TABLE IF NOT EXISTS daily_channel_ctr (
    metric_date  TEXT PRIMARY KEY,
    impressions  INTEGER,
    views        INTEGER,
    ctr          REAL
);
"""


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        conn.executescript(SCHEMA)


if __name__ == "__main__":
    init_db()
    print(f"Initialized database at {DB_PATH}")
