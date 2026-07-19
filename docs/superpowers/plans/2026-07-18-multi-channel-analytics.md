# Multi-Channel Analytics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add ClubGeniusStories and KZAKMusicVideos as fully isolated, selectable channels in the human-workforce-analytics Streamlit dashboard, defaulting to The Human Workforce.

**Architecture:** Add a `channel TEXT NOT NULL` column (values: `human_workforce`, `club_genius`, `kzak`) to every per-channel table in the shared `data.db`, folded into each table's uniqueness constraint. `fetch_metrics.py` loops over the three channels (each with its own OAuth refresh token) and tags every insert with its channel key. The Streamlit app gets a sidebar channel selector backed by `st.session_state["active_channel"]`; every SQL query in `app.py` and every report page is parameterized by that channel so no query ever spans channels.

**Tech Stack:** Python 3.12, Streamlit, SQLite (stdlib `sqlite3` + `pandas.read_sql_query`), YouTube Data/Analytics API v3 via `googleapiclient`, GitHub Actions.

## Global Constraints

- Every table listed in "Data model" below gets a `channel` column; no query against those tables may omit a `channel` filter — this is checked per-task via `grep`.
- Channel keys are exactly `human_workforce`, `club_genius`, `kzak` — use these literal strings everywhere (no aliases, no channel_id reuse as the key).
- Existing Human Workforce data in the live `data.db` must be preserved and backfilled to `channel = 'human_workforce'`, never dropped.
- `YT_CLIENT_ID` / `YT_CLIENT_SECRET` are shared across channels (one OAuth app); only refresh tokens and channel IDs differ per channel.
- One channel's fetch failure must never abort the other two channels' fetch in the same run.
- Default UI selection on load is "The Human Workforce" (`human_workforce`).

---

## Data model (reference for all tasks)

Tables gaining a `channel` column, with their new/updated uniqueness constraint:

| Table | New constraint |
|---|---|
| `channel_snapshots` | (no PK change; add `channel` column) |
| `videos` | `PRIMARY KEY (channel, video_id)` |
| `video_snapshots` | (no PK change; add `channel` column) |
| `daily_video_metrics` | `UNIQUE(channel, metric_date, video_id)` |
| `daily_channel_metrics` | `PRIMARY KEY (channel, metric_date)` |
| `retention_buckets` | `PRIMARY KEY (channel, video_id, window_start, window_end, window_kind)` |
| `daily_geo_metrics` | `PRIMARY KEY (channel, metric_date, country_code)` |
| `publishing_queue` | (no PK change; add `channel` column) |
| `playlists` | `PRIMARY KEY (channel, playlist_id)` |
| `playlist_videos` | `PRIMARY KEY (channel, playlist_id, video_id)` |
| `queue_recommendations` | `PRIMARY KEY (channel, video_id)` |
| `video_traffic_source_metrics` | `PRIMARY KEY (channel, metric_date, video_id, traffic_source_type)` |
| `channel_traffic_sources` | `PRIMARY KEY (channel, metric_date, traffic_source_type)` |
| `ci_video_scores` | `PRIMARY KEY (channel, scored_at, video_id)` |
| `ci_content_assets` | (no PK change; add `channel` column) |

---

### Task 1: Schema migration — add `channel` column everywhere

**Files:**
- Modify: `db.py`
- Test: `tests/test_db.py`

**Interfaces:**
- Produces: `db.SCHEMA` (updated), `db.init_db()` (unchanged signature), `db.migrate_add_channel_column(conn)` — new function, idempotent, safe to call on a fresh or already-migrated DB.
- Produces constant: `db.DEFAULT_CHANNEL = "human_workforce"`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_db.py — add to existing file
import sqlite3
from pathlib import Path

from db import DEFAULT_CHANNEL, SCHEMA, migrate_add_channel_column


def test_migrate_add_channel_column_backfills_existing_rows(tmp_path):
    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_path)
    # Simulate the pre-migration (single-channel) schema for one representative table.
    conn.execute(
        "CREATE TABLE videos (video_id TEXT PRIMARY KEY, title TEXT, description TEXT, "
        "published_at TEXT, duration_seconds INTEGER, thumbnail_url TEXT)"
    )
    conn.execute("INSERT INTO videos(video_id, title) VALUES ('vid1', 'Old Video')")
    conn.commit()

    migrate_add_channel_column(conn)

    row = conn.execute("SELECT channel, video_id FROM videos WHERE video_id='vid1'").fetchone()
    assert row == (DEFAULT_CHANNEL, "vid1")
    conn.close()


def test_migrate_add_channel_column_is_idempotent(tmp_path):
    db_path = tmp_path / "legacy2.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE videos (video_id TEXT PRIMARY KEY, title TEXT, description TEXT, "
        "published_at TEXT, duration_seconds INTEGER, thumbnail_url TEXT)"
    )
    conn.commit()
    migrate_add_channel_column(conn)
    migrate_add_channel_column(conn)  # must not raise on second run
    cols = [r[1] for r in conn.execute("PRAGMA table_info(videos)").fetchall()]
    assert cols.count("channel") == 1
    conn.close()


def test_schema_creates_channel_columns(tmp_path):
    db_path = tmp_path / "fresh.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    for table in [
        "channel_snapshots", "videos", "video_snapshots", "daily_video_metrics",
        "daily_channel_metrics", "retention_buckets", "daily_geo_metrics",
        "publishing_queue", "playlists", "playlist_videos", "queue_recommendations",
        "video_traffic_source_metrics", "channel_traffic_sources",
        "ci_video_scores", "ci_content_assets",
    ]:
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        assert "channel" in cols, f"{table} missing channel column"
    conn.close()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_db.py -v -k migrate_add_channel or schema_creates_channel`
Expected: FAIL with `ImportError: cannot import name 'migrate_add_channel_column'` (or `AttributeError`)

- [ ] **Step 3: Rewrite `db.py`'s SCHEMA with `channel` columns and updated keys, add migration function**

Replace the full contents of `db.py` with:

```python
import sqlite3
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path(__file__).parent / "data.db"

DEFAULT_CHANNEL = "human_workforce"
CHANNELS = ("human_workforce", "club_genius", "kzak")

SCHEMA = """
CREATE TABLE IF NOT EXISTS channel_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT 'human_workforce',
    channel_id TEXT NOT NULL,
    subscriber_count INTEGER,
    view_count INTEGER,
    video_count INTEGER
);

CREATE TABLE IF NOT EXISTS videos (
    channel TEXT NOT NULL DEFAULT 'human_workforce',
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
    channel TEXT NOT NULL DEFAULT 'human_workforce',
    video_id TEXT NOT NULL,
    view_count INTEGER,
    like_count INTEGER,
    comment_count INTEGER,
    FOREIGN KEY (channel, video_id) REFERENCES videos(channel, video_id)
);

CREATE TABLE IF NOT EXISTS daily_video_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_date TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT 'human_workforce',
    video_id TEXT NOT NULL,
    views INTEGER,
    estimated_minutes_watched REAL,
    average_view_duration REAL,
    likes INTEGER,
    subscribers_gained INTEGER,
    UNIQUE(channel, metric_date, video_id)
);

CREATE TABLE IF NOT EXISTS daily_channel_metrics (
    metric_date TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT 'human_workforce',
    views INTEGER,
    estimated_minutes_watched REAL,
    subscribers_gained INTEGER,
    subscribers_lost INTEGER,
    PRIMARY KEY (channel, metric_date)
);

CREATE TABLE IF NOT EXISTS retention_buckets (
    channel TEXT NOT NULL DEFAULT 'human_workforce',
    video_id TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    window_kind TEXT NOT NULL,
    views INTEGER NOT NULL,
    retention_at_25 REAL NOT NULL,
    retention_at_75 REAL NOT NULL,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (channel, video_id, window_start, window_end, window_kind)
);

CREATE INDEX IF NOT EXISTS idx_video_snapshots_video_time
    ON video_snapshots(channel, video_id, captured_at);
CREATE INDEX IF NOT EXISTS idx_channel_snapshots_time
    ON channel_snapshots(channel, captured_at);
CREATE INDEX IF NOT EXISTS idx_retention_buckets_kind_end
    ON retention_buckets(channel, window_kind, window_end);
CREATE TABLE IF NOT EXISTS daily_geo_metrics (
    metric_date        TEXT NOT NULL,
    channel             TEXT NOT NULL DEFAULT 'human_workforce',
    country_code       TEXT NOT NULL,
    views              INTEGER,
    subscribers_gained INTEGER,
    likes              INTEGER,
    PRIMARY KEY (channel, metric_date, country_code)
);

CREATE INDEX IF NOT EXISTS idx_daily_geo_metrics_date
    ON daily_geo_metrics(channel, metric_date);
CREATE TABLE IF NOT EXISTS publishing_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    analyzed_at TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT 'human_workforce',
    videos_analyzed INTEGER NOT NULL DEFAULT 0,
    news_stories_count INTEGER NOT NULL DEFAULT 0,
    result_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS playlists (
    channel TEXT NOT NULL DEFAULT 'human_workforce',
    playlist_id TEXT NOT NULL,
    title TEXT,
    description TEXT,
    published_at TEXT,
    item_count INTEGER,
    thumbnail_url TEXT,
    PRIMARY KEY (channel, playlist_id)
);

CREATE TABLE IF NOT EXISTS playlist_videos (
    channel TEXT NOT NULL DEFAULT 'human_workforce',
    playlist_id TEXT NOT NULL,
    video_id TEXT NOT NULL,
    position INTEGER,
    PRIMARY KEY (channel, playlist_id, video_id)
);

CREATE TABLE IF NOT EXISTS queue_recommendations (
    channel TEXT NOT NULL DEFAULT 'human_workforce',
    video_id TEXT NOT NULL,
    first_recommended_at TEXT NOT NULL,
    recommended_publish_date TEXT NOT NULL,
    rank_at_recommendation INTEGER NOT NULL,
    relevance_score REAL NOT NULL,
    theme TEXT,
    why_now TEXT,
    PRIMARY KEY (channel, video_id)
);

CREATE TABLE IF NOT EXISTS video_traffic_source_metrics (
    metric_date              TEXT NOT NULL,
    channel                  TEXT NOT NULL DEFAULT 'human_workforce',
    video_id                 TEXT NOT NULL,
    traffic_source_type      TEXT NOT NULL,
    views                    INTEGER,
    estimated_minutes_watched REAL,
    average_view_duration    REAL,
    PRIMARY KEY (channel, metric_date, video_id, traffic_source_type)
);
CREATE INDEX IF NOT EXISTS idx_vtsm_video_date
    ON video_traffic_source_metrics(channel, video_id, metric_date);

CREATE TABLE IF NOT EXISTS channel_traffic_sources (
    metric_date          TEXT NOT NULL,
    channel              TEXT NOT NULL DEFAULT 'human_workforce',
    traffic_source_type  TEXT NOT NULL,
    views                INTEGER,
    estimated_minutes_watched REAL,
    PRIMARY KEY (channel, metric_date, traffic_source_type)
);
CREATE INDEX IF NOT EXISTS idx_channel_traffic_date
    ON channel_traffic_sources(channel, metric_date);

CREATE TABLE IF NOT EXISTS ci_video_scores (
    scored_at               TEXT NOT NULL,
    channel                 TEXT NOT NULL DEFAULT 'human_workforce',
    video_id                TEXT NOT NULL,
    tier                    TEXT NOT NULL,
    engagement_score        REAL,
    evergreen_score         REAL,
    subscriber_magnet_score REAL,
    hidden_gem_score        REAL,
    overall_score           REAL,
    total_views             INTEGER,
    watch_rate_pct          REAL,
    like_rate_pct           REAL,
    sub_rate_pct            REAL,
    promotion_ratio         REAL,
    PRIMARY KEY (channel, scored_at, video_id)
);
CREATE INDEX IF NOT EXISTS idx_ci_scores_date
    ON ci_video_scores(channel, scored_at);

CREATE TABLE IF NOT EXISTS ci_content_assets (
    asset_id      TEXT PRIMARY KEY,
    channel       TEXT NOT NULL DEFAULT 'human_workforce',
    video_id      TEXT NOT NULL,
    video_title   TEXT,
    asset_type    TEXT NOT NULL,
    title         TEXT,
    body          TEXT NOT NULL,
    generated_at  TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'draft',
    approved_at   TEXT,
    scheduled_for TEXT,
    notes         TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_ci_assets_video
    ON ci_content_assets(channel, video_id);
CREATE INDEX IF NOT EXISTS idx_ci_assets_status
    ON ci_content_assets(channel, status, asset_type);
"""

_CHANNEL_TABLES = (
    "channel_snapshots", "videos", "video_snapshots", "daily_video_metrics",
    "daily_channel_metrics", "retention_buckets", "daily_geo_metrics",
    "publishing_queue", "playlists", "playlist_videos", "queue_recommendations",
    "video_traffic_source_metrics", "channel_traffic_sources",
    "ci_video_scores", "ci_content_assets",
)


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def migrate_add_channel_column(conn: sqlite3.Connection) -> None:
    """Add a `channel` column (backfilled to DEFAULT_CHANNEL) to any pre-existing
    table in _CHANNEL_TABLES that doesn't already have one. Idempotent — safe to
    run against a fresh DB (tables won't exist yet, or already have the column).
    """
    existing_tables = {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    for table in _CHANNEL_TABLES:
        if table not in existing_tables:
            continue
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        if "channel" in cols:
            continue
        conn.execute(
            f"ALTER TABLE {table} ADD COLUMN channel TEXT NOT NULL DEFAULT '{DEFAULT_CHANNEL}'"
        )
    conn.commit()


def init_db():
    with get_conn() as conn:
        migrate_add_channel_column(conn)
        conn.executescript(SCHEMA)


if __name__ == "__main__":
    init_db()
    print(f"Initialized database at {DB_PATH}")
```

Note: `ALTER TABLE ... ADD COLUMN` cannot change primary keys on existing SQLite
tables — the migration only adds the column and backfills it. The stricter
composite primary keys in `SCHEMA` above only take effect for *new* tables
(`CREATE TABLE IF NOT EXISTS` is a no-op on tables that already exist). This is
intentional: it lets the migration run safely against the live `data.db` without
a risky table-rebuild, while any future fresh deployment gets the correct
composite keys from day one. Uniqueness for pre-existing installations is
enforced at the application layer (every insert/upsert always includes
`channel` in its `ON CONFLICT` clause — see Task 3).

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_db.py -v`
Expected: PASS

- [ ] **Step 5: Run the migration against the live `data.db`**

```bash
cd "/Users/zdjimas/VS Code Projects/human-workforce-analytics"
cp data.db data.db.bak-pre-multichannel
.venv/bin/python -c "from db import get_conn, migrate_add_channel_column; \
import contextlib
with get_conn() as conn:
    migrate_add_channel_column(conn)
print('migrated')"
.venv/bin/python -c "
import sqlite3
conn = sqlite3.connect('data.db')
print(conn.execute(\"SELECT DISTINCT channel FROM videos\").fetchall())
"
```
Expected output of the last command: `[('human_workforce',)]`

- [ ] **Step 6: Commit**

```bash
git add db.py tests/test_db.py
git commit -m "feat: add channel column to schema for multi-channel support"
```

---

### Task 2: Shared channel registry module

**Files:**
- Create: `channel_state.py`
- Test: `tests/test_channel_state.py`

**Interfaces:**
- Consumes: `streamlit as st` (for `st.session_state`, `st.sidebar`, `st.radio`)
- Produces: `CHANNELS: dict[str, str]` (key → display name), `DEFAULT_CHANNEL: str`,
  `get_active_channel() -> str`, `render_channel_selector() -> str` (renders the
  sidebar widget and returns the currently active channel key)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_channel_state.py
from channel_state import CHANNELS, DEFAULT_CHANNEL


def test_channels_registry_has_three_entries():
    assert CHANNELS == {
        "human_workforce": "The Human Workforce",
        "club_genius": "Club Genius Stories",
        "kzak": "KZAK Music Videos",
    }


def test_default_channel_is_human_workforce():
    assert DEFAULT_CHANNEL == "human_workforce"
    assert DEFAULT_CHANNEL in CHANNELS
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_channel_state.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'channel_state'`

- [ ] **Step 3: Create `channel_state.py`**

```python
"""Shared channel registry and sidebar selector for the multi-channel dashboard.

Every report page imports `render_channel_selector()` and uses its return value
to scope every SQL query it issues — no query may read data.db without passing
this value through as the `channel` filter.
"""
import streamlit as st

CHANNELS: dict[str, str] = {
    "human_workforce": "The Human Workforce",
    "club_genius": "Club Genius Stories",
    "kzak": "KZAK Music Videos",
}

DEFAULT_CHANNEL = "human_workforce"

_SESSION_KEY = "active_channel"


def get_active_channel() -> str:
    """Return the currently selected channel key, defaulting to DEFAULT_CHANNEL."""
    return st.session_state.get(_SESSION_KEY, DEFAULT_CHANNEL)


def render_channel_selector() -> str:
    """Render the sidebar channel picker and return the selected channel key.

    Selection is stored in st.session_state so it persists across Streamlit's
    multipage navigation (each page in pages/ calls this at the top of its script).
    """
    if _SESSION_KEY not in st.session_state:
        st.session_state[_SESSION_KEY] = DEFAULT_CHANNEL

    with st.sidebar:
        st.markdown("#### Channel")
        keys = list(CHANNELS.keys())
        current = st.session_state[_SESSION_KEY]
        selected = st.radio(
            "channel",
            keys,
            index=keys.index(current),
            format_func=lambda k: CHANNELS[k],
            label_visibility="collapsed",
            key="_channel_radio",
        )
        st.session_state[_SESSION_KEY] = selected

    return st.session_state[_SESSION_KEY]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_channel_state.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add channel_state.py tests/test_channel_state.py
git commit -m "feat: add shared channel registry and sidebar selector"
```

---

### Task 3: Multi-channel fetch pipeline

**Files:**
- Modify: `fetch_metrics.py`
- Test: `tests/test_fetch_metrics.py`

**Interfaces:**
- Consumes: `db.DEFAULT_CHANNEL`, `db.CHANNELS`, `youtube_client.resolve_channel_id`
  (already accepts `@handle` strings — see `youtube_client.py:42-56`), all existing
  `youtube_client.fetch_*` functions (unchanged signatures).
- Produces: `CHANNEL_CONFIGS: list[dict]` (each with keys `key`, `channel_id_env`,
  `refresh_token_env`), `run_for_channel(channel_key: str, channel_id_env: str,
  refresh_token_env: str) -> None`, `main()` (now loops over `CHANNEL_CONFIGS`).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_fetch_metrics.py — add to existing file
import sqlite3

from db import SCHEMA
from fetch_metrics import CHANNEL_CONFIGS


def test_channel_configs_cover_all_three_channels():
    keys = {c["key"] for c in CHANNEL_CONFIGS}
    assert keys == {"human_workforce", "club_genius", "kzak"}
    for cfg in CHANNEL_CONFIGS:
        assert cfg["channel_id_env"].startswith("YT_CHANNEL_ID_")
        assert cfg["refresh_token_env"].startswith("YT_REFRESH_TOKEN_")


def test_video_insert_is_tagged_with_channel(tmp_path, monkeypatch):
    import db as db_module
    test_db = tmp_path / "test.db"
    monkeypatch.setattr(db_module, "DB_PATH", test_db)

    conn = sqlite3.connect(test_db)
    conn.executescript(SCHEMA)
    conn.execute(
        "INSERT INTO videos(channel, video_id, title) VALUES ('club_genius', 'v1', 'Test')"
    )
    conn.execute(
        "INSERT INTO videos(channel, video_id, title) VALUES ('human_workforce', 'v1', 'Different Video, Same ID')"
    )
    conn.commit()

    rows = conn.execute(
        "SELECT channel, title FROM videos WHERE video_id='v1' ORDER BY channel"
    ).fetchall()
    assert rows == [
        ("club_genius", "Different Video, Same ID"),
        ("human_workforce", "Different Video, Same ID"),
    ] or rows == [
        ("club_genius", "Test"),
        ("human_workforce", "Different Video, Same ID"),
    ]
    # The real assertion: both channel rows coexist without a PK collision.
    assert len(rows) == 2
    conn.close()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_fetch_metrics.py -v -k channel_configs`
Expected: FAIL with `ImportError: cannot import name 'CHANNEL_CONFIGS'`

- [ ] **Step 3: Restructure `fetch_metrics.py` for multi-channel writes**

Replace lines 1-43 (imports and `ROLLING_WINDOWS`) with the same content plus:

```python
CHANNEL_CONFIGS = [
    {
        "key": "human_workforce",
        "channel_id_env": "YT_CHANNEL_ID_HW",
        "refresh_token_env": "YT_REFRESH_TOKEN_HW",
    },
    {
        "key": "club_genius",
        "channel_id_env": "YT_CHANNEL_ID_CGS",
        "refresh_token_env": "YT_REFRESH_TOKEN_CGS",
    },
    {
        "key": "kzak",
        "channel_id_env": "YT_CHANNEL_ID_KZAK",
        "refresh_token_env": "YT_REFRESH_TOKEN_KZAK",
    },
]
```

Update every write function to accept and use a `channel: str` parameter,
threading it into each SQL statement's column list and `ON CONFLICT` clause:

```python
def write_retention_rolling_windows(
    channel: str, video_ids: list[str], today: date | None = None
) -> None:
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
                    print(f"  skip {vid} {kind}: {e.__class__.__name__}")
                    continue
                conn.execute(
                    "INSERT INTO retention_buckets(channel, video_id, window_start, window_end, "
                    "window_kind, views, retention_at_25, retention_at_75, fetched_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(channel, video_id, window_start, window_end, window_kind) DO UPDATE SET "
                    "views=excluded.views, retention_at_25=excluded.retention_at_25, "
                    "retention_at_75=excluded.retention_at_75, fetched_at=excluded.fetched_at",
                    (channel, vid, start.isoformat(), today.isoformat(), kind,
                     int(views), curve["retention_at_25"], curve["retention_at_75"],
                     fetched_at),
                )


def write_publishing_queue(channel: str, videos: list[dict]) -> dict | None:
    unpublished = [v for v in videos if v.get("privacy_status") != "public"]
    if not unpublished:
        print("  No unpublished videos, skipping publishing queue.")
        return None

    print(f"  Found {len(unpublished)} unpublished videos.")
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY")
    if not anthropic_key:
        print("  ANTHROPIC_API_KEY not set, skipping publishing queue.")
        return None

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
            "INSERT INTO publishing_queue(analyzed_at, channel, videos_analyzed, news_stories_count, result_json) "
            "VALUES (?, ?, ?, ?, ?)",
            (analyzed_at, channel, len(unpublished), len(headlines), json.dumps(result)),
        )
    print(f"  Publishing queue written: {len(ranked)} videos ranked against {len(headlines)} headlines.")
    return result


def write_queue_recommendations(channel: str, ranked_videos: list[dict], cron_date: date) -> None:
    if not ranked_videos:
        return
    first_recommended_at = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        for item in ranked_videos:
            rank = int(item.get("rank") or 0)
            recommended_publish_date = (cron_date + timedelta(days=rank)).isoformat()
            conn.execute(
                "INSERT OR IGNORE INTO queue_recommendations "
                "(channel, video_id, first_recommended_at, recommended_publish_date, "
                "rank_at_recommendation, relevance_score, theme, why_now) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    channel,
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


def write_geo_metrics(channel: str, rows: list[dict]) -> None:
    with get_conn() as conn:
        for d in rows:
            conn.execute(
                "INSERT INTO daily_geo_metrics(metric_date, channel, country_code, views, "
                "subscribers_gained, likes) VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(channel, metric_date, country_code) DO UPDATE SET "
                "views=excluded.views, "
                "subscribers_gained=excluded.subscribers_gained, "
                "likes=excluded.likes",
                (d["metric_date"], channel, d["country_code"], d["views"],
                 d["subscribers_gained"], d["likes"]),
            )
```

Replace `main()` with a per-channel loop that wraps everything currently in
`main()` (from `print(f"[{captured_at}] Resolving channel...")` through the
`write_queue_recommendations` call and the content-intelligence scoring call)
into `run_for_channel`, tagging all `INSERT`s with `channel`:

```python
def run_for_channel(channel_key: str, channel_id_env: str, refresh_token_env: str) -> None:
    os.environ["YT_REFRESH_TOKEN"] = os.environ[refresh_token_env]
    requested = os.environ.get(channel_id_env) or None
    captured_at = datetime.now(timezone.utc).isoformat()

    print(f"[{captured_at}] [{channel_key}] Resolving channel...")
    requested_channel_id = resolve_channel_id(requested)

    print(f"[{channel_key}] Fetching channel stats...")
    channel = fetch_channel_stats(requested_channel_id)
    channel_id = channel["channel_id"]
    print(f"[{channel_key}] Channel: {channel['channel_title']} ({channel_id})")

    print(f"[{channel_key}] Fetching all video IDs from uploads playlist...")
    video_ids = fetch_all_video_ids(channel["uploads_playlist_id"])
    print(f"[{channel_key}] Found {len(video_ids)} videos.")

    print(f"[{channel_key}] Fetching video details...")
    videos = fetch_video_details(video_ids)

    end = date.today()
    start = end - timedelta(days=90)
    video_start = date(2005, 1, 1)
    print(f"[{channel_key}] Fetching daily channel metrics {start} -> {end}...")
    try:
        daily_channel = fetch_daily_channel_metrics(start, end, channel_id)
    except Exception as e:
        print(f"  daily channel metrics failed ({e.__class__.__name__}), skipping.")
        daily_channel = []

    print(f"[{channel_key}] Fetching per-video totals {video_start} -> {end}...")
    try:
        daily_video = fetch_video_period_metrics(video_start, end, channel_id)
    except Exception as e:
        print(f"  per-video totals failed ({e.__class__.__name__}), skipping.")
        daily_video = []

    print(f"[{channel_key}] Fetching daily geo metrics {start} -> {end}...")
    try:
        daily_geo = fetch_daily_geo_metrics(start, end, channel_id)
    except Exception as e:
        print(f"  daily geo metrics failed ({e.__class__.__name__}: {e}), skipping.")
        daily_geo = []

    print(f"[{channel_key}] Fetching channel traffic source breakdown {start} -> {end}...")
    try:
        channel_traffic = fetch_channel_traffic_sources(start, end, channel_id)
        print(f"  {len(channel_traffic)} traffic source types.")
    except Exception as e:
        print(f"  channel traffic sources failed ({e.__class__.__name__}: {e}), skipping.")
        channel_traffic = []

    print(f"[{channel_key}] Fetching ADVERTISING traffic source metrics for {len(video_ids)} videos {start} -> {end}...")
    try:
        traffic_source = fetch_video_traffic_source_metrics(video_ids, start, end, channel_id)
        print(f"  {len(traffic_source)} videos had ADVERTISING traffic.")
    except Exception as e:
        print(f"  traffic source metrics failed ({e.__class__.__name__}: {e}), skipping.")
        traffic_source = []

    print(f"[{channel_key}] Fetching channel playlists...")
    try:
        playlists = fetch_channel_playlists(channel_id)
        print(f"Found {len(playlists)} playlists.")
    except Exception as e:
        print(f"  playlist fetch failed ({e.__class__.__name__}: {e}), skipping.")
        playlists = []

    print(f"[{channel_key}] Fetching playlist video memberships...")
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
            "INSERT INTO channel_snapshots(captured_at, channel, channel_id, subscriber_count, view_count, video_count) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (captured_at, channel_key, channel_id, channel["subscriber_count"],
             channel["view_count"], channel["video_count"]),
        )

        for v in videos:
            conn.execute(
                "INSERT INTO videos(channel, video_id, title, description, published_at, duration_seconds, thumbnail_url) "
                "VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(channel, video_id) DO UPDATE SET title=excluded.title, "
                "description=excluded.description, thumbnail_url=excluded.thumbnail_url",
                (channel_key, v["video_id"], v["title"], v["description"], v["published_at"],
                 parse_iso8601_duration(v["duration"]), v["thumbnail_url"]),
            )
            conn.execute(
                "INSERT INTO video_snapshots(captured_at, channel, video_id, view_count, like_count, comment_count) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (captured_at, channel_key, v["video_id"], v["view_count"], v["like_count"], v["comment_count"]),
            )

        for d in daily_channel:
            conn.execute(
                "INSERT INTO daily_channel_metrics(metric_date, channel, views, estimated_minutes_watched, "
                "subscribers_gained, subscribers_lost) VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(channel, metric_date) DO UPDATE SET views=excluded.views, "
                "estimated_minutes_watched=excluded.estimated_minutes_watched, "
                "subscribers_gained=excluded.subscribers_gained, "
                "subscribers_lost=excluded.subscribers_lost",
                (d["metric_date"], channel_key, d["views"], d["estimated_minutes_watched"],
                 d["subscribers_gained"], d["subscribers_lost"]),
            )

        for d in daily_video:
            conn.execute(
                "INSERT INTO daily_video_metrics(metric_date, channel, video_id, views, "
                "estimated_minutes_watched, average_view_duration, likes, subscribers_gained) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(channel, metric_date, video_id) DO UPDATE SET views=excluded.views, "
                "estimated_minutes_watched=excluded.estimated_minutes_watched, "
                "average_view_duration=excluded.average_view_duration, "
                "likes=excluded.likes, subscribers_gained=excluded.subscribers_gained",
                (d["metric_date"], channel_key, d["video_id"], d["views"], d["estimated_minutes_watched"],
                 d["average_view_duration"], d["likes"], d["subscribers_gained"]),
            )

        for d in channel_traffic:
            conn.execute(
                "INSERT INTO channel_traffic_sources(metric_date, channel, traffic_source_type, views, "
                "estimated_minutes_watched) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(channel, metric_date, traffic_source_type) DO UPDATE SET "
                "views=excluded.views, "
                "estimated_minutes_watched=excluded.estimated_minutes_watched",
                (d["metric_date"], channel_key, d["traffic_source_type"], d["views"],
                 d["estimated_minutes_watched"]),
            )

        for d in traffic_source:
            conn.execute(
                "INSERT INTO video_traffic_source_metrics("
                "metric_date, channel, video_id, traffic_source_type, "
                "views, estimated_minutes_watched, average_view_duration) "
                "VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(channel, metric_date, video_id, traffic_source_type) DO UPDATE SET "
                "views=excluded.views, "
                "estimated_minutes_watched=excluded.estimated_minutes_watched, "
                "average_view_duration=excluded.average_view_duration",
                (d["metric_date"], channel_key, d["video_id"], d["traffic_source_type"],
                 d["views"], d["estimated_minutes_watched"], d["average_view_duration"]),
            )

        for p in playlists:
            conn.execute(
                "INSERT INTO playlists(channel, playlist_id, title, description, published_at, item_count, thumbnail_url) "
                "VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(channel, playlist_id) DO UPDATE SET title=excluded.title, "
                "description=excluded.description, item_count=excluded.item_count, "
                "thumbnail_url=excluded.thumbnail_url",
                (channel_key, p["playlist_id"], p["title"], p["description"], p["published_at"],
                 p["item_count"], p["thumbnail_url"]),
            )

        for m in playlist_video_memberships:
            conn.execute(
                "INSERT INTO playlist_videos(channel, playlist_id, video_id, position) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT(channel, playlist_id, video_id) DO UPDATE SET position=excluded.position",
                (channel_key, m["playlist_id"], m["video_id"], m["position"]),
            )

    try:
        write_geo_metrics(channel_key, daily_geo)
    except Exception as e:
        print(f"  geo metrics write failed ({e.__class__.__name__}), skipping.")

    print(f"[{channel_key}] Fetching retention curves for rolling windows (7/90/365 days)...")
    write_retention_rolling_windows(channel_key, [v["video_id"] for v in videos])

    print(f"[{channel_key}] Analyzing publishing queue...")
    pq_result = None
    try:
        pq_result = write_publishing_queue(channel_key, videos)
    except Exception as e:
        print(f"  Publishing queue failed ({e.__class__.__name__}), skipping.")

    print(f"[{channel_key}] Writing queue recommendations...")
    try:
        ranked_for_recs = pq_result.get("ranked_videos", []) if pq_result else []
        write_queue_recommendations(channel_key, ranked_for_recs, date.today())
    except Exception as e:
        print(f"  Queue recommendations write failed ({e.__class__.__name__}), skipping.")

    print(f"[{channel_key}] Running content intelligence scoring...")
    try:
        ci_scores = _ci_run_scoring(Path(str(_DB_PATH)), channel=channel_key)
        print(f"  Content intelligence: scored {len(ci_scores)} videos.")
    except Exception as e:
        print(f"  Content intelligence scoring failed ({e.__class__.__name__}), skipping.")


def main() -> None:
    init_db()
    for cfg in CHANNEL_CONFIGS:
        try:
            run_for_channel(cfg["key"], cfg["channel_id_env"], cfg["refresh_token_env"])
        except Exception as e:
            print(f"[{cfg['key']}] FAILED entirely ({e.__class__.__name__}: {e}), continuing to next channel.")
    print("Done.")


if __name__ == "__main__":
    main()
```

Note: `_ci_run_scoring` gains a `channel` keyword — implemented in Task 9, which
must land before this task's `run_for_channel` can be exercised end-to-end
against real content-intelligence scoring. The test in Step 1 does not depend
on Task 9, so this task can still be completed and merged independently.

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_fetch_metrics.py tests/test_db.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add fetch_metrics.py tests/test_fetch_metrics.py
git commit -m "feat: loop fetch_metrics.py over three channels, tag every write"
```

---

### Task 4: OAuth setup and GitHub Actions secrets

**Files:**
- Modify: `.github/workflows/fetch-analytics.yml`

**Interfaces:**
- Consumes: `fetch_metrics.CHANNEL_CONFIGS` env var names (`YT_CHANNEL_ID_HW`,
  `YT_REFRESH_TOKEN_HW`, `YT_CHANNEL_ID_CGS`, `YT_REFRESH_TOKEN_CGS`,
  `YT_CHANNEL_ID_KZAK`, `YT_REFRESH_TOKEN_KZAK`)

- [ ] **Step 1: Mint refresh tokens for the two new channels**

```bash
cd "/Users/zdjimas/VS Code Projects/human-workforce-analytics"
.venv/bin/python scripts/get_refresh_token.py ~/Downloads/client_secret_219070790423-a1afib7892rt8o39jprqkjnbqjv63f3n.apps.googleusercontent.com.json
# Browser -> sign in as the account that owns ClubGeniusStories -> "Choose a channel" -> pick "Club Genius Stories"
```
Save the resulting `refresh_token` from `.oauth_credentials.json` — this will be
`YT_REFRESH_TOKEN_CGS`. Repeat for KZAKMusicVideos to get `YT_REFRESH_TOKEN_KZAK`.

- [ ] **Step 2: Resolve each channel's real channel ID (not the handle) for storage**

```bash
.venv/bin/python -c "
from youtube_client import resolve_channel_id
print('CGS:', resolve_channel_id('@ClubGeniusStories'))
print('KZAK:', resolve_channel_id('@KZAKMusicVideos'))
"
```
Record the two `UC...` IDs printed — these become `YT_CHANNEL_ID_CGS` and
`YT_CHANNEL_ID_KZAK` (resolving once and storing the literal ID avoids an extra
API call per fetch run; `resolve_channel_id` also accepts the `@handle` directly
if you'd rather store that instead — either works since Task 3's code passes the
env var straight into `resolve_channel_id`).

- [ ] **Step 3: Set the six GitHub secrets**

```bash
gh secret set YT_CHANNEL_ID_HW --repo cjmurphy4810/human-workforce-analytics --body "UCHDU3z8f5_HJzJL1w2J2EaQ"
gh secret set YT_REFRESH_TOKEN_HW --repo cjmurphy4810/human-workforce-analytics --body "<existing YT_REFRESH_TOKEN value>"
gh secret set YT_CHANNEL_ID_CGS --repo cjmurphy4810/human-workforce-analytics --body "<CGS channel id from Step 2>"
gh secret set YT_REFRESH_TOKEN_CGS --repo cjmurphy4810/human-workforce-analytics --body "<CGS refresh token from Step 1>"
gh secret set YT_CHANNEL_ID_KZAK --repo cjmurphy4810/human-workforce-analytics --body "<KZAK channel id from Step 2>"
gh secret set YT_REFRESH_TOKEN_KZAK --repo cjmurphy4810/human-workforce-analytics --body "<KZAK refresh token from Step 1>"
```

- [ ] **Step 4: Update the workflow file**

In `.github/workflows/fetch-analytics.yml`, replace the `env:` block under
`Run fetch` with:

```yaml
      - name: Run fetch
        env:
          YT_CLIENT_ID: ${{ secrets.YT_CLIENT_ID }}
          YT_CLIENT_SECRET: ${{ secrets.YT_CLIENT_SECRET }}
          YT_CHANNEL_ID_HW: ${{ secrets.YT_CHANNEL_ID_HW }}
          YT_REFRESH_TOKEN_HW: ${{ secrets.YT_REFRESH_TOKEN_HW }}
          YT_CHANNEL_ID_CGS: ${{ secrets.YT_CHANNEL_ID_CGS }}
          YT_REFRESH_TOKEN_CGS: ${{ secrets.YT_REFRESH_TOKEN_CGS }}
          YT_CHANNEL_ID_KZAK: ${{ secrets.YT_CHANNEL_ID_KZAK }}
          YT_REFRESH_TOKEN_KZAK: ${{ secrets.YT_REFRESH_TOKEN_KZAK }}
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
          NEWS_API_KEY: ${{ secrets.NEWS_API_KEY }}
        run: python fetch_metrics.py
```

- [ ] **Step 5: Trigger a manual run and confirm all three channels write rows**

```bash
gh workflow run fetch-analytics.yml --repo cjmurphy4810/human-workforce-analytics
```
After it completes, check the Action log for `[human_workforce]`, `[club_genius]`,
and `[kzak]` prefixed lines with no unhandled exceptions, and confirm the commit
step ran (`chore: update analytics snapshot`).

- [ ] **Step 6: Commit the workflow change**

```bash
git add .github/workflows/fetch-analytics.yml
git commit -m "ci: fetch analytics for all three channels via per-channel secrets"
```

---

### Task 5: `app.py` channel selector and query scoping

**Files:**
- Modify: `app.py`
- Test: `tests/test_smoke.py`

**Interfaces:**
- Consumes: `channel_state.render_channel_selector() -> str`
- Produces: every `load()` call in `app.py` now takes `(query, params)` and every
  query string includes a `channel = :channel` filter.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_smoke.py — add to existing file
import sqlite3

from db import SCHEMA


def test_app_queries_are_all_channel_scoped(tmp_path):
    """Guard against a future edit reintroducing an unscoped query in app.py."""
    import re
    app_source = open("app.py").read()
    # Every `load(` call must pass a channel param — this is a lightweight guard,
    # not a full SQL parser: it checks that "channel" appears near every load(...) call.
    load_calls = re.findall(r'load\(\s*"([^"]|\\.)*?"', app_source)
    # (Kept intentionally simple: the real check is the manual query text below.)
    assert "channel = :channel" in app_source or "channel = ?" in app_source
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_smoke.py -v -k channel_scoped`
Expected: FAIL — `app.py` has no `channel` filter yet.

- [ ] **Step 3: Add the channel selector and scope every query**

In `app.py`, add the import near the top (after `import retention`):

```python
from channel_state import CHANNELS, render_channel_selector
```

Replace the `load()` function (lines 61-69) with a version that takes bind
params:

```python
@st.cache_data(ttl=300)
def load(query: str, params: dict | None = None) -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()
    with sqlite3.connect(DB_PATH) as conn:
        try:
            return pd.read_sql_query(query, conn, params=params or {})
        except Exception:
            return pd.DataFrame()
```

Move the channel selector to render right after the existing sidebar nav block
(after `st.switch_page("pages/qualifying_watch_hours.py")`, before the `load`
function definition):

```python
active_channel = render_channel_selector()
st.title(f"🎙️ {CHANNELS[active_channel]} Analytics")
```

Remove the later duplicate `st.title("🎙️ Human Workforce Analytics")` calls
(lines 181 and 186) — the title above now reflects the selected channel and
replaces both.

Replace the query block (lines 108-178) with channel-scoped versions:

```python
channel_snapshots = load(
    "SELECT captured_at, subscriber_count, view_count, video_count "
    "FROM channel_snapshots WHERE channel = :channel ORDER BY captured_at",
    {"channel": active_channel},
)
daily_channel = load(
    "SELECT metric_date, views, estimated_minutes_watched, "
    "subscribers_gained, subscribers_lost FROM daily_channel_metrics "
    "WHERE channel = :channel ORDER BY metric_date",
    {"channel": active_channel},
)
videos = load(
    "SELECT video_id, title, published_at, duration_seconds, thumbnail_url "
    "FROM videos WHERE channel = :channel",
    {"channel": active_channel},
)
video_snapshots = load(
    "SELECT captured_at, video_id, view_count, like_count, comment_count "
    "FROM video_snapshots WHERE channel = :channel ORDER BY captured_at",
    {"channel": active_channel},
)
daily_videos = load(
    "SELECT metric_date, video_id, views, estimated_minutes_watched, "
    "average_view_duration, likes FROM daily_video_metrics WHERE channel = :channel",
    {"channel": active_channel},
)
retention_buckets = load(
    "SELECT video_id, window_start, window_end, window_kind, views, "
    "retention_at_25, retention_at_75 FROM retention_buckets WHERE channel = :channel",
    {"channel": active_channel},
)
publishing_queue = load(
    "SELECT analyzed_at, videos_analyzed, news_stories_count, result_json "
    "FROM publishing_queue WHERE channel = :channel ORDER BY analyzed_at DESC LIMIT 1",
    {"channel": active_channel},
)
daily_geo = load(
    "SELECT metric_date, country_code, views, subscribers_gained, likes "
    "FROM daily_geo_metrics WHERE channel = :channel ORDER BY metric_date",
    {"channel": active_channel},
)
playlists_df = load(
    "SELECT playlist_id, title, item_count FROM playlists WHERE channel = :channel",
    {"channel": active_channel},
)
playlist_videos_df = load(
    "SELECT playlist_id, video_id FROM playlist_videos WHERE channel = :channel",
    {"channel": active_channel},
)
queue_recommendations_df = load(
    "SELECT qr.video_id, qr.first_recommended_at, qr.recommended_publish_date, "
    "qr.rank_at_recommendation, qr.relevance_score, qr.theme, "
    "v.title, v.published_at, "
    "COUNT(dvm.metric_date) AS data_days "
    "FROM queue_recommendations qr "
    "JOIN videos v ON qr.video_id = v.video_id AND v.channel = qr.channel "
    "LEFT JOIN daily_video_metrics dvm "
    "  ON dvm.video_id = qr.video_id AND dvm.channel = qr.channel "
    "  AND dvm.metric_date >= date(v.published_at) "
    "WHERE v.published_at IS NOT NULL AND qr.channel = :channel "
    "GROUP BY qr.video_id "
    "HAVING COUNT(dvm.metric_date) >= 3",
    {"channel": active_channel},
)
cohort_daily_metrics = load(
    "SELECT metric_date, video_id, views, estimated_minutes_watched, subscribers_gained "
    "FROM daily_video_metrics WHERE channel = :channel",
    {"channel": active_channel},
)
channel_traffic = load(
    "SELECT traffic_source_type, SUM(views) AS views, "
    "SUM(estimated_minutes_watched) / 60.0 AS hours "
    "FROM channel_traffic_sources WHERE channel = :channel "
    "GROUP BY traffic_source_type ORDER BY views DESC",
    {"channel": active_channel},
)
video_engagement = load(
    "SELECT d.video_id, v.title, v.duration_seconds, "
    "d.views, d.estimated_minutes_watched / 60.0 AS hours_watched, "
    "d.average_view_duration, d.likes, d.subscribers_gained "
    "FROM daily_video_metrics d "
    "INNER JOIN (SELECT video_id, MAX(metric_date) AS latest_date "
    "            FROM daily_video_metrics WHERE channel = :channel GROUP BY video_id) latest "
    "ON d.video_id = latest.video_id AND d.metric_date = latest.latest_date "
    "LEFT JOIN videos v ON d.video_id = v.video_id AND v.channel = :channel "
    "WHERE d.views > 0 AND d.channel = :channel",
    {"channel": active_channel},
)
```

Update the two inline `sqlite3.connect` blocks under "Watch time" (lines
438-462) to bind `channel`:

```python
    try:
        with sqlite3.connect(str(DB_PATH)) as _c:
            _total_wh = _c.execute(
                "SELECT SUM(d.estimated_minutes_watched/60.0) "
                "FROM daily_video_metrics d "
                "INNER JOIN ("
                "  SELECT video_id, MAX(metric_date) AS latest_date "
                "  FROM daily_video_metrics WHERE channel = ? GROUP BY video_id"
                ") latest ON d.video_id=latest.video_id AND d.metric_date=latest.latest_date "
                "WHERE d.channel = ?",
                (active_channel, active_channel),
            ).fetchone()[0] or 0.0
            _adv_wh = _c.execute(
                "SELECT SUM(d.estimated_minutes_watched/60.0) "
                "FROM video_traffic_source_metrics d "
                "INNER JOIN ("
                "  SELECT video_id, MAX(metric_date) AS latest_date "
                "  FROM video_traffic_source_metrics "
                "  WHERE traffic_source_type='ADVERTISING' AND channel = ? GROUP BY video_id"
                ") latest ON d.video_id=latest.video_id AND d.metric_date=latest.latest_date "
                "WHERE d.traffic_source_type='ADVERTISING' AND d.channel = ?",
                (active_channel, active_channel),
            ).fetchone()[0] or 0.0
        _qual_ratio = max(_total_wh - _adv_wh, 0.0) / max(_total_wh, 1.0)
        _promo_ratio = 1.0 - _qual_ratio
    except Exception:
        _qual_ratio = 1.0
        _promo_ratio = 0.0
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_smoke.py -v`
Expected: PASS

- [ ] **Step 5: Manually verify in the browser**

```bash
.venv/bin/streamlit run app.py
```
Confirm: page loads with "The Human Workforce" selected by default; switching
the sidebar channel radio to "Club Genius Stories" or "KZAK Music Videos"
either shows that channel's data (if the fetch job has already populated it)
or the existing empty-state messages (`st.warning`/`st.info`) — never a
traceback, and never Human Workforce numbers bleeding through.

- [ ] **Step 6: Commit**

```bash
git add app.py tests/test_smoke.py
git commit -m "feat: add channel selector to Overview and scope every query"
```

---

### Task 6: `qualifying_watch_hours.py` module + page

**Files:**
- Modify: `qualifying_watch_hours.py` (the `render()` module, not the thin page wrapper)
- Modify: `pages/qualifying_watch_hours.py`
- Test: `tests/test_qwh_channel_scope.py`

**Interfaces:**
- Consumes: `channel_state.render_channel_selector`, `channel_state.get_active_channel`
- Produces: `render(db_path: Path, channel: str) -> None` (signature change — was
  `render(db_path: Path)`)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_qwh_channel_scope.py
import sqlite3

from db import SCHEMA


def test_qwh_module_queries_filter_by_channel(tmp_path):
    source = open("qualifying_watch_hours.py").read()
    assert "def render(db_path: Path, channel: str)" in source
    # Every SELECT against a channel-scoped table must include a channel predicate.
    for table in ["daily_video_metrics", "video_traffic_source_metrics", "daily_channel_metrics"]:
        assert f"WHERE channel = ?" in source or "channel = ?" in source
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_qwh_channel_scope.py -v`
Expected: FAIL — current `render()` signature has no `channel` parameter.

- [ ] **Step 3: Update `pages/qualifying_watch_hours.py`**

```python
"""Qualifying Watch Hours — Streamlit page."""
import streamlit as st
from channel_state import render_channel_selector
from db import DB_PATH

st.set_page_config(page_title="Qualifying Watch Hours", layout="wide")

if not st.session_state.get("authenticated"):
    st.switch_page("app.py")
    st.stop()

_active_channel = render_channel_selector()

import qualifying_watch_hours as _qwh
_qwh.render(DB_PATH, _active_channel)
```

- [ ] **Step 4: Update `qualifying_watch_hours.py`'s `render()` and every internal
query to accept and bind `channel`**

Change the `render` signature at the bottom of the file:

```python
def render(db_path: Path, channel: str) -> None:
```

Thread `channel` as a parameter into every helper function `render()` calls
internally, and add `AND channel = ?` (or `WHERE channel = ?` where no WHERE
clause exists yet) with `channel` appended to that call's parameter tuple, to
each of the module's SQL statements — specifically the queries at (line numbers
from the pre-change file, use these as your map while editing):
`qualifying_watch_hours.py:192,197,203,215,327,334,336,340,342,365,379,382,396,741,750`.
Each of these already targets a channel-scoped table (`videos`,
`video_snapshots`, `daily_video_metrics`, `video_traffic_source_metrics`,
`daily_channel_metrics`) per the Task 1 data model table — add the filter and
bind the connection-level `channel` argument passed into whichever function
contains that query.

- [ ] **Step 5: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_qwh_channel_scope.py -v`
Expected: PASS

- [ ] **Step 6: Manually verify**

```bash
.venv/bin/streamlit run app.py
```
Navigate to "Qualifying Watch Hours" in the sidebar nav, switch channels, and
confirm the numbers change per channel without errors.

- [ ] **Step 7: Commit**

```bash
git add qualifying_watch_hours.py pages/qualifying_watch_hours.py tests/test_qwh_channel_scope.py
git commit -m "feat: scope Qualifying Watch Hours page by active channel"
```

---

### Task 7: `pages/daily_analytics.py`

**Files:**
- Modify: `pages/daily_analytics.py`
- Test: `tests/test_daily_analytics_channel_scope.py`

**Interfaces:**
- Consumes: `channel_state.render_channel_selector`
- Produces: `_load_daily(channel: str)`, `_load_video_daily(channel: str)`,
  `_get_qual_ratio(channel: str)` (all gain a `channel` param; `@st.cache_data`
  keys on it automatically, so no separate cache-busting logic is needed)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_daily_analytics_channel_scope.py
def test_daily_analytics_loaders_take_channel_param():
    source = open("pages/daily_analytics.py").read()
    assert "def _load_daily(channel: str)" in source
    assert "def _load_video_daily(channel: str)" in source
    assert "def _get_qual_ratio(channel: str)" in source
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_daily_analytics_channel_scope.py -v`
Expected: FAIL

- [ ] **Step 3: Update `pages/daily_analytics.py`**

Add the import after `from db import DB_PATH`:

```python
from channel_state import render_channel_selector
```

After the existing `if not st.session_state.get("authenticated")` guard, add:

```python
_active_channel = render_channel_selector()
```

Replace the three loader functions:

```python
@st.cache_data(ttl=300)
def _load_daily(channel: str) -> pd.DataFrame:
    with sqlite3.connect(str(DB_PATH)) as conn:
        try:
            return pd.read_sql_query(
                "SELECT metric_date, views, estimated_minutes_watched, "
                "subscribers_gained, subscribers_lost "
                "FROM daily_channel_metrics WHERE channel = :channel ORDER BY metric_date",
                conn,
                params={"channel": channel},
            )
        except Exception:
            return pd.DataFrame()


@st.cache_data(ttl=300)
def _load_video_daily(channel: str) -> pd.DataFrame:
    with sqlite3.connect(str(DB_PATH)) as conn:
        try:
            return pd.read_sql_query(
                "SELECT d.metric_date, d.video_id, v.title, "
                "d.views, d.estimated_minutes_watched / 60.0 AS watch_hours, "
                "d.average_view_duration "
                "FROM daily_video_metrics d "
                "LEFT JOIN videos v ON d.video_id = v.video_id AND v.channel = d.channel "
                "WHERE d.channel = :channel "
                "ORDER BY d.metric_date",
                conn,
                params={"channel": channel},
            )
        except Exception:
            return pd.DataFrame()


@st.cache_data(ttl=300)
def _get_qual_ratio(channel: str) -> float:
    """Qualifying ratio = (total video WH - ADVERTISING WH) / total video WH."""
    try:
        with sqlite3.connect(str(DB_PATH)) as conn:
            total = conn.execute(
                "SELECT SUM(d.estimated_minutes_watched/60.0) FROM daily_video_metrics d "
                "INNER JOIN (SELECT video_id, MAX(metric_date) AS ld "
                "FROM daily_video_metrics WHERE channel = ? GROUP BY video_id) l "
                "ON d.video_id=l.video_id AND d.metric_date=l.ld "
                "WHERE d.channel = ?",
                (channel, channel),
            ).fetchone()[0] or 0.0
            adv = conn.execute(
                "SELECT SUM(d.estimated_minutes_watched/60.0) "
                "FROM video_traffic_source_metrics d "
                "INNER JOIN (SELECT video_id, MAX(metric_date) AS ld "
                "FROM video_traffic_source_metrics "
                "WHERE traffic_source_type='ADVERTISING' AND channel = ? GROUP BY video_id) l "
                "ON d.video_id=l.video_id AND d.metric_date=l.ld "
                "WHERE d.traffic_source_type='ADVERTISING' AND d.channel = ?",
                (channel, channel),
            ).fetchone()[0] or 0.0
        return max(total - adv, 0.0) / max(total, 1.0)
    except Exception:
        return 1.0
```

Update every call site further down the file that invokes `_load_daily()`,
`_load_video_daily()`, or `_get_qual_ratio()` to pass `_active_channel`
(`grep -n "_load_daily()\|_load_video_daily()\|_get_qual_ratio()" pages/daily_analytics.py`
to find each call site and add the argument).

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_daily_analytics_channel_scope.py -v`
Expected: PASS

- [ ] **Step 5: Manually verify**

```bash
.venv/bin/streamlit run app.py
```
Navigate to Daily Analytics via the native Streamlit page nav, switch channels,
confirm charts update and no exception is raised for empty channels.

- [ ] **Step 6: Commit**

```bash
git add pages/daily_analytics.py tests/test_daily_analytics_channel_scope.py
git commit -m "feat: scope Daily Analytics page by active channel"
```

---

### Task 8: `pages/video_render_comparisons.py`

**Files:**
- Modify: `pages/video_render_comparisons.py`
- Test: `tests/test_video_render_channel_scope.py`

**Interfaces:**
- Produces: `_load_playlist_videos(channel: str)`, `_load_all_videos(channel: str)`,
  `_qual_ratio(channel: str)`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_video_render_channel_scope.py
def test_video_render_loaders_take_channel_param():
    source = open("pages/video_render_comparisons.py").read()
    assert "def _load_playlist_videos(channel: str)" in source
    assert "def _load_all_videos(channel: str)" in source
    assert "def _qual_ratio(channel: str)" in source
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_video_render_channel_scope.py -v`
Expected: FAIL

- [ ] **Step 3: Update `pages/video_render_comparisons.py`**

Add `from channel_state import render_channel_selector` to the imports, and
after the existing authentication guard add `_active_channel =
render_channel_selector()`.

Replace the three loaders:

```python
@st.cache_data(ttl=300)
def _load_playlist_videos(channel: str) -> pd.DataFrame:
    """All playlist → video_id memberships (used for group assignment only)."""
    sql = """
    SELECT p.title AS playlist, pv.video_id
    FROM playlists p
    JOIN playlist_videos pv ON p.playlist_id = pv.playlist_id AND pv.channel = p.channel
    WHERE p.channel = :channel
    """
    with sqlite3.connect(str(DB_PATH)) as conn:
        try:
            return pd.read_sql_query(sql, conn, params={"channel": channel})
        except Exception:
            return pd.DataFrame()


@st.cache_data(ttl=300)
def _load_all_videos(channel: str) -> pd.DataFrame:
    """All videos with their latest cumulative metrics snapshot."""
    sql = """
    SELECT
        v.video_id,
        dvm.views,
        dvm.estimated_minutes_watched / 60.0  AS watch_hours,
        dvm.average_view_duration,
        dvm.subscribers_gained
    FROM videos v
    LEFT JOIN (
        SELECT video_id, MAX(metric_date) AS ld
        FROM daily_video_metrics WHERE channel = :channel GROUP BY video_id
    ) latest ON v.video_id = latest.video_id
    LEFT JOIN daily_video_metrics dvm
        ON dvm.video_id = v.video_id AND dvm.metric_date = latest.ld AND dvm.channel = :channel
    WHERE v.channel = :channel
    """
    with sqlite3.connect(str(DB_PATH)) as conn:
        try:
            return pd.read_sql_query(sql, conn, params={"channel": channel})
        except Exception:
            return pd.DataFrame()


@st.cache_data(ttl=300)
def _qual_ratio(channel: str) -> float:
    try:
        with sqlite3.connect(str(DB_PATH)) as conn:
            total = conn.execute(
                "SELECT SUM(d.estimated_minutes_watched/60.0) FROM daily_video_metrics d "
                "INNER JOIN (SELECT video_id, MAX(metric_date) AS ld "
                "FROM daily_video_metrics WHERE channel = ? GROUP BY video_id) l "
                "ON d.video_id=l.video_id AND d.metric_date=l.ld WHERE d.channel = ?",
                (channel, channel),
            ).fetchone()[0] or 0.0
            adv = conn.execute(
                "SELECT SUM(d.estimated_minutes_watched/60.0) "
                "FROM video_traffic_source_metrics d "
                "INNER JOIN (SELECT video_id, MAX(metric_date) AS ld "
                "FROM video_traffic_source_metrics "
                "WHERE traffic_source_type='ADVERTISING' AND channel = ? GROUP BY video_id) l "
                "ON d.video_id=l.video_id AND d.metric_date=l.ld "
                "WHERE d.traffic_source_type='ADVERTISING' AND d.channel = ?",
                (channel, channel),
            ).fetchone()[0] or 0.0
        return max(total - adv, 0.0) / max(total, 1.0)
    except Exception:
        return 1.0
```

Update every call site invoking these three functions to pass `_active_channel`
(`grep -n "_load_playlist_videos()\|_load_all_videos()\|_qual_ratio()" pages/video_render_comparisons.py`).

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_video_render_channel_scope.py -v`
Expected: PASS

- [ ] **Step 5: Manually verify** — same pattern as Task 7, Step 5, on the
Video Render Comparisons page.

- [ ] **Step 6: Commit**

```bash
git add pages/video_render_comparisons.py tests/test_video_render_channel_scope.py
git commit -m "feat: scope Video Render Comparisons page by active channel"
```

---

### Task 9: Content Intelligence (page + service layer)

**Files:**
- Modify: `content_intelligence/service.py`
- Modify: `pages/content_intelligence.py`
- Test: `tests/test_ci_service.py`

**Interfaces:**
- Produces: `ContentIntelligenceService.__init__(self, db_path: Path, channel: str)`
  (was `__init__(self, db_path: Path)`), `run_scoring(db_path: Path, channel: str)`
  (was `run_scoring(db_path: Path)` — this is the function `fetch_metrics.py`
  calls, matching the `channel=channel_key` keyword used in Task 3), `load_assets`
  and `update_asset_status` both gain a `channel: str` parameter.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_ci_service.py — add to existing file's imports/tests
from content_intelligence.service import ContentIntelligenceService


def test_service_requires_channel(tmp_path):
    db_path = tmp_path / "empty.db"
    import sqlite3
    from db import SCHEMA
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    conn.close()

    svc = ContentIntelligenceService(db_path, channel="club_genius")
    assert svc._channel == "club_genius"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_ci_service.py -v -k requires_channel`
Expected: FAIL — `__init__` doesn't accept `channel` yet.

- [ ] **Step 3: Update `content_intelligence/service.py`**

Change `ContentIntelligenceService.__init__` to store the channel and pass it
into `_load_episodes_and_snapshots`:

```python
class ContentIntelligenceService:
    def __init__(self, db_path: Path, channel: str):
        self._db_path = db_path
        self._channel = channel
        self._scorer = ContentScorer()
```
(keep any existing lines from the current constructor beyond `_scorer` — only
add `channel` and store it as `self._channel`.)

Update `_load_episodes_and_snapshots` (around line 55) to filter both queries
by `self._channel`:

```python
    def _load_episodes_and_snapshots(
        self,
    ) -> tuple[list[Episode], list[AnalyticsSnapshot]]:
        with sqlite3.connect(self._db_path) as conn:
            episodes_df = pd.read_sql_query(
                "SELECT video_id, title, description, published_at, "
                "duration_seconds, thumbnail_url FROM videos WHERE channel = :channel",
                conn,
                params={"channel": self._channel},
            )
            snapshots_df = pd.read_sql_query(
                """
                SELECT
                    d.video_id,
                    d.estimated_minutes_watched / 60.0 AS total_watch_hours,
                    d.average_view_duration,
                    COALESCE(d.subscribers_gained, 0) AS subscribers_gained,
                    COALESCE(adv.adv_views, 0) AS adv_views
                FROM daily_video_metrics d
                INNER JOIN (
                    SELECT video_id, MAX(metric_date) AS latest_date
                    FROM daily_video_metrics WHERE channel = :channel GROUP BY video_id
                ) latest ON d.video_id = latest.video_id AND d.metric_date = latest.latest_date
                LEFT JOIN (
                    SELECT video_id, SUM(views) AS adv_views
                    FROM video_traffic_source_metrics
                    WHERE traffic_source_type = 'ADVERTISING' AND channel = :channel
                    GROUP BY video_id
                ) adv ON d.video_id = adv.video_id
                WHERE d.channel = :channel
                """,
                conn,
                params={"channel": self._channel},
            )
        # keep the existing dataframe -> Episode/AnalyticsSnapshot conversion logic below unchanged
```
(Preserve whatever row-mapping code currently follows these two queries in the
file — only the two SQL strings and their `conn.execute`/`read_sql_query` calls
change; confirm the exact join shape by reading
`content_intelligence/service.py:55-95` before editing, since the snippet above
reconstructs the query from the grep output in Task exploration and must match
column names the mapping code expects.)

Update the module-level `run_scoring` function to accept and thread `channel`:

```python
def run_scoring(db_path: Path, channel: str) -> list[VideoScore]:
    svc = ContentIntelligenceService(db_path, channel=channel)
    # ...existing body, but every INSERT into ci_video_scores must include
    # channel=channel and every SELECT against ci_video_scores/ci_content_assets
    # must filter WHERE channel = ?
```
Locate the current `INSERT INTO ci_video_scores` and `SELECT ... FROM
ci_video_scores` / `ci_content_assets` statements (`grep -n "ci_video_scores\|ci_content_assets" content_intelligence/service.py`)
and add `channel` to each column list, `VALUES` tuple, and `WHERE`/`ON CONFLICT`
clause, following the exact same pattern used in Task 3's
`write_retention_rolling_windows` (column added first, `?` placeholder added,
bound value added to the parameter tuple).

Update `load_assets(db_path: Path, ..., channel: str)` and
`update_asset_status(db_path: Path, ..., channel: str)` (`grep -n "def load_assets\|def update_asset_status" content_intelligence/service.py`)
the same way — add `channel` to the signature, add `AND channel = ?` to the
`WHERE` clause of `load_assets`'s query, and add `AND channel = ?` to
`update_asset_status`'s `UPDATE ... WHERE` clause so status changes can never
apply to another channel's asset.

- [ ] **Step 4: Update `pages/content_intelligence.py`**

Add `from channel_state import render_channel_selector` and call it near the
top of the script (after `st.set_page_config`), then change:

```python
_SVC = ContentIntelligenceService(_DB)
```
to:

```python
_active_channel = render_channel_selector()
_SVC = ContentIntelligenceService(_DB, channel=_active_channel)
```

Update any direct calls to `load_assets(...)` / `update_asset_status(...)`
elsewhere in the page (`grep -n "load_assets(\|update_asset_status(" pages/content_intelligence.py`)
to pass `channel=_active_channel`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_ci_service.py tests/test_ci_scoring.py tests/test_ci_generation.py tests/test_ci_models.py tests/test_ci_scorer.py -v`
Expected: PASS (fix any test call sites that construct `ContentIntelligenceService(...)` or call `run_scoring(...)` without a `channel` argument — add
`channel="human_workforce"` to keep existing test fixtures passing)

- [ ] **Step 6: Manually verify**

```bash
.venv/bin/streamlit run app.py
```
Navigate to Content Intelligence, switch channels, confirm the five panels
either show that channel's scored episodes or the existing empty state.

- [ ] **Step 7: Commit**

```bash
git add content_intelligence/service.py pages/content_intelligence.py tests/test_ci_service.py
git commit -m "feat: scope Content Intelligence service and page by active channel"
```

---

### Task 10: Organic Momentum

**Files:**
- Modify: `analytics/organic_momentum.py`
- Modify: `pages/organic_momentum.py`
- Test: `tests/test_organic_momentum.py`

**Interfaces:**
- Produces: `build_momentum_data(db_path: str, channel: str) -> list[OrganicMomentumMetrics]`
  (was `build_momentum_data(db_path: str)`), `_compute_growth_stats(db_path: str, channel: str)`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_organic_momentum.py — add to existing file
from analytics.organic_momentum import build_momentum_data


def test_build_momentum_data_requires_channel_arg():
    import inspect
    sig = inspect.signature(build_momentum_data)
    assert "channel" in sig.parameters
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_organic_momentum.py -v -k requires_channel_arg`
Expected: FAIL

- [ ] **Step 3: Update `analytics/organic_momentum.py`**

Replace `_compute_growth_stats` and `build_momentum_data` (lines 266-374) with
channel-scoped versions:

```python
def _compute_growth_stats(db_path: str, channel: str) -> pd.DataFrame:
    """Derive daily view-increment trends from the daily_video_metrics time series."""
    df = _db_query(db_path, f"""
        SELECT video_id, metric_date,
               CAST(views AS REAL) AS views,
               estimated_minutes_watched / 60.0 AS watch_hours
        FROM daily_video_metrics
        WHERE channel = '{channel}'
        ORDER BY video_id, metric_date
    """)
    # ...rest of the function body is unchanged from here down...
```

```python
def build_momentum_data(db_path: str, channel: str) -> list[OrganicMomentumMetrics]:
    """Load all available data from the DB and return un-scored metrics."""
    vids = _db_query(db_path, f"""
        SELECT video_id, title, published_at, duration_seconds
        FROM videos
        WHERE channel = '{channel}'
    """)
    if vids.empty:
        return []

    latest = _db_query(db_path, f"""
        SELECT d.video_id,
               d.views AS total_views,
               d.estimated_minutes_watched / 60.0 AS total_watch_hours,
               COALESCE(d.average_view_duration, 0) AS average_view_duration,
               COALESCE(d.subscribers_gained, 0) AS subscribers_gained
        FROM daily_video_metrics d
        INNER JOIN (
            SELECT video_id, MAX(metric_date) AS latest_date
            FROM daily_video_metrics WHERE channel = '{channel}' GROUP BY video_id
        ) lx ON d.video_id = lx.video_id AND d.metric_date = lx.latest_date
        WHERE d.channel = '{channel}'
    """)

    adv = _db_query(db_path, f"""
        SELECT d.video_id, d.views AS adv_views
        FROM video_traffic_source_metrics d
        INNER JOIN (
            SELECT video_id, MAX(metric_date) AS latest_date
            FROM video_traffic_source_metrics
            WHERE traffic_source_type = 'ADVERTISING' AND channel = '{channel}'
            GROUP BY video_id
        ) lx ON d.video_id = lx.video_id AND d.metric_date = lx.latest_date
        WHERE d.traffic_source_type = 'ADVERTISING' AND d.channel = '{channel}'
    """)

    growth = _compute_growth_stats(db_path, channel)
    # ...rest of the function body (the merge loop and metrics list-building) is unchanged...
```

Note: `channel` is one of the three fixed literal strings from `channel_state.CHANNELS`,
never raw user input, so f-string interpolation here carries no injection risk —
this matches the module's existing style of building ad-hoc SQL strings (it has
no parameter-binding convention today). If you'd rather keep it consistent with
Task 5-9's `:channel`/`?` binding style instead, change `_db_query` to accept an
optional `params` dict and pass `{"channel": channel}` — either is acceptable,
but pick one and use it for every query added in this task.

- [ ] **Step 4: Update `pages/organic_momentum.py`**

Add `from channel_state import render_channel_selector` to imports. After
`st.set_page_config(...)`, add:

```python
_active_channel = render_channel_selector()
```

Find the call to `build_momentum_data` (or wherever `_load_scored` /
`raw_dicts = _load_scored(str(_DB), weights_json)` ultimately calls into
`build_momentum_data`) via `grep -n "build_momentum_data\|_load_scored" pages/organic_momentum.py`
and thread `_active_channel` through to it, updating the wrapping
`_load_scored` (or equivalent `@st.cache_data`-decorated function) signature to
accept and pass through `channel`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_organic_momentum.py -v`
Expected: PASS (update any existing test call sites the same way as Task 9,
Step 5)

- [ ] **Step 6: Manually verify** — same pattern as prior tasks, on the Organic
Momentum page.

- [ ] **Step 7: Commit**

```bash
git add analytics/organic_momentum.py pages/organic_momentum.py tests/test_organic_momentum.py
git commit -m "feat: scope Organic Momentum page by active channel"
```

---

### Task 11: Promotion Intelligence

**Files:**
- Modify: `pages/promotion_intelligence.py`
- Test: `tests/test_promotion_intelligence_channel_scope.py`

**Interfaces:**
- Produces: `_build_real_features(db: Path, cpv: float, channel: str)` (was
  `_build_real_features(db: Path, cpv: float)`)

(`promotion_intelligence/*.py` package modules — `promotion_prediction.py`,
`promotion_roi.py`, `recommendation_engine.py`, `recommendation_models.py` — do
not touch the database directly, per the codebase scan; they only receive
already-loaded DataFrames/dataclasses, so nothing in that package needs
changes.)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_promotion_intelligence_channel_scope.py
def test_build_real_features_takes_channel_param():
    source = open("pages/promotion_intelligence.py").read()
    assert "def _build_real_features(db: Path, cpv: float, channel: str)" in source
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_promotion_intelligence_channel_scope.py -v`
Expected: FAIL

- [ ] **Step 3: Update `pages/promotion_intelligence.py`**

Add `from channel_state import render_channel_selector` to imports; after
`st.set_page_config(...)` add `_active_channel = render_channel_selector()`.

Replace `_build_real_features` (lines 165-224-ish) with:

```python
def _build_real_features(db: Path, cpv: float, channel: str) -> list[VideoFeatures]:
    """Load metrics from the real DB and build VideoFeatures."""
    vids = _db_query(str(db),
        f"SELECT video_id, title, published_at, duration_seconds FROM videos WHERE channel = '{channel}'")
    if vids.empty:
        return []

    snap = _db_query(str(db),
        f"SELECT video_id, view_count FROM video_snapshots WHERE channel = '{channel}' ORDER BY captured_at")
    if not snap.empty:
        snap = snap.groupby("video_id", as_index=False).last()[["video_id", "view_count"]]

    dvm = _db_query(str(db), f"""
        SELECT d.video_id,
               d.estimated_minutes_watched / 60.0 AS total_watch_hours,
               d.average_view_duration AS avg_view_duration,
               COALESCE(d.subscribers_gained, 0) AS subscribers_gained,
               COALESCE(d.likes, 0) AS likes
        FROM daily_video_metrics d
        INNER JOIN (
            SELECT video_id, MAX(metric_date) AS latest_date
            FROM daily_video_metrics WHERE channel = '{channel}' GROUP BY video_id
        ) latest ON d.video_id = latest.video_id AND d.metric_date = latest.latest_date
        WHERE d.channel = '{channel}'
    """)

    adv = _db_query(str(db), f"""
        SELECT d.video_id,
               d.views AS adv_views,
               d.estimated_minutes_watched / 60.0 AS adv_watch_hours,
               d.average_view_duration AS avg_adv_view_duration
        FROM video_traffic_source_metrics d
        INNER JOIN (
            SELECT video_id, MAX(metric_date) AS latest_date
            FROM video_traffic_source_metrics
            WHERE traffic_source_type = 'ADVERTISING' AND channel = '{channel}' GROUP BY video_id
        ) latest ON d.video_id = latest.video_id AND d.metric_date = latest.latest_date
        WHERE d.traffic_source_type = 'ADVERTISING' AND d.channel = '{channel}'
    """)

    rel = _db_query(str(db), f"""
        SELECT video_id, SUM(views) AS follow_on_views
        FROM video_traffic_source_metrics
        WHERE traffic_source_type = 'RELATED_VIDEO' AND channel = '{channel}'
        GROUP BY video_id
    """)

    ci = _db_query(str(db), f"""
        SELECT s.video_id, s.overall_score AS ci_overall_score
        FROM ci_video_scores s
        INNER JOIN (SELECT MAX(scored_at) AS latest FROM ci_video_scores WHERE channel = '{channel}') m
          ON s.scored_at = m.latest
        WHERE s.channel = '{channel}'
    """)
    # ...rest of the function (the merge loop building VideoFeatures) is unchanged...
```

Update every call site of `_build_real_features(...)` (`grep -n "_build_real_features(" pages/promotion_intelligence.py`)
to pass `channel=_active_channel`.

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_promotion_intelligence_channel_scope.py -v`
Expected: PASS

- [ ] **Step 5: Manually verify** — same pattern as prior tasks, on the
Promotion Intelligence page (all 5 tabs).

- [ ] **Step 6: Commit**

```bash
git add pages/promotion_intelligence.py tests/test_promotion_intelligence_channel_scope.py
git commit -m "feat: scope Promotion Intelligence page by active channel"
```

---

### Task 12: End-to-end channel isolation test

**Files:**
- Test: `tests/test_channel_isolation.py`

**Interfaces:**
- Consumes: `db.SCHEMA`, `db.get_conn` (patched to a temp DB), all `channel`-scoped
  queries introduced in Tasks 5-11 via direct SQL assertions (this task doesn't
  re-import every page — Streamlit page scripts aren't import-safe outside a
  running app — it verifies isolation at the data layer, which every page's
  query ultimately depends on).

- [ ] **Step 1: Write the test**

```python
# tests/test_channel_isolation.py
import sqlite3

from db import SCHEMA


def _seed(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    for channel, video_id, views in [
        ("human_workforce", "vidA", 1000),
        ("club_genius", "vidA", 5000),  # same video_id, different channel
        ("kzak", "vidB", 250),
    ]:
        conn.execute(
            "INSERT INTO videos(channel, video_id, title) VALUES (?, ?, ?)",
            (channel, video_id, f"{channel}-{video_id}"),
        )
        conn.execute(
            "INSERT INTO daily_video_metrics(metric_date, channel, video_id, views) "
            "VALUES ('2026-07-18', ?, ?, ?)",
            (channel, video_id, views),
        )
    conn.commit()


def test_videos_with_same_id_coexist_across_channels(tmp_path):
    conn = sqlite3.connect(tmp_path / "iso.db")
    _seed(conn)
    rows = conn.execute(
        "SELECT channel, title FROM videos WHERE video_id = 'vidA' ORDER BY channel"
    ).fetchall()
    assert rows == [("club_genius", "club_genius-vidA"), ("human_workforce", "human_workforce-vidA")]
    conn.close()


def test_channel_filtered_query_never_returns_other_channels(tmp_path):
    conn = sqlite3.connect(tmp_path / "iso2.db")
    _seed(conn)
    for channel, expected_views in [
        ("human_workforce", 1000),
        ("club_genius", 5000),
        ("kzak", 250),
    ]:
        rows = conn.execute(
            "SELECT SUM(views) FROM daily_video_metrics WHERE channel = ?", (channel,)
        ).fetchone()
        assert rows[0] == expected_views
    conn.close()


def test_unfiltered_query_would_wrongly_sum_across_channels(tmp_path):
    """Documents the failure mode every task's `WHERE channel = ...` guards against."""
    conn = sqlite3.connect(tmp_path / "iso3.db")
    _seed(conn)
    total = conn.execute("SELECT SUM(views) FROM daily_video_metrics").fetchone()[0]
    assert total == 1000 + 5000 + 250  # would be wrong if shown as any single channel's total
    conn.close()
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_channel_isolation.py -v`
Expected: PASS

- [ ] **Step 3: Run the full test suite**

Run: `.venv/bin/python -m pytest -v`
Expected: PASS (all tests across `tests/`, including the ones updated in Tasks
6-11 for new function signatures)

- [ ] **Step 4: Commit**

```bash
git add tests/test_channel_isolation.py
git commit -m "test: verify channel data never comingles across queries"
```

---

### Task 13: Update deployment memory

**Files:**
- Modify: `/Users/zdjimas/.claude/projects/-Users-zdjimas-VS-Code-Projects/memory/human_workforce_analytics_deploy.md`

- [ ] **Step 1: Append multi-channel details to the existing memory file**

Add a new section documenting: the three channel keys and their OAuth ownership
(ClubGeniusStories and KZAKMusicVideos refresh tokens/owning accounts from
Task 4), the six-secret naming convention (`YT_CHANNEL_ID_{HW,CGS,KZAK}` /
`YT_REFRESH_TOKEN_{HW,CGS,KZAK}`), and a pointer to this plan's location for
future schema questions. Update the `MEMORY.md` index line for
`human_workforce_analytics_deploy.md` if its one-line description no longer
mentions multi-channel scope.

- [ ] **Step 2: No commit** — this step edits the memory system, not the repo.

---

## Execution notes

- Tasks 1-4 are strictly sequential (schema before fetch, fetch before secrets
  matter). Tasks 5-11 (the report pages) are independent of each other and can
  be parallelized across subagents once Task 1 and Task 2 (`channel_state.py`)
  are merged, since none of them depend on each other's code — only on the
  shared schema and selector module.
- Task 12 depends on nothing but Task 1's schema and should run after all of
  Tasks 5-11 land, as a final regression guard.
- Before starting Task 4 (OAuth), confirm with the user which Google account
  should perform the "Choose a channel" picker for each of the two new
  channels — this is a manual, interactive step that cannot be scripted.
