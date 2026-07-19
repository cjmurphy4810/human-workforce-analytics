# Multi-Channel Analytics — Design

## Purpose

Expand the human-workforce-analytics dashboard to report on two additional YouTube
channels — ClubGeniusStories and KZAKMusicVideos — alongside the existing Human
Workforce channel. The dashboard should default to Human Workforce on load, and let
the user switch to either of the other two channels. Every report page shows the
same kinds of metrics per channel, but no data is ever combined or aggregated across
channels — each channel's numbers stay fully isolated.

## Channels

| Key                | Display name        | Source                                          |
|---------------------|----------------------|--------------------------------------------------|
| `human_workforce`   | The Human Workforce  | existing channel (`UCHDU3z8f5_HJzJL1w2J2EaQ`)     |
| `club_genius`       | Club Genius Stories  | https://www.youtube.com/@ClubGeniusStories       |
| `kzak`              | KZAK Music Videos    | https://www.youtube.com/@KZAKMusicVideos         |

Each is a distinct brand account the user owns/administers, each requiring its own
OAuth refresh token (same pattern already used for Human Workforce — see
`human_workforce_analytics_deploy.md` memory for the re-auth procedure).

## Data model

Add a `channel TEXT NOT NULL` column to every per-channel table in `db.py`'s
`SCHEMA`, and include it in that table's uniqueness constraint so the same video ID
or metric_date can exist once per channel without colliding:

- `channel_snapshots` (already has `channel_id`, the real YouTube ID — add `channel`
  as the short key alongside it)
- `videos`, `video_snapshots`
- `daily_video_metrics`, `daily_channel_metrics`, `daily_geo_metrics`
- `playlists`, `playlist_videos`
- `channel_traffic_sources`, `video_traffic_source_metrics`
- `retention_buckets`
- `ci_video_scores`, `ci_content_assets`
- `publishing_queue`, `queue_recommendations`

Existing rows (Human Workforce data already in `data.db`) get backfilled with
`channel = 'human_workforce'` in a migration step, so historical data isn't lost.

One `data.db` file, shared schema, channel column as the isolation boundary. No
query in the app is ever allowed to omit a `WHERE channel = ?` filter on these
tables.

## Fetch pipeline

`fetch_metrics.py` already accepts a `channel_id` parameter that flows through every
`youtube_client.py` fetch function (`fetch_channel_stats`, `fetch_daily_channel_metrics`,
etc. all take `channel_id: str | None`). Restructure it to loop over a list of channel
configs:

```python
CHANNELS = [
    {"key": "human_workforce", "channel_id_env": "YT_CHANNEL_ID_HW", "refresh_token_env": "YT_REFRESH_TOKEN_HW"},
    {"key": "club_genius", "channel_id_env": "YT_CHANNEL_ID_CGS", "refresh_token_env": "YT_REFRESH_TOKEN_CGS"},
    {"key": "kzak", "channel_id_env": "YT_CHANNEL_ID_KZAK", "refresh_token_env": "YT_REFRESH_TOKEN_KZAK"},
]
```

For each channel: swap in its refresh token, resolve its channel ID, run the
existing fetch/write logic, tagging every inserted row with `channel=<key>`. One
channel's transient failure (e.g. a 500 from YouTube Analytics) must not abort the
other two — wrap each channel's run in its own try/except, matching the existing
per-video error handling in `write_retention_rolling_windows`.

`YT_CLIENT_ID`/`YT_CLIENT_SECRET` (the OAuth app credentials) are shared across all
three, since it's one OAuth client authorizing three separate brand accounts. Only
the refresh token and channel ID differ per channel.

### GitHub Actions secrets

New secrets needed (in addition to existing `YT_CLIENT_ID`/`YT_CLIENT_SECRET`/
`ANTHROPIC_API_KEY`/`NEWS_API_KEY`):

- `YT_CHANNEL_ID_HW`, `YT_REFRESH_TOKEN_HW` (rename/reuse existing `YT_CHANNEL_ID`/
  `YT_REFRESH_TOKEN`)
- `YT_CHANNEL_ID_CGS`, `YT_REFRESH_TOKEN_CGS`
- `YT_CHANNEL_ID_KZAK`, `YT_REFRESH_TOKEN_KZAK`

`.github/workflows/fetch-analytics.yml` passes all six through as env vars; the
matrix/loop lives inside `fetch_metrics.py`, not in the workflow file, so the DB
commit-back step still runs once per workflow run (avoids racing 3 concurrent
commits to `data.db`).

## Dashboard UI

**Channel selector**: added to the sidebar in `app.py`, above the existing page nav
radio. A `st.radio` or `st.selectbox` listing the three display names, defaulting to
"The Human Workforce". Selection is stored in `st.session_state["active_channel"]`
(the short key), which persists across Streamlit's multipage navigation so switching
pages doesn't reset the channel.

**Query filtering**: every `load()` call in `app.py` and every direct
`sqlite3.connect` query in the six `pages/*.py` files (`content_intelligence.py`,
`daily_analytics.py`, `promotion_intelligence.py`, `organic_momentum.py`,
`qualifying_watch_hours.py`, `video_render_comparisons.py`) adds
`WHERE channel = :channel` (or `AND channel = :channel`), parameterized with
`st.session_state["active_channel"]`. `db.py`'s connection helper stays
channel-agnostic — filtering happens at the query call site, consistent with the
existing pattern where each page already builds its own SQL.

**Scope**: all seven report views (Overview + the six `pages/*.py` reports) become
channel-aware, since they all read from tables that now carry the `channel` column.

**Caching**: `app.py`'s `@st.cache_data(ttl=300)` on `load(query: str)` caches by
query string — since the channel filter is baked into the query/params, switching
channels naturally produces a cache miss and fresh data, no cache-key changes
needed.

## Error handling

- Migration: if `channel` column addition runs against a `data.db` that already has
  rows, backfill existing rows to `human_workforce` before enforcing new uniqueness
  constraints, so the migration is safe to run against the live production DB.
- Fetch: per-channel try/except in the loop so one channel's API hiccup doesn't
  block the other two channels' data for that run.
- Dashboard: if a selected channel has no rows yet (e.g. first run before the fetch
  job populates it), pages should render their existing "empty state" (most already
  handle empty DataFrames via `if not DB_PATH.exists(): return pd.DataFrame()` /
  similar checks) rather than erroring.

## Testing

- `tests/` already covers `fetch_metrics.py` and `youtube_client.py` behavior;
  extend those tests to assert channel-tagged inserts and that a channel filter
  excludes other channels' rows.
- Manual verification: run the dashboard locally with seeded multi-channel test data,
  confirm switching the channel selector changes every page's numbers and that no
  page ever shows a blended total across channels.
