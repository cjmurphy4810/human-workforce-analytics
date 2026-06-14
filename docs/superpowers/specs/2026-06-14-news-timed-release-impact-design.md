# News-Timed Release Impact — Design Spec
**Date:** 2026-06-14
**Project:** human-workforce-analytics dashboard

## Goal

Add a "News-Timed Release Impact" section to the dashboard that tracks podcast episodes which graduated from the Publishing Queue and were published within a ±72–96h window of their recommended publish date. The section compares how timing precision correlates with early performance (views, hours watched, subscriber gain, audience retention), showing directly whether hitting the recommended news window makes a measurable difference.

## Background

The Publishing Queue feature (spec: `2026-05-13-publishing-queue-design.md`) ranks unpublished episodes by relevance to current news and recommends a publish date (`today + rank`). This feature captures what happens *after* a recommended episode goes live — building a cohort of "on-time" episodes and tracking their performance against each other.

## Architecture

Three files change. No new API calls, no new secrets, no changes to `ai_client.py`, `youtube_client.py`, `requirements.txt`, or the GitHub workflow.

### New table: `queue_recommendations`

Persists the first time each video appears in the publishing queue. `INSERT OR IGNORE` on `video_id` ensures the initial recommendation is immutable — subsequent cron runs where the video re-appears at a different rank do not overwrite it.

```sql
CREATE TABLE IF NOT EXISTS queue_recommendations (
    video_id TEXT PRIMARY KEY,
    first_recommended_at TEXT NOT NULL,
    recommended_publish_date TEXT NOT NULL,
    rank_at_recommendation INTEGER NOT NULL,
    relevance_score REAL NOT NULL,
    theme TEXT,
    why_now TEXT
);
```

- `first_recommended_at` — ISO 8601 timestamp of the cron run that first placed this video in the queue.
- `recommended_publish_date` — DATE string (`YYYY-MM-DD`) computed as `(date of cron run) + rank` days. This is the date the system recommended for publishing.
- `rank_at_recommendation`, `relevance_score`, `theme`, `why_now` — snapshot of the queue result at first recommendation.

### Eligibility Criteria (computed at render time)

A video enters the "News-Timed Release Impact" cohort when ALL of the following are true:

1. Has a `queue_recommendations` record (was in the queue at some point).
2. Is now public — `videos.published_at` is not null and the video appears in the `videos` table (populated by `fetch_metrics.py` from the uploads playlist).
3. `published_at` falls within `[recommended_publish_date − 72h, recommended_publish_date + 96h]`.
4. Has ≥ 3 rows in `daily_video_metrics` since `published_at` (at least 3 days of API data).

### Timing Delta

```
timing_delta_hours = (published_at − recommended_publish_date) in hours
```

- Negative → published before the recommended date (early, up to 72h before qualifies).
- Positive → published after the recommended date (late, up to 96h after qualifies).
- Displayed as `−52h` or `+18h` on the scorecard.
- Color coding: `|delta| ≤ 24h` → green; `24h < |delta| ≤ 60h` → yellow; `60h < |delta| ≤ 96h` → gray.

### Performance Normalization

Each eligible video's `daily_video_metrics` rows are aligned to a **days-since-publish** offset (Day 1, Day 2, Day 3 …) computed as `metric_date − date(published_at)`. This makes trajectory charts comparable regardless of when episodes were published.

Cumulative metrics per day offset:
- `cumulative_views` — sum of `views` from Day 1 through Day N.
- `cumulative_hours` — sum of `estimated_minutes_watched / 60` from Day 1 through Day N.
- `cumulative_subs` — sum of `subscribers_gained` from `daily_video_metrics` from Day 1 through Day N (column already present in schema).

Retention depth (from `retention_buckets`, latest rolling window per video):
- `retention_at_25` and `retention_at_75` — percentage of viewers who watched past each quartile mark.

## Data Flow (per cron run)

1. `fetch_metrics.py` calls `write_publishing_queue()` as today (no change to existing logic).
2. Immediately after, a new `write_queue_recommendations()` function reads the `ranked_videos` from the just-written `result_json` and runs:
   ```python
   INSERT OR IGNORE INTO queue_recommendations
       (video_id, first_recommended_at, recommended_publish_date,
        rank_at_recommendation, relevance_score, theme, why_now)
   VALUES (?, ?, ?, ?, ?, ?, ?)
   ```
   `recommended_publish_date = (date of this cron run) + timedelta(days=rank)`.
3. Dashboard `app.py` reads the cohort at page load via a SQL query that joins `queue_recommendations`, `videos`, and counts `daily_video_metrics` rows. Eligibility filter and timing delta are applied in Python after the query.

## Dashboard Section

**Location:** After the existing "Publishing Queue" section at the bottom of `app.py`.

**Section header:** "News-Timed Release Impact"

**Caption:** "Episodes that graduated from the Publishing Queue and went live within the ±72–96h timing window. Shows whether publishing on time with the news cycle drives stronger early performance."

### Layout (top to bottom)

**1. Scorecard table**

One row per eligible podcast. Columns:

| Title | Timing | Days Live | Views | Hours Watched | Subs Gained | Kept 25% | Kept 75% |
|---|---|---|---|---|---|---|---|
| Episode X | +18h | 7 | 1,240 | 42.3 | 31 | 74% | 48% |
| Episode Y | −44h | 12 | 880 | 29.1 | 18 | 68% | 39% |

- Timing cell is colored green / yellow / gray per delta thresholds above.
- Table sorted by timing delta ascending (closest to recommended date first) by default.
- `st.dataframe` with `hide_index=True`.

**2. Trajectory charts (two columns)**

- Left: Cumulative views by days-since-publish. One `go.Scatter` line per eligible podcast, labeled by truncated title. X-axis: "Days Since Publication". Y-axis: "Cumulative Views".
- Right: Cumulative hours watched by days-since-publish. Same structure.
- Both charts share the same line colors per podcast for visual consistency (assign colors from `px.colors.qualitative.Plotly` keyed by `video_id`).
- `height=380`, `showlegend=True`.

**3. Retention comparison (bar chart)**

Grouped horizontal bar chart: one group per podcast, two bars each — `retention_at_25` (blue) and `retention_at_75` (green). Sorted by timing delta ascending (same order as scorecard). Y-axis: episode titles (truncated to 40 chars). X-axis: percentage 0–100%.

### Empty States

- No eligible videos yet: `st.info("No episodes have graduated from the queue within the timing window yet. This section populates once a recommended episode goes live and accumulates 3 days of data.")`
- Eligible videos exist but no retention data: render scorecard and trajectory charts; skip retention chart and show `st.caption("Retention data not yet available for these episodes.")`.

## Modified Files

| File | Change |
|---|---|
| `db.py` | Add `queue_recommendations` table to `SCHEMA` string |
| `fetch_metrics.py` | Add `write_queue_recommendations(conn, result_json, cron_date)` function; call it after `write_publishing_queue()` in `main()`, wrapped in try/except |
| `app.py` | Load `queue_recommendations` at top; add "News-Timed Release Impact" section after "Publishing Queue"; compute eligibility, timing delta, normalized trajectories, and render scorecard + two trajectory charts + retention bar chart |

## Error Handling

- `write_queue_recommendations()` is wrapped in try/except in `main()`, matching the existing pattern for `write_publishing_queue()`. Failure silently logs and does not crash the pipeline.
- If `result_json` has `"news_available": false` (no ranked videos), `write_queue_recommendations()` is a no-op.
- If `daily_video_metrics` has fewer than 3 rows for a video since `published_at`, the video is excluded from the cohort at render time.
- If `retention_buckets` has no rows for an eligible video, retention metrics show as `None` in the scorecard and the retention chart skips that video.
- All eligibility filtering in `app.py` is pure Python/pandas — no extra DB queries in a loop.

## Testing

- `test_db.py`: assert `queue_recommendations` table exists after `init_db()`.
- `test_fetch_metrics.py`: add `test_write_queue_recommendations_inserts_on_first_run` — mock DB, call function twice with same video_id, assert only one row exists (INSERT OR IGNORE).
- `test_fetch_metrics.py`: add `test_write_queue_recommendations_noop_when_no_ranked_videos` — pass `result_json` with empty `ranked_videos`, assert no rows inserted.
- No tests for `app.py` rendering (visual verification).
