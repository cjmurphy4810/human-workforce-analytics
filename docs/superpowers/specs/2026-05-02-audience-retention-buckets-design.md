# Audience Retention Bucket Charts — Design

**Date:** 2026-05-02
**Status:** Approved for implementation planning
**Scope:** Add audience-retention bucket visualizations to the Human Workforce Analytics dashboard, both channel-wide and per-video.

## Problem

The dashboard currently surfaces views, watch hours, and average view duration but says nothing about *how viewers are distributed across the video*. A 50% average watch time could mean "everyone watches half" or "half watch all of it and half bounce immediately" — those are very different audience signals and drive very different content decisions.

We want a chart family that splits viewers into three retention buckets per video — **0–25%**, **25–75%**, **75–100%** — and aggregates those splits across all videos for the channel-wide view, and shows the split for one video in the deep-dive section.

## Definitions

- **Bucket:** a fraction-of-video range. Three buckets: 0–25% (`b1`), 25–75% (`b2`), 75–100% (`b3`).
- **Bucket share for a video:** the fraction of that video's views whose watch-time falls in the bucket. Computed from the YouTube Analytics audience retention curve:
  - `b1_share(v) = 1 - audienceWatchRatio(v, 0.25)`
  - `b2_share(v) = audienceWatchRatio(v, 0.25) - audienceWatchRatio(v, 0.75)`
  - `b3_share(v) = audienceWatchRatio(v, 0.75)`
- **Snapshot:** the bucket split aggregated across all videos *and* all views in the selected timeframe — a single set of three numbers.
- **Trend:** the bucket split computed for each time-bin within the selected timeframe — many sets of three numbers, one per bin.
- **Timeframe toggle:** the existing `range_picker` widget — *Last week / Last month / Last quarter / Last year*.

## Architecture

### Data fetching

Add one new function to `youtube_client.py`:

```
fetch_retention_curve(video_id, start, end) -> {
    "video_id": str,
    "window_start": date,
    "window_end": date,
    "retention_at_25": float,   # audienceWatchRatio at elapsedVideoTimeRatio=0.25
    "retention_at_75": float,   # at 0.75
}
```

Implementation: query the YouTube Analytics `audienceRetention` report with `dimensions=elapsedVideoTimeRatio`, `filters=video=={id};audienceType==ORGANIC`, `metrics=audienceWatchRatio`. The response contains 101 rows (0.00 to 1.00 in 0.01 steps); we read the values at 0.25 and 0.75 (linearly interpolating if YouTube's bin doesn't land exactly on those marks).

The full curve is collapsed to two scalars at fetch time — we only ever need those two. This keeps storage tiny and the dashboard logic simple.

### Storage

New table in `db.py`:

```sql
CREATE TABLE retention_buckets (
    video_id TEXT NOT NULL,
    window_start DATE NOT NULL,
    window_end DATE NOT NULL,
    views INTEGER NOT NULL,        -- views attributed to this (video, window)
    retention_at_25 REAL NOT NULL,
    retention_at_75 REAL NOT NULL,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (video_id, window_start, window_end)
);
CREATE INDEX retention_buckets_window
    ON retention_buckets(window_start, window_end);
```

`views` is sourced from the same date range via the existing `daily_video_metrics` data — we don't fetch it separately. We store it on the row to keep snapshot aggregation a single SQL query without joining.

### Window strategy

Two kinds of rows are written:

**Snapshot windows (for KPI cards):** three rolling windows are recomputed each fetch run:
- `now-7d → now`
- `now-90d → now`  (covers both Last month and Last quarter toggles by sub-filtering)
- `now-365d → now`

Cost: `3 × N_videos` API calls per fetch run.

**Trend windows (for the stacked bar chart):** one row per `(video, ISO week)` for the past 52 weeks. Filled once via a backfill script, then incrementally extended one week at a time on each scheduled fetch.

Cost: `~52 × N_videos` one-time backfill (~2,600 calls at current channel size); thereafter `N_videos` per week.

The snapshot rows and trend rows live in the same table — they're distinguished by `window_start` / `window_end`. The `views` field on each row is the views during that specific window.

### Aggregation queries

Two read paths from the dashboard:

**Channel-wide snapshot for the selected timeframe:**

Toggle → window mapping:
- *Last week* → use the 7-day rolling row directly (views + ratios both span 7 days).
- *Last month* → use the 90-day row's `r25`/`r75`, but pull view counts from `daily_video_metrics` filtered to the last 30 days. Bucket counts = `30d_views × shares_from_90d_ratios`.
- *Last quarter* → use the 90-day rolling row directly.
- *Last year* → use the 365-day rolling row directly.

Steps:
1. Resolve toggle to (window row, views source) per the table above.
2. `SELECT video_id, views, retention_at_25, retention_at_75 FROM retention_buckets WHERE window_start = ? AND window_end = ?`.
3. For *Last month* only, override `views` per row with the matching `daily_video_metrics` 30-day sum.
4. Compute per-video bucket counts: `b1_count = views × (1 - r25)`, `b2_count = views × (r25 - r75)`, `b3_count = views × r75`.
5. Sum across videos for total bucket counts; divide by total views for percentages.

**Channel-wide trend:**
1. Determine bin granularity from the selected timeframe (1W → daily, 1M → daily, 1Q → weekly, 1Y → monthly).
2. Pull weekly rows from `retention_buckets` covering the timeframe.
3. For daily granularity: we don't have daily rows. Fall back to the most recent weekly row's ratios, scaled by the daily view count from `daily_video_metrics` — see *Tradeoffs*.
4. For weekly/monthly granularity: aggregate weekly rows directly.
5. For each bin: sum bucket counts across videos.

**Per-video snapshot and trend (deep dive section):**
- Same queries, scoped with `WHERE video_id = ?`.

## Tradeoffs and Decisions

**Why we collapse the retention curve to two scalars at fetch time:**
The full 101-point curve is overkill — we only ever read it at 0.25 and 0.75. Storing those two values keeps the table small (kilobytes, not megabytes), the schema simple, and the dashboard queries fast. If a future requirement asks for finer buckets (e.g., deciles), we re-fetch — the API call is the same, just store more columns.

**Why per-video relative buckets, not absolute minutes:**
Decided in brainstorming. Absolute minutes (e.g., "0–2.5 min" buckets) make videos of different lengths incomparable — a 4-minute video can't have any "75-100%" views under absolute thresholds, but it can have viewers who watched the whole thing. Relative bucketing answers "are people sticking around?" consistently regardless of length.

**Why three rolling snapshot windows instead of one window per toggle:**
The toggles are 7 / 30 / 90 / 365 days. Fetching all four per video doubles cost vs. just three. The 90-day window gives us "Last month" by sub-filtering the 90-day data — we lose a small amount of accuracy (the retention curve is averaged across the full 90 days, not the most recent 30), but it's a worthwhile trade for half the API quota.

**Why the daily trend bin uses scaled weekly retention:**
True daily retention curves would mean `~365 × N_videos` API calls per backfill — an order of magnitude more than the weekly approach. Instead, for daily bins we hold the retention ratios constant within a week (using the weekly row's `r25` / `r75`) and let the daily view count drive the bin height. This means the *shape* of the daily trend reflects view-volume, while the *bucket-split proportion* updates weekly. Acceptable trade — daily retention shifts within a week are typically small.

**Why we cap `audienceWatchRatio` at 1.0:**
The metric can exceed 1.0 due to rewatches. Treating those literally would make `b3_share` exceed 100% of views, breaking the "viewer distribution" mental model. Capping at 1.0 means rewatches are ignored — they show up as a saturation, not a bucket spike. Documented in code so the cap isn't surprising.

**Phase split (recommended for the implementation plan):**
- **Phase 1:** snapshot KPI cards only — uses the three rolling-window rows. Ships with no backfill required, just the next fetch run.
- **Phase 2:** trend stacked-bar charts — depends on the weekly backfill completing.

This lets us ship the most-asked-for piece (the bucket split numbers) within one fetch cycle, and the trend visualization can follow once the backfill data is in.

## UI Changes

All in `app.py`. No new files.

### New section: "Audience Retention" (channel-wide)

Inserted between the existing "Watch Time" section and "Per-Video Deep Dive".

```
Audience Retention
[ Last week | Last month | Last quarter | Last year ]   ← range_picker

┌─────────────────────────┬──────────────────────────┬─────────────────────────┐
│ Dropped early (0–25%)   │ Mid-watch (25–75%)       │ Stuck around (75–100%)  │
│ 12,400 views            │ 18,200 views             │ 4,600 views             │
│ 35.3%                   │ 51.7%                    │ 13.1%                   │
└─────────────────────────┴──────────────────────────┴─────────────────────────┘

Stacked bar chart: x = time bin, y = view count, segments colored by bucket
(daily for 1W/1M, weekly for 1Q, monthly for 1Y)
```

KPI cards rendered with `st.columns(3)` and `st.metric`. Trend chart rendered with Plotly stacked bar (`go.Figure` with three `add_bar` calls + `barmode="stack"`). Bucket colors: `#E45756` (dropped early, red-ish), `#F2B701` (mid, amber), `#54A24B` (stuck around, green) — matches the existing palette in the velocity chart.

### Deep-dive section additions

Append to the existing per-video block, after the two existing charts. Same widgets (KPI cards + stacked trend bar), filtered to `video_id == selected`. Reuses the deep-dive's existing range picker — no new toggle.

### Empty / loading states

- **No retention data yet:** section shows `st.info("Retention data still loading. First backfill takes ~10 min on initial run.")`
- **Selected timeframe has no view data:** section hides, identical to how empty `daily_videos` already behaves.
- **A video has no retention data (too few views per YouTube's privacy threshold):** silently skipped from aggregation. Does not appear in the per-video deep dive (its retention block hides with an info note).

## Build Sequence

**Phase 1 (snapshot only — ships in one fetch cycle):**
1. **Schema:** add `retention_buckets` table + migration to `db.py`. Migration must be idempotent (check `sqlite_master` before `CREATE TABLE`).
2. **Fetcher:** add `fetch_retention_curve(video_id, start, end)` to `youtube_client.py`. Unit-testable with a recorded API response.
3. **Fetch integration:** extend `fetch_metrics.py` to write the three rolling-window rows (7/90/365 day) for every video on each run. View counts pulled from existing `daily_video_metrics`.
4. **Snapshot UI:** KPI cards in both the new channel-wide section and the deep-dive section. Reads only from rolling-window rows.

**Phase 2 (trend — depends on backfill):**

5. **Backfill script:** `scripts/backfill_retention.py` — fills weekly rows for the past 52 weeks per video. Resumable (checks existing rows, skips them). Logs progress. Will take ~30 min to complete at current channel size.
6. **Weekly trend write:** extend `fetch_metrics.py` once more to append the latest week's row on each run.
7. **Trend UI:** stacked-bar trend chart in both sections. Reads from weekly rows + daily view counts.

Each step lands as its own PR.

## Open questions

None — design approved during brainstorming. Phase 1 / Phase 2 split confirmed; ~2,600-call backfill confirmed acceptable.
