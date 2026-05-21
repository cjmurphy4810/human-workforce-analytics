# Geographic Trends Feature — Design Spec
Date: 2026-05-21

## Overview

Add a new **Geographic Trends** section to the Human Workforce Analytics dashboard that shows daily growth of views, subscribers gained, and likes broken down by the top-5 countries, using data from the YouTube Analytics API.

This answers the core channel health question: are Shorts-driven Indian viewers converting into long-form podcast subscribers, and is the US audience share growing over time?

---

## Scope

- Metrics tracked by country: **views, subscribers_gained, likes** (comments excluded — not supported reliably by the YouTube Analytics API with the `country` dimension)
- Countries shown: **top 5 by total views** in the selected time range (dynamically computed, not hard-coded)
- Date range: user-selectable via existing `range_picker()` helper (Last week / Last month / Last quarter / Last year)
- Placement: new section between **Growth Velocity** and **Top Videos**

---

## Data Layer

### New DB table (`db.py`)

```sql
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
```

Added to the existing `SCHEMA` string. Table is append-only with upsert; `init_db()` handles creation automatically on next run.

### New fetch function (`youtube_client.py`)

```python
def fetch_daily_geo_metrics(start: date, end: date, channel_id: str | None = None) -> list[dict]:
```

- Calls `analytics_service().reports().query()` with `dimensions="day,country"` and `metrics="views,subscribersGained,likes"`
- Returns list of `{metric_date, country_code, views, subscribers_gained, likes}` dicts
- Same signature/shape as existing `fetch_daily_channel_metrics`

### Fetch pipeline update (`fetch_metrics.py`)

- Import `fetch_daily_geo_metrics` from `youtube_client`
- Call it with the same `start`/`end` window (90-day lookback) and same `try/except` guard as `fetch_daily_channel_metrics`
- Write rows with `INSERT ... ON CONFLICT(metric_date, country_code) DO UPDATE SET ...` — idempotent upsert, safe to re-run

---

## Dashboard Layer (`app.py`)

### Data load

```python
daily_geo = load(
    "SELECT metric_date, country_code, views, subscribers_gained, likes "
    "FROM daily_geo_metrics ORDER BY metric_date"
)
```

Added alongside the other `load()` calls at module level (cached via `@st.cache_data`).

### New section

Inserted between Growth Velocity and Top Videos:

```
st.subheader("Geographic Trends")
st.caption("Top 5 countries by views in the selected range. ...")
range_picker("geo_range")
```

**Top-5 selection:** After applying `filter_days()` to the selected range, group by `country_code` and sum views. Take the 5 country codes with the highest total. All three charts are restricted to only those 5 countries.

**Country name mapping:** A small inline dict maps ISO 3166-1 alpha-2 codes to readable names (`"IN" → "India"`, `"US" → "United States"`, `"RO" → "Romania"`, etc.). Unknown codes fall back to the 2-letter code itself — no external dependency needed.

**Chart:** `make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.07)` — same pattern as Growth Velocity:

| Row | Metric | Series |
|-----|--------|--------|
| 1 | Views per day | One `go.Scatter` line per top-5 country |
| 2 | Subscribers gained per day | One line per country |
| 3 | Likes per day | One line per country |

- `hovermode="x unified"` — hovering one date shows all 5 countries simultaneously
- `height=720`, legend visible (country names must be identifiable)
- Data is grouped by `(metric_date, country_name)` and pivoted before charting

**Empty state:** If `daily_geo.empty`, show `st.info("Geographic data still loading. Run fetch_metrics.py to populate.")` — same guard pattern used throughout the dashboard.

---

## Constraints & Notes

- **UTC date gotcha:** Uses `filter_days()` helper (range-based cutoff, not `date.today()` equality) — consistent with all other sections.
- **No new dependencies:** All chart/data tooling (`plotly`, `pandas`, `sqlite3`) is already in `requirements.txt`.
- **API quota:** One additional Analytics API query per fetch run (all countries + all days in window in a single request). Negligible quota impact.
- **Backfill:** The first fetch after deploy will write 90 days of geo history. Subsequent runs upsert updates only.
- **Comments excluded by design:** The Analytics API `country` dimension does not reliably support `comments` as a metric. Attempting to include it risks intermittent 400 errors in the GitHub Actions workflow.
