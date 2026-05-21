# Geographic Trends Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a "Geographic Trends" dashboard section showing daily views, subscribers gained, and likes for the top-5 countries, backed by a new `daily_geo_metrics` SQLite table populated via the YouTube Analytics API.

**Architecture:** New `daily_geo_metrics` table (one row per date+country) is added to `db.py`. A new `fetch_daily_geo_metrics()` in `youtube_client.py` queries the Analytics API with `dimensions="day,country"`. A new `write_geo_metrics()` helper in `fetch_metrics.py` upserts rows every 4-hour fetch cycle. A new "Geographic Trends" section in `app.py` renders a 3-panel line chart using the existing `make_subplots`, `filter_days`, and `range_picker` patterns.

**Tech Stack:** Python 3.12, SQLite (`sqlite3`), Google YouTube Analytics API v2 (`googleapiclient`), Streamlit, Plotly

---

### Task 1: Add `daily_geo_metrics` table to DB schema

**Files:**
- Modify: `db.py`
- Test: `tests/test_db.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_db.py`:

```python
def test_daily_geo_metrics_table_created():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            with sqlite3.connect(db_path) as conn:
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='daily_geo_metrics'"
                )
                assert cursor.fetchone() is not None


def test_daily_geo_metrics_primary_key_constraint():
    """Duplicate (metric_date, country_code) must raise IntegrityError."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "INSERT INTO daily_geo_metrics(metric_date, country_code, views, "
                    "subscribers_gained, likes) VALUES ('2026-05-01', 'IN', 100, 5, 10)"
                )
                try:
                    conn.execute(
                        "INSERT INTO daily_geo_metrics(metric_date, country_code, views, "
                        "subscribers_gained, likes) VALUES ('2026-05-01', 'IN', 200, 10, 20)"
                    )
                    raised = False
                except sqlite3.IntegrityError:
                    raised = True
                assert raised
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd ~/VS\ Code\ Projects/human-workforce-analytics && \
.venv/bin/pytest tests/test_db.py::test_daily_geo_metrics_table_created tests/test_db.py::test_daily_geo_metrics_primary_key_constraint -v
```

Expected: FAIL with `OperationalError: no such table: daily_geo_metrics`

- [ ] **Step 3: Add the table and index to `SCHEMA` in `db.py`**

In `db.py`, inside the `SCHEMA` triple-quoted string, find this block (around line 73):

```python
CREATE INDEX IF NOT EXISTS idx_retention_buckets_kind_end
    ON retention_buckets(window_kind, window_end);
CREATE TABLE IF NOT EXISTS publishing_queue (
```

Insert the new table and index between those two statements:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd ~/VS\ Code\ Projects/human-workforce-analytics && \
.venv/bin/pytest tests/test_db.py::test_daily_geo_metrics_table_created tests/test_db.py::test_daily_geo_metrics_primary_key_constraint -v
```

Expected: PASS

- [ ] **Step 5: Run full `test_db.py` to check for regressions**

```bash
cd ~/VS\ Code\ Projects/human-workforce-analytics && \
.venv/bin/pytest tests/test_db.py -v
```

Expected: All tests PASS.

- [ ] **Step 6: Commit**

```bash
cd ~/VS\ Code\ Projects/human-workforce-analytics && \
git add db.py tests/test_db.py && \
git commit -m "feat: add daily_geo_metrics table to schema"
```

---

### Task 2: Add `fetch_daily_geo_metrics` to `youtube_client.py`

**Files:**
- Modify: `youtube_client.py`
- Test: `tests/test_youtube_client.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_youtube_client.py`:

```python
def _fake_geo_response(rows):
    """Return a mock that mimics analytics_service().reports().query().execute()."""
    service = MagicMock()
    service.reports().query().execute.return_value = {"rows": rows}
    return service


def test_fetch_daily_geo_metrics_parses_rows():
    """Each API row [date, country, views, subs, likes] maps to correct dict keys."""
    rows = [
        ["2026-05-01", "IN", 12345, 210, 500],
        ["2026-05-01", "US", 543, 12, 30],
    ]
    with patch("youtube_client.analytics_service") as mock_svc:
        mock_svc.return_value = _fake_geo_response(rows)
        result = youtube_client.fetch_daily_geo_metrics(
            start=date(2026, 5, 1), end=date(2026, 5, 7)
        )

    assert len(result) == 2
    assert result[0] == {
        "metric_date": "2026-05-01",
        "country_code": "IN",
        "views": 12345,
        "subscribers_gained": 210,
        "likes": 500,
    }
    assert result[1]["country_code"] == "US"
    assert result[1]["views"] == 543


def test_fetch_daily_geo_metrics_returns_empty_list_when_no_rows():
    """Empty API response returns an empty list without error."""
    with patch("youtube_client.analytics_service") as mock_svc:
        mock_svc.return_value = _fake_geo_response([])
        result = youtube_client.fetch_daily_geo_metrics(
            start=date(2026, 5, 1), end=date(2026, 5, 7)
        )
    assert result == []


def test_fetch_daily_geo_metrics_uses_channel_id_when_provided():
    """When channel_id is given, the ids param is 'channel==<id>', not 'channel==MINE'."""
    with patch("youtube_client.analytics_service") as mock_svc:
        mock_svc.return_value = _fake_geo_response([])
        youtube_client.fetch_daily_geo_metrics(
            start=date(2026, 5, 1), end=date(2026, 5, 7),
            channel_id="UCHDU3z8f5_HJzJL1w2J2EaQ",
        )
        call_kwargs = mock_svc.return_value.reports.return_value.query.call_args[1]
        assert call_kwargs["ids"] == "channel==UCHDU3z8f5_HJzJL1w2J2EaQ"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd ~/VS\ Code\ Projects/human-workforce-analytics && \
.venv/bin/pytest tests/test_youtube_client.py::test_fetch_daily_geo_metrics_parses_rows tests/test_youtube_client.py::test_fetch_daily_geo_metrics_returns_empty_list_when_no_rows tests/test_youtube_client.py::test_fetch_daily_geo_metrics_uses_channel_id_when_provided -v
```

Expected: FAIL with `AttributeError: module 'youtube_client' has no attribute 'fetch_daily_geo_metrics'`

- [ ] **Step 3: Add `fetch_daily_geo_metrics` to `youtube_client.py`**

In `youtube_client.py`, insert this function after `fetch_video_views_in_window` and before `parse_iso8601_duration`:

```python
def fetch_daily_geo_metrics(start: date, end: date, channel_id: str | None = None) -> list[dict]:
    """Fetch daily views, subscribers_gained, and likes broken down by country."""
    yt = analytics_service()
    ids = f"channel=={channel_id}" if channel_id else "channel==MINE"
    resp = yt.reports().query(
        ids=ids,
        startDate=start.isoformat(),
        endDate=end.isoformat(),
        metrics="views,subscribersGained,likes",
        dimensions="day,country",
    ).execute()
    rows = resp.get("rows", [])
    return [
        {
            "metric_date": r[0],
            "country_code": r[1],
            "views": int(r[2]),
            "subscribers_gained": int(r[3]),
            "likes": int(r[4]),
        }
        for r in rows
    ]
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd ~/VS\ Code\ Projects/human-workforce-analytics && \
.venv/bin/pytest tests/test_youtube_client.py::test_fetch_daily_geo_metrics_parses_rows tests/test_youtube_client.py::test_fetch_daily_geo_metrics_returns_empty_list_when_no_rows tests/test_youtube_client.py::test_fetch_daily_geo_metrics_uses_channel_id_when_provided -v
```

Expected: PASS

- [ ] **Step 5: Run full `test_youtube_client.py` to check for regressions**

```bash
cd ~/VS\ Code\ Projects/human-workforce-analytics && \
.venv/bin/pytest tests/test_youtube_client.py -v
```

Expected: All tests PASS.

- [ ] **Step 6: Commit**

```bash
cd ~/VS\ Code\ Projects/human-workforce-analytics && \
git add youtube_client.py tests/test_youtube_client.py && \
git commit -m "feat: add fetch_daily_geo_metrics to youtube_client"
```

---

### Task 3: Wire geo metrics into the fetch pipeline

**Files:**
- Modify: `fetch_metrics.py`
- Test: `tests/test_fetch_metrics.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_fetch_metrics.py`:

```python
def test_write_geo_metrics_upserts_rows():
    """write_geo_metrics inserts rows and upserts on conflict without adding duplicates."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()

            from fetch_metrics import write_geo_metrics

            rows = [
                {"metric_date": "2026-05-01", "country_code": "IN",
                 "views": 1000, "subscribers_gained": 20, "likes": 50},
                {"metric_date": "2026-05-01", "country_code": "US",
                 "views": 200, "subscribers_gained": 3, "likes": 8},
            ]
            write_geo_metrics(rows)

            with sqlite3.connect(db_path) as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM daily_geo_metrics"
                ).fetchone()[0]
                assert count == 2

                views = conn.execute(
                    "SELECT views FROM daily_geo_metrics "
                    "WHERE metric_date='2026-05-01' AND country_code='IN'"
                ).fetchone()[0]
                assert views == 1000

            # Upsert: re-insert same key with updated views — row count stays at 2
            write_geo_metrics([
                {"metric_date": "2026-05-01", "country_code": "IN",
                 "views": 1500, "subscribers_gained": 25, "likes": 60},
            ])

            with sqlite3.connect(db_path) as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM daily_geo_metrics"
                ).fetchone()[0]
                assert count == 2

                views = conn.execute(
                    "SELECT views FROM daily_geo_metrics "
                    "WHERE metric_date='2026-05-01' AND country_code='IN'"
                ).fetchone()[0]
                assert views == 1500


def test_write_geo_metrics_empty_list_is_noop():
    """Calling write_geo_metrics with an empty list writes nothing and doesn't error."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            from fetch_metrics import write_geo_metrics
            write_geo_metrics([])
            with sqlite3.connect(db_path) as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM daily_geo_metrics"
                ).fetchone()[0]
                assert count == 0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd ~/VS\ Code\ Projects/human-workforce-analytics && \
.venv/bin/pytest tests/test_fetch_metrics.py::test_write_geo_metrics_upserts_rows tests/test_fetch_metrics.py::test_write_geo_metrics_empty_list_is_noop -v
```

Expected: FAIL with `ImportError: cannot import name 'write_geo_metrics' from 'fetch_metrics'`

- [ ] **Step 3a: Update the `youtube_client` import in `fetch_metrics.py`**

Replace the existing `from youtube_client import (...)` block:

```python
from youtube_client import (
    fetch_all_video_ids,
    fetch_channel_stats,
    fetch_daily_channel_metrics,
    fetch_daily_geo_metrics,
    fetch_retention_curve,
    fetch_video_details,
    fetch_video_period_metrics,
    fetch_video_views_in_window,
    parse_iso8601_duration,
    resolve_channel_id,
)
```

- [ ] **Step 3b: Add `write_geo_metrics` to `fetch_metrics.py`**

Insert this function before `main()`:

```python
def write_geo_metrics(rows: list[dict]) -> None:
    with get_conn() as conn:
        for d in rows:
            conn.execute(
                "INSERT INTO daily_geo_metrics(metric_date, country_code, views, "
                "subscribers_gained, likes) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(metric_date, country_code) DO UPDATE SET "
                "views=excluded.views, "
                "subscribers_gained=excluded.subscribers_gained, "
                "likes=excluded.likes",
                (d["metric_date"], d["country_code"], d["views"],
                 d["subscribers_gained"], d["likes"]),
            )
```

- [ ] **Step 3c: Add the fetch call inside `main()`**

In `main()`, after the existing `fetch_video_period_metrics` try/except block (around line 156) and before `with get_conn() as conn:`, add:

```python
    print(f"Fetching daily geo metrics {start} -> {end}...")
    try:
        daily_geo = fetch_daily_geo_metrics(start, end, channel_id)
    except Exception as e:
        print(f"  daily geo metrics failed ({e.__class__.__name__}), skipping.")
        daily_geo = []
```

- [ ] **Step 3d: Call `write_geo_metrics` from `main()`**

In `main()`, after the closing of the `with get_conn() as conn:` block and before `print("Fetching retention curves...")`, add:

```python
    write_geo_metrics(daily_geo)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd ~/VS\ Code\ Projects/human-workforce-analytics && \
.venv/bin/pytest tests/test_fetch_metrics.py::test_write_geo_metrics_upserts_rows tests/test_fetch_metrics.py::test_write_geo_metrics_empty_list_is_noop -v
```

Expected: PASS

- [ ] **Step 5: Run full `test_fetch_metrics.py` to check for regressions**

```bash
cd ~/VS\ Code\ Projects/human-workforce-analytics && \
.venv/bin/pytest tests/test_fetch_metrics.py -v
```

Expected: All tests PASS.

- [ ] **Step 6: Commit**

```bash
cd ~/VS\ Code\ Projects/human-workforce-analytics && \
git add fetch_metrics.py tests/test_fetch_metrics.py && \
git commit -m "feat: wire daily_geo_metrics into fetch pipeline"
```

---

### Task 4: Add Geographic Trends section to `app.py`

**Files:**
- Modify: `app.py`

This section is UI — there is no unit test for it. Correctness is verified by running the app locally with a populated DB.

- [ ] **Step 1: Add the `daily_geo` load query**

In `app.py`, after the `publishing_queue = load(...)` call (around line 103) and before the empty-data guard at line 108, add:

```python
daily_geo = load(
    "SELECT metric_date, country_code, views, subscribers_gained, likes "
    "FROM daily_geo_metrics ORDER BY metric_date"
)
```

- [ ] **Step 2: Add the country name mapping constant**

In `app.py`, after the `RANGES` dict (around line 57), add:

```python
COUNTRY_NAMES = {
    "IN": "India", "US": "United States", "GB": "United Kingdom",
    "RO": "Romania", "CA": "Canada", "AU": "Australia",
    "PK": "Pakistan", "NG": "Nigeria", "DE": "Germany",
    "BR": "Brazil", "PH": "Philippines", "BD": "Bangladesh",
    "ID": "Indonesia", "MX": "Mexico", "FR": "France",
}
```

- [ ] **Step 3: Add the Geographic Trends section**

In `app.py`, find the comment `# --- Top videos ---` (around line 199). Insert the following block **immediately before** that comment:

```python
# --- Geographic Trends ---

if not daily_geo.empty:
    st.subheader("Geographic Trends")
    st.caption(
        "Daily views, subscribers gained, and likes for the top 5 countries by views "
        "in the selected range."
    )
    days = range_picker("geo_range")
    geo = filter_days(daily_geo, "metric_date", days).copy()
    geo["country"] = geo["country_code"].map(lambda c: COUNTRY_NAMES.get(c, c))

    top5 = (
        geo.groupby("country")["views"].sum()
        .nlargest(5).index.tolist()
    )
    geo = geo[geo["country"].isin(top5)]

    if geo.empty:
        st.info("No geographic data for this range yet.")
    else:
        fig = make_subplots(
            rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.07,
            subplot_titles=("Views per day", "Subscribers gained per day", "Likes per day"),
        )
        colors = px.colors.qualitative.Plotly
        for i, country in enumerate(top5):
            cdf = geo[geo["country"] == country].sort_values("metric_date")
            color = colors[i % len(colors)]
            fig.add_scatter(
                x=cdf["metric_date"], y=cdf["views"],
                name=country, mode="lines", line=dict(color=color),
                legendgroup=country, row=1, col=1,
            )
            fig.add_scatter(
                x=cdf["metric_date"], y=cdf["subscribers_gained"],
                name=country, mode="lines", line=dict(color=color),
                legendgroup=country, showlegend=False, row=2, col=1,
            )
            fig.add_scatter(
                x=cdf["metric_date"], y=cdf["likes"],
                name=country, mode="lines", line=dict(color=color),
                legendgroup=country, showlegend=False, row=3, col=1,
            )
        fig.update_layout(height=720, hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)
else:
    st.subheader("Geographic Trends")
    st.info("Geographic data still loading. Run `python fetch_metrics.py` to populate.")


```

- [ ] **Step 4: Run the smoke test**

```bash
cd ~/VS\ Code\ Projects/human-workforce-analytics && \
.venv/bin/pytest tests/test_smoke.py -v
```

Expected: PASS (smoke test imports `app` module without error)

- [ ] **Step 5: Run the full test suite**

```bash
cd ~/VS\ Code\ Projects/human-workforce-analytics && \
.venv/bin/pytest -v
```

Expected: All tests PASS.

- [ ] **Step 6: Commit**

```bash
cd ~/VS\ Code\ Projects/human-workforce-analytics && \
git add app.py && \
git commit -m "feat: add Geographic Trends section to dashboard"
```
