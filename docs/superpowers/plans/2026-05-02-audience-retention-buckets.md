# Audience Retention Bucket Charts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add retention-bucket KPI cards and stacked-bar trend charts to the Human Workforce Analytics dashboard, both channel-wide (aggregated across videos) and per-video, computed from YouTube's audience retention curve.

**Architecture:** Fetch the `audienceWatchRatio` curve per (video, time-window) from the YouTube Analytics API. Store only the two values we need (ratio at 25% and 75%) in a new `retention_buckets` table. Pure aggregation logic lives in a new `retention.py` module so it's testable without Streamlit. Phase 1 ships KPI cards using three rolling-window rows; Phase 2 adds trend charts backed by per-week rows + a one-time backfill.

**Tech Stack:** Python 3.11+, SQLite, `googleapiclient` (YouTube Analytics API v2), Streamlit, Plotly. New dev dep: `pytest`.

**Spec:** [`docs/superpowers/specs/2026-05-02-audience-retention-buckets-design.md`](../specs/2026-05-02-audience-retention-buckets-design.md)

---

## File Structure

**New files:**
- `retention.py` — pure aggregation functions (bucket shares, snapshot rollup, trend binning). Testable without Streamlit or DB.
- `tests/__init__.py` — marks tests dir as a package.
- `tests/test_youtube_client.py` — tests for the new `fetch_retention_curve` function (mocks API).
- `tests/test_retention.py` — tests for aggregation logic.
- `tests/test_db.py` — tests for the migration.
- `tests/test_fetch_metrics.py` — integration test for fetch run writing rows.
- `scripts/backfill_retention.py` — one-time weekly backfill script (Phase 2).

**Modified files:**
- `db.py` — adds `retention_buckets` table + index to `SCHEMA`.
- `youtube_client.py` — adds `fetch_retention_curve(video_id, start, end)`.
- `fetch_metrics.py` — calls the new fetcher; writes 3 rolling-window rows + (Phase 2) the latest weekly row.
- `app.py` — adds "Audience Retention" section + extends Per-Video Deep Dive.
- `requirements.txt` — adds `pytest`.

---

## Phase 1 — Snapshot KPI Cards

### Task 1: Set up test infrastructure

**Files:**
- Modify: `requirements.txt`
- Create: `tests/__init__.py`
- Create: `tests/test_smoke.py`

- [ ] **Step 1: Add pytest to requirements**

Edit `requirements.txt` — append one line:

```
pytest>=8.0.0
```

- [ ] **Step 2: Install it**

Run: `pip install -r requirements.txt`
Expected: pytest installs successfully.

- [ ] **Step 3: Create empty tests package**

Create `tests/__init__.py` with empty content.

- [ ] **Step 4: Write a smoke test**

Create `tests/test_smoke.py`:

```python
def test_imports():
    """Verify the project's modules can be imported under pytest."""
    import db
    import youtube_client
    assert callable(db.init_db)
    assert callable(youtube_client.fetch_channel_stats)
```

- [ ] **Step 5: Run pytest to verify discovery and pass**

Run: `pytest tests/ -v`
Expected: 1 test passes.

- [ ] **Step 6: Commit**

```bash
git add requirements.txt tests/__init__.py tests/test_smoke.py
git commit -m "test: add pytest infrastructure with smoke test"
```

---

### Task 2: Add `retention_buckets` table to schema

**Files:**
- Modify: `db.py:7-60` (extend `SCHEMA`)
- Create: `tests/test_db.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_db.py`:

```python
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch


def test_retention_buckets_table_created():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            with sqlite3.connect(db_path) as conn:
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='retention_buckets'"
                )
                assert cursor.fetchone() is not None


def test_retention_buckets_init_is_idempotent():
    """Running init_db twice must not error."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            db.init_db()


def test_retention_buckets_primary_key():
    """Inserting a duplicate (video_id, window_start, window_end, window_kind) must conflict."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "INSERT INTO retention_buckets (video_id, window_start, window_end, "
                    "window_kind, views, retention_at_25, retention_at_75, fetched_at) "
                    "VALUES ('v1', '2026-01-01', '2026-01-08', 'weekly', 100, 0.6, 0.3, "
                    "'2026-01-08T00:00:00Z')"
                )
                try:
                    conn.execute(
                        "INSERT INTO retention_buckets (video_id, window_start, window_end, "
                        "window_kind, views, retention_at_25, retention_at_75, fetched_at) "
                        "VALUES ('v1', '2026-01-01', '2026-01-08', 'weekly', 200, 0.7, 0.4, "
                        "'2026-01-08T00:00:00Z')"
                    )
                    raised = False
                except sqlite3.IntegrityError:
                    raised = True
                assert raised


def test_retention_buckets_window_kind_disambiguates():
    """A rolling7 row and a weekly row can share start/end if their kinds differ."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "INSERT INTO retention_buckets (video_id, window_start, window_end, "
                    "window_kind, views, retention_at_25, retention_at_75, fetched_at) "
                    "VALUES ('v1', '2026-01-01', '2026-01-08', 'rolling7', 100, 0.6, 0.3, "
                    "'2026-01-08T00:00:00Z')"
                )
                conn.execute(
                    "INSERT INTO retention_buckets (video_id, window_start, window_end, "
                    "window_kind, views, retention_at_25, retention_at_75, fetched_at) "
                    "VALUES ('v1', '2026-01-01', '2026-01-08', 'weekly', 100, 0.6, 0.3, "
                    "'2026-01-08T00:00:00Z')"
                )
                count = conn.execute(
                    "SELECT COUNT(*) FROM retention_buckets"
                ).fetchone()[0]
                assert count == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_db.py -v`
Expected: FAIL with "no such table: retention_buckets".

- [ ] **Step 3: Add table to schema**

Edit `db.py`. In the `SCHEMA` string, after the `daily_channel_metrics` table definition (around line 54) and before the index definitions, add:

```sql
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
```

`window_kind` is one of: `'rolling7'`, `'rolling90'`, `'rolling365'`, `'weekly'`. It's part of the primary key so that a rolling-7d row and a weekly row that happen to share start/end (possible on Sundays) can coexist.

Also add this index right after the existing two indexes (before the closing `"""`):

```sql
CREATE INDEX IF NOT EXISTS idx_retention_buckets_kind_end
    ON retention_buckets(window_kind, window_end);
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_db.py -v`
Expected: 4 tests pass.

- [ ] **Step 5: Apply migration to the live DB**

Run: `python db.py`
Expected: prints `Initialized database at ...`. The new table is added without affecting existing data.

- [ ] **Step 6: Verify the live DB still has its data**

Run: `python -c "import sqlite3; c=sqlite3.connect('data.db'); print(list(c.execute('SELECT count(*) FROM channel_snapshots')))"`
Expected: a non-zero count, matching what was there before.

- [ ] **Step 7: Commit**

```bash
git add db.py tests/test_db.py
git commit -m "feat(db): add retention_buckets table and idempotent migration"
```

---

### Task 3: Add `fetch_retention_curve` to youtube_client

**Files:**
- Modify: `youtube_client.py` (append a new function)
- Create: `tests/test_youtube_client.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_youtube_client.py`:

```python
from datetime import date
from unittest.mock import MagicMock, patch

import youtube_client


def _fake_retention_response(rows):
    """Return a mock that mimics analytics_service().reports().query().execute()."""
    service = MagicMock()
    service.reports().query().execute.return_value = {"rows": rows}
    return service


def test_fetch_retention_curve_extracts_25_and_75_exactly():
    """Given exact rows at 0.25 and 0.75, return those values directly."""
    rows = [[round(0.01 * i, 2), 1.0 - 0.01 * i] for i in range(101)]
    # rows: [[0.00, 1.00], [0.01, 0.99], ..., [0.25, 0.75], ..., [0.75, 0.25], ..., [1.00, 0.00]]

    with patch("youtube_client.analytics_service") as mock_svc:
        mock_svc.return_value = _fake_retention_response(rows)
        result = youtube_client.fetch_retention_curve(
            video_id="v1", start=date(2026, 1, 1), end=date(2026, 1, 8)
        )

    assert result["video_id"] == "v1"
    assert result["window_start"] == "2026-01-01"
    assert result["window_end"] == "2026-01-08"
    assert abs(result["retention_at_25"] - 0.75) < 1e-6
    assert abs(result["retention_at_75"] - 0.25) < 1e-6


def test_fetch_retention_curve_interpolates_when_target_not_present():
    """If 0.25 lies between 0.24 and 0.26, interpolate linearly."""
    rows = [
        [0.24, 0.80],
        [0.26, 0.70],  # 0.25 should interpolate to 0.75
        [0.74, 0.40],
        [0.76, 0.30],  # 0.75 should interpolate to 0.35
    ]
    with patch("youtube_client.analytics_service") as mock_svc:
        mock_svc.return_value = _fake_retention_response(rows)
        result = youtube_client.fetch_retention_curve(
            video_id="v1", start=date(2026, 1, 1), end=date(2026, 1, 8)
        )

    assert abs(result["retention_at_25"] - 0.75) < 1e-6
    assert abs(result["retention_at_75"] - 0.35) < 1e-6


def test_fetch_retention_curve_caps_above_one():
    """audienceWatchRatio can exceed 1.0 due to rewatches; cap at 1.0."""
    rows = [
        [0.25, 1.4],
        [0.75, 0.5],
    ]
    with patch("youtube_client.analytics_service") as mock_svc:
        mock_svc.return_value = _fake_retention_response(rows)
        result = youtube_client.fetch_retention_curve(
            video_id="v1", start=date(2026, 1, 1), end=date(2026, 1, 8)
        )

    assert result["retention_at_25"] == 1.0
    assert result["retention_at_75"] == 0.5


def test_fetch_retention_curve_returns_none_for_empty_response():
    """Videos with too few views return no rows; we return None to signal skip."""
    with patch("youtube_client.analytics_service") as mock_svc:
        mock_svc.return_value = _fake_retention_response([])
        result = youtube_client.fetch_retention_curve(
            video_id="v1", start=date(2026, 1, 1), end=date(2026, 1, 8)
        )

    assert result is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_youtube_client.py -v`
Expected: FAIL with `AttributeError: module 'youtube_client' has no attribute 'fetch_retention_curve'`.

- [ ] **Step 3: Implement the fetcher**

Append to `youtube_client.py`:

```python
def fetch_retention_curve(video_id: str, start: date, end: date) -> dict | None:
    """Fetch audienceWatchRatio at 25% and 75% elapsed time for one video.

    Returns None if the API has no data for this (video, window) — typically
    because the video has too few views to clear YouTube's privacy threshold.
    """
    yt = analytics_service()
    resp = yt.reports().query(
        ids="channel==MINE",
        startDate=start.isoformat(),
        endDate=end.isoformat(),
        metrics="audienceWatchRatio",
        dimensions="elapsedVideoTimeRatio",
        filters=f"video=={video_id};audienceType==ORGANIC",
    ).execute()
    rows = resp.get("rows", [])
    if not rows:
        return None

    points = sorted((float(r[0]), float(r[1])) for r in rows)

    def at(target: float) -> float:
        for i, (t, _) in enumerate(points):
            if abs(t - target) < 1e-6:
                return min(points[i][1], 1.0)
            if t > target:
                if i == 0:
                    return min(points[0][1], 1.0)
                t0, v0 = points[i - 1]
                t1, v1 = points[i]
                ratio = (target - t0) / (t1 - t0)
                return min(v0 + ratio * (v1 - v0), 1.0)
        return min(points[-1][1], 1.0)

    return {
        "video_id": video_id,
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "retention_at_25": at(0.25),
        "retention_at_75": at(0.75),
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_youtube_client.py -v`
Expected: 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add youtube_client.py tests/test_youtube_client.py
git commit -m "feat(youtube): add fetch_retention_curve with interpolation + rewatch cap"
```

---

### Task 4: Create `retention.py` aggregation module

**Files:**
- Create: `retention.py`
- Create: `tests/test_retention.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_retention.py`:

```python
import pandas as pd
import pytest

import retention


def test_bucket_shares_basic():
    """r25=0.6, r75=0.3 → b1=0.4, b2=0.3, b3=0.3."""
    shares = retention.bucket_shares(r25=0.6, r75=0.3)
    assert shares == pytest.approx((0.4, 0.3, 0.3))


def test_bucket_shares_caps_above_one():
    """A r25 > 1.0 (shouldn't happen, but defensive) is capped."""
    shares = retention.bucket_shares(r25=1.2, r75=0.5)
    assert shares == pytest.approx((0.0, 0.5, 0.5))


def test_bucket_shares_floors_negative():
    """If r75 > r25 (data anomaly), b2 cannot be negative — floor at 0."""
    shares = retention.bucket_shares(r25=0.4, r75=0.5)
    b1, b2, b3 = shares
    assert b1 == pytest.approx(0.6)
    assert b2 == 0.0
    assert b3 == pytest.approx(0.5)


def test_aggregate_snapshot_sums_across_videos():
    """Two videos contribute their bucket counts; totals sum."""
    rows = pd.DataFrame([
        {"video_id": "v1", "views": 1000, "retention_at_25": 0.6, "retention_at_75": 0.3},
        {"video_id": "v2", "views": 500,  "retention_at_25": 0.8, "retention_at_75": 0.5},
    ])
    snap = retention.aggregate_snapshot(rows)
    # v1: b1=400, b2=300, b3=300; v2: b1=100, b2=150, b3=250
    assert snap["b1_count"] == pytest.approx(500.0)
    assert snap["b2_count"] == pytest.approx(450.0)
    assert snap["b3_count"] == pytest.approx(550.0)
    assert snap["total_views"] == 1500
    assert snap["b1_pct"] == pytest.approx(500 / 1500)
    assert snap["b2_pct"] == pytest.approx(450 / 1500)
    assert snap["b3_pct"] == pytest.approx(550 / 1500)


def test_aggregate_snapshot_empty_returns_zeros():
    """Empty input returns all zeros, not a divide-by-zero."""
    snap = retention.aggregate_snapshot(pd.DataFrame(columns=[
        "video_id", "views", "retention_at_25", "retention_at_75"
    ]))
    assert snap["total_views"] == 0
    assert snap["b1_count"] == 0
    assert snap["b1_pct"] == 0.0
    assert snap["b2_pct"] == 0.0
    assert snap["b3_pct"] == 0.0


def test_window_bounds_for_toggle():
    """Each toggle resolves to (window_start, window_end, days_back) for the snapshot."""
    today = pd.Timestamp("2026-05-02").date()
    assert retention.window_bounds_for_toggle("Last week", today=today) == (
        pd.Timestamp("2026-04-25").date(), today, 7,
    )
    assert retention.window_bounds_for_toggle("Last month", today=today) == (
        pd.Timestamp("2026-02-01").date(), today, 30,
    )
    assert retention.window_bounds_for_toggle("Last quarter", today=today) == (
        pd.Timestamp("2026-02-01").date(), today, 90,
    )
    assert retention.window_bounds_for_toggle("Last year", today=today) == (
        pd.Timestamp("2025-05-02").date(), today, 365,
    )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_retention.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'retention'`.

- [ ] **Step 3: Create the module**

Create `retention.py`:

```python
"""Aggregation logic for audience retention buckets.

Pure functions — no DB, no Streamlit, no API. Easy to test in isolation.
"""

from datetime import date, timedelta

import pandas as pd

# Maps the dashboard's range_picker labels to (rolling_window_days, snapshot_days).
# rolling_window_days = which retention_buckets row to read (7/90/365).
# snapshot_days = how far back to scope view counts.
_TOGGLE_WINDOWS = {
    "Last week":    (7, 7),
    "Last month":   (90, 30),
    "Last quarter": (90, 90),
    "Last year":    (365, 365),
}


def bucket_shares(r25: float, r75: float) -> tuple[float, float, float]:
    """Convert retention ratios into (b1, b2, b3) shares that sum to <=1.

    b1 = viewers who dropped before 25%      = 1 - r25
    b2 = viewers who reached 25% but not 75% = r25 - r75
    b3 = viewers who reached 75%             = r75
    Shares are clamped to [0, 1] for defense against API anomalies.
    """
    r25 = min(max(r25, 0.0), 1.0)
    r75 = min(max(r75, 0.0), 1.0)
    if r75 > r25:
        r75 = r25  # impossible curve, treat the inversion as zero mid-bucket
    return (1.0 - r25, r25 - r75, r75)


def aggregate_snapshot(rows: pd.DataFrame) -> dict:
    """Aggregate per-video retention rows into channel-wide bucket totals.

    `rows` must have columns: video_id, views, retention_at_25, retention_at_75.
    Returns counts and percentages for the three buckets.
    """
    if rows.empty:
        return {
            "total_views": 0,
            "b1_count": 0.0, "b2_count": 0.0, "b3_count": 0.0,
            "b1_pct": 0.0, "b2_pct": 0.0, "b3_pct": 0.0,
        }

    shares = rows.apply(
        lambda r: bucket_shares(r["retention_at_25"], r["retention_at_75"]),
        axis=1, result_type="expand",
    )
    shares.columns = ["s1", "s2", "s3"]
    counts = shares.multiply(rows["views"], axis=0)
    total = int(rows["views"].sum())
    b1, b2, b3 = float(counts["s1"].sum()), float(counts["s2"].sum()), float(counts["s3"].sum())
    return {
        "total_views": total,
        "b1_count": b1, "b2_count": b2, "b3_count": b3,
        "b1_pct": b1 / total if total else 0.0,
        "b2_pct": b2 / total if total else 0.0,
        "b3_pct": b3 / total if total else 0.0,
    }


def window_bounds_for_toggle(toggle: str, today: date) -> tuple[date, date, int]:
    """Resolve a range_picker label to (snapshot_start, snapshot_end, rolling_window_days).

    The rolling_window_days is which row in retention_buckets to read
    (always 7, 90, or 365 — the three windows fetch_metrics writes).
    The snapshot_start..snapshot_end is the time range we'll scope view counts to
    (matters only for "Last month", which reuses the 90-day ratio with 30-day views).
    """
    if toggle not in _TOGGLE_WINDOWS:
        raise ValueError(f"Unknown toggle: {toggle}")
    rolling_days, snapshot_days = _TOGGLE_WINDOWS[toggle]
    snapshot_start = today - timedelta(days=snapshot_days)
    return snapshot_start, today, rolling_days
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_retention.py -v`
Expected: 6 tests pass.

- [ ] **Step 5: Commit**

```bash
git add retention.py tests/test_retention.py
git commit -m "feat: add retention aggregation module with bucket math + toggle mapping"
```

---

### Task 5: Wire fetcher into `fetch_metrics.py`

**Files:**
- Modify: `fetch_metrics.py`
- Create: `tests/test_fetch_metrics.py`

- [ ] **Step 1: Write the failing integration test**

Create `tests/test_fetch_metrics.py`:

```python
import sqlite3
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import patch


def test_write_retention_rolling_windows_writes_three_rows_per_video():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()

            # Seed two videos with daily view rows so the views column can be summed.
            with sqlite3.connect(db_path) as conn:
                for vid in ("v1", "v2"):
                    conn.execute(
                        "INSERT INTO videos(video_id, title, published_at, duration_seconds) "
                        "VALUES (?, ?, ?, ?)",
                        (vid, f"Video {vid}", "2026-01-01T00:00:00Z", 600),
                    )
                    for d in range(0, 400, 5):
                        conn.execute(
                            "INSERT INTO daily_video_metrics(metric_date, video_id, views, "
                            "estimated_minutes_watched, average_view_duration, likes, "
                            "subscribers_gained) VALUES (?, ?, ?, 0, 0, 0, 0)",
                            ((date.today().fromordinal(date.today().toordinal() - d)).isoformat(),
                             vid, 10),
                        )

            from fetch_metrics import write_retention_rolling_windows

            def fake_curve(video_id, start, end):
                return {
                    "video_id": video_id,
                    "window_start": start.isoformat(),
                    "window_end": end.isoformat(),
                    "retention_at_25": 0.7,
                    "retention_at_75": 0.4,
                }

            with patch("fetch_metrics.fetch_retention_curve", side_effect=fake_curve):
                write_retention_rolling_windows(["v1", "v2"], today=date(2026, 5, 2))

            with sqlite3.connect(db_path) as conn:
                rows = list(conn.execute(
                    "SELECT video_id, window_start, window_end, window_kind, views "
                    "FROM retention_buckets ORDER BY video_id, window_kind"
                ))
                # 3 windows × 2 videos = 6 rows
                assert len(rows) == 6
                kinds = sorted({r[3] for r in rows})
                assert kinds == ["rolling365", "rolling7", "rolling90"]
                # views per row > 0 (we seeded daily metrics)
                assert all(r[4] > 0 for r in rows)


def test_write_retention_rolling_windows_skips_when_curve_is_none():
    """If the API has no data, we don't insert a row."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "INSERT INTO videos(video_id, title, published_at, duration_seconds) "
                    "VALUES ('v1', 'V1', '2026-01-01T00:00:00Z', 600)"
                )

            from fetch_metrics import write_retention_rolling_windows
            with patch("fetch_metrics.fetch_retention_curve", return_value=None):
                write_retention_rolling_windows(["v1"], today=date(2026, 5, 2))

            with sqlite3.connect(db_path) as conn:
                count = conn.execute(
                    "SELECT count(*) FROM retention_buckets"
                ).fetchone()[0]
                assert count == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_fetch_metrics.py -v`
Expected: FAIL with `ImportError: cannot import name 'write_retention_rolling_windows'`.

- [ ] **Step 3: Implement `write_retention_rolling_windows`**

Edit `fetch_metrics.py`. Add this import near the top:

```python
from datetime import date, datetime, timedelta, timezone
```

(`date` and `timedelta` are already there — confirm it.)

Update the imports from `youtube_client`:

```python
from youtube_client import (
    fetch_all_video_ids,
    fetch_channel_stats,
    fetch_daily_channel_metrics,
    fetch_retention_curve,
    fetch_video_details,
    fetch_video_period_metrics,
    parse_iso8601_duration,
    resolve_channel_id,
)
```

Then add this new function above `main()`:

```python
ROLLING_WINDOWS = (
    (7, "rolling7"),
    (90, "rolling90"),
    (365, "rolling365"),
)


def write_retention_rolling_windows(video_ids: list[str], today: date | None = None) -> None:
    """Fetch retention curves for three rolling windows per video and persist them.

    For each window, view counts are summed from daily_video_metrics so we don't
    need a separate API call for views. If the API returns no retention data
    (e.g., low-view video), we skip it silently — the dashboard tolerates absence.
    """
    today = today or date.today()
    fetched_at = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        for vid in video_ids:
            for days, kind in ROLLING_WINDOWS:
                start = today - timedelta(days=days)
                curve = fetch_retention_curve(vid, start, today)
                if curve is None:
                    continue
                views = conn.execute(
                    "SELECT COALESCE(SUM(views), 0) FROM daily_video_metrics "
                    "WHERE video_id = ? AND metric_date BETWEEN ? AND ?",
                    (vid, start.isoformat(), today.isoformat()),
                ).fetchone()[0]
                conn.execute(
                    "INSERT INTO retention_buckets(video_id, window_start, window_end, "
                    "window_kind, views, retention_at_25, retention_at_75, fetched_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(video_id, window_start, window_end, window_kind) DO UPDATE SET "
                    "views=excluded.views, retention_at_25=excluded.retention_at_25, "
                    "retention_at_75=excluded.retention_at_75, fetched_at=excluded.fetched_at",
                    (vid, start.isoformat(), today.isoformat(), kind,
                     int(views), curve["retention_at_25"], curve["retention_at_75"],
                     fetched_at),
                )
```

Also import `get_conn` (already imported at the top) and confirm.

Then call it from `main()`. Find this line in `main()`:

```python
    print(f"Fetching per-video totals {start} -> {end}...")
    daily_video = fetch_video_period_metrics(start, end, channel_id)
```

After the `with get_conn() as conn:` block that follows it (the existing big insert block — at the end after `for d in daily_video:`), add a new step *outside* that block:

```python
    print("Fetching retention curves for rolling windows (7/90/365 days)...")
    write_retention_rolling_windows([v["video_id"] for v in videos])
```

This must come *after* the main `with get_conn() as conn:` block so that `daily_video_metrics` is up-to-date when we compute view counts.

- [ ] **Step 4: Run integration tests to verify pass**

Run: `pytest tests/ -v`
Expected: all tests pass (smoke + db + youtube_client + retention + fetch_metrics).

- [ ] **Step 5: Run a real fetch to populate the live DB**

Run: `python fetch_metrics.py`
Expected: log output ending with the retention-windows step, and the script completes.

- [ ] **Step 6: Verify live retention rows landed**

Run:

```bash
python -c "
import sqlite3
c = sqlite3.connect('data.db')
print('Total rows:', c.execute('SELECT COUNT(*) FROM retention_buckets').fetchone()[0])
print('Distinct windows:', list(c.execute('SELECT window_end - window_start AS span, COUNT(*) FROM retention_buckets GROUP BY span')))
"
```

Expected: total rows roughly equal to 3 × number of videos (some may be skipped if low-view); distinct windows shows three groups.

- [ ] **Step 7: Commit**

```bash
git add fetch_metrics.py tests/test_fetch_metrics.py
git commit -m "feat(fetch): write retention rolling windows for 7/90/365 days per video"
```

---

### Task 6: Add channel-wide "Audience Retention" KPI section to `app.py`

**Files:**
- Modify: `app.py` (insert new section between Watch Time and Per-Video Deep Dive)

- [ ] **Step 1: Add the imports**

At the top of `app.py`, add to the existing imports:

```python
from datetime import date, timedelta

import retention
```

- [ ] **Step 2: Add a loader for retention data**

After the existing `load(...)` calls (around line 91), add:

```python
retention_buckets = load(
    "SELECT video_id, window_start, window_end, window_kind, views, "
    "retention_at_25, retention_at_75 FROM retention_buckets"
)
```

- [ ] **Step 3: Add the new section**

Insert this block in `app.py` between the Watch Time section (ends after the `st.plotly_chart` for cumulative hours) and the `# --- Per-video deep dive ---` comment:

```python
# --- Audience retention ---

if not retention_buckets.empty:
    st.subheader("Audience Retention")
    st.caption(
        "Where viewers drop off as a share of each video's length, "
        "aggregated across all videos."
    )
    toggle = st.radio(
        "Range",
        list(RANGES.keys()),
        index=2,  # Last quarter
        horizontal=True,
        key="retention_range",
        label_visibility="collapsed",
    )

    today = date.today()
    snap_start, snap_end, rolling_days = retention.window_bounds_for_toggle(toggle, today)
    rolling_kind = f"rolling{rolling_days}"
    rolling_start = today - timedelta(days=rolling_days)

    rb = retention_buckets[retention_buckets["window_kind"] == rolling_kind].copy()
    rb["window_start"] = pd.to_datetime(rb["window_start"]).dt.date
    rb["window_end"] = pd.to_datetime(rb["window_end"]).dt.date
    rb = rb[(rb["window_start"] == rolling_start) & (rb["window_end"] == today)]

    if toggle == "Last month" and not rb.empty:
        # Override views with the actual 30-day sum from daily_video_metrics.
        dv = daily_videos.copy()
        dv["metric_date"] = pd.to_datetime(dv["metric_date"]).dt.date
        scoped = dv[(dv["metric_date"] >= snap_start) & (dv["metric_date"] <= snap_end)]
        views_30d = scoped.groupby("video_id")["views"].sum().to_dict()
        rb = rb.copy()
        rb["views"] = rb["video_id"].map(views_30d).fillna(0).astype(int)

    snap = retention.aggregate_snapshot(rb)

    if snap["total_views"] == 0:
        st.info("No retention data yet for this range. Run `python fetch_metrics.py`.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Dropped early (0–25%)",
                  f"{int(snap['b1_count']):,} views",
                  f"{snap['b1_pct'] * 100:.1f}%")
        c2.metric("Mid-watch (25–75%)",
                  f"{int(snap['b2_count']):,} views",
                  f"{snap['b2_pct'] * 100:.1f}%")
        c3.metric("Stuck around (75–100%)",
                  f"{int(snap['b3_count']):,} views",
                  f"{snap['b3_pct'] * 100:.1f}%")
else:
    st.subheader("Audience Retention")
    st.info("Retention data still loading. Run `python fetch_metrics.py` to populate.")
```

- [ ] **Step 4: Run the dashboard locally**

Run: `streamlit run app.py`
Open the browser tab Streamlit prints.

- [ ] **Step 5: Manual smoke test**

Verify in the browser:
- New "Audience Retention" section appears between "Watch Time" and "Per-Video Deep Dive".
- Three KPI cards render side-by-side, each with a count and a percentage.
- Toggling the range picker updates the numbers (each toggle should produce different counts).
- The total of the three counts approximately equals total views in the selected range (off by < 1% due to rounding).
- Stop the dashboard with Ctrl+C.

- [ ] **Step 6: Commit**

```bash
git add app.py
git commit -m "feat(ui): add channel-wide Audience Retention KPI cards"
```

---

### Task 7: Add per-video retention KPI cards to the deep-dive section

**Files:**
- Modify: `app.py` (extend the existing `# --- Per-video deep dive ---` block)

- [ ] **Step 1: Locate the deep-dive block**

Find the existing block beginning with `# --- Per-video deep dive ---` (around line 225). The current `if selected:` block ends with the per-fetch view totals chart.

- [ ] **Step 2: Add retention cards for the selected video**

Inside the `if selected:` block, after the existing two charts and before the closing of the `if selected:` block, add:

```python
        # Retention bucket cards for this video
        today_ = date.today()
        v_toggle = st.session_state.get("video_range", "Last quarter")
        v_snap_start, v_snap_end, v_rolling_days = retention.window_bounds_for_toggle(
            v_toggle, today_
        )
        v_rolling_kind = f"rolling{v_rolling_days}"
        v_rolling_start = today_ - timedelta(days=v_rolling_days)

        rb_one = retention_buckets[
            (retention_buckets["video_id"] == selected)
            & (retention_buckets["window_kind"] == v_rolling_kind)
        ].copy()
        if not rb_one.empty:
            rb_one["window_start"] = pd.to_datetime(rb_one["window_start"]).dt.date
            rb_one["window_end"] = pd.to_datetime(rb_one["window_end"]).dt.date
            rb_one = rb_one[
                (rb_one["window_start"] == v_rolling_start)
                & (rb_one["window_end"] == today_)
            ]
            if v_toggle == "Last month" and not rb_one.empty:
                dv_one = daily_videos[daily_videos["video_id"] == selected].copy()
                dv_one["metric_date"] = pd.to_datetime(dv_one["metric_date"]).dt.date
                scoped = dv_one[
                    (dv_one["metric_date"] >= v_snap_start)
                    & (dv_one["metric_date"] <= v_snap_end)
                ]
                rb_one = rb_one.copy()
                rb_one["views"] = int(scoped["views"].sum())

            v_snap = retention.aggregate_snapshot(rb_one)
            if v_snap["total_views"] > 0:
                st.markdown("**Retention buckets for this video**")
                c1, c2, c3 = st.columns(3)
                c1.metric("Dropped early (0–25%)",
                          f"{int(v_snap['b1_count']):,} views",
                          f"{v_snap['b1_pct'] * 100:.1f}%")
                c2.metric("Mid-watch (25–75%)",
                          f"{int(v_snap['b2_count']):,} views",
                          f"{v_snap['b2_pct'] * 100:.1f}%")
                c3.metric("Stuck around (75–100%)",
                          f"{int(v_snap['b3_count']):,} views",
                          f"{v_snap['b3_pct'] * 100:.1f}%")
            else:
                st.info("No retention data for this video in the selected range.")
        else:
            st.info("No retention data captured for this video yet.")
```

- [ ] **Step 3: Run the dashboard**

Run: `streamlit run app.py`

- [ ] **Step 4: Manual smoke test**

- Pick a video from the deep-dive selector.
- Confirm the three KPI cards appear below the existing per-video charts.
- Toggle the range picker — counts should update.
- Pick a different video — the cards should re-render with that video's numbers.
- Stop with Ctrl+C.

- [ ] **Step 5: Commit**

```bash
git add app.py
git commit -m "feat(ui): add per-video retention KPI cards to deep dive"
```

**Phase 1 milestone:** Snapshot KPI cards work for both channel-wide and per-video. The dashboard ships with no backfill; the next scheduled `fetch_metrics.py` run populates the table.

---

## Phase 2 — Trend Stacked-Bar Charts

### Task 8: Extend `fetch_metrics.py` with weekly window writes

**Files:**
- Modify: `fetch_metrics.py`
- Modify: `tests/test_fetch_metrics.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_fetch_metrics.py`:

```python
def test_write_retention_weekly_window_writes_one_row_per_video():
    """The latest ISO week's row gets written for each video."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "INSERT INTO videos(video_id, title, published_at, duration_seconds) "
                    "VALUES ('v1', 'V1', '2026-01-01T00:00:00Z', 600)"
                )
                for d in range(0, 14):
                    conn.execute(
                        "INSERT INTO daily_video_metrics(metric_date, video_id, views, "
                        "estimated_minutes_watched, average_view_duration, likes, "
                        "subscribers_gained) VALUES (?, 'v1', 5, 0, 0, 0, 0)",
                        ((date.today().fromordinal(date.today().toordinal() - d)).isoformat(),),
                    )

            from fetch_metrics import write_retention_weekly_window

            def fake_curve(video_id, start, end):
                return {
                    "video_id": video_id,
                    "window_start": start.isoformat(),
                    "window_end": end.isoformat(),
                    "retention_at_25": 0.7,
                    "retention_at_75": 0.4,
                }

            with patch("fetch_metrics.fetch_retention_curve", side_effect=fake_curve):
                # Pick a known week: Mon 2026-04-27 → Sun 2026-05-03 (today fake = 2026-05-02)
                write_retention_weekly_window(["v1"], today=date(2026, 5, 2))

            with sqlite3.connect(db_path) as conn:
                rows = list(conn.execute(
                    "SELECT window_start, window_end, window_kind FROM retention_buckets"
                ))
                assert len(rows) == 1
                # ISO week containing 2026-05-02 (Saturday): Mon 2026-04-27 → Sun 2026-05-03
                assert rows[0] == ("2026-04-27", "2026-05-03", "weekly")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_fetch_metrics.py::test_write_retention_weekly_window_writes_one_row_per_video -v`
Expected: FAIL with `ImportError: cannot import name 'write_retention_weekly_window'`.

- [ ] **Step 3: Implement the weekly writer**

Add to `fetch_metrics.py`:

```python
def _iso_week_bounds(today: date) -> tuple[date, date]:
    """Return (Monday, Sunday) for the ISO week containing `today`."""
    monday = today - timedelta(days=today.weekday())
    sunday = monday + timedelta(days=6)
    return monday, sunday


def write_retention_weekly_window(video_ids: list[str], today: date | None = None) -> None:
    """Fetch retention for the ISO week containing `today` and persist one row per video."""
    today = today or date.today()
    start, end = _iso_week_bounds(today)
    fetched_at = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        for vid in video_ids:
            curve = fetch_retention_curve(vid, start, end)
            if curve is None:
                continue
            views = conn.execute(
                "SELECT COALESCE(SUM(views), 0) FROM daily_video_metrics "
                "WHERE video_id = ? AND metric_date BETWEEN ? AND ?",
                (vid, start.isoformat(), end.isoformat()),
            ).fetchone()[0]
            conn.execute(
                "INSERT INTO retention_buckets(video_id, window_start, window_end, "
                "window_kind, views, retention_at_25, retention_at_75, fetched_at) "
                "VALUES (?, ?, ?, 'weekly', ?, ?, ?, ?) "
                "ON CONFLICT(video_id, window_start, window_end, window_kind) DO UPDATE SET "
                "views=excluded.views, retention_at_25=excluded.retention_at_25, "
                "retention_at_75=excluded.retention_at_75, fetched_at=excluded.fetched_at",
                (vid, start.isoformat(), end.isoformat(),
                 int(views), curve["retention_at_25"], curve["retention_at_75"],
                 fetched_at),
            )
```

Then call it in `main()`, right after the `write_retention_rolling_windows(...)` call:

```python
    print("Fetching retention for the current ISO week...")
    write_retention_weekly_window([v["video_id"] for v in videos])
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/ -v`
Expected: all tests pass including the new weekly one.

- [ ] **Step 5: Commit**

```bash
git add fetch_metrics.py tests/test_fetch_metrics.py
git commit -m "feat(fetch): write current ISO-week retention row on each fetch run"
```

---

### Task 9: Backfill script for the past 52 weeks

**Files:**
- Create: `scripts/backfill_retention.py`
- Modify: `tests/test_fetch_metrics.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_fetch_metrics.py`:

```python
def test_backfill_retention_resumable():
    """Existing rows are skipped on re-run."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "INSERT INTO videos(video_id, title, published_at, duration_seconds) "
                    "VALUES ('v1', 'V1', '2026-01-01T00:00:00Z', 600)"
                )

            calls = []

            def fake_curve(video_id, start, end):
                calls.append((video_id, start, end))
                return {
                    "video_id": video_id,
                    "window_start": start.isoformat(),
                    "window_end": end.isoformat(),
                    "retention_at_25": 0.7,
                    "retention_at_75": 0.4,
                }

            import sys
            sys.path.insert(0, "scripts")
            try:
                with patch("youtube_client.fetch_retention_curve", side_effect=fake_curve):
                    import importlib
                    import backfill_retention
                    importlib.reload(backfill_retention)
                    backfill_retention.backfill(["v1"], weeks=3, today=date(2026, 5, 2))
                    first_pass_calls = len(calls)
                    backfill_retention.backfill(["v1"], weeks=3, today=date(2026, 5, 2))
                    second_pass_calls = len(calls) - first_pass_calls
            finally:
                sys.path.remove("scripts")

            # First pass: 3 weeks of API calls. Second pass: 0 (all rows exist).
            assert first_pass_calls == 3
            assert second_pass_calls == 0

            with sqlite3.connect(db_path) as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM retention_buckets"
                ).fetchone()[0]
                assert count == 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_fetch_metrics.py::test_backfill_retention_resumable -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'backfill_retention'`.

- [ ] **Step 3: Implement the backfill script**

Create `scripts/backfill_retention.py`:

```python
"""Backfill retention_buckets with weekly rows for the past N weeks per video.

Resumable: rows already present (matching video_id + window_start + window_end)
are skipped without an API call.

Usage:  python scripts/backfill_retention.py [--weeks 52]
"""

import argparse
import os
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# Make the project root importable when this runs as `python scripts/backfill_retention.py`
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db import DB_PATH, get_conn, init_db  # noqa: E402
from youtube_client import fetch_retention_curve  # noqa: E402


def _iso_week_bounds(d: date) -> tuple[date, date]:
    monday = d - timedelta(days=d.weekday())
    sunday = monday + timedelta(days=6)
    return monday, sunday


def backfill(video_ids: list[str], weeks: int, today: date | None = None) -> None:
    today = today or date.today()
    fetched_at = datetime.now(timezone.utc).isoformat()
    init_db()

    with get_conn() as conn:
        for vid in video_ids:
            for w in range(weeks):
                ref = today - timedelta(weeks=w)
                start, end = _iso_week_bounds(ref)
                exists = conn.execute(
                    "SELECT 1 FROM retention_buckets "
                    "WHERE video_id = ? AND window_start = ? AND window_end = ? "
                    "AND window_kind = 'weekly'",
                    (vid, start.isoformat(), end.isoformat()),
                ).fetchone()
                if exists:
                    continue
                curve = fetch_retention_curve(vid, start, end)
                if curve is None:
                    continue
                views = conn.execute(
                    "SELECT COALESCE(SUM(views), 0) FROM daily_video_metrics "
                    "WHERE video_id = ? AND metric_date BETWEEN ? AND ?",
                    (vid, start.isoformat(), end.isoformat()),
                ).fetchone()[0]
                conn.execute(
                    "INSERT INTO retention_buckets(video_id, window_start, window_end, "
                    "window_kind, views, retention_at_25, retention_at_75, fetched_at) "
                    "VALUES (?, ?, ?, 'weekly', ?, ?, ?, ?)",
                    (vid, start.isoformat(), end.isoformat(),
                     int(views), curve["retention_at_25"], curve["retention_at_75"],
                     fetched_at),
                )
                print(f"  {vid} {start} → {end}: r25={curve['retention_at_25']:.2f} "
                      f"r75={curve['retention_at_75']:.2f}")


def _all_video_ids_from_db() -> list[str]:
    with sqlite3.connect(DB_PATH) as conn:
        return [r[0] for r in conn.execute("SELECT video_id FROM videos")]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--weeks", type=int, default=52)
    args = parser.parse_args()
    video_ids = _all_video_ids_from_db()
    if not video_ids:
        print("No videos in DB. Run fetch_metrics.py first.")
        return
    print(f"Backfilling {args.weeks} weeks for {len(video_ids)} videos...")
    backfill(video_ids, weeks=args.weeks)
    print("Done.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/ -v`
Expected: all tests pass including resumability.

- [ ] **Step 5: Run the actual backfill**

Run: `python scripts/backfill_retention.py --weeks 52`
Expected: prints progress for each (video, week) it fetches; takes ~10–30 min depending on channel size.

- [ ] **Step 6: Verify rows landed**

Run:

```bash
python -c "
import sqlite3
c = sqlite3.connect('data.db')
print('Total rows:', c.execute('SELECT COUNT(*) FROM retention_buckets').fetchone()[0])
print('Distinct weeks:', c.execute('SELECT COUNT(DISTINCT window_start) FROM retention_buckets WHERE window_end - window_start <= 7').fetchone()[0])
"
```

Expected: total rows in the thousands; distinct weeks ≈ 52 (some weeks may be missing if videos didn't exist yet).

- [ ] **Step 7: Commit**

```bash
git add scripts/backfill_retention.py tests/test_fetch_metrics.py
git commit -m "feat(scripts): add resumable 52-week retention backfill"
```

---

### Task 10: Add trend-bin aggregation to `retention.py`

**Files:**
- Modify: `retention.py`
- Modify: `tests/test_retention.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_retention.py`:

```python
def test_bin_granularity_for_toggle():
    assert retention.bin_granularity_for_toggle("Last week") == "D"
    assert retention.bin_granularity_for_toggle("Last month") == "D"
    assert retention.bin_granularity_for_toggle("Last quarter") == "W"
    assert retention.bin_granularity_for_toggle("Last year") == "MS"


def test_aggregate_trend_bins_weekly():
    """Two weekly retention rows + matching daily views → weekly trend bins."""
    weekly_rb = pd.DataFrame([
        {"video_id": "v1", "window_start": "2026-04-20", "window_end": "2026-04-26",
         "retention_at_25": 0.6, "retention_at_75": 0.3},
        {"video_id": "v1", "window_start": "2026-04-27", "window_end": "2026-05-03",
         "retention_at_25": 0.8, "retention_at_75": 0.5},
    ])
    daily_views = pd.DataFrame([
        {"video_id": "v1", "metric_date": "2026-04-22", "views": 100},
        {"video_id": "v1", "metric_date": "2026-04-29", "views": 200},
    ])
    bins = retention.aggregate_trend_bins(
        weekly_rb, daily_views,
        start=pd.Timestamp("2026-04-20").date(),
        end=pd.Timestamp("2026-05-03").date(),
        granularity="W",
    )
    # Two bins; b3 of week 1 = 100 * 0.3 = 30, b3 of week 2 = 200 * 0.5 = 100
    assert len(bins) == 2
    assert bins.iloc[0]["b3_count"] == pytest.approx(30.0)
    assert bins.iloc[1]["b3_count"] == pytest.approx(100.0)


def test_aggregate_trend_bins_daily_uses_weekly_ratios():
    """Daily bins inherit retention ratios from the containing week."""
    weekly_rb = pd.DataFrame([
        {"video_id": "v1", "window_start": "2026-04-27", "window_end": "2026-05-03",
         "retention_at_25": 0.8, "retention_at_75": 0.5},
    ])
    daily_views = pd.DataFrame([
        {"video_id": "v1", "metric_date": "2026-04-29", "views": 50},
        {"video_id": "v1", "metric_date": "2026-04-30", "views": 70},
    ])
    bins = retention.aggregate_trend_bins(
        weekly_rb, daily_views,
        start=pd.Timestamp("2026-04-29").date(),
        end=pd.Timestamp("2026-04-30").date(),
        granularity="D",
    )
    assert len(bins) == 2
    # Day 1: 50 * 0.5 = 25; Day 2: 70 * 0.5 = 35
    assert bins.iloc[0]["b3_count"] == pytest.approx(25.0)
    assert bins.iloc[1]["b3_count"] == pytest.approx(35.0)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_retention.py -v`
Expected: FAIL with `AttributeError: module 'retention' has no attribute 'bin_granularity_for_toggle'`.

- [ ] **Step 3: Implement the trend functions**

Append to `retention.py`:

```python
_BIN_GRANULARITY = {
    "Last week":    "D",   # 7 daily bars
    "Last month":   "D",   # ~30 daily bars
    "Last quarter": "W",   # ~13 weekly bars
    "Last year":    "MS",  # 12 monthly bars (month-start)
}


def bin_granularity_for_toggle(toggle: str) -> str:
    """Return a pandas offset alias for the bin size of this toggle."""
    if toggle not in _BIN_GRANULARITY:
        raise ValueError(f"Unknown toggle: {toggle}")
    return _BIN_GRANULARITY[toggle]


def aggregate_trend_bins(
    weekly_rows: pd.DataFrame,
    daily_views: pd.DataFrame,
    start: date,
    end: date,
    granularity: str,
) -> pd.DataFrame:
    """Build a per-bin (date, b1_count, b2_count, b3_count) frame.

    Strategy: each daily view row inherits its week's retention ratios,
    then we multiply views by bucket shares and resample to the chosen granularity.
    """
    if weekly_rows.empty or daily_views.empty:
        return pd.DataFrame(columns=["bin", "b1_count", "b2_count", "b3_count"])

    w = weekly_rows.copy()
    w["week_start"] = pd.to_datetime(w["window_start"]).dt.date
    w["week_end"] = pd.to_datetime(w["window_end"]).dt.date

    dv = daily_views.copy()
    dv["metric_date"] = pd.to_datetime(dv["metric_date"]).dt.date
    dv = dv[(dv["metric_date"] >= start) & (dv["metric_date"] <= end)]
    if dv.empty:
        return pd.DataFrame(columns=["bin", "b1_count", "b2_count", "b3_count"])

    def lookup_week(video_id, day):
        match = w[
            (w["video_id"] == video_id)
            & (w["week_start"] <= day)
            & (w["week_end"] >= day)
        ]
        if match.empty:
            return (None, None)
        row = match.iloc[0]
        return (row["retention_at_25"], row["retention_at_75"])

    ratios = [lookup_week(r.video_id, r.metric_date) for r in dv.itertuples(index=False)]
    dv["r25"] = [r[0] for r in ratios]
    dv["r75"] = [r[1] for r in ratios]
    dv = dv.dropna(subset=["r25", "r75"])

    shares = dv.apply(lambda r: bucket_shares(r["r25"], r["r75"]),
                      axis=1, result_type="expand")
    shares.columns = ["s1", "s2", "s3"]
    dv["b1_count"] = shares["s1"] * dv["views"]
    dv["b2_count"] = shares["s2"] * dv["views"]
    dv["b3_count"] = shares["s3"] * dv["views"]

    dv["metric_date"] = pd.to_datetime(dv["metric_date"])
    binned = (
        dv.set_index("metric_date")[["b1_count", "b2_count", "b3_count"]]
        .resample(granularity).sum()
        .reset_index()
        .rename(columns={"metric_date": "bin"})
    )
    return binned
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_retention.py -v`
Expected: all tests pass (the original 6 plus 3 new = 9).

- [ ] **Step 5: Commit**

```bash
git add retention.py tests/test_retention.py
git commit -m "feat: add trend-bin aggregation with per-week ratio inheritance"
```

---

### Task 11: Add channel-wide stacked-bar trend chart to `app.py`

**Files:**
- Modify: `app.py` (extend the Audience Retention section)

- [ ] **Step 1: Locate the Audience Retention section**

Find the block beginning with `# --- Audience retention ---`. The KPI cards from Task 6 are inside the `if snap["total_views"] == 0:` else branch.

- [ ] **Step 2: Add the stacked bar chart**

In the else branch (after the three `c1.metric / c2.metric / c3.metric` calls), append:

```python
        # Trend stacked bar — uses only weekly rows
        granularity = retention.bin_granularity_for_toggle(toggle)
        weekly_rb = retention_buckets[
            retention_buckets["window_kind"] == "weekly"
        ].copy()
        bins = retention.aggregate_trend_bins(
            weekly_rb, daily_videos,
            start=snap_start, end=snap_end, granularity=granularity,
        )
        if not bins.empty:
            fig = go.Figure()
            fig.add_bar(x=bins["bin"], y=bins["b1_count"],
                        name="Dropped early (0–25%)", marker_color="#E45756")
            fig.add_bar(x=bins["bin"], y=bins["b2_count"],
                        name="Mid-watch (25–75%)", marker_color="#F2B701")
            fig.add_bar(x=bins["bin"], y=bins["b3_count"],
                        name="Stuck around (75–100%)", marker_color="#54A24B")
            fig.update_layout(
                barmode="stack",
                title="Retention bucket counts over time",
                xaxis_title="Date", yaxis_title="Views",
                legend=dict(orientation="h", yanchor="bottom", y=1.02,
                            xanchor="right", x=1),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.caption("Not enough weekly retention data yet to draw the trend.")
```

- [ ] **Step 3: Run the dashboard**

Run: `streamlit run app.py`

- [ ] **Step 4: Manual smoke test**

- "Audience Retention" section shows KPI cards on top and a stacked bar chart below.
- Toggling to "Last week" shows ~7 daily bars; "Last quarter" shows ~13 weekly bars; "Last year" shows ~12 monthly bars.
- Each bar has three colored segments (red / amber / green); legend labels match.
- Hovering shows tooltips with the bucket name and count.
- Stop with Ctrl+C.

- [ ] **Step 5: Commit**

```bash
git add app.py
git commit -m "feat(ui): add channel-wide retention trend stacked bar chart"
```

---

### Task 12: Add per-video trend chart to deep-dive section

**Files:**
- Modify: `app.py` (extend the per-video deep-dive block)

- [ ] **Step 1: Locate the deep-dive retention section**

Find the block from Task 7 ending with the `c3.metric("Stuck around (75–100%)", ...)` call inside `if v_snap["total_views"] > 0:`.

- [ ] **Step 2: Add the trend chart for the selected video**

After the three `c1/c2/c3.metric` calls and before the `else: st.info("No retention data...")`, append:

```python
                v_granularity = retention.bin_granularity_for_toggle(v_toggle)
                v_weekly_rb = retention_buckets[
                    (retention_buckets["video_id"] == selected)
                    & (retention_buckets["window_kind"] == "weekly")
                ].copy()
                v_dv = daily_videos[daily_videos["video_id"] == selected]
                v_bins = retention.aggregate_trend_bins(
                    v_weekly_rb, v_dv,
                    start=v_snap_start, end=v_snap_end, granularity=v_granularity,
                )
                if not v_bins.empty:
                    fig = go.Figure()
                    fig.add_bar(x=v_bins["bin"], y=v_bins["b1_count"],
                                name="Dropped early (0–25%)", marker_color="#E45756")
                    fig.add_bar(x=v_bins["bin"], y=v_bins["b2_count"],
                                name="Mid-watch (25–75%)", marker_color="#F2B701")
                    fig.add_bar(x=v_bins["bin"], y=v_bins["b3_count"],
                                name="Stuck around (75–100%)", marker_color="#54A24B")
                    fig.update_layout(
                        barmode="stack",
                        title=f"Retention over time — {titles[selected]}",
                        xaxis_title="Date", yaxis_title="Views",
                        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                                    xanchor="right", x=1),
                    )
                    st.plotly_chart(fig, use_container_width=True)
```

- [ ] **Step 3: Run the dashboard**

Run: `streamlit run app.py`

- [ ] **Step 4: Manual smoke test**

- Select a video in the deep-dive section.
- KPI cards still render (from Task 7).
- Below them, a stacked bar trend chart appears for the selected video.
- Switching the deep-dive range picker changes bin granularity.
- Switching to a different video re-renders both cards and chart.
- Stop with Ctrl+C.

- [ ] **Step 5: Commit**

```bash
git add app.py
git commit -m "feat(ui): add per-video retention trend stacked bar chart"
```

**Phase 2 milestone:** Trend charts work in both sections. The full feature is shipped.

---

## Verification Checklist

Run before declaring done:

- [ ] `pytest tests/ -v` — all tests pass.
- [ ] `streamlit run app.py` — dashboard loads with no errors in the terminal.
- [ ] Channel-wide "Audience Retention" section shows three KPI cards + stacked trend chart.
- [ ] Per-video deep-dive section shows the same when a video is selected.
- [ ] All four range-picker toggles (1W / 1M / 1Q / 1Y) update the numbers and the chart bin granularity.
- [ ] No video appears in two buckets at once (sanity check on math: bucket counts sum to total views ± rounding).
