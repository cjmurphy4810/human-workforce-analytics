# News-Timed Release Impact Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a "News-Timed Release Impact" dashboard section that tracks podcast episodes that graduated from the Publishing Queue and went live within the ±72–96h recommended timing window, then compares their early performance across views, hours watched, subscriber gain, and audience retention.

**Architecture:** A new `queue_recommendations` table (keyed on `video_id`, INSERT OR IGNORE) records the first time each video appears in the Publishing Queue. `fetch_metrics.py` writes to it after every successful publishing queue analysis. `app.py` joins this table with `videos` and `daily_video_metrics` at render time to compute the eligible cohort, timing delta, normalized day-offset trajectories, and a retention comparison.

**Tech Stack:** Python 3.12, SQLite, Streamlit, Plotly, pandas. No new dependencies.

---

## File Map

| File | Change |
|---|---|
| `db.py` | Add `queue_recommendations` DDL to `SCHEMA` |
| `fetch_metrics.py` | Add `write_queue_recommendations(ranked_videos, cron_date)` function; update `write_publishing_queue` to return `result`; call `write_queue_recommendations` from `main()` |
| `app.py` | Load eligible cohort; add "News-Timed Release Impact" section with scorecard table, trajectory charts, retention bar chart |
| `tests/test_db.py` | Add `test_queue_recommendations_table_created` |
| `tests/test_fetch_metrics.py` | Add three tests for `write_queue_recommendations` |

---

## Task 1: Add `queue_recommendations` table to `db.py`

**Files:**
- Modify: `db.py`
- Test: `tests/test_db.py`

- [ ] **Step 1: Write the failing test**

Open `tests/test_db.py` and add at the bottom:

```python
def test_queue_recommendations_table_created():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            with sqlite3.connect(db_path) as conn:
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='queue_recommendations'"
                )
                assert cursor.fetchone() is not None


def test_queue_recommendations_insert_or_ignore():
    """Inserting the same video_id twice must result in exactly one row."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            with sqlite3.connect(db_path) as conn:
                for _ in range(2):
                    conn.execute(
                        "INSERT OR IGNORE INTO queue_recommendations "
                        "(video_id, first_recommended_at, recommended_publish_date, "
                        "rank_at_recommendation, relevance_score, theme, why_now) "
                        "VALUES ('v1', '2026-06-14T10:00:00Z', '2026-06-15', 1, 8.5, 'AI', 'Timely.')"
                    )
                count = conn.execute(
                    "SELECT COUNT(*) FROM queue_recommendations"
                ).fetchone()[0]
                assert count == 1
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd "/Users/zdjimas/VS Code Projects/human-workforce-analytics"
.venv/bin/python -m pytest tests/test_db.py::test_queue_recommendations_table_created tests/test_db.py::test_queue_recommendations_insert_or_ignore -v
```

Expected: FAIL with `OperationalError: no such table: queue_recommendations`

- [ ] **Step 3: Add DDL to `db.py`**

In `db.py`, find the end of the `SCHEMA` string (just before the closing `"""`). Add the new table after the `playlist_videos` table DDL:

```python
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

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/python -m pytest tests/test_db.py::test_queue_recommendations_table_created tests/test_db.py::test_queue_recommendations_insert_or_ignore -v
```

Expected: PASS

- [ ] **Step 5: Run full test suite to check for regressions**

```bash
.venv/bin/python -m pytest tests/ -v
```

Expected: all previously passing tests still pass.

- [ ] **Step 6: Commit**

```bash
git add db.py tests/test_db.py
git commit -m "feat: add queue_recommendations table to schema"
```

---

## Task 2: Add `write_queue_recommendations()` to `fetch_metrics.py`

**Files:**
- Modify: `fetch_metrics.py`
- Test: `tests/test_fetch_metrics.py`

- [ ] **Step 1: Write the failing tests**

Open `tests/test_fetch_metrics.py` and add at the bottom:

```python
# --- write_queue_recommendations tests ---

def test_write_queue_recommendations_inserts_first_occurrence():
    """Happy path: one ranked video → one row in queue_recommendations."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db as db_module
            db_module.init_db()
            from fetch_metrics import write_queue_recommendations
            ranked = [
                {
                    "rank": 1,
                    "video_id": "v1",
                    "title": "AI Episode",
                    "theme": "AI workforce",
                    "relevance_score": 9.0,
                    "why_now": "Major AI news today.",
                }
            ]
            write_queue_recommendations(ranked, date(2026, 6, 14))
            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    "SELECT video_id, recommended_publish_date, rank_at_recommendation, relevance_score "
                    "FROM queue_recommendations WHERE video_id = 'v1'"
                ).fetchone()
                assert row is not None
                assert row[0] == "v1"
                assert row[1] == "2026-06-15"   # cron_date + 1 day (rank=1)
                assert row[2] == 1
                assert row[3] == 9.0


def test_write_queue_recommendations_ignores_duplicate_video_id():
    """Calling write_queue_recommendations twice with the same video_id keeps only the first row."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db as db_module
            db_module.init_db()
            from fetch_metrics import write_queue_recommendations
            video = [{"rank": 1, "video_id": "v1", "title": "T", "theme": "AI",
                      "relevance_score": 8.0, "why_now": "First time."}]
            write_queue_recommendations(video, date(2026, 6, 14))
            # Second call simulates next cron run with same video at different rank
            video2 = [{"rank": 3, "video_id": "v1", "title": "T", "theme": "AI",
                       "relevance_score": 5.0, "why_now": "Second time."}]
            write_queue_recommendations(video2, date(2026, 6, 15))
            with sqlite3.connect(db_path) as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM queue_recommendations"
                ).fetchone()[0]
                assert count == 1
                # First insertion's values must be preserved
                row = conn.execute(
                    "SELECT rank_at_recommendation, relevance_score, recommended_publish_date "
                    "FROM queue_recommendations WHERE video_id = 'v1'"
                ).fetchone()
                assert row[0] == 1
                assert row[1] == 8.0
                assert row[2] == "2026-06-15"


def test_write_queue_recommendations_noop_when_empty():
    """Calling with an empty list writes nothing and does not error."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db as db_module
            db_module.init_db()
            from fetch_metrics import write_queue_recommendations
            write_queue_recommendations([], date(2026, 6, 14))
            with sqlite3.connect(db_path) as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM queue_recommendations"
                ).fetchone()[0]
                assert count == 0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/python -m pytest tests/test_fetch_metrics.py::test_write_queue_recommendations_inserts_first_occurrence tests/test_fetch_metrics.py::test_write_queue_recommendations_ignores_duplicate_video_id tests/test_fetch_metrics.py::test_write_queue_recommendations_noop_when_empty -v
```

Expected: FAIL with `ImportError` or `AttributeError` (function does not exist yet).

- [ ] **Step 3: Implement `write_queue_recommendations` in `fetch_metrics.py`**

In `fetch_metrics.py`, add this function after `write_publishing_queue` (around line 124):

```python
def write_queue_recommendations(ranked_videos: list[dict], cron_date: date) -> None:
    """Persist first-time queue appearances to queue_recommendations (INSERT OR IGNORE)."""
    if not ranked_videos:
        return
    first_recommended_at = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        for item in ranked_videos:
            rank = item.get("rank", 0)
            recommended_publish_date = (cron_date + timedelta(days=int(rank))).isoformat()
            conn.execute(
                "INSERT OR IGNORE INTO queue_recommendations "
                "(video_id, first_recommended_at, recommended_publish_date, "
                "rank_at_recommendation, relevance_score, theme, why_now) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    item.get("video_id"),
                    first_recommended_at,
                    recommended_publish_date,
                    int(rank),
                    float(item.get("relevance_score", 0)),
                    item.get("theme"),
                    item.get("why_now"),
                ),
            )
    print(f"  Queue recommendations: {len(ranked_videos)} videos processed (INSERT OR IGNORE).")
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/python -m pytest tests/test_fetch_metrics.py::test_write_queue_recommendations_inserts_first_occurrence tests/test_fetch_metrics.py::test_write_queue_recommendations_ignores_duplicate_video_id tests/test_fetch_metrics.py::test_write_queue_recommendations_noop_when_empty -v
```

Expected: PASS

- [ ] **Step 5: Run full test suite**

```bash
.venv/bin/python -m pytest tests/ -v
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add fetch_metrics.py tests/test_fetch_metrics.py
git commit -m "feat: add write_queue_recommendations to fetch_metrics"
```

---

## Task 3: Wire `write_queue_recommendations` into `main()`

**Files:**
- Modify: `fetch_metrics.py` only

This task has two changes: (a) make `write_publishing_queue` return its `result` dict so `main()` can pass `ranked_videos` to `write_queue_recommendations` without a DB round-trip, and (b) call `write_queue_recommendations` from `main()`.

- [ ] **Step 1: Update `write_publishing_queue` return value**

In `fetch_metrics.py`, find the last line of `write_publishing_queue`:

```python
    print(f"  Publishing queue written: {len(ranked)} videos ranked against {len(headlines)} headlines.")
```

Change it to:

```python
    print(f"  Publishing queue written: {len(ranked)} videos ranked against {len(headlines)} headlines.")
    return result
```

The function currently returns `None` implicitly at the two early-return branches (`no unpublished videos` and `no API key`). Those are fine as-is — `main()` will treat `None` as a skip signal.

- [ ] **Step 2: Verify existing `write_publishing_queue` tests still pass** (return value change is backward-compatible)

```bash
.venv/bin/python -m pytest tests/test_fetch_metrics.py -k "publishing_queue" -v
```

Expected: all four existing tests pass.

- [ ] **Step 3: Update `main()` to call `write_queue_recommendations`**

In `fetch_metrics.py`, find this block in `main()`:

```python
    print("Analyzing publishing queue...")
    try:
        write_publishing_queue(videos)
    except Exception as e:
        print(f"  Publishing queue failed ({e.__class__.__name__}), skipping.")
```

Replace it with:

```python
    print("Analyzing publishing queue...")
    pq_result = None
    try:
        pq_result = write_publishing_queue(videos)
    except Exception as e:
        print(f"  Publishing queue failed ({e.__class__.__name__}), skipping.")

    print("Writing queue recommendations...")
    try:
        ranked_for_recs = pq_result.get("ranked_videos", []) if pq_result else []
        write_queue_recommendations(ranked_for_recs, date.today())
    except Exception as e:
        print(f"  Queue recommendations write failed ({e.__class__.__name__}), skipping.")
```

- [ ] **Step 4: Run full test suite**

```bash
.venv/bin/python -m pytest tests/ -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add fetch_metrics.py
git commit -m "feat: wire write_queue_recommendations into main cron pipeline"
```

---

## Task 4: Add News-Timed Release Impact section to `app.py`

**Files:**
- Modify: `app.py` only

This is one task because all the rendering logic is tightly coupled (eligibility → scorecard → trajectories → retention). No new test file — visual verification only.

- [ ] **Step 1: Add the cohort data load at the top of `app.py`**

Find the block of `load()` calls at the top of `app.py` (lines 91–127, just before the `if channel_snapshots.empty:` guard). Add these two new loads after the existing `playlist_videos_df` load:

```python
queue_recommendations_df = load(
    "SELECT qr.video_id, qr.first_recommended_at, qr.recommended_publish_date, "
    "qr.rank_at_recommendation, qr.relevance_score, qr.theme, "
    "v.title, v.published_at, "
    "COUNT(dvm.metric_date) AS data_days "
    "FROM queue_recommendations qr "
    "JOIN videos v ON qr.video_id = v.video_id "
    "LEFT JOIN daily_video_metrics dvm "
    "  ON dvm.video_id = qr.video_id "
    "  AND dvm.metric_date >= date(v.published_at) "
    "WHERE v.published_at IS NOT NULL "
    "GROUP BY qr.video_id "
    "HAVING COUNT(dvm.metric_date) >= 3"
)
cohort_daily_metrics = load(
    "SELECT metric_date, video_id, views, estimated_minutes_watched, subscribers_gained "
    "FROM daily_video_metrics"
)
```

- [ ] **Step 2: Add the section at the bottom of `app.py`**

After the closing `if headlines:` block of the existing Publishing Queue section (the very last line of the file), add the following. Paste the entire block as-is:

```python

# --- News-Timed Release Impact ---

st.subheader("News-Timed Release Impact")
st.caption(
    "Episodes that graduated from the Publishing Queue and went live within the "
    "±72–96h recommended timing window. Shows whether publishing on time with "
    "the news cycle drives stronger early performance."
)

if queue_recommendations_df.empty:
    st.info(
        "No episodes have graduated from the queue within the timing window yet. "
        "This section populates once a recommended episode goes live and accumulates "
        "3 days of data."
    )
else:
    cohort = queue_recommendations_df.copy()

    # Compute timing delta in hours
    cohort["published_at_dt"] = pd.to_datetime(cohort["published_at"]).dt.tz_localize(None)
    cohort["recommended_dt"] = pd.to_datetime(cohort["recommended_publish_date"])
    cohort["timing_hours"] = (
        (cohort["published_at_dt"] - cohort["recommended_dt"]).dt.total_seconds() / 3600
    )

    # Apply timing window: [-72h, +96h]
    cohort = cohort[
        (cohort["timing_hours"] >= -72) & (cohort["timing_hours"] <= 96)
    ].copy()

    if cohort.empty:
        st.info(
            "No episodes have graduated from the queue within the timing window yet. "
            "This section populates once a recommended episode goes live and accumulates "
            "3 days of data."
        )
    else:
        eligible_ids = cohort["video_id"].tolist()
        cohort["days_live"] = (
            (pd.Timestamp.utcnow().tz_localize(None) - cohort["published_at_dt"]).dt.days
        )

        # --- Aggregate totals from daily metrics ---
        cm = cohort_daily_metrics[cohort_daily_metrics["video_id"].isin(eligible_ids)].copy()
        pub_map = cohort.set_index("video_id")["published_at_dt"].to_dict()
        cm["published_at_dt"] = cm["video_id"].map(pub_map)
        cm["metric_date_dt"] = pd.to_datetime(cm["metric_date"]).dt.tz_localize(None)
        cm["day_offset"] = (cm["metric_date_dt"] - cm["published_at_dt"]).dt.days + 1
        cm = cm[cm["day_offset"] >= 1]

        totals = cm.groupby("video_id").agg(
            total_views=("views", "sum"),
            total_hours=("estimated_minutes_watched", lambda x: x.sum() / 60),
            total_subs=("subscribers_gained", "sum"),
        ).reset_index()
        cohort = cohort.merge(totals, on="video_id", how="left")

        # --- Retention depth ---
        ret = retention_buckets[retention_buckets["video_id"].isin(eligible_ids)].copy()
        if not ret.empty:
            ret["window_end"] = pd.to_datetime(ret["window_end"])
            latest_ret = ret.sort_values("window_end").groupby("video_id").last().reset_index()[
                ["video_id", "retention_at_25", "retention_at_75"]
            ]
            cohort = cohort.merge(latest_ret, on="video_id", how="left")
        else:
            cohort["retention_at_25"] = None
            cohort["retention_at_75"] = None

        # Sort by timing delta ascending (closest to recommended first)
        cohort = cohort.sort_values("timing_hours")

        # --- Timing delta color helper ---
        def _timing_label(h: float) -> str:
            sign = "+" if h >= 0 else ""
            return f"{sign}{h:.0f}h"

        def _timing_color(h: float) -> str:
            if abs(h) <= 24:
                return "🟢"
            elif abs(h) <= 60:
                return "🟡"
            return "⚪"

        # --- Scorecard table ---
        st.markdown("#### Cohort Scorecard")
        score_rows = []
        for _, row in cohort.iterrows():
            score_rows.append({
                "Title": row["title"][:55] + "…" if len(str(row["title"])) > 55 else row["title"],
                "Timing": f"{_timing_color(row['timing_hours'])} {_timing_label(row['timing_hours'])}",
                "Days Live": int(row["days_live"]),
                "Views": int(row.get("total_views", 0) or 0),
                "Hours Watched": round(row.get("total_hours", 0) or 0, 1),
                "Subs Gained": int(row.get("total_subs", 0) or 0),
                "Kept 25%": f"{row['retention_at_25'] * 100:.1f}%" if pd.notna(row.get("retention_at_25")) else "—",
                "Kept 75%": f"{row['retention_at_75'] * 100:.1f}%" if pd.notna(row.get("retention_at_75")) else "—",
            })
        st.dataframe(pd.DataFrame(score_rows), use_container_width=True, hide_index=True)
        st.caption("🟢 ±24h of recommended date  🟡 24–60h  ⚪ 60–96h")

        # --- Trajectory charts ---
        st.markdown("#### Performance Trajectory (Days Since Publication)")

        colors = px.colors.qualitative.Plotly
        color_map = {vid: colors[i % len(colors)] for i, vid in enumerate(eligible_ids)}
        title_map = cohort.set_index("video_id")["title"].apply(
            lambda t: str(t)[:35] + "…" if len(str(t)) > 35 else str(t)
        ).to_dict()

        cm_sorted = cm.sort_values(["video_id", "day_offset"])
        cm_sorted["cumulative_views"] = cm_sorted.groupby("video_id")["views"].cumsum()
        cm_sorted["cumulative_hours"] = (
            cm_sorted.groupby("video_id")["estimated_minutes_watched"].cumsum() / 60
        )

        fig_views = go.Figure()
        fig_hours = go.Figure()
        for vid in eligible_ids:
            vm = cm_sorted[cm_sorted["video_id"] == vid]
            label = title_map.get(vid, vid)
            clr = color_map[vid]
            fig_views.add_scatter(
                x=vm["day_offset"], y=vm["cumulative_views"],
                name=label, mode="lines+markers", line=dict(color=clr),
            )
            fig_hours.add_scatter(
                x=vm["day_offset"], y=vm["cumulative_hours"],
                name=label, mode="lines+markers", line=dict(color=clr),
            )

        fig_views.update_layout(
            title="Cumulative Views by Day Since Publication",
            xaxis_title="Days Since Publication",
            yaxis_title="Cumulative Views",
            height=380,
            showlegend=True,
        )
        fig_hours.update_layout(
            title="Cumulative Hours Watched by Day Since Publication",
            xaxis_title="Days Since Publication",
            yaxis_title="Cumulative Hours",
            height=380,
            showlegend=True,
        )

        tc1, tc2 = st.columns(2)
        with tc1:
            st.plotly_chart(fig_views, use_container_width=True)
        with tc2:
            st.plotly_chart(fig_hours, use_container_width=True)

        # --- Retention comparison ---
        st.markdown("#### Retention Depth Comparison")
        ret_cohort = cohort[cohort["retention_at_25"].notna()].copy()
        if ret_cohort.empty:
            st.caption("Retention data not yet available for these episodes.")
        else:
            ret_cohort["label"] = ret_cohort["title"].apply(
                lambda t: str(t)[:40] + "…" if len(str(t)) > 40 else str(t)
            )
            fig_ret = go.Figure()
            fig_ret.add_bar(
                x=ret_cohort["retention_at_25"] * 100,
                y=ret_cohort["label"],
                orientation="h",
                name="Kept 25%",
                marker_color="#4C78A8",
            )
            fig_ret.add_bar(
                x=ret_cohort["retention_at_75"] * 100,
                y=ret_cohort["label"],
                orientation="h",
                name="Kept 75%",
                marker_color="#54A24B",
            )
            fig_ret.update_layout(
                barmode="group",
                title="Audience Retention at 25% and 75% of Video Length",
                xaxis_title="% of viewers",
                height=max(300, len(ret_cohort) * 60),
                showlegend=True,
            )
            fig_ret.update_xaxes(range=[0, 100])
            fig_ret.update_yaxes(autorange="reversed")
            st.plotly_chart(fig_ret, use_container_width=True)
```

- [ ] **Step 3: Verify the dashboard renders without error**

```bash
cd "/Users/zdjimas/VS Code Projects/human-workforce-analytics"
.venv/bin/python -m py_compile app.py && echo "Syntax OK"
```

Expected: `Syntax OK` with no errors.

- [ ] **Step 4: Run the full test suite**

```bash
.venv/bin/python -m pytest tests/ -v
```

Expected: all tests pass (no `app.py` unit tests, but the smoke test should pass).

- [ ] **Step 5: Commit**

```bash
git add app.py
git commit -m "feat: add News-Timed Release Impact dashboard section"
```

---

## Self-Review Checklist

After completing all tasks, verify:

- [ ] `queue_recommendations` table exists after `init_db()` (Task 1 test covers this)
- [ ] INSERT OR IGNORE preserves first recommendation on repeated runs (Task 2 test covers this)
- [ ] `write_queue_recommendations` is a no-op with empty ranked list (Task 2 test covers this)
- [ ] `main()` wraps both `write_publishing_queue` and `write_queue_recommendations` in separate try/except blocks so one failure doesn't skip the other
- [ ] Timing window filter: `timing_hours >= -72` AND `timing_hours <= 96` (implemented in Task 4)
- [ ] 3-day data gate: `HAVING COUNT(dvm.metric_date) >= 3` (in the SQL query in Task 4)
- [ ] Empty state message shown when cohort is empty (Task 4)
- [ ] Retention chart gracefully skipped when no retention data (Task 4 `ret_cohort.empty` branch)
- [ ] Color consistency: same `color_map` used for both trajectory charts (Task 4)
