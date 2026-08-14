# Public Channel Analytics Demo Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a separate, password-free Streamlit demo for the fictional AI Engineering Genius channel using six months of deterministic synthetic data and no production credentials or identifiable content.

**Architecture:** Add a self-contained `demo/` Streamlit package with its own configuration, database, pages, synthetic-data generator, and presentation helpers. Reuse pure analytics/model modules from the repository, but never import production entry points, credential clients, `db.DB_PATH`, `channel_state`, or `data.db`; demo pages receive their channel and database from `demo.config`. Generate the bundled SQLite fixture offline from a fixed seed and enforce the isolation boundary with automated privacy scans and route smoke tests.

**Tech Stack:** Python 3.12, Streamlit, SQLite, pandas, NumPy, Plotly, pytest

**Spec:** `docs/superpowers/specs/2026-08-14-public-channel-analytics-demo-design.md`

## Global Constraints

- The public demo requires no password and contains no authentication form.
- The only active channel key is `ai_engineering_genius`, displayed as `AI Engineering Genius`.
- `Automation Architects`, `Future Systems Lab`, and `Practical AI Studio` appear as disabled controls and never alter query state.
- Demo runtime code must not import live-service clients, read production secrets, read `data.db`, or fall back to a production channel.
- The database contains at least 184 consecutive days of deterministic synthetic history ending on `2026-08-14`.
- No production title, playlist, video ID, thumbnail URL, recommendation copy, or exact row-level metric series may enter the demo artifact.
- Every page identifies its figures as simulated demo data.
- Deployment is not part of implementation; publish only after separate user authorization.

## File Structure

- `demo/__init__.py` — package marker only.
- `demo/config.py` — immutable demo identity, fixed date, database path, and fictional disabled-channel definitions.
- `demo/ui.py` — shared sidebar, demo notice, page setup, and public-friendly empty-state helpers.
- `demo/db.py` — demo-only schema creation and SQLite connection helpers; never references the production DB.
- `demo/generate_data.py` — deterministic offline fixture builder and integrity validation.
- `demo/app.py` — password-free overview entry point and explicit multipage navigation.
- `demo/pages/*.py` — demo report pages adapted from current reports and wired only to demo configuration/data.
- `demo/assets/channel_mark.svg` — local generic AI Engineering Genius mark used where artwork is needed.
- `demo/data/demo.db` — generated, bundled synthetic SQLite fixture.
- `demo/requirements.txt` — public deployment dependencies with no Google/YouTube/OAuth clients.
- `tests/demo/test_config.py` — identity and disabled-channel behavior.
- `tests/demo/test_generate_data.py` — determinism, date coverage, reconciliation, and integrity.
- `tests/demo/test_privacy_boundary.py` — forbidden names, IDs, URLs, credentials, imports, and paths.
- `tests/demo/test_pages.py` — static and Streamlit smoke coverage for all seven routes.
- `tests/demo/test_analytics_coverage.py` — non-empty report inputs and representative calculation coverage.
- `.streamlit/config.toml` — retain production settings unchanged; demo deployment uses repository defaults unless a later deployment task requires separate configuration.

---

### Task 1: Establish the Demo Configuration and UI Boundary

**Files:**
- Create: `demo/__init__.py`
- Create: `demo/config.py`
- Create: `demo/ui.py`
- Create: `tests/demo/test_config.py`

**Interfaces:**
- Produces: `DEMO_CHANNEL_KEY: str`, `DEMO_CHANNEL_NAME: str`, `DEMO_AS_OF: date`, `DEMO_DB_PATH: Path`, `DISABLED_CHANNELS: tuple[str, ...]`
- Produces: `configure_page(page_title: str) -> None`, `render_demo_sidebar(active_page: str) -> str`, `render_demo_notice() -> None`, `render_empty_state(report_name: str) -> None`
- Guarantees: `render_demo_sidebar()` always returns `ai_engineering_genius`.

- [ ] **Step 1: Write failing configuration tests**

```python
# tests/demo/test_config.py
from datetime import date

from demo.config import (
    DEMO_AS_OF,
    DEMO_CHANNEL_KEY,
    DEMO_CHANNEL_NAME,
    DISABLED_CHANNELS,
)


def test_demo_identity_is_fixed_and_fictional():
    assert DEMO_CHANNEL_KEY == "ai_engineering_genius"
    assert DEMO_CHANNEL_NAME == "AI Engineering Genius"
    assert DEMO_AS_OF == date(2026, 8, 14)


def test_portfolio_channels_are_present_but_not_selectable():
    assert DISABLED_CHANNELS == (
        "Automation Architects",
        "Future Systems Lab",
        "Practical AI Studio",
    )
```

- [ ] **Step 2: Run the tests and verify the missing-module failure**

Run: `.venv/bin/pytest tests/demo/test_config.py -v`

Expected: FAIL during collection with `ModuleNotFoundError: No module named 'demo'`.

- [ ] **Step 3: Implement immutable demo configuration**

```python
# demo/config.py
from datetime import date
from pathlib import Path

DEMO_CHANNEL_KEY = "ai_engineering_genius"
DEMO_CHANNEL_NAME = "AI Engineering Genius"
DEMO_AS_OF = date(2026, 8, 14)
DEMO_DB_PATH = Path(__file__).parent / "data" / "demo.db"
DISABLED_CHANNELS = (
    "Automation Architects",
    "Future Systems Lab",
    "Practical AI Studio",
)
```

Create an empty `demo/__init__.py`.

- [ ] **Step 4: Implement shared Streamlit presentation**

```python
# demo/ui.py
import streamlit as st

from demo.config import DEMO_CHANNEL_KEY, DEMO_CHANNEL_NAME, DISABLED_CHANNELS


def configure_page(page_title: str) -> None:
    st.set_page_config(
        page_title=f"{page_title} | Channel Analytics Demo",
        page_icon="📊",
        layout="wide",
    )


def render_demo_sidebar(active_page: str) -> str:
    with st.sidebar:
        st.markdown("### Channel Portfolio")
        st.button(DEMO_CHANNEL_NAME, type="primary", use_container_width=True)
        for name in DISABLED_CHANNELS:
            st.button(name, disabled=True, use_container_width=True)
        st.caption("Additional channels can be configured through our consulting service.")
        st.divider()
        st.markdown("**Built for your channel**")
        st.caption(
            "We configure this analytics workspace around your content library, "
            "growth goals, and publishing workflow."
        )
    return DEMO_CHANNEL_KEY


def render_demo_notice() -> None:
    st.info(
        "Demo workspace — AI Engineering Genius and all displayed results are "
        "simulated. Figures illustrate product capabilities, not guaranteed outcomes.",
        icon="🧪",
    )


def render_empty_state(report_name: str) -> None:
    st.warning(f"The simulated {report_name} dataset is temporarily unavailable.")
```

- [ ] **Step 5: Run configuration tests**

Run: `.venv/bin/pytest tests/demo/test_config.py -v`

Expected: 2 passed.

- [ ] **Step 6: Commit the boundary**

```bash
git add demo/__init__.py demo/config.py demo/ui.py tests/demo/test_config.py
git commit -m "feat: establish isolated demo configuration"
```

---

### Task 2: Build the Demo-Only Schema and Deterministic Fixture Generator

**Files:**
- Create: `demo/db.py`
- Create: `demo/generate_data.py`
- Create: `tests/demo/test_generate_data.py`
- Generate: `demo/data/demo.db`

**Interfaces:**
- Consumes: constants from `demo.config`.
- Produces: `connect_demo_db(path: Path = DEMO_DB_PATH) -> Iterator[sqlite3.Connection]`
- Produces: `build_demo_database(path: Path, *, seed: int = 8142026) -> None`
- Produces: `validate_demo_database(path: Path) -> list[str]`, returning an empty list when valid.

- [ ] **Step 1: Write failing fixture tests**

```python
# tests/demo/test_generate_data.py
import hashlib
import sqlite3

from demo.config import DEMO_CHANNEL_KEY
from demo.generate_data import build_demo_database, validate_demo_database


def _digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_generation_is_deterministic(tmp_path):
    first = tmp_path / "first.db"
    second = tmp_path / "second.db"
    build_demo_database(first, seed=8142026)
    build_demo_database(second, seed=8142026)
    assert _digest(first) == _digest(second)


def test_fixture_has_six_months_and_one_channel(tmp_path):
    path = tmp_path / "demo.db"
    build_demo_database(path)
    with sqlite3.connect(path) as conn:
        start, end, days = conn.execute(
            "SELECT MIN(metric_date), MAX(metric_date), COUNT(*) "
            "FROM daily_channel_metrics"
        ).fetchone()
        channels = conn.execute(
            "SELECT DISTINCT channel FROM daily_channel_metrics"
        ).fetchall()
    assert (start, end, days) == ("2026-02-12", "2026-08-14", 184)
    assert channels == [(DEMO_CHANNEL_KEY,)]


def test_fixture_integrity_validator_passes(tmp_path):
    path = tmp_path / "demo.db"
    build_demo_database(path)
    assert validate_demo_database(path) == []
```

- [ ] **Step 2: Run the tests and verify generator imports fail**

Run: `.venv/bin/pytest tests/demo/test_generate_data.py -v`

Expected: FAIL because `demo.generate_data` does not exist.

- [ ] **Step 3: Implement the demo schema**

In `demo/db.py`, define only the tables required by the seven reports: `channel_snapshots`, `videos`, `video_snapshots`, `daily_video_metrics`, `daily_channel_metrics`, `retention_buckets`, `daily_geo_metrics`, `publishing_queue`, `playlists`, `playlist_videos`, `queue_recommendations`, `video_traffic_source_metrics`, `channel_traffic_sources`, `ci_video_scores`, and `ci_content_assets`. Copy column definitions required by current SQL, change every channel default to `ai_engineering_genius`, enable `PRAGMA foreign_keys = ON`, and add explicit foreign keys for all video and playlist relationships.

Implement `SCHEMA` as a literal SQL string using the corresponding table definitions in `db.py`, limited to the 15 tables named in this step. Make these exact changes while copying: set every `channel` default to `ai_engineering_genius`; use `(channel, video_id)` as the foreign key from video-dependent tables; use `(channel, playlist_id)` and `(channel, video_id)` as the two foreign keys from `playlist_videos`; and retain the composite primary/unique keys already defined in `db.py`. Do not include `playlist_metrics`, `video_ctr_metrics`, `daily_channel_ctr`, `_schema_migrations`, or any migration function.

```python
# demo/db.py — connection boundary following the literal SCHEMA definition
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from demo.config import DEMO_DB_PATH


@contextmanager
def connect_demo_db(path: Path = DEMO_DB_PATH) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA)
    try:
        yield conn
    finally:
        conn.close()
```

The completed `demo/db.py` must contain the literal schema; do not import `db.SCHEMA` because its defaults and migrations contain production identity.

- [ ] **Step 4: Implement fictional catalog and deterministic daily generation**

In `demo/generate_data.py`, define at least 48 fictional videos across these local playlist groups:

```python
PLAYLISTS = {
    "pl_agent": "Reliable AI Agents",
    "pl_eval": "Evaluation Systems in Practice",
    "pl_retrieval": "Production Retrieval Architecture",
    "pl_observe": "AI Observability Lab",
    "pl_secure": "Secure Automation",
    "pl_short": "Engineering Shorts",
    "pl_visual": "Visual Engineering Briefings",
    "pl_hd": "Deep-Dive Workshops",
    "pl_original": "Studio Originals",
}
```

Use a local `random.Random(seed)` instance and integer arithmetic for stored counts. Generate the date range with:

```python
dates = [DEMO_AS_OF - timedelta(days=offset) for offset in reversed(range(184))]
```

For each date, derive a weekly seasonality term, a gradual growth term, and seeded noise. Insert daily channel metrics, per-video metrics for published videos, five geographic rows, and traffic-source rows. Derive snapshots cumulatively from the generated daily rows. Generate retention values in valid ranges (`0 <= retention_at_75 <= retention_at_25 <= 1`), fictional publishing recommendations, content scores, and draft assets that reference generated video IDs only.

Write to `path.with_suffix(".tmp")`, validate it, then use `Path.replace(path)` only after validation succeeds. Set fixed SQLite pragmas and insert rows in a stable sorted order so equal seeds yield byte-identical files.

- [ ] **Step 5: Implement integrity validation**

```python
def validate_demo_database(path: Path) -> list[str]:
    errors: list[str] = []
    with sqlite3.connect(path) as conn:
        if conn.execute("PRAGMA foreign_key_check").fetchall():
            errors.append("foreign key violations")
        channels = {
            row[0] for row in conn.execute(
                "SELECT DISTINCT channel FROM daily_channel_metrics"
            )
        }
        if channels != {DEMO_CHANNEL_KEY}:
            errors.append(f"unexpected channels: {sorted(channels)}")
        days = conn.execute(
            "SELECT COUNT(*) FROM daily_channel_metrics"
        ).fetchone()[0]
        if days < 184:
            errors.append(f"insufficient history: {days} days")
        orphan_count = conn.execute(
            "SELECT COUNT(*) FROM daily_video_metrics d "
            "LEFT JOIN videos v ON v.channel=d.channel AND v.video_id=d.video_id "
            "WHERE v.video_id IS NULL"
        ).fetchone()[0]
        if orphan_count:
            errors.append(f"orphan video metrics: {orphan_count}")
    return errors
```

Extend the validator with cumulative monotonicity, retention bounds, playlist item-count reconciliation, and channel-snapshot video-count checks.

- [ ] **Step 6: Run generator tests and correct determinism issues**

Run: `.venv/bin/pytest tests/demo/test_generate_data.py -v`

Expected: 3 passed.

- [ ] **Step 7: Generate and inspect the bundled fixture**

Run: `.venv/bin/python -m demo.generate_data --output demo/data/demo.db --seed 8142026`

Run: `sqlite3 demo/data/demo.db "SELECT MIN(metric_date), MAX(metric_date), COUNT(*) FROM daily_channel_metrics; SELECT COUNT(*) FROM videos; SELECT DISTINCT channel FROM videos;"`

Expected: `2026-02-12|2026-08-14|184`, at least 48 videos, and only `ai_engineering_genius`.

- [ ] **Step 8: Commit the fixture system**

```bash
git add demo/db.py demo/generate_data.py demo/data/demo.db tests/demo/test_generate_data.py
git commit -m "feat: generate deterministic synthetic channel data"
```

---

### Task 3: Create the Password-Free Overview and Explicit Navigation

**Files:**
- Create: `demo/app.py`
- Create: `demo/assets/channel_mark.svg`
- Create: `tests/demo/test_pages.py`

**Interfaces:**
- Consumes: `DEMO_DB_PATH`, `DEMO_CHANNEL_KEY`, and shared functions from `demo.ui`.
- Produces: public Streamlit entry point with explicit navigation for seven pages.
- Guarantees: no password widget, authentication state, production fetch instruction, or implicit discovery of production `pages/`.

- [ ] **Step 1: Write failing entry-point tests**

```python
# tests/demo/test_pages.py
from pathlib import Path

DEMO_ROOT = Path("demo")


def test_demo_entry_point_has_no_authentication_surface():
    source = (DEMO_ROOT / "app.py").read_text()
    forbidden = ("check_password", "dashboard_password", 'type="password"', "authenticated")
    assert all(token not in source for token in forbidden)


def test_demo_exposes_all_reports():
    source = (DEMO_ROOT / "app.py").read_text()
    for label in (
        "Overview", "Daily Analytics", "Qualifying Watch Hours",
        "Organic Momentum", "Promotion Intelligence",
        "Content Intelligence", "Video Render Comparisons",
    ):
        assert label in source
```

- [ ] **Step 2: Run entry-point tests and verify failure**

Run: `.venv/bin/pytest tests/demo/test_pages.py -v`

Expected: FAIL because `demo/app.py` is missing.

- [ ] **Step 3: Implement explicit public navigation**

Use Streamlit's explicit navigation API so the demo never exposes the repository's production `pages/` directory:

```python
# demo/app.py
import streamlit as st

from demo.ui import configure_page

configure_page("AI Engineering Genius")

pages = {
    "Channel Analytics": [
        st.Page("demo/pages/overview.py", title="Overview", icon="📊", default=True),
        st.Page("demo/pages/daily_analytics.py", title="Daily Analytics", icon="📅"),
        st.Page("demo/pages/qualifying_watch_hours.py", title="Qualifying Watch Hours", icon="⏱️"),
        st.Page("demo/pages/organic_momentum.py", title="Organic Momentum", icon="🌱"),
        st.Page("demo/pages/promotion_intelligence.py", title="Promotion Intelligence", icon="📣"),
        st.Page("demo/pages/content_intelligence.py", title="Content Intelligence", icon="🧠"),
        st.Page("demo/pages/video_render_comparisons.py", title="Video Render Comparisons", icon="🎬"),
    ]
}
st.navigation(pages, position="sidebar").run()
```

Because `configure_page` may only be called once, page modules must not call `st.set_page_config`.

- [ ] **Step 4: Create a local generic SVG mark**

Create `demo/assets/channel_mark.svg` with a simple abstract circuit-node motif and the letters `AEG`. Do not embed external images, fonts, links, scripts, metadata, or production names.

- [ ] **Step 5: Run entry-point tests**

Run: `.venv/bin/pytest tests/demo/test_pages.py -v`

Expected: 2 passed.

- [ ] **Step 6: Commit the shell**

```bash
git add demo/app.py demo/assets/channel_mark.svg tests/demo/test_pages.py
git commit -m "feat: add public demo navigation shell"
```

---

### Task 4: Adapt Overview, Daily Analytics, and Watch-Hour Reports

**Files:**
- Create: `demo/pages/__init__.py`
- Create: `demo/pages/overview.py`
- Create: `demo/pages/daily_analytics.py`
- Create: `demo/pages/qualifying_watch_hours.py`
- Create: `demo/report_data.py`
- Create: `tests/demo/test_analytics_coverage.py`

**Interfaces:**
- Consumes: `DEMO_DB_PATH`, `DEMO_CHANNEL_KEY`, `DEMO_AS_OF`, and `demo.ui`.
- Produces: `query_frame(sql: str, params: dict[str, object]) -> pandas.DataFrame`.
- Produces: functional Overview, Daily Analytics, and Qualifying Watch Hours pages.

- [ ] **Step 1: Write failing data-coverage tests**

```python
# tests/demo/test_analytics_coverage.py
import sqlite3

from demo.config import DEMO_CHANNEL_KEY, DEMO_DB_PATH


def test_overview_inputs_are_non_empty():
    with sqlite3.connect(DEMO_DB_PATH) as conn:
        for table in (
            "channel_snapshots", "daily_channel_metrics", "videos",
            "daily_geo_metrics", "playlists", "retention_buckets",
            "channel_traffic_sources", "publishing_queue",
        ):
            count = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE channel = ?",
                (DEMO_CHANNEL_KEY,),
            ).fetchone()[0]
            assert count > 0, table


def test_qualifying_hours_have_paid_and_organic_inputs():
    with sqlite3.connect(DEMO_DB_PATH) as conn:
        types = {
            row[0] for row in conn.execute(
                "SELECT DISTINCT traffic_source_type "
                "FROM video_traffic_source_metrics WHERE channel = ?",
                (DEMO_CHANNEL_KEY,),
            )
        }
    assert "ADVERTISING" in types
    assert len(types - {"ADVERTISING"}) >= 3
```

- [ ] **Step 2: Run coverage tests**

Run: `.venv/bin/pytest tests/demo/test_analytics_coverage.py -v`

Expected: PASS if Task 2 populated every required table; otherwise fail on the missing fixture family and fix the Task 2 generator before continuing.

- [ ] **Step 3: Implement the demo query boundary**

```python
# demo/report_data.py
import sqlite3
import pandas as pd

from demo.config import DEMO_DB_PATH


def query_frame(sql: str, params: dict[str, object]) -> pd.DataFrame:
    if not DEMO_DB_PATH.exists():
        return pd.DataFrame()
    with sqlite3.connect(DEMO_DB_PATH) as conn:
        try:
            return pd.read_sql_query(sql, conn, params=params)
        except sqlite3.Error:
            return pd.DataFrame()
```

All page queries must call `query_frame` with `{"channel": DEMO_CHANNEL_KEY}`. Do not accept an arbitrary database path from query strings, session state, or environment variables.

- [ ] **Step 4: Adapt the Overview page**

Copy the report sections from `app.py` into `demo/pages/overview.py`, remove password and navigation code, and replace imports/paths with:

```python
from demo.config import DEMO_AS_OF, DEMO_CHANNEL_KEY, DEMO_CHANNEL_NAME
from demo.report_data import query_frame
from demo.ui import render_demo_notice, render_demo_sidebar, render_empty_state

active_channel = render_demo_sidebar("Overview")
render_demo_notice()
st.title(f"📊 {DEMO_CHANNEL_NAME} Analytics")
```

Replace `pd.Timestamp.utcnow()` cutoffs with `pd.Timestamp(DEMO_AS_OF)` so every range contains stable data. Replace production refresh instructions and stale-live-data warnings with `render_empty_state`. Retain charts, filters, projections, playlists, engagement, retention, per-video deep dive, and publishing queue. Rewrite publishing copy to say “simulated planning signals” instead of implying current news monitoring.

- [ ] **Step 5: Adapt Daily Analytics**

Copy `pages/daily_analytics.py` to `demo/pages/daily_analytics.py`. Remove the authentication redirect and `st.set_page_config`; use `DEMO_DB_PATH` and `DEMO_CHANNEL_KEY`; render the shared sidebar and demo notice; replace all `date.today()` and current-time anchors with `DEMO_AS_OF`.

- [ ] **Step 6: Adapt Qualifying Watch Hours**

Create `demo/pages/qualifying_watch_hours.py` as a demo wrapper around the pure report renderer:

```python
import streamlit as st

from demo.config import DEMO_CHANNEL_KEY, DEMO_DB_PATH
from demo.ui import render_demo_notice, render_demo_sidebar
import qualifying_watch_hours as report

render_demo_sidebar("Qualifying Watch Hours")
render_demo_notice()
report.render(DEMO_DB_PATH, DEMO_CHANNEL_KEY)
```

Modify `qualifying_watch_hours.render` to accept `as_of: date | None = None` and `empty_message: str | None = None`. Resolve `effective_as_of = as_of or date.today()` and use it for every report cutoff. Resolve the empty copy as `empty_message or "No qualifying watch-hour data is available."`. Existing production callers retain current behavior by omitting both arguments; the demo passes `DEMO_AS_OF` and `"The simulated qualifying watch-hour dataset is temporarily unavailable."`.

- [ ] **Step 7: Add static path/isolation assertions for the three pages**

Extend `tests/demo/test_pages.py`:

```python
def test_core_demo_pages_use_demo_data_boundary():
    for name in ("overview.py", "daily_analytics.py", "qualifying_watch_hours.py"):
        source = (DEMO_ROOT / "pages" / name).read_text()
        assert "demo.config" in source
        assert "data.db" not in source
        assert "from db import DB_PATH" not in source
        assert "authenticated" not in source
```

- [ ] **Step 8: Run focused tests**

Run: `.venv/bin/pytest tests/demo/test_pages.py tests/demo/test_analytics_coverage.py -v`

Expected: all tests pass.

- [ ] **Step 9: Commit the core reports**

```bash
git add demo/pages demo/report_data.py tests/demo/test_pages.py tests/demo/test_analytics_coverage.py qualifying_watch_hours.py
git commit -m "feat: add core synthetic analytics reports"
```

---

### Task 5: Adapt Momentum, Promotion, Content, and Render-Comparison Reports

**Files:**
- Create: `demo/pages/organic_momentum.py`
- Create: `demo/pages/promotion_intelligence.py`
- Create: `demo/pages/content_intelligence.py`
- Create: `demo/pages/video_render_comparisons.py`
- Modify: `tests/demo/test_pages.py`
- Modify: `tests/demo/test_analytics_coverage.py`

**Interfaces:**
- Consumes: demo configuration, fixture tables, shared UI, and existing pure modules under `analytics/`, `models/`, `promotion_intelligence/`, and `content_intelligence/`.
- Produces: four remaining functional public report pages.

- [ ] **Step 1: Write failing static isolation tests for advanced reports**

```python
def test_advanced_demo_pages_use_demo_data_boundary():
    names = (
        "organic_momentum.py", "promotion_intelligence.py",
        "content_intelligence.py", "video_render_comparisons.py",
    )
    for name in names:
        source = (DEMO_ROOT / "pages" / name).read_text()
        assert "demo.config" in source
        assert "from db import DB_PATH" not in source
        assert "from channel_state" not in source
        assert "authenticated" not in source
```

- [ ] **Step 2: Run the test and verify the missing-page failure**

Run: `.venv/bin/pytest tests/demo/test_pages.py::test_advanced_demo_pages_use_demo_data_boundary -v`

Expected: FAIL with `FileNotFoundError` for the first missing page.

- [ ] **Step 3: Adapt Organic Momentum**

Copy `pages/organic_momentum.py`, remove its embedded `_DEMO_*` fallback catalog and all production-specific topic labels. Query `DEMO_DB_PATH` only, scope every SQL statement to `DEMO_CHANNEL_KEY`, use `DEMO_AS_OF`, and map topics to `Agents`, `Evaluation`, `Retrieval`, `Observability`, `Security`, `Deployment`, and `General`. Retain score weights, filters, rankings, charts, promotion analysis, and recommended-action explainers.

- [ ] **Step 4: Adapt Promotion Intelligence**

Copy `pages/promotion_intelligence.py`, delete `_DEMO_VIDEOS` and random fallback generation, and require the synthetic fixture. Use the engineering topic taxonomy above, retain recommendation cards, the complete table, ROI calculator, visualizations, and explainability. The ROI caption must say that financial figures are simulated planning scenarios, not forecasts or guarantees.

- [ ] **Step 5: Adapt Content Intelligence**

Copy `pages/content_intelligence.py`, construct `ContentIntelligenceService(DEMO_DB_PATH, channel=DEMO_CHANNEL_KEY)`, and use the shared demo notice/sidebar. Keep all five tabs and asset filtering. Disable asset status mutations in the public demo: replace approve/reject/schedule buttons with disabled preview controls and explanatory copy so public visitors cannot mutate the bundled fixture.

- [ ] **Step 6: Adapt Video Render Comparisons**

Copy `pages/video_render_comparisons.py` and map fixed render groups to fictional playlists:

```python
_SHORTS_PLAYLISTS = {"Engineering Shorts"}
_VISUAL_PLAYLISTS = {"Visual Engineering Briefings"}
_HD_PLAYLISTS = {"Deep-Dive Workshops"}
_JELLYPOD_PLAYLISTS = {"Studio Originals"}
```

Use demo configuration and preserve all comparison controls, charts, and summary tables.

- [ ] **Step 7: Extend non-empty advanced-report coverage**

Add assertions that `ci_video_scores` contains every tier used by the UI, `ci_content_assets` contains at least two asset types, render playlists each have at least four videos, and promotion inputs include both advertised and organic-only videos.

- [ ] **Step 8: Run focused tests**

Run: `.venv/bin/pytest tests/demo/test_pages.py tests/demo/test_analytics_coverage.py -v`

Expected: all tests pass.

- [ ] **Step 9: Commit advanced reports**

```bash
git add demo/pages tests/demo/test_pages.py tests/demo/test_analytics_coverage.py
git commit -m "feat: complete public demo report suite"
```

---

### Task 6: Enforce the Privacy Boundary

**Files:**
- Create: `tests/demo/privacy_rules.py`
- Create: `tests/demo/test_privacy_boundary.py`

**Interfaces:**
- Consumes: the complete `demo/` source tree and `demo/data/demo.db`.
- Produces: `scan_demo_artifacts(root: Path, db_path: Path) -> list[str]` in `tests.demo.privacy_rules` so forbidden production tokens never enter the deployable package.
- Guarantees: CI fails on production identity, remote artwork, real IDs, credentials, live-service imports, or production database references.

- [ ] **Step 1: Write failing privacy tests**

```python
# tests/demo/test_privacy_boundary.py
from pathlib import Path

from demo.config import DEMO_DB_PATH
from tests.demo.privacy_rules import scan_demo_artifacts


def test_demo_artifacts_pass_privacy_scan():
    assert scan_demo_artifacts(Path("demo"), DEMO_DB_PATH) == []


def test_scanner_detects_forbidden_content(tmp_path):
    root = tmp_path / "demo"
    root.mkdir()
    (root / "leak.py").write_text("from youtube_client import fetch_channel_stats")
    errors = scan_demo_artifacts(root, tmp_path / "missing.db")
    assert any("youtube_client" in error for error in errors)
```

- [ ] **Step 2: Run privacy tests and verify failure**

Run: `.venv/bin/pytest tests/demo/test_privacy_boundary.py -v`

Expected: FAIL because `scan_demo_artifacts` is missing.

- [ ] **Step 3: Implement source and database scanning**

```python
# tests/demo/privacy_rules.py
FORBIDDEN_SOURCE_TOKENS = (
    "youtube_client", "services.youtube_analytics", "services.google_ads",
    ".streamlit/secrets", "oauth_credentials", "from db import DB_PATH",
    "/data.db", "dashboard_password",
)

FORBIDDEN_BRAND_TOKENS = (
    "the human workforce", "club genius", "kzak", "techy chef",
)


def scan_demo_artifacts(root: Path, db_path: Path) -> list[str]:
    errors: list[str] = []
    for path in sorted(root.rglob("*")):
        if path.suffix.lower() not in {".py", ".md", ".toml", ".svg"}:
            continue
        text = path.read_text(errors="replace").lower()
        for token in FORBIDDEN_SOURCE_TOKENS + FORBIDDEN_BRAND_TOKENS:
            if token.lower() in text:
                errors.append(f"{path}: forbidden token {token}")
    if db_path.exists():
        with sqlite3.connect(db_path) as conn:
            for table, columns in TEXT_COLUMNS.items():
                expression = " || ' ' || ".join(f"COALESCE({c}, '')" for c in columns)
                rows = conn.execute(f"SELECT {expression} FROM {table}").fetchall()
                for index, row in enumerate(rows):
                    value = row[0].lower()
                    if value.startswith("http://") or value.startswith("https://"):
                        errors.append(f"{table}[{index}]: remote URL")
                    for token in FORBIDDEN_BRAND_TOKENS:
                        if token in value:
                            errors.append(f"{table}[{index}]: forbidden brand {token}")
    return errors
```

Define `TEXT_COLUMNS` explicitly for `videos`, `playlists`, `publishing_queue`, `queue_recommendations`, and `ci_content_assets`. Add a YouTube-ID detector that rejects known production IDs loaded by the test from the production DB but never writes them into demo artifacts. Add empty `tests/__init__.py` and `tests/demo/__init__.py` files when they are absent so the scanner imports consistently.

- [ ] **Step 4: Run the privacy suite**

Run: `.venv/bin/pytest tests/demo/test_privacy_boundary.py -v`

Expected: 2 passed and an empty scan for the real demo tree.

- [ ] **Step 5: Run a repository-level textual audit**

Run: `rg -n -i "human workforce|club genius|kzak|techy chef|youtube_client|google_ads|dashboard_password|oauth" demo`

Expected: no matches because privacy rules live outside the deployable `demo/` tree.

- [ ] **Step 6: Commit privacy enforcement**

```bash
git add tests/__init__.py tests/demo/__init__.py tests/demo/privacy_rules.py tests/demo/test_privacy_boundary.py
git commit -m "test: enforce public demo privacy boundary"
```

---

### Task 7: Package, Smoke-Test, and Visually Verify the Demo

**Files:**
- Create: `demo/requirements.txt`
- Create: `demo/README.md`
- Modify: `tests/demo/test_pages.py`
- Modify: `demo/pages/*.py` only for defects found during verification.

**Interfaces:**
- Consumes: completed demo app and synthetic fixture.
- Produces: locally verified, deployment-ready demo package.

- [ ] **Step 1: Add deployment dependencies**

Create `demo/requirements.txt` containing only the packages imported by demo runtime and shared pure analytics modules:

```text
streamlit
pandas
numpy
plotly
pydantic
```

Pin versions to the already-working versions resolved by the production `requirements.txt`; do not add Google, YouTube, OAuth, Anthropic, or OpenAI SDKs.

- [ ] **Step 2: Add operator documentation**

Document these commands in `demo/README.md`:

```bash
.venv/bin/python -m demo.generate_data --output demo/data/demo.db --seed 8142026
.venv/bin/streamlit run demo/app.py
.venv/bin/pytest tests/demo -v
```

State that the database is synthetic, generation is offline, the app is public/password-free, and deployment requires separate authorization.

- [ ] **Step 3: Add Streamlit smoke tests**

Use `streamlit.testing.v1.AppTest` to run `demo/app.py`. Assert the default Overview route renders the demo notice and channel identity with no exception. Route-module existence, imports, and privacy are covered statically in `test_pages.py`; the browser pass in Step 6 loads and interacts with every non-default route through the real `st.navigation` UI.

```python
from streamlit.testing.v1 import AppTest


def test_demo_app_starts_without_password():
    app = AppTest.from_file("demo/app.py").run(timeout=30)
    assert not app.exception
    assert "AI Engineering Genius" in " ".join(x.value for x in app.title)
    assert not app.text_input
```

- [ ] **Step 4: Run all automated tests**

Run: `.venv/bin/pytest tests/demo -v`

Expected: all demo tests pass.

Run: `.venv/bin/pytest -q`

Expected: the complete production and demo test suites pass with no regressions.

- [ ] **Step 5: Start the local demo**

Run: `.venv/bin/streamlit run demo/app.py --server.headless true --server.port 8510`

Expected: Streamlit reports `http://localhost:8510` and stays running for browser verification.

- [ ] **Step 6: Verify every route visually and interactively**

At a standard desktop viewport, inspect all seven pages. On each page confirm the simulated-data notice, AI Engineering Genius identity, disabled portfolio channels, non-empty charts/tables, and absence of production copy. Exercise at least one date range, tab, filter, or selector on each report. On Promotion Intelligence, change the ROI budget; on Content Intelligence, verify asset actions cannot mutate data; on Video Render Comparisons, switch comparison mode.

- [ ] **Step 7: Verify runtime logs and final privacy scan**

Check browser console and Streamlit terminal output for exceptions. Then run:

```bash
.venv/bin/python -c "from pathlib import Path; from demo.config import DEMO_DB_PATH; from tests.demo.privacy_rules import scan_demo_artifacts; errors=scan_demo_artifacts(Path('demo'), DEMO_DB_PATH); assert not errors, errors"
git diff --check
git status --short
```

Expected: no browser/terminal exceptions, empty privacy errors, no whitespace errors, and only intended files listed.

- [ ] **Step 8: Commit the verified package**

```bash
git add demo/requirements.txt demo/README.md demo tests/demo
git commit -m "docs: package verified public analytics demo"
```

Deployment remains intentionally unperformed. Present the locally verified app and request explicit authorization before creating or updating any public Streamlit deployment.
