from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest
from streamlit.testing.v1 import AppTest

from demo.build_artifact import build_public_demo_artifact


DEMO_ROOT = Path("demo")


@pytest.fixture
def artifact(tmp_path):
    return build_public_demo_artifact(tmp_path / "artifact")


def test_demo_app_starts_without_password(artifact, monkeypatch):
    monkeypatch.chdir(artifact)
    app = AppTest.from_file(artifact / "app.py").run(timeout=30)
    assert not app.exception
    assert "AI Engineering Genius" in " ".join(x.value for x in app.title)
    assert not app.text_input


def test_built_demo_app_is_importable_from_streamlit_script_context(artifact):
    result = subprocess.run(
        [sys.executable, "app.py"],
        cwd=artifact,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


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


def test_core_demo_pages_use_demo_data_boundary():
    for name in ("overview.py", "daily_analytics.py", "qualifying_watch_hours.py"):
        source = (DEMO_ROOT / "pages" / name).read_text()
        assert "demo.config" in source
        assert "data.db" not in source
        assert "from db import DB_PATH" not in source
        assert "authenticated" not in source


def test_qualifying_demo_sums_the_bounded_daily_increment_window(
    artifact,
    monkeypatch,
):
    database = artifact / "demo" / "data" / "demo.db"
    with sqlite3.connect(database) as connection:
        expected = connection.execute(
            "WITH totals AS ("
            "SELECT v.video_id, v.duration_seconds, "
            "SUM(d.estimated_minutes_watched)/60.0 watch_hours "
            "FROM videos v JOIN daily_video_metrics d "
            "ON d.channel=v.channel AND d.video_id=v.video_id "
            "WHERE v.channel='ai_engineering_genius' "
            "AND d.metric_date BETWEEN '2025-08-15' AND '2026-08-14' "
            "GROUP BY v.video_id), advertising AS ("
            "SELECT video_id, SUM(estimated_minutes_watched)/60.0 hours "
            "FROM video_traffic_source_metrics "
            "WHERE channel='ai_engineering_genius' "
            "AND traffic_source_type='ADVERTISING' "
            "AND metric_date BETWEEN '2025-08-15' AND '2026-08-14' "
            "GROUP BY video_id) "
            "SELECT SUM(CASE WHEN t.duration_seconds BETWEEN 1 AND 180 THEN 0 "
            "ELSE MAX(t.watch_hours-COALESCE(a.hours,0),0) END) "
            "FROM totals t LEFT JOIN advertising a ON a.video_id=t.video_id"
        ).fetchone()[0]

    monkeypatch.chdir(artifact)
    app = AppTest.from_file(artifact / "app.py", default_timeout=30).run()
    app.switch_page("demo/pages/qualifying_watch_hours.py").run(timeout=30)
    metrics = {metric.label: metric.value for metric in app.metric}

    assert not app.exception
    assert metrics["Qualifying Watch Hours"] == f"{expected:,.1f}"


def test_advanced_demo_pages_use_demo_data_boundary():
    names = (
        "organic_momentum.py",
        "promotion_intelligence.py",
        "content_intelligence.py",
        "video_render_comparisons.py",
    )
    for name in names:
        source = (DEMO_ROOT / "pages" / name).read_text()
        assert "demo.config" in source
        assert "from db import DB_PATH" not in source
        assert "from channel_state" not in source
        assert "authenticated" not in source
