from pathlib import Path
import subprocess
import sys

from streamlit.testing.v1 import AppTest


DEMO_ROOT = Path("demo")


def test_demo_app_starts_without_password():
    app = AppTest.from_file("demo/app.py").run(timeout=30)
    assert not app.exception
    assert "AI Engineering Genius" in " ".join(x.value for x in app.title)
    assert not app.text_input


def test_demo_app_is_importable_from_streamlit_script_context():
    result = subprocess.run(
        [sys.executable, "demo/app.py"],
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


def test_qualifying_demo_explicitly_uses_increment_storage_contract():
    source = (DEMO_ROOT / "pages" / "qualifying_watch_hours.py").read_text()
    assert "daily_metrics_are_increments=True" in source


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
