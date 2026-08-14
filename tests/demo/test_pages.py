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
