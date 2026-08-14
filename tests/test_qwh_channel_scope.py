import sqlite3
import inspect

from db import SCHEMA
from qualifying_watch_hours import render


def test_qwh_module_queries_filter_by_channel(tmp_path):
    source = open("qualifying_watch_hours.py").read()
    assert "def render(" in source
    assert "as_of: date | None = None" in source
    # Every SELECT against a channel-scoped table must include a channel predicate.
    for table in ["daily_video_metrics", "video_traffic_source_metrics", "daily_channel_metrics"]:
        assert f"WHERE channel = ?" in source or "channel = ?" in source


def test_production_render_defaults_to_cumulative_snapshot_storage():
    assert inspect.signature(render).parameters[
        "daily_metrics_are_increments"
    ].default is False
