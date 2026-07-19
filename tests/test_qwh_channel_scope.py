import sqlite3

from db import SCHEMA


def test_qwh_module_queries_filter_by_channel(tmp_path):
    source = open("qualifying_watch_hours.py").read()
    assert "def render(db_path: Path, channel: str)" in source
    # Every SELECT against a channel-scoped table must include a channel predicate.
    for table in ["daily_video_metrics", "video_traffic_source_metrics", "daily_channel_metrics"]:
        assert f"WHERE channel = ?" in source or "channel = ?" in source
