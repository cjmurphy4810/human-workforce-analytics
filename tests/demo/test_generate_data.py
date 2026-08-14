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
