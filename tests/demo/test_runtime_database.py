from __future__ import annotations

import hashlib
from pathlib import Path
import sqlite3

from demo.config import DEMO_DB_PATH
from demo.report_data import (
    DatabaseFailureReason,
    inspect_demo_database,
    query_frame,
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_query_frame_returns_typed_success_for_packaged_fixture():
    result = query_frame(
        "SELECT channel_id FROM channel_snapshots WHERE channel=:channel LIMIT 1",
        {"channel": "ai_engineering_genius"},
    )

    assert result.is_available
    assert result.failure is None
    assert result.value is not None
    assert not result.value.empty


def test_missing_database_returns_typed_unavailable_without_creating_file(tmp_path):
    missing = tmp_path / "missing.db"

    result = inspect_demo_database(missing)
    query = query_frame("SELECT 1", {}, path=missing)

    assert not result.is_available
    assert result.failure.reason is DatabaseFailureReason.MISSING
    assert not query.is_available
    assert query.failure.reason is DatabaseFailureReason.MISSING
    assert not missing.exists()


def test_malformed_database_returns_typed_unavailable(tmp_path):
    malformed = tmp_path / "malformed.db"
    malformed.write_bytes(b"not a SQLite database")

    result = inspect_demo_database(malformed)

    assert not result.is_available
    assert result.failure.reason is DatabaseFailureReason.MALFORMED


def test_unreadable_database_returns_typed_unavailable(tmp_path):
    unreadable = tmp_path / "unreadable.db"
    unreadable.write_bytes(DEMO_DB_PATH.read_bytes())
    unreadable.chmod(0)
    try:
        result = inspect_demo_database(unreadable)
    finally:
        unreadable.chmod(0o600)

    assert not result.is_available
    assert result.failure.reason is DatabaseFailureReason.UNREADABLE


def test_query_errors_are_typed_and_packaged_fixture_is_never_modified():
    before = _sha256(DEMO_DB_PATH)

    result = query_frame(
        "DELETE FROM daily_channel_metrics WHERE channel=:channel",
        {"channel": "ai_engineering_genius"},
    )

    assert not result.is_available
    assert result.failure.reason is DatabaseFailureReason.QUERY
    assert _sha256(DEMO_DB_PATH) == before
    with sqlite3.connect(f"file:{DEMO_DB_PATH.resolve()}?mode=ro", uri=True) as conn:
        assert conn.execute("SELECT COUNT(*) FROM daily_channel_metrics").fetchone()[0]
