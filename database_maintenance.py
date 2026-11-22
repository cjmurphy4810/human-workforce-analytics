"""Bound and validate the repository-backed analytics database."""

from __future__ import annotations

import argparse
import sqlite3
from datetime import date, timedelta
from pathlib import Path


HISTORY_DAYS = 370
DEFAULT_MAX_BYTES = 95 * 1024 * 1024
REQUIRED_TABLES = {"channel_snapshots", "video_snapshots", "retention_buckets"}


def _delete(conn: sqlite3.Connection, statement: str, parameters=()) -> int:
    before = conn.total_changes
    conn.execute(statement, parameters)
    return conn.total_changes - before


def maintain_database(db_path: Path, today: date | None = None) -> dict[str, int]:
    """Prune rows production never reads, cap trend history, and compact SQLite."""
    db_path = Path(db_path)
    cutoff = (today or date.today()) - timedelta(days=HISTORY_DAYS)
    deleted: dict[str, int] = {}

    with sqlite3.connect(db_path) as conn:
        deleted["retention_buckets"] = _delete(
            conn,
            """
            DELETE FROM retention_buckets
            WHERE rowid IN (
                SELECT rowid FROM (
                    SELECT rowid,
                           ROW_NUMBER() OVER (
                               PARTITION BY channel, video_id, window_kind
                               ORDER BY window_end DESC, fetched_at DESC,
                                        window_start DESC, rowid DESC
                           ) AS row_number
                    FROM retention_buckets
                )
                WHERE row_number > 1
            )
            """,
        )
        deleted["publishing_queue"] = _delete(
            conn,
            """
            DELETE FROM publishing_queue
            WHERE id IN (
                SELECT id FROM (
                    SELECT id,
                           ROW_NUMBER() OVER (
                               PARTITION BY channel
                               ORDER BY analyzed_at DESC, id DESC
                           ) AS row_number
                    FROM publishing_queue
                )
                WHERE row_number > 1
            )
            """,
        )
        deleted["ci_video_scores"] = _delete(
            conn,
            """
            DELETE FROM ci_video_scores AS scores
            WHERE scored_at < (
                SELECT MAX(latest.scored_at)
                FROM ci_video_scores AS latest
                WHERE latest.channel = scores.channel
            )
            """,
        )
        for table in ("channel_snapshots", "video_snapshots"):
            deleted[table] = _delete(
                conn,
                f"DELETE FROM {table} WHERE date(captured_at) < ?",
                (cutoff.isoformat(),),
            )

    with sqlite3.connect(db_path, isolation_level=None) as conn:
        conn.execute("VACUUM")
        conn.execute("PRAGMA optimize")

    return deleted


def check_database(db_path: Path, max_bytes: int) -> dict[str, int | str]:
    """Raise when the database is corrupt or too large to publish safely."""
    db_path = Path(db_path)
    if not db_path.exists():
        raise ValueError(f"database does not exist: {db_path}")

    size_bytes = db_path.stat().st_size
    if size_bytes == 0:
        raise ValueError(f"database is empty: {db_path}")
    if size_bytes > max_bytes:
        raise ValueError(
            f"database size {size_bytes} bytes exceeds publish limit {max_bytes} bytes"
        )

    try:
        with sqlite3.connect(f"{db_path.resolve().as_uri()}?mode=ro", uri=True) as conn:
            integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
    except sqlite3.DatabaseError as exc:
        raise ValueError(f"database integrity check failed: {exc}") from exc
    if integrity != "ok":
        raise ValueError(f"database integrity check failed: {integrity}")
    missing_tables = REQUIRED_TABLES - tables
    if missing_tables:
        raise ValueError(
            "database is missing required analytics tables: "
            + ", ".join(sorted(missing_tables))
        )
    return {
        "integrity": integrity,
        "size_bytes": size_bytes,
        "max_bytes": max_bytes,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("db_path", type=Path, nargs="?", default=Path("data.db"))
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument("--max-mib", type=int, default=95)
    args = parser.parse_args()

    if not args.check_only:
        deleted = maintain_database(args.db_path)
        print(f"Pruned rows: {deleted}")
    health = check_database(args.db_path, max_bytes=args.max_mib * 1024 * 1024)
    print(
        f"Database health: {health['integrity']}; "
        f"size={health['size_bytes']} bytes; limit={health['max_bytes']} bytes"
    )


if __name__ == "__main__":
    main()
