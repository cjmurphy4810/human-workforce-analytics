from pathlib import Path
import sqlite3

import pytest

from demo.config import DEMO_DB_PATH
from tests.demo.privacy_rules import scan_demo_artifacts


PRODUCTION_DB_PATH = Path("data.db")


def _load_production_youtube_ids(path: Path) -> set[str]:
    with sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True) as conn:
        identifiers = {
            row[0]
            for query in (
                "SELECT video_id FROM videos",
                "SELECT playlist_id FROM playlists",
                "SELECT channel_id FROM channel_snapshots",
            )
            for row in conn.execute(query)
            if row[0]
        }
    return identifiers


def _create_scannable_database(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE videos (
                channel TEXT, video_id TEXT, title TEXT, description TEXT,
                published_at TEXT, thumbnail_url TEXT
            );
            CREATE TABLE playlists (
                channel TEXT, playlist_id TEXT, title TEXT, description TEXT,
                published_at TEXT, thumbnail_url TEXT
            );
            CREATE TABLE publishing_queue (
                analyzed_at TEXT, channel TEXT, result_json TEXT
            );
            CREATE TABLE queue_recommendations (
                channel TEXT, video_id TEXT, first_recommended_at TEXT,
                recommended_publish_date TEXT, theme TEXT, why_now TEXT
            );
            CREATE TABLE ci_content_assets (
                asset_id TEXT, channel TEXT, video_id TEXT, video_title TEXT,
                asset_type TEXT, title TEXT, body TEXT, generated_at TEXT,
                status TEXT, approved_at TEXT, scheduled_for TEXT, notes TEXT
            );
            """
        )


def test_demo_artifacts_pass_privacy_scan():
    production_ids = _load_production_youtube_ids(PRODUCTION_DB_PATH)

    assert production_ids
    assert (
        scan_demo_artifacts(
            Path("demo"),
            DEMO_DB_PATH,
            known_youtube_ids=production_ids,
        )
        == []
    )


def test_scanner_detects_forbidden_content(tmp_path):
    root = tmp_path / "demo"
    root.mkdir()
    (root / "leak.py").write_text(
        "from youtube_client import fetch_channel_stats",
        encoding="utf-8",
    )

    errors = scan_demo_artifacts(root, tmp_path / "missing.db")

    assert any("youtube_client" in error for error in errors)


@pytest.mark.parametrize(
    ("leak", "expected_label"),
    [
        ("from services.youtube_analytics import report", "services.youtube_analytics"),
        ("from services.google_ads import spend", "services.google_ads"),
        ("credentials = oauth_credentials.json", "oauth_credentials"),
        ("password = st.secrets['dashboard_password']", "dashboard_password"),
        ("from db import DB_PATH", "from db import DB_PATH"),
    ],
)
def test_scanner_detects_live_service_credential_and_database_references(
    tmp_path,
    leak,
    expected_label,
):
    root = tmp_path / "demo"
    root.mkdir()
    (root / "leak.py").write_text(leak, encoding="utf-8")
    db_path = tmp_path / "demo.db"
    _create_scannable_database(db_path)

    errors = scan_demo_artifacts(root, db_path)

    assert any(expected_label.lower() in error.lower() for error in errors)


def test_scanner_detects_forbidden_credential_paths(tmp_path):
    root = tmp_path / "demo"
    secrets_directory = root / ".streamlit"
    secrets_directory.mkdir(parents=True)
    (secrets_directory / "secrets.toml").write_text("", encoding="utf-8")
    db_path = tmp_path / "demo.db"
    _create_scannable_database(db_path)

    errors = scan_demo_artifacts(root, db_path)

    assert any("forbidden path token .streamlit/secrets" in error for error in errors)


def test_scanner_detects_remote_artwork_but_allows_svg_namespace(tmp_path):
    root = tmp_path / "demo"
    root.mkdir()
    (root / "mark.svg").write_text(
        '<svg xmlns="http://www.w3.org/2000/svg">'
        '<image href="https://cdn.example.invalid/mark.png" />'
        "</svg>",
        encoding="utf-8",
    )
    db_path = tmp_path / "demo.db"
    _create_scannable_database(db_path)

    errors = scan_demo_artifacts(root, db_path)

    remote_errors = [error for error in errors if "remote URL" in error]
    assert len(remote_errors) == 1


def test_scanner_detects_database_brands_and_remote_urls(tmp_path):
    root = tmp_path / "demo"
    root.mkdir()
    db_path = tmp_path / "demo.db"
    _create_scannable_database(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO videos(video_id, title, thumbnail_url) VALUES (?, ?, ?)",
            ("fictional_video", "The Human Workforce briefing", "asset.svg"),
        )
        conn.execute(
            "INSERT INTO playlists(playlist_id, title, thumbnail_url) VALUES (?, ?, ?)",
            ("fictional_playlist", "Fictional", "https://cdn.example.invalid/art.png"),
        )

    errors = scan_demo_artifacts(root, db_path)

    assert any("videos[0]: forbidden brand" in error for error in errors)
    assert any("playlists[0]: remote URL" in error for error in errors)


@pytest.mark.parametrize(
    ("table", "columns", "values"),
    [
        ("publishing_queue", "analyzed_at, channel, result_json", ("now", "demo", "Club Genius")),
        (
            "queue_recommendations",
            "channel, video_id, first_recommended_at, recommended_publish_date, theme, why_now",
            ("demo", "candidate", "now", "later", "KZAK", "Fictional"),
        ),
        (
            "ci_content_assets",
            "asset_id, channel, video_id, video_title, asset_type, title, body, generated_at, status, notes",
            ("asset", "demo", "video", "Fictional", "post", "Fictional", "Techy Chef", "now", "draft", ""),
        ),
    ],
)
def test_scanner_checks_every_required_database_table(tmp_path, table, columns, values):
    root = tmp_path / "demo"
    root.mkdir()
    db_path = tmp_path / "demo.db"
    _create_scannable_database(db_path)
    placeholders = ", ".join("?" for _ in values)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"INSERT INTO {table} ({columns}) VALUES ({placeholders})",
            values,
        )

    errors = scan_demo_artifacts(root, db_path)

    assert any(error.startswith(f"{table}[0]: forbidden brand") for error in errors)


def test_scanner_detects_known_youtube_ids_without_echoing_them(tmp_path):
    root = tmp_path / "demo"
    root.mkdir()
    known_id = "AaBbCcDdE_1"
    (root / "leak.py").write_text(
        f'video_id = "{known_id}"',
        encoding="utf-8",
    )
    db_path = tmp_path / "demo.db"
    _create_scannable_database(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO publishing_queue(analyzed_at, channel, result_json) VALUES (?, ?, ?)",
            ("now", "demo", f'{{"video_id": "{known_id}"}}'),
        )

    errors = scan_demo_artifacts(
        root,
        db_path,
        known_youtube_ids={known_id},
    )

    assert len([error for error in errors if "production YouTube ID" in error]) == 2
    assert all(known_id not in error for error in errors)


def test_scanner_reports_missing_source_and_database_paths(tmp_path):
    errors = scan_demo_artifacts(
        tmp_path / "missing-demo",
        tmp_path / "missing.db",
    )

    assert any("source root is missing" in error for error in errors)
    assert any("database is missing" in error for error in errors)


def test_scanner_reports_malformed_database_instead_of_raising(tmp_path):
    root = tmp_path / "demo"
    root.mkdir()
    db_path = tmp_path / "demo.db"
    db_path.write_bytes(b"not a SQLite database")

    errors = scan_demo_artifacts(root, db_path)

    assert any("database scan failed" in error for error in errors)


def test_scanner_reports_missing_tables_and_columns_instead_of_raising(tmp_path):
    root = tmp_path / "demo"
    root.mkdir()
    db_path = tmp_path / "demo.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE videos (video_id TEXT)")

    errors = scan_demo_artifacts(root, db_path)

    assert any("videos: database scan failed" in error for error in errors)
    assert any("playlists: database scan failed" in error for error in errors)


def test_scanner_handles_undecodable_source_bytes(tmp_path):
    root = tmp_path / "demo"
    root.mkdir()
    (root / "leak.py").write_bytes(b"\xffyoutube_client")
    db_path = tmp_path / "demo.db"
    _create_scannable_database(db_path)

    errors = scan_demo_artifacts(root, db_path)

    assert any("youtube_client" in error for error in errors)
