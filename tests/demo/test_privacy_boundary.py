import json
from pathlib import Path
import sqlite3

import pytest

from demo.build_artifact import build_public_demo_artifact
from tests.demo.privacy_rules import (
    TEXT_COLUMNS,
    load_production_privacy_reference,
    scan_demo_artifacts,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DEMO_ROOT = REPOSITORY_ROOT / "demo"
DEMO_DB_PATH = DEMO_ROOT / "data" / "demo.db"
PRODUCTION_DB_PATH = REPOSITORY_ROOT / "data.db"

PRODUCTION_IDENTIFIER_COLUMNS = {
    "channel_snapshots": ("channel", "channel_id"),
    "channel_traffic_sources": ("channel",),
    "ci_content_assets": ("asset_id", "channel", "video_id"),
    "ci_video_scores": ("channel", "video_id"),
    "daily_channel_metrics": ("channel",),
    "daily_geo_metrics": ("channel",),
    "daily_video_metrics": ("channel", "video_id"),
    "playlist_metrics": ("playlist_id",),
    "playlist_videos": ("channel", "playlist_id", "video_id"),
    "playlists": ("channel", "playlist_id"),
    "publishing_queue": ("channel", "result_json"),
    "queue_recommendations": ("channel", "video_id"),
    "retention_buckets": ("channel", "video_id"),
    "video_ctr_metrics": ("video_id",),
    "video_snapshots": ("channel", "video_id"),
    "video_traffic_source_metrics": ("channel", "video_id"),
    "videos": ("channel", "video_id"),
}


def _identifiers_from_queue_payload(raw: str) -> set[str]:
    identifiers: set[str] = set()

    def visit(value, key=""):
        if isinstance(value, dict):
            for child_key, child_value in value.items():
                visit(child_value, child_key)
        elif isinstance(value, list):
            for child_value in value:
                visit(child_value, key)
        elif key.endswith("_id") and isinstance(value, str) and value:
            identifiers.add(value)

    visit(json.loads(raw))
    return identifiers


def _load_production_youtube_ids(path: Path) -> set[str]:
    with sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True) as conn:
        identifiers: set[str] = set()
        for table, columns in PRODUCTION_IDENTIFIER_COLUMNS.items():
            actual_columns = {
                row[1]
                for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()
            }
            missing_columns = set(columns) - actual_columns
            if missing_columns:
                raise AssertionError(f"production identifier schema mismatch: {table}")
            for column in columns:
                values = conn.execute(
                    f'SELECT DISTINCT "{column}" FROM "{table}" '
                    f'WHERE "{column}" IS NOT NULL'
                ).fetchall()
                for (value,) in values:
                    if column == "result_json":
                        identifiers.update(_identifiers_from_queue_payload(value))
                    elif value:
                        identifiers.add(str(value))
    return identifiers


def _create_production_identifier_database(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        for table, columns in PRODUCTION_IDENTIFIER_COLUMNS.items():
            definitions = ", ".join(f'"{column}" TEXT' for column in columns)
            conn.execute(f'CREATE TABLE "{table}" ({definitions})')


def _create_scannable_database(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE videos (
                channel TEXT, video_id TEXT, title TEXT, description TEXT,
                published_at TEXT, thumbnail_url TEXT
            );
            CREATE TABLE channel_snapshots (
                captured_at TEXT, channel TEXT, channel_id TEXT
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


def test_deployable_artifact_passes_runtime_production_privacy_scan(tmp_path):
    artifact = build_public_demo_artifact(tmp_path / "artifact")
    db_path = artifact / "demo" / "data" / "demo.db"
    reference = load_production_privacy_reference(PRODUCTION_DB_PATH)

    assert reference.identifiers
    assert reference.content
    assert (
        scan_demo_artifacts(
            artifact,
            db_path,
            known_youtube_ids=reference.identifiers,
            known_production_texts=reference.content,
        )
        == []
    )


def test_demo_privacy_scan_is_independent_of_current_working_directory(
    tmp_path,
    monkeypatch,
):
    artifact_root = tmp_path / "artifact"
    build_public_demo_artifact(artifact_root)
    artifact_db = artifact_root / "demo" / "data" / "demo.db"
    reference = load_production_privacy_reference(PRODUCTION_DB_PATH)
    monkeypatch.chdir(tmp_path)

    assert (
        scan_demo_artifacts(
            artifact_root,
            artifact_db,
            known_youtube_ids=reference.identifiers,
            known_production_texts=reference.content,
        )
        == []
    )


def test_privacy_column_manifest_covers_every_demo_text_and_identifier_column():
    with sqlite3.connect(f"file:{DEMO_DB_PATH.resolve()}?mode=ro", uri=True) as conn:
        actual = {}
        for table in TEXT_COLUMNS:
            actual[table] = {
                row[1]
                for row in conn.execute(f'PRAGMA table_info("{table}")')
                if "TEXT" in str(row[2]).upper()
                or row[1] == "channel"
                or row[1].endswith("_id")
            }

    assert {table: set(columns) for table, columns in TEXT_COLUMNS.items()} == actual


def test_scanner_detects_normalized_exact_production_content_without_echoing_it(
    tmp_path,
):
    root = tmp_path / "demo"
    root.mkdir()
    db_path = tmp_path / "demo.db"
    _create_scannable_database(db_path)
    production_title = "Confidential   Production Episode"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO videos(video_id, title) VALUES (?, ?)",
            ("fictional", "  CONFIDENTIAL production episode  "),
        )
        conn.execute(
            "INSERT INTO channel_snapshots(channel, channel_id) VALUES (?, ?)",
            ("demo", "production-channel-id"),
        )

    errors = scan_demo_artifacts(
        root,
        db_path,
        known_youtube_ids={"production-channel-id"},
        known_production_texts={production_title},
    )

    assert any("forbidden exact production content" in error for error in errors)
    assert any("forbidden production YouTube ID" in error for error in errors)
    assert all("confidential" not in error.lower() for error in errors)
    assert all("production-channel-id" not in error for error in errors)


def test_scanner_detects_short_exact_production_database_content(tmp_path):
    root = tmp_path / "demo"
    root.mkdir()
    db_path = tmp_path / "demo.db"
    _create_scannable_database(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO videos(video_id, title) VALUES (?, ?)",
            ("fictional", "Private"),
        )

    errors = scan_demo_artifacts(
        root,
        db_path,
        known_production_texts={" private "},
    )

    assert any("forbidden exact production content" in error for error in errors)
    assert all("private" not in error.lower() for error in errors)


def test_scanner_detects_utf8_production_content_in_text_artifact(tmp_path):
    root = tmp_path / "demo"
    root.mkdir()
    production_copy = "Private résumé — launch briefing"
    (root / "briefing.txt").write_text(production_copy, encoding="utf-8")
    db_path = tmp_path / "demo.db"
    _create_scannable_database(db_path)

    errors = scan_demo_artifacts(
        root,
        db_path,
        known_production_texts={production_copy},
    )

    assert any("forbidden exact production content" in error for error in errors)
    assert all(production_copy not in error for error in errors)


def test_scanner_preserves_unicode_content_after_invalid_artifact_bytes(tmp_path):
    root = tmp_path / "demo"
    root.mkdir()
    production_copy = "Private résumé — launch briefing"
    (root / "mixed.bin").write_bytes(
        b"\xff" + production_copy.encode("utf-8") + b"\x80"
    )
    db_path = tmp_path / "demo.db"
    _create_scannable_database(db_path)

    errors = scan_demo_artifacts(
        root,
        db_path,
        known_production_texts={production_copy},
    )

    assert any("forbidden exact production content" in error for error in errors)
    assert all(production_copy not in error for error in errors)


def test_scanner_nfkc_normalizes_utf8_artifact_content(tmp_path):
    root = tmp_path / "demo"
    root.mkdir()
    production_copy = "Private Office IV Briefing"
    compatibility_copy = "Ｐｒｉｖａｔｅ Office Ⅳ Briefing"
    (root / "briefing.txt").write_text(compatibility_copy, encoding="utf-8")
    db_path = tmp_path / "demo.db"
    _create_scannable_database(db_path)

    errors = scan_demo_artifacts(
        root,
        db_path,
        known_production_texts={production_copy},
    )

    assert any("forbidden exact production content" in error for error in errors)
    assert all(production_copy not in error for error in errors)
    assert all(compatibility_copy not in error for error in errors)


def test_scanner_redacts_unicode_production_content_from_filename(tmp_path):
    root = tmp_path / "demo"
    root.mkdir()
    production_copy = "Private résumé — launch briefing"
    (root / f"{production_copy}.txt").write_text(
        "youtube_client",
        encoding="utf-8",
    )
    db_path = tmp_path / "demo.db"
    _create_scannable_database(db_path)

    errors = scan_demo_artifacts(
        root,
        db_path,
        known_production_texts={production_copy},
    )

    assert any("<redacted-production-content>" in error for error in errors)
    assert all(production_copy not in error for error in errors)


def test_scanner_redacts_nfkc_equivalent_production_content_from_path(tmp_path):
    root = tmp_path / "demo"
    root.mkdir()
    production_copy = "Private Office IV Briefing"
    compatibility_copy = "Ｐｒｉｖａｔｅ Office Ⅳ Briefing"
    leaked_directory = root / compatibility_copy
    leaked_directory.mkdir()
    (leaked_directory / "leak.py").write_text(
        "youtube_client",
        encoding="utf-8",
    )
    db_path = tmp_path / "demo.db"
    _create_scannable_database(db_path)

    errors = scan_demo_artifacts(
        root,
        db_path,
        known_production_texts={production_copy},
    )

    assert any("<redacted-production-content>" in error for error in errors)
    assert all(production_copy not in error for error in errors)
    assert all(compatibility_copy not in error for error in errors)


@pytest.mark.parametrize("path_kind", ["file", "directory"])
def test_scanner_redacts_short_unicode_production_content_from_relative_paths(
    tmp_path,
    path_kind,
):
    root = tmp_path / "demo"
    root.mkdir()
    production_copy = "Café"
    if path_kind == "file":
        leak_path = root / f"{production_copy}.py"
    else:
        leaked_directory = root / production_copy
        leaked_directory.mkdir()
        leak_path = leaked_directory / "leak.py"
    leak_path.write_text("youtube_client", encoding="utf-8")
    db_path = tmp_path / "demo.db"
    _create_scannable_database(db_path)

    errors = scan_demo_artifacts(
        root,
        db_path,
        known_production_texts={production_copy},
    )

    assert any("forbidden token youtube_client" in error for error in errors)
    assert any("<redacted-production-content>" in error for error in errors)
    assert all(production_copy not in error for error in errors)


def test_scanner_redacts_short_unicode_production_content_from_source_root(tmp_path):
    production_copy = "Café"
    missing_root = tmp_path / production_copy
    db_path = tmp_path / "demo.db"
    _create_scannable_database(db_path)

    errors = scan_demo_artifacts(
        missing_root,
        db_path,
        known_production_texts={production_copy},
    )

    assert any("source root is missing" in error for error in errors)
    assert any("<redacted-production-content>" in error for error in errors)
    assert all(production_copy not in error for error in errors)


def test_scanner_decodes_escaped_unicode_in_publishing_queue_json(tmp_path):
    root = tmp_path / "demo"
    root.mkdir()
    db_path = tmp_path / "demo.db"
    _create_scannable_database(db_path)
    production_copy = "Private résumé — launch briefing"
    payload = json.dumps(
        {"ranked_videos": [{"analysis": {"copy": production_copy}}]},
        ensure_ascii=True,
    )
    assert "\\u" in payload
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO publishing_queue(analyzed_at, channel, result_json) "
            "VALUES (?, ?, ?)",
            ("now", "demo", payload),
        )

    errors = scan_demo_artifacts(
        root,
        db_path,
        known_production_texts={production_copy},
    )

    assert any("forbidden exact production content" in error for error in errors)
    assert all(production_copy not in error for error in errors)


def test_secondary_identifier_table_values_are_detected_and_redacted(tmp_path):
    production_db = tmp_path / "production.db"
    _create_production_identifier_database(production_db)
    secondary_id = "SecOndary_1"
    with sqlite3.connect(production_db) as conn:
        conn.execute(
            "INSERT INTO video_traffic_source_metrics(video_id) VALUES (?)",
            (secondary_id,),
        )
    production_ids = _load_production_youtube_ids(production_db)
    root = tmp_path / "demo"
    root.mkdir()
    (root / "artifact.unknown").write_bytes(
        f"prefix{secondary_id}suffix".encode("ascii")
    )
    db_path = tmp_path / "demo.db"
    _create_scannable_database(db_path)

    errors = scan_demo_artifacts(
        root,
        db_path,
        known_youtube_ids=production_ids,
    )

    assert secondary_id in production_ids
    assert any("production YouTube ID" in error for error in errors)
    assert all(secondary_id not in error for error in errors)


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


@pytest.mark.parametrize(
    ("filename", "contents"),
    [
        ("artifact.unknown", b"youtube_client"),
        ("extensionless", b"youtube_client"),
        ("artifact.bin", b"\x00\xffyoutube_client\x00"),
    ],
)
def test_scanner_checks_every_regular_artifact(tmp_path, filename, contents):
    root = tmp_path / "demo"
    root.mkdir()
    (root / filename).write_bytes(contents)
    db_path = tmp_path / "demo.db"
    _create_scannable_database(db_path)

    errors = scan_demo_artifacts(root, db_path)

    assert any("youtube_client" in error for error in errors)


def test_scanner_checks_relative_paths_for_brands(tmp_path):
    root = tmp_path / "demo"
    branded_directory = root / "Club Genius"
    branded_directory.mkdir(parents=True)
    (branded_directory / "artifact").write_bytes(b"")
    db_path = tmp_path / "demo.db"
    _create_scannable_database(db_path)

    errors = scan_demo_artifacts(root, db_path)

    assert any(
        "forbidden" in error and "club genius" in error.lower()
        for error in errors
    )


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


@pytest.mark.parametrize(
    "remote_reference",
    [
        '<img src="//cdn.example.invalid/image.png">',
        '<image href="//cdn.example.invalid/image.svg" />',
        ".hero { background-image: url(//cdn.example.invalid/image.png); }",
        "asset = 's3://fictional-bucket/image.png'",
    ],
)
def test_scanner_detects_protocol_relative_and_remote_scheme_references(
    tmp_path,
    remote_reference,
):
    root = tmp_path / "demo"
    root.mkdir()
    (root / "artifact").write_text(remote_reference, encoding="utf-8")
    db_path = tmp_path / "demo.db"
    _create_scannable_database(db_path)

    errors = scan_demo_artifacts(root, db_path)

    assert any("remote URL" in error for error in errors)


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


def test_scanner_detects_adjacent_ids_and_redacts_id_bearing_filenames(tmp_path):
    root = tmp_path / "demo"
    root.mkdir()
    known_id = "Adjacent__1"
    (root / f"before{known_id}after.bin").write_bytes(
        f"prefix{known_id}suffix".encode("ascii")
    )
    db_path = tmp_path / "demo.db"
    _create_scannable_database(db_path)

    errors = scan_demo_artifacts(
        root,
        db_path,
        known_youtube_ids={known_id},
    )

    assert any("production YouTube ID" in error for error in errors)
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
    production_copy = "Private ASCII production briefing"
    (root / "leak.bin").write_bytes(
        b"\x89PNG\r\n\x1a\n\xffyoutube_client\x00"
        + production_copy.encode("ascii")
        + b"\x80"
    )
    db_path = tmp_path / "demo.db"
    _create_scannable_database(db_path)

    errors = scan_demo_artifacts(
        root,
        db_path,
        known_production_texts={production_copy},
    )

    assert any("youtube_client" in error for error in errors)
    assert any("forbidden exact production content" in error for error in errors)
    assert all(production_copy not in error for error in errors)
