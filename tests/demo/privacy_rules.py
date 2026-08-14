"""Test-only rules for enforcing the deployable demo's privacy boundary."""

from __future__ import annotations

from collections.abc import Iterable
from contextlib import closing
from pathlib import Path
import re
import sqlite3


FORBIDDEN_SOURCE_TOKENS = (
    "youtube_client",
    "services.youtube_analytics",
    "services.google_ads",
    "googleapiclient",
    "google.oauth",
    ".streamlit/secrets",
    "st.secrets",
    "oauth_credentials",
    "credentials.json",
    "client_secret",
    "service_account",
    "from db import DB_PATH",
    "data.db",
    "dashboard_password",
)

FORBIDDEN_BRAND_TOKENS = (
    "the human workforce",
    "club genius",
    "kzak",
    "techy chef",
)

TEXT_COLUMNS = {
    "videos": (
        "channel",
        "video_id",
        "title",
        "description",
        "published_at",
        "thumbnail_url",
    ),
    "playlists": (
        "channel",
        "playlist_id",
        "title",
        "description",
        "published_at",
        "thumbnail_url",
    ),
    "publishing_queue": (
        "analyzed_at",
        "channel",
        "result_json",
    ),
    "queue_recommendations": (
        "channel",
        "video_id",
        "first_recommended_at",
        "recommended_publish_date",
        "theme",
        "why_now",
    ),
    "ci_content_assets": (
        "asset_id",
        "channel",
        "video_id",
        "video_title",
        "asset_type",
        "title",
        "body",
        "generated_at",
        "status",
        "approved_at",
        "scheduled_for",
        "notes",
    ),
}

_REMOTE_SCHEME_PATTERN = re.compile(
    r"(?:https?|ftps?|s3|gs|wss?)://[^\s\"'<>)}\]]+",
    re.IGNORECASE,
)
_PROTOCOL_RELATIVE_PATTERN = re.compile(
    r"(?<!:)//(?:"
    r"localhost|"
    r"(?:\d{1,3}\.){3}\d{1,3}|"
    r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}"
    r")(?::\d{1,5})?(?:/[^\s\"'<>)}\]]*)?",
    re.IGNORECASE,
)
_ALLOWED_SOURCE_URLS = {
    "http://www.w3.org/1999/xlink",
    "http://www.w3.org/2000/svg",
}
_ASCII_TEXT_PATTERN = re.compile(rb"[\x09\x0a\x0d\x20-\x7e]+")


def _production_id_pattern(known_youtube_ids: Iterable[str]) -> re.Pattern[str] | None:
    identifiers = sorted(
        {str(identifier) for identifier in known_youtube_ids if str(identifier)},
        key=lambda value: (-len(value), value),
    )
    if not identifiers:
        return None
    alternatives = "|".join(re.escape(identifier) for identifier in identifiers)
    return re.compile(alternatives)


def _has_remote_reference(text: str, *, allow_standard_urls: bool) -> bool:
    for match in _REMOTE_SCHEME_PATTERN.finditer(text):
        if not allow_standard_urls or match.group(0) not in _ALLOWED_SOURCE_URLS:
            return True
    return _PROTOCOL_RELATIVE_PATTERN.search(text) is not None


def _redact_production_ids(
    value: str,
    production_id_pattern: re.Pattern[str] | None,
) -> str:
    if production_id_pattern is None:
        return value
    return production_id_pattern.sub("<redacted-production-id>", value)


def _ascii_text(raw: bytes) -> str:
    return "\n".join(
        match.group(0).decode("ascii")
        for match in _ASCII_TEXT_PATTERN.finditer(raw)
    )


def _scan_text(
    location: str,
    text: str,
    production_id_pattern: re.Pattern[str] | None,
    *,
    allow_standard_urls: bool,
    database_text: bool = False,
) -> list[str]:
    errors: list[str] = []
    lowered = text.lower()
    for token in FORBIDDEN_SOURCE_TOKENS:
        if token.lower() in lowered:
            errors.append(f"{location}: forbidden token {token}")
    for token in FORBIDDEN_BRAND_TOKENS:
        if token.lower() in lowered:
            category = "forbidden brand" if database_text else "forbidden token"
            errors.append(f"{location}: {category} {token}")

    if _has_remote_reference(text, allow_standard_urls=allow_standard_urls):
        errors.append(f"{location}: remote URL")

    if production_id_pattern is not None and production_id_pattern.search(text):
        errors.append(f"{location}: forbidden production YouTube ID")
    return errors


def _scan_source_tree(
    root: Path,
    db_path: Path,
    production_id_pattern: re.Pattern[str] | None,
) -> list[str]:
    if not root.exists():
        return [f"{root}: source root is missing"]
    if not root.is_dir():
        return [f"{root}: source root is not a directory"]

    errors: list[str] = []
    try:
        paths = sorted(root.rglob("*"))
    except OSError as exc:
        return [f"{root}: source scan failed ({exc.__class__.__name__})"]

    for path in paths:
        relative_location = str(path.relative_to(root))
        normalized_location = path.relative_to(root).as_posix()
        path_errors = _scan_text(
            relative_location,
            normalized_location,
            production_id_pattern,
            allow_standard_urls=True,
        )
        errors.extend(
            error.replace(": forbidden token", ": forbidden path token", 1)
            for error in path_errors
        )

        if path.is_dir():
            continue
        if not path.is_file():
            errors.append(f"{relative_location}: unsupported artifact")
            continue
        if path.resolve() == db_path.resolve():
            continue
        try:
            raw = path.read_bytes()
        except OSError as exc:
            errors.append(
                f"{relative_location}: source read failed ({exc.__class__.__name__})"
            )
            continue
        text = _ascii_text(raw)
        errors.extend(
            _scan_text(
                relative_location,
                text,
                production_id_pattern,
                allow_standard_urls=True,
            )
        )
    return errors


def _scan_database(
    db_path: Path,
    production_id_pattern: re.Pattern[str] | None,
) -> list[str]:
    if not db_path.exists():
        return [f"{db_path}: database is missing"]
    if not db_path.is_file():
        return [f"{db_path}: database path is not a file"]

    errors: list[str] = []
    try:
        conn = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True)
    except (OSError, sqlite3.Error) as exc:
        return [f"{db_path}: database scan failed ({exc.__class__.__name__})"]

    with closing(conn):
        try:
            conn.execute("PRAGMA schema_version").fetchone()
        except sqlite3.Error as exc:
            return [f"{db_path}: database scan failed ({exc.__class__.__name__})"]

        for table, columns in TEXT_COLUMNS.items():
            try:
                actual_columns = {
                    row[1]
                    for row in conn.execute(
                        f'PRAGMA table_info("{table}")'
                    ).fetchall()
                }
                missing_columns = set(columns) - actual_columns
                if missing_columns:
                    errors.append(f"{table}: database scan failed (missing columns)")
                    continue
                expression = " || ' ' || ".join(
                    f"CAST(COALESCE(\"{column}\", '') AS TEXT)"
                    for column in columns
                )
                rows = conn.execute(f'SELECT {expression} FROM "{table}"').fetchall()
            except sqlite3.Error as exc:
                errors.append(
                    f"{table}: database scan failed ({exc.__class__.__name__})"
                )
                continue

            for index, row in enumerate(rows):
                value = row[0] or ""
                errors.extend(
                    _scan_text(
                        f"{table}[{index}]",
                        value,
                        production_id_pattern,
                        allow_standard_urls=True,
                        database_text=True,
                    )
                )
    return errors


def scan_demo_artifacts(
    root: Path,
    db_path: Path,
    *,
    known_youtube_ids: Iterable[str] = (),
) -> list[str]:
    """Return privacy-boundary violations without raising on bad artifacts."""
    root = Path(root)
    db_path = Path(db_path)
    production_id_pattern = _production_id_pattern(known_youtube_ids)
    errors = _scan_source_tree(root, db_path, production_id_pattern) + _scan_database(
        db_path,
        production_id_pattern,
    )
    return [
        _redact_production_ids(error, production_id_pattern)
        for error in errors
    ]
