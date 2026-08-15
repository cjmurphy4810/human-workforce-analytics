"""Test-only rules for enforcing the deployable demo's privacy boundary."""

from __future__ import annotations

from collections.abc import Iterable
from contextlib import closing
from dataclasses import dataclass
import json
from pathlib import Path
import re
import sqlite3
import unicodedata


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
    "channel_snapshots": (
        "captured_at",
        "channel",
        "channel_id",
    ),
    "channel_traffic_sources": (
        "metric_date",
        "channel",
        "traffic_source_type",
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
    "ci_video_scores": (
        "scored_at",
        "channel",
        "video_id",
        "tier",
    ),
    "daily_channel_metrics": (
        "metric_date",
        "channel",
    ),
    "daily_geo_metrics": (
        "metric_date",
        "channel",
        "country_code",
    ),
    "daily_video_metrics": (
        "metric_date",
        "channel",
        "video_id",
    ),
    "playlist_videos": (
        "channel",
        "playlist_id",
        "video_id",
    ),
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
    "retention_buckets": (
        "channel",
        "video_id",
        "window_start",
        "window_end",
        "window_kind",
        "fetched_at",
    ),
    "video_snapshots": (
        "captured_at",
        "channel",
        "video_id",
    ),
    "video_traffic_source_metrics": (
        "metric_date",
        "channel",
        "video_id",
        "traffic_source_type",
    ),
}

PRODUCTION_CONTENT_COLUMNS = {
    "videos": ("title", "description"),
    "playlists": ("title", "description"),
    "queue_recommendations": ("theme", "why_now"),
    "ci_content_assets": ("video_title", "title", "body", "notes"),
}


@dataclass(frozen=True)
class ProductionPrivacyReference:
    identifiers: frozenset[str]
    content: frozenset[str]

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
_MIN_DISTINCTIVE_CONTENT_LENGTH = 12


def _production_id_pattern(known_youtube_ids: Iterable[str]) -> re.Pattern[str] | None:
    identifiers = sorted(
        {str(identifier) for identifier in known_youtube_ids if str(identifier)},
        key=lambda value: (-len(value), value),
    )
    if not identifiers:
        return None
    alternatives = "|".join(re.escape(identifier) for identifier in identifiers)
    return re.compile(alternatives)


def _normalize_content(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", str(value)).casefold()
    return " ".join(normalized.split())


def _production_content_values(values: Iterable[str]) -> frozenset[str]:
    """Return distinctive values safe for embedded artifact-text matching."""
    return frozenset(
        {
            normalized
            for value in values
            if (normalized := _normalize_content(value))
            and len(normalized) >= _MIN_DISTINCTIVE_CONTENT_LENGTH
        }
    )


def _visit_queue_values(
    value,
    *,
    key: str = "",
    identifiers: set[str],
    content: set[str],
) -> None:
    if isinstance(value, dict):
        for child_key, child in value.items():
            _visit_queue_values(
                child,
                key=str(child_key),
                identifiers=identifiers,
                content=content,
            )
    elif isinstance(value, list):
        for child in value:
            _visit_queue_values(
                child,
                key=key,
                identifiers=identifiers,
                content=content,
            )
    elif isinstance(value, str) and value:
        if key.endswith("_id"):
            identifiers.add(value)
        if key in {"news_headlines", "theme", "title", "why_now"}:
            content.add(value)


def load_production_privacy_reference(path: Path) -> ProductionPrivacyReference:
    """Load identifiers and exact content directly from the production fixture."""
    path = Path(path)
    identifiers: set[str] = set()
    content: set[str] = set()
    with closing(
        sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
    ) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
        for table in tables:
            columns = [
                row[1]
                for row in connection.execute(
                    f'PRAGMA table_info("{table}")'
                ).fetchall()
            ]
            identifier_columns = [
                column
                for column in columns
                if column == "channel" or column.endswith("_id")
            ]
            for column in identifier_columns:
                values = connection.execute(
                    f'SELECT DISTINCT "{column}" FROM "{table}" '
                    f'WHERE "{column}" IS NOT NULL'
                ).fetchall()
                identifiers.update(str(value) for (value,) in values if value)

        for table, columns in PRODUCTION_CONTENT_COLUMNS.items():
            if table not in tables:
                raise AssertionError(f"production privacy schema mismatch: {table}")
            actual_columns = {
                row[1]
                for row in connection.execute(
                    f'PRAGMA table_info("{table}")'
                ).fetchall()
            }
            if not set(columns).issubset(actual_columns):
                raise AssertionError(f"production privacy schema mismatch: {table}")
            for column in columns:
                values = connection.execute(
                    f'SELECT DISTINCT "{column}" FROM "{table}" '
                    f'WHERE "{column}" IS NOT NULL'
                ).fetchall()
                content.update(str(value) for (value,) in values if value)

        if "publishing_queue" not in tables:
            raise AssertionError("production privacy schema mismatch: publishing_queue")
        for (raw,) in connection.execute(
            "SELECT result_json FROM publishing_queue WHERE result_json IS NOT NULL"
        ).fetchall():
            _visit_queue_values(
                json.loads(raw),
                identifiers=identifiers,
                content=content,
            )

    return ProductionPrivacyReference(
        identifiers=frozenset(identifiers),
        content=frozenset(content),
    )


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


def _redact_production_content_location(
    location: str,
    production_content: frozenset[str],
) -> str:
    normalized_location = _normalize_content(location)
    if any(value in normalized_location for value in production_content):
        return "<redacted-production-content>"
    return location


def _ascii_text(raw: bytes) -> str:
    return "\n".join(
        match.group(0).decode("ascii")
        for match in _ASCII_TEXT_PATTERN.finditer(raw)
    )


def _decode_artifact_text(raw: bytes) -> str:
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        decoded = raw.decode("utf-8", errors="replace")
        return f"{decoded}\n{_ascii_text(raw)}"


def _iter_json_string_leaves(value):
    if isinstance(value, dict):
        for child in value.values():
            yield from _iter_json_string_leaves(child)
    elif isinstance(value, list):
        for child in value:
            yield from _iter_json_string_leaves(child)
    elif isinstance(value, str):
        yield value


def _scan_text(
    location: str,
    text: str,
    production_id_pattern: re.Pattern[str] | None,
    production_content: frozenset[str],
    *,
    allow_standard_urls: bool,
    database_text: bool = False,
    embedded_content: bool = True,
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
    normalized = _normalize_content(text)
    has_production_content = False
    if normalized and production_content:
        if embedded_content:
            has_production_content = any(
                value in normalized for value in production_content
            )
        else:
            has_production_content = normalized in production_content
    if has_production_content:
        errors.append(f"{location}: forbidden exact production content")
    return errors


def _scan_publishing_queue_json(
    location: str,
    raw_json: str,
    production_id_pattern: re.Pattern[str] | None,
    production_content: frozenset[str],
) -> list[str]:
    errors = _scan_text(
        location,
        raw_json,
        production_id_pattern,
        frozenset(),
        allow_standard_urls=True,
        database_text=True,
    )
    try:
        payload = json.loads(raw_json)
    except (json.JSONDecodeError, TypeError):
        errors.extend(
            _scan_text(
                location,
                raw_json,
                production_id_pattern,
                production_content,
                allow_standard_urls=True,
                database_text=True,
            )
        )
        errors.append(f"{location}: database scan failed (invalid JSON)")
        return list(dict.fromkeys(errors))

    for value in _iter_json_string_leaves(payload):
        errors.extend(
            _scan_text(
                location,
                value,
                production_id_pattern,
                production_content,
                allow_standard_urls=True,
                database_text=True,
                embedded_content=False,
            )
        )
    return list(dict.fromkeys(errors))


def _scan_source_tree(
    root: Path,
    db_path: Path,
    production_id_pattern: re.Pattern[str] | None,
    production_content: frozenset[str],
) -> list[str]:
    root_location = _redact_production_content_location(
        str(root),
        production_content,
    )
    if not root.exists():
        return [f"{root_location}: source root is missing"]
    if not root.is_dir():
        return [f"{root_location}: source root is not a directory"]

    errors: list[str] = []
    try:
        paths = sorted(root.rglob("*"))
    except OSError as exc:
        return [
            f"{root_location}: source scan failed ({exc.__class__.__name__})"
        ]

    for path in paths:
        relative_path = path.relative_to(root)
        relative_location = _redact_production_content_location(
            str(relative_path),
            production_content,
        )
        normalized_location = relative_path.as_posix()
        path_errors = _scan_text(
            relative_location,
            normalized_location,
            production_id_pattern,
            production_content,
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
        text = _decode_artifact_text(raw)
        errors.extend(
            _scan_text(
                relative_location,
                text,
                production_id_pattern,
                production_content,
                allow_standard_urls=True,
            )
        )
    return errors


def _scan_database(
    db_path: Path,
    production_id_pattern: re.Pattern[str] | None,
    production_content: frozenset[str],
) -> list[str]:
    db_location = _redact_production_content_location(
        str(db_path),
        production_content,
    )
    if not db_path.exists():
        return [f"{db_location}: database is missing"]
    if not db_path.is_file():
        return [f"{db_location}: database path is not a file"]

    errors: list[str] = []
    try:
        conn = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True)
    except (OSError, sqlite3.Error) as exc:
        return [
            f"{db_location}: database scan failed ({exc.__class__.__name__})"
        ]

    with closing(conn):
        try:
            conn.execute("PRAGMA schema_version").fetchone()
        except sqlite3.Error as exc:
            return [
                f"{db_location}: database scan failed ({exc.__class__.__name__})"
            ]

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
                selected = ", ".join(f'"{column}"' for column in columns)
                rows = conn.execute(f'SELECT {selected} FROM "{table}"').fetchall()
            except sqlite3.Error as exc:
                errors.append(
                    f"{table}: database scan failed ({exc.__class__.__name__})"
                )
                continue

            for index, row in enumerate(rows):
                for column, raw_value in zip(columns, row):
                    value = str(raw_value or "")
                    if table == "publishing_queue" and column == "result_json":
                        errors.extend(
                            _scan_publishing_queue_json(
                                f"{table}[{index}]",
                                value,
                                production_id_pattern,
                                production_content,
                            )
                        )
                        continue
                    errors.extend(
                        _scan_text(
                            f"{table}[{index}]",
                            value,
                            production_id_pattern,
                            production_content,
                            allow_standard_urls=True,
                            database_text=True,
                            embedded_content=False,
                        )
                    )
    return errors


def scan_demo_artifacts(
    root: Path,
    db_path: Path,
    *,
    known_youtube_ids: Iterable[str] = (),
    known_production_texts: Iterable[str] = (),
) -> list[str]:
    """Return privacy-boundary violations without raising on bad artifacts."""
    root = Path(root)
    db_path = Path(db_path)
    production_id_pattern = _production_id_pattern(known_youtube_ids)
    all_production_content = frozenset(
        normalized
        for value in known_production_texts
        if (normalized := _normalize_content(value))
    )
    distinctive_production_content = _production_content_values(
        all_production_content
    )
    errors = _scan_source_tree(
        root,
        db_path,
        production_id_pattern,
        distinctive_production_content,
    ) + _scan_database(
        db_path,
        production_id_pattern,
        all_production_content,
    )
    return [
        _redact_production_ids(error, production_id_pattern)
        for error in errors
    ]
