"""Build the complete, allowlisted public-demo deployment directory."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil


SOURCE_REVISION = "public-demo-v1"
MANIFEST_NAME = "artifact-manifest.json"

# Every deployed byte other than the generated manifest must be named here.
# In particular, this list intentionally excludes the production application,
# database, pages, services, credentials, generator, tests, and documentation.
ARTIFACT_FILES: tuple[tuple[str, str], ...] = (
    ("demo/app.py", "app.py"),
    ("demo/requirements.txt", "requirements.txt"),
    ("demo/__init__.py", "demo/__init__.py"),
    ("demo/analytics.py", "demo/analytics.py"),
    ("demo/config.py", "demo/config.py"),
    ("demo/report_data.py", "demo/report_data.py"),
    ("demo/ui.py", "demo/ui.py"),
    ("demo/data/demo.db", "demo/data/demo.db"),
    ("demo/pages/__init__.py", "demo/pages/__init__.py"),
    ("demo/pages/content_intelligence.py", "demo/pages/content_intelligence.py"),
    ("demo/pages/daily_analytics.py", "demo/pages/daily_analytics.py"),
    ("demo/pages/organic_momentum.py", "demo/pages/organic_momentum.py"),
    ("demo/pages/overview.py", "demo/pages/overview.py"),
    ("demo/pages/promotion_intelligence.py", "demo/pages/promotion_intelligence.py"),
    (
        "demo/pages/qualifying_watch_hours.py",
        "demo/pages/qualifying_watch_hours.py",
    ),
    (
        "demo/pages/video_render_comparisons.py",
        "demo/pages/video_render_comparisons.py",
    ),
    ("analytics/__init__.py", "analytics/__init__.py"),
    (
        "analytics/organic_momentum_scoring.py",
        "analytics/organic_momentum_scoring.py",
    ),
    ("analytics/promotion_efficiency.py", "analytics/promotion_efficiency.py"),
    ("models/__init__.py", "models/__init__.py"),
    ("models/organic_momentum.py", "models/organic_momentum.py"),
    ("models/promotion.py", "models/promotion.py"),
    ("promotion_intelligence/__init__.py", "promotion_intelligence/__init__.py"),
    (
        "promotion_intelligence/promotion_prediction.py",
        "promotion_intelligence/promotion_prediction.py",
    ),
    (
        "promotion_intelligence/promotion_roi.py",
        "promotion_intelligence/promotion_roi.py",
    ),
    (
        "promotion_intelligence/recommendation_engine.py",
        "promotion_intelligence/recommendation_engine.py",
    ),
    (
        "promotion_intelligence/recommendation_models.py",
        "promotion_intelligence/recommendation_models.py",
    ),
    ("content_intelligence/__init__.py", "content_intelligence/__init__.py"),
    ("content_intelligence/config.py", "content_intelligence/config.py"),
    ("content_intelligence/models.py", "content_intelligence/models.py"),
    (
        "content_intelligence/scoring/__init__.py",
        "content_intelligence/scoring/__init__.py",
    ),
    (
        "content_intelligence/scoring/scorer.py",
        "content_intelligence/scoring/scorer.py",
    ),
    ("projections.py", "projections.py"),
    ("retention.py", "retention.py"),
)


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_public_demo_artifact(output: Path) -> Path:
    """Copy the audited runtime closure into a new deterministic directory."""
    output = Path(output)
    if output.exists():
        raise FileExistsError(f"artifact output already exists: {output}")

    repository_root = _repository_root()
    output.mkdir(parents=True)
    entries: list[dict[str, object]] = []
    for source_name, artifact_name in ARTIFACT_FILES:
        source = repository_root / source_name
        if not source.is_file():
            raise FileNotFoundError(f"allowlisted source is missing: {source_name}")
        destination = output / artifact_name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
        entries.append(
            {
                "path": artifact_name,
                "sha256": _digest(destination),
                "size": destination.stat().st_size,
            }
        )

    manifest = {
        "entrypoint": "app.py",
        "files": sorted(entries, key=lambda entry: str(entry["path"])),
        "launch": "streamlit run app.py",
        "source_revision": SOURCE_REVISION,
    }
    (output / MANIFEST_NAME).write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return output


def verify_artifact(root: Path) -> list[str]:
    """Return manifest-integrity errors for a built deployment directory."""
    root = Path(root)
    manifest_path = root / MANIFEST_NAME
    if not manifest_path.is_file():
        return ["artifact manifest is missing"]
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        entries = manifest["files"]
    except (OSError, UnicodeError, json.JSONDecodeError, KeyError, TypeError):
        return ["artifact manifest is invalid"]

    expected = {str(entry.get("path", "")): entry for entry in entries}
    actual = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and path != manifest_path
    }
    errors: list[str] = []
    if set(expected) != actual:
        errors.append("artifact contents do not match the manifest")

    for relative_name, entry in sorted(expected.items()):
        relative = PurePosixPath(relative_name)
        if not relative_name or relative.is_absolute() or ".." in relative.parts:
            errors.append("artifact manifest contains an unsafe path")
            continue
        path = root / relative_name
        if not path.is_file():
            errors.append(f"manifested file is missing: {relative_name}")
            continue
        if entry.get("size") != path.stat().st_size:
            errors.append(f"manifested size mismatch: {relative_name}")
        if entry.get("sha256") != _digest(path):
            errors.append(f"manifested digest mismatch: {relative_name}")
    return errors


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> None:
    arguments = _parse_args()
    artifact = build_public_demo_artifact(arguments.output)
    print(artifact.resolve())


if __name__ == "__main__":
    main()
