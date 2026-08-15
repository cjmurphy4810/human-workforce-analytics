from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from demo.build_artifact import build_public_demo_artifact, verify_artifact


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
ROUTES = (
    "demo/pages/overview.py",
    "demo/pages/daily_analytics.py",
    "demo/pages/qualifying_watch_hours.py",
    "demo/pages/organic_momentum.py",
    "demo/pages/promotion_intelligence.py",
    "demo/pages/content_intelligence.py",
    "demo/pages/video_render_comparisons.py",
)
ROUTE_HEADINGS = {
    "demo/pages/overview.py": "AI Engineering Genius Analytics",
    "demo/pages/daily_analytics.py": "Daily Analytics",
    "demo/pages/qualifying_watch_hours.py": "Qualifying Watch Hours",
    "demo/pages/organic_momentum.py": "Organic Momentum",
    "demo/pages/promotion_intelligence.py": "Promotion Intelligence",
    "demo/pages/content_intelligence.py": "Content Intelligence",
    "demo/pages/video_render_comparisons.py": "Video Render Comparisons",
}
FORBIDDEN_ARTIFACT_PARTS = {
    ".streamlit",
    "data.db",
    "docs",
    "pages",
    "README.md",
    "scripts",
    "services",
}
MAINTENANCE_MESSAGE = (
    "The simulated analytics workspace is temporarily unavailable. "
    "Please try again later."
)


def _artifact_bytes(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_builder_produces_deterministic_manifested_allowlisted_artifact(tmp_path):
    first = build_public_demo_artifact(tmp_path / "first")
    second = build_public_demo_artifact(tmp_path / "second")

    assert _artifact_bytes(first) == _artifact_bytes(second)
    assert verify_artifact(first) == []

    manifest = json.loads((first / "artifact-manifest.json").read_text())
    manifested_paths = {entry["path"] for entry in manifest["files"]}
    actual_paths = set(_artifact_bytes(first)) - {"artifact-manifest.json"}
    assert manifested_paths == actual_paths
    for entry in manifest["files"]:
        payload = (first / entry["path"]).read_bytes()
        assert entry["sha256"] == hashlib.sha256(payload).hexdigest()
        assert entry["size"] == len(payload)

    assert manifest["entrypoint"] == "app.py"
    assert manifest["launch"] == "streamlit run app.py"
    assert manifest["source_revision"] == "public-demo-v1"


def test_builder_cli_creates_a_verified_artifact(tmp_path):
    output = tmp_path / "public-demo"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "demo.build_artifact",
            "--output",
            str(output),
        ],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == str(output.resolve())
    assert verify_artifact(output) == []


def test_artifact_excludes_production_surfaces_and_runtime_bootstrap(tmp_path):
    artifact = build_public_demo_artifact(tmp_path / "artifact")
    paths = set(_artifact_bytes(artifact))

    assert "demo/data/demo.db" in paths
    assert "demo/pages/qualifying_watch_hours.py" in paths
    assert "qualifying_watch_hours.py" not in paths
    assert "demo/generate_data.py" not in paths
    assert "demo/db.py" not in paths
    assert "demo/README.md" not in paths
    assert "demo/assets/channel_mark.svg" not in paths
    assert "analytics/organic_momentum.py" not in paths
    assert "analytics/organic_momentum_scoring.py" in paths
    assert "content_intelligence/service.py" not in paths
    assert "content_intelligence/scoring/engine.py" not in paths
    assert not any(
        part in FORBIDDEN_ARTIFACT_PARTS
        for path in paths
        for part in Path(path).parts
        if not path.startswith("demo/pages/")
    )

    entrypoint = (artifact / "app.py").read_text()
    assert "sys.path" not in entrypoint
    assert "demo/pages/" in entrypoint

    combined_source = "\n".join(
        path.read_text(errors="ignore")
        for path in artifact.rglob("*.py")
    )
    assert "services.google_ads" not in combined_source
    assert "youtube_client" not in combined_source
    for path in artifact.rglob("*.py"):
        if path.relative_to(artifact).as_posix() != "demo/report_data.py":
            assert "sqlite3" not in path.read_text(), path


def test_artifact_runs_every_route_without_source_repository_on_sys_path(tmp_path):
    artifact = build_public_demo_artifact(tmp_path / "artifact")
    fixture = artifact / "demo" / "data" / "demo.db"
    fixture_hash = hashlib.sha256(fixture.read_bytes()).hexdigest()
    probe = """
from pathlib import Path
import sys
from streamlit.testing.v1 import AppTest

artifact = Path.cwd().resolve()
source_repository = Path(%r).resolve()

def reject_source_repository_reads(event, arguments):
    if event != "open" or not arguments or not isinstance(arguments[0], (str, bytes)):
        return
    try:
        candidate = Path(arguments[0]).resolve()
    except (OSError, TypeError, ValueError):
        return
    if candidate == source_repository or source_repository in candidate.parents:
        raise AssertionError("artifact attempted to read the source repository")

sys.addaudithook(reject_source_repository_reads)
for item in sys.path:
    if item and Path(item).resolve() == source_repository:
        raise AssertionError("source repository leaked onto sys.path")

app = AppTest.from_file(artifact / "app.py", default_timeout=30).run()
assert not app.exception, app.exception
for route in %r:
    app.switch_page(route).run(timeout=30)
    assert not app.exception, (route, app.exception)
    headings = " ".join(
        str(element.value) for element in (*app.title, *app.header)
    )
    assert %r[route] in headings, (route, headings)
    assert any("simulated" in str(element.value).lower() for element in app.info)
    buttons = {button.label: button for button in app.button}
    assert buttons["AI Engineering Genius"].disabled is False
    for label in (
        "Automation Architects",
        "Future Systems Lab",
        "Practical AI Studio",
    ):
        assert buttons[label].disabled is True, (route, label)
print("seven routes passed")
""" % (str(REPOSITORY_ROOT), ROUTES, ROUTE_HEADINGS)
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in {"PYTHONPATH", "PYTHONHOME"}
    }
    result = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=artifact,
        env=environment,
        capture_output=True,
        text=True,
        timeout=180,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().endswith("seven routes passed")
    assert "use_container_width" not in result.stderr
    assert hashlib.sha256(fixture.read_bytes()).hexdigest() == fixture_hash


@pytest.mark.parametrize("fixture_state", ["missing", "malformed", "unreadable"])
def test_every_artifact_route_handles_unavailable_fixture_without_mutating_it(
    tmp_path,
    fixture_state,
):
    artifact = build_public_demo_artifact(tmp_path / "artifact")
    fixture = artifact / "demo" / "data" / "demo.db"
    if fixture_state == "missing":
        fixture.unlink()
        before_hash = None
    elif fixture_state == "malformed":
        fixture.write_bytes(b"not a SQLite database")
        before_hash = hashlib.sha256(fixture.read_bytes()).hexdigest()
    else:
        before_hash = hashlib.sha256(fixture.read_bytes()).hexdigest()
        fixture.chmod(0)

    probe = """
from pathlib import Path
from streamlit.testing.v1 import AppTest

artifact = Path.cwd().resolve()
expected = %r
routes = %r
app = AppTest.from_file(artifact / "app.py", default_timeout=30).run()
for route in routes:
    if route != routes[0]:
        app.switch_page(route).run(timeout=30)
    assert not app.exception, (route, app.exception)
    rendered = " ".join(
        str(element.value)
        for kind in (app.warning, app.info, app.error, app.markdown)
        for element in kind
    )
    assert expected in rendered, (route, rendered)
    assert ".db" not in rendered.lower(), (route, rendered)
print("unavailable routes passed")
""" % (MAINTENANCE_MESSAGE, ROUTES)
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in {"PYTHONPATH", "PYTHONHOME"}
    }
    try:
        result = subprocess.run(
            [sys.executable, "-c", probe],
            cwd=artifact,
            env=environment,
            capture_output=True,
            text=True,
            timeout=180,
        )
    finally:
        if fixture_state == "unreadable":
            fixture.chmod(0o600)

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().endswith("unavailable routes passed")
    if before_hash is None:
        assert not fixture.exists()
    else:
        assert hashlib.sha256(fixture.read_bytes()).hexdigest() == before_hash
