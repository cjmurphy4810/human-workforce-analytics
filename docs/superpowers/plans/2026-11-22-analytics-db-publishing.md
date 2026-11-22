# Analytics Database Publishing Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore daily multi-channel analytics publication and bound the SQLite artifact below GitHub's file limit.

**Architecture:** Add one database-maintenance boundary that prunes only superseded production data, compacts SQLite, and enforces integrity/size checks. Invoke it after collection and again as a workflow publication gate.

**Tech Stack:** Python 3.12, SQLite, pytest, GitHub Actions

**Spec:** `docs/superpowers/specs/2026-11-22-analytics-db-publishing-design.md`

## Global Constraints

- All cleanup is channel-partitioned.
- Preserve 370 days of channel and video snapshot history.
- Preserve one latest retention row per channel/video/window kind, one latest publishing queue row per channel, and one latest content-intelligence score batch per channel.
- Reject a database over 95 MiB or with a failed SQLite integrity check before publication.

## Review Focus

- Tied timestamps must retain exactly one deterministic latest row.
- Cleanup of one channel must not delete another channel's data.
- Empty tables must remain valid and report zero deletions.
- The history cutoff must preserve the boundary date.
- The workflow gate must return a non-zero exit for oversized or corrupt input.

---

### Task 1: Database retention and health gate

**Files:**
- Create: `database_maintenance.py`
- Create: `tests/test_database_maintenance.py`

**Interfaces:**
- Produces: `maintain_database(db_path: Path, today: date | None = None) -> dict[str, int]`
- Produces: `check_database(db_path: Path, max_bytes: int) -> dict[str, int | str]`

- [ ] Write tests covering channel isolation, deterministic latest-row retention, the 370-day boundary, empty tables, compaction, integrity, and oversize rejection.
- [ ] Run the new test module and verify it fails because the maintenance module does not exist.
- [ ] Implement the minimal maintenance and check functions plus a CLI.
- [ ] Run the new test module and verify it passes.

### Task 2: Collector and workflow integration

**Files:**
- Modify: `fetch_metrics.py`
- Modify: `.github/workflows/fetch-analytics.yml`
- Modify: `tests/test_fetch_metrics.py`

**Interfaces:**
- Consumes: `maintain_database` and the maintenance CLI from Task 1.

- [ ] Add a failing collector test proving maintenance runs once after all selected channels, including when one channel collection fails.
- [ ] Invoke maintenance after the collection loop.
- [ ] Add a workflow publication gate before the commit step.
- [ ] Run focused tests, then the full suite and report any baseline-only failure separately.

### Task 3: Compact and validate the production artifact

**Files:**
- Modify: `data.db`

**Interfaces:**
- Consumes: Task 1's CLI.

- [ ] Run maintenance against `data.db` and record before/after bytes and per-table deletions.
- [ ] Verify all five channels retain current channel, video, daily, and retention data.
- [ ] Run SQLite integrity check and the 95 MiB publication gate.
- [ ] Run focused and full regression verification.
