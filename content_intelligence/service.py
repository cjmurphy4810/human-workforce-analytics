"""Orchestrate the content intelligence pipeline."""
from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

from content_intelligence.models import ContentAsset, VideoScore
from content_intelligence.scoring.engine import score_videos


def run_scoring(db_path: Path, scored_at: date | None = None) -> list[VideoScore]:
    """Score all videos and upsert results to ci_video_scores."""
    scores = score_videos(db_path, scored_at)
    if not scores:
        return []

    with sqlite3.connect(str(db_path)) as conn:
        for s in scores:
            conn.execute(
                "INSERT INTO ci_video_scores "
                "(scored_at, video_id, tier, engagement_score, evergreen_score, "
                "subscriber_magnet_score, hidden_gem_score, overall_score, "
                "total_views, watch_rate_pct, like_rate_pct, sub_rate_pct, promotion_ratio) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(scored_at, video_id) DO UPDATE SET "
                "tier=excluded.tier, "
                "engagement_score=excluded.engagement_score, "
                "evergreen_score=excluded.evergreen_score, "
                "subscriber_magnet_score=excluded.subscriber_magnet_score, "
                "hidden_gem_score=excluded.hidden_gem_score, "
                "overall_score=excluded.overall_score, "
                "total_views=excluded.total_views, "
                "watch_rate_pct=excluded.watch_rate_pct, "
                "like_rate_pct=excluded.like_rate_pct, "
                "sub_rate_pct=excluded.sub_rate_pct, "
                "promotion_ratio=excluded.promotion_ratio",
                (
                    s.scored_at, s.video_id, s.tier,
                    s.engagement_score, s.evergreen_score,
                    s.subscriber_magnet_score, s.hidden_gem_score, s.overall_score,
                    s.total_views, s.watch_rate_pct, s.like_rate_pct,
                    s.sub_rate_pct, s.promotion_ratio,
                ),
            )
    return scores


def save_asset(db_path: Path, asset: ContentAsset) -> None:
    """Persist a ContentAsset to ci_content_assets."""
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "INSERT INTO ci_content_assets "
            "(asset_id, video_id, video_title, asset_type, title, body, "
            "generated_at, status, approved_at, scheduled_for, notes) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(asset_id) DO UPDATE SET "
            "status=excluded.status, "
            "approved_at=excluded.approved_at, "
            "scheduled_for=excluded.scheduled_for, "
            "notes=excluded.notes",
            (
                asset.asset_id, asset.video_id, asset.video_title,
                asset.asset_type, asset.title, asset.body,
                asset.generated_at, asset.status,
                asset.approved_at, asset.scheduled_for, asset.notes,
            ),
        )


def update_asset_status(
    db_path: Path,
    asset_id: str,
    status: str,
    approved_at: str | None = None,
    scheduled_for: str | None = None,
    notes: str | None = None,
) -> None:
    """Update status and optional fields on an existing asset."""
    fields: list[str] = ["status=?"]
    params: list[object] = [status]
    if approved_at is not None:
        fields.append("approved_at=?")
        params.append(approved_at)
    if scheduled_for is not None:
        fields.append("scheduled_for=?")
        params.append(scheduled_for)
    if notes is not None:
        fields.append("notes=?")
        params.append(notes)
    params.append(asset_id)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            f"UPDATE ci_content_assets SET {', '.join(fields)} WHERE asset_id=?",
            params,
        )


def load_scores(db_path: Path, scored_at: date | None = None) -> list[dict]:
    """Load the latest (or specific date) scores as plain dicts, ordered by overall_score desc."""
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        if scored_at:
            rows = conn.execute(
                "SELECT * FROM ci_video_scores WHERE scored_at=? ORDER BY overall_score DESC",
                (scored_at.isoformat(),),
            ).fetchall()
        else:
            rows = conn.execute("""
                SELECT s.* FROM ci_video_scores s
                INNER JOIN (SELECT MAX(scored_at) AS latest FROM ci_video_scores) m
                  ON s.scored_at = m.latest
                ORDER BY s.overall_score DESC
            """).fetchall()
    return [dict(r) for r in rows]


def load_assets(
    db_path: Path,
    asset_type: str | None = None,
    status: str | None = None,
    video_id: str | None = None,
) -> list[dict]:
    """Load content assets with optional filters, newest first."""
    clauses: list[str] = []
    params: list[object] = []
    if asset_type:
        clauses.append("asset_type=?")
        params.append(asset_type)
    if status:
        clauses.append("status=?")
        params.append(status)
    if video_id:
        clauses.append("video_id=?")
        params.append(video_id)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"SELECT * FROM ci_content_assets {where} ORDER BY generated_at DESC",
            params,
        ).fetchall()
    return [dict(r) for r in rows]
