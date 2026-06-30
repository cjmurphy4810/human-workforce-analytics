"""Content Intelligence service layer.

Provides two APIs:

1. ContentIntelligenceService — OOP class for Phase 1 dashboard panels.
   Loads Episodes + AnalyticsSnapshots from the existing SQLite DB and
   delegates scoring/classification to ContentScorer.

2. Module-level backward-compat functions (run_scoring, save_asset, etc.)
   used by fetch_metrics.py and the Streamlit page until Phase 2 completes
   the full migration.
"""
from __future__ import annotations

import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any

from content_intelligence.config import ScoringConfig
from content_intelligence.models import (
    CLASSIFICATION_ACTIONS,
    AnalyticsSnapshot,
    AssetStatus,
    AssetType,
    ContentAsset,
    Episode,
    LegacyContentAsset,
    VideoScore,
)
from content_intelligence.scoring.engine import score_videos
from content_intelligence.scoring.scorer import ContentScorer


# ── ContentIntelligenceService ────────────────────────────────────────────────


class ContentIntelligenceService:
    """High-level service for the Content Intelligence dashboard.

    Reads from the existing analytics SQLite DB (no migrations required).
    All data transformation is done in Python — the DB schema is unchanged.
    """

    def __init__(
        self,
        db_path: Path,
        config: ScoringConfig | None = None,
    ) -> None:
        self._db_path = db_path
        self._scorer = ContentScorer(config)

    # ── Data loading ──────────────────────────────────────────────────────────

    def _load_episodes_and_snapshots(
        self,
    ) -> tuple[list[Episode], list[AnalyticsSnapshot]]:
        """Load Episodes + AnalyticsSnapshots from the existing DB schema."""
        with sqlite3.connect(str(self._db_path)) as conn:
            conn.row_factory = sqlite3.Row

            video_rows = conn.execute(
                "SELECT video_id, title, description, published_at, "
                "duration_seconds, thumbnail_url FROM videos"
            ).fetchall()

            metric_rows = conn.execute("""
                SELECT
                    d.video_id,
                    d.views,
                    d.estimated_minutes_watched / 60.0 AS watch_hours,
                    COALESCE(d.average_view_duration, 0) AS average_view_duration_seconds,
                    COALESCE(d.likes, 0) AS likes,
                    COALESCE(d.subscribers_gained, 0) AS subscribers_gained,
                    COALESCE(adv.adv_views, 0) AS adv_views,
                    d.metric_date
                FROM daily_video_metrics d
                INNER JOIN (
                    SELECT video_id, MAX(metric_date) AS latest_date
                    FROM daily_video_metrics GROUP BY video_id
                ) latest ON d.video_id = latest.video_id
                         AND d.metric_date = latest.latest_date
                LEFT JOIN (
                    SELECT video_id, SUM(views) AS adv_views
                    FROM video_traffic_source_metrics
                    WHERE traffic_source_type = 'ADVERTISING'
                    GROUP BY video_id
                ) adv ON d.video_id = adv.video_id
                WHERE d.views > 0
            """).fetchall()

        metric_map: dict[str, dict[str, Any]] = {r["video_id"]: dict(r) for r in metric_rows}

        episodes: list[Episode] = []
        snapshots: list[AnalyticsSnapshot] = []

        for row in video_rows:
            vid = row["video_id"]
            ep = Episode(
                id=vid,
                youtube_video_id=vid,
                title=row["title"] or vid,
                description=row["description"] or "",
                published_date=_parse_date(row["published_at"]),
                duration_seconds=row["duration_seconds"] or 0,
                thumbnail_url=row["thumbnail_url"] or "",
            )
            episodes.append(ep)

            if vid in metric_map:
                m = metric_map[vid]
                dur = ep.duration_seconds or 1
                avg_dur = float(m["average_view_duration_seconds"])
                avg_pct = min(avg_dur / dur * 100.0, 100.0) if dur > 0 else 0.0
                views = int(m["views"])
                adv_views = int(m["adv_views"])
                # impressions proxy: total views / organic_ratio (rough estimate)
                organic_views = max(views - adv_views, 0)
                snap = AnalyticsSnapshot(
                    episode_id=vid,
                    snapshot_date=_parse_date(m["metric_date"]) or date.today(),
                    views=views,
                    watch_hours=round(float(m["watch_hours"]), 2),
                    average_view_duration_seconds=avg_dur,
                    average_percentage_viewed=round(avg_pct, 1),
                    ctr=0.0,  # not available in the Analytics API v2 channel reports
                    subscribers_gained=int(m["subscribers_gained"]),
                    likes=int(m["likes"]),
                    impressions=adv_views + organic_views,
                )
                snapshots.append(snap)

        return episodes, snapshots

    # ── Service methods ───────────────────────────────────────────────────────

    def score_content_library(self) -> list[Episode]:
        """Score and rank all episodes in the content library."""
        episodes, snapshots = self._load_episodes_and_snapshots()
        return self._scorer.rank_episodes(episodes, snapshots)

    def get_top_episodes(self, n: int = 10) -> list[Episode]:
        """Return the top-N episodes by composite score."""
        ranked = self.score_content_library()
        return ranked[:n]

    def get_subscriber_magnets(self) -> list[Episode]:
        """Return episodes classified as subscriber magnets."""
        return self._filter_by_classification("subscriber_magnet")

    def get_hidden_gems(self) -> list[Episode]:
        """Return episodes classified as hidden gems."""
        return self._filter_by_classification("hidden_gem")

    def get_repackaging_opportunities(self) -> list[Episode]:
        """Return episodes that need thumbnail/title repackaging."""
        return self._filter_by_classification("needs_repackaging")

    def create_asset_draft(
        self,
        episode: Episode,
        asset_type: AssetType,
        title: str,
        content: str,
        platform: str = "",
    ) -> ContentAsset:
        """Create an in-memory draft ContentAsset (not persisted — caller saves)."""
        return ContentAsset(
            episode_id=episode.id,
            asset_type=asset_type,
            title=title,
            content=content,
            platform=platform,
            status=AssetStatus.draft,
        )

    def _filter_by_classification(self, label: str) -> list[Episode]:
        episodes, snapshots = self._load_episodes_and_snapshots()
        snap_map = {s.episode_id: s for s in snapshots}
        result: list[Episode] = []
        for ep in episodes:
            snap = snap_map.get(ep.id)
            if snap is None:
                continue
            classes = self._scorer.classify_episode(snap)
            if label in classes:
                ep.score = self._scorer.score_episode(snap)
                ep.classifications = classes
                result.append(ep)
        result.sort(key=lambda e: e.score or 0.0, reverse=True)
        return result

    def get_recommended_action(self, episode: Episode) -> str:
        """Return a comma-joined set of recommended actions for an episode."""
        if not episode.classifications:
            return "Analyse performance data for more specific recommendations."
        actions = [
            CLASSIFICATION_ACTIONS[c]
            for c in episode.classifications
            if c in CLASSIFICATION_ACTIONS
        ]
        return " · ".join(actions) if actions else "Monitor performance trends."


# ── Backward-compat module-level functions ────────────────────────────────────
# Used by fetch_metrics.py and the legacy Streamlit page view.


def run_scoring(db_path: Path, scored_at: date | None = None) -> list[VideoScore]:
    """Score all videos and upsert results to ci_video_scores (legacy path)."""
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


def save_asset(db_path: Path, asset: "LegacyContentAsset") -> None:
    """Persist a legacy ContentAsset dataclass to ci_content_assets."""
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
    """Update status and optional fields on an existing legacy asset."""
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


def load_scores(db_path: Path, scored_at: date | None = None) -> list[dict[str, Any]]:
    """Load the latest (or specific date) scores as plain dicts."""
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
) -> list[dict[str, Any]]:
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


# ── Helpers ───────────────────────────────────────────────────────────────────


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.rstrip("Z")).date()
    except ValueError:
        return None
