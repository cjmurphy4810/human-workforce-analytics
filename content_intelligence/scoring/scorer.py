"""ContentScorer — configurable, weight-based episode scoring and classification."""
from __future__ import annotations

from content_intelligence.config import DEFAULT_CONFIG, ScoringConfig
from content_intelligence.models import AnalyticsSnapshot, Episode


class ContentScorer:
    """Score and classify episodes using configurable weights and thresholds.

    All scoring is deterministic and threshold-based — no ML or LLM calls.
    Scores are normalised to [0, 100].
    """

    def __init__(self, config: ScoringConfig | None = None) -> None:
        self.config = config or DEFAULT_CONFIG

    # ── Public API ────────────────────────────────────────────────────────────

    def score_episode(self, snapshot: AnalyticsSnapshot) -> float:
        """Return a normalised composite score in [0, 100] for one snapshot."""
        w = self.config.weights
        t = self.config.thresholds

        normalised = {
            "ctr": _norm(snapshot.ctr, t.ctr_max),
            "average_percentage_viewed": _norm(
                snapshot.average_percentage_viewed, t.avg_pct_viewed_max
            ),
            "watch_hours": _norm(snapshot.watch_hours, t.watch_hours_max),
            "subscribers_gained": _norm(
                float(snapshot.subscribers_gained), t.subscribers_gained_max
            ),
            "comments": _norm(float(snapshot.comments), t.comments_max),
            "shares": _norm(float(snapshot.shares), t.shares_max),
            "returning_viewers": _norm(
                float(snapshot.returning_viewers), t.returning_viewers_max
            ),
        }

        raw = (
            w.ctr * normalised["ctr"]
            + w.average_percentage_viewed * normalised["average_percentage_viewed"]
            + w.watch_hours * normalised["watch_hours"]
            + w.subscribers_gained * normalised["subscribers_gained"]
            + w.comments * normalised["comments"]
            + w.shares * normalised["shares"]
            + w.returning_viewers * normalised["returning_viewers"]
        )
        return round(raw * 100, 2)

    def rank_episodes(
        self,
        episodes: list[Episode],
        snapshots: list[AnalyticsSnapshot],
    ) -> list[Episode]:
        """Score every episode and return them sorted best-first.

        Episodes without a matching snapshot receive score=0.
        Mutates the Episode objects in place (sets .score and .classifications).
        """
        snap_map: dict[str, AnalyticsSnapshot] = {s.episode_id: s for s in snapshots}

        result: list[Episode] = []
        for ep in episodes:
            snap = snap_map.get(ep.id) or snap_map.get(ep.youtube_video_id)
            if snap is not None:
                ep.score = self.score_episode(snap)
                ep.classifications = self.classify_episode(snap)
            else:
                ep.score = 0.0
                ep.classifications = []
            result.append(ep)

        result.sort(key=lambda e: e.score or 0.0, reverse=True)
        return result

    def classify_episode(self, snapshot: AnalyticsSnapshot) -> list[str]:
        """Return a list of classification labels that apply to this snapshot.

        Classifications are non-exclusive — an episode can hold multiple labels.
        """
        ct = self.config.classification
        labels: list[str] = []

        views = max(snapshot.views, 1)

        # subscriber_magnet
        sub_rate = snapshot.subscribers_gained / views
        if sub_rate >= ct.subscriber_magnet_min_sub_rate:
            labels.append("subscriber_magnet")

        # hidden_gem — loved by viewers who find it, but low exposure
        if (
            snapshot.average_percentage_viewed >= ct.hidden_gem_min_avg_pct_viewed
            and snapshot.impressions < ct.hidden_gem_max_impressions
        ):
            labels.append("hidden_gem")

        # high_engagement
        engagement_rate = (snapshot.comments + snapshot.likes) / views
        if engagement_rate >= ct.high_engagement_min_engagement_rate:
            labels.append("high_engagement")

        # evergreen_candidate
        if (
            snapshot.average_percentage_viewed >= ct.evergreen_candidate_min_avg_pct_viewed
            and snapshot.views >= ct.evergreen_candidate_min_views
        ):
            labels.append("evergreen_candidate")

        # needs_repackaging — content retains but doesn't attract clicks
        if (
            snapshot.average_percentage_viewed >= ct.needs_repackaging_min_avg_pct_viewed
            and snapshot.ctr < ct.needs_repackaging_max_ctr
            and snapshot.ctr > 0  # only flag if we have CTR data
        ):
            labels.append("needs_repackaging")

        # high_watch_time
        if snapshot.watch_hours >= ct.high_watch_time_min_hours:
            labels.append("high_watch_time")

        # low_ctr_opportunity
        if (
            0 < snapshot.ctr < ct.low_ctr_opportunity_max_ctr
            and snapshot.average_percentage_viewed >= ct.low_ctr_opportunity_min_avg_pct_viewed
        ):
            labels.append("low_ctr_opportunity")

        return labels


# ── Internal helpers ──────────────────────────────────────────────────────────

def _norm(value: float, ceiling: float) -> float:
    """Normalise value to [0, 1] capped at ceiling."""
    if ceiling <= 0:
        return 0.0
    return min(value / ceiling, 1.0)
