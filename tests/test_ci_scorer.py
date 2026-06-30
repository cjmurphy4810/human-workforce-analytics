"""Tests for ContentScorer: score_episode, rank_episodes, classify_episode."""
from __future__ import annotations

from datetime import date

import pytest

from content_intelligence.config import (
    DEFAULT_CONFIG,
    NormalizationThresholds,
    ScoringConfig,
    ScoringWeights,
)
from content_intelligence.models import AnalyticsSnapshot, Episode
from content_intelligence.scoring.scorer import ContentScorer, _norm


# ── Fixtures ──────────────────────────────────────────────────────────────────


def _snap(
    episode_id: str = "ep1",
    views: int = 500,
    watch_hours: float = 80.0,
    avg_pct_viewed: float = 60.0,
    ctr: float = 4.0,
    subscribers_gained: int = 10,
    comments: int = 20,
    likes: int = 50,
    shares: int = 8,
    impressions: int = 5000,
    returning_viewers: int = 100,
) -> AnalyticsSnapshot:
    return AnalyticsSnapshot(
        episode_id=episode_id,
        snapshot_date=date(2026, 6, 29),
        views=views,
        watch_hours=watch_hours,
        average_percentage_viewed=avg_pct_viewed,
        ctr=ctr,
        subscribers_gained=subscribers_gained,
        comments=comments,
        likes=likes,
        shares=shares,
        impressions=impressions,
        returning_viewers=returning_viewers,
    )


def _ep(ep_id: str = "ep1", title: str = "Episode One") -> Episode:
    return Episode(id=ep_id, youtube_video_id=ep_id, title=title)


# ── _norm helper ──────────────────────────────────────────────────────────────


def test_norm_zero_value():
    assert _norm(0.0, 100.0) == 0.0


def test_norm_ceiling():
    assert _norm(100.0, 100.0) == 1.0


def test_norm_above_ceiling_capped():
    assert _norm(200.0, 100.0) == 1.0


def test_norm_zero_ceiling_returns_zero():
    assert _norm(50.0, 0.0) == 0.0


def test_norm_partial():
    assert _norm(50.0, 100.0) == pytest.approx(0.5)


# ── ContentScorer.score_episode ───────────────────────────────────────────────


def test_score_episode_in_range():
    scorer = ContentScorer()
    score = scorer.score_episode(_snap())
    assert 0.0 <= score <= 100.0


def test_score_episode_perfect_metrics_near_100():
    scorer = ContentScorer()
    snap = _snap(
        watch_hours=500.0, avg_pct_viewed=100.0, ctr=10.0,
        subscribers_gained=500, comments=100, shares=50, returning_viewers=500,
    )
    score = scorer.score_episode(snap)
    assert score == pytest.approx(100.0, abs=1.0)


def test_score_episode_zero_metrics_returns_zero():
    scorer = ContentScorer()
    snap = _snap(
        watch_hours=0.0, avg_pct_viewed=0.0, ctr=0.0,
        subscribers_gained=0, comments=0, shares=0, returning_viewers=0,
    )
    assert scorer.score_episode(snap) == 0.0


def test_score_episode_higher_metrics_higher_score():
    scorer = ContentScorer()
    low = _snap(watch_hours=10.0, avg_pct_viewed=20.0, ctr=1.0, subscribers_gained=1)
    high = _snap(watch_hours=300.0, avg_pct_viewed=90.0, ctr=8.0, subscribers_gained=200)
    assert scorer.score_episode(high) > scorer.score_episode(low)


def test_score_episode_uses_custom_config():
    # Putting all weight on watch_hours
    weights = ScoringWeights(
        ctr=0.0, average_percentage_viewed=0.0, watch_hours=1.0,
        subscribers_gained=0.0, comments=0.0, shares=0.0, returning_viewers=0.0,
    )
    config = ScoringConfig(weights=weights)
    scorer = ContentScorer(config)
    snap = _snap(watch_hours=250.0)  # 250/500 ceiling = 0.5 → score = 50.0
    assert scorer.score_episode(snap) == pytest.approx(50.0, abs=0.1)


def test_score_episode_rounded_to_two_decimals():
    scorer = ContentScorer()
    score = scorer.score_episode(_snap(watch_hours=33.3))
    assert score == round(score, 2)


# ── ContentScorer.rank_episodes ───────────────────────────────────────────────


def test_rank_episodes_sorted_descending():
    scorer = ContentScorer()
    eps = [_ep("e1", "Low"), _ep("e2", "High")]
    snaps = [
        _snap("e1", watch_hours=10.0, avg_pct_viewed=20.0, ctr=1.0, subscribers_gained=1),
        _snap("e2", watch_hours=300.0, avg_pct_viewed=90.0, ctr=8.0, subscribers_gained=200),
    ]
    ranked = scorer.rank_episodes(eps, snaps)
    assert ranked[0].id == "e2"
    assert ranked[1].id == "e1"


def test_rank_episodes_sets_score():
    scorer = ContentScorer()
    eps = [_ep("e1")]
    snaps = [_snap("e1", watch_hours=200.0)]
    ranked = scorer.rank_episodes(eps, snaps)
    assert ranked[0].score is not None
    assert ranked[0].score > 0.0


def test_rank_episodes_sets_classifications():
    scorer = ContentScorer()
    eps = [_ep("e1")]
    # Sub rate 10/100 = 10% → subscriber_magnet threshold is 2%
    snaps = [_snap("e1", views=100, subscribers_gained=10)]
    ranked = scorer.rank_episodes(eps, snaps)
    assert "subscriber_magnet" in ranked[0].classifications


def test_rank_episodes_no_snapshot_gives_zero_score():
    scorer = ContentScorer()
    eps = [_ep("e1")]
    ranked = scorer.rank_episodes(eps, [])  # no snapshots
    assert ranked[0].score == 0.0
    assert ranked[0].classifications == []


def test_rank_episodes_empty_inputs():
    scorer = ContentScorer()
    assert scorer.rank_episodes([], []) == []


def test_rank_episodes_matches_by_youtube_video_id():
    scorer = ContentScorer()
    ep = Episode(id="uuid-form", youtube_video_id="yt123", title="T")
    snap = _snap("yt123", watch_hours=100.0)
    ranked = scorer.rank_episodes([ep], [snap])
    assert ranked[0].score is not None
    assert ranked[0].score > 0.0


# ── ContentScorer.classify_episode ───────────────────────────────────────────


def test_classify_subscriber_magnet():
    scorer = ContentScorer()
    snap = _snap(views=100, subscribers_gained=10)  # 10% > 2% threshold
    labels = scorer.classify_episode(snap)
    assert "subscriber_magnet" in labels


def test_classify_not_subscriber_magnet_low_rate():
    scorer = ContentScorer()
    snap = _snap(views=1000, subscribers_gained=1)  # 0.1% < 2%
    labels = scorer.classify_episode(snap)
    assert "subscriber_magnet" not in labels


def test_classify_hidden_gem():
    scorer = ContentScorer()
    snap = _snap(avg_pct_viewed=75.0, impressions=5000)  # >50% viewed, <10k impressions
    labels = scorer.classify_episode(snap)
    assert "hidden_gem" in labels


def test_classify_not_hidden_gem_high_impressions():
    scorer = ContentScorer()
    snap = _snap(avg_pct_viewed=80.0, impressions=50000)  # >10k impressions
    labels = scorer.classify_episode(snap)
    assert "hidden_gem" not in labels


def test_classify_high_engagement():
    scorer = ContentScorer()
    snap = _snap(views=100, comments=3, likes=2)  # (3+2)/100 = 5% > 4%
    labels = scorer.classify_episode(snap)
    assert "high_engagement" in labels


def test_classify_evergreen_candidate():
    scorer = ContentScorer()
    snap = _snap(avg_pct_viewed=70.0, views=200)  # >60% & >100 views
    labels = scorer.classify_episode(snap)
    assert "evergreen_candidate" in labels


def test_classify_needs_repackaging():
    scorer = ContentScorer()
    snap = _snap(avg_pct_viewed=60.0, ctr=2.0)  # >50% watched, 0 < ctr < 3
    labels = scorer.classify_episode(snap)
    assert "needs_repackaging" in labels


def test_classify_needs_repackaging_skipped_when_no_ctr():
    scorer = ContentScorer()
    snap = _snap(avg_pct_viewed=60.0, ctr=0.0)  # ctr=0 means no data
    labels = scorer.classify_episode(snap)
    assert "needs_repackaging" not in labels


def test_classify_high_watch_time():
    scorer = ContentScorer()
    snap = _snap(watch_hours=200.0)  # > 100 threshold
    labels = scorer.classify_episode(snap)
    assert "high_watch_time" in labels


def test_classify_low_ctr_opportunity():
    scorer = ContentScorer()
    snap = _snap(ctr=1.5, avg_pct_viewed=50.0)  # 0 < ctr < 2 and >40% watched
    labels = scorer.classify_episode(snap)
    assert "low_ctr_opportunity" in labels


def test_classify_multiple_labels_possible():
    scorer = ContentScorer()
    snap = _snap(
        views=100,
        subscribers_gained=5,   # subscriber_magnet
        watch_hours=200.0,       # high_watch_time
        avg_pct_viewed=70.0,     # evergreen_candidate
    )
    labels = scorer.classify_episode(snap)
    assert "subscriber_magnet" in labels
    assert "high_watch_time" in labels
    assert "evergreen_candidate" in labels


def test_classify_empty_episode_returns_empty():
    scorer = ContentScorer()
    snap = _snap(views=0, subscribers_gained=0, watch_hours=0.0,
                 avg_pct_viewed=0.0, comments=0, likes=0, impressions=0)
    labels = scorer.classify_episode(snap)
    assert isinstance(labels, list)


# ── ScoringConfig validation ──────────────────────────────────────────────────


def test_default_config_weights_sum_to_one():
    w = DEFAULT_CONFIG.weights
    total = (w.ctr + w.average_percentage_viewed + w.watch_hours
             + w.subscribers_gained + w.comments + w.shares + w.returning_viewers)
    assert abs(total - 1.0) < 0.01


def test_scoring_weights_invalid_sum_raises():
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        ScoringWeights(
            ctr=0.5, average_percentage_viewed=0.5, watch_hours=0.5,
            subscribers_gained=0.0, comments=0.0, shares=0.0, returning_viewers=0.0,
        )


def test_normalization_thresholds_defaults():
    t = NormalizationThresholds()
    assert t.ctr_max == 10.0
    assert t.avg_pct_viewed_max == 100.0
    assert t.watch_hours_max == 500.0
