"""Content Intelligence — Streamlit dashboard page (Phase 1).

Five panels:
  1. Top Episodes          — ranked by composite score
  2. Subscriber Magnets   — highest viewer-to-subscriber conversion
  3. Hidden Gems          — loved content that hasn't been discovered yet
  4. Repackaging Opps     — good content held back by a weak thumbnail/title
  5. Draft Asset Library  — review and approve drafted content assets

Uses ContentIntelligenceService backed by the existing analytics SQLite DB.
No LLM generation, no auto-publishing in Phase 1.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

from channel_state import render_channel_selector
from content_intelligence.models import CLASSIFICATION_ACTIONS
from content_intelligence.service import (
    ContentIntelligenceService,
    load_assets,
    update_asset_status,
)
from db import DB_PATH

st.set_page_config(page_title="Content Intelligence", layout="wide")

_active_channel = render_channel_selector()
_DB = Path(DB_PATH)
_SVC = ContentIntelligenceService(_DB, channel=_active_channel)

# ── Classification display map ────────────────────────────────────────────────

_CLS_LABELS: dict[str, str] = {
    "subscriber_magnet": "🧲 Subscriber Magnet",
    "hidden_gem": "💎 Hidden Gem",
    "high_engagement": "🔥 High Engagement",
    "evergreen_candidate": "🌿 Evergreen",
    "needs_repackaging": "🎨 Needs Repackaging",
    "high_watch_time": "⏱ High Watch Time",
    "low_ctr_opportunity": "📈 Low CTR Opportunity",
}


# ── Cached data loading ───────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def _load_scored_library(channel: str) -> tuple[list[dict], dict[str, dict]]:
    """Return (ranked episode dicts, snapshot dict map) from one DB query."""
    svc = ContentIntelligenceService(_DB, channel=channel)
    episodes, snapshots = svc._load_episodes_and_snapshots()
    ranked = svc._scorer.rank_episodes(list(episodes), snapshots)
    snap_map = {s.episode_id: s.model_dump() for s in snapshots}
    return [ep.model_dump() for ep in ranked], snap_map


@st.cache_data(ttl=300)
def _load_asset_library(channel: str) -> list[dict]:
    return load_assets(_DB, channel=channel)


# ── Row helpers ───────────────────────────────────────────────────────────────


def _as_row(ep: dict, snap_map: dict[str, dict]) -> dict:
    snap = snap_map.get(ep["id"], {})
    cls = ep.get("classifications") or []
    actions = [CLASSIFICATION_ACTIONS[c] for c in cls if c in CLASSIFICATION_ACTIONS]
    return {
        "title": ep["title"],
        "score": round(ep.get("score") or 0.0, 1),
        "views": snap.get("views", 0),
        "watch_hours": round(snap.get("watch_hours", 0.0), 1),
        "ctr": snap.get("ctr", 0.0),
        "subscribers_gained": snap.get("subscribers_gained", 0),
        "avg_pct_viewed": round(snap.get("average_percentage_viewed", 0.0), 1),
        "classifications": cls,
        "recommended_action": " · ".join(actions) or "Monitor performance trends.",
        "youtube_video_id": ep["youtube_video_id"],
    }


def _filter_by_cls(rows: list[dict], label: str) -> list[dict]:
    return [r for r in rows if label in r["classifications"]]


# ── Rendering helpers ─────────────────────────────────────────────────────────


def _episode_table(rows: list[dict]) -> None:
    if not rows:
        return
    df = pd.DataFrame(rows)
    display = df[["title", "score", "views", "watch_hours", "subscribers_gained", "avg_pct_viewed"]].copy()
    display.columns = ["Title", "Score", "Views", "Watch Hrs", "Subs Gained", "Avg Watched %"]
    st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Score": st.column_config.ProgressColumn(
                "Score", min_value=0, max_value=100, format="%.0f"
            ),
        },
    )


def _episode_cards(rows: list[dict]) -> None:
    for row in rows:
        icon = "🏆" if row["score"] >= 70 else ("⚡" if row["score"] >= 40 else "📊")
        with st.expander(f"{icon} **{row['title']}** — Score: {row['score']:.0f}/100"):
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Score", f"{row['score']:.0f}/100")
            c2.metric("Views", f"{row['views']:,}")
            c3.metric("Watch Hours", f"{row['watch_hours']:.0f} h")
            c4.metric("Subs Gained", f"{row['subscribers_gained']:,}")
            c5.metric("Avg Watched", f"{row['avg_pct_viewed']:.0f}%")

            if row["classifications"]:
                badges = " · ".join(
                    _CLS_LABELS.get(c, c) for c in row["classifications"]
                )
                st.markdown(f"**Labels:** {badges}")

            if row["recommended_action"]:
                st.info(f"**Recommended action:** {row['recommended_action']}", icon="💡")

            st.caption(f"Video ID: `{row['youtube_video_id']}`")


def _panel(rows: list[dict], empty_msg: str) -> None:
    if not rows:
        st.info(empty_msg, icon="ℹ️")
        return
    st.markdown(f"**{len(rows)} episodes identified**")
    view, cards = st.tabs(["Table", "Cards"])
    with view:
        _episode_table(rows)
    with cards:
        _episode_cards(rows)


def _asset_tile(asset: dict, channel: str) -> None:
    key_id = asset.get("asset_id") or asset.get("id", "")
    with st.expander(f"{asset.get('title', '—')}  ·  `{asset.get('status', 'draft')}`"):
        body = asset.get("body") or asset.get("content") or ""
        st.markdown(body)
        st.divider()
        ca, cr = st.columns(2)
        status = asset.get("status", "draft")
        if status == "draft":
            if ca.button("Approve", key=f"appr_{key_id}"):
                update_asset_status(
                    _DB, key_id, "approved", channel=channel,
                    approved_at=datetime.now(timezone.utc).isoformat(),
                )
                st.cache_data.clear()
                st.rerun()
            if cr.button("Reject", key=f"rejt_{key_id}"):
                update_asset_status(_DB, key_id, "rejected", channel=channel)
                st.cache_data.clear()
                st.rerun()
        elif status == "approved":
            if ca.button("Mark Published", key=f"pub_{key_id}"):
                update_asset_status(_DB, key_id, "published", channel=channel)
                st.cache_data.clear()
                st.rerun()
            if cr.button("Revoke", key=f"rev_{key_id}"):
                update_asset_status(_DB, key_id, "draft", channel=channel)
                st.cache_data.clear()
                st.rerun()


# ── Page ──────────────────────────────────────────────────────────────────────

st.title("Content Intelligence")
st.caption(
    "Identifies winning episodes from existing analytics data. "
    "Phase 1: scoring, classification, and draft asset review. "
    "No auto-publishing."
)

# Load data once; all panels derive from this
_all_eps, _snap_map = _load_scored_library(_active_channel)
_rows = [_as_row(ep, _snap_map) for ep in _all_eps]

if not _rows:
    st.info(
        "No analytics data found. Trigger a fetch via GitHub Actions (workflow_dispatch) "
        "or run `python fetch_metrics.py` locally to populate metrics.",
        icon="ℹ️",
    )
    st.stop()

st.caption(f"{len(_rows)} videos scored")

tab_top, tab_mag, tab_gem, tab_repkg, tab_lib = st.tabs([
    "🏆 Top Episodes",
    "🧲 Subscriber Magnets",
    "💎 Hidden Gems",
    "🎨 Repackaging Opportunities",
    "📁 Draft Asset Library",
])

# ── 1. Top Episodes ───────────────────────────────────────────────────────────
with tab_top:
    st.subheader("Top Episodes")
    st.caption(
        "Ranked by composite score (watch rate, subscriber conversion, watch hours, "
        "engagement, and viewer return rate). A higher score means more valuable content."
    )
    _n = st.slider("Show top N", min_value=5, max_value=50, value=20, step=5)
    _panel(
        _rows[:_n],
        "No analytics data — run a fetch to populate metrics.",
    )

# ── 2. Subscriber Magnets ─────────────────────────────────────────────────────
with tab_mag:
    st.subheader("Subscriber Magnets")
    st.caption(
        "Episodes where ≥2% of viewers subscribed after watching. "
        "Feature these in playlists, end-screens, and community posts."
    )
    _panel(
        _filter_by_cls(_rows, "subscriber_magnet"),
        "No subscriber magnets identified yet. "
        "This requires at least one video with ≥2 subscriber conversions per 100 views.",
    )

# ── 3. Hidden Gems ────────────────────────────────────────────────────────────
with tab_gem:
    st.subheader("Hidden Gems")
    st.caption(
        "Episodes with high viewer retention (≥50% average watched) but low impressions. "
        "Viewers love this content — it just hasn't been discovered. Promote it."
    )
    _panel(
        _filter_by_cls(_rows, "hidden_gem"),
        "No hidden gems found. "
        "Either all high-retention videos already have high impression counts, "
        "or analytics data needs refreshing.",
    )

# ── 4. Repackaging Opportunities ─────────────────────────────────────────────
with tab_repkg:
    st.subheader("Repackaging Opportunities")
    st.caption(
        "Episodes where viewers who click **stay and watch** (≥50% average watched) "
        "but CTR is below 3%. The content is strong — the thumbnail or title is not "
        "driving enough clicks."
    )
    _panel(
        _filter_by_cls(_rows, "needs_repackaging"),
        "No repackaging opportunities identified. "
        "This panel requires CTR data — the YouTube Analytics API v2 does not expose "
        "impression CTR for channel reports, so all episodes are currently excluded.",
    )

# ── 5. Draft Asset Library ────────────────────────────────────────────────────
with tab_lib:
    st.subheader("Draft Asset Library")
    st.caption(
        "Content assets awaiting review. "
        "Phase 2 will auto-generate drafts from episode transcripts. "
        "Assets added manually will appear here."
    )
    _assets = _load_asset_library(_active_channel)
    if not _assets:
        st.info(
            "No assets yet. Asset generation (Phase 2) will populate this library "
            "with community posts, polls, quote cards, and more.",
            icon="ℹ️",
        )
    else:
        _f1, _f2 = st.columns(2)
        _fstat = _f1.selectbox(
            "Status",
            ["All", "draft", "approved", "rejected", "published"],
            key="lib_status",
        )
        _ftype = _f2.selectbox(
            "Type",
            ["All"] + sorted({a.get("asset_type", "") for a in _assets if a.get("asset_type")}),
            key="lib_type",
        )

        _filtered_assets = [
            a for a in _assets
            if (_fstat == "All" or a.get("status") == _fstat)
            and (_ftype == "All" or a.get("asset_type") == _ftype)
        ]

        _counts = pd.Series([a.get("status", "draft") for a in _assets]).value_counts()
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Assets", len(_assets))
        m2.metric("Drafts", _counts.get("draft", 0))
        m3.metric("Approved", _counts.get("approved", 0))
        m4.metric("Published", _counts.get("published", 0))

        st.divider()
        if not _filtered_assets:
            st.caption("No assets match the selected filters.")
        for _a in _filtered_assets:
            _asset_tile(_a, _active_channel)
