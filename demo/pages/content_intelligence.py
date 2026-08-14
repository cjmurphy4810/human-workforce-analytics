"""Read-only Content Intelligence report for the deterministic public demo."""

from __future__ import annotations

from datetime import timedelta

import pandas as pd
import streamlit as st

from content_intelligence.models import (
    CLASSIFICATION_ACTIONS,
    TIER_LABELS,
    AnalyticsSnapshot,
    Episode,
)
from content_intelligence.service import ContentIntelligenceService, load_assets
from demo.analytics import (
    aggregate_video_window,
    content_repackaging_rows,
    content_tier_rows,
    rank_persisted_content_rows,
)
from demo.config import DEMO_AS_OF, DEMO_CHANNEL_KEY, DEMO_DB_PATH
from demo.report_data import query_frame
from demo.ui import render_demo_notice, render_demo_sidebar, render_empty_state


_SVC = ContentIntelligenceService(DEMO_DB_PATH, channel=DEMO_CHANNEL_KEY)
_CLASSIFICATION_LABELS = {
    "subscriber_magnet": "🧲 Subscriber Magnet",
    "hidden_gem": "💎 Hidden Gem",
    "high_engagement": "🔥 High Engagement",
    "evergreen_candidate": "🌿 Evergreen",
    "needs_repackaging": "🎨 Needs Repackaging",
    "high_watch_time": "⏱ High Watch Time",
    "low_ctr_opportunity": "📈 Low CTR Opportunity",
}


@st.cache_data(ttl=300)
def _load_scored_library() -> list[dict[str, object]]:
    """Score trailing-year daily increments with the existing content scorer."""
    start = DEMO_AS_OF - timedelta(days=364)
    aggregates = {
        str(row["video_id"]): row
        for row in aggregate_video_window(
            DEMO_DB_PATH,
            start=start,
            end=DEMO_AS_OF,
            channel=DEMO_CHANNEL_KEY,
        )
    }
    videos = query_frame(
        "SELECT video_id, title, description, published_at, duration_seconds, thumbnail_url "
        "FROM videos WHERE channel=:channel AND date(published_at)<=:as_of",
        {"channel": DEMO_CHANNEL_KEY, "as_of": DEMO_AS_OF.isoformat()},
    )
    persisted_scores = query_frame(
        "SELECT video_id, tier, overall_score, engagement_score, evergreen_score, "
        "subscriber_magnet_score, hidden_gem_score, watch_rate_pct "
        "FROM ci_video_scores "
        "WHERE channel=:channel AND scored_at=(SELECT MAX(scored_at) FROM ci_video_scores "
        "WHERE channel=:channel AND scored_at<=:as_of)",
        {"channel": DEMO_CHANNEL_KEY, "as_of": DEMO_AS_OF.isoformat()},
    )
    video_map = {str(row["video_id"]): row for row in videos.to_dict("records")}

    episodes: list[Episode] = []
    snapshots: list[AnalyticsSnapshot] = []
    snapshot_map: dict[str, AnalyticsSnapshot] = {}
    for row in videos.to_dict("records"):
        video_id = str(row["video_id"])
        aggregate = aggregates.get(video_id)
        if not aggregate:
            continue
        length = int(row.get("duration_seconds") or 0)
        average_duration = float(aggregate["average_view_duration"])
        snapshot = AnalyticsSnapshot(
            episode_id=video_id,
            snapshot_date=DEMO_AS_OF,
            views=int(aggregate["views"]),
            watch_hours=round(float(aggregate["estimated_minutes_watched"]) / 60.0, 2),
            average_view_duration_seconds=average_duration,
            average_percentage_viewed=(
                round(min(average_duration / length * 100.0, 100.0), 1)
                if length
                else 0.0
            ),
            subscribers_gained=int(aggregate["subscribers_gained"]),
            likes=int(aggregate["likes"]),
            impressions=int(aggregate["views"]),
        )
        episode = Episode(
            id=video_id,
            youtube_video_id=video_id,
            title=str(row.get("title") or video_id),
            description=str(row.get("description") or ""),
            published_date=pd.to_datetime(
                row.get("published_at"), errors="coerce"
            ).date(),
            duration_seconds=length,
            thumbnail_url=str(row.get("thumbnail_url") or ""),
        )
        episodes.append(episode)
        snapshots.append(snapshot)
        snapshot_map[video_id] = snapshot

    scored_signals = _SVC._scorer.rank_episodes(episodes, snapshots)
    classification_map = {
        episode.id: list(episode.classifications) for episode in scored_signals
    }
    rows: list[dict[str, object]] = []
    for persisted in persisted_scores.to_dict("records"):
        video_id = str(persisted["video_id"])
        metadata = video_map.get(video_id)
        snapshot = snapshot_map.get(video_id)
        if metadata is None or snapshot is None:
            continue
        classifications = classification_map.get(video_id, [])
        actions = [
            CLASSIFICATION_ACTIONS[label]
            for label in classifications
            if label in CLASSIFICATION_ACTIONS
        ]
        rows.append(
            {
                "video_id": video_id,
                "title": str(metadata.get("title") or video_id),
                "score": round(float(persisted["overall_score"]), 1),
                "overall_score": float(persisted["overall_score"]),
                "tier": str(persisted["tier"]),
                "watch_rate_pct": float(persisted["watch_rate_pct"]),
                "views": snapshot.views,
                "watch_hours": snapshot.watch_hours,
                "subscribers_gained": snapshot.subscribers_gained,
                "avg_pct_viewed": snapshot.average_percentage_viewed,
                "classifications": classifications,
                "recommended_action": " · ".join(actions)
                or "Monitor performance trends.",
                "engagement_score": float(persisted.get("engagement_score", 0.0)),
                "evergreen_score": float(persisted.get("evergreen_score", 0.0)),
                "subscriber_magnet_score": float(
                    persisted.get("subscriber_magnet_score", 0.0)
                ),
                "hidden_gem_score": float(persisted.get("hidden_gem_score", 0.0)),
            }
        )
    return rank_persisted_content_rows(rows)


@st.cache_data(ttl=300)
def _load_asset_library() -> list[dict]:
    return load_assets(DEMO_DB_PATH, channel=DEMO_CHANNEL_KEY)


def _episode_table(rows: list[dict[str, object]]) -> None:
    display = pd.DataFrame(rows)
    if display.empty:
        return
    display = display[
        [
            "title",
            "tier",
            "score",
            "views",
            "watch_hours",
            "subscribers_gained",
            "avg_pct_viewed",
        ]
    ].rename(
        columns={
            "title": "Title",
            "tier": "Tier",
            "score": "Score",
            "views": "Views",
            "watch_hours": "Watch Hours",
            "subscribers_gained": "Subscribers Gained",
            "avg_pct_viewed": "Average Watched %",
        }
    )
    display["Tier"] = display["Tier"].map(lambda tier: TIER_LABELS.get(tier, tier))
    st.dataframe(display, width="stretch", hide_index=True)


def _episode_cards(rows: list[dict[str, object]]) -> None:
    for row in rows:
        with st.expander(f"{row['title']} — {float(row['score']):.0f}/100"):
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Content Score", f"{float(row['score']):.0f}/100")
            c2.metric("Views", f"{int(row['views']):,}")
            c3.metric("Watch Hours", f"{float(row['watch_hours']):,.0f}")
            c4.metric("Average Watched", f"{float(row['avg_pct_viewed']):.0f}%")
            labels = row["classifications"]
            if labels:
                st.markdown(
                    "**Signals:** "
                    + " · ".join(
                        _CLASSIFICATION_LABELS.get(label, label) for label in labels
                    )
                )
            st.info(str(row["recommended_action"]), icon="💡")
            st.caption(f"Video ID: `{row['video_id']}`")


def _panel(rows: list[dict[str, object]], empty_message: str) -> None:
    if not rows:
        st.info(empty_message)
        return
    table_tab, card_tab = st.tabs(["Table", "Cards"])
    with table_tab:
        _episode_table(rows)
    with card_tab:
        _episode_cards(rows)


def _asset_tile(asset: dict) -> None:
    key_id = str(asset.get("asset_id") or asset.get("id") or "asset")
    with st.expander(
        f"{asset.get('title', 'Untitled asset')} · {asset.get('status', 'draft')}"
    ):
        st.markdown(str(asset.get("body") or asset.get("content") or ""))
        st.caption(
            f"{str(asset.get('asset_type', '')).replace('_', ' ').title()} · derived from {asset.get('video_title', 'a demo video')}"
        )
        approve, reject, schedule = st.columns(3)
        approve.button(
            "Preview Approve",
            key=f"preview_approve_{key_id}",
            disabled=True,
            width="stretch",
        )
        reject.button(
            "Preview Reject",
            key=f"preview_reject_{key_id}",
            disabled=True,
            width="stretch",
        )
        schedule.button(
            "Preview Schedule",
            key=f"preview_schedule_{key_id}",
            disabled=True,
            width="stretch",
        )
        st.caption(
            "Public demo controls are previews only. Asset approval, rejection, and scheduling "
            "are disabled so the bundled fixture remains read-only."
        )


render_demo_sidebar("Content Intelligence")
render_demo_notice()
st.title("🧠 Content Intelligence")
st.caption(
    f"Scores daily increments aggregated through {DEMO_AS_OF:%B %d, %Y} and surfaces "
    "content patterns, repackaging opportunities, and a read-only draft asset library."
)

rows = _load_scored_library()
if not rows:
    render_empty_state("content intelligence")
    st.stop()

st.caption(f"{len(rows)} videos scored with the production content model")
tab_top, tab_magnets, tab_gems, tab_repackaging, tab_assets = st.tabs(
    [
        "🏆 Top Episodes",
        "🧲 Subscriber Magnets",
        "💎 Hidden Gems",
        "🎨 Repackaging Opportunities",
        "📁 Draft Asset Library",
    ]
)

with tab_top:
    st.subheader("Top Episodes")
    show_count = st.slider("Show top N", min_value=5, max_value=50, value=20, step=5)
    _panel(rows[:show_count], "No scored episodes are available.")

with tab_magnets:
    st.subheader("Subscriber Magnets")
    magnets = content_tier_rows(rows, "subscriber_magnet")
    _panel(magnets, "No subscriber magnets meet the current scoring thresholds.")

with tab_gems:
    st.subheader("Hidden Gems")
    gems = content_tier_rows(rows, "hidden_gem")
    _panel(gems, "No hidden gems meet the current scoring thresholds.")

with tab_repackaging:
    st.subheader("Repackaging Opportunities")
    repackaging = content_repackaging_rows(rows)
    _panel(repackaging, "No repackaging opportunities meet the current thresholds.")

with tab_assets:
    st.subheader("Draft Asset Library")
    st.info(
        "This public library is read-only. Workflow controls are shown as disabled previews and never write to the fixture.",
        icon="🔒",
    )
    assets = _load_asset_library()
    if not assets:
        render_empty_state("content assets")
    else:
        filter_a, filter_b = st.columns(2)
        statuses = sorted({str(asset.get("status", "draft")) for asset in assets})
        asset_types = sorted({str(asset.get("asset_type", "")) for asset in assets})
        selected_status = filter_a.selectbox("Status", ["All"] + statuses)
        selected_type = filter_b.selectbox("Asset type", ["All"] + asset_types)
        filtered_assets = [
            asset
            for asset in assets
            if (selected_status == "All" or asset.get("status") == selected_status)
            and (selected_type == "All" or asset.get("asset_type") == selected_type)
        ]
        status_counts = pd.Series(
            [asset.get("status", "draft") for asset in assets]
        ).value_counts()
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Assets", len(assets))
        m2.metric("Drafts", int(status_counts.get("draft", 0)))
        m3.metric("Approved", int(status_counts.get("approved", 0)))
        m4.metric("Published", int(status_counts.get("published", 0)))
        for asset in filtered_assets:
            _asset_tile(asset)
