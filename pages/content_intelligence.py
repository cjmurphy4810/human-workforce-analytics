"""Content Intelligence — Streamlit multipage page.

Identifies winning videos and generates draft community/social assets
for human review and approval. Nothing is auto-published.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import anthropic
import pandas as pd
import streamlit as st

from content_intelligence.models import (
    ASSET_TYPE_LABELS,
    TIER_LABELS,
    AssetType,
    VideoScore,
)
from content_intelligence.service import (
    load_assets,
    load_scores,
    run_scoring,
    save_asset,
    update_asset_status,
)
from content_intelligence.generation.drafts import generate_asset
from db import DB_PATH

st.set_page_config(page_title="Content Intelligence", layout="wide")


# ── helpers ───────────────────────────────────────────────────────────────────

def _ai_client() -> anthropic.Anthropic | None:
    try:
        key = st.secrets.get("ANTHROPIC_API_KEY", "") or ""
        if not key:
            import os
            key = os.environ.get("ANTHROPIC_API_KEY", "")
        return anthropic.Anthropic(api_key=key) if key else None
    except Exception:
        return None


@st.cache_data(ttl=300)
def _scores_cached(db_path: str) -> list[dict]:
    return load_scores(Path(db_path))


@st.cache_data(ttl=300)
def _assets_cached(db_path: str, asset_type: str = "", status: str = "", video_id: str = "") -> list[dict]:
    return load_assets(
        Path(db_path),
        asset_type=asset_type or None,
        status=status or None,
        video_id=video_id or None,
    )


def _to_df(scores: list[dict]) -> pd.DataFrame:
    if not scores:
        return pd.DataFrame()
    df = pd.DataFrame(scores)
    df["tier_label"] = df["tier"].map(TIER_LABELS)
    df["promo_pct"] = (df["promotion_ratio"] * 100).round(1)
    return df


def _score_cards(row: pd.Series) -> None:
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Overall", f"{row['overall_score']:.0f}/100")
    c2.metric("Engagement", f"{row['engagement_score']:.0f}/100")
    c3.metric("Evergreen", f"{row['evergreen_score']:.0f}/100")
    c4.metric("Sub Magnet", f"{row['subscriber_magnet_score']:.0f}/100")
    c5.metric("Hidden Gem", f"{row['hidden_gem_score']:.0f}/100")


def _asset_tile(asset: dict, db_path: Path) -> None:
    with st.expander(f"{asset['title']}  ·  `{asset['status']}`", expanded=False):
        body = asset["body"]
        if asset["asset_type"] == "poll":
            try:
                p = json.loads(body)
                st.markdown(f"**{p['question']}**")
                for opt in p.get("options", []):
                    st.markdown(f"- {opt}")
            except (json.JSONDecodeError, KeyError):
                st.code(body)
        elif asset["asset_type"] == "course_idea":
            try:
                c = json.loads(body)
                st.markdown(f"### {c.get('course_title', '')}")
                st.caption(f"Audience: {c.get('target_audience', '')} · {c.get('estimated_duration', '')}")
                for m in c.get("modules", []):
                    st.markdown(f"**{m['title']}** — {m['description']}")
            except (json.JSONDecodeError, KeyError):
                st.code(body)
        else:
            st.markdown(body)

        st.divider()
        ca, cr, cn = st.columns([1, 1, 4])
        status = asset["status"]

        if status == "draft":
            if ca.button("Approve", key=f"approve_{asset['asset_id']}"):
                update_asset_status(
                    db_path, asset["asset_id"], "approved",
                    approved_at=datetime.now(timezone.utc).isoformat(),
                )
                st.cache_data.clear()
                st.rerun()
            if cr.button("Reject", key=f"reject_{asset['asset_id']}"):
                update_asset_status(db_path, asset["asset_id"], "rejected")
                st.cache_data.clear()
                st.rerun()
        elif status == "approved":
            if ca.button("Published", key=f"publish_{asset['asset_id']}"):
                update_asset_status(db_path, asset["asset_id"], "published")
                st.cache_data.clear()
                st.rerun()
            if cr.button("Revoke", key=f"revoke_{asset['asset_id']}"):
                update_asset_status(db_path, asset["asset_id"], "draft")
                st.cache_data.clear()
                st.rerun()

        new_notes = cn.text_input(
            "Notes", value=asset.get("notes") or "",
            key=f"notes_{asset['asset_id']}",
            placeholder="Reviewer notes…",
        )
        if new_notes != (asset.get("notes") or ""):
            update_asset_status(db_path, asset["asset_id"], status, notes=new_notes)
            st.cache_data.clear()


def _gen_section(scores_df: pd.DataFrame, asset_type: AssetType, client: anthropic.Anthropic | None, db_path: Path) -> None:
    if scores_df.empty:
        st.info("Run scoring first.", icon="ℹ️")
        return
    options = scores_df.sort_values("overall_score", ascending=False).drop_duplicates("video_id")
    titles = options["title"].tolist()
    selected_title = st.selectbox("Video", titles, key=f"sel_{asset_type}")
    selected_id = options[options["title"] == selected_title]["video_id"].iloc[0]
    selected_row = options[options["video_id"] == selected_id].iloc[0]

    if st.button(f"Generate {ASSET_TYPE_LABELS[asset_type]}", key=f"gen_{asset_type}", type="primary"):
        if not client:
            st.error("ANTHROPIC_API_KEY not configured.", icon="🔑")
        else:
            vs = VideoScore(
                video_id=selected_row["video_id"],
                title=selected_row.get("title", selected_title),
                scored_at=selected_row.get("scored_at", ""),
                total_views=int(selected_row.get("total_views", 0)),
                watch_rate_pct=float(selected_row.get("watch_rate_pct", 0)),
                like_rate_pct=float(selected_row.get("like_rate_pct", 0)),
                sub_rate_pct=float(selected_row.get("sub_rate_pct", 0)),
                promotion_ratio=float(selected_row.get("promotion_ratio", 0)),
                engagement_score=float(selected_row.get("engagement_score", 0)),
                evergreen_score=float(selected_row.get("evergreen_score", 0)),
                subscriber_magnet_score=float(selected_row.get("subscriber_magnet_score", 0)),
                hidden_gem_score=float(selected_row.get("hidden_gem_score", 0)),
                overall_score=float(selected_row.get("overall_score", 0)),
                tier=selected_row.get("tier", "average"),
            )
            with st.spinner(f"Generating {ASSET_TYPE_LABELS[asset_type]}…"):
                try:
                    asset = generate_asset(client, vs, asset_type)
                    save_asset(db_path, asset)
                    st.cache_data.clear()
                    st.success("Draft saved — see Asset Library or Approval Queue.")
                except Exception as exc:
                    st.error(f"Generation failed: {exc}")

    st.divider()
    st.markdown(f"**Saved {ASSET_TYPE_LABELS[asset_type]} drafts for this video**")
    saved = load_assets(db_path, asset_type=asset_type, video_id=selected_id)
    if not saved:
        st.caption("No drafts yet.")
    else:
        for a in saved:
            _asset_tile(a, db_path)


# ── page ──────────────────────────────────────────────────────────────────────

st.title("Content Intelligence")
st.caption(
    "Identifies winning videos using existing analytics data and generates "
    "draft content assets for human review. Nothing is auto-published."
)

_db_path = Path(DB_PATH)
_client = _ai_client()

if not _client:
    st.sidebar.warning("ANTHROPIC_API_KEY not set — generation disabled.", icon="🔑")

with st.sidebar:
    st.header("Controls")
    if st.button("Run Scoring Now", type="primary", use_container_width=True):
        with st.spinner("Scoring all videos…"):
            try:
                _scored = run_scoring(_db_path)
                st.cache_data.clear()
                st.success(f"Scored {len(_scored)} videos.")
            except Exception as _e:
                st.error(f"Scoring failed: {_e}")
    st.caption("Reads existing analytics data — no new YouTube API calls.")

_scores = _scores_cached(str(_db_path))
_df = _to_df(_scores)

if _df.empty:
    st.info("No scores yet. Click **Run Scoring Now** in the sidebar to analyse your video catalog.", icon="ℹ️")
    st.stop()

_scored_date = _scores[0].get("scored_at", "—") if _scores else "—"
st.caption(f"Latest scores: {_scored_date}  ·  {len(_scores)} videos")

(
    _t_top, _t_mag, _t_gem,
    _t_lib, _t_com, _t_pol,
    _t_qte, _t_hok, _t_lin,
    _t_crs, _t_apq,
) = st.tabs([
    "Top Episodes", "Subscriber Magnets", "Hidden Gems",
    "Asset Library", "Community Posts", "Polls",
    "Quote Cards", "Short Hooks", "LinkedIn Posts",
    "Course Ideas", "Approval Queue",
])

# Top Episodes
with _t_top:
    st.subheader("Top Episodes")
    st.caption("Highest overall score AND above-median views — your best-performing content.")
    _top = _df[_df["tier"] == "top_episode"].sort_values("overall_score", ascending=False)
    if _top.empty:
        st.info("No top episodes identified yet.", icon="ℹ️")
    else:
        for _, _row in _top.iterrows():
            with st.expander(f"🏆 {_row['title']}", expanded=False):
                _score_cards(_row)
                st.caption(
                    f"Views: {int(_row['total_views']):,}  ·  "
                    f"Watch Rate: {_row['watch_rate_pct']:.1f}%  ·  "
                    f"Like Rate: {_row['like_rate_pct']:.2f}%  ·  "
                    f"Promo: {_row['promo_pct']:.0f}%"
                )

# Subscriber Magnets
with _t_mag:
    st.subheader("Subscriber Magnets")
    st.caption("Top subscriber conversion rate relative to organic views.")
    _mag = _df[_df["tier"] == "subscriber_magnet"].sort_values("subscriber_magnet_score", ascending=False)
    if _mag.empty:
        st.info("No subscriber magnets identified.", icon="ℹ️")
    else:
        for _, _row in _mag.iterrows():
            with st.expander(f"🧲 {_row['title']}", expanded=False):
                _score_cards(_row)
                st.caption(
                    f"Sub Rate: {_row['sub_rate_pct']:.3f}%  ·  "
                    f"Organic: {(1 - _row['promotion_ratio']) * 100:.0f}%  ·  "
                    f"Views: {int(_row['total_views']):,}"
                )

# Hidden Gems
with _t_gem:
    st.subheader("Hidden Gems")
    st.caption("High engagement but under-promoted — candidates for organic resharing.")
    _gem = _df[_df["tier"] == "hidden_gem"].sort_values("hidden_gem_score", ascending=False)
    if _gem.empty:
        st.info("No hidden gems identified.", icon="ℹ️")
    else:
        for _, _row in _gem.iterrows():
            with st.expander(f"💎 {_row['title']}", expanded=False):
                _score_cards(_row)
                st.caption(
                    f"Views: {int(_row['total_views']):,}  ·  "
                    f"Engagement: {_row['engagement_score']:.0f}/100  ·  "
                    f"Promo: {_row['promo_pct']:.0f}%"
                )

# Asset Library
with _t_lib:
    st.subheader("Asset Library")
    _all = load_assets(_db_path)
    if not _all:
        st.info("No assets yet. Use the asset tabs to generate drafts.", icon="ℹ️")
    else:
        _lf1, _lf2 = st.columns(2)
        _ftype = _lf1.selectbox("Filter by type", ["All"] + list(ASSET_TYPE_LABELS.values()), key="lib_ftype")
        _fstat = _lf2.selectbox("Filter by status", ["All", "draft", "approved", "rejected", "published"], key="lib_fstat")
        _ftype_key = next((k for k, v in ASSET_TYPE_LABELS.items() if v == _ftype), None)
        _filtered = [
            a for a in _all
            if (_ftype == "All" or a["asset_type"] == _ftype_key)
            and (_fstat == "All" or a["status"] == _fstat)
        ]
        _stat_counts = pd.Series([a["status"] for a in _all]).value_counts()
        _lk1, _lk2, _lk3, _lk4 = st.columns(4)
        _lk1.metric("Total", len(_all))
        _lk2.metric("Drafts", _stat_counts.get("draft", 0))
        _lk3.metric("Approved", _stat_counts.get("approved", 0))
        _lk4.metric("Published", _stat_counts.get("published", 0))
        st.divider()
        for _a in _filtered:
            _asset_tile(_a, _db_path)

# Generation tabs
with _t_com:
    st.subheader("Community Posts")
    _gen_section(_df, "community_post", _client, _db_path)

with _t_pol:
    st.subheader("Polls")
    _gen_section(_df, "poll", _client, _db_path)

with _t_qte:
    st.subheader("Quote Cards")
    _gen_section(_df, "quote_card", _client, _db_path)

with _t_hok:
    st.subheader("Short Hooks")
    _gen_section(_df, "short_hook", _client, _db_path)

with _t_lin:
    st.subheader("LinkedIn Posts")
    _gen_section(_df, "linkedin_post", _client, _db_path)

with _t_crs:
    st.subheader("Course Ideas")
    _gen_section(_df, "course_idea", _client, _db_path)

# Approval Queue
with _t_apq:
    st.subheader("Approval Queue")
    st.caption("All draft assets awaiting review.")
    _drafts = load_assets(_db_path, status="draft")
    if not _drafts:
        st.success("Inbox zero — no pending drafts.", icon="✅")
    else:
        _by_type: dict[str, list] = {}
        for _a in _drafts:
            _by_type.setdefault(_a["asset_type"], []).append(_a)
        for _atype, _items in sorted(_by_type.items()):
            st.markdown(f"**{ASSET_TYPE_LABELS.get(_atype, _atype)}** ({len(_items)} pending)")
            for _a in _items:
                _asset_tile(_a, _db_path)
