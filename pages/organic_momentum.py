"""Organic Momentum — page for the Human Workforce Analytics dashboard.

Answers:
  1. Which videos keep growing after promotion stops?
  2. Which promoted videos only produced paid spikes?
  3. Which videos deserve another $5–$20 promotion?
  4. Which videos should become long-form follow-ups?
  5. Which topics create repeatable organic momentum?
  6. Which videos produce qualifying watch hours most efficiently?
"""
from __future__ import annotations

import random
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import json as _json
import pandas as pd
import plotly.express as px
import streamlit as st

from analytics.organic_momentum import (
    MomentumScorer,
    ScoreWeights,
    build_momentum_data,
)
from channel_state import render_channel_selector
from models.organic_momentum import (
    MOMENTUM_CLASS_COLOR,
    MOMENTUM_CLASS_ICON,
    MomentumClass,
    OrganicMomentumMetrics,
    PromotionStatus,
    ScoreBreakdown,
)

st.set_page_config(page_title="Organic Momentum", layout="wide")

_active_channel = render_channel_selector()

_DB = Path(__file__).parent.parent / "data.db"

# ── Helpers ───────────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def _db_query(db_str: str, sql: str) -> pd.DataFrame:
    p = Path(db_str)
    if not p.exists():
        return pd.DataFrame()
    with sqlite3.connect(str(p)) as conn:
        try:
            return pd.read_sql_query(sql, conn)
        except Exception:
            return pd.DataFrame()


def _detect_topic(title: str) -> str:
    t = title.lower()
    if any(w in t for w in ["ai", "artificial intelligence", "chatgpt", "machine learning", "deepfake"]):
        return "AI"
    if any(w in t for w in ["job", "career", "hire", "layoff", "workforce", "worker"]):
        return "Workforce"
    if any(w in t for w in ["leader", "boss", "manage", "executive", "ceo"]):
        return "Leadership"
    if any(w in t for w in ["salary", "pay", "negotiat", "compensa"]):
        return "Compensation"
    if any(w in t for w in ["cybersecurity", "security", "hack", "scam", "espionage", "threat"]):
        return "Security"
    if any(w in t for w in ["remote", "hybrid", "office", "work from"]):
        return "Remote Work"
    if any(w in t for w in ["entrepreneur", "startup", "business", "hustle"]):
        return "Entrepreneurship"
    return "General"


# ── Demo data ─────────────────────────────────────────────────────────────────

_DEMO_SEED = 99
_DEMO_VIDEOS: list[tuple[str, str, int, bool]] = [
    ("d001", "AI Is Replacing White Collar Jobs Faster Than You Think", 2847, True),
    ("d002", "The Hidden Cost of Corporate Loyalty", 3214, True),
    ("d003", "Why Your LinkedIn Profile Is Lying To Employers", 1892, False),
    ("d004", "The Great Layoff: What No One Is Telling You", 3478, True),
    ("d005", "Negotiating Your Salary in a Recession", 2634, True),
    ("d006", "The Future of Remote Work in 2026", 2156, True),
    ("d007", "Side Hustles That Actually Scale Into Businesses", 2089, False),
    ("d008", "HR Secrets: What They Won't Say in Your Interview", 2478, True),
    ("d009", "The Skills Gap Nobody Is Talking About", 2912, False),
    ("d010", "Why Quiet Quitting Misses the Point", 1645, False),
    ("d011", "Corporate Burnout: Data Causes and Solutions", 2341, True),
    ("d012", "The Rise of AI Managers and What It Means for You", 3124, True),
    ("d013", "From Employee to Entrepreneur: A Realistic Guide", 2789, False),
    ("d014", "The Automation Paradox: More Jobs or Fewer?", 2956, True),
    ("d015", "What the Best Bosses Do Differently", 1823, False),
    ("d016", "Gen Z vs Millennials at Work: The Real Differences", 2234, True),
    ("d017", "Recession-Proof Careers: The Data-Backed List", 2678, True),
    ("d018", "How to Get Promoted Without Playing Office Politics", 2112, False),
]

_BASE_PUB = datetime(2025, 7, 1)


def _build_demo_data() -> list[OrganicMomentumMetrics]:
    rng = random.Random(_DEMO_SEED)
    scorer = MomentumScorer()
    metrics: list[OrganicMomentumMetrics] = []

    for i, (vid, title, length_s, promoted) in enumerate(_DEMO_VIDEOS):
        pub_dt = _BASE_PUB + timedelta(days=i * 14 + rng.randint(-3, 3))
        days_live = max((datetime.now() - pub_dt).days, 1)

        # Simulate different momentum profiles
        profile = i % 8
        base_views = rng.randint(2_000, 20_000)
        total_views = base_views + int(days_live * rng.uniform(3, 12))

        if promoted:
            promo_pct = rng.uniform(0.20, 0.60)
            promo_views = int(total_views * promo_pct)
        else:
            promo_pct = 0.0
            promo_views = 0

        organic_views = total_views - promo_views
        avg_dur = length_s * rng.uniform(0.30, 0.80)
        total_wh = total_views * avg_dur / 3600.0
        subs = int(organic_views * rng.uniform(0.002, 0.020))
        qualifying_wh = max(total_wh * (1.0 - promo_pct * 0.8), 0.0)

        # Growth rate varies by profile
        if profile == 0:    # breakout
            vgr = rng.uniform(0.30, 0.80)
            whgr = rng.uniform(0.25, 0.70)
        elif profile == 1:  # promising
            vgr = rng.uniform(0.05, 0.30)
            whgr = rng.uniform(0.05, 0.25)
        elif profile == 2:  # paid spike
            vgr = rng.uniform(-0.50, -0.10)
            whgr = rng.uniform(-0.45, -0.10)
        elif profile == 3:  # organic sleeper
            vgr = rng.uniform(0.10, 0.50)
            whgr = rng.uniform(0.08, 0.40)
            total_views = rng.randint(300, 1500)
            organic_views = total_views
            promo_views = 0
        elif profile == 4:  # needs packaging
            vgr = rng.uniform(-0.20, 0.05)
            whgr = rng.uniform(-0.15, 0.05)
            avg_dur = length_s * rng.uniform(0.65, 0.85)  # good retention
        elif profile == 5:  # retention problem
            vgr = rng.uniform(-0.30, 0.0)
            whgr = rng.uniform(-0.30, 0.0)
            avg_dur = length_s * rng.uniform(0.10, 0.22)  # people bail
        elif profile == 6:  # do not promote
            vgr = rng.uniform(-0.60, -0.20)
            whgr = rng.uniform(-0.55, -0.20)
        else:               # mixed
            vgr = rng.uniform(-0.10, 0.15)
            whgr = rng.uniform(-0.10, 0.12)

        recent_dv = max(float(total_views) / max(days_live, 1) * (1.0 + vgr * 0.5), 0.1)
        peak_dv = recent_dv * rng.uniform(1.5, 4.0)

        avg_pct = min((avg_dur / max(length_s, 1)) * 100.0, 100.0)
        organic_lift = organic_views / max(promo_views, 1) if promo_views > 0 else 0.0

        m = OrganicMomentumMetrics(
            video_id=vid,
            title=title,
            published_date=pub_dt.strftime("%Y-%m-%d"),
            video_length_seconds=length_s,
            promotion_status=PromotionStatus.promoted if promoted else PromotionStatus.not_promoted,
            promotion_start_date=None,
            promotion_end_date=None,
            promotion_cost=0.0,
            total_views=total_views,
            organic_views=organic_views,
            promotion_views=promo_views,
            post_promotion_organic_views=int(organic_views * max(0.4, 0.7 + vgr * 0.3)),
            total_watch_hours=round(total_wh, 2),
            estimated_qualifying_watch_hours=round(qualifying_wh, 2),
            post_promotion_organic_watch_hours=round(qualifying_wh * max(0.4, 0.7 + vgr * 0.3), 2),
            average_view_duration_seconds=avg_dur,
            average_percentage_viewed=round(avg_pct, 1),
            ctr=0.0,
            impressions=0,
            engaged_views=0,
            returning_viewers=0,
            subscribers=subs,
            follow_on_views=0,
            browse_views=0,
            suggested_views=0,
            search_views=0,
            organic_lift=round(organic_lift, 2),
            organic_watch_hour_lift=round(organic_lift * 0.3, 2),
            organic_momentum_per_dollar=0.0,
            view_growth_rate=round(vgr, 4),
            wh_growth_rate=round(whgr, 4),
            recent_daily_views=round(recent_dv, 1),
            peak_daily_views=round(peak_dv, 1),
            data_points=rng.randint(20, 60),
            organic_momentum_score=0.0,
            score_breakdown=ScoreBreakdown(),
            classification=MomentumClass.insufficient_data,
            recommended_action="",
            data_quality_flag="estimated",
        )
        metrics.append(m)

    return scorer.score_all(metrics)


# ── Data loader ───────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def _load_scored(db_str: str, weights_json: str, channel: str) -> list[dict]:
    """Cache returns plain dicts to avoid Pydantic/pickle issues."""
    import json
    w_dict = json.loads(weights_json)
    weights = ScoreWeights(**w_dict)
    metrics = build_momentum_data(db_str, channel)
    if not metrics:
        return []
    scored = MomentumScorer(weights).score_all(metrics)
    return [_m_to_dict(m) for m in scored]


def _m_to_dict(m: OrganicMomentumMetrics) -> dict:
    return {
        "video_id": m.video_id,
        "title": m.title,
        "published_date": m.published_date,
        "video_length_seconds": m.video_length_seconds,
        "promotion_status": m.promotion_status.value,
        "promotion_cost": m.promotion_cost,
        "total_views": m.total_views,
        "organic_views": m.organic_views,
        "promotion_views": m.promotion_views,
        "post_promotion_organic_views": m.post_promotion_organic_views,
        "total_watch_hours": m.total_watch_hours,
        "estimated_qualifying_watch_hours": m.estimated_qualifying_watch_hours,
        "post_promotion_organic_watch_hours": m.post_promotion_organic_watch_hours,
        "average_view_duration_seconds": m.average_view_duration_seconds,
        "average_percentage_viewed": m.average_percentage_viewed,
        "ctr": m.ctr,
        "impressions": m.impressions,
        "subscribers": m.subscribers,
        "follow_on_views": m.follow_on_views,
        "organic_lift": m.organic_lift,
        "organic_watch_hour_lift": m.organic_watch_hour_lift,
        "organic_momentum_per_dollar": m.organic_momentum_per_dollar,
        "view_growth_rate": m.view_growth_rate,
        "wh_growth_rate": m.wh_growth_rate,
        "recent_daily_views": m.recent_daily_views,
        "peak_daily_views": m.peak_daily_views,
        "data_points": m.data_points,
        "organic_momentum_score": m.organic_momentum_score,
        "classification": m.classification.value,
        "recommended_action": m.recommended_action,
        "data_quality_flag": m.data_quality_flag,
        # breakdown
        "bd_view_growth": m.score_breakdown.organic_views_growth,
        "bd_wh_growth": m.score_breakdown.organic_wh_growth,
        "bd_organic_ratio": m.score_breakdown.organic_ratio,
        "bd_completion": m.score_breakdown.completion_rate,
        "bd_sub_conv": m.score_breakdown.subscriber_conversion,
    }


def _dict_to_m(d: dict) -> OrganicMomentumMetrics:
    return OrganicMomentumMetrics(
        video_id=d["video_id"],
        title=d["title"],
        published_date=d["published_date"],
        video_length_seconds=d["video_length_seconds"],
        promotion_status=PromotionStatus(d["promotion_status"]),
        promotion_start_date=None,
        promotion_end_date=None,
        promotion_cost=d["promotion_cost"],
        total_views=d["total_views"],
        organic_views=d["organic_views"],
        promotion_views=d["promotion_views"],
        post_promotion_organic_views=d["post_promotion_organic_views"],
        total_watch_hours=d["total_watch_hours"],
        estimated_qualifying_watch_hours=d["estimated_qualifying_watch_hours"],
        post_promotion_organic_watch_hours=d["post_promotion_organic_watch_hours"],
        average_view_duration_seconds=d["average_view_duration_seconds"],
        average_percentage_viewed=d["average_percentage_viewed"],
        ctr=d["ctr"],
        impressions=d["impressions"],
        engaged_views=0,
        returning_viewers=0,
        subscribers=d["subscribers"],
        follow_on_views=d["follow_on_views"],
        browse_views=0,
        suggested_views=0,
        search_views=0,
        organic_lift=d["organic_lift"],
        organic_watch_hour_lift=d["organic_watch_hour_lift"],
        organic_momentum_per_dollar=d["organic_momentum_per_dollar"],
        view_growth_rate=d["view_growth_rate"],
        wh_growth_rate=d["wh_growth_rate"],
        recent_daily_views=d["recent_daily_views"],
        peak_daily_views=d["peak_daily_views"],
        data_points=d["data_points"],
        organic_momentum_score=d["organic_momentum_score"],
        score_breakdown=ScoreBreakdown(
            organic_views_growth=d["bd_view_growth"],
            organic_wh_growth=d["bd_wh_growth"],
            organic_ratio=d["bd_organic_ratio"],
            completion_rate=d["bd_completion"],
            subscriber_conversion=d["bd_sub_conv"],
        ),
        classification=MomentumClass(d["classification"]),
        recommended_action=d["recommended_action"],
        data_quality_flag=d["data_quality_flag"],
    )


# ═══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.header("Organic Momentum")
    st.markdown("---")

    st.markdown("**Filters**")
    promo_filter = st.radio(
        "Promotion status",
        ["All", "Promoted", "Not Promoted"],
        horizontal=True,
        key="om_promo",
    )
    min_views = st.number_input("Min total views", min_value=0, value=50, step=10, key="om_minv")
    cls_options = [c.value for c in MomentumClass if c != MomentumClass.insufficient_data]
    cls_filter = st.multiselect(
        "Classifications", cls_options, default=cls_options, key="om_cls"
    )

    st.markdown("---")
    st.markdown("**Advanced — Score Weights**")
    with st.expander("Adjust weights (must sum to 1.0)", expanded=False):
        w_vgr = st.slider("View growth trend", 0.0, 0.40, 0.20, 0.01, key="w_vgr")
        w_whgr = st.slider("Watch-hour growth trend", 0.0, 0.40, 0.20, 0.01, key="w_whgr")
        w_org = st.slider("Organic traffic ratio", 0.0, 0.30, 0.15, 0.01, key="w_org")
        w_comp = st.slider("Completion rate", 0.0, 0.20, 0.10, 0.01, key="w_comp")
        w_avgp = st.slider("Avg % viewed", 0.0, 0.20, 0.10, 0.01, key="w_avgp")
        w_sub = st.slider("Subscriber conversion", 0.0, 0.20, 0.10, 0.01, key="w_sub")
        w_ret = st.slider("Returning proxy", 0.0, 0.15, 0.05, 0.01, key="w_ret")
        w_fo = st.slider("Follow-on proxy", 0.0, 0.15, 0.05, 0.01, key="w_fo")
        w_ctr = st.slider("CTR proxy", 0.0, 0.15, 0.05, 0.01, key="w_ctr")
        total_w = round(w_vgr + w_whgr + w_org + w_comp + w_avgp + w_sub + w_ret + w_fo + w_ctr, 2)
        if abs(total_w - 1.0) > 0.01:
            st.warning(f"Weights sum to {total_w:.2f} — must equal 1.00")
            weights_valid = False
        else:
            st.success("✓ Weights sum to 1.00")
            weights_valid = True

    st.markdown("---")
    st.markdown("**Cross-links**")
    st.page_link("pages/content_intelligence.py", label="Content Intelligence", icon="🧠")
    st.page_link("pages/promotion_intelligence.py", label="Promotion Intelligence", icon="📣")

# ── Build scored data ─────────────────────────────────────────────────────────

if weights_valid:
    custom_weights = ScoreWeights(
        organic_views_growth=w_vgr,
        organic_wh_growth=w_whgr,
        organic_ratio=w_org,
        completion_rate=w_comp,
        avg_pct_viewed=w_avgp,
        subscriber_conversion=w_sub,
        returning_proxy=w_ret,
        follow_on_proxy=w_fo,
        ctr_proxy=w_ctr,
    )
else:
    custom_weights = ScoreWeights()

weights_json = _json.dumps(custom_weights.as_dict())

raw_dicts = _load_scored(str(_DB), weights_json, _active_channel)
if raw_dicts:
    all_metrics = [_dict_to_m(d) for d in raw_dicts]
    is_demo = False
else:
    all_metrics = _build_demo_data()
    is_demo = True

# ── Apply filters ─────────────────────────────────────────────────────────────

filtered = all_metrics
if promo_filter == "Promoted":
    filtered = [m for m in filtered if m.promotion_status == PromotionStatus.promoted]
elif promo_filter == "Not Promoted":
    filtered = [m for m in filtered if m.promotion_status == PromotionStatus.not_promoted]

filtered = [m for m in filtered if m.total_views >= min_views]

cls_filter_set = set(cls_filter) | {MomentumClass.insufficient_data.value}
filtered = [m for m in filtered if m.classification.value in cls_filter_set]

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE HEADER
# ═══════════════════════════════════════════════════════════════════════════════

st.title("🌱 Organic Momentum")
st.caption(
    "Measures whether a video continues gaining organic traction after its release "
    "or promotion period. Score 0–100 derived from view trend, watch-hour trend, "
    "organic traffic ratio, completion rate, and subscriber conversion."
)

if is_demo:
    st.info(
        "**Demo Mode** — no analytics data found in the database. "
        "Showing synthetic data to illustrate the feature.",
        icon="ℹ️",
    )

if not filtered:
    st.warning("No videos match the current filters.")
    st.stop()

# ═══════════════════════════════════════════════════════════════════════════════
# EXECUTIVE SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════

st.subheader("Executive Summary")

scored = [m for m in filtered if m.classification != MomentumClass.insufficient_data]
promoted_scored = [m for m in scored if m.promotion_status == PromotionStatus.promoted]
non_promoted = [m for m in scored if m.promotion_status == PromotionStatus.not_promoted]


def _top(lst: list[OrganicMomentumMetrics], key) -> Optional[OrganicMomentumMetrics]:
    return max(lst, key=key) if lst else None


def _bot(lst: list[OrganicMomentumMetrics], key) -> Optional[OrganicMomentumMetrics]:
    return min(lst, key=key) if lst else None


top_momentum = _top(scored, lambda m: m.organic_momentum_score)
top_wh_lift = _top(promoted_scored, lambda m: m.organic_watch_hour_lift)
best_roi = _top(promoted_scored, lambda m: m.organic_lift)
worst_spike = _bot(promoted_scored, lambda m: m.organic_lift) if promoted_scored else None
best_sleeper = _top(
    [m for m in non_promoted if m.view_growth_rate > 0],
    lambda m: m.view_growth_rate,
)
promote_next = _top(
    [m for m in non_promoted if m.classification in (MomentumClass.promising, MomentumClass.breakout)],
    lambda m: m.organic_momentum_score,
)

topics = {}
for m in scored:
    t = _detect_topic(m.title)
    topics.setdefault(t, []).append(m.organic_momentum_score)
best_topic = max(topics, key=lambda t: sum(topics[t]) / len(topics[t])) if topics else "—"
best_topic_avg = round(sum(topics.get(best_topic, [0])) / max(len(topics.get(best_topic, [1])), 1), 1)

total_qualifying_wh = sum(m.estimated_qualifying_watch_hours for m in scored)
weekly_budget_suggestion = round(
    sum(m.recent_daily_views for m in scored if m.classification == MomentumClass.promising) * 0.025 * 7,
    2,
)


def _kpi(label: str, value: str, sub: str = "", icon: str = "") -> None:
    st.metric(f"{icon} {label}".strip(), value, sub)


col1, col2, col3, col4 = st.columns(4)
with col1:
    if top_momentum:
        ico = MOMENTUM_CLASS_ICON.get(top_momentum.classification, "")
        st.metric(
            "🏆 Top Momentum Video",
            f"{top_momentum.organic_momentum_score:.0f} / 100",
            top_momentum.title[:32] + "…" if len(top_momentum.title) > 32 else top_momentum.title,
        )
    if promote_next:
        st.metric(
            "📣 Best to Promote Next",
            promote_next.title[:28] + "…" if len(promote_next.title) > 28 else promote_next.title,
            f"Score {promote_next.organic_momentum_score:.0f}",
        )

with col2:
    if top_wh_lift:
        st.metric(
            "⏱️ Highest Organic WH Lift",
            top_wh_lift.title[:28] + "…" if len(top_wh_lift.title) > 28 else top_wh_lift.title,
            f"{top_wh_lift.organic_watch_hour_lift:.2f}× lift",
        )
    if best_sleeper:
        st.metric(
            "😴 Best Sleeper",
            best_sleeper.title[:28] + "…" if len(best_sleeper.title) > 28 else best_sleeper.title,
            f"+{best_sleeper.view_growth_rate * 100:.0f}% daily growth",
        )

with col3:
    if best_roi:
        st.metric(
            "💰 Best Promotion ROI",
            best_roi.title[:28] + "…" if len(best_roi.title) > 28 else best_roi.title,
            f"{best_roi.organic_lift:.1f}× organic lift",
        )
    st.metric(
        "📚 Best Topic Pattern",
        best_topic,
        f"Avg momentum {best_topic_avg}",
    )

with col4:
    if worst_spike:
        st.metric(
            "💸 Worst Paid Spike",
            worst_spike.title[:28] + "…" if len(worst_spike.title) > 28 else worst_spike.title,
            f"{worst_spike.organic_lift:.2f}× organic lift",
        )
    st.metric(
        "💵 Suggested Weekly Budget",
        f"${weekly_budget_suggestion:.0f}",
        "for Promising videos at $0.025 CPV",
    )

st.markdown("---")

# ═══════════════════════════════════════════════════════════════════════════════
# TABS
# ═══════════════════════════════════════════════════════════════════════════════

tab_rank, tab_trends, tab_promo, tab_charts, tab_actions = st.tabs([
    "🏆 Rankings",
    "📈 Trends",
    "📣 Promotion Analysis",
    "📊 Charts",
    "⚡ Recommended Actions",
])

# ─────────────────────────────────────────────────────────────────────────────
# TAB 1 — RANKINGS
# ─────────────────────────────────────────────────────────────────────────────

with tab_rank:
    st.subheader("Organic Momentum Rankings")
    st.caption(f"{len(filtered)} videos · sorted by Organic Momentum Score")

    rows = []
    for m in filtered:
        cls_icon = MOMENTUM_CLASS_ICON.get(m.classification, "")
        length_min = f"{m.video_length_seconds // 60}:{m.video_length_seconds % 60:02d}"
        rows.append({
            "Title": m.title,
            "Published": m.published_date,
            "Length": length_min,
            "Status": m.promotion_status.value,
            "Total Views": m.total_views,
            "Organic Views": m.organic_views,
            "Promo Views": m.promotion_views,
            "Post-Promo Organic": m.post_promotion_organic_views,
            "Watch Hrs": round(m.total_watch_hours, 1),
            "Qualifying Hrs": round(m.estimated_qualifying_watch_hours, 1),
            "Post-Promo WH": round(m.post_promotion_organic_watch_hours, 1),
            "Avg Dur (s)": round(m.average_view_duration_seconds),
            "Avg Watched %": round(m.average_percentage_viewed, 1),
            "Subs": m.subscribers,
            "Organic Lift": round(m.organic_lift, 2),
            "WH Lift": round(m.organic_watch_hour_lift, 2),
            "Daily Views (recent)": round(m.recent_daily_views, 1),
            "View Growth": f"{m.view_growth_rate:+.0%}",
            "Score": round(m.organic_momentum_score, 1),
            "Class": f"{cls_icon} {m.classification.value}",
            "Action": m.recommended_action,
            "Data": m.data_quality_flag,
        })

    tbl_df = pd.DataFrame(rows)
    st.dataframe(
        tbl_df,
        use_container_width=True,
        height=520,
        column_config={
            "Score": st.column_config.ProgressColumn("Score", min_value=0, max_value=100, format="%.1f"),
            "Avg Watched %": st.column_config.ProgressColumn("Avg Watched %", min_value=0, max_value=100, format="%.1f%%"),
            "View Growth": st.column_config.TextColumn("View Growth"),
        },
        hide_index=True,
    )

# ─────────────────────────────────────────────────────────────────────────────
# TAB 2 — TRENDS
# ─────────────────────────────────────────────────────────────────────────────

with tab_trends:
    st.subheader("Momentum Score Leaderboard")

    lb_df = pd.DataFrame([
        {
            "Title": m.title[:45] + "…" if len(m.title) > 45 else m.title,
            "Score": round(m.organic_momentum_score, 1),
            "Classification": m.classification.value,
        }
        for m in filtered[:20]
    ])
    color_map = {cls.value: color for cls, color in MOMENTUM_CLASS_COLOR.items()}
    fig_lb = px.bar(
        lb_df.sort_values("Score"),
        x="Score",
        y="Title",
        color="Classification",
        color_discrete_map=color_map,
        orientation="h",
        height=max(350, len(lb_df) * 28),
    )
    fig_lb.update_layout(
        yaxis_title="",
        xaxis_range=[0, 100],
        legend={"orientation": "h", "y": -0.15},
        margin={"l": 10, "r": 20, "t": 30, "b": 80},
    )
    st.plotly_chart(fig_lb, use_container_width=True)

    st.subheader("View Growth Rate Distribution")
    st.caption("Positive = growing daily views; negative = declining. Hover for title.")
    gr_df = pd.DataFrame([
        {
            "Title": m.title[:45] + "…" if len(m.title) > 45 else m.title,
            "View Growth Rate": round(m.view_growth_rate * 100, 1),
            "Watch-Hour Growth": round(m.wh_growth_rate * 100, 1),
            "Classification": m.classification.value,
            "Score": round(m.organic_momentum_score, 1),
            "Total Views": m.total_views,
        }
        for m in filtered
    ])
    fig_gr = px.scatter(
        gr_df,
        x="View Growth Rate",
        y="Watch-Hour Growth",
        color="Classification",
        size="Total Views",
        size_max=35,
        hover_name="Title",
        hover_data=["Score", "View Growth Rate", "Watch-Hour Growth"],
        color_discrete_map=color_map,
        labels={"View Growth Rate": "View Growth (% change)", "Watch-Hour Growth": "WH Growth (% change)"},
        height=440,
    )
    fig_gr.add_vline(x=0, line_dash="dash", line_color="rgba(255,255,255,0.3)")
    fig_gr.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.3)")
    fig_gr.update_layout(legend={"orientation": "h", "y": -0.2})
    st.plotly_chart(fig_gr, use_container_width=True)

    st.subheader("Daily View Trend (Recent vs Peak)")
    st.caption("Shows recent daily views vs peak daily views — the gap indicates decay or growth.")
    tr_df = pd.DataFrame([
        {
            "Title": m.title[:40] + "…" if len(m.title) > 40 else m.title,
            "Recent Daily Views": round(m.recent_daily_views, 1),
            "Peak Daily Views": round(m.peak_daily_views, 1),
            "Classification": m.classification.value,
        }
        for m in sorted(filtered, key=lambda x: x.recent_daily_views, reverse=True)[:15]
    ])
    fig_tr = px.bar(
        tr_df,
        x="Title",
        y=["Peak Daily Views", "Recent Daily Views"],
        barmode="group",
        color_discrete_map={"Peak Daily Views": "#6b7280", "Recent Daily Views": "#22c55e"},
        height=420,
    )
    fig_tr.update_layout(
        xaxis_tickangle=-45,
        legend_title="",
        margin={"b": 140},
    )
    st.plotly_chart(fig_tr, use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# TAB 3 — PROMOTION ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────

with tab_promo:
    st.subheader("Promotion Analysis")
    promoted_list = [m for m in filtered if m.promotion_status == PromotionStatus.promoted]

    if not promoted_list:
        st.info("No promoted videos in the current filter set.", icon="ℹ️")
    else:
        st.caption(
            f"{len(promoted_list)} promoted videos · "
            "Post-promotion organic views are estimated from organic ratio × growth momentum."
        )
        st.info(
            "Promotion start/end dates are not yet in the database. "
            "Pre/during/post splits shown below are estimated from organic traffic ratio "
            "and view growth rate. Connect Google Ads for exact dates.",
            icon="⚠️",
        )

        pa_df = pd.DataFrame([
            {
                "Title": m.title[:40] + "…" if len(m.title) > 40 else m.title,
                "Promo Views": m.promotion_views,
                "Organic Views": m.organic_views,
                "Post-Promo Organic": m.post_promotion_organic_views,
                "Organic Lift": round(m.organic_lift, 2),
                "WH Lift": round(m.organic_watch_hour_lift, 2),
                "Classification": m.classification.value,
                "Score": round(m.organic_momentum_score, 1),
            }
            for m in sorted(promoted_list, key=lambda m: m.organic_momentum_score, reverse=True)
        ])

        # Stacked bar: Promo vs Organic views per video
        st.subheader("Organic vs Promoted Views")
        fig_pa = px.bar(
            pa_df,
            x="Title",
            y=["Promo Views", "Organic Views"],
            barmode="stack",
            color_discrete_map={"Promo Views": "#f59e0b", "Organic Views": "#22c55e"},
            hover_data=["Score", "Organic Lift"],
            height=420,
        )
        fig_pa.update_layout(
            xaxis_tickangle=-45,
            legend_title="Traffic Type",
            margin={"b": 140},
        )
        st.plotly_chart(fig_pa, use_container_width=True)

        # Post-promo organic watch hours
        st.subheader("Post-Promotion Qualifying Watch Hours (estimated)")
        wh_cols = st.columns(2)
        with wh_cols[0]:
            wh_df = pd.DataFrame([
                {
                    "Title": m.title[:40] + "…" if len(m.title) > 40 else m.title,
                    "Total Watch Hrs": round(m.total_watch_hours, 1),
                    "Qualifying Hrs": round(m.estimated_qualifying_watch_hours, 1),
                    "Post-Promo WH": round(m.post_promotion_organic_watch_hours, 1),
                }
                for m in sorted(promoted_list, key=lambda m: m.post_promotion_organic_watch_hours, reverse=True)[:10]
            ])
            fig_wh = px.bar(
                wh_df,
                x="Title",
                y=["Total Watch Hrs", "Qualifying Hrs", "Post-Promo WH"],
                barmode="group",
                color_discrete_map={
                    "Total Watch Hrs": "#6b7280",
                    "Qualifying Hrs": "#3b82f6",
                    "Post-Promo WH": "#22c55e",
                },
                height=400,
            )
            fig_wh.update_layout(xaxis_tickangle=-45, legend_title="", margin={"b": 140})
            st.plotly_chart(fig_wh, use_container_width=True)

        with wh_cols[1]:
            st.dataframe(pa_df, use_container_width=True, hide_index=True, height=400)

# ─────────────────────────────────────────────────────────────────────────────
# TAB 4 — CHARTS
# ─────────────────────────────────────────────────────────────────────────────

with tab_charts:
    if len(filtered) < 2:
        st.info("Need at least 2 videos to render charts.", icon="ℹ️")
    else:
        viz_df = pd.DataFrame([
            {
                "Title": m.title[:45] + "…" if len(m.title) > 45 else m.title,
                "Score": round(m.organic_momentum_score, 1),
                "Classification": m.classification.value,
                "Organic Ratio %": round(
                    m.organic_views / max(m.total_views, 1) * 100, 1
                ),
                "Completion %": min(round(m.average_percentage_viewed, 1), 105.0),
                "Avg Duration (s)": round(m.average_view_duration_seconds),
                "Post-Promo Watch Hrs": max(
                    round(m.post_promotion_organic_watch_hours, 1), 0.0
                ),
                "Qualifying Hrs": max(
                    round(m.estimated_qualifying_watch_hours, 1), 0.0
                ),
                "Total Views": m.total_views,
                "Organic Views": m.organic_views,
                "Subscribers": m.subscribers,
                "View Growth %": round(m.view_growth_rate * 100, 1),
                "WH Growth %": round(m.wh_growth_rate * 100, 1),
                "Promoted": m.promotion_status.value,
            }
            for m in filtered
        ])
        color_map = {cls.value: color for cls, color in MOMENTUM_CLASS_COLOR.items()}

        # ── Chart 1: Organic Ratio vs Momentum Score ─────────────────────────
        st.subheader("Organic Traffic Ratio vs Momentum Score")
        st.caption(
            "Right = more traffic comes from organic sources · "
            "Up = higher score · size = total views · hover for details"
        )
        fig_c1 = px.scatter(
            viz_df,
            x="Organic Ratio %",
            y="Score",
            color="Classification",
            size="Total Views",
            size_max=35,
            hover_name="Title",
            hover_data=["Organic Views", "Completion %", "View Growth %"],
            color_discrete_map=color_map,
            height=440,
        )
        fig_c1.add_vline(
            x=50, line_dash="dot", line_color="rgba(255,255,255,0.25)"
        )
        fig_c1.update_layout(
            xaxis={"range": [-5, 105], "title": "Organic Traffic Ratio (%)"},
            yaxis={"range": [-2, 102], "title": "Momentum Score (0–100)"},
            legend={"orientation": "h", "y": -0.22},
        )
        st.plotly_chart(fig_c1, use_container_width=True)

        c2left, c2right = st.columns(2)

        with c2left:
            # ── Chart 2: Organic Views vs Qualifying Hours ──────────────────
            st.subheader("Organic Views vs Qualifying Hours")
            st.caption("Size = subscriber count · hover for details")
            fig_c2 = px.scatter(
                viz_df,
                x="Organic Views",
                y="Qualifying Hrs",
                color="Classification",
                size="Subscribers",
                size_max=35,
                hover_name="Title",
                hover_data=["Score", "View Growth %"],
                color_discrete_map=color_map,
                height=380,
            )
            fig_c2.update_layout(
                showlegend=False,
                yaxis={"rangemode": "tozero"},
            )
            st.plotly_chart(fig_c2, use_container_width=True)

        with c2right:
            # ── Chart 3: View Growth vs Score ────────────────────────────────
            st.subheader("View Growth vs Momentum Score")
            st.caption("Positive X = growing · hover for details")
            fig_c3 = px.scatter(
                viz_df,
                x="View Growth %",
                y="Score",
                color="Classification",
                size="Total Views",
                size_max=35,
                hover_name="Title",
                hover_data=["WH Growth %", "Completion %"],
                color_discrete_map=color_map,
                height=380,
            )
            fig_c3.add_vline(x=0, line_dash="dash", line_color="rgba(255,255,255,0.3)")
            fig_c3.update_layout(
                showlegend=False,
                yaxis={"range": [-2, 102]},
            )
            st.plotly_chart(fig_c3, use_container_width=True)

        # ── Chart 4: Bubble — Score × Qualifying Hours × Organic Views ───────
        st.subheader("Momentum Score × Qualifying Watch Hours")
        st.caption(
            "Bubble size = organic views.  "
            "Upper-right with large bubbles = high-scoring, YPP-contributing content.  "
            "Hover for title."
        )
        bubble_df = viz_df.copy()
        bubble_df["Bubble Size"] = (bubble_df["Organic Views"] + 1).clip(lower=1)
        fig_bubble = px.scatter(
            bubble_df,
            x="Score",
            y="Qualifying Hrs",
            size="Bubble Size",
            color="Classification",
            color_discrete_map=color_map,
            hover_name="Title",
            hover_data=["Completion %", "View Growth %", "Organic Ratio %"],
            size_max=55,
            height=480,
        )
        fig_bubble.update_layout(
            xaxis={"range": [-2, 102], "title": "Momentum Score (0–100)"},
            yaxis={"rangemode": "tozero", "title": "Qualifying Watch Hours"},
            legend={"orientation": "h", "y": -0.18},
        )
        st.plotly_chart(fig_bubble, use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# TAB 5 — RECOMMENDED ACTIONS
# ─────────────────────────────────────────────────────────────────────────────

with tab_actions:
    st.subheader("Recommended Actions")
    st.caption("Grouped by classification · click to expand each video's score breakdown.")

    cls_order = [
        MomentumClass.breakout,
        MomentumClass.promising,
        MomentumClass.organic_sleeper,
        MomentumClass.needs_packaging,
        MomentumClass.paid_spike,
        MomentumClass.retention_problem,
        MomentumClass.do_not_promote,
        MomentumClass.insufficient_data,
    ]

    for cls in cls_order:
        group = [m for m in filtered if m.classification == cls]
        if not group:
            continue
        ico = MOMENTUM_CLASS_ICON.get(cls, "")
        color = MOMENTUM_CLASS_COLOR.get(cls, "#6b7280")
        st.markdown(
            f"<h4 style='color:{color}'>{ico} {cls.value} ({len(group)})</h4>",
            unsafe_allow_html=True,
        )
        for m in group:
            with st.expander(
                f"{m.title[:70]}… · Score {m.organic_momentum_score:.0f}"
                if len(m.title) > 70
                else f"{m.title} · Score {m.organic_momentum_score:.0f}",
                expanded=False,
            ):
                ac1, ac2, ac3 = st.columns(3)
                with ac1:
                    st.metric("Total Views", f"{m.total_views:,}")
                    st.metric("Organic Views", f"{m.organic_views:,}")
                    st.metric("Subscribers", f"{m.subscribers:,}")
                with ac2:
                    st.metric("Watch Hours", f"{m.total_watch_hours:.1f} h")
                    st.metric("Qualifying Hrs", f"{m.estimated_qualifying_watch_hours:.1f} h")
                    st.metric("Avg Watched %", f"{m.average_percentage_viewed:.1f}%")
                with ac3:
                    st.metric("View Growth", f"{m.view_growth_rate:+.0%}")
                    st.metric("Recent Daily Views", f"{m.recent_daily_views:.0f}")
                    st.metric("Data Quality", m.data_quality_flag)

                st.markdown(f"**Recommended Action:** {m.recommended_action}")

                # Score breakdown mini-bar
                bd = m.score_breakdown
                bd_data = pd.DataFrame([
                    {"Component": "View Growth", "Points": bd.organic_views_growth},
                    {"Component": "WH Growth", "Points": bd.organic_wh_growth},
                    {"Component": "Organic Ratio", "Points": bd.organic_ratio},
                    {"Component": "Completion", "Points": bd.completion_rate},
                    {"Component": "Sub Conv", "Points": bd.subscriber_conversion},
                ])
                fig_bd = px.bar(
                    bd_data, x="Points", y="Component", orientation="h",
                    range_x=[0, 25], height=200,
                    color_discrete_sequence=["#3b82f6"],
                )
                fig_bd.update_layout(margin={"l": 10, "r": 10, "t": 5, "b": 5}, showlegend=False)
                st.plotly_chart(fig_bd, use_container_width=True)
