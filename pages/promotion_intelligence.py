"""Promotion Intelligence — Streamlit dashboard page.

Analyses historical promotion performance and recommends future promotions
using a rule-based weighted scoring engine (no external ML libraries).

Five sections (tabs):
  1. Recommendation Cards — top/bottom/specialist lists
  2. All Videos           — sortable scored table
  3. ROI Calculator       — $5/$10/$20/$50 budget scenarios per video
  4. Visualizations       — scatter / heatmap / bubble charts
  5. Explainability       — per-video natural-language reasoning
"""
from __future__ import annotations

import random
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.express as px
import streamlit as st

from analytics.promotion_efficiency import compute_efficiency_scores
from channel_state import render_channel_selector
from db import DB_PATH
from models.promotion import VideoPromotionMetrics, make_metrics
from promotion_intelligence.promotion_prediction import PromotionPredictor
from promotion_intelligence.promotion_roi import BUDGET_TIERS, ROICalculator
from promotion_intelligence.recommendation_engine import RecommendationEngine
from promotion_intelligence.recommendation_models import (
    PROMOTION_CLASS_COLOR,
    PROMOTION_CLASS_ICON,
    PromotionClass,
    PromotionOpportunity,
    ROIEstimate,
    VideoFeatures,
)

st.set_page_config(page_title="Promotion Intelligence", layout="wide")
_active_channel = render_channel_selector()

_DB = Path(DB_PATH)


# ── Topic detection ───────────────────────────────────────────────────────────

_TOPIC_MAP: dict[str, list[str]] = {
    "AI": ["ai ", "artificial intelligence", "machine learning", "automation", "chatgpt", "gpt", "robot"],
    "Career": ["career", "job", "hiring", "resume", "salary", "promotion", "interview", "layoff"],
    "Leadership": ["leadership", "leader", "boss", "manag", "executive", "ceo", "c-suite"],
    "Entrepreneurship": ["entrepreneur", "business", "startup", "freelance", "side hustle", "founder"],
    "Wellness": ["burnout", "stress", "mental health", "wellness", "wellbeing", "anxiety"],
    "Culture": ["culture", "remote", "hybrid", "office", "diversity", "quiet quitting"],
    "Workforce": ["workforce", "employment", "worker", "labor", "talent", "skill", "gap"],
}


def _detect_topic(title: str) -> str:
    lower = title.lower()
    for topic, keywords in _TOPIC_MAP.items():
        if any(kw in lower for kw in keywords):
            return topic
    return "General"


# ── Demo data ─────────────────────────────────────────────────────────────────

_DEMO_SEED = 42
_DEMO_VIDEOS: list[tuple[str, str, int, str, str, str, str]] = [
    ("d001", "AI Is Replacing White Collar Jobs Faster Than You Think", 2847, "AI Jobs Series", "Workforce AI", "Career", "Future of Work"),
    ("d002", "The Hidden Cost of Corporate Loyalty", 3214, "Corporate Culture", "Workforce AI", "Leadership", ""),
    ("d003", "Why Your LinkedIn Profile Is Lying To Employers", 1892, "", "", "Career", ""),
    ("d004", "The Great Layoff: What No One Is Telling You", 3478, "Layoff Series", "Workforce AI", "Career", ""),
    ("d005", "Negotiating Your Salary in a Recession", 2634, "Salary Series", "Workforce AI", "Career", ""),
    ("d006", "The Future of Remote Work in 2026", 2156, "Remote Work Series", "", "Workforce", ""),
    ("d007", "Side Hustles That Actually Scale Into Businesses", 2089, "", "", "Entrepreneurship", ""),
    ("d008", "HR Secrets: What They Won't Say in Your Interview", 2478, "HR Insider Series", "HR Campaign", "Career", ""),
    ("d009", "The Skills Gap Nobody Is Talking About", 2912, "", "", "Workforce", ""),
    ("d010", "Why Quiet Quitting Misses the Point", 1645, "", "", "Culture", ""),
    ("d011", "Corporate Burnout: Data Causes and Solutions", 2341, "Wellness Series", "Workforce AI", "Wellness", ""),
    ("d012", "The Rise of AI Managers and What It Means for You", 3124, "AI Jobs Series", "Workforce AI", "AI", ""),
    ("d013", "From Employee to Entrepreneur: A Realistic Guide", 2789, "", "", "Entrepreneurship", ""),
    ("d014", "The Automation Paradox: More Jobs or Fewer?", 2956, "AI Jobs Series", "Workforce AI", "AI", ""),
    ("d015", "What the Best Bosses Do Differently", 1823, "", "", "Leadership", ""),
    ("d016", "Gen Z vs Millennials at Work: The Real Differences", 2234, "Generational Series", "HR Campaign", "Culture", ""),
    ("d017", "Recession-Proof Careers: The Data-Backed List", 2678, "Salary Series", "Workforce AI", "Career", ""),
    ("d018", "How to Get Promoted Without Playing Office Politics", 2112, "", "", "Career", ""),
]

_PROMO_IDS: set[str] = {
    "d001", "d002", "d004", "d005", "d006",
    "d008", "d009", "d011", "d012", "d014", "d016", "d017",
}

_BASE_DATE = datetime(2025, 7, 1)


def _build_demo_features(cpv: float = 0.025) -> list[VideoFeatures]:
    rng = random.Random(_DEMO_SEED)
    raw: list[VideoPromotionMetrics] = []

    for i, (vid, title, length_s, series, _campaign, topic, book) in enumerate(_DEMO_VIDEOS):
        published = _BASE_DATE + timedelta(days=i * 14 + rng.randint(-3, 3))
        days_live = max((datetime.now() - published).days, 1)
        has_promo = vid in _PROMO_IDS

        total_views = rng.randint(3_000, 25_000) + int(days_live * rng.uniform(2, 8))
        avg_dur = length_s * rng.uniform(0.35, 0.75)
        total_wh = total_views * avg_dur / 3600.0

        if has_promo:
            promo_pct = rng.uniform(0.28, 0.62)
            promo_views = int(total_views * promo_pct)
            avg_promo_dur = avg_dur * rng.uniform(0.30, 0.50)
        else:
            promo_views = 0
            avg_promo_dur = 0.0

        subs = int(total_views * rng.uniform(0.003, 0.018))
        follow_on = int(total_views * rng.uniform(0.04, 0.18))
        promo_cost = promo_views * cpv

        m = make_metrics(
            video_id=vid,
            title=title,
            published=published,
            length_seconds=length_s,
            total_views=total_views,
            promotion_views=promo_views,
            total_watch_hours=total_wh,
            avg_promotion_view_duration_seconds=avg_promo_dur,
            promotion_cost=promo_cost,
            subscribers=subs,
            follow_on_views=follow_on,
            avg_view_duration_seconds=avg_dur,
            series=series,
            data_source="ESTIMATED" if has_promo else "NONE",
        )
        raw.append(m)

    raw = compute_efficiency_scores(raw)

    features: list[VideoFeatures] = []
    for m, (_, _, _, series, _camp, topic, book) in zip(raw, _DEMO_VIDEOS):
        feat = _vpm_to_features(m, cpv, topic=topic, series=series, book=book)
        features.append(feat)
    return features


# ── DB data loader ────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def _db_query(db_str: str, sql: str, params: tuple | dict | None = None) -> pd.DataFrame:
    p = Path(db_str)
    if not p.exists():
        return pd.DataFrame()
    with sqlite3.connect(str(p)) as conn:
        try:
            return pd.read_sql_query(sql, conn, params=params)
        except Exception:
            return pd.DataFrame()


def _build_real_features(db: Path, cpv: float, channel: str) -> list[VideoFeatures]:
    """Load metrics from the real DB and build VideoFeatures."""
    vids = _db_query(str(db),
        "SELECT video_id, title, published_at, duration_seconds FROM videos WHERE channel = :channel",
        params={"channel": channel})
    if vids.empty:
        return []

    # Latest cumulative view count from video_snapshots
    snap = _db_query(str(db),
        "SELECT video_id, view_count FROM video_snapshots WHERE channel = :channel ORDER BY captured_at",
        params={"channel": channel})
    if not snap.empty:
        snap = snap.groupby("video_id", as_index=False).last()[["video_id", "view_count"]]

    # Latest analytics from daily_video_metrics
    dvm = _db_query(str(db), """
        SELECT d.video_id,
               d.estimated_minutes_watched / 60.0 AS total_watch_hours,
               d.average_view_duration AS avg_view_duration,
               COALESCE(d.subscribers_gained, 0) AS subscribers_gained,
               COALESCE(d.likes, 0) AS likes
        FROM daily_video_metrics d
        INNER JOIN (
            SELECT video_id, MAX(metric_date) AS latest_date
            FROM daily_video_metrics WHERE channel = :channel GROUP BY video_id
        ) latest ON d.video_id = latest.video_id AND d.metric_date = latest.latest_date
        WHERE d.channel = :channel
    """, params={"channel": channel})

    # ADVERTISING traffic (paid promotion)
    adv = _db_query(str(db), """
        SELECT d.video_id,
               d.views AS adv_views,
               d.estimated_minutes_watched / 60.0 AS adv_watch_hours,
               d.average_view_duration AS avg_adv_view_duration
        FROM video_traffic_source_metrics d
        INNER JOIN (
            SELECT video_id, MAX(metric_date) AS latest_date
            FROM video_traffic_source_metrics
            WHERE traffic_source_type = 'ADVERTISING' AND channel = :channel GROUP BY video_id
        ) latest ON d.video_id = latest.video_id AND d.metric_date = latest.latest_date
        WHERE d.traffic_source_type = 'ADVERTISING' AND d.channel = :channel
    """, params={"channel": channel})

    # RELATED_VIDEO traffic (follow-on discovery)
    rel = _db_query(str(db), """
        SELECT video_id, SUM(views) AS follow_on_views
        FROM video_traffic_source_metrics
        WHERE traffic_source_type = 'RELATED_VIDEO' AND channel = :channel
        GROUP BY video_id
    """, params={"channel": channel})

    # Latest CI score for this video
    ci = _db_query(str(db), """
        SELECT s.video_id, s.overall_score AS ci_overall_score
        FROM ci_video_scores s
        INNER JOIN (SELECT MAX(scored_at) AS latest FROM ci_video_scores WHERE channel = :channel) m
          ON s.scored_at = m.latest
        WHERE s.channel = :channel
    """, params={"channel": channel})

    df = vids.copy()
    for side, col_default in [
        (snap, {"view_count": 0}),
        (dvm, {"total_watch_hours": 0.0, "avg_view_duration": 0.0,
               "subscribers_gained": 0, "likes": 0}),
        (adv, {"adv_views": 0, "adv_watch_hours": 0.0, "avg_adv_view_duration": 0.0}),
        (rel, {"follow_on_views": 0}),
        (ci, {"ci_overall_score": 0.0}),
    ]:
        if not side.empty:
            df = df.merge(side, on="video_id", how="left")
        for col, default in col_default.items():
            if col not in df.columns:
                df[col] = default
            df[col] = df[col].fillna(default)

    # Build raw VideoPromotionMetrics for efficiency scoring
    has_adv = not adv.empty
    raw: list[VideoPromotionMetrics] = []
    for _, row in df.iterrows():
        published: Optional[datetime] = None
        if pd.notna(row.get("published_at")):
            try:
                published = pd.to_datetime(row["published_at"]).to_pydatetime().replace(tzinfo=None)
            except Exception:
                pass

        adv_views = int(row.get("adv_views", 0))
        avg_adv_dur = float(row.get("avg_adv_view_duration", 0))
        avg_dur = float(row.get("avg_view_duration", 0))

        m = make_metrics(
            video_id=str(row["video_id"]),
            title=str(row.get("title", "")),
            published=published,
            length_seconds=int(row.get("duration_seconds") or 0),
            total_views=int(row["view_count"]),
            promotion_views=adv_views if has_adv else 0,
            total_watch_hours=float(row["total_watch_hours"]),
            avg_promotion_view_duration_seconds=avg_adv_dur,
            promotion_cost=adv_views * cpv,
            subscribers=int(row["subscribers_gained"]),
            follow_on_views=int(row.get("follow_on_views", 0)),
            avg_view_duration_seconds=avg_dur,
            data_source="API_ACTUAL" if has_adv and adv_views > 0 else ("API_ACTUAL" if has_adv else "NONE"),
        )
        raw.append(m)

    # Post-process: override promotion_watch_hours with API_ACTUAL values
    if has_adv:
        corrected: list[VideoPromotionMetrics] = []
        for m, (_, row) in zip(raw, df.iterrows()):
            import dataclasses as _dc
            if m.data_source == "API_ACTUAL" and float(row.get("adv_watch_hours", 0)) > 0:
                adv_wh = float(row["adv_watch_hours"])
                org_wh = max(m.total_watch_hours - adv_wh, 0.0)
                promo_pct = (m.promotion_views / max(m.total_views, 1)) * 100
                corrected.append(_dc.replace(
                    m,
                    promotion_watch_hours=adv_wh,
                    organic_watch_hours=org_wh,
                    estimated_qualifying_hours=org_wh,
                    promotion_percentage=promo_pct,
                ))
            else:
                corrected.append(m)
        raw = corrected

    raw = compute_efficiency_scores(raw)

    # Build VideoFeatures
    ci_map = dict(zip(df["video_id"], df["ci_overall_score"]))
    features: list[VideoFeatures] = []
    for m in raw:
        feat = _vpm_to_features(
            m, cpv,
            topic=_detect_topic(m.title),
            ci_overall_score=float(ci_map.get(m.video_id, 0.0)),
        )
        features.append(feat)
    return features


def _vpm_to_features(
    m: VideoPromotionMetrics,
    cpv: float,
    topic: str = "",
    series: str = "",
    book: str = "",
    ci_overall_score: float = 0.0,
) -> VideoFeatures:
    """Convert a VideoPromotionMetrics to VideoFeatures for the scoring engine."""
    today = datetime.now()
    age_days = max((today - m.published).days, 1) if m.published else 365
    length_s = m.length_seconds or 1

    retention_pct = min(m.avg_view_duration_seconds / length_s * 100.0, 100.0)
    sub_per_1k = m.subscribers / max(m.organic_views, 1) * 1000.0
    vpd = m.total_views / max(age_days, 1)
    follow_on_rate = m.follow_on_views / max(m.total_views, 1) * 100.0
    promo_ratio = m.promotion_percentage  # already %
    organic_mult = m.organic_views / max(m.promotion_views, 1) if m.promotion_views > 0 else 0.0

    has_data = m.organic_views >= 50 and age_days >= 14

    return VideoFeatures(
        video_id=m.video_id,
        title=m.title,
        total_views=m.total_views,
        organic_views=m.organic_views,
        promotion_views=m.promotion_views,
        subscribers_gained=m.subscribers,
        follow_on_views=m.follow_on_views,
        likes=getattr(m, "likes", 0),
        total_watch_hours=m.total_watch_hours,
        organic_watch_hours=m.organic_watch_hours,
        qualifying_hours=m.estimated_qualifying_hours,
        avg_view_duration_seconds=m.avg_view_duration_seconds,
        avg_promotion_view_duration_seconds=m.avg_promotion_view_duration_seconds,
        audience_retention_pct=round(retention_pct, 1),
        subscriber_conversion_per_1k=round(sub_per_1k, 2),
        views_per_day=round(vpd, 1),
        follow_on_rate_pct=round(follow_on_rate, 1),
        promotion_ratio_pct=round(promo_ratio, 1),
        organic_multiplier=round(organic_mult, 2),
        promotion_efficiency_score=m.promotion_efficiency_score,
        ci_overall_score=ci_overall_score,
        cpv=cpv,
        promotion_cost_estimated=m.promotion_cost,
        cost_per_qualified_hour=m.cost_per_qualified_hour,
        cost_per_subscriber=m.cost_per_subscriber,
        cost_per_follow_on_view=m.cost_per_follow_on_view,
        video_age_days=age_days,
        length_seconds=length_s,
        language=m.language or "en",
        series=series or m.series,
        book=book,
        topic=topic or _detect_topic(m.title),
        data_source=m.data_source,
        has_sufficient_data=has_data,
    )


# ── Page rendering helpers ────────────────────────────────────────────────────


def _cls_badge(cls: PromotionClass) -> str:
    icon = PROMOTION_CLASS_ICON.get(cls, "")
    return f"{icon} {cls.value}"


def _score_bar(score: float) -> str:
    filled = round(score / 5)
    return "█" * filled + "░" * (20 - filled)


def _opp_card(opp: PromotionOpportunity, expanded: bool = False) -> None:
    feat = opp.features
    icon = PROMOTION_CLASS_ICON.get(opp.classification, "")
    title_short = opp.title[:60] + "…" if len(opp.title) > 60 else opp.title

    with st.expander(
        f"#{opp.rank}  {icon} **{title_short}** — Score: **{opp.score:.0f}/100**",
        expanded=expanded,
    ):
        c1, c2, c3 = st.columns(3)
        c1.metric("Promotion Score", f"{opp.score:.0f}/100")
        c2.metric("Classification", opp.classification.value)
        c3.metric("Organic Views", f"{feat.organic_views:,}")

        c4, c5, c6, c7 = st.columns(4)
        c4.metric("Retention", f"{feat.audience_retention_pct:.0f}%")
        c5.metric("Subs / 1K views", f"{feat.subscriber_conversion_per_1k:.1f}")
        c6.metric("Qualifying Hrs", f"{feat.qualifying_hours:.0f} h")
        c7.metric("PES", f"{feat.promotion_efficiency_score:.0f}/100")

        st.markdown(
            f"> 💡 {opp.explanation}",
        )

        bd = opp.breakdown
        score_df = pd.DataFrame({
            "Component": [
                "Audience Retention (25)",
                "Subscriber Conversion (20)",
                "Organic Watch Hours (20)",
                "Views Per Day (15)",
                "Follow-on Rate (10)",
                "Promotion Efficiency (10)",
            ],
            "Score": [
                bd.retention, bd.subscriber_conversion,
                bd.organic_hours, bd.views_per_day,
                bd.follow_on_rate, bd.promotion_efficiency,
            ],
            "Max": [25, 20, 20, 15, 10, 10],
        })
        score_df["% of Max"] = (score_df["Score"] / score_df["Max"] * 100).round(0)
        st.dataframe(
            score_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Score": st.column_config.ProgressColumn("Score", min_value=0, max_value=25, format="%.1f"),
                "% of Max": st.column_config.ProgressColumn("% of Max", min_value=0, max_value=100, format="%.0f%%"),
            },
        )


def _roi_card(est: ROIEstimate) -> None:
    confidence_color = {"high": "🟢", "medium": "🟡", "low": "🔴"}.get(est.confidence, "⚪")
    st.markdown(f"### ${est.budget:.0f} Budget  {confidence_color} {est.confidence.title()} Confidence")
    c1, c2, c3 = st.columns(3)
    c1.metric("Est. Views", f"{est.estimated_views:,}")
    c2.metric("Est. Subscribers", f"{est.estimated_subscribers:,}")
    c3.metric("Est. Qualifying Hrs", f"{est.estimated_qualifying_hours:.1f} h")
    c4, c5, c6 = st.columns(3)
    c4.metric("Organic Lift", f"+{est.estimated_organic_lift:,} views")
    c5.metric("Follow-on Views", f"{est.estimated_follow_on_views:,}")
    c6.metric("Expected PES", f"{est.expected_promotion_efficiency:.0f}/100")
    c7, c8, c9 = st.columns(3)
    c7.metric("$/Qualifying Hr", f"${est.cost_per_qualified_hour_projected:.2f}")
    c8.metric("$/Subscriber", f"${est.cost_per_subscriber_projected:.2f}")
    c9.metric("$/Follow-on View", f"${est.cost_per_follow_on_projected:.3f}")
    st.caption(f"ℹ️ {est.confidence_reason}")


# ── Main page ─────────────────────────────────────────────────────────────────

st.title("Promotion Intelligence")
st.caption(
    "Scores every video on its promotion potential using organic performance, "
    "historical promotion efficiency, and growth signals. "
    "Recommends where ad dollars will generate the highest qualifying-hour and subscriber ROI."
)

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Configuration")
    cpv = st.number_input(
        "Cost Per View (CPV) $",
        min_value=0.005, max_value=0.20, value=0.025, step=0.005, format="%.3f",
        help="YouTube in-stream CPV. Override with your actual channel CPV if known.",
    )
    min_organic_views = st.number_input(
        "Min organic views for scoring",
        min_value=10, max_value=500, value=50, step=10,
    )
    min_age_days = st.number_input(
        "Min video age (days)",
        min_value=1, max_value=90, value=14, step=1,
    )
    sat_ratio = st.slider(
        "Saturation threshold (% promoted views)",
        min_value=40, max_value=90, value=65, step=5,
    )

    st.divider()
    st.header("Filters")

# ── Load data ──────────────────────────────────────────────────────────────────

real_features = _build_real_features(_DB, cpv, channel=_active_channel)
if real_features:
    all_features = real_features
    _data_mode = "real"
else:
    all_features = _build_demo_features(cpv)
    _data_mode = "demo"
    st.info(
        "No analytics data found in the database — showing demo data. "
        "Run a fetch via GitHub Actions to populate real metrics.",
        icon="ℹ️",
    )

# Sidebar filters (applied after scoring so scores are consistent)
with st.sidebar:
    all_topics = sorted({f.topic for f in all_features if f.topic})
    all_series = sorted({f.series for f in all_features if f.series})
    all_langs = sorted({f.language for f in all_features})

    sel_topics = st.multiselect("Topic", all_topics, default=all_topics)
    sel_series = st.multiselect("Series", all_series, default=all_series)
    sel_langs = st.multiselect("Language", all_langs, default=all_langs)
    min_score = st.slider("Min Promotion Score", 0, 100, 0, step=5)

# ── Score all videos ──────────────────────────────────────────────────────────

engine = RecommendationEngine(
    all_features,
    min_organic_views=min_organic_views,
    min_age_days=min_age_days,
    saturation_promo_ratio=float(sat_ratio),
)
all_opps: list[PromotionOpportunity] = engine.rank_all()

# Apply sidebar filters
def _passes_filters(o: PromotionOpportunity) -> bool:
    f = o.features
    if sel_topics and f.topic not in sel_topics:
        return False
    if sel_series and f.series and f.series not in sel_series:
        return False
    if sel_langs and f.language not in sel_langs:
        return False
    if o.score < min_score:
        return False
    return True

filtered_opps = [o for o in all_opps if _passes_filters(o)]

# ── Summary metrics ───────────────────────────────────────────────────────────

counts: dict[PromotionClass, int] = {c: 0 for c in PromotionClass}
for o in filtered_opps:
    counts[o.classification] += 1

sm1, sm2, sm3, sm4, sm5, sm6 = st.columns(6)
sm1.metric("Videos Analyzed", len(filtered_opps))
sm2.metric("🚀 Promote Now", counts[PromotionClass.promote_immediately])
sm3.metric("👀 Watch Organically", counts[PromotionClass.watch_organically])
sm4.metric("🚫 Do Not Promote", counts[PromotionClass.do_not_promote])
sm5.metric("⏳ Needs Data", counts[PromotionClass.needs_more_data])
sm6.metric("📊 Already Saturated", counts[PromotionClass.already_saturated])

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab_cards, tab_all, tab_roi, tab_viz, tab_explain = st.tabs([
    "🏆 Recommendation Cards",
    "📋 All Videos",
    "💰 ROI Calculator",
    "📊 Visualizations",
    "🔍 Explainability",
])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Recommendation Cards
# ═══════════════════════════════════════════════════════════════════════════════

with tab_cards:
    cards = engine.get_cards(all_opps)

    st.subheader("Top 10 Videos To Promote")
    if cards.top_10_to_promote:
        for o in cards.top_10_to_promote:
            _opp_card(o)
    else:
        st.info("No videos currently classified as 'Promote Immediately'.", icon="ℹ️")

    st.divider()
    st.subheader("Top 10 Videos To Stop Promoting")
    if cards.top_10_to_stop:
        for o in cards.top_10_to_stop:
            _opp_card(o)
    else:
        st.info("No over-invested videos identified.", icon="ℹ️")

    st.divider()
    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("Most Efficient Promotion")
        if cards.most_efficient:
            _opp_card(cards.most_efficient)
        else:
            st.caption("No promotion history available.")

        st.subheader("Highest Organic Multiplier")
        if cards.highest_organic_multiplier:
            o = cards.highest_organic_multiplier
            st.markdown(
                f"**{o.title[:50]}**  \n"
                f"Organic multiplier: **{o.features.organic_multiplier:.1f}×**  \n"
                f"Score: {o.score:.0f}/100  ·  {o.classification.value}"
            )
        else:
            st.caption("No promotion history to compute multiplier.")

        st.subheader("Highest Subscriber Generator")
        if cards.highest_subscriber_generator:
            o = cards.highest_subscriber_generator
            st.markdown(
                f"**{o.title[:50]}**  \n"
                f"Subscribers: **{o.features.subscribers_gained:,}**  \n"
                f"Conv. rate: {o.features.subscriber_conversion_per_1k:.1f} / 1K views"
            )

    with col_r:
        st.subheader("Least Efficient Promotion")
        if cards.least_efficient:
            _opp_card(cards.least_efficient)
        else:
            st.caption("No promotion history available.")

        st.subheader("Highest Qualifying Hour Generator")
        if cards.highest_qualifying_hour_generator:
            o = cards.highest_qualifying_hour_generator
            st.markdown(
                f"**{o.title[:50]}**  \n"
                f"Qualifying hours: **{o.features.qualifying_hours:.0f} h**  \n"
                f"Score: {o.score:.0f}/100"
            )

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — All Videos
# ═══════════════════════════════════════════════════════════════════════════════

with tab_all:
    st.subheader(f"All Videos ({len(filtered_opps)} shown)")

    rows = []
    for o in filtered_opps:
        f = o.features
        rows.append({
            "Rank": o.rank,
            "Title": o.title,
            "Score": o.score,
            "Classification": o.classification.value,
            "Topic": f.topic,
            "Series": f.series or "—",
            "Retention %": f.audience_retention_pct,
            "Subs / 1K": f.subscriber_conversion_per_1k,
            "Organic Hrs": round(f.organic_watch_hours, 1),
            "Qualifying Hrs": round(f.qualifying_hours, 1),
            "Views/Day": round(f.views_per_day, 1),
            "Promo %": round(f.promotion_ratio_pct, 1),
            "PES": round(f.promotion_efficiency_score, 1),
            "Age (days)": f.video_age_days,
        })

    df_all = pd.DataFrame(rows)
    st.dataframe(
        df_all,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Score": st.column_config.ProgressColumn("Score", min_value=0, max_value=100, format="%.0f"),
            "Retention %": st.column_config.ProgressColumn("Retention %", min_value=0, max_value=100, format="%.0f%%"),
            "PES": st.column_config.ProgressColumn("PES", min_value=0, max_value=100, format="%.0f"),
        },
    )

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — ROI Calculator
# ═══════════════════════════════════════════════════════════════════════════════

with tab_roi:
    st.subheader("ROI Calculator")
    st.caption(
        f"Projections use CPV = ${cpv:.3f}. Override CPV in the sidebar. "
        "Estimates are directional — validate against actual campaign data."
    )

    opp_titles = [f"#{o.rank} — {o.title[:60]}" for o in filtered_opps]
    if not opp_titles:
        st.info("No videos match current filters.", icon="ℹ️")
    else:
        sel_idx = st.selectbox(
            "Select a video",
            range(len(opp_titles)),
            format_func=lambda i: opp_titles[i],
            key="roi_video_sel",
        )
        sel_opp = filtered_opps[sel_idx]
        feat = sel_opp.features

        st.markdown(
            f"**{sel_opp.title}**  \n"
            f"{PROMOTION_CLASS_ICON.get(sel_opp.classification, '')} "
            f"{sel_opp.classification.value}  ·  Score: {sel_opp.score:.0f}/100  \n"
            f"> {sel_opp.explanation}"
        )
        st.divider()

        # Predictor for custom budget
        predictor = PromotionPredictor(cpv=cpv)
        calculator = ROICalculator(cpv=cpv)

        st.markdown("#### Budget Scenario Comparison")
        tier_cols = st.columns(len(BUDGET_TIERS))
        for col, budget in zip(tier_cols, BUDGET_TIERS):
            est = calculator.estimate_roi(sel_opp, budget)
            with col:
                conf_icon = {"high": "🟢", "medium": "🟡", "low": "🔴"}.get(est.confidence, "⚪")
                st.markdown(f"**${budget:.0f}** {conf_icon}")
                st.metric("Views", f"{est.estimated_views:,}")
                st.metric("Subs", f"{est.estimated_subscribers:,}")
                st.metric("Organic Lift", f"+{est.estimated_organic_lift:,}")
                st.metric("Follow-on", f"{est.estimated_follow_on_views:,}")
                st.metric("Qual. Hrs", f"{est.estimated_qualifying_hours:.1f} h")
                st.metric("Exp. PES", f"{est.expected_promotion_efficiency:.0f}/100")
                st.metric("$/Qual Hr", f"${est.cost_per_qualified_hour_projected:.2f}")
                st.metric("$/Sub", f"${est.cost_per_subscriber_projected:.2f}")

        st.divider()
        st.markdown("#### Custom Budget")
        custom_budget = st.number_input(
            "Budget ($)", min_value=1.0, max_value=10_000.0, value=25.0, step=5.0,
            key="roi_custom_budget",
        )
        custom_est = calculator.estimate_roi(sel_opp, custom_budget)
        _roi_card(custom_est)

        narr = predictor.explain_prediction(
            custom_budget, sel_opp,
            custom_est.estimated_views,
            custom_est.estimated_qualifying_hours,
        )
        st.caption(narr)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — Visualizations
# ═══════════════════════════════════════════════════════════════════════════════

with tab_viz:
    if len(filtered_opps) < 2:
        st.info("Need at least 2 videos to render charts.", icon="ℹ️")
    else:
        viz_df = pd.DataFrame([
            {
                "Title": o.title[:40] + "…" if len(o.title) > 40 else o.title,
                "Score": o.score,
                "Classification": o.classification.value,
                "Retention %": o.features.audience_retention_pct,
                "PES": o.features.promotion_efficiency_score,
                "Organic Watch Hours": o.features.organic_watch_hours,
                "Qualifying Hours": o.features.qualifying_hours,
                "Promo Cost ($)": o.features.promotion_cost_estimated,
                "Subscribers Gained": o.features.subscribers_gained,
                "Follow-on Views": max(o.features.follow_on_views, 1),
                "Views Per Day": o.features.views_per_day,
                "Topic": o.features.topic,
            }
            for o in filtered_opps
        ])

        color_map = {cls.value: color for cls, color in PROMOTION_CLASS_COLOR.items()}

        # ── Scatter: Promotion Cost vs Qualifying Hours ───────────────────
        st.subheader("Scatter — Promotion Cost vs Qualifying Hours")
        st.caption("Size = Views Per Day.  Higher right = more qualifying hours per dollar spent.  Hover for title.")
        fig_scatter = px.scatter(
            viz_df,
            x="Promo Cost ($)",
            y="Qualifying Hours",
            color="Classification",
            size="Views Per Day",
            hover_name="Title",
            hover_data=["Score", "Retention %", "PES"],
            color_discrete_map=color_map,
            size_max=40,
        )
        fig_scatter.update_layout(height=480, legend={"orientation": "h", "y": -0.25})
        st.plotly_chart(fig_scatter, use_container_width=True)

        # ── Heatmap: Retention vs Promotion Efficiency ────────────────────
        st.subheader("Heatmap — Audience Retention vs Promotion Efficiency")
        st.caption(
            "Cell value = average Promotion Opportunity Score for videos in that bucket. "
            "Green = stronger promotion candidates."
        )
        ret_bins = [0, 20, 40, 60, 80, 100]
        pes_bins = [0, 20, 40, 60, 80, 100]
        ret_labels = ["0-20%", "20-40%", "40-60%", "60-80%", "80-100%"]
        pes_labels = ["PES 0-20", "PES 20-40", "PES 40-60", "PES 60-80", "PES 80-100"]

        viz_df["ret_bin"] = pd.cut(viz_df["Retention %"], bins=ret_bins, labels=ret_labels, include_lowest=True)
        viz_df["pes_bin"] = pd.cut(viz_df["PES"], bins=pes_bins, labels=pes_labels, include_lowest=True)

        heat_df = (
            viz_df.groupby(["pes_bin", "ret_bin"], observed=True)["Score"]
            .mean()
            .reset_index()
            .pivot(index="pes_bin", columns="ret_bin", values="Score")
            .reindex(index=pes_labels, columns=ret_labels)
            .fillna(0)
        )

        fig_heat = px.imshow(
            heat_df,
            color_continuous_scale="RdYlGn",
            zmin=0, zmax=100,
            text_auto=".0f",
            aspect="auto",
            labels={"x": "Audience Retention", "y": "Promotion Efficiency Score", "color": "Avg Score"},
        )
        fig_heat.update_layout(height=380)
        st.plotly_chart(fig_heat, use_container_width=True)

        # ── Bubble: Subscribers × Qualifying Hours (size = Follow-on Views) ──
        st.subheader("Bubble — Subscribers × Qualifying Hours")
        st.caption(
            "Bubble size = Follow-on Views.  Color = classification.  "
            "Ideal promotions are upper-right with large bubbles.  Hover for title."
        )
        # Label only the top 5 by score so the chart stays readable
        top5_titles = set(
            viz_df.nlargest(5, "Score")["Title"].tolist()
        )
        viz_df["Label"] = viz_df["Title"].where(viz_df["Title"].isin(top5_titles), "")
        fig_bubble = px.scatter(
            viz_df,
            x="Subscribers Gained",
            y="Qualifying Hours",
            size="Follow-on Views",
            color="Classification",
            color_discrete_map=color_map,
            hover_name="Title",
            hover_data=["Score", "Subscribers Gained", "Qualifying Hours", "PES"],
            size_max=50,
            text="Label",
        )
        fig_bubble.update_traces(textposition="top center", textfont_size=9)
        fig_bubble.update_layout(
            height=520,
            legend={"orientation": "h", "y": -0.2},
        )
        st.plotly_chart(fig_bubble, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5 — Explainability
# ═══════════════════════════════════════════════════════════════════════════════

with tab_explain:
    st.subheader("Recommendation Explainability")
    st.caption(
        "Each recommendation is derived from six measurable factors. "
        "Expand any video to see the full scoring breakdown and reasoning."
    )

    _cls_filter = st.selectbox(
        "Filter by classification",
        ["All"] + [c.value for c in PromotionClass],
        key="explain_cls_filter",
    )

    explain_opps = filtered_opps
    if _cls_filter != "All":
        explain_opps = [o for o in filtered_opps if o.classification.value == _cls_filter]

    if not explain_opps:
        st.info("No videos match the selected filter.", icon="ℹ️")
    else:
        for o in explain_opps:
            _opp_card(o, expanded=False)
