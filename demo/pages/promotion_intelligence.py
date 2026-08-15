"""Promotion-intelligence report for the deterministic public demo."""

from __future__ import annotations

import dataclasses
from datetime import timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

from analytics.promotion_efficiency import compute_efficiency_scores
from demo.analytics import aggregate_video_window, filter_promotion_opportunities
from demo.config import DEMO_AS_OF, DEMO_CHANNEL_KEY, DEMO_DB_PATH
from demo.report_data import require_frame
from demo.ui import render_demo_notice, render_demo_sidebar, render_empty_state
from models.promotion import VideoPromotionMetrics, make_metrics
from promotion_intelligence.promotion_prediction import PromotionPredictor
from promotion_intelligence.promotion_roi import (
    BUDGET_TIERS,
    ROICalculator,
    format_projected_cost,
)
from promotion_intelligence.recommendation_engine import RecommendationEngine
from promotion_intelligence.recommendation_models import (
    PROMOTION_CLASS_COLOR,
    PROMOTION_CLASS_ICON,
    PromotionClass,
    PromotionOpportunity,
    ROIEstimate,
    VideoFeatures,
)


_TOPIC_KEYWORDS = {
    "Agents": ("agent", "assistant", "tool-calling", "workflow", "state machine"),
    "Evaluation": ("eval", "regression", "reviewer", "rubric", "scoring"),
    "Retrieval": ("retrieval", "search", "chunk", "rerank", "index"),
    "Observability": ("observability", "trace", "metric", "quality drop", "signal"),
    "Security": (
        "secure",
        "security",
        "secret",
        "threat",
        "poison",
        "privilege",
        "audit",
    ),
    "Deployment": ("deploy", "ship", "production", "latency", "downtime", "release"),
}


def _detect_topic(title: str) -> str:
    lower = title.lower()
    for topic, keywords in _TOPIC_KEYWORDS.items():
        if any(keyword in lower for keyword in keywords):
            return topic
    return "General"


def _to_features(
    metric: VideoPromotionMetrics,
    *,
    cpv: float,
    topic: str,
    ci_score: float,
) -> VideoFeatures:
    age_days = (
        max((DEMO_AS_OF - metric.published.date()).days, 1) if metric.published else 365
    )
    length = max(metric.length_seconds, 1)
    return VideoFeatures(
        video_id=metric.video_id,
        title=metric.title,
        total_views=metric.total_views,
        organic_views=metric.organic_views,
        promotion_views=metric.promotion_views,
        subscribers_gained=metric.subscribers,
        follow_on_views=metric.follow_on_views,
        likes=0,
        total_watch_hours=metric.total_watch_hours,
        organic_watch_hours=metric.organic_watch_hours,
        qualifying_hours=metric.estimated_qualifying_hours,
        avg_view_duration_seconds=metric.avg_view_duration_seconds,
        avg_promotion_view_duration_seconds=metric.avg_promotion_view_duration_seconds,
        audience_retention_pct=round(
            min(metric.avg_view_duration_seconds / length * 100.0, 100.0), 1
        ),
        subscriber_conversion_per_1k=round(
            metric.subscribers / max(metric.organic_views, 1) * 1000.0, 2
        ),
        views_per_day=round(metric.total_views / age_days, 1),
        follow_on_rate_pct=round(
            metric.follow_on_views / max(metric.total_views, 1) * 100.0, 1
        ),
        promotion_ratio_pct=round(metric.promotion_percentage, 1),
        organic_multiplier=round(
            metric.organic_views / max(metric.promotion_views, 1), 2
        )
        if metric.promotion_views
        else 0.0,
        promotion_efficiency_score=metric.promotion_efficiency_score,
        ci_overall_score=ci_score,
        cpv=cpv,
        promotion_cost_estimated=metric.promotion_cost,
        cost_per_qualified_hour=metric.cost_per_qualified_hour,
        cost_per_subscriber=metric.cost_per_subscriber,
        cost_per_follow_on_view=metric.cost_per_follow_on_view,
        video_age_days=age_days,
        length_seconds=length,
        topic=topic,
        data_source="API_ACTUAL" if metric.promotion_views else "NONE",
        has_sufficient_data=metric.organic_views >= 50 and age_days >= 14,
    )


@st.cache_data(ttl=300)
def _load_features(cpv: float) -> list[VideoFeatures]:
    """Aggregate the explicit trailing-year window before running core engines."""
    start = DEMO_AS_OF - timedelta(days=364)
    aggregates = {
        str(row["video_id"]): row
        for row in aggregate_video_window(
            DEMO_DB_PATH, start=start, end=DEMO_AS_OF, channel=DEMO_CHANNEL_KEY
        )
    }
    videos = require_frame(
        "SELECT video_id, title, published_at, duration_seconds FROM videos "
        "WHERE channel=:channel AND date(published_at)<=:as_of ORDER BY published_at",
        {"channel": DEMO_CHANNEL_KEY, "as_of": DEMO_AS_OF.isoformat()},
    )
    ci_scores = require_frame(
        "SELECT video_id, overall_score FROM ci_video_scores WHERE channel=:channel "
        "AND scored_at=(SELECT MAX(scored_at) FROM ci_video_scores "
        "WHERE channel=:channel AND scored_at<=:as_of)",
        {"channel": DEMO_CHANNEL_KEY, "as_of": DEMO_AS_OF.isoformat()},
    )
    ci_map = dict(
        zip(ci_scores.get("video_id", []), ci_scores.get("overall_score", []))
    )

    raw: list[VideoPromotionMetrics] = []
    topics: dict[str, str] = {}
    for video in videos.to_dict("records"):
        video_id = str(video["video_id"])
        aggregate = aggregates.get(video_id)
        if not aggregate:
            continue
        sources = aggregate.get("traffic_sources", {})
        advertising = sources.get("ADVERTISING", {})
        advertising_views = int(advertising.get("views", 0))
        advertising_minutes = float(advertising.get("estimated_minutes_watched", 0.0))
        advertising_duration = float(advertising.get("average_view_duration", 0.0))
        total_hours = float(aggregate["estimated_minutes_watched"]) / 60.0
        related_views = int(sources.get("RELATED_VIDEO", {}).get("views", 0))
        published = pd.to_datetime(video.get("published_at"), errors="coerce")
        published_dt = (
            None
            if pd.isna(published)
            else published.to_pydatetime().replace(tzinfo=None)
        )
        metric = make_metrics(
            video_id=video_id,
            title=str(video.get("title") or video_id),
            published=published_dt,
            length_seconds=int(video.get("duration_seconds") or 0),
            total_views=int(aggregate["views"]),
            promotion_views=advertising_views,
            total_watch_hours=total_hours,
            avg_promotion_view_duration_seconds=advertising_duration,
            promotion_cost=advertising_views * cpv,
            subscribers=int(aggregate["subscribers_gained"]),
            follow_on_views=related_views,
            avg_view_duration_seconds=float(aggregate["average_view_duration"]),
            data_source="API_ACTUAL" if advertising_views else "NONE",
        )
        if advertising_views:
            promotion_hours = advertising_minutes / 60.0
            organic_hours = max(total_hours - promotion_hours, 0.0)
            qualifying_hours = (
                0.0 if 0 < metric.length_seconds <= 180 else organic_hours
            )
            metric = dataclasses.replace(
                metric,
                promotion_watch_hours=promotion_hours,
                organic_watch_hours=organic_hours,
                estimated_qualifying_hours=qualifying_hours,
                cost_per_qualified_hour=(
                    metric.promotion_cost / qualifying_hours
                    if qualifying_hours
                    else 0.0
                ),
            )
        raw.append(metric)
        topics[video_id] = _detect_topic(metric.title)

    scored = compute_efficiency_scores(raw)
    return [
        _to_features(
            metric,
            cpv=cpv,
            topic=topics[metric.video_id],
            ci_score=float(ci_map.get(metric.video_id, 0.0)),
        )
        for metric in scored
    ]


def _opportunity_card(
    opportunity: PromotionOpportunity, *, expanded: bool = False
) -> None:
    feature = opportunity.features
    icon = PROMOTION_CLASS_ICON.get(opportunity.classification, "")
    with st.expander(
        f"#{opportunity.rank} {icon} {opportunity.title} — {opportunity.score:.0f}/100",
        expanded=expanded,
    ):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Promotion Score", f"{opportunity.score:.0f}/100")
        c2.metric("Retention", f"{feature.audience_retention_pct:.0f}%")
        c3.metric("Qualifying Hours", f"{feature.qualifying_hours:,.0f}")
        c4.metric("Efficiency", f"{feature.promotion_efficiency_score:.0f}/100")
        st.info(opportunity.explanation, icon="💡")
        breakdown = opportunity.breakdown
        st.dataframe(
            pd.DataFrame(
                {
                    "Component": [
                        "Retention",
                        "Subscriber conversion",
                        "Organic hours",
                        "Views per day",
                        "Follow-on rate",
                        "Promotion efficiency",
                    ],
                    "Points": [
                        breakdown.retention,
                        breakdown.subscriber_conversion,
                        breakdown.organic_hours,
                        breakdown.views_per_day,
                        breakdown.follow_on_rate,
                        breakdown.promotion_efficiency,
                    ],
                    "Maximum": [25, 20, 20, 15, 10, 10],
                }
            ),
            width="stretch",
            hide_index=True,
        )


def _roi_card(estimate: ROIEstimate) -> None:
    icon = {"high": "🟢", "medium": "🟡", "low": "🔴"}.get(estimate.confidence, "⚪")
    st.markdown(f"#### ${estimate.budget:.0f} scenario {icon}")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Simulated Views", f"{estimate.estimated_views:,}")
    c2.metric("Simulated Subscribers", f"{estimate.estimated_subscribers:,}")
    c3.metric(
        "Eligible Organic-Lift Hours", f"{estimate.estimated_qualifying_hours:.1f}"
    )
    c4.metric(
        "Cost / Eligible Hour",
        format_projected_cost(estimate.cost_per_qualified_hour_projected),
    )
    st.caption(estimate.confidence_reason)


render_demo_sidebar("Promotion Intelligence")
render_demo_notice()
st.title("📣 Promotion Intelligence")
st.caption(
    f"Uses daily increments aggregated across the trailing year through {DEMO_AS_OF:%B %d, %Y}, "
    "then applies the production promotion-efficiency and recommendation engines."
)

with st.sidebar:
    st.header("Promotion Configuration")
    cpv = st.number_input(
        "Cost per view ($)",
        min_value=0.005,
        max_value=0.20,
        value=0.025,
        step=0.005,
        format="%.3f",
    )
    minimum_organic = st.number_input(
        "Minimum organic views", min_value=10, max_value=500, value=50, step=10
    )
    minimum_age = st.number_input(
        "Minimum video age (days)", min_value=1, max_value=90, value=14
    )
    saturation = st.slider("Saturation threshold (% promoted views)", 40, 90, 65, 5)

features = _load_features(cpv)
if not features:
    render_empty_state("promotion intelligence")
    st.stop()

with st.sidebar:
    topics = sorted({feature.topic for feature in features})
    selected_topics = st.multiselect("Topic", topics, default=topics)
    minimum_score = st.slider("Minimum promotion score", 0, 100, 0, 5)

engine = RecommendationEngine(
    features,
    min_organic_views=minimum_organic,
    min_age_days=minimum_age,
    saturation_promo_ratio=float(saturation),
)
opportunities = engine.rank_all()
filtered = filter_promotion_opportunities(
    opportunities,
    topics=set(selected_topics),
    minimum_score=minimum_score,
)

counts = {classification: 0 for classification in PromotionClass}
for item in filtered:
    counts[item.classification] += 1
kpis = st.columns(6)
kpis[0].metric("Videos Analyzed", len(filtered))
for column, classification in zip(kpis[1:], PromotionClass):
    column.metric(
        f"{PROMOTION_CLASS_ICON.get(classification, '')} {classification.value}",
        counts[classification],
    )

tab_cards, tab_all, tab_roi, tab_visuals, tab_explain = st.tabs(
    [
        "🏆 Recommendation Cards",
        "📋 All Videos",
        "💰 ROI Calculator",
        "📊 Visualizations",
        "🔍 Explainability",
    ]
)

with tab_cards:
    cards = engine.get_cards(filtered)
    st.subheader("Top Videos to Promote")
    if cards.top_10_to_promote:
        for item in cards.top_10_to_promote:
            _opportunity_card(item)
    else:
        st.info("No videos currently meet the Promote Immediately threshold.")
    st.subheader("Videos to Stop Promoting")
    if cards.top_10_to_stop:
        for item in cards.top_10_to_stop:
            _opportunity_card(item)
    else:
        st.info("No over-invested videos were identified.")
    specialist_a, specialist_b = st.columns(2)
    with specialist_a:
        st.markdown("**Most Efficient Promotion**")
        if cards.most_efficient:
            _opportunity_card(cards.most_efficient)
        st.markdown("**Highest Organic Multiplier**")
        if cards.highest_organic_multiplier:
            _opportunity_card(cards.highest_organic_multiplier)
    with specialist_b:
        st.markdown("**Least Efficient Promotion**")
        if cards.least_efficient:
            _opportunity_card(cards.least_efficient)
        st.markdown("**Highest Subscriber Generator**")
        if cards.highest_subscriber_generator:
            _opportunity_card(cards.highest_subscriber_generator)
        st.markdown("**Highest Qualifying-Hour Generator**")
        if cards.highest_qualifying_hour_generator:
            _opportunity_card(cards.highest_qualifying_hour_generator)

table_rows = [
    {
        "Rank": item.rank,
        "Title": item.title,
        "Score": item.score,
        "Classification": item.classification.value,
        "Topic": item.features.topic,
        "Retention %": item.features.audience_retention_pct,
        "Organic Views": item.features.organic_views,
        "Promotion Views": item.features.promotion_views,
        "Subscribers Gained": item.features.subscribers_gained,
        "Follow-on Views": item.features.follow_on_views,
        "Qualifying Hours": round(item.features.qualifying_hours, 1),
        "Views/Day": item.features.views_per_day,
        "Promotion Efficiency": item.features.promotion_efficiency_score,
    }
    for item in filtered
]
table = pd.DataFrame(table_rows)

with tab_all:
    st.subheader(f"Complete Scored Library ({len(table)} videos)")
    st.dataframe(table, width="stretch", hide_index=True)

with tab_roi:
    st.subheader("ROI Calculator")
    st.caption(
        "Financial figures are simulated planning scenarios, not forecasts or guarantees. "
        f"Scenarios use a configurable ${cpv:.3f} cost per view. Eligible hours model "
        "only projected long-form organic lift; paid watch time and Shorts are excluded."
    )
    if filtered:
        selected_index = st.selectbox(
            "Select a video",
            range(len(filtered)),
            format_func=lambda index: (
                f"#{filtered[index].rank} — {filtered[index].title}"
            ),
        )
        selected = filtered[selected_index]
        calculator = ROICalculator(cpv=cpv)
        scenario_columns = st.columns(len(BUDGET_TIERS))
        for column, budget in zip(scenario_columns, BUDGET_TIERS):
            with column:
                _roi_card(calculator.estimate_roi(selected, budget))
        custom_budget = st.number_input(
            "Custom planning budget ($)", 1.0, 10_000.0, 25.0, 5.0
        )
        estimate = calculator.estimate_roi(selected, custom_budget)
        _roi_card(estimate)
        narrative = PromotionPredictor(cpv=cpv).explain_prediction(
            custom_budget,
            selected,
            estimate.estimated_views,
            estimate.estimated_qualifying_hours,
        )
        st.caption(narrative)
    else:
        st.info("No videos match the current filters.")

with tab_visuals:
    if len(table) < 2:
        st.info("At least two matching videos are needed for comparisons.")
    else:
        color_map = {item.value: color for item, color in PROMOTION_CLASS_COLOR.items()}
        st.plotly_chart(
            px.scatter(
                table,
                x="Promotion Views",
                y="Qualifying Hours",
                size="Views/Day",
                color="Classification",
                hover_name="Title",
                color_discrete_map=color_map,
                title="Promotion Volume vs Qualifying Hours",
            ),
            width="stretch",
        )
        heatmap = table.copy()
        heatmap["Retention Band"] = pd.cut(
            heatmap["Retention %"],
            bins=[0, 20, 40, 60, 80, 100],
            labels=["0–20%", "20–40%", "40–60%", "60–80%", "80–100%"],
            include_lowest=True,
        )
        heatmap["Efficiency Band"] = pd.cut(
            heatmap["Promotion Efficiency"],
            bins=[0, 20, 40, 60, 80, 100],
            labels=["0–20", "20–40", "40–60", "60–80", "80–100"],
            include_lowest=True,
        )
        heat_values = heatmap.pivot_table(
            index="Efficiency Band",
            columns="Retention Band",
            values="Score",
            aggfunc="mean",
            observed=False,
        ).fillna(0)
        st.plotly_chart(
            px.imshow(
                heat_values,
                color_continuous_scale="RdYlGn",
                zmin=0,
                zmax=100,
                text_auto=".0f",
                aspect="auto",
                title="Average Promotion Score by Retention and Efficiency",
            ),
            width="stretch",
        )
        bubble = table.copy()
        bubble["Bubble Size"] = bubble["Follow-on Views"].clip(lower=1)
        st.plotly_chart(
            px.scatter(
                bubble,
                x="Subscribers Gained",
                y="Qualifying Hours",
                size="Bubble Size",
                color="Classification",
                hover_name="Title",
                color_discrete_map=color_map,
                title="Subscribers and Qualifying Hours (size = Follow-on Views)",
            ),
            width="stretch",
        )
        st.plotly_chart(
            px.scatter(
                table,
                x="Retention %",
                y="Promotion Efficiency",
                size="Organic Views",
                color="Topic",
                hover_name="Title",
                title="Retention and Promotion Efficiency",
            ),
            width="stretch",
        )

with tab_explain:
    st.subheader("Recommendation Explainability")
    classification = st.selectbox(
        "Classification", ["All"] + [item.value for item in PromotionClass]
    )
    explain = (
        filtered
        if classification == "All"
        else [item for item in filtered if item.classification.value == classification]
    )
    for item in explain:
        _opportunity_card(item)
