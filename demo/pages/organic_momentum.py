"""Organic-momentum report for the deterministic public demo."""

from __future__ import annotations

import json
from datetime import timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

from analytics.organic_momentum_scoring import MomentumScorer, calculate_growth_rate
from demo.analytics import (
    aggregate_video_window,
    eligible_organic_watch_hours,
    organic_window_totals,
)
from demo.config import DEMO_AS_OF, DEMO_CHANNEL_KEY, DEMO_DB_PATH
from demo.report_data import require_frame
from demo.ui import render_demo_notice, render_demo_sidebar, render_empty_state
from models.organic_momentum import (
    MOMENTUM_CLASS_COLOR,
    MOMENTUM_CLASS_ICON,
    MomentumClass,
    OrganicMomentumMetrics,
    PromotionStatus,
    ScoreBreakdown,
    ScoreWeights,
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


def _window_map(start, end) -> dict[str, dict[str, object]]:
    return {
        str(row["video_id"]): row
        for row in aggregate_video_window(
            DEMO_DB_PATH,
            start=start,
            end=end,
            channel=DEMO_CHANNEL_KEY,
            include_observed_days=True,
        )
    }


@st.cache_data(ttl=300)
def _load_metrics(weights_json: str) -> list[OrganicMomentumMetrics]:
    """Aggregate matching windows of daily increments, then use the core scorer."""
    library_start = DEMO_AS_OF - timedelta(days=364)
    recent_start = DEMO_AS_OF - timedelta(days=29)
    baseline_end = recent_start - timedelta(days=1)
    baseline_start = baseline_end - timedelta(days=29)

    library = _window_map(library_start, DEMO_AS_OF)
    recent = _window_map(recent_start, DEMO_AS_OF)
    baseline = _window_map(baseline_start, baseline_end)
    videos = require_frame(
        "SELECT video_id, title, published_at, duration_seconds FROM videos "
        "WHERE channel=:channel AND date(published_at)<=:as_of ORDER BY published_at",
        {"channel": DEMO_CHANNEL_KEY, "as_of": DEMO_AS_OF.isoformat()},
    )

    metrics: list[OrganicMomentumMetrics] = []
    for row in videos.to_dict("records"):
        video_id = str(row["video_id"])
        total = library.get(video_id)
        if not total:
            continue
        recent_row = recent.get(video_id, {})
        baseline_row = baseline.get(video_id, {})
        sources = total.get("traffic_sources", {})
        recent_sources = recent_row.get("traffic_sources", {})
        advertising = sources.get("ADVERTISING", {})
        recent_advertising = recent_sources.get("ADVERTISING", {})

        length = int(row.get("duration_seconds") or 0)
        total_views = int(total["views"])
        total_hours = float(total["estimated_minutes_watched"]) / 60.0
        advertising_views = int(advertising.get("views", 0))
        advertising_hours = (
            float(advertising.get("estimated_minutes_watched", 0.0)) / 60.0
        )
        organic_total = organic_window_totals(total)
        organic_recent = organic_window_totals(recent_row)
        organic_baseline = organic_window_totals(baseline_row)
        organic_views = int(organic_total["views"])
        qualifying_hours = eligible_organic_watch_hours(
            length,
            total_hours,
            advertising_hours,
        )
        recent_days = max(int(recent_row.get("observed_days", 0)), 1)
        baseline_days = max(int(baseline_row.get("observed_days", 0)), 1)
        recent_daily_views = float(organic_recent["views"]) / recent_days
        baseline_daily_views = float(organic_baseline["views"]) / baseline_days
        recent_daily_hours = float(organic_recent["watch_hours"]) / recent_days
        baseline_daily_hours = float(organic_baseline["watch_hours"]) / baseline_days
        view_growth = calculate_growth_rate(recent_daily_views, baseline_daily_views)
        hour_growth = calculate_growth_rate(recent_daily_hours, baseline_daily_hours)
        recent_organic = int(organic_recent["views"])
        recent_organic_hours = eligible_organic_watch_hours(
            length,
            float(recent_row.get("estimated_minutes_watched", 0.0)) / 60.0,
            float(recent_advertising.get("estimated_minutes_watched", 0.0)) / 60.0,
        )
        average_duration = float(total["average_view_duration"])
        average_percentage = (
            min(average_duration / length * 100.0, 100.0) if length else 0.0
        )
        related_views = int(sources.get("RELATED_VIDEO", {}).get("views", 0))

        metrics.append(
            OrganicMomentumMetrics(
                video_id=video_id,
                title=str(row.get("title") or video_id),
                published_date=str(row.get("published_at") or "")[:10],
                video_length_seconds=length,
                promotion_status=(
                    PromotionStatus.promoted
                    if advertising_views
                    else PromotionStatus.not_promoted
                ),
                promotion_start_date=None,
                promotion_end_date=None,
                promotion_cost=advertising_views * 0.025,
                total_views=total_views,
                organic_views=organic_views,
                promotion_views=advertising_views,
                post_promotion_organic_views=recent_organic,
                total_watch_hours=round(total_hours, 2),
                estimated_qualifying_watch_hours=round(qualifying_hours, 2),
                post_promotion_organic_watch_hours=round(recent_organic_hours, 2),
                average_view_duration_seconds=average_duration,
                average_percentage_viewed=round(average_percentage, 1),
                ctr=0.0,
                impressions=0,
                engaged_views=0,
                returning_viewers=0,
                subscribers=int(total["subscribers_gained"]),
                follow_on_views=related_views,
                browse_views=int(sources.get("BROWSE_FEATURES", {}).get("views", 0)),
                suggested_views=related_views,
                search_views=int(sources.get("YT_SEARCH", {}).get("views", 0)),
                organic_lift=(
                    organic_views / advertising_views if advertising_views else 0.0
                ),
                organic_watch_hour_lift=(
                    qualifying_hours / advertising_hours if advertising_hours else 0.0
                ),
                organic_momentum_per_dollar=(
                    recent_organic_hours / (advertising_views * 0.025)
                    if advertising_views
                    else 0.0
                ),
                view_growth_rate=max(-1.0, min(5.0, view_growth)),
                wh_growth_rate=max(-1.0, min(5.0, hour_growth)),
                recent_daily_views=recent_daily_views,
                peak_daily_views=max(recent_daily_views, baseline_daily_views),
                data_points=int(total.get("observed_days", 0)),
                organic_momentum_score=0.0,
                score_breakdown=ScoreBreakdown(),
                classification=MomentumClass.insufficient_data,
                recommended_action="",
                data_quality_flag="simulated daily increments",
            )
        )

    return MomentumScorer(ScoreWeights(**json.loads(weights_json))).score_all(metrics)


render_demo_sidebar("Organic Momentum")
render_demo_notice()
st.title("🌱 Organic Momentum")
st.caption(
    f"Compares two matching 30-day windows and totals daily increments through "
    f"{DEMO_AS_OF:%B %d, %Y}. Scores use the production momentum model."
)

with st.sidebar:
    st.header("Organic Momentum Filters")
    promotion_filter = st.radio(
        "Promotion status", ["All", "Promoted", "Not Promoted"], horizontal=True
    )
    minimum_views = st.number_input(
        "Minimum total views", min_value=0, value=100, step=100
    )
    st.divider()
    with st.expander("Advanced score weights"):
        weight_values = {
            "organic_views_growth": st.slider("View growth trend", 0.0, 0.4, 0.2, 0.01),
            "organic_wh_growth": st.slider(
                "Watch-hour growth trend", 0.0, 0.4, 0.2, 0.01
            ),
            "organic_ratio": st.slider("Organic traffic ratio", 0.0, 0.3, 0.15, 0.01),
            "completion_rate": st.slider("Completion rate", 0.0, 0.2, 0.1, 0.01),
            "avg_pct_viewed": st.slider("Average percent viewed", 0.0, 0.2, 0.1, 0.01),
            "subscriber_conversion": st.slider(
                "Subscriber conversion", 0.0, 0.2, 0.1, 0.01
            ),
            "returning_proxy": st.slider(
                "Returning-viewer proxy", 0.0, 0.15, 0.05, 0.01
            ),
            "follow_on_proxy": st.slider("Follow-on proxy", 0.0, 0.15, 0.05, 0.01),
            "ctr_proxy": st.slider("CTR proxy", 0.0, 0.15, 0.05, 0.01),
        }
        weights_total = sum(weight_values.values())
        if abs(weights_total - 1.0) > 0.01:
            st.warning(
                f"Weights total {weights_total:.2f}; defaults apply until they total 1.00."
            )
            weight_values = ScoreWeights().as_dict()
        else:
            st.caption("Weights total 1.00")

all_metrics = _load_metrics(json.dumps(weight_values, sort_keys=True))
if not all_metrics:
    render_empty_state("organic momentum")
    st.stop()

class_options = [
    item.value for item in MomentumClass if item is not MomentumClass.insufficient_data
]
selected_classes = st.sidebar.multiselect(
    "Classifications", class_options, default=class_options
)
filtered = [metric for metric in all_metrics if metric.total_views >= minimum_views]
if promotion_filter != "All":
    wanted = (
        PromotionStatus.promoted
        if promotion_filter == "Promoted"
        else PromotionStatus.not_promoted
    )
    filtered = [metric for metric in filtered if metric.promotion_status is wanted]
if selected_classes:
    filtered = [
        metric for metric in filtered if metric.classification.value in selected_classes
    ]

if not filtered:
    st.warning("No videos match the current filters.")
    st.stop()

top = filtered[0]
promoted = [m for m in filtered if m.promotion_status is PromotionStatus.promoted]
sleepers = [
    m
    for m in filtered
    if m.promotion_status is PromotionStatus.not_promoted and m.view_growth_rate > 0
]
topic_scores: dict[str, list[float]] = {}
for metric in filtered:
    topic_scores.setdefault(_detect_topic(metric.title), []).append(
        metric.organic_momentum_score
    )
best_topic = max(
    topic_scores, key=lambda key: sum(topic_scores[key]) / len(topic_scores[key])
)

k1, k2, k3, k4 = st.columns(4)
k1.metric("Top Momentum", f"{top.organic_momentum_score:.0f}/100", top.title[:36])
k2.metric("Promoted Videos", len(promoted), "with paid-source history")
k3.metric("Organic Sleepers", len(sleepers), "still growing without ads")
k4.metric("Best Topic Pattern", best_topic)

tab_rank, tab_trends, tab_promo, tab_charts, tab_actions = st.tabs(
    [
        "🏆 Rankings",
        "📈 Trends",
        "📣 Promotion Analysis",
        "📊 Charts",
        "⚡ Recommended Actions",
    ]
)

rows = [
    {
        "Title": m.title,
        "Topic": _detect_topic(m.title),
        "Status": m.promotion_status.value,
        "Views": m.total_views,
        "Organic Views": m.organic_views,
        "Qualifying Hours": round(m.estimated_qualifying_watch_hours, 1),
        "Recent Views/Day": round(m.recent_daily_views, 1),
        "View Growth %": round(m.view_growth_rate * 100, 1),
        "Score": round(m.organic_momentum_score, 1),
        "Classification": m.classification.value,
        "Class": f"{MOMENTUM_CLASS_ICON.get(m.classification, '')} {m.classification.value}",
        "Action": m.recommended_action,
    }
    for m in filtered
]
frame = pd.DataFrame(rows)

with tab_rank:
    st.subheader("Organic Momentum Rankings")
    st.dataframe(frame, width="stretch", hide_index=True)

with tab_trends:
    st.subheader("Matching-Window Growth")
    chart = px.scatter(
        frame,
        x="View Growth %",
        y="Score",
        size="Views",
        color="Topic",
        hover_name="Title",
        title="Recent 30 Days vs Previous 30 Days",
    )
    chart.add_vline(x=0, line_dash="dash")
    st.plotly_chart(chart, width="stretch")

with tab_promo:
    st.subheader("Promotion Analysis")
    if not promoted:
        st.info("No promoted videos match the current filters.")
    else:
        promo_frame = pd.DataFrame(
            {
                "Title": [m.title for m in promoted],
                "Promotion Views": [m.promotion_views for m in promoted],
                "Organic Lift": [m.organic_lift for m in promoted],
                "Organic Watch-Hour Lift": [
                    m.organic_watch_hour_lift for m in promoted
                ],
                "Score": [m.organic_momentum_score for m in promoted],
            }
        )
        st.dataframe(promo_frame, width="stretch", hide_index=True)
        st.plotly_chart(
            px.scatter(
                promo_frame,
                x="Promotion Views",
                y="Organic Lift",
                size="Score",
                hover_name="Title",
            ),
            width="stretch",
        )

with tab_charts:
    st.subheader("Score by Topic")
    topic_frame = frame.groupby("Topic", as_index=False).agg(
        Score=("Score", "mean"), Videos=("Title", "count")
    )
    st.plotly_chart(
        px.bar(
            topic_frame.sort_values("Score"),
            x="Score",
            y="Topic",
            orientation="h",
            color="Score",
        ),
        width="stretch",
    )
    colors = {item.value: color for item, color in MOMENTUM_CLASS_COLOR.items()}
    st.plotly_chart(
        px.bar(
            frame.sort_values("Score"),
            x="Score",
            y="Title",
            orientation="h",
            color="Classification",
            color_discrete_map=colors,
        ),
        width="stretch",
    )

with tab_actions:
    st.subheader("Recommended Actions")
    for metric in filtered:
        with st.expander(
            f"{MOMENTUM_CLASS_ICON.get(metric.classification, '')} {metric.title} — {metric.organic_momentum_score:.0f}/100"
        ):
            st.info(metric.recommended_action, icon="💡")
            breakdown = metric.score_breakdown
            st.dataframe(
                pd.DataFrame(
                    {
                        "Factor": [
                            "Organic views growth",
                            "Organic watch-hour growth",
                            "Organic ratio",
                            "Completion",
                            "Subscriber conversion",
                        ],
                        "Weighted points": [
                            breakdown.organic_views_growth,
                            breakdown.organic_wh_growth,
                            breakdown.organic_ratio,
                            breakdown.completion_rate,
                            breakdown.subscriber_conversion,
                        ],
                    }
                ),
                width="stretch",
                hide_index=True,
            )
