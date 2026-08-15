"""Daily and cumulative performance for the deterministic demo channel."""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from demo.config import DEMO_AS_OF, DEMO_CHANNEL_KEY
from demo.report_data import require_frame
from demo.ui import render_demo_notice, render_demo_sidebar, render_empty_state


render_demo_sidebar("Daily Analytics")
render_demo_notice()
st.header("Daily Analytics")

_PARAMS = {"channel": DEMO_CHANNEL_KEY, "as_of": DEMO_AS_OF.isoformat()}


def _load_daily() -> pd.DataFrame:
    return require_frame(
        "SELECT metric_date, views, estimated_minutes_watched, subscribers_gained, "
        "subscribers_lost FROM daily_channel_metrics WHERE channel=:channel "
        "AND metric_date<=:as_of ORDER BY metric_date",
        _PARAMS,
    )


def _load_video_daily() -> pd.DataFrame:
    return require_frame(
        "SELECT d.metric_date, d.video_id, v.title, d.views, "
        "d.estimated_minutes_watched/60.0 AS watch_hours, d.average_view_duration "
        "FROM daily_video_metrics d LEFT JOIN videos v ON v.channel=d.channel "
        "AND v.video_id=d.video_id WHERE d.channel=:channel "
        "AND d.metric_date<=:as_of ORDER BY d.metric_date",
        _PARAMS,
    )


def _load_daily_adjustments() -> pd.DataFrame:
    return require_frame(
        "WITH advertising AS (SELECT metric_date, SUM(estimated_minutes_watched) minutes "
        "FROM video_traffic_source_metrics WHERE channel=:channel AND metric_date<=:as_of "
        "AND traffic_source_type='ADVERTISING' GROUP BY metric_date), "
        "shorts AS (SELECT d.metric_date, SUM(d.estimated_minutes_watched) minutes "
        "FROM daily_video_metrics d JOIN videos v ON v.channel=d.channel AND v.video_id=d.video_id "
        "WHERE d.channel=:channel AND d.metric_date<=:as_of AND v.duration_seconds>0 "
        "AND v.duration_seconds<=180 GROUP BY d.metric_date), "
        "short_ads AS (SELECT d.metric_date, SUM(d.estimated_minutes_watched) minutes "
        "FROM video_traffic_source_metrics d JOIN videos v ON v.channel=d.channel "
        "AND v.video_id=d.video_id WHERE d.channel=:channel AND d.metric_date<=:as_of "
        "AND d.traffic_source_type='ADVERTISING' AND v.duration_seconds>0 "
        "AND v.duration_seconds<=180 GROUP BY d.metric_date) "
        "SELECT c.metric_date, COALESCE(a.minutes,0) advertising_minutes, "
        "COALESCE(s.minutes,0) shorts_minutes, COALESCE(sa.minutes,0) shorts_ad_minutes "
        "FROM daily_channel_metrics c LEFT JOIN advertising a ON a.metric_date=c.metric_date "
        "LEFT JOIN shorts s ON s.metric_date=c.metric_date "
        "LEFT JOIN short_ads sa ON sa.metric_date=c.metric_date "
        "WHERE c.channel=:channel AND c.metric_date<=:as_of ORDER BY c.metric_date",
        _PARAMS,
    )


daily = _load_daily()
video = _load_video_daily()
adjustments = _load_daily_adjustments()

if daily.empty:
    render_empty_state("daily analytics")
    st.stop()

daily["metric_date"] = pd.to_datetime(daily["metric_date"])
if not adjustments.empty:
    adjustments["metric_date"] = pd.to_datetime(adjustments["metric_date"])
    daily = daily.merge(adjustments, on="metric_date", how="left")
for column in ("advertising_minutes", "shorts_minutes", "shorts_ad_minutes"):
    if column not in daily:
        daily[column] = 0.0
    daily[column] = daily[column].fillna(0.0)
daily["watch_hours"] = daily["estimated_minutes_watched"] / 60.0
daily["promotion_hours"] = daily["advertising_minutes"] / 60.0
daily["organic_hours"] = (daily["watch_hours"] - daily["promotion_hours"]).clip(lower=0)
daily["shorts_organic_hours"] = (
    daily["shorts_minutes"] - daily["shorts_ad_minutes"]
).clip(lower=0) / 60.0
daily["qualifying_hours"] = (
    daily["organic_hours"] - daily["shorts_organic_hours"]
).clip(lower=0)
daily["net_subs"] = daily["subscribers_gained"] - daily["subscribers_lost"]
daily["avg_view_dur_sec"] = (
    daily["estimated_minutes_watched"] * 60.0 / daily["views"].clip(lower=1)
)
daily["year_month"] = daily["metric_date"].dt.to_period("M")
daily["day_of_month"] = daily["metric_date"].dt.day

if not video.empty:
    video["metric_date"] = pd.to_datetime(video["metric_date"])
    video["year_month"] = video["metric_date"].dt.to_period("M")
    video["weighted_dur"] = video["average_view_duration"] * video["views"]

current_month = pd.Period(DEMO_AS_OF, "M")
all_months = sorted(daily["year_month"].unique(), reverse=True)
prior_months = [month for month in all_months if month < current_month]

st.caption(
    "Each daily, monthly, and selected-period qualifying value subtracts promotion "
    "and organic Shorts from that same period through "
    f"{DEMO_AS_OF:%B %d, %Y}."
)

st.subheader("Channel Performance by Day")
control_a, control_b = st.columns([2, 4])
with control_a:
    view_mode = st.radio(
        "Display mode",
        ["Daily", "Cumulative", "Running Average"],
        horizontal=True,
        key="demo_da_mode",
    )
with control_b:
    selected_prior = st.multiselect(
        "Compare prior months",
        [str(month) for month in prior_months],
        default=[],
        key="demo_da_prior",
    )

selected_periods = [current_month] + [pd.Period(value, "M") for value in selected_prior]
compare_mode = len(selected_periods) > 1
colors = ["#4C78A8", "#F58518", "#54A24B", "#E45756", "#B279A2"]


def _build_chart(column: str, label: str) -> go.Figure:
    figure = go.Figure()
    for index, period in enumerate(selected_periods):
        month = daily[daily["year_month"] == period].sort_values("metric_date")
        if month.empty:
            continue
        values = month[column]
        if view_mode == "Cumulative":
            values = values.cumsum()
        elif view_mode == "Running Average":
            values = values.cumsum() / np.arange(1, len(values) + 1)
        x_values = month["day_of_month"] if compare_mode else month["metric_date"]
        common = dict(
            x=x_values,
            y=values,
            name=period.strftime("%b %Y"),
            marker_color=colors[index % len(colors)],
        )
        if view_mode == "Daily" and not compare_mode:
            figure.add_bar(**common)
        else:
            figure.add_scatter(
                x=x_values,
                y=values,
                name=period.strftime("%b %Y"),
                mode="lines+markers",
                line=dict(color=colors[index % len(colors)], width=3),
            )
    figure.update_layout(
        title=label,
        xaxis_title="Day of Month" if compare_mode else "Date",
        yaxis_title=label,
        height=340,
        hovermode="x unified",
    )
    return figure


tab_views, tab_watch, tab_qual = st.tabs(
    ["Views", "Total Watch Hours", "Qualifying Watch Hours"]
)
with tab_views:
    st.plotly_chart(_build_chart("views", "Views"), width="stretch")
with tab_watch:
    st.plotly_chart(_build_chart("watch_hours", "Watch Hours"), width="stretch")
with tab_qual:
    st.plotly_chart(
        _build_chart("qualifying_hours", "Qualifying Watch Hours"),
        width="stretch",
    )

mtd = daily[daily["year_month"] == current_month]
if not mtd.empty:
    st.markdown(f"**Month-to-Date — {current_month.strftime('%B %Y')}**")
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Views", f"{int(mtd['views'].sum()):,}", f"{mtd['views'].mean():,.0f}/day")
    k2.metric("Watch Hours", f"{mtd['watch_hours'].sum():,.1f}", f"{mtd['watch_hours'].mean():,.1f}/day")
    k3.metric("Qualifying Hours", f"{mtd['qualifying_hours'].sum():,.1f}", f"{mtd['qualifying_hours'].mean():,.1f}/day")
    k4.metric("Net Subscribers", f"{int(mtd['net_subs'].sum()):+,}", f"{mtd['net_subs'].mean():+.1f}/day")
    k5.metric("Days with Data", int(mtd["metric_date"].nunique()))

st.divider()
st.subheader("Average Watch Time")
st.caption(
    "Compare view-weighted duration by video or inspect daily watch depth alongside subscriber movement."
)

earliest = all_months[-1]
period_map = {
    "This Month": (current_month, current_month),
    "Last Month": (current_month - 1, current_month - 1),
    "Last 3 Months": (current_month - 2, current_month),
    "Last 6 Months": (current_month - 5, current_month),
    "All Time": (earliest, current_month),
}
period_col, view_col = st.columns(2)
with period_col:
    period_label = st.selectbox("Period", list(period_map), key="demo_da_period")
with view_col:
    duration_view = st.radio(
        "View", ["By Video", "By Day"], horizontal=True, key="demo_da_duration_view"
    )

period_start, period_end = period_map[period_label]
period_daily = daily[
    daily["year_month"].between(period_start, period_end)
].sort_values("metric_date").copy()

if duration_view == "By Video":
    if video.empty:
        render_empty_state("per-video watch-time")
    else:
        period_video = video[video["year_month"].between(period_start, period_end)]
        aggregate = (
            period_video.groupby(["video_id", "title"], dropna=False)
            .agg(
                views=("views", "sum"),
                watch_hours=("watch_hours", "sum"),
                weighted_duration=("weighted_dur", "sum"),
            )
            .reset_index()
        )
        aggregate = aggregate[aggregate["views"] > 0]
        aggregate["avg_minutes"] = (
            aggregate["weighted_duration"] / aggregate["views"] / 60.0
        )
        aggregate["short_title"] = aggregate["title"].fillna(aggregate["video_id"]).map(
            lambda value: str(value)[:58] + ("…" if len(str(value)) > 58 else "")
        )
        aggregate = aggregate.sort_values("avg_minutes", ascending=False)
        chart = px.bar(
            aggregate,
            x="avg_minutes",
            y="short_title",
            orientation="h",
            color="views",
            color_continuous_scale="Blues",
            title=f"Average View Duration by Video — {period_label}",
            labels={"avg_minutes": "Average minutes", "short_title": ""},
        )
        chart.update_yaxes(autorange="reversed")
        chart.update_layout(height=max(380, len(aggregate) * 28))
        st.plotly_chart(chart, width="stretch")
else:
    if period_daily.empty:
        render_empty_state("daily watch-time")
    else:
        period_daily["avg_minutes"] = period_daily["avg_view_dur_sec"] / 60.0
        period_daily["rolling_minutes"] = period_daily["avg_minutes"].rolling(7, min_periods=1).mean()
        daily_chart = go.Figure()
        daily_chart.add_scatter(
            x=period_daily["metric_date"], y=period_daily["avg_minutes"],
            name="Average duration", mode="lines+markers",
        )
        daily_chart.add_scatter(
            x=period_daily["metric_date"], y=period_daily["rolling_minutes"],
            name="7-day average", mode="lines", line=dict(color="#F58518", dash="dash"),
        )
        daily_chart.add_bar(
            x=period_daily["metric_date"], y=period_daily["net_subs"],
            name="Net subscribers", yaxis="y2", marker_color="rgba(84,162,75,.35)",
        )
        daily_chart.update_layout(
            title=f"Daily Watch Depth — {period_label}",
            yaxis=dict(title="Average minutes"),
            yaxis2=dict(title="Net subscribers", overlaying="y", side="right"),
            hovermode="x unified",
            height=420,
        )
        st.plotly_chart(daily_chart, width="stretch")

st.caption(
    "All totals above aggregate daily increments; average durations are weighted by views."
)
