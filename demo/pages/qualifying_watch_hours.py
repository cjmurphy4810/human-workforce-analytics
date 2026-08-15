"""Public-only qualifying watch-hours report over the synthetic fixture."""

from __future__ import annotations

from datetime import timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from demo.analytics import eligible_organic_watch_hours
from demo.config import DEMO_AS_OF, DEMO_CHANNEL_KEY
from demo.report_data import require_frame
from demo.ui import render_demo_notice, render_demo_sidebar, render_empty_state


_WINDOW_START = DEMO_AS_OF - timedelta(days=364)
_PARAMS = {
    "channel": DEMO_CHANNEL_KEY,
    "start": _WINDOW_START.isoformat(),
    "as_of": DEMO_AS_OF.isoformat(),
}
_YPP_THRESHOLD = 3_000.0


@st.cache_data(ttl=300)
def _load_video_metrics() -> pd.DataFrame:
    return require_frame(
        "WITH metrics AS ("
        "SELECT video_id, SUM(views) views, "
        "SUM(estimated_minutes_watched) minutes, "
        "SUM(average_view_duration*views)/NULLIF(SUM(views),0) average_duration "
        "FROM daily_video_metrics WHERE channel=:channel "
        "AND metric_date BETWEEN :start AND :as_of GROUP BY video_id), "
        "advertising AS ("
        "SELECT video_id, SUM(views) advertising_views, "
        "SUM(estimated_minutes_watched) advertising_minutes "
        "FROM video_traffic_source_metrics WHERE channel=:channel "
        "AND metric_date BETWEEN :start AND :as_of "
        "AND traffic_source_type='ADVERTISING' GROUP BY video_id) "
        "SELECT v.video_id, v.title, COALESCE(v.duration_seconds,0) duration_seconds, "
        "COALESCE(m.views,0) views, COALESCE(m.minutes,0) minutes, "
        "COALESCE(m.average_duration,0) average_duration, "
        "COALESCE(a.advertising_views,0) advertising_views, "
        "COALESCE(a.advertising_minutes,0) advertising_minutes "
        "FROM videos v LEFT JOIN metrics m ON m.video_id=v.video_id "
        "LEFT JOIN advertising a ON a.video_id=v.video_id "
        "WHERE v.channel=:channel AND date(v.published_at)<=:as_of",
        _PARAMS,
    )


@st.cache_data(ttl=300)
def _load_daily_metrics() -> pd.DataFrame:
    return require_frame(
        "WITH advertising AS ("
        "SELECT metric_date, SUM(estimated_minutes_watched) minutes "
        "FROM video_traffic_source_metrics WHERE channel=:channel "
        "AND metric_date BETWEEN :start AND :as_of "
        "AND traffic_source_type='ADVERTISING' GROUP BY metric_date), "
        "shorts AS ("
        "SELECT d.metric_date, SUM(d.estimated_minutes_watched) minutes "
        "FROM daily_video_metrics d JOIN videos v "
        "ON v.channel=d.channel AND v.video_id=d.video_id "
        "WHERE d.channel=:channel AND d.metric_date BETWEEN :start AND :as_of "
        "AND v.duration_seconds>0 AND v.duration_seconds<=180 GROUP BY d.metric_date), "
        "short_ads AS ("
        "SELECT d.metric_date, SUM(d.estimated_minutes_watched) minutes "
        "FROM video_traffic_source_metrics d JOIN videos v "
        "ON v.channel=d.channel AND v.video_id=d.video_id "
        "WHERE d.channel=:channel AND d.metric_date BETWEEN :start AND :as_of "
        "AND d.traffic_source_type='ADVERTISING' "
        "AND v.duration_seconds>0 AND v.duration_seconds<=180 GROUP BY d.metric_date) "
        "SELECT c.metric_date, c.estimated_minutes_watched minutes, "
        "COALESCE(a.minutes,0) advertising_minutes, "
        "COALESCE(s.minutes,0) shorts_minutes, "
        "COALESCE(sa.minutes,0) short_advertising_minutes "
        "FROM daily_channel_metrics c "
        "LEFT JOIN advertising a ON a.metric_date=c.metric_date "
        "LEFT JOIN shorts s ON s.metric_date=c.metric_date "
        "LEFT JOIN short_ads sa ON sa.metric_date=c.metric_date "
        "WHERE c.channel=:channel AND c.metric_date BETWEEN :start AND :as_of "
        "ORDER BY c.metric_date",
        _PARAMS,
    )


def _prepare_videos(frame: pd.DataFrame) -> pd.DataFrame:
    videos = frame.copy()
    numeric = (
        "duration_seconds",
        "views",
        "minutes",
        "average_duration",
        "advertising_views",
        "advertising_minutes",
    )
    for column in numeric:
        videos[column] = pd.to_numeric(videos[column], errors="coerce").fillna(0.0)
    videos["watch_hours"] = videos["minutes"] / 60.0
    videos["promotion_hours"] = videos["advertising_minutes"] / 60.0
    videos["organic_hours"] = (
        videos["watch_hours"] - videos["promotion_hours"]
    ).clip(lower=0.0)
    videos["qualifying_hours"] = videos.apply(
        lambda row: eligible_organic_watch_hours(
            int(row["duration_seconds"]),
            float(row["watch_hours"]),
            float(row["promotion_hours"]),
        ),
        axis=1,
    )
    videos["promotion_cost"] = videos["advertising_views"] * 0.025
    videos["cost_per_qualifying_hour"] = videos.apply(
        lambda row: (
            float(row["promotion_cost"]) / float(row["qualifying_hours"])
            if row["promotion_cost"] > 0 and row["qualifying_hours"] > 0
            else None
        ),
        axis=1,
    )
    videos["qualifying_share"] = (
        videos["qualifying_hours"] / videos["watch_hours"].clip(lower=0.000001)
    ).clip(lower=0.0, upper=1.0)
    videos["format"] = videos["duration_seconds"].map(
        lambda duration: "Shorts" if 0 < duration <= 180 else "Long-form"
    )
    videos["promotion_state"] = videos["advertising_views"].map(
        lambda views: "Promoted" if views > 0 else "Organic only"
    )
    return videos


def _prepare_daily(frame: pd.DataFrame) -> pd.DataFrame:
    daily = frame.copy()
    daily["metric_date"] = pd.to_datetime(daily["metric_date"])
    for column in (
        "minutes",
        "advertising_minutes",
        "shorts_minutes",
        "short_advertising_minutes",
    ):
        daily[column] = pd.to_numeric(daily[column], errors="coerce").fillna(0.0)
    daily["watch_hours"] = daily["minutes"] / 60.0
    daily["promotion_hours"] = daily["advertising_minutes"] / 60.0
    daily["organic_hours"] = (
        daily["watch_hours"] - daily["promotion_hours"]
    ).clip(lower=0.0)
    shorts_organic = (
        daily["shorts_minutes"] - daily["short_advertising_minutes"]
    ).clip(lower=0.0) / 60.0
    daily["qualifying_hours"] = (daily["organic_hours"] - shorts_organic).clip(
        lower=0.0
    )
    return daily


render_demo_sidebar("Qualifying Watch Hours")
render_demo_notice()
st.title("⏱️ Qualifying Watch Hours")
st.caption(
    "Estimates YouTube Partner Program qualifying watch hours by excluding "
    "advertising-generated watch time and watch time from Shorts."
)
st.caption(
    f"All calculations use daily increments from {_WINDOW_START:%B %d, %Y} through "
    f"the simulated snapshot on {DEMO_AS_OF:%B %d, %Y}."
)

video_source = _load_video_metrics()
daily_source = _load_daily_metrics()
if video_source.empty or daily_source.empty:
    render_empty_state("qualifying watch-hour")
    st.stop()

videos = _prepare_videos(video_source)
daily = _prepare_daily(daily_source)

filter_a, filter_b = st.columns(2)
with filter_a:
    selected_formats = st.multiselect(
        "Video format",
        ["Long-form", "Shorts"],
        default=["Long-form", "Shorts"],
    )
with filter_b:
    selected_promotion = st.multiselect(
        "Promotion status",
        ["Promoted", "Organic only"],
        default=["Promoted", "Organic only"],
    )
filtered = videos[
    videos["format"].isin(selected_formats)
    & videos["promotion_state"].isin(selected_promotion)
].copy()

total_watch = float(filtered["watch_hours"].sum())
promotion_watch = float(filtered["promotion_hours"].sum())
organic_watch = float(filtered["organic_hours"].sum())
qualifying_watch = float(filtered["qualifying_hours"].sum())

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Watch Hours", f"{total_watch:,.1f}")
k2.metric("Promotion Watch Hours", f"{promotion_watch:,.1f}")
k3.metric("Organic Watch Hours", f"{organic_watch:,.1f}")
k4.metric("Qualifying Watch Hours", f"{qualifying_watch:,.1f}")

if promotion_watch == 0:
    st.info(
        "No advertising watch time is present in this selection; all non-Shorts "
        "watch hours qualify."
    )

summary_tab, trend_tab, videos_tab, impact_tab, projection_tab = st.tabs(
    [
        "Summary",
        "Qualifying Hours Trend",
        "Video Efficiency",
        "Promotion Impact",
        "YPP Projection",
    ]
)

with summary_tab:
    breakdown = pd.DataFrame(
        {
            "Category": [
                "Qualifying long-form",
                "Advertising excluded",
                "Shorts excluded",
            ],
            "Hours": [
                qualifying_watch,
                promotion_watch,
                max(organic_watch - qualifying_watch, 0.0),
            ],
        }
    )
    st.plotly_chart(
        px.bar(
            breakdown,
            x="Category",
            y="Hours",
            color="Category",
            title="Trailing-Year Watch-Hour Classification",
        ),
        width="stretch",
    )
    progress = min(qualifying_watch / _YPP_THRESHOLD, 1.0)
    st.progress(progress, text=f"{qualifying_watch:,.0f} of {_YPP_THRESHOLD:,.0f} hours")

with trend_tab:
    weekly = (
        daily.set_index("metric_date")[
            ["watch_hours", "promotion_hours", "qualifying_hours"]
        ]
        .resample("W-MON")
        .sum()
        .reset_index()
    )
    weekly["cumulative_qualifying"] = weekly["qualifying_hours"].cumsum()
    trend = go.Figure()
    trend.add_scatter(
        x=weekly["metric_date"],
        y=weekly["cumulative_qualifying"],
        mode="lines+markers",
        name="Cumulative qualifying hours",
    )
    trend.add_bar(
        x=weekly["metric_date"],
        y=weekly["promotion_hours"],
        name="Weekly promotion hours",
        yaxis="y2",
        opacity=0.35,
    )
    trend.update_layout(
        title="Qualifying Hours Trend",
        yaxis=dict(title="Cumulative qualifying hours"),
        yaxis2=dict(title="Weekly promotion hours", overlaying="y", side="right"),
        hovermode="x unified",
        height=430,
    )
    st.plotly_chart(trend, width="stretch")

with videos_tab:
    display = filtered[
        [
            "title",
            "format",
            "promotion_state",
            "views",
            "watch_hours",
            "promotion_hours",
            "qualifying_hours",
            "qualifying_share",
            "cost_per_qualifying_hour",
        ]
    ].sort_values("qualifying_hours", ascending=False)
    display = display.rename(
        columns={
            "title": "Video",
            "format": "Format",
            "promotion_state": "Promotion",
            "views": "Views",
            "watch_hours": "Watch Hours",
            "promotion_hours": "Promotion Hours",
            "qualifying_hours": "Qualifying Hours",
            "qualifying_share": "Qualifying Share",
            "cost_per_qualifying_hour": "Cost / Qualifying Hour",
        }
    )
    st.dataframe(display, width="stretch", hide_index=True)

with impact_tab:
    promoted = filtered[filtered["advertising_views"] > 0].copy()
    if promoted.empty:
        st.info(
            "The simulated selection contains no promotion rows; all non-Shorts "
            "watch hours qualify."
        )
    else:
        st.plotly_chart(
            px.scatter(
                promoted,
                x="promotion_cost",
                y="qualifying_hours",
                size="views",
                color="qualifying_share",
                hover_name="title",
                title="Promotion Spend vs Qualifying Watch Hours",
                labels={
                    "promotion_cost": "Estimated promotion cost ($)",
                    "qualifying_hours": "Qualifying watch hours",
                },
            ),
            width="stretch",
        )

with projection_tab:
    recent = daily.tail(28)
    weekly_rate = float(recent["qualifying_hours"].sum()) / 4.0
    growth = st.slider(
        "Weekly growth assumption",
        min_value=-10,
        max_value=30,
        value=5,
        step=1,
        format="%d%%",
    )
    remaining = max(_YPP_THRESHOLD - qualifying_watch, 0.0)
    projected_rate = max(weekly_rate * (1 + growth / 100.0), 0.0)
    weeks = remaining / projected_rate if projected_rate > 0 else None
    p1, p2, p3 = st.columns(3)
    p1.metric("Recent Weekly Pace", f"{weekly_rate:,.1f} h")
    p2.metric("Hours Remaining", f"{remaining:,.1f} h")
    p3.metric("Projected Weeks", f"{weeks:,.1f}" if weeks is not None else "N/A")
    st.caption(
        "Projection is a planning scenario based on simulated data, not a guarantee."
    )
