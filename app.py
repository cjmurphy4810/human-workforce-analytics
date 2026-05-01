"""Streamlit dashboard for Human Workforce podcast YouTube analytics."""

import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

DB_PATH = Path(__file__).parent / "data.db"

st.set_page_config(page_title="Human Workforce Analytics", page_icon="🎙️", layout="wide")


def check_password() -> bool:
    if st.session_state.get("authenticated"):
        return True
    expected = st.secrets.get("dashboard_password", "")
    if not expected:
        st.error("Dashboard password not configured. Set `dashboard_password` in Streamlit secrets.")
        return False
    pwd = st.text_input("Password", type="password")
    if pwd and pwd == expected:
        st.session_state["authenticated"] = True
        st.rerun()
    elif pwd:
        st.error("Incorrect password.")
    return False


if not check_password():
    st.stop()


@st.cache_data(ttl=300)
def load(query: str) -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(query, conn)


RANGES = {
    "Last week": 7,
    "Last month": 30,
    "Last quarter": 90,
    "Last year": 365,
}


def range_picker(key: str, default: str = "Last quarter") -> int:
    """Render a horizontal radio for time range and return number of days."""
    pick = st.radio(
        "Range",
        list(RANGES.keys()),
        index=list(RANGES.keys()).index(default),
        horizontal=True,
        key=key,
        label_visibility="collapsed",
    )
    return RANGES[pick]


def filter_days(df: pd.DataFrame, date_col: str, days: int) -> pd.DataFrame:
    cutoff = pd.Timestamp.utcnow().tz_localize(None) - pd.Timedelta(days=days)
    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col]).dt.tz_localize(None)
    return out[out[date_col] >= cutoff]


channel_snapshots = load(
    "SELECT captured_at, subscriber_count, view_count, video_count "
    "FROM channel_snapshots ORDER BY captured_at"
)
daily_channel = load(
    "SELECT metric_date, views, estimated_minutes_watched, "
    "subscribers_gained, subscribers_lost FROM daily_channel_metrics ORDER BY metric_date"
)
videos = load(
    "SELECT video_id, title, published_at, duration_seconds, thumbnail_url FROM videos"
)
video_snapshots = load(
    "SELECT captured_at, video_id, view_count, like_count, comment_count "
    "FROM video_snapshots ORDER BY captured_at"
)
daily_videos = load(
    "SELECT metric_date, video_id, views, estimated_minutes_watched, "
    "average_view_duration, likes FROM daily_video_metrics"
)

if channel_snapshots.empty:
    st.title("🎙️ Human Workforce Analytics")
    st.warning("No data yet. Run `python fetch_metrics.py` or wait for the first scheduled fetch.")
    st.stop()


st.title("🎙️ Human Workforce Analytics")
latest = channel_snapshots.iloc[-1]
prev = channel_snapshots.iloc[-2] if len(channel_snapshots) > 1 else latest

c1, c2, c3, c4 = st.columns(4)
c1.metric("Subscribers", f"{latest['subscriber_count']:,}",
          int(latest["subscriber_count"] - prev["subscriber_count"]))
c2.metric("Total Views", f"{latest['view_count']:,}",
          int(latest["view_count"] - prev["view_count"]))
c3.metric("Total Videos", int(latest["video_count"]))
c4.metric("Last Updated", pd.to_datetime(latest["captured_at"]).strftime("%b %d, %H:%M UTC"))


# --- Channel trends ---

st.subheader("Channel Trends")
days = range_picker("trend_range")
ct = filter_days(channel_snapshots, "captured_at", days)

t1, t2 = st.columns(2)
with t1:
    fig = px.line(ct, x="captured_at", y="subscriber_count",
                  title="Subscribers Over Time", markers=True)
    st.plotly_chart(fig, use_container_width=True)
with t2:
    fig = px.line(ct, x="captured_at", y="view_count",
                  title="Total Views Over Time", markers=True)
    st.plotly_chart(fig, use_container_width=True)


# --- Daily performance ---

if not daily_channel.empty:
    st.subheader("Daily Performance")
    days = range_picker("perf_range")
    dc = filter_days(daily_channel, "metric_date", days)
    dc["net_subs"] = dc["subscribers_gained"] - dc["subscribers_lost"]

    d1, d2 = st.columns(2)
    with d1:
        fig = px.bar(dc, x="metric_date", y="views", title="Views per Day")
        st.plotly_chart(fig, use_container_width=True)
    with d2:
        fig = px.bar(dc, x="metric_date", y="net_subs",
                     title="Net Subscribers per Day",
                     color="net_subs", color_continuous_scale=["red", "lightgray", "green"])
        st.plotly_chart(fig, use_container_width=True)


# --- Velocity (combined daily increments + 7-day rolling average) ---

if not daily_channel.empty:
    st.subheader("Growth Velocity")
    st.caption(
        "Daily increments (bars) with 7-day rolling average (line). "
        "Rising rolling averages = growth is accelerating."
    )
    days = range_picker("vel_range")
    v = filter_days(daily_channel, "metric_date", days).sort_values("metric_date")
    v["watch_hours"] = v["estimated_minutes_watched"] / 60
    v["net_subs"] = v["subscribers_gained"] - v["subscribers_lost"]
    v["views_ma"] = v["views"].rolling(7, min_periods=1).mean()
    v["subs_ma"] = v["net_subs"].rolling(7, min_periods=1).mean()
    v["hours_ma"] = v["watch_hours"].rolling(7, min_periods=1).mean()

    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.07,
        subplot_titles=("Views per day", "Net subscribers per day", "Watch hours per day"),
    )
    fig.add_bar(x=v["metric_date"], y=v["views"], name="Views",
                marker_color="rgba(76,120,168,0.6)", row=1, col=1)
    fig.add_scatter(x=v["metric_date"], y=v["views_ma"], name="Views (7-day avg)",
                    mode="lines", line=dict(color="#F58518", width=3), row=1, col=1)
    fig.add_bar(x=v["metric_date"], y=v["net_subs"], name="Subs",
                marker_color="rgba(84,162,75,0.6)", row=2, col=1)
    fig.add_scatter(x=v["metric_date"], y=v["subs_ma"], name="Subs (7-day avg)",
                    mode="lines", line=dict(color="#F58518", width=3), row=2, col=1)
    fig.add_bar(x=v["metric_date"], y=v["watch_hours"], name="Watch hours",
                marker_color="rgba(176,108,158,0.6)", row=3, col=1)
    fig.add_scatter(x=v["metric_date"], y=v["hours_ma"], name="Hours (7-day avg)",
                    mode="lines", line=dict(color="#F58518", width=3), row=3, col=1)
    fig.update_layout(height=720, showlegend=False, hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)


# --- Top videos ---

if not video_snapshots.empty:
    st.subheader("Top Videos (All-Time)")
    latest_per_video = (
        video_snapshots.sort_values("captured_at")
        .groupby("video_id").last().reset_index()
    )
    top = latest_per_video.merge(videos, on="video_id").sort_values("view_count", ascending=False)
    st.dataframe(
        top[["title", "view_count", "like_count", "comment_count", "published_at"]].head(20),
        use_container_width=True,
        hide_index=True,
    )


# --- Watch time ---

if not daily_channel.empty:
    st.subheader("Watch Time")
    days = range_picker("wt_range")
    hours = filter_days(daily_channel, "metric_date", days).sort_values("metric_date")
    hours["hours_watched"] = hours["estimated_minutes_watched"] / 60
    hours["cumulative_hours"] = hours["hours_watched"].cumsum()

    fig = go.Figure()
    fig.add_bar(x=hours["metric_date"], y=hours["hours_watched"],
                name="Hours watched (per day)", marker_color="#4C78A8")
    fig.add_scatter(x=hours["metric_date"], y=hours["cumulative_hours"],
                    name="Cumulative hours", mode="lines+markers",
                    yaxis="y2", line=dict(color="#F58518", width=3))
    fig.update_layout(
        title="Daily and Cumulative Watch Time",
        xaxis_title="Date",
        yaxis=dict(title="Hours watched per day"),
        yaxis2=dict(title="Cumulative hours", overlaying="y", side="right"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig, use_container_width=True)


# --- Per-video deep dive ---

if not videos.empty:
    st.subheader("Per-Video Deep Dive")
    titles = videos.set_index("video_id")["title"].to_dict()
    selected = st.selectbox("Pick a video", options=list(titles.keys()),
                            format_func=lambda v: titles[v])
    if selected:
        days = range_picker("video_range", default="Last quarter")
        history = video_snapshots[video_snapshots["video_id"] == selected]
        history = filter_days(history, "captured_at", days)
        if not history.empty:
            fig = px.line(history, x="captured_at", y=["view_count", "like_count", "comment_count"],
                          title=f"Engagement growth — {titles[selected]}", markers=True)
            st.plotly_chart(fig, use_container_width=True)
        per_day = daily_videos[daily_videos["video_id"] == selected]
        if not per_day.empty:
            per_day = filter_days(per_day, "metric_date", days)
            fig = px.bar(per_day, x="metric_date", y="views",
                         title=f"Per-fetch view totals (since {per_day['metric_date'].min().date()})")
            st.plotly_chart(fig, use_container_width=True)
