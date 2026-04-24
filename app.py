"""Streamlit dashboard for Human Workforce podcast YouTube analytics."""

import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
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


st.subheader("Channel Trends")
channel_snapshots["captured_at"] = pd.to_datetime(channel_snapshots["captured_at"])

t1, t2 = st.columns(2)
with t1:
    fig = px.line(channel_snapshots, x="captured_at", y="subscriber_count",
                  title="Subscribers Over Time", markers=True)
    st.plotly_chart(fig, use_container_width=True)
with t2:
    fig = px.line(channel_snapshots, x="captured_at", y="view_count",
                  title="Total Views Over Time", markers=True)
    st.plotly_chart(fig, use_container_width=True)


if not daily_channel.empty:
    st.subheader("Daily Performance (Last 7 Days)")
    daily_channel["metric_date"] = pd.to_datetime(daily_channel["metric_date"])
    daily_channel["net_subs"] = daily_channel["subscribers_gained"] - daily_channel["subscribers_lost"]

    d1, d2 = st.columns(2)
    with d1:
        fig = px.bar(daily_channel, x="metric_date", y="views", title="Views per Day")
        st.plotly_chart(fig, use_container_width=True)
    with d2:
        fig = px.bar(daily_channel, x="metric_date", y="net_subs",
                     title="Net Subscribers per Day",
                     color="net_subs", color_continuous_scale=["red", "lightgray", "green"])
        st.plotly_chart(fig, use_container_width=True)


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


if not videos.empty:
    st.subheader("Per-Video Deep Dive")
    titles = videos.set_index("video_id")["title"].to_dict()
    selected = st.selectbox("Pick a video", options=list(titles.keys()),
                            format_func=lambda v: titles[v])
    if selected:
        history = video_snapshots[video_snapshots["video_id"] == selected].copy()
        history["captured_at"] = pd.to_datetime(history["captured_at"])
        if not history.empty:
            fig = px.line(history, x="captured_at", y=["view_count", "like_count", "comment_count"],
                          title=f"Engagement growth — {titles[selected]}", markers=True)
            st.plotly_chart(fig, use_container_width=True)
        per_day = daily_videos[daily_videos["video_id"] == selected].copy()
        if not per_day.empty:
            per_day["metric_date"] = pd.to_datetime(per_day["metric_date"])
            fig = px.bar(per_day, x="metric_date", y="views", title="Daily views (last 7 days)")
            st.plotly_chart(fig, use_container_width=True)
