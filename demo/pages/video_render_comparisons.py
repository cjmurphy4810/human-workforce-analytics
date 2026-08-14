"""Compare the four fictional render formats in the public demo."""

from __future__ import annotations

from datetime import timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

from demo.analytics import eligible_organic_watch_hours
from demo.config import DEMO_AS_OF, DEMO_CHANNEL_KEY
from demo.report_data import query_frame
from demo.ui import render_demo_notice, render_demo_sidebar, render_empty_state


_SHORTS_PLAYLISTS = {"Engineering Shorts"}
_VISUAL_PLAYLISTS = {"Visual Engineering Briefings"}
_HD_PLAYLISTS = {"Deep-Dive Workshops"}
_JELLYPOD_PLAYLISTS = {"Studio Originals"}

_GROUP_PLAYLISTS = {
    "Engineering Shorts": _SHORTS_PLAYLISTS,
    "Visual Briefings": _VISUAL_PLAYLISTS,
    "Deep-Dive Workshops": _HD_PLAYLISTS,
    "Studio Originals": _JELLYPOD_PLAYLISTS,
}
_GROUP_COLORS = {
    "Engineering Shorts": "#F2A900",
    "Visual Briefings": "#2A78D6",
    "Deep-Dive Workshops": "#1BAF7A",
    "Studio Originals": "#6650B5",
}
_PARAMS = {
    "channel": DEMO_CHANNEL_KEY,
    "start": (DEMO_AS_OF - timedelta(days=364)).isoformat(),
    "as_of": DEMO_AS_OF.isoformat(),
}


@st.cache_data(ttl=300)
def _load_playlist_videos() -> pd.DataFrame:
    return query_frame(
        "SELECT p.title AS playlist, pv.video_id FROM playlists p "
        "JOIN playlist_videos pv ON pv.channel=p.channel AND pv.playlist_id=p.playlist_id "
        "WHERE p.channel=:channel",
        _PARAMS,
    )


@st.cache_data(ttl=300)
def _load_video_window() -> pd.DataFrame:
    """Aggregate each video's daily increments over one explicit inclusive window."""
    return query_frame(
        "WITH metrics AS ("
        "SELECT video_id, SUM(views) AS views, "
        "SUM(estimated_minutes_watched)/60.0 AS watch_hours, "
        "SUM(average_view_duration*views)/NULLIF(SUM(views),0) AS average_view_duration, "
        "SUM(subscribers_gained) AS subscribers_gained "
        "FROM daily_video_metrics WHERE channel=:channel "
        "AND metric_date BETWEEN :start AND :as_of GROUP BY video_id), "
        "advertising AS ("
        "SELECT video_id, SUM(estimated_minutes_watched)/60.0 AS advertising_hours "
        "FROM video_traffic_source_metrics WHERE channel=:channel "
        "AND metric_date BETWEEN :start AND :as_of "
        "AND traffic_source_type='ADVERTISING' GROUP BY video_id) "
        "SELECT v.video_id, COALESCE(v.duration_seconds,0) duration_seconds, "
        "COALESCE(m.views,0) views, COALESCE(m.watch_hours,0) watch_hours, "
        "COALESCE(m.average_view_duration,0) average_view_duration, "
        "COALESCE(m.subscribers_gained,0) subscribers_gained, "
        "COALESCE(a.advertising_hours,0) advertising_hours "
        "FROM videos v LEFT JOIN metrics m ON m.video_id=v.video_id "
        "LEFT JOIN advertising a ON a.video_id=v.video_id "
        "WHERE v.channel=:channel AND date(v.published_at)<=:as_of",
        _PARAMS,
    )


def _group_stats(
    video_ids: set[str],
    label: str,
    videos: pd.DataFrame,
) -> dict[str, float | int | str]:
    selected = (
        videos[videos["video_id"].isin(video_ids)].drop_duplicates("video_id").copy()
    )
    for column in (
        "views",
        "watch_hours",
        "advertising_hours",
        "subscribers_gained",
        "average_view_duration",
        "duration_seconds",
    ):
        selected[column] = selected[column].fillna(0.0)
    count = int(selected["video_id"].nunique())
    if not count:
        return {"group": label, "video_count": 0}
    views = float(selected["views"].sum())
    watch_hours = float(selected["watch_hours"].sum())
    qualifying_hours = float(
        selected.apply(
            lambda row: eligible_organic_watch_hours(
                int(row["duration_seconds"]),
                float(row["watch_hours"]),
                float(row["advertising_hours"]),
            ),
            axis=1,
        ).sum()
    )
    subscribers = float(selected["subscribers_gained"].sum())
    average_duration = float(
        (selected["average_view_duration"] * selected["views"]).sum() / max(views, 1.0)
    )
    return {
        "group": label,
        "video_count": count,
        "views": int(views),
        "watch_hours": watch_hours,
        "qualifying_hours": qualifying_hours,
        "subscribers": int(subscribers),
        "avg_view_dur_sec": average_duration,
        "avg_views_per_video": views / count,
        "avg_wh_per_video": watch_hours / count,
        "avg_qh_per_video": qualifying_hours / count,
        "avg_subs_per_video": subscribers / count,
        "qualifying_ratio": qualifying_hours / max(watch_hours, 1.0),
    }


def _duration(seconds: float) -> str:
    minutes, remainder = divmod(int(max(seconds, 0)), 60)
    return f"{minutes}:{remainder:02d}"


render_demo_sidebar("Video Render Comparisons")
render_demo_notice()
st.title("🎬 Video Render Comparisons")
st.caption(
    f"Compares daily increments aggregated across the same trailing-year window through "
    f"{DEMO_AS_OF:%B %d, %Y}. Each fictional playlist maps to exactly one render format."
)

playlist_videos = _load_playlist_videos()
all_videos = _load_video_window()
if playlist_videos.empty or all_videos.empty:
    render_empty_state("video render comparison")
    st.stop()

claimed: set[str] = set()
groups: list[dict[str, float | int | str]] = []
for label, playlist_names in _GROUP_PLAYLISTS.items():
    video_ids = (
        set(
            playlist_videos[playlist_videos["playlist"].isin(playlist_names)][
                "video_id"
            ]
        )
        - claimed
    )
    claimed.update(video_ids)
    groups.append(_group_stats(video_ids, label, all_videos))

group_frame = pd.DataFrame([group for group in groups if group["video_count"] > 0])
if group_frame.empty:
    render_empty_state("video render comparison metrics")
    st.stop()

st.caption(
    "Qualifying hours are summed per video: Shorts are zero and long-form videos subtract "
    "ADVERTISING watch time within the same selected window. Playlist groups are "
    "de-duplicated before aggregation."
)

show_mode = st.radio(
    "Show",
    ["Totals for selected window", "Per-video averages"],
    horizontal=True,
    key="demo_render_mode",
)
is_average = show_mode.startswith("Per-video")
metric_columns = (
    {
        "Average Views / Video": "avg_views_per_video",
        "Average Watch Hours / Video": "avg_wh_per_video",
        "Average Qualifying Hours / Video": "avg_qh_per_video",
        "Average Subscribers / Video": "avg_subs_per_video",
    }
    if is_average
    else {
        "Views": "views",
        "Watch Hours": "watch_hours",
        "Qualifying Hours": "qualifying_hours",
        "Subscribers": "subscribers",
    }
)

melted = group_frame[["group"] + list(metric_columns.values())].melt(
    id_vars="group", var_name="column", value_name="value"
)
melted["Metric"] = melted["column"].map(
    {column: label for label, column in metric_columns.items()}
)
chart = px.bar(
    melted,
    x="Metric",
    y="value",
    color="group",
    barmode="group",
    color_discrete_map=_GROUP_COLORS,
    text_auto=".3s",
    labels={"value": "", "group": "Render Format", "Metric": ""},
    title=(
        "Per-Video Averages by Render Format"
        if is_average
        else "Trailing-Year Totals by Render Format"
    ),
)
chart.update_traces(textposition="outside", cliponaxis=False)
chart.update_layout(height=480, hovermode="x unified", legend=dict(orientation="h"))
st.plotly_chart(chart, width="stretch")

st.subheader("Average View Time")
duration_columns = st.columns(len(group_frame))
for column, (_, row) in zip(duration_columns, group_frame.iterrows()):
    column.metric(str(row["group"]), _duration(float(row["avg_view_dur_sec"])))

st.subheader("Qualifying Share")
st.plotly_chart(
    px.bar(
        group_frame,
        x="group",
        y="qualifying_ratio",
        color="group",
        color_discrete_map=_GROUP_COLORS,
        text_auto=".1%",
        labels={"group": "Render Format", "qualifying_ratio": "Qualifying Share"},
    ),
    width="stretch",
)

st.subheader("Group Summary")
summary = group_frame[
    [
        "group",
        "video_count",
        "views",
        "watch_hours",
        "qualifying_hours",
        "subscribers",
        "avg_view_dur_sec",
        "avg_views_per_video",
        "avg_wh_per_video",
        "avg_qh_per_video",
        "avg_subs_per_video",
    ]
].copy()
summary["avg_view_time"] = summary["avg_view_dur_sec"].map(_duration)
summary = summary.drop(columns="avg_view_dur_sec").rename(
    columns={
        "group": "Render Format",
        "video_count": "Videos",
        "views": "Views",
        "watch_hours": "Watch Hours",
        "qualifying_hours": "Qualifying Hours",
        "subscribers": "Subscribers",
        "avg_views_per_video": "Average Views/Video",
        "avg_wh_per_video": "Average Watch Hours/Video",
        "avg_qh_per_video": "Average Qualifying Hours/Video",
        "avg_subs_per_video": "Average Subscribers/Video",
        "avg_view_time": "Average View Time",
    }
)
st.dataframe(summary, width="stretch", hide_index=True)
st.caption(
    "Engineering Shorts → Engineering Shorts · Visual Briefings → Visual Engineering Briefings · "
    "Deep-Dive Workshops → Deep-Dive Workshops · Studio Originals → Studio Originals."
)
