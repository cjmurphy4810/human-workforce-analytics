"""Full synthetic overview for the public channel-analytics demo."""

from __future__ import annotations

import html
import json

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

import projections
import retention
from demo.config import DEMO_AS_OF, DEMO_CHANNEL_KEY, DEMO_CHANNEL_NAME
from demo.report_data import query_frame
from demo.ui import render_demo_notice, render_demo_sidebar, render_empty_state


active_channel = render_demo_sidebar("Overview")
render_demo_notice()
st.title(f"📊 {DEMO_CHANNEL_NAME} Analytics")

_PARAMS = {"channel": DEMO_CHANNEL_KEY, "as_of": DEMO_AS_OF.isoformat()}
_RANGES = {"Last week": 7, "Last month": 30, "Last quarter": 90, "Last year": 365}
_COUNTRIES = {
    "US": "United States", "CA": "Canada", "GB": "United Kingdom",
    "IN": "India", "DE": "Germany", "AU": "Australia",
    "BR": "Brazil", "FR": "France", "JP": "Japan", "NL": "Netherlands",
}


def _query(sql: str) -> pd.DataFrame:
    return query_frame(
        sql,
        {"channel": DEMO_CHANNEL_KEY, "as_of": _PARAMS["as_of"]},
    )


def _range_picker(key: str, default: str = "Last quarter") -> int:
    choice = st.radio(
        "Range",
        list(_RANGES),
        index=list(_RANGES).index(default),
        horizontal=True,
        key=key,
        label_visibility="collapsed",
    )
    return _RANGES[choice]


def _filter_days(frame: pd.DataFrame, date_column: str, days: int) -> pd.DataFrame:
    cutoff = pd.Timestamp(DEMO_AS_OF) - pd.Timedelta(days=days - 1)
    result = frame.copy()
    result[date_column] = pd.to_datetime(result[date_column]).dt.tz_localize(None)
    return result[(result[date_column] >= cutoff) & (result[date_column] <= pd.Timestamp(DEMO_AS_OF))]


channel_snapshots = _query(
    "SELECT captured_at, subscriber_count, view_count, video_count FROM channel_snapshots "
    "WHERE channel=:channel AND date(captured_at)<=:as_of ORDER BY captured_at"
)
daily_channel = _query(
    "SELECT metric_date, views, estimated_minutes_watched, subscribers_gained, "
    "subscribers_lost FROM daily_channel_metrics WHERE channel=:channel "
    "AND metric_date<=:as_of ORDER BY metric_date"
)
videos = _query(
    "SELECT video_id, title, published_at, duration_seconds, thumbnail_url FROM videos "
    "WHERE channel=:channel AND date(published_at)<=:as_of"
)
video_snapshots = _query(
    "SELECT captured_at, video_id, view_count, like_count, comment_count FROM video_snapshots "
    "WHERE channel=:channel AND date(captured_at)<=:as_of ORDER BY captured_at"
)
daily_videos = _query(
    "SELECT metric_date, video_id, views, estimated_minutes_watched, average_view_duration, "
    "likes, subscribers_gained FROM daily_video_metrics WHERE channel=:channel "
    "AND metric_date<=:as_of ORDER BY metric_date"
)
daily_geo = _query(
    "SELECT metric_date, country_code, views, subscribers_gained, likes FROM daily_geo_metrics "
    "WHERE channel=:channel AND metric_date<=:as_of ORDER BY metric_date"
)
playlists = _query(
    "SELECT playlist_id, title, item_count FROM playlists WHERE channel=:channel"
)
playlist_videos = _query(
    "SELECT playlist_id, video_id FROM playlist_videos WHERE channel=:channel"
)
retention_buckets = _query(
    "SELECT video_id, window_start, window_end, window_kind, views, retention_at_25, "
    "retention_at_75 FROM retention_buckets WHERE channel=:channel AND window_end<=:as_of"
)
traffic_sources = _query(
    "SELECT metric_date, traffic_source_type, views, estimated_minutes_watched "
    "FROM channel_traffic_sources WHERE channel=:channel AND metric_date<=:as_of"
)
video_traffic = _query(
    "SELECT metric_date, video_id, traffic_source_type, views, estimated_minutes_watched "
    "FROM video_traffic_source_metrics WHERE channel=:channel AND metric_date<=:as_of"
)
publishing_queue = _query(
    "SELECT analyzed_at, videos_analyzed, news_stories_count, result_json FROM publishing_queue "
    "WHERE channel=:channel AND date(analyzed_at)<=:as_of ORDER BY analyzed_at DESC LIMIT 1"
)
queue_history = _query(
    "SELECT q.video_id, q.recommended_publish_date, q.rank_at_recommendation, "
    "q.relevance_score, q.theme, v.title, v.published_at FROM queue_recommendations q "
    "JOIN videos v ON v.channel=q.channel AND v.video_id=q.video_id "
    "WHERE q.channel=:channel AND date(v.published_at)<=:as_of"
)

if channel_snapshots.empty:
    render_empty_state("overview")
    st.stop()

latest = channel_snapshots.iloc[-1]
previous = channel_snapshots.iloc[-2] if len(channel_snapshots) > 1 else latest
summary = st.columns(4)
summary[0].metric("Subscribers", f"{int(latest['subscriber_count']):,}", int(latest["subscriber_count"] - previous["subscriber_count"]))
summary[1].metric("Total Views", f"{int(latest['view_count']):,}", int(latest["view_count"] - previous["view_count"]))
summary[2].metric("Published Videos", int(latest["video_count"]))
summary[3].metric("Snapshot", pd.to_datetime(latest["captured_at"]).strftime("%b %d, %Y"))

st.subheader("Channel Trends")
trend_days = _range_picker("demo_overview_trend")
trends = _filter_days(channel_snapshots, "captured_at", trend_days)
trend_a, trend_b = st.columns(2)
with trend_a:
    st.plotly_chart(px.line(trends, x="captured_at", y="subscriber_count", markers=True, title="Subscribers Over Time"), use_container_width=True)
with trend_b:
    st.plotly_chart(px.line(trends, x="captured_at", y="view_count", markers=True, title="Total Views Over Time"), use_container_width=True)

if not daily_channel.empty:
    st.subheader("Daily Performance")
    performance_days = _range_picker("demo_overview_performance")
    performance = _filter_days(daily_channel, "metric_date", performance_days)
    performance["net_subscribers"] = performance["subscribers_gained"] - performance["subscribers_lost"]
    perf_a, perf_b = st.columns(2)
    with perf_a:
        st.plotly_chart(px.bar(performance, x="metric_date", y="views", title="Views per Day"), use_container_width=True)
    with perf_b:
        st.plotly_chart(px.bar(performance, x="metric_date", y="net_subscribers", title="Net Subscribers per Day", color="net_subscribers", color_continuous_scale="RdYlGn"), use_container_width=True)

    st.subheader("Growth Velocity")
    velocity_days = _range_picker("demo_overview_velocity")
    velocity = _filter_days(daily_channel, "metric_date", velocity_days).sort_values("metric_date")
    velocity["watch_hours"] = velocity["estimated_minutes_watched"] / 60.0
    velocity["net_subscribers"] = velocity["subscribers_gained"] - velocity["subscribers_lost"]
    velocity["views_average"] = velocity["views"].rolling(7, min_periods=1).mean()
    velocity["subs_average"] = velocity["net_subscribers"].rolling(7, min_periods=1).mean()
    velocity["hours_average"] = velocity["watch_hours"].rolling(7, min_periods=1).mean()
    velocity_chart = make_subplots(rows=3, cols=1, shared_xaxes=True, subplot_titles=("Views per day", "Net subscribers per day", "Watch hours per day"))
    for row, value, average, color in (
        (1, "views", "views_average", "#4C78A8"),
        (2, "net_subscribers", "subs_average", "#54A24B"),
        (3, "watch_hours", "hours_average", "#B279A2"),
    ):
        velocity_chart.add_bar(x=velocity["metric_date"], y=velocity[value], marker_color=color, row=row, col=1, showlegend=False)
        velocity_chart.add_scatter(x=velocity["metric_date"], y=velocity[average], line=dict(color="#F58518", width=3), row=row, col=1, showlegend=False)
    velocity_chart.update_layout(height=700, hovermode="x unified")
    st.plotly_chart(velocity_chart, use_container_width=True)

st.subheader("Geographic Trends")
if daily_geo.empty:
    render_empty_state("geographic trends")
else:
    geo_days = _range_picker("demo_overview_geo")
    geo = _filter_days(daily_geo, "metric_date", geo_days)
    geo = geo.groupby("country_code", as_index=False).agg(views=("views", "sum"), subscribers_gained=("subscribers_gained", "sum"), likes=("likes", "sum"))
    geo["country"] = geo["country_code"].map(lambda code: _COUNTRIES.get(code, code))
    geo = geo.sort_values("views", ascending=False)
    selected_countries = st.multiselect("Show regions", geo["country"].tolist(), default=geo["country"].tolist(), key="demo_geo_filter")
    geo = geo[geo["country"].isin(selected_countries)] if selected_countries else geo
    geo_chart = make_subplots(rows=1, cols=3, subplot_titles=("Views", "Subscribers Gained", "Likes"), shared_yaxes=True)
    for column, metric, color in ((1, "views", "#4C78A8"), (2, "subscribers_gained", "#F58518"), (3, "likes", "#54A24B")):
        geo_chart.add_bar(x=geo[metric], y=geo["country"], orientation="h", marker_color=color, row=1, col=column, showlegend=False)
    geo_chart.update_yaxes(autorange="reversed")
    geo_chart.update_layout(height=max(320, len(geo) * 38))
    st.plotly_chart(geo_chart, use_container_width=True)

st.subheader("Top Videos")
if video_snapshots.empty or videos.empty:
    render_empty_state("video snapshots")
else:
    latest_video_snapshots = video_snapshots.sort_values("captured_at").groupby("video_id", as_index=False).last()
    top_videos = latest_video_snapshots.merge(videos, on="video_id").sort_values("view_count", ascending=False)
    st.dataframe(top_videos[["title", "view_count", "like_count", "comment_count", "published_at"]].head(20), use_container_width=True, hide_index=True)

st.subheader("Playlists")
st.caption("Performance sums each video's daily increments across the selected period.")
if playlists.empty or playlist_videos.empty or daily_videos.empty:
    render_empty_state("playlist analytics")
else:
    playlist_days = _range_picker("demo_overview_playlists")
    playlist_daily = _filter_days(daily_videos, "metric_date", playlist_days)
    per_video = playlist_daily.groupby("video_id", as_index=False).agg(views=("views", "sum"), minutes=("estimated_minutes_watched", "sum"), likes=("likes", "sum"))
    playlist_rollup = playlist_videos.merge(per_video, on="video_id", how="left").fillna(0)
    playlist_rollup = playlist_rollup.groupby("playlist_id", as_index=False).agg(views=("views", "sum"), minutes=("minutes", "sum"), likes=("likes", "sum")).merge(playlists, on="playlist_id", how="right").fillna(0)
    playlist_rollup["watch_hours"] = playlist_rollup["minutes"] / 60.0
    playlist_rollup = playlist_rollup.sort_values("views", ascending=False)
    playlist_a, playlist_b = st.columns(2)
    with playlist_a:
        chart = px.bar(playlist_rollup, x="views", y="title", orientation="h", title="Views per Playlist")
        chart.update_yaxes(autorange="reversed")
        st.plotly_chart(chart, use_container_width=True)
    with playlist_b:
        chart = px.bar(playlist_rollup, x="watch_hours", y="title", orientation="h", title="Watch Hours per Playlist")
        chart.update_yaxes(autorange="reversed")
        st.plotly_chart(chart, use_container_width=True)
    st.dataframe(playlist_rollup[["title", "item_count", "views", "watch_hours", "likes"]], use_container_width=True, hide_index=True)

if not daily_channel.empty:
    st.subheader("Watch Time")
    watch_days = _range_picker("demo_overview_watch")
    watch = _filter_days(daily_channel, "metric_date", watch_days).sort_values("metric_date")
    watch["watch_hours"] = watch["estimated_minutes_watched"] / 60.0
    advertising_daily = (
        video_traffic[video_traffic["traffic_source_type"] == "ADVERTISING"]
        .groupby("metric_date", as_index=False)["estimated_minutes_watched"]
        .sum()
        .rename(columns={"estimated_minutes_watched": "advertising_minutes"})
        if not video_traffic.empty
        else pd.DataFrame(columns=["metric_date", "advertising_minutes"])
    )
    short_ids = set(
        videos.loc[videos["duration_seconds"].between(1, 180), "video_id"]
    )
    shorts_daily = (
        daily_videos[daily_videos["video_id"].isin(short_ids)]
        .groupby("metric_date", as_index=False)["estimated_minutes_watched"]
        .sum()
        .rename(columns={"estimated_minutes_watched": "shorts_minutes"})
    )
    short_ads_daily = (
        video_traffic[
            video_traffic["video_id"].isin(short_ids)
            & (video_traffic["traffic_source_type"] == "ADVERTISING")
        ]
        .groupby("metric_date", as_index=False)["estimated_minutes_watched"]
        .sum()
        .rename(columns={"estimated_minutes_watched": "shorts_ad_minutes"})
    )
    for adjustments in (advertising_daily, shorts_daily, short_ads_daily):
        if not adjustments.empty:
            adjustments["metric_date"] = pd.to_datetime(adjustments["metric_date"])
            watch = watch.merge(adjustments, on="metric_date", how="left")
    for column in ("advertising_minutes", "shorts_minutes", "shorts_ad_minutes"):
        if column not in watch:
            watch[column] = 0.0
        watch[column] = watch[column].fillna(0.0)
    watch["promotion_hours"] = watch["advertising_minutes"] / 60.0
    watch["organic_hours"] = (watch["watch_hours"] - watch["promotion_hours"]).clip(lower=0)
    watch["shorts_organic_hours"] = (
        watch["shorts_minutes"] - watch["shorts_ad_minutes"]
    ).clip(lower=0) / 60.0
    watch["qualifying_hours"] = (
        watch["organic_hours"] - watch["shorts_organic_hours"]
    ).clip(lower=0)
    watch_chart = go.Figure()
    watch_chart.add_bar(x=watch["metric_date"], y=watch["qualifying_hours"], name="Qualifying hours", marker_color="#54A24B")
    watch_chart.add_bar(x=watch["metric_date"], y=watch["promotion_hours"], name="Promotion hours", marker_color="#E45756")
    watch_chart.update_layout(barmode="stack", title="Daily Watch Hours: Qualifying vs Promotion", hovermode="x unified")
    st.plotly_chart(watch_chart, use_container_width=True)
    selected_total = watch["watch_hours"].sum()
    selected_qualifying = watch["qualifying_hours"].sum()
    selected_ratio = selected_qualifying / max(selected_total, 1.0)
    st.caption(
        f"Selected-period qualifying share: {selected_ratio * 100:.1f}%. "
        "Qualifying subtracts promotion and organic Shorts from the same displayed days."
    )

st.subheader("Discovery & Engagement Analysis")
discovery_tab, engagement_tab, table_tab = st.tabs(["Traffic Sources", "Video Engagement", "Detail Table"])
with discovery_tab:
    if traffic_sources.empty:
        render_empty_state("traffic-source")
    else:
        traffic_days = _range_picker("demo_overview_traffic")
        traffic = _filter_days(traffic_sources, "metric_date", traffic_days)
        traffic = traffic.groupby("traffic_source_type", as_index=False).agg(views=("views", "sum"), minutes=("estimated_minutes_watched", "sum"))
        traffic["watch_hours"] = traffic["minutes"] / 60.0
        traffic = traffic.sort_values("views", ascending=False)
        traffic_chart = px.bar(traffic, x="views", y="traffic_source_type", orientation="h", color="watch_hours", color_continuous_scale="Blues", title="Discovery Sources")
        traffic_chart.update_yaxes(autorange="reversed")
        st.plotly_chart(traffic_chart, use_container_width=True)

engagement = pd.DataFrame()
if not daily_videos.empty and not videos.empty:
    engagement = daily_videos.groupby("video_id", as_index=False).agg(
        views=("views", "sum"),
        minutes=("estimated_minutes_watched", "sum"),
        likes=("likes", "sum"),
        subscribers=("subscribers_gained", "sum"),
    )
    weighted = daily_videos.assign(weighted=daily_videos["average_view_duration"] * daily_videos["views"]).groupby("video_id")["weighted"].sum()
    engagement["average_duration"] = engagement["video_id"].map(weighted) / engagement["views"].clip(lower=1)
    engagement = engagement.merge(videos[["video_id", "title", "duration_seconds"]], on="video_id", how="left")
    engagement["watch_hours"] = engagement["minutes"] / 60.0
    engagement["watch_rate"] = engagement["average_duration"] / engagement["duration_seconds"].clip(lower=1) * 100
    engagement["like_rate"] = engagement["likes"] / engagement["views"].clip(lower=1) * 100
    engagement["subscriber_rate"] = engagement["subscribers"] / engagement["views"].clip(lower=1) * 100

with engagement_tab:
    if engagement.empty:
        render_empty_state("engagement")
    else:
        ranking = st.selectbox("Rank by", ["watch_rate", "views", "like_rate", "subscriber_rate"], key="demo_engagement_rank")
        ranked = engagement.sort_values(ranking, ascending=False).head(20).copy()
        ranked["short_title"] = ranked["title"].map(lambda value: str(value)[:45] + ("…" if len(str(value)) > 45 else ""))
        engagement_chart = px.bar(ranked, x=ranking, y="short_title", orientation="h", color="watch_rate", color_continuous_scale="RdYlGn", title="Video Engagement Ranking")
        engagement_chart.update_yaxes(autorange="reversed")
        st.plotly_chart(engagement_chart, use_container_width=True)
with table_tab:
    if engagement.empty:
        render_empty_state("engagement detail")
    else:
        st.dataframe(engagement[["title", "views", "watch_hours", "watch_rate", "like_rate", "subscriber_rate"]].sort_values("views", ascending=False), use_container_width=True, hide_index=True)


def _render_retention(frame: pd.DataFrame, range_label: str, video_id: str | None = None) -> None:
    _, _, rolling_days = retention.window_bounds_for_toggle(range_label, DEMO_AS_OF)
    rows = frame[frame["window_kind"] == f"rolling{rolling_days}"].copy()
    if video_id is not None:
        rows = rows[rows["video_id"] == video_id]
    if rows.empty:
        render_empty_state("retention")
        return
    rows = rows.sort_values("window_end").groupby("video_id", as_index=False).tail(1)
    snapshot = retention.aggregate_snapshot(rows)
    if snapshot["total_views"] == 0:
        render_empty_state("retention")
        return
    retention_cols = st.columns(3)
    retention_cols[0].metric("Dropped early (0–25%)", f"{int(snapshot['b1_count']):,}", f"{snapshot['b1_pct'] * 100:.1f}%", delta_color="off")
    retention_cols[1].metric("Mid-watch (25–75%)", f"{int(snapshot['b2_count']):,}", f"{snapshot['b2_pct'] * 100:.1f}%", delta_color="off")
    retention_cols[2].metric("Stayed through 75%", f"{int(snapshot['b3_count']):,}", f"{snapshot['b3_pct'] * 100:.1f}%", delta_color="off")


st.subheader("Audience Retention")
if retention_buckets.empty:
    render_empty_state("retention")
else:
    retention_range = st.radio("Range", list(_RANGES), index=2, horizontal=True, key="demo_retention_range", label_visibility="collapsed")
    _render_retention(retention_buckets, retention_range)

st.subheader("Per-Video Deep Dive")
if videos.empty:
    render_empty_state("per-video analysis")
else:
    title_map = videos.set_index("video_id")["title"].to_dict()
    selected_video = st.selectbox("Pick a video", list(title_map), format_func=lambda video_id: title_map[video_id], key="demo_video_deep_dive")
    video_days = _range_picker("demo_video_range")
    history = _filter_days(video_snapshots[video_snapshots["video_id"] == selected_video], "captured_at", video_days) if not video_snapshots.empty else pd.DataFrame()
    per_day = _filter_days(daily_videos[daily_videos["video_id"] == selected_video], "metric_date", video_days) if not daily_videos.empty else pd.DataFrame()
    deep_a, deep_b = st.columns(2)
    with deep_a:
        if history.empty:
            render_empty_state("video snapshot")
        else:
            st.plotly_chart(px.line(history, x="captured_at", y=["view_count", "like_count", "comment_count"], markers=True, title="Engagement Growth"), use_container_width=True)
    with deep_b:
        if per_day.empty:
            render_empty_state("video daily metrics")
        else:
            st.plotly_chart(px.bar(per_day, x="metric_date", y="views", title="Daily View Increments"), use_container_width=True)
    if not retention_buckets.empty:
        _render_retention(retention_buckets, st.session_state.get("demo_video_range", "Last quarter"), selected_video)

if not daily_channel.empty:
    st.subheader("Growth Projections")
    st.caption("Directional projections use the last 30 simulated days through the fixed demo date.")
    horizons = {"30 days": 30, "90 days": 90, "1 year": 365}
    horizon_label = st.radio("Horizon", list(horizons), horizontal=True, key="demo_projection_horizon", label_visibility="collapsed")
    rates = projections.linear_daily_rates(daily_channel, lookback_days=30)
    totals = {
        "subscribers": int(latest["subscriber_count"]),
        "views": int(latest["view_count"]),
        "hours": int(daily_channel["estimated_minutes_watched"].sum() / 60),
    }
    projection = projections.project(totals, rates, horizons[horizon_label])
    projection_cols = st.columns(3)
    projection_cols[0].metric(f"Subscribers in {horizon_label}", f"{projection['projected_subscribers']:,}", f"+{projection['delta_subscribers']:,}")
    projection_cols[1].metric(f"Total Views in {horizon_label}", f"{projection['projected_views']:,}", f"+{projection['delta_views']:,}")
    projection_cols[2].metric(f"Watch Hours in {horizon_label}", f"{projection['projected_hours']:,}", f"+{projection['delta_hours']:,}")

st.subheader("Publishing Queue")
st.caption("Simulated planning signals rank unpublished concepts by timing, theme fit, and expected audience relevance.")
if publishing_queue.empty:
    render_empty_state("publishing queue")
else:
    queue_row = publishing_queue.iloc[0]
    queue_result = json.loads(queue_row["result_json"])
    ranked_items = queue_result.get("ranked_videos", [])
    st.caption(f"Planning snapshot {pd.to_datetime(queue_row['analyzed_at']).strftime('%b %d, %Y')} · {int(queue_row['videos_analyzed'])} concepts analyzed")
    if not ranked_items:
        render_empty_state("publishing queue")
    for item in ranked_items:
        score = max(0.0, min(float(item.get("relevance_score", 0)), 10.0))
        with st.container(border=True):
            item_left, item_right = st.columns([5, 1])
            with item_left:
                st.markdown(f"**#{item.get('rank', '?')} — {item.get('title', 'Untitled concept')}**")
                st.caption(f"Theme: {item.get('theme', 'General')} · simulated planning signal")
                st.markdown(f"<span style='color:gray;font-style:italic'>{html.escape(item.get('why_now', ''))}</span>", unsafe_allow_html=True)
            with item_right:
                st.metric("Relevance", f"{score:.1f}/10")
                st.progress(score / 10)

st.subheader("Release-Timing Impact")
st.caption("Compares simulated releases published near their recommended planning date using accumulated daily increments.")
if queue_history.empty or daily_videos.empty:
    render_empty_state("release-timing impact")
else:
    cohort = queue_history.copy()
    cohort["published_at"] = pd.to_datetime(cohort["published_at"]).dt.tz_localize(None)
    cohort["recommended_publish_date"] = pd.to_datetime(cohort["recommended_publish_date"]).dt.tz_localize(None)
    cohort["timing_hours"] = (cohort["published_at"] - cohort["recommended_publish_date"]).dt.total_seconds() / 3600.0
    cohort = cohort[cohort["timing_hours"].between(-72, 96)]
    cohort_metrics = daily_videos[daily_videos["video_id"].isin(cohort["video_id"])].groupby("video_id", as_index=False).agg(views=("views", "sum"), minutes=("estimated_minutes_watched", "sum"), subscribers=("subscribers_gained", "sum"))
    cohort = cohort.merge(cohort_metrics, on="video_id", how="left").fillna({"views": 0, "minutes": 0, "subscribers": 0})
    cohort["watch_hours"] = cohort["minutes"] / 60.0
    cohort["timing"] = cohort["timing_hours"].map(lambda hours: f"{hours:+.0f}h")
    cohort = cohort.sort_values("timing_hours")
    st.dataframe(cohort[["title", "timing", "theme", "relevance_score", "views", "watch_hours", "subscribers"]], use_container_width=True, hide_index=True)

st.caption(f"All report cutoffs are fixed at {DEMO_AS_OF:%B %d, %Y}; all displayed channel data is synthetic.")
