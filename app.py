"""Streamlit dashboard for Human Workforce podcast YouTube analytics."""

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

import html
import json

import projections
import retention

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
        try:
            return pd.read_sql_query(query, conn)
        except Exception:
            return pd.DataFrame()


RANGES = {
    "Last week": 7,
    "Last month": 30,
    "Last quarter": 90,
    "Last year": 365,
}

COUNTRY_NAMES = {
    "IN": "India", "US": "United States", "GB": "United Kingdom",
    "RO": "Romania", "CA": "Canada", "AU": "Australia",
    "PK": "Pakistan", "NG": "Nigeria", "DE": "Germany",
    "BR": "Brazil", "PH": "Philippines", "BD": "Bangladesh",
    "ID": "Indonesia", "MX": "Mexico", "FR": "France",
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
retention_buckets = load(
    "SELECT video_id, window_start, window_end, window_kind, views, "
    "retention_at_25, retention_at_75 FROM retention_buckets"
)
publishing_queue = load(
    "SELECT analyzed_at, videos_analyzed, news_stories_count, result_json "
    "FROM publishing_queue ORDER BY analyzed_at DESC LIMIT 1"
)
daily_geo = load(
    "SELECT metric_date, country_code, views, subscribers_gained, likes "
    "FROM daily_geo_metrics ORDER BY metric_date"
)
playlists_df = load(
    "SELECT playlist_id, title, item_count FROM playlists"
)
playlist_videos_df = load(
    "SELECT playlist_id, video_id FROM playlist_videos"
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


# --- Geographic Trends ---

if not daily_geo.empty:
    st.subheader("Geographic Trends")
    geo = daily_geo.copy()
    geo["country"] = geo["country_code"].map(lambda c: COUNTRY_NAMES.get(c, c))
    latest_geo_date = geo["metric_date"].max()
    geo = geo[geo["metric_date"] == latest_geo_date].sort_values("views", ascending=False)
    if geo.empty:
        st.info("No geographic data yet.")
    else:
        all_countries = geo["country"].tolist()
        selected_countries = st.multiselect(
            "Show regions",
            options=all_countries,
            default=all_countries,
            key="geo_filter",
        )
        geo_filtered = geo[geo["country"].isin(selected_countries)] if selected_countries else geo
        fig = make_subplots(
            rows=1, cols=3,
            subplot_titles=("Views", "Subscribers Gained", "Likes"),
            shared_yaxes=True,
        )
        colors = px.colors.qualitative.Plotly
        fig.add_bar(x=geo_filtered["views"], y=geo_filtered["country"], orientation="h",
                    name="Views", marker_color=colors[0], row=1, col=1)
        fig.add_bar(x=geo_filtered["subscribers_gained"], y=geo_filtered["country"], orientation="h",
                    name="Subscribers", marker_color=colors[1], row=1, col=2)
        fig.add_bar(x=geo_filtered["likes"], y=geo_filtered["country"], orientation="h",
                    name="Likes", marker_color=colors[2], row=1, col=3)
        fig.update_layout(height=max(320, len(geo_filtered) * 36), showlegend=False)
        fig.update_yaxes(autorange="reversed")
        st.plotly_chart(fig, use_container_width=True)
        st.caption(f"Snapshot: {latest_geo_date} · covers the preceding 90 days")
else:
    st.subheader("Geographic Trends")
    st.info("Geographic data still loading. Run `python fetch_metrics.py` to populate.")


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


# --- Playlists ---

st.subheader("Playlists")
st.caption("Cumulative performance of the videos contained in each playlist.")

if playlist_videos_df.empty or playlists_df.empty:
    st.info("No playlist data yet. Run `python fetch_metrics.py` to populate.")
else:
    # Latest snapshot per video
    latest_vs = (
        video_snapshots.sort_values("captured_at")
        .groupby("video_id", as_index=False).last()
    )[["video_id", "view_count", "like_count"]]

    # Latest 90-day watch minutes per video
    latest_dvm = (
        daily_videos.sort_values("metric_date")
        .groupby("video_id", as_index=False).last()
    )[["video_id", "estimated_minutes_watched"]]

    pv = playlist_videos_df.merge(latest_vs, on="video_id", how="left")
    pv = pv.merge(latest_dvm, on="video_id", how="left")
    pv["hours_watched"] = pv["estimated_minutes_watched"].fillna(0) / 60

    pl_agg = pv.groupby("playlist_id", as_index=False).agg(
        views=("view_count", "sum"),
        hours_watched=("hours_watched", "sum"),
        likes=("like_count", "sum"),
    )
    pl = pl_agg.merge(playlists_df, on="playlist_id", how="right").fillna(0)
    pl = pl.sort_values("views", ascending=False)

    # Deduplicate for channel-wide totals — a video in N playlists should count once
    unique_videos = pv.drop_duplicates("video_id")
    total_views = int(unique_videos["view_count"].fillna(0).sum())
    total_hours = unique_videos["hours_watched"].sum()

    pm1, pm2, pm3 = st.columns(3)
    pm1.metric("Total Views (unique videos)", f"{total_views:,}")
    pm2.metric("Watch Hours (unique videos, 90d)", f"{total_hours:,.1f}")
    pm3.metric("Playlists", f"{len(pl):,}")

    # Catch-all / operational playlists that dwarf the others — excluded by default
    _DEFAULT_EXCLUDE = {"All Podcast Videos", "MultiLanguageTitleDescriptionVideo", "The Human Workforce Podcast Series"}
    all_titles = pl["title"].tolist()
    default_selection = [t for t in all_titles if t not in _DEFAULT_EXCLUDE]

    selected = st.multiselect(
        "Show playlists",
        options=all_titles,
        default=default_selection,
        key="playlist_filter",
    )

    pl_filtered = pl[pl["title"].isin(selected)] if selected else pl

    pc1, pc2 = st.columns(2)
    with pc1:
        fig = px.bar(
            pl_filtered,
            x="views",
            y="title",
            orientation="h",
            title="Views per Playlist",
            labels={"views": "Views", "title": ""},
            height=max(300, len(pl_filtered) * 36),
        )
        fig.update_yaxes(autorange="reversed", tickfont=dict(size=11))
        fig.update_layout(margin=dict(l=0))
        st.plotly_chart(fig, use_container_width=True)
    with pc2:
        fig = px.bar(
            pl_filtered,
            x="hours_watched",
            y="title",
            orientation="h",
            title="Watch Hours per Playlist (90d)",
            labels={"hours_watched": "Hours", "title": ""},
            height=max(300, len(pl_filtered) * 36),
        )
        fig.update_yaxes(autorange="reversed", tickfont=dict(size=11))
        fig.update_layout(margin=dict(l=0))
        st.plotly_chart(fig, use_container_width=True)

    display_cols = ["title", "item_count", "views", "hours_watched", "likes"]
    rename_map = {
        "title": "Playlist",
        "item_count": "Videos",
        "views": "Views",
        "hours_watched": "Hours Watched",
        "likes": "Likes",
    }
    display_df = pl_filtered[display_cols].rename(columns=rename_map)
    display_df["Hours Watched"] = display_df["Hours Watched"].round(1)
    display_df["Views"] = display_df["Views"].astype(int)
    display_df["Likes"] = display_df["Likes"].astype(int)
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    st.caption("Views and hours reflect cumulative totals of videos in each playlist. A video in multiple playlists is counted in each.")


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


def render_retention(rb_full, toggle, today, video_id=None):
    """Render the retention summary line + KPI cards for either channel-wide or per-video."""
    _, _, rolling_days = retention.window_bounds_for_toggle(toggle, today)
    rolling_kind = f"rolling{rolling_days}"

    rb = rb_full[rb_full["window_kind"] == rolling_kind].copy()
    if video_id:
        rb = rb[rb["video_id"] == video_id]
    rb["window_start"] = pd.to_datetime(rb["window_start"]).dt.date
    rb["window_end"] = pd.to_datetime(rb["window_end"]).dt.date
    # Take the latest snapshot per video — server TZ may be a day ahead of when
    # the fetch wrote window_end, so strict equality on `today` drops everything.
    rb = rb.sort_values("window_end").groupby("video_id", as_index=False).tail(1)

    snap = retention.aggregate_snapshot(rb)

    if snap["total_views"] == 0:
        st.info("No retention data for this range yet.")
        return

    st.markdown(
        f"<div style='font-size:1.05rem; margin:0.25rem 0 0.75rem;'>"
        f"<span style='color:#E45756; font-weight:700;'>{snap['b1_pct']*100:.1f}%</span> "
        f"dropped early &nbsp;·&nbsp; "
        f"<span style='color:#F2B701; font-weight:700;'>{snap['b2_pct']*100:.1f}%</span> "
        f"mid-watch &nbsp;·&nbsp; "
        f"<span style='color:#54A24B; font-weight:700;'>{snap['b3_pct']*100:.1f}%</span> "
        f"stuck around &nbsp;<span style='color:#888;'>({snap['total_views']:,} views)</span>"
        f"</div>",
        unsafe_allow_html=True,
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("Dropped early (0–25%)",
              f"{int(snap['b1_count']):,} views",
              f"{snap['b1_pct'] * 100:.1f}%", delta_color="off")
    c2.metric("Mid-watch (25–75%)",
              f"{int(snap['b2_count']):,} views",
              f"{snap['b2_pct'] * 100:.1f}%", delta_color="off")
    c3.metric("Stuck around (75–100%)",
              f"{int(snap['b3_count']):,} views",
              f"{snap['b3_pct'] * 100:.1f}%", delta_color="off")


# --- Audience retention ---

if not retention_buckets.empty:
    st.subheader("Audience Retention")
    st.caption(
        "Where viewers drop off as a share of each video's length, "
        "aggregated across all videos."
    )
    toggle = st.radio(
        "Range",
        list(RANGES.keys()),
        index=2,
        horizontal=True,
        key="retention_range",
        label_visibility="collapsed",
    )
    render_retention(retention_buckets, toggle, date.today())
else:
    st.subheader("Audience Retention")
    st.info("Retention data still loading. Run `python fetch_metrics.py` to populate.")


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

        if not retention_buckets.empty:
            st.markdown("**Retention buckets for this video**")
            v_toggle = st.session_state.get("video_range", "Last quarter")
            render_retention(
                retention_buckets, v_toggle, date.today(),
                video_id=selected,
            )


# --- Growth projections ---

if not daily_channel.empty:
    st.subheader("Growth Projections")
    st.caption(
        "Linear extrapolation of the last 30 days' average daily pace. "
        "Watch hours start from the cumulative total within the fetch window, "
        "not lifetime — view as directional, not actuarial."
    )

    HORIZONS = {"30 days": 30, "90 days": 90, "1 year": 365}
    horizon_label = st.radio(
        "Horizon",
        list(HORIZONS.keys()),
        index=0,
        horizontal=True,
        key="projection_horizon",
        label_visibility="collapsed",
    )
    horizon_days = HORIZONS[horizon_label]

    rates = projections.linear_daily_rates(daily_channel, lookback_days=30)
    current_totals = {
        "subscribers": int(latest["subscriber_count"]),
        "views": int(latest["view_count"]),
        "hours": int(daily_channel["estimated_minutes_watched"].sum() / 60),
    }
    p = projections.project(current_totals, rates, horizon_days)

    p1, p2, p3 = st.columns(3)
    p1.metric(
        f"Subscribers in {horizon_label}",
        f"{p['projected_subscribers']:,}",
        f"+{p['delta_subscribers']:,} ({rates['net_subs_per_day']:.1f}/day)",
    )
    p2.metric(
        f"Total Views in {horizon_label}",
        f"{p['projected_views']:,}",
        f"+{p['delta_views']:,} ({int(rates['views_per_day']):,}/day)",
    )
    p3.metric(
        f"Watch Hours in {horizon_label}",
        f"{p['projected_hours']:,}",
        f"+{p['delta_hours']:,} ({rates['hours_per_day']:.1f}/day)",
    )


# --- Publishing Queue ---

st.subheader("Publishing Queue")
st.caption(
    "Unpublished episodes ranked by relevance to today's top news stories. "
    "Updated every 4 hours. Use this to decide which story to schedule in YouTube Studio."
)

if publishing_queue.empty:
    st.info(
        "No publishing queue data yet. "
        "Set ANTHROPIC_API_KEY and NEWS_API_KEY, then run `python fetch_metrics.py`."
    )
else:
    pq = publishing_queue.iloc[0]
    analyzed_at = pd.to_datetime(pq["analyzed_at"]).tz_localize(None)
    hours_ago = (pd.Timestamp.utcnow().tz_localize(None) - analyzed_at).total_seconds() / 3600

    meta_col, warn_col = st.columns([4, 1])
    with meta_col:
        st.caption(
            f"Analyzed {analyzed_at.strftime('%b %d, %H:%M UTC')} · "
            f"{int(pq['videos_analyzed'])} unpublished videos · "
            f"{int(pq['news_stories_count'])} news stories"
        )
    with warn_col:
        if hours_ago > 8:
            st.warning(f"⚠ {hours_ago:.0f}h stale")

    result = json.loads(pq["result_json"])
    ranked = result.get("ranked_videos", [])

    if not result.get("news_available"):
        st.warning("News headlines unavailable — videos shown by theme only, not ranked by current events.")

    if not ranked:
        st.info("No unpublished videos in queue.")
    else:
        today = pd.Timestamp.utcnow().date()
        for item in ranked:
            rank = item.get("rank", "?")
            raw_score = item.get("relevance_score", 0)
            try:
                score = float(raw_score)
            except (TypeError, ValueError):
                score = 0.0
            score = max(0.0, min(10.0, score))

            scheduled_raw = item.get("scheduled_at")
            if scheduled_raw:
                scheduled_str = pd.to_datetime(scheduled_raw).strftime("%b %d, %Y")
            else:
                scheduled_str = "Not scheduled"

            try:
                rec_date = today + timedelta(days=int(rank))
                rec_str = rec_date.strftime("%b %d, %Y")
            except (TypeError, ValueError):
                rec_date = None
                rec_str = "—"

            show_earlier = (
                scheduled_raw is not None
                and rec_date is not None
                and rec_date < pd.to_datetime(scheduled_raw).date()
            )

            with st.container(border=True):
                left, right = st.columns([5, 1])
                with left:
                    st.markdown(f"**#{rank} — {item.get('title', 'Untitled')}**")
                    st.caption(f"🏷 {item.get('theme', '')}")
                    date_line = f"📅 Scheduled: {scheduled_str} → Recommend: {rec_str}"
                    if show_earlier:
                        date_line += " ⚡ Earlier"
                    st.caption(date_line)
                    st.markdown(f"<span style='color:gray; font-style:italic;'>{html.escape(item.get('why_now', ''))}</span>", unsafe_allow_html=True)
                with right:
                    st.metric("Relevance", f"{score:.0f}/10")
                    st.progress(score / 10)

    headlines = result.get("news_headlines", [])
    if headlines:
        with st.expander("News headlines used"):
            for h in headlines:
                st.markdown(f"- **{h['title']}** — {h.get('source', '')}")
