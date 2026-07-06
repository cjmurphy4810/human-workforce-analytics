"""Daily Analytics — channel-level daily and cumulative performance with month comparison."""
from __future__ import annotations

import sqlite3
from datetime import date

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from db import DB_PATH

st.set_page_config(page_title="Daily Analytics", layout="wide")

if not st.session_state.get("authenticated"):
    st.switch_page("app.py")
    st.stop()

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def _load_daily() -> pd.DataFrame:
    with sqlite3.connect(str(DB_PATH)) as conn:
        try:
            return pd.read_sql_query(
                "SELECT metric_date, views, estimated_minutes_watched, "
                "subscribers_gained, subscribers_lost "
                "FROM daily_channel_metrics ORDER BY metric_date",
                conn,
            )
        except Exception:
            return pd.DataFrame()


@st.cache_data(ttl=300)
def _load_video_daily() -> pd.DataFrame:
    with sqlite3.connect(str(DB_PATH)) as conn:
        try:
            return pd.read_sql_query(
                "SELECT d.metric_date, d.video_id, v.title, "
                "d.views, d.estimated_minutes_watched / 60.0 AS watch_hours, "
                "d.average_view_duration "
                "FROM daily_video_metrics d "
                "LEFT JOIN videos v ON d.video_id = v.video_id "
                "ORDER BY d.metric_date",
                conn,
            )
        except Exception:
            return pd.DataFrame()


@st.cache_data(ttl=300)
def _get_qual_ratio() -> float:
    """Qualifying ratio = (total video WH - ADVERTISING WH) / total video WH."""
    try:
        with sqlite3.connect(str(DB_PATH)) as conn:
            total = conn.execute(
                "SELECT SUM(d.estimated_minutes_watched/60.0) FROM daily_video_metrics d "
                "INNER JOIN (SELECT video_id, MAX(metric_date) AS ld "
                "FROM daily_video_metrics GROUP BY video_id) l "
                "ON d.video_id=l.video_id AND d.metric_date=l.ld"
            ).fetchone()[0] or 0.0
            adv = conn.execute(
                "SELECT SUM(d.estimated_minutes_watched/60.0) "
                "FROM video_traffic_source_metrics d "
                "INNER JOIN (SELECT video_id, MAX(metric_date) AS ld "
                "FROM video_traffic_source_metrics "
                "WHERE traffic_source_type='ADVERTISING' GROUP BY video_id) l "
                "ON d.video_id=l.video_id AND d.metric_date=l.ld "
                "WHERE d.traffic_source_type='ADVERTISING'"
            ).fetchone()[0] or 0.0
        return max(total - adv, 0.0) / max(total, 1.0)
    except Exception:
        return 1.0


# ---------------------------------------------------------------------------
# Prepare data
# ---------------------------------------------------------------------------

_raw_daily = _load_daily()
_raw_video = _load_video_daily()
qual_ratio = _get_qual_ratio()

st.header("Daily Analytics")

if _raw_daily.empty:
    st.info("No daily data yet. Run `python fetch_metrics.py` to populate.")
    st.stop()

daily = _raw_daily.copy()
daily["metric_date"] = pd.to_datetime(daily["metric_date"])
daily["watch_hours"] = daily["estimated_minutes_watched"] / 60.0
daily["qualifying_hours"] = daily["watch_hours"] * qual_ratio
daily["net_subs"] = daily["subscribers_gained"] - daily["subscribers_lost"]
# Channel avg view duration in seconds: (minutes × 60) / views
daily["avg_view_dur_sec"] = (
    daily["estimated_minutes_watched"] * 60.0 / daily["views"].clip(lower=1)
)
daily["year_month"] = daily["metric_date"].dt.to_period("M")
daily["day_of_month"] = daily["metric_date"].dt.day

if not _raw_video.empty:
    video = _raw_video.copy()
    video["metric_date"] = pd.to_datetime(video["metric_date"])
    video["year_month"] = video["metric_date"].dt.to_period("M")
    video["weighted_dur"] = video["average_view_duration"] * video["views"]
else:
    video = pd.DataFrame()

today = date.today()
current_month = pd.Period(today, "M")
all_months_sorted = sorted(daily["year_month"].unique(), reverse=True)
prior_months = [m for m in all_months_sorted if m < current_month]

st.caption(
    f"Qualifying hours = total watch hours × {qual_ratio * 100:.0f}% qualifying ratio "
    f"(total minus ADVERTISING traffic source)."
)

_COLORS = ["#4C78A8", "#F58518", "#54A24B", "#E45756", "#B279A2", "#9D755D", "#BAB0AC"]

# ---------------------------------------------------------------------------
# Section 1: Channel Daily Performance
# ---------------------------------------------------------------------------

st.subheader("Channel Performance by Day")

ctrl1, ctrl2 = st.columns([2, 4])
with ctrl1:
    view_mode = st.radio(
        "Display mode",
        ["Daily", "Cumulative", "Running Average"],
        horizontal=True,
        key="da_mode",
        help=(
            "Daily = raw day value · "
            "Cumulative = running sum from start of month · "
            "Running Average = cumulative ÷ days elapsed (avg-per-day through this point)"
        ),
    )
with ctrl2:
    prior_opts = [str(m) for m in prior_months]
    selected_prior = st.multiselect(
        "Compare prior months",
        options=prior_opts,
        default=[],
        key="da_prior",
        help="Current month is always shown. Select prior months to overlay for comparison.",
    )

selected_periods = [current_month] + [pd.Period(m, "M") for m in selected_prior]
compare_mode = len(selected_periods) > 1


def _build_chart(col: str, y_label: str, chart_title: str) -> go.Figure:
    fig = go.Figure()
    for i, period in enumerate(selected_periods):
        m = daily[daily["year_month"] == period].sort_values("metric_date").copy()
        if m.empty:
            continue
        color = _COLORS[i % len(_COLORS)]
        name = period.strftime("%b %Y")
        is_current = period == current_month
        n = len(m)

        if view_mode == "Daily":
            y = m[col].values
        elif view_mode == "Cumulative":
            y = m[col].cumsum().values
        else:  # Running Average = cumulative / days elapsed
            y = m[col].cumsum().values / np.arange(1, n + 1)

        x = m["day_of_month"].values if compare_mode else m["metric_date"].values

        if not compare_mode and view_mode == "Daily":
            fig.add_bar(x=x, y=y, name=name, marker_color=color)
        else:
            fig.add_scatter(
                x=x,
                y=y,
                name=name,
                mode="lines+markers",
                line=dict(
                    color=color,
                    width=3 if is_current else 2,
                    dash="solid" if is_current else "dot",
                ),
                marker=dict(size=5 if is_current else 4),
            )

    fig.update_layout(
        title=chart_title,
        xaxis_title="Day of Month" if compare_mode else "Date",
        yaxis_title=y_label,
        height=340,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


tab_v, tab_wh, tab_qh = st.tabs(["Views", "Total Watch Hours", "Qualifying Watch Hours"])
with tab_v:
    st.plotly_chart(
        _build_chart("views", "Views", "Views"), use_container_width=True
    )
with tab_wh:
    st.plotly_chart(
        _build_chart("watch_hours", "Hours", "Total Watch Hours"), use_container_width=True
    )
with tab_qh:
    st.plotly_chart(
        _build_chart("qualifying_hours", "Qualifying Hours", "Qualifying Watch Hours"),
        use_container_width=True,
    )

# MTD summary KPIs
mtd = daily[daily["year_month"] == current_month]
if not mtd.empty:
    st.markdown(f"**Month-to-Date — {current_month.strftime('%B %Y')}**")
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Views", f"{int(mtd['views'].sum()):,}", f"{mtd['views'].mean():,.0f}/day avg")
    k2.metric(
        "Watch Hours",
        f"{mtd['watch_hours'].sum():,.1f}",
        f"{mtd['watch_hours'].mean():,.1f}/day avg",
    )
    k3.metric(
        "Qualifying Hours",
        f"{mtd['qualifying_hours'].sum():,.1f}",
        f"{mtd['qualifying_hours'].mean():,.1f}/day avg",
    )
    k4.metric(
        "Net Subscribers",
        f"{int(mtd['net_subs'].sum()):+,}",
        f"{mtd['net_subs'].mean():+.1f}/day avg",
    )
    k5.metric("Days with Data", int(mtd["metric_date"].nunique()))

# ---------------------------------------------------------------------------
# Section 2: Average Watch Time
# ---------------------------------------------------------------------------

st.divider()
st.subheader("Average Watch Time")
st.caption(
    "By Video: weighted avg view duration per video for the period. "
    "By Day: daily channel-wide avg view duration with subscriber overlay."
)

earliest = all_months_sorted[-1] if all_months_sorted else current_month
_PERIOD_MAP: dict[str, tuple] = {
    "This Month":    (current_month, current_month),
    "Last Month":    (current_month - 1, current_month - 1),
    "Last 3 Months": (current_month - 3, current_month),
    "Last 6 Months": (current_month - 6, current_month),
    "All Time":      (earliest, current_month),
}

wt_c1, wt_c2 = st.columns([2, 2])
with wt_c1:
    wt_period = st.selectbox("Period", list(_PERIOD_MAP.keys()), key="da_wt_period")
with wt_c2:
    wt_view = st.radio(
        "View",
        ["By Video", "By Day"],
        horizontal=True,
        key="da_wt_view",
        help=(
            "By Video: each video's weighted avg view duration for the period. "
            "By Day: channel-level avg view duration per day with net subscriber overlay."
        ),
    )

p_start, p_end = _PERIOD_MAP[wt_period]
period_daily = daily[
    (daily["year_month"] >= p_start) & (daily["year_month"] <= p_end)
].sort_values("metric_date").copy()
period_daily["avg_view_dur_min"] = period_daily["avg_view_dur_sec"] / 60.0

if not video.empty:
    period_video = video[
        (video["year_month"] >= p_start) & (video["year_month"] <= p_end)
    ].copy()
else:
    period_video = pd.DataFrame()

# ---------- By Video ----------
if wt_view == "By Video":
    if period_video.empty:
        st.info("No per-video data available for this period.")
    else:
        pv_agg = (
            period_video
            .groupby(["video_id", "title"], dropna=False)
            .agg(
                total_views=("views", "sum"),
                total_watch_hours=("watch_hours", "sum"),
                weighted_dur_sum=("weighted_dur", "sum"),
            )
            .reset_index()
        )
        pv_agg["avg_view_dur_sec"] = (
            pv_agg["weighted_dur_sum"] / pv_agg["total_views"].clip(lower=1)
        )
        pv_agg["avg_view_dur_min"] = pv_agg["avg_view_dur_sec"] / 60.0
        pv_agg = pv_agg[pv_agg["total_views"] > 0].sort_values(
            "avg_view_dur_sec", ascending=False
        )
        pv_agg["short_title"] = (
            pv_agg["title"]
            .fillna(pv_agg["video_id"])
            .apply(lambda t: str(t)[:60] + "…" if len(str(t)) > 60 else str(t))
        )

        fig_bv = px.bar(
            pv_agg,
            x="avg_view_dur_min",
            y="short_title",
            orientation="h",
            color="total_views",
            color_continuous_scale="Blues",
            labels={
                "avg_view_dur_min": "Avg View Duration (min)",
                "short_title": "",
                "total_views": "Views",
            },
            title=f"Avg View Duration by Video — {wt_period}",
            hover_data={
                "total_views": True,
                "total_watch_hours": ":.1f",
                "avg_view_dur_sec": ":.0f",
            },
        )
        fig_bv.update_yaxes(autorange="reversed")
        fig_bv.update_layout(height=max(380, len(pv_agg) * 30))
        st.plotly_chart(fig_bv, use_container_width=True)

        total_v = pv_agg["total_views"].sum()
        w_avg_sec = (
            (pv_agg["avg_view_dur_sec"] * pv_agg["total_views"]).sum() / max(total_v, 1)
        )
        mins, secs = divmod(int(w_avg_sec), 60)
        st.caption(
            f"Weighted avg across {len(pv_agg)} videos: **{mins}:{secs:02d}** · "
            f"{int(total_v):,} views · {pv_agg['total_watch_hours'].sum():,.1f} watch hours"
        )

# ---------- By Day ----------
else:
    if period_daily.empty:
        st.info("No data for selected period.")
    else:
        period_daily["rolling_avg_min"] = (
            period_daily["avg_view_dur_min"].rolling(7, min_periods=1).mean()
        )
        period_daily["cum_views"] = period_daily["views"].cumsum()
        period_daily["cum_watch_hrs"] = period_daily["watch_hours"].cumsum()

        day_tab, cum_tab = st.tabs(["Daily View", "Cumulative View"])

        with day_tab:
            fig_day = go.Figure()
            fig_day.add_scatter(
                x=period_daily["metric_date"],
                y=period_daily["avg_view_dur_min"],
                name="Avg View Duration (min)",
                mode="lines+markers",
                line=dict(color="#4C78A8", width=2),
            )
            fig_day.add_scatter(
                x=period_daily["metric_date"],
                y=period_daily["rolling_avg_min"],
                name="7-day rolling avg",
                mode="lines",
                line=dict(color="#F58518", width=2, dash="dash"),
            )
            fig_day.add_bar(
                x=period_daily["metric_date"],
                y=period_daily["net_subs"],
                name="Net Subscribers",
                yaxis="y2",
                marker_color="rgba(84,162,75,0.4)",
            )
            fig_day.update_layout(
                title=f"Daily Avg View Duration & Net Subscribers — {wt_period}",
                xaxis_title="Date",
                yaxis=dict(title="Avg View Duration (min)"),
                yaxis2=dict(title="Net Subscribers", overlaying="y", side="right"),
                hovermode="x unified",
                height=420,
                legend=dict(
                    orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
                ),
            )
            st.plotly_chart(fig_day, use_container_width=True)

        with cum_tab:
            fig_cum = go.Figure()
            fig_cum.add_scatter(
                x=period_daily["metric_date"],
                y=period_daily["cum_views"],
                name="Cumulative Views",
                mode="lines",
                line=dict(color="#4C78A8", width=2),
            )
            fig_cum.add_scatter(
                x=period_daily["metric_date"],
                y=period_daily["cum_watch_hrs"],
                name="Cumulative Watch Hours",
                mode="lines",
                line=dict(color="#F58518", width=2),
                yaxis="y2",
            )
            fig_cum.update_layout(
                title=f"Cumulative Views & Watch Hours — {wt_period}",
                xaxis_title="Date",
                yaxis=dict(title="Cumulative Views"),
                yaxis2=dict(title="Cumulative Watch Hours", overlaying="y", side="right"),
                hovermode="x unified",
                height=380,
                legend=dict(
                    orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
                ),
            )
            st.plotly_chart(fig_cum, use_container_width=True)

        st.caption(
            "Avg View Duration = daily (estimated_minutes_watched × 60) ÷ daily views. "
            "Net Subscribers = subscribers gained − lost. "
            "Rising avg view duration alongside subscriber growth signals healthy organic momentum."
        )
