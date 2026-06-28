"""
Qualifying Watch Hours dashboard page.

Answers: "For every dollar spent promoting a video, how many qualifying watch hours
did we actually create?"

Qualifying hours = Total Watch Hours - Promotion Watch Hours.
Promotion Watch Hours = Promotion Views × Avg Promotion View Duration.
"""
from __future__ import annotations

import io
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from analytics.promotion_efficiency import compute_efficiency_scores
from analytics.qualifying_hours import compute_qualifying_hours, recompute_with_sim_duration
from models.promotion import VideoPromotionMetrics, make_metrics
from services.google_ads import GoogleAdsCSVAdapter


# ---------------------------------------------------------------------------
# Demo data
# ---------------------------------------------------------------------------

_DEMO_SEED = 42
_DEMO_VIDEOS: list[tuple[str, str, int, str, str, str]] = [
    ("d001", "AI Is Replacing White Collar Jobs Faster Than You Think", 2847, "AI Jobs Series", "Workforce AI", "Career"),
    ("d002", "The Hidden Cost of Corporate Loyalty", 3214, "Corporate Culture", "Workforce AI", "Leadership"),
    ("d003", "Why Your LinkedIn Profile Is Lying To Employers", 1892, "", "", "Career"),
    ("d004", "The Great Layoff: What No One Is Telling You", 3478, "Layoff Series", "Workforce AI", "Employment"),
    ("d005", "Negotiating Your Salary in a Recession", 2634, "Salary Series", "Workforce AI", "Career"),
    ("d006", "The Future of Remote Work in 2026", 2156, "Remote Work Series", "", "Workforce"),
    ("d007", "Side Hustles That Actually Scale Into Businesses", 2089, "", "", "Entrepreneurship"),
    ("d008", "HR Secrets: What They Won't Say in Your Interview", 2478, "HR Insider Series", "HR Campaign", "Career"),
    ("d009", "The Skills Gap Nobody Is Talking About", 2912, "", "", "Workforce"),
    ("d010", "Why Quiet Quitting Misses the Point", 1645, "", "", "Culture"),
    ("d011", "Corporate Burnout: Data Causes and Solutions", 2341, "Wellness Series", "Workforce AI", "Wellness"),
    ("d012", "The Rise of AI Managers and What It Means for You", 3124, "AI Jobs Series", "Workforce AI", "AI"),
    ("d013", "From Employee to Entrepreneur: A Realistic Guide", 2789, "", "", "Entrepreneurship"),
    ("d014", "The Automation Paradox: More Jobs or Fewer?", 2956, "AI Jobs Series", "Workforce AI", "AI"),
    ("d015", "What the Best Bosses Do Differently", 1823, "", "", "Leadership"),
    ("d016", "Gen Z vs Millennials at Work: The Real Differences", 2234, "Generational Series", "HR Campaign", "Culture"),
    ("d017", "Recession-Proof Careers: The Data-Backed List", 2678, "Salary Series", "Workforce AI", "Career"),
    ("d018", "How to Get Promoted Without Playing Office Politics", 2112, "", "", "Career"),
]

_PROMO_SLOTS: set[str] = {
    "d001", "d002", "d004", "d005", "d006",
    "d008", "d009", "d011", "d012", "d014", "d016", "d017",
}

_BASE_DATE = datetime(2025, 7, 1)


def _build_demo_metrics() -> list[VideoPromotionMetrics]:
    rng = random.Random(_DEMO_SEED)

    metrics: list[VideoPromotionMetrics] = []
    for i, (vid_id, title, length_sec, campaign, series, playlist) in enumerate(_DEMO_VIDEOS):
        published = _BASE_DATE + timedelta(days=i * 14 + rng.randint(-3, 3))
        days_live = max((datetime.now() - published).days, 1)

        has_promo = vid_id in _PROMO_SLOTS
        base_views = rng.randint(3_000, 25_000) + int(days_live * rng.uniform(2, 8))

        if has_promo:
            promo_pct = rng.uniform(0.28, 0.62)
            promo_views = int(base_views * promo_pct)
            # Paid-discovery viewers watch 28–48% of episode before dropping off
            promo_duration = rng.uniform(0.28, 0.48) * length_sec
            promo_cost = rng.uniform(80, 480)
            ctr = rng.uniform(0.6, 4.2)
        else:
            promo_views = 0
            promo_duration = 0.0
            promo_cost = 0.0
            ctr = 0.0

        organic_views = base_views - promo_views
        avg_organic_dur = rng.uniform(0.42, 0.68) * length_sec
        total_wh = (
            organic_views * avg_organic_dur / 3600
            + promo_views * (promo_duration if promo_duration > 0 else avg_organic_dur) / 3600
        )

        subscribers = int(promo_views * rng.uniform(0.004, 0.018)) if has_promo else rng.randint(0, 15)
        follow_on_views = int(organic_views * rng.uniform(0.08, 0.28))

        metrics.append(make_metrics(
            video_id=vid_id,
            title=title,
            published=published,
            length_seconds=length_sec,
            total_views=base_views,
            promotion_views=promo_views,
            total_watch_hours=total_wh,
            avg_promotion_view_duration_seconds=promo_duration,
            promotion_cost=promo_cost,
            subscribers=subscribers,
            follow_on_views=follow_on_views,
            avg_view_duration_seconds=avg_organic_dur,
            ctr=ctr,
            status="active",
            campaign=campaign,
            country="US",
            language="en",
            playlist=playlist,
            series=series,
            promotion_duration_estimated=promo_duration == 0 and has_promo,
        ))

    return compute_efficiency_scores(metrics)


# ---------------------------------------------------------------------------
# Time-series helper (synthetic cohort rollup for demo mode)
# ---------------------------------------------------------------------------

def _build_timeseries(metrics: list[VideoPromotionMetrics]) -> pd.DataFrame:
    """Generate a weekly time-series by distributing each video's hours across its life."""
    rows = []
    for m in metrics:
        if m.published is None:
            continue
        weeks = max(int((datetime.now() - m.published).days / 7), 1)
        # View velocity decays over time (front-loaded)
        weights = [max(1.0 / (w + 1), 0.02) for w in range(weeks)]
        total_w = sum(weights)
        for w_idx, weight in enumerate(weights):
            frac = weight / total_w
            week_date = m.published + timedelta(weeks=w_idx)
            rows.append({
                "week": week_date.date(),
                "organic_hours": m.organic_watch_hours * frac,
                "promotion_hours": m.promotion_watch_hours * frac,
                "qualifying_hours": m.estimated_qualifying_hours * frac,
            })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    return (
        df.groupby("week")
        .sum()
        .reset_index()
        .sort_values("week")
    )


# ---------------------------------------------------------------------------
# Metrics → DataFrame helper
# ---------------------------------------------------------------------------

def _to_df(metrics: list[VideoPromotionMetrics]) -> pd.DataFrame:
    records = []
    for m in metrics:
        records.append({
            "video_id": m.video_id,
            "Video": m.title,
            "Published": m.published.strftime("%Y-%m-%d") if m.published else "",
            "Length": _fmt_duration(m.length_seconds),
            "Total Views": m.total_views,
            "Organic Views": m.organic_views,
            "Promotion Views": m.promotion_views,
            "Total Watch Hours": round(m.total_watch_hours, 1),
            "Promotion Watch Hours": round(m.promotion_watch_hours, 1),
            "Est. Qualifying Hours": round(m.estimated_qualifying_hours, 1),
            "Promotion %": round(m.promotion_percentage, 1),
            "Subscribers": m.subscribers,
            "Follow-on Views": m.follow_on_views,
            "Promotion Cost": round(m.promotion_cost, 2),
            "Cost / Organic Hour": round(m.cost_per_qualified_hour, 2),
            "Cost / Subscriber": round(m.cost_per_subscriber, 2),
            "Cost / Follow-on View": round(m.cost_per_follow_on_view, 2),
            "CTR %": round(m.ctr, 2),
            "Avg View Duration": _fmt_duration(int(m.avg_view_duration_seconds)),
            "Watch Time / View": _fmt_duration(
                int(m.total_watch_hours * 3600 / max(m.total_views, 1))
            ),
            "Efficiency Score": round(m.promotion_efficiency_score, 1),
            "Status": m.status,
            "campaign": m.campaign,
            "playlist": m.playlist,
            "series": m.series,
            "language": m.language,
        })
    return pd.DataFrame(records)


def _fmt_duration(seconds: int) -> str:
    if seconds <= 0:
        return "0:00"
    h, remainder = divmod(seconds, 3600)
    m, s = divmod(remainder, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


# ---------------------------------------------------------------------------
# Filter helpers
# ---------------------------------------------------------------------------

def _apply_filters(
    metrics: list[VideoPromotionMetrics],
    date_start: Optional[datetime],
    date_end: Optional[datetime],
    campaigns: list[str],
    playlists: list[str],
    series_list: list[str],
    languages: list[str],
    promo_status: str,
) -> list[VideoPromotionMetrics]:
    out = []
    for m in metrics:
        if date_start and m.published and m.published < date_start:
            continue
        if date_end and m.published and m.published > date_end:
            continue
        if campaigns and m.campaign not in campaigns:
            continue
        if playlists and m.playlist not in playlists:
            continue
        if series_list and m.series not in series_list:
            continue
        if languages and m.language not in languages:
            continue
        if promo_status == "Promoted only" and m.promotion_views == 0:
            continue
        if promo_status == "Organic only" and m.promotion_views > 0:
            continue
        out.append(m)
    return out


# ---------------------------------------------------------------------------
# CSV loader
# ---------------------------------------------------------------------------

def _try_load_csv(uploaded) -> Optional[list[VideoPromotionMetrics]]:
    """Attempt to parse an uploaded promotion CSV and return metrics or None."""
    try:
        adapter = GoogleAdsCSVAdapter()
        adapter.load_from_buffer(io.StringIO(uploaded.read().decode("utf-8")))
        df = adapter.fetch_video_promotion_stats([], ("2000-01-01", "2099-12-31"))
        if df.empty:
            return None
        out = []
        for _, row in df.iterrows():
            out.append(make_metrics(
                video_id=str(row.get("video_id", "")),
                title=str(row.get("title", row.get("video_id", ""))),
                published=None,
                length_seconds=int(row.get("duration_seconds", 0)),
                total_views=int(row.get("total_views", row.get("views", 0))),
                promotion_views=int(row.get("views", 0)),
                total_watch_hours=float(row.get("total_watch_hours", 0)),
                avg_promotion_view_duration_seconds=float(row.get("avg_promo_view_duration", 30)),
                promotion_cost=float(row.get("cost_usd", 0)),
                subscribers=int(row.get("subscribers_gained", 0)),
                follow_on_views=int(row.get("follow_on_views", 0)),
                avg_view_duration_seconds=float(row.get("average_view_duration", 0)),
                ctr=float(row.get("ctr", 0)),
                campaign=str(row.get("campaign", "")),
                promotion_duration_estimated=True,
            ))
        return compute_efficiency_scores(out)
    except Exception as exc:
        st.error(f"Could not parse promotion CSV: {exc}")
        return None


# ---------------------------------------------------------------------------
# Chart renderers
# ---------------------------------------------------------------------------

def _chart_stacked_area(ts: pd.DataFrame) -> None:
    if ts.empty:
        st.info("No time-series data available.")
        return
    fig = go.Figure()
    fig.add_scatter(
        x=ts["week"], y=ts["organic_hours"],
        name="Organic Watch Hours",
        fill="tozeroy",
        mode="lines",
        line=dict(color="#54A24B", width=2),
        fillcolor="rgba(84,162,75,0.35)",
    )
    fig.add_scatter(
        x=ts["week"], y=ts["promotion_hours"],
        name="Promotion Watch Hours",
        fill="tozeroy",
        mode="lines",
        line=dict(color="#E45756", width=2),
        fillcolor="rgba(228,87,86,0.25)",
    )
    fig.update_layout(
        title="Watch Hours Over Time: Organic vs Promotion",
        xaxis_title="Week",
        yaxis_title="Hours",
        height=380,
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)


def _chart_qualifying_line(ts: pd.DataFrame) -> None:
    if ts.empty:
        st.info("No time-series data available.")
        return
    ts = ts.copy()
    ts["cumulative_qualifying"] = ts["qualifying_hours"].cumsum()
    fig = go.Figure()
    fig.add_scatter(
        x=ts["week"], y=ts["qualifying_hours"],
        name="Weekly Qualifying Hours",
        mode="lines+markers",
        line=dict(color="#4C78A8", width=2),
    )
    fig.add_scatter(
        x=ts["week"], y=ts["cumulative_qualifying"],
        name="Cumulative Qualifying Hours",
        mode="lines",
        line=dict(color="#F58518", width=2, dash="dot"),
        yaxis="y2",
    )
    fig.update_layout(
        title="Estimated Qualifying Hours Over Time",
        xaxis_title="Week",
        yaxis=dict(title="Weekly Hours"),
        yaxis2=dict(title="Cumulative Hours", overlaying="y", side="right"),
        height=380,
        hovermode="x unified",
        legend=dict(orientation="h", y=1.02),
    )
    st.plotly_chart(fig, use_container_width=True)


def _chart_scatter_cost_vs_hours(df: pd.DataFrame) -> None:
    promo = df[df["Promotion Cost"] > 0].copy()
    if promo.empty:
        st.info("No promoted videos in current filter.")
        return
    fig = px.scatter(
        promo,
        x="Promotion Cost",
        y="Est. Qualifying Hours",
        size="Total Views",
        color="Efficiency Score",
        color_continuous_scale="RdYlGn",
        hover_name="Video",
        hover_data=["Promotion %", "Cost / Organic Hour"],
        labels={
            "Promotion Cost": "Promotion Cost ($)",
            "Est. Qualifying Hours": "Est. Qualifying Hours",
        },
        title="Promotion Cost vs Estimated Qualifying Hours",
    )
    fig.update_layout(height=420)
    st.plotly_chart(fig, use_container_width=True)


def _chart_bubble(df: pd.DataFrame) -> None:
    promo = df[df["Promotion Cost"] > 0].copy()
    if promo.empty:
        st.info("No promoted videos in current filter.")
        return
    fig = px.scatter(
        promo,
        x="Promotion Cost",
        y="Est. Qualifying Hours",
        size="Follow-on Views",
        color="Subscribers",
        color_continuous_scale="Blues",
        hover_name="Video",
        size_max=60,
        labels={
            "Promotion Cost": "Promotion Cost ($)",
            "Est. Qualifying Hours": "Organic Watch Hours",
            "Follow-on Views": "Bubble = Follow-on Views",
        },
        title="Promotion Cost · Organic Hours · Subscribers (Bubble = Follow-on Views)",
    )
    fig.update_layout(height=440)
    st.plotly_chart(fig, use_container_width=True)


def _chart_top25_qualifying(df: pd.DataFrame) -> None:
    top = df.nlargest(25, "Est. Qualifying Hours")[["Video", "Est. Qualifying Hours", "Efficiency Score"]].copy()
    top["short_title"] = top["Video"].apply(lambda t: t[:50] + "…" if len(t) > 50 else t)
    fig = px.bar(
        top,
        x="Est. Qualifying Hours",
        y="short_title",
        orientation="h",
        color="Efficiency Score",
        color_continuous_scale="RdYlGn",
        labels={"short_title": "", "Est. Qualifying Hours": "Est. Qualifying Hours"},
        title="Top 25 Videos by Estimated Qualifying Hours",
    )
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(height=max(360, len(top) * 28))
    st.plotly_chart(fig, use_container_width=True)


def _chart_worst_promotions(df: pd.DataFrame) -> None:
    promo = df[(df["Promotion Cost"] > 0) & (df["Cost / Organic Hour"] > 0)].copy()
    if promo.empty:
        st.info("No promoted videos with cost data in current filter.")
        return
    worst = promo.nlargest(15, "Cost / Organic Hour")[["Video", "Cost / Organic Hour", "Promotion Cost", "Est. Qualifying Hours"]].copy()
    worst["short_title"] = worst["Video"].apply(lambda t: t[:50] + "…" if len(t) > 50 else t)
    fig = px.bar(
        worst,
        x="Cost / Organic Hour",
        y="short_title",
        orientation="h",
        color="Cost / Organic Hour",
        color_continuous_scale="Reds",
        labels={"short_title": "", "Cost / Organic Hour": "Cost per Qualifying Hour ($)"},
        title="Worst Promotions: Highest Cost per Qualifying Hour",
    )
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(height=max(320, len(worst) * 28))
    st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# Promotion impact panel
# ---------------------------------------------------------------------------

def _render_promotion_impact(metrics: list[VideoPromotionMetrics]) -> None:
    st.subheader("Promotion Impact Panel")
    promoted = [m for m in metrics if m.promotion_views > 0]
    if not promoted:
        st.info("No promoted videos in current selection.")
        return

    promoted_sorted = sorted(promoted, key=lambda m: m.estimated_qualifying_hours, reverse=True)
    for m in promoted_sorted:
        with st.expander(f"**{m.title[:70]}**", expanded=False):
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Promotion Views", f"{m.promotion_views:,}")
            c2.metric("Promotion Cost", f"${m.promotion_cost:,.0f}")
            c3.metric("Subscribers (Promo)", f"{m.subscribers:,}")
            c4.metric("Follow-on Views", f"{m.follow_on_views:,}")

            c5, c6, c7, c8 = st.columns(4)
            c5.metric("Organic Views After", f"{m.organic_views:,}")
            c6.metric("Organic Watch Hours", f"{m.organic_watch_hours:,.1f}")
            c7.metric("Est. Qualifying Hours", f"{m.estimated_qualifying_hours:,.1f}")
            c8.metric("Promotion Watch Hours", f"{m.promotion_watch_hours:,.1f}")

            c9, c10, c11 = st.columns(3)
            c9.metric("Cost / Qualifying Hour", f"${m.cost_per_qualified_hour:,.2f}")
            c10.metric("Cost / Subscriber", f"${m.cost_per_subscriber:,.2f}" if m.cost_per_subscriber > 0 else "—")
            c11.metric("Efficiency Score", f"{m.promotion_efficiency_score:.0f}/100")

            net = m.organic_watch_hours - m.promotion_watch_hours
            delta_color = "normal" if net >= 0 else "inverse"
            st.metric(
                "Net Gain (Organic Hours − Promo Hours)",
                f"{net:+.1f} hours",
                delta_color=delta_color,
            )
            if m.promotion_duration_estimated:
                st.caption(
                    "⚠ Promotion watch hours estimated using overall average view duration "
                    "because per-promotion duration was unavailable."
                )


# ---------------------------------------------------------------------------
# Main render function
# ---------------------------------------------------------------------------

def render(db_path: Path) -> None:
    st.header("Qualifying Watch Hours")
    st.caption(
        "Estimates YouTube Partner Program qualifying watch hours by removing "
        "promotion-generated watch time from total watch time."
    )

    # --- Sidebar: data source & filters ---
    with st.sidebar:
        st.markdown("---")
        st.markdown("**Data Source**")
        source_mode = st.radio(
            "data_source",
            ["Demo Data", "Upload Promotion CSV"],
            label_visibility="collapsed",
            key="qwh_source",
        )

        uploaded_file = None
        if source_mode == "Upload Promotion CSV":
            uploaded_file = st.file_uploader(
                "Promotion CSV",
                type=["csv"],
                key="qwh_csv",
                help="Expected columns: video_id, campaign, cost_usd, views (paid). See services/google_ads.py for full schema.",
            )

        st.markdown("---")
        st.markdown("**Filters**")

    # --- Load base metrics ---
    if source_mode == "Upload Promotion CSV" and uploaded_file is not None:
        base_metrics = _try_load_csv(uploaded_file) or _build_demo_metrics()
        is_demo = uploaded_file is None
    else:
        base_metrics = _build_demo_metrics()
        is_demo = True

    if is_demo:
        st.info(
            "**Demo Mode** — showing synthetic data to illustrate all calculations. "
            "Upload a Promotion CSV in the sidebar to use real data.",
            icon="🧪",
        )

    # --- Sidebar filters (derived from actual metrics) ---
    all_campaigns = sorted({m.campaign for m in base_metrics if m.campaign})
    all_playlists = sorted({m.playlist for m in base_metrics if m.playlist})
    all_series = sorted({m.series for m in base_metrics if m.series})
    all_languages = sorted({m.language for m in base_metrics if m.language})

    published_dates = [m.published for m in base_metrics if m.published]
    min_date = min(published_dates).date() if published_dates else None
    max_date = max(published_dates).date() if published_dates else None

    with st.sidebar:
        date_start = st.date_input("Published from", value=min_date, key="qwh_date_start")
        date_end = st.date_input("Published to", value=max_date, key="qwh_date_end")

        sel_campaigns = st.multiselect("Campaign", all_campaigns, default=all_campaigns, key="qwh_campaigns")
        sel_playlists = st.multiselect("Playlist", all_playlists, default=all_playlists, key="qwh_playlists")
        sel_series = st.multiselect("Series", all_series, default=all_series, key="qwh_series")
        sel_languages = st.multiselect("Language", all_languages, default=all_languages, key="qwh_languages")
        promo_status = st.selectbox(
            "Promotion Status",
            ["All", "Promoted only", "Organic only"],
            key="qwh_promo_status",
        )

    # Apply filters
    start_dt = datetime.combine(date_start, datetime.min.time()) if date_start else None
    end_dt = datetime.combine(date_end, datetime.max.time()) if date_end else None
    metrics = _apply_filters(
        base_metrics,
        date_start=start_dt,
        date_end=end_dt,
        campaigns=sel_campaigns if sel_campaigns != all_campaigns else [],
        playlists=sel_playlists if sel_playlists != all_playlists else [],
        series_list=sel_series if sel_series != all_series else [],
        languages=sel_languages if sel_languages != all_languages else [],
        promo_status=promo_status,
    )

    if not metrics:
        st.warning("No videos match the current filters.")
        return

    # --- Simulation panel ---
    with st.expander("Simulation Panel — What If Promotion View Duration Changes?", expanded=False):
        st.caption(
            "Adjust the assumed average promotion view duration. "
            "Qualifying hours recalculate instantly without changing stored data."
        )
        sim_col1, sim_col2 = st.columns([3, 1])
        with sim_col1:
            sim_duration = st.slider(
                "Assumed avg promotion view duration (seconds)",
                min_value=5,
                max_value=120,
                value=30,
                step=5,
                key="qwh_sim_duration",
            )
        with sim_col2:
            quick = st.selectbox("Quick set", ["Custom", "15 sec", "30 sec", "45 sec", "60 sec"], key="qwh_sim_quick")
            if quick != "Custom":
                sim_duration = int(quick.split()[0])

        use_sim = st.checkbox("Apply simulation", value=False, key="qwh_sim_active")
        if use_sim:
            metrics = recompute_with_sim_duration(metrics, float(sim_duration))
            metrics = compute_efficiency_scores(metrics)
            st.warning(
                f"**Simulation active** — promotion watch hours recomputed using {sim_duration}s avg view duration.",
                icon="🔄",
            )

    # --- Overview cards ---
    report = compute_qualifying_hours(metrics)
    total_promo_cost = sum(m.promotion_cost for m in metrics)
    total_cost_per_qual = (
        total_promo_cost / report.estimated_qualifying_hours
        if report.estimated_qualifying_hours > 0 and total_promo_cost > 0
        else 0.0
    )
    total_organic_hrs = report.organic_watch_hours

    st.subheader("Overview")
    ov1, ov2, ov3 = st.columns(3)
    ov4, ov5, ov6 = st.columns(3)

    ov1.metric(
        "Est. Qualifying Hours",
        f"{report.estimated_qualifying_hours:,.1f} hrs",
        help="Total Watch Hours minus Promotion Watch Hours",
    )
    ov2.metric(
        "Promotion Watch Hours",
        f"{report.promotion_watch_hours:,.1f} hrs",
        delta=f"-{report.promotion_pct:.1f}% of total",
        delta_color="inverse",
    )
    ov3.metric(
        "Organic Watch Hours",
        f"{total_organic_hrs:,.1f} hrs",
    )
    ov4.metric(
        "Promotion %",
        f"{report.promotion_pct:.1f}%",
        help="Promotion watch hours as % of total",
    )
    ov5.metric(
        "Avg Organic View Duration",
        _fmt_duration(int(report.avg_organic_view_duration_seconds)),
    )
    ov6.metric(
        "Est. Hours Lost to Promotion",
        f"{report.hours_lost_to_promotion:,.1f} hrs",
        delta=f"${total_cost_per_qual:,.2f} / qualifying hr" if total_cost_per_qual > 0 else None,
        delta_color="off",
    )

    # --- Charts ---
    st.subheader("Charts")
    ts = _build_timeseries(metrics)

    chart_tabs = st.tabs([
        "Organic vs Promotion", "Qualifying Hours Trend",
        "Cost vs Hours", "Bubble",
        "Top 25 Videos", "Worst Promotions",
    ])
    with chart_tabs[0]:
        _chart_stacked_area(ts)
    with chart_tabs[1]:
        _chart_qualifying_line(ts)
    with chart_tabs[2]:
        _chart_scatter_cost_vs_hours(_to_df(metrics))
    with chart_tabs[3]:
        _chart_bubble(_to_df(metrics))
    with chart_tabs[4]:
        _chart_top25_qualifying(_to_df(metrics))
    with chart_tabs[5]:
        _chart_worst_promotions(_to_df(metrics))

    # --- Video table ---
    st.subheader("Video Table")
    df = _to_df(metrics)
    display_cols = [
        "Video", "Published", "Length", "Total Views", "Organic Views", "Promotion Views",
        "Total Watch Hours", "Promotion Watch Hours", "Est. Qualifying Hours",
        "Promotion %", "Subscribers", "Follow-on Views",
        "Promotion Cost", "Cost / Organic Hour", "Cost / Subscriber", "Cost / Follow-on View",
        "CTR %", "Avg View Duration", "Watch Time / View",
        "Efficiency Score", "Status",
    ]
    display_df = df[display_cols].copy()

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Promotion Cost": st.column_config.NumberColumn(format="$%.2f"),
            "Cost / Organic Hour": st.column_config.NumberColumn(format="$%.2f"),
            "Cost / Subscriber": st.column_config.NumberColumn(format="$%.2f"),
            "Cost / Follow-on View": st.column_config.NumberColumn(format="$%.3f"),
            "Promotion %": st.column_config.NumberColumn(format="%.1f%%"),
            "CTR %": st.column_config.NumberColumn(format="%.2f%%"),
            "Efficiency Score": st.column_config.ProgressColumn(
                min_value=0,
                max_value=100,
                format="%.0f",
            ),
        },
    )

    # --- Promotion Impact Panel ---
    _render_promotion_impact(metrics)

    # --- Future API connections notice ---
    with st.expander("Connect Real Data Sources", expanded=False):
        st.markdown("""
**YouTube Analytics API** — watch time per video, broken out by traffic source (paid vs organic).
Wire up `services/youtube_analytics.YouTubeAnalyticsAPIAdapter` with OAuth credentials.

**Google Ads API** — campaign spend, paid views, CPV by video.
Wire up `services/google_ads.GoogleAdsAPIAdapter` with a manager account client.

**Promotion Export CSV** — drop a CSV from Google Ads / YouTube Studio.
Use the **Upload Promotion CSV** option in the sidebar.

**YouTube Data API** — video metadata (title, duration, publish date).
Already partially wired through `services/youtube_analytics.LocalDBAnalyticsAdapter`.
        """)
