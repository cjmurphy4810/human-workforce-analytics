"""Playlist Breakdown — aggregate key metrics per playlist with comparison filter."""
from __future__ import annotations

import sqlite3

import pandas as pd
import plotly.express as px
import streamlit as st

from db import DB_PATH

st.set_page_config(page_title="Playlist Breakdown", layout="wide")

if not st.session_state.get("authenticated"):
    st.switch_page("app.py")
    st.stop()

# Catch-all playlists that roll up all/most videos — excluded by default
_CATCHALL = {
    "All Podcast Videos",
    "MultiLanguageTitleDescriptionVideo",
    "The Human Workforce Podcast Series",
}

# Sequential blue ramp: light → dark = low → high
_SEQ = [[0, "#cde2fb"], [0.4, "#3987e5"], [1, "#0d366b"]]

# Content type group colors (from categorical palette slots)
_GROUP_COLORS = {
    "Shorts":             "#eda100",  # slot 3 – yellow
    "Visual Podcasts":    "#2a78d6",  # slot 1 – blue
    "HD Videos":          "#1baf7a",  # slot 2 – aqua
    "Jellypod Originals": "#4a3aa7",  # slot 5 – violet
}

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def _load_raw() -> pd.DataFrame:
    """Per-playlist per-video metrics (latest snapshot)."""
    sql = """
    SELECT
        p.title           AS playlist,
        pv.video_id,
        dvm.views,
        dvm.estimated_minutes_watched / 60.0  AS watch_hours,
        dvm.average_view_duration,
        dvm.subscribers_gained
    FROM playlists p
    JOIN playlist_videos pv ON p.playlist_id = pv.playlist_id
    LEFT JOIN (
        SELECT video_id, MAX(metric_date) AS ld
        FROM daily_video_metrics GROUP BY video_id
    ) latest ON pv.video_id = latest.video_id
    LEFT JOIN daily_video_metrics dvm
        ON dvm.video_id = pv.video_id AND dvm.metric_date = latest.ld
    """
    with sqlite3.connect(str(DB_PATH)) as conn:
        try:
            return pd.read_sql_query(sql, conn)
        except Exception:
            return pd.DataFrame()


@st.cache_data(ttl=300)
def _load_all_videos() -> pd.DataFrame:
    """All videos with latest metrics — used for Jellypod Originals catch-all."""
    sql = """
    SELECT
        v.video_id,
        dvm.views,
        dvm.estimated_minutes_watched / 60.0  AS watch_hours,
        dvm.average_view_duration,
        dvm.subscribers_gained
    FROM videos v
    LEFT JOIN (
        SELECT video_id, MAX(metric_date) AS ld
        FROM daily_video_metrics GROUP BY video_id
    ) latest ON v.video_id = latest.video_id
    LEFT JOIN daily_video_metrics dvm
        ON dvm.video_id = v.video_id AND dvm.metric_date = latest.ld
    """
    with sqlite3.connect(str(DB_PATH)) as conn:
        try:
            return pd.read_sql_query(sql, conn)
        except Exception:
            return pd.DataFrame()


@st.cache_data(ttl=300)
def _qual_ratio() -> float:
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


def _playlist_agg(df: pd.DataFrame, ratio: float) -> pd.DataFrame:
    d = df.copy()
    for c in ("views", "watch_hours", "subscribers_gained", "average_view_duration"):
        d[c] = d[c].fillna(0.0)
    d["_wd"] = d["average_view_duration"] * d["views"]
    agg = (
        d.groupby("playlist", sort=False)
        .agg(
            video_count=("video_id", "nunique"),
            views=("views", "sum"),
            watch_hours=("watch_hours", "sum"),
            subscribers=("subscribers_gained", "sum"),
            _wds=("_wd", "sum"),
        )
        .reset_index()
    )
    agg["qualifying_hours"] = agg["watch_hours"] * ratio
    agg["avg_view_dur_sec"] = agg["_wds"] / agg["views"].clip(lower=1)
    agg["avg_view_dur_min"] = agg["avg_view_dur_sec"] / 60.0
    return agg.drop(columns=["_wds"])


def _group_stats(video_ids: set, label: str, source: pd.DataFrame, ratio: float) -> dict:
    empty = {
        "group": label, "video_count": 0, "views": 0, "watch_hours": 0.0,
        "qualifying_hours": 0.0, "subscribers": 0, "avg_view_dur_sec": 0.0,
        "avg_views_per_video": 0.0, "avg_wh_per_video": 0.0,
        "avg_qh_per_video": 0.0, "avg_subs_per_video": 0.0,
    }
    if not video_ids:
        return empty
    d = source[source["video_id"].isin(video_ids)].drop_duplicates("video_id").copy()
    for c in ("views", "watch_hours", "subscribers_gained", "average_view_duration"):
        d[c] = d[c].fillna(0.0)
    n = max(d["video_id"].nunique(), 1)
    tv = d["views"].sum()
    twh = d["watch_hours"].sum()
    tsub = d["subscribers_gained"].sum()
    return {
        "group": label,
        "video_count": int(n),
        "views": int(tv),
        "watch_hours": twh,
        "qualifying_hours": twh * ratio,
        "subscribers": int(tsub),
        "avg_view_dur_sec": (d["average_view_duration"] * d["views"]).sum() / max(tv, 1),
        "avg_views_per_video": tv / n,
        "avg_wh_per_video": twh / n,
        "avg_qh_per_video": twh * ratio / n,
        "avg_subs_per_video": tsub / n,
    }


def _dur(sec: float) -> str:
    m, s = divmod(int(max(sec, 0)), 60)
    return f"{m}:{s:02d}"


# ---------------------------------------------------------------------------
# Load + prepare
# ---------------------------------------------------------------------------

raw = _load_raw()
all_vids_df = _load_all_videos()
ratio = _qual_ratio()

st.header("Playlist Breakdown")

if raw.empty:
    st.info("No playlist data yet. Run `python fetch_metrics.py` to populate.")
    st.stop()

agg_all = _playlist_agg(raw, ratio)
all_playlists = sorted(agg_all["playlist"].tolist())
default_sel = [p for p in all_playlists if p not in _CATCHALL]

# ---------------------------------------------------------------------------
# Playlist filter
# ---------------------------------------------------------------------------

selected = st.multiselect(
    "Playlists to compare",
    options=all_playlists,
    default=default_sel,
    key="pb_filter",
    help=(
        "Catch-all playlists (All Podcast Videos, Podcast Series, MultiLanguage) "
        "excluded by default — they aggregate all videos and skew comparisons."
    ),
)

if not selected:
    st.warning("Select at least one playlist above.")
    st.stop()

agg = agg_all[agg_all["playlist"].isin(selected)].copy()

# Deduplicated channel totals across selected playlists
deduped = raw[raw["playlist"].isin(selected)].drop_duplicates("video_id").copy()
for c in ("views", "watch_hours", "subscribers_gained", "average_view_duration"):
    deduped[c] = deduped[c].fillna(0.0)

_tv   = int(deduped["views"].sum())
_twh  = deduped["watch_hours"].sum()
_tqh  = _twh * ratio
_tsub = int(deduped["subscribers_gained"].sum())
_vids = deduped["video_id"].nunique()
_wt   = (deduped["average_view_duration"] * deduped["views"]).sum() / max(deduped["views"].sum(), 1)
_wm, _ws = divmod(int(_wt), 60)

# ---------------------------------------------------------------------------
# KPI row
# ---------------------------------------------------------------------------

st.markdown(f"**{len(selected)} playlist{'s' if len(selected) != 1 else ''} · {_vids} unique videos**")

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Views",             f"{_tv:,}")
k2.metric("Watch Hours",       f"{_twh:,.1f}")
k3.metric("Qualifying Hours",  f"{_tqh:,.1f}")
k4.metric("Subscribers",       f"{_tsub:,}")
k5.metric("Avg View Time",     f"{_wm}:{_ws:02d}")
k6.metric("Qualifying Ratio",  f"{ratio * 100:.0f}%")

st.caption(
    "Totals deduplicate videos shared across playlists. "
    "Per-playlist rows below count a video in every playlist it belongs to. "
    "⚠ Impressions and Follow-on Views not yet available in the database."
)

st.divider()

# ---------------------------------------------------------------------------
# Metric comparison chart
# ---------------------------------------------------------------------------

st.subheader("Metric Comparison")

_METRICS: dict[str, tuple[str, str, str]] = {
    "Views":            ("views",            "Views",                   "%{x:,.0f}"),
    "Watch Hours":      ("watch_hours",      "Watch Hours (hrs)",       "%{x:,.1f}"),
    "Qualifying Hours": ("qualifying_hours", "Qualifying Hours",        "%{x:,.1f}"),
    "Subscribers":      ("subscribers",      "Subscribers Gained",      "%{x:,.0f}"),
    "Avg View Time":    ("avg_view_dur_min", "Avg View Duration (min)", "%{x:.2f}"),
}

sel_metric = st.radio(
    "Metric",
    list(_METRICS.keys()),
    horizontal=True,
    key="pb_metric",
)

col, y_lbl, txt_fmt = _METRICS[sel_metric]

chart_df = agg[["playlist", col, "video_count"]].sort_values(col, ascending=True).copy()
chart_df["label"] = chart_df["playlist"].apply(
    lambda t: t[:52] + "…" if len(t) > 52 else t
)

fig = px.bar(
    chart_df,
    x=col,
    y="label",
    orientation="h",
    color=col,
    color_continuous_scale=_SEQ,
    text=col,
    labels={col: y_lbl, "label": "", "video_count": "Videos"},
    hover_data={"video_count": True, "label": False},
    title=f"{sel_metric} by Playlist",
)
fig.update_traces(
    texttemplate=txt_fmt,
    textposition="outside",
    cliponaxis=False,
    marker_line_width=0,
)
fig.update_layout(
    coloraxis_showscale=False,
    xaxis_title=y_lbl,
    yaxis_title="",
    height=max(300, len(chart_df) * 46),
    margin=dict(l=0, r=150, t=44, b=36),
    hovermode="y unified",
)
st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# Full comparison table
# ---------------------------------------------------------------------------

st.subheader("All Metrics by Playlist")

tbl = (
    agg[["playlist", "video_count", "views", "watch_hours",
         "qualifying_hours", "subscribers", "avg_view_dur_sec"]]
    .sort_values("views", ascending=False)
    .copy()
)
tbl["Avg View Time"] = tbl["avg_view_dur_sec"].apply(_dur)
tbl = tbl.drop(columns=["avg_view_dur_sec"]).rename(columns={
    "playlist":          "Playlist",
    "video_count":       "Videos",
    "views":             "Views",
    "watch_hours":       "Watch Hours",
    "qualifying_hours":  "Qualifying Hours",
    "subscribers":       "Subscribers",
})

st.dataframe(
    tbl, use_container_width=True, hide_index=True,
    column_config={
        "Views":            st.column_config.NumberColumn(format="%d"),
        "Watch Hours":      st.column_config.NumberColumn(format="%.1f"),
        "Qualifying Hours": st.column_config.NumberColumn(format="%.1f"),
        "Subscribers":      st.column_config.NumberColumn(format="%d"),
        "Videos":           st.column_config.NumberColumn(format="%d"),
    },
)
st.caption(
    "Per-playlist rows count videos in each playlist independently. "
    "Watch Hours = latest 90-day fetch window. "
    "Qualifying Hours = Watch Hours × channel-wide qualifying ratio."
)

# ---------------------------------------------------------------------------
# Content Type Group Comparison
# ---------------------------------------------------------------------------

st.divider()
st.subheader("Content Type Group Comparison")
st.caption(
    "Assign playlists to each content type. "
    "Jellypod Originals automatically captures every video not assigned to another group. "
    "Totals are cumulative to date; per-video averages normalize for group size."
)

with st.expander("Configure content type groups", expanded=True):
    gc1, gc2, gc3 = st.columns(3)
    with gc1:
        grp_shorts = st.multiselect(
            "🟡 Shorts",
            options=all_playlists,
            default=[p for p in ["Shorts"] if p in all_playlists],
            key="cg_shorts",
        )
    with gc2:
        grp_visual = st.multiselect(
            "🔵 Visual Podcasts",
            options=all_playlists,
            default=[],
            key="cg_visual",
            help="Select the playlists that contain visual podcast episodes.",
        )
    with gc3:
        grp_hd = st.multiselect(
            "🟢 HD Videos",
            options=all_playlists,
            default=[],
            key="cg_hd",
            help="Select the playlists that contain HD-produced videos.",
        )
    st.caption(
        "Priority order when a video appears in multiple assigned groups: "
        "Shorts → Visual Podcasts → HD Videos → Jellypod Originals."
    )

# Build video sets with priority ordering
_all_vids  = set(all_vids_df["video_id"].unique()) if not all_vids_df.empty else set()
_vids_sh   = set(raw[raw["playlist"].isin(grp_shorts)]["video_id"]) if grp_shorts else set()
_vids_vis  = set(raw[raw["playlist"].isin(grp_visual)]["video_id"]) - _vids_sh if grp_visual else set()
_vids_hd   = set(raw[raw["playlist"].isin(grp_hd)]["video_id"]) - _vids_sh - _vids_vis if grp_hd else set()
_vids_jly  = _all_vids - _vids_sh - _vids_vis - _vids_hd

groups_data = [
    _group_stats(_vids_sh,  "Shorts",             all_vids_df, ratio),
    _group_stats(_vids_vis, "Visual Podcasts",     all_vids_df, ratio),
    _group_stats(_vids_hd,  "HD Videos",           all_vids_df, ratio),
    _group_stats(_vids_jly, "Jellypod Originals",  all_vids_df, ratio),
]
grp_df = pd.DataFrame([g for g in groups_data if g["video_count"] > 0])

if grp_df.empty:
    st.info("No videos found. Assign at least one playlist above to see the comparison.")
else:
    show_mode = st.radio(
        "Show",
        ["Totals (cumulative to date)", "Per-video averages"],
        horizontal=True,
        key="cg_mode",
    )

    is_avg = show_mode.startswith("Per-video")

    _METRIC_COLS: dict[str, tuple[str, str]] = (
        {
            "Avg Views / Video":    ("avg_views_per_video", ",.0f"),
            "Avg Watch Hrs / Video":("avg_wh_per_video",    ",.1f"),
            "Avg Qual Hrs / Video": ("avg_qh_per_video",    ",.1f"),
            "Avg Subs / Video":     ("avg_subs_per_video",  ",.1f"),
        } if is_avg else {
            "Views":           ("views",           ",.0f"),
            "Watch Hours":     ("watch_hours",     ",.1f"),
            "Qualifying Hours":("qualifying_hours",",.1f"),
            "Subscribers":     ("subscribers",     ",.0f"),
        }
    )

    # Melt for grouped bar chart
    val_map  = {k: v[0] for k, v in _METRIC_COLS.items()}
    melted = grp_df[["group"] + list(val_map.values())].melt(
        id_vars="group", var_name="col", value_name="value"
    )
    melted["Metric"] = melted["col"].map({v: k for k, v in val_map.items()})

    fig_g = px.bar(
        melted,
        x="Metric",
        y="value",
        color="group",
        barmode="group",
        color_discrete_map=_GROUP_COLORS,
        text_auto=".3s",
        labels={"value": "", "group": "Content Type", "Metric": ""},
        title="Content Type — " + ("Per-Video Averages" if is_avg else "Cumulative Totals to Date"),
    )
    fig_g.update_traces(
        textposition="outside",
        cliponaxis=False,
        marker_line_width=0,
    )
    fig_g.update_layout(
        height=460,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=50, b=40),
    )
    st.plotly_chart(fig_g, use_container_width=True)

    # Avg View Time shown separately (it's already an average, not a sum)
    avg_vt_cols = st.columns(len(grp_df))
    for i, row in grp_df.iterrows():
        m, s = divmod(int(max(row["avg_view_dur_sec"], 0)), 60)
        avg_vt_cols[i].metric(
            f"{row['group']} — Avg View Time",
            f"{m}:{s:02d}",
            help="Weighted avg view duration (view duration × views / total views).",
        )

    # Full summary table
    st.markdown("**Group summary table**")
    tbl_g = grp_df[[
        "group", "video_count", "views", "watch_hours", "qualifying_hours",
        "subscribers", "avg_view_dur_sec",
        "avg_views_per_video", "avg_wh_per_video",
        "avg_qh_per_video", "avg_subs_per_video",
    ]].copy()
    tbl_g["avg_view_time"] = tbl_g["avg_view_dur_sec"].apply(_dur)
    tbl_g = tbl_g.drop(columns=["avg_view_dur_sec"]).rename(columns={
        "group":               "Content Type",
        "video_count":         "Videos",
        "views":               "Views",
        "watch_hours":         "Watch Hrs",
        "qualifying_hours":    "Qual Hrs",
        "subscribers":         "Subscribers",
        "avg_view_time":       "Avg View Time",
        "avg_views_per_video": "Avg Views/Video",
        "avg_wh_per_video":    "Avg WH/Video",
        "avg_qh_per_video":    "Avg QH/Video",
        "avg_subs_per_video":  "Avg Subs/Video",
    })
    st.dataframe(
        tbl_g, use_container_width=True, hide_index=True,
        column_config={
            "Views":           st.column_config.NumberColumn(format="%d"),
            "Watch Hrs":       st.column_config.NumberColumn(format="%.1f"),
            "Qual Hrs":        st.column_config.NumberColumn(format="%.1f"),
            "Subscribers":     st.column_config.NumberColumn(format="%d"),
            "Videos":          st.column_config.NumberColumn(format="%d"),
            "Avg Views/Video": st.column_config.NumberColumn(format="%.0f"),
            "Avg WH/Video":    st.column_config.NumberColumn(format="%.1f"),
            "Avg QH/Video":    st.column_config.NumberColumn(format="%.1f"),
            "Avg Subs/Video":  st.column_config.NumberColumn(format="%.1f"),
        },
    )
    st.caption(
        "Jellypod Originals = all videos not assigned to Shorts, Visual Podcasts, or HD Videos. "
        "Per-video averages normalize for group size — use these to compare content quality "
        "independent of how many videos each format has."
    )
