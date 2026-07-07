"""Video Render Comparisons — compare performance across 4 fixed rendering formats."""
from __future__ import annotations

import sqlite3

import pandas as pd
import plotly.express as px
import streamlit as st

from db import DB_PATH

st.set_page_config(page_title="Video Render Comparisons", layout="wide")

if not st.session_state.get("authenticated"):
    st.switch_page("app.py")
    st.stop()

# ---------------------------------------------------------------------------
# Fixed render-group → playlist mapping
# ---------------------------------------------------------------------------

_SHORTS_PLAYLISTS   = {"Shorts"}
_VISUAL_PLAYLISTS   = {"Visual Podcasts"}
_HD_PLAYLISTS       = {"HD Videos"}
_JELLYPOD_PLAYLISTS = {
    "The Human Workforce Podcast Series",
    "The Human Workforce International Podcasts",
}

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
def _load_playlist_videos() -> pd.DataFrame:
    """All playlist → video_id memberships (used for group assignment only)."""
    sql = """
    SELECT p.title AS playlist, pv.video_id
    FROM playlists p
    JOIN playlist_videos pv ON p.playlist_id = pv.playlist_id
    """
    with sqlite3.connect(str(DB_PATH)) as conn:
        try:
            return pd.read_sql_query(sql, conn)
        except Exception:
            return pd.DataFrame()


@st.cache_data(ttl=300)
def _load_all_videos() -> pd.DataFrame:
    """All videos with their latest cumulative metrics snapshot."""
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


def _group_stats(video_ids: set, label: str, all_vids: pd.DataFrame, ratio: float) -> dict:
    empty = {
        "group": label, "video_count": 0, "views": 0, "watch_hours": 0.0,
        "qualifying_hours": 0.0, "subscribers": 0, "avg_view_dur_sec": 0.0,
        "avg_views_per_video": 0.0, "avg_wh_per_video": 0.0,
        "avg_qh_per_video": 0.0, "avg_subs_per_video": 0.0,
    }
    if not video_ids:
        return empty
    d = all_vids[all_vids["video_id"].isin(video_ids)].drop_duplicates("video_id").copy()
    for c in ("views", "watch_hours", "subscribers_gained", "average_view_duration"):
        d[c] = d[c].fillna(0.0)
    n  = max(d["video_id"].nunique(), 1)
    tv = d["views"].sum()
    tw = d["watch_hours"].sum()
    ts = d["subscribers_gained"].sum()
    return {
        "group": label,
        "video_count": int(n),
        "views": int(tv),
        "watch_hours": tw,
        "qualifying_hours": tw * ratio,
        "subscribers": int(ts),
        "avg_view_dur_sec": (d["average_view_duration"] * d["views"]).sum() / max(tv, 1),
        "avg_views_per_video": tv / n,
        "avg_wh_per_video": tw / n,
        "avg_qh_per_video": tw * ratio / n,
        "avg_subs_per_video": ts / n,
    }


def _dur(sec: float) -> str:
    m, s = divmod(int(max(sec, 0)), 60)
    return f"{m}:{s:02d}"


# ---------------------------------------------------------------------------
# Load data + assign groups
# ---------------------------------------------------------------------------

pv       = _load_playlist_videos()
all_vids = _load_all_videos()
ratio    = _qual_ratio()

st.header("Video Render Comparisons")

if pv.empty or all_vids.empty:
    st.info("No data yet. Run `python fetch_metrics.py` to populate.")
    st.stop()

# Shorts, Visual Podcasts, and HD Videos are each their own distinct set.
# Jellypod Originals = Podcast Series playlists minus any video already in
# one of the three explicit render groups (no double-counting).
vids_shorts   = set(pv[pv["playlist"].isin(_SHORTS_PLAYLISTS)]["video_id"])
vids_visual   = set(pv[pv["playlist"].isin(_VISUAL_PLAYLISTS)]["video_id"])
vids_hd       = set(pv[pv["playlist"].isin(_HD_PLAYLISTS)]["video_id"])
vids_jellypod = (
    set(pv[pv["playlist"].isin(_JELLYPOD_PLAYLISTS)]["video_id"])
    - vids_shorts - vids_visual - vids_hd
)

groups_data = [
    _group_stats(vids_shorts,   "Shorts",             all_vids, ratio),
    _group_stats(vids_visual,   "Visual Podcasts",    all_vids, ratio),
    _group_stats(vids_hd,       "HD Videos",          all_vids, ratio),
    _group_stats(vids_jellypod, "Jellypod Originals", all_vids, ratio),
]
grp_df = pd.DataFrame([g for g in groups_data if g["video_count"] > 0])

st.caption(
    f"Qualifying ratio: {ratio * 100:.0f}% (channel-wide — total minus ADVERTISING traffic). "
    "Jellypod Originals = Human Workforce Podcast Series + International Podcasts, "
    "with Visual Podcast and HD Video episodes excluded to avoid double-counting."
)

if grp_df.empty:
    st.info("No video metrics found for any render group.")
    st.stop()

# ---------------------------------------------------------------------------
# Totals vs Per-video toggle + grouped bar chart
# ---------------------------------------------------------------------------

st.divider()

show_mode = st.radio(
    "Show",
    ["Totals (cumulative to date)", "Per-video averages"],
    horizontal=True,
    key="vrc_mode",
)
is_avg = show_mode.startswith("Per-video")

_METRIC_COLS: dict[str, tuple[str, str]] = (
    {
        "Avg Views / Video":    ("avg_views_per_video", ",.0f"),
        "Avg Watch Hrs / Video":("avg_wh_per_video",    ",.1f"),
        "Avg Qual Hrs / Video": ("avg_qh_per_video",    ",.1f"),
        "Avg Subs / Video":     ("avg_subs_per_video",  ",.1f"),
    } if is_avg else {
        "Views":            ("views",            ",.0f"),
        "Watch Hours":      ("watch_hours",      ",.1f"),
        "Qualifying Hours": ("qualifying_hours", ",.1f"),
        "Subscribers":      ("subscribers",      ",.0f"),
    }
)

val_map = {k: v[0] for k, v in _METRIC_COLS.items()}
melted  = grp_df[["group"] + list(val_map.values())].melt(
    id_vars="group", var_name="col", value_name="value"
)
melted["Metric"] = melted["col"].map({v: k for k, v in val_map.items()})

fig = px.bar(
    melted,
    x="Metric",
    y="value",
    color="group",
    barmode="group",
    color_discrete_map=_GROUP_COLORS,
    text_auto=".3s",
    labels={"value": "", "group": "Render Format", "Metric": ""},
    title="Per-Video Averages by Render Format" if is_avg else "Cumulative Totals by Render Format",
)
fig.update_traces(
    textposition="outside",
    cliponaxis=False,
    marker_line_width=0,
)
fig.update_layout(
    height=480,
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    margin=dict(t=50, b=40),
)
st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# Avg view time tiles
# ---------------------------------------------------------------------------

vt_cols = st.columns(len(grp_df))
for i, (_, row) in enumerate(grp_df.iterrows()):
    vt_cols[i].metric(
        f"{row['group']} — Avg View Time",
        _dur(row["avg_view_dur_sec"]),
    )

# ---------------------------------------------------------------------------
# Summary table
# ---------------------------------------------------------------------------

st.markdown("**Group summary**")
tbl = grp_df[[
    "group", "video_count", "views", "watch_hours", "qualifying_hours",
    "subscribers", "avg_view_dur_sec",
    "avg_views_per_video", "avg_wh_per_video",
    "avg_qh_per_video", "avg_subs_per_video",
]].copy()
tbl["avg_view_time"] = tbl["avg_view_dur_sec"].apply(_dur)
tbl = tbl.drop(columns=["avg_view_dur_sec"]).rename(columns={
    "group":               "Render Format",
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
    tbl, use_container_width=True, hide_index=True,
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
    "Shorts → 'Shorts' playlist · "
    "Visual Podcasts → 'Visual Podcasts' playlist (Magic Video template) · "
    "HD Videos → 'HD Videos' playlist · "
    "Jellypod Originals → Human Workforce Podcast Series + International Podcasts "
    "(episodes shared with other render formats excluded to avoid double-counting)."
)
