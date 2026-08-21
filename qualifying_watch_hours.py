"""
Qualifying Watch Hours dashboard page.

Answers: "For every dollar spent promoting a video, how many qualifying watch hours
did we actually create?"

Qualifying Hours = Total Watch Hours - Advertising-Generated Watch Hours
                   - Organic Shorts Watch Hours.
Advertising-Generated Watch Hours = Advertising Views × Avg Advertising View Duration.
"""
from __future__ import annotations

import io
import random
import sqlite3
from datetime import date, datetime, timedelta
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

_YPP_WATCH_HOURS_THRESHOLD = 3_000
_BONUS_THRESHOLD = 4_000  # 12-month growth target
NO_PROMOTION_DATA_MESSAGE = (
    "No promotion data loaded — all non-Shorts watch hours are counting as "
    "qualifying. Upload a Promotion CSV in the sidebar to subtract "
    "promotion-generated hours."
)


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


def _build_demo_metrics(as_of: date | None = None) -> list[VideoPromotionMetrics]:
    effective_as_of = as_of or date.today()
    rng = random.Random(_DEMO_SEED)

    metrics: list[VideoPromotionMetrics] = []
    for i, (vid_id, title, length_sec, campaign, series, playlist) in enumerate(_DEMO_VIDEOS):
        published = _BASE_DATE + timedelta(days=i * 14 + rng.randint(-3, 3))
        days_live = max((effective_as_of - published.date()).days, 1)

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

def _build_timeseries(
    metrics: list[VideoPromotionMetrics], as_of: date | None = None
) -> pd.DataFrame:
    """Generate a weekly time-series by distributing each video's hours across its life."""
    effective_as_of = as_of or date.today()
    rows = []
    for m in metrics:
        if m.published is None:
            continue
        weeks = max(int((effective_as_of - m.published.date()).days / 7), 1)
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
# Real DB data loaders
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def _db_query(db_path_str: str, sql: str, params: tuple = ()) -> pd.DataFrame:
    db = Path(db_path_str)
    if not db.exists():
        return pd.DataFrame()
    with sqlite3.connect(db) as conn:
        try:
            return pd.read_sql_query(sql, conn, params=params)
        except Exception:
            return pd.DataFrame()


def _cumulative_snapshot_deltas(
    frame: pd.DataFrame,
    *,
    group_columns: list[str],
    value_columns: list[str],
    start: date,
) -> pd.DataFrame:
    """Convert cumulative snapshots to non-negative increments within a window."""
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    result["metric_date"] = pd.to_datetime(result["metric_date"])
    result = result.sort_values(group_columns + ["metric_date"])
    for column in value_columns:
        # A per-video lifetime-to-date total can only truly grow. In practice this
        # pipeline's ADVERTISING/per-video snapshots regularly dip for a week or more
        # (observed: one video fell from 26,487 to 5,779 min over 14 days, then snapped
        # back to exactly 26,487) before self-correcting — a temporary reporting glitch,
        # not a real reduction. Collapsing each video's series to its running maximum
        # before differencing ignores those dips (rather than either dropping that
        # day's real growth, or misreading the dip's raw value as a fresh lifetime
        # baseline and double-counting it) while still capturing genuine growth as soon
        # as a new high is observed, even if the latest snapshot is itself mid-dip.
        result[column] = result.groupby(group_columns)[column].cummax()
        delta = result.groupby(group_columns)[column].diff()
        # A first snapshot with no pre-window baseline is a lifetime value, not an
        # increment — exclude it until a later snapshot supplies an observable delta.
        result[column] = delta.clip(lower=0.0).fillna(0.0)
    return result[result["metric_date"] >= pd.Timestamp(start)]


def _cumulative_window_total(
    frame: pd.DataFrame,
    *,
    group_columns: list[str],
    value_column: str,
    window_start: date,
    as_of: date,
) -> float:
    """Sum each group's growth in a lifetime-cumulative counter within a window.

    Unlike _cumulative_snapshot_deltas (summing every day-over-day delta — needed for a
    day-by-day trend chart), an aggregate window total only needs two points per group:
    the latest running-max value at-or-before `as_of`, minus the latest running-max
    value strictly before `window_start` (0 if no such row exists — i.e. this group's
    entire tracked history already falls inside the window, which is the normal case
    for any channel younger than the window). Summing daily deltas instead
    systematically undercounts here: a video's first-ever snapshot is excluded as an
    unknowable baseline even when the channel didn't exist before the window (so there
    is no real pre-window history to exclude), and a same-value "recovery" day after a
    multi-day reporting dip can otherwise get re-added as if it were fresh growth.
    """
    if frame.empty:
        return 0.0
    df = frame.copy()
    df["metric_date"] = pd.to_datetime(df["metric_date"])
    df = df.sort_values(group_columns + ["metric_date"])
    df[value_column] = df.groupby(group_columns)[value_column].cummax()

    as_of_ts = pd.Timestamp(as_of)
    start_ts = pd.Timestamp(window_start)

    end_vals = df[df["metric_date"] <= as_of_ts].groupby(group_columns)[value_column].last()
    start_vals = df[df["metric_date"] < start_ts].groupby(group_columns)[value_column].last()
    end_vals, start_vals = end_vals.align(start_vals, fill_value=0.0)
    return float((end_vals - start_vals).clip(lower=0.0).sum())


def _build_real_metrics(
    db_path: Path,
    channel: str,
    *,
    as_of: date | None = None,
    daily_metrics_are_increments: bool = False,
) -> list[VideoPromotionMetrics]:
    """Build VideoPromotionMetrics from real DB data.

    Promotion watch hours come from insightTrafficSourceType=ADVERTISING via
    video_traffic_source_metrics (data_source='API_ACTUAL').

    Fallback: if ADVERTISING data is unavailable for a video, promotion_watch_hours
    is estimated as promotion_views × average_view_duration / 3600 (data_source='ESTIMATED').
    Follow-on views are tracked separately and never counted as watch hours.

    If no promotion data exists at all, data_source='NONE' and qualifying hours exclude
    Shorts but cannot yet exclude advertising-generated watch time.
    """
    effective_as_of = as_of or date.today()
    window_start = effective_as_of - timedelta(days=364)
    window = (channel, window_start.isoformat(), effective_as_of.isoformat())

    vids = _db_query(str(db_path),
        "SELECT video_id, title, published_at, duration_seconds FROM videos "
        "WHERE channel = ? AND (published_at IS NULL OR date(published_at) <= ?)",
        (channel, effective_as_of.isoformat()))
    if vids.empty:
        return []

    if daily_metrics_are_increments:
        dvm = _db_query(str(db_path),
            "SELECT video_id, COALESCE(SUM(views), 0) AS view_count, "
            "COALESCE(SUM(estimated_minutes_watched), 0)/60.0 AS total_watch_hours, "
            "COALESCE(SUM(average_view_duration * views) / NULLIF(SUM(views), 0), 0) "
            "AS avg_view_duration FROM daily_video_metrics "
            "WHERE channel = ? AND metric_date BETWEEN ? AND ? GROUP BY video_id",
            window)
        adv = _db_query(str(db_path),
            "SELECT video_id, COALESCE(SUM(estimated_minutes_watched), 0)/60.0 "
            "AS advertising_watch_hours, COALESCE(SUM(views), 0) AS advertising_views, "
            "COALESCE(SUM(average_view_duration * views) / NULLIF(SUM(views), 0), 0) "
            "AS avg_advertising_view_duration FROM video_traffic_source_metrics "
            "WHERE traffic_source_type = 'ADVERTISING' AND channel = ? "
            "AND metric_date BETWEEN ? AND ? GROUP BY video_id",
            window)
    else:
        # Production rows are cumulative snapshots. Select exactly the latest row at
        # or before the cutoff for each video; summing snapshots double-counts history.
        dvm = _db_query(str(db_path),
            "SELECT d.video_id, d.views AS view_count, "
            "d.estimated_minutes_watched/60.0 AS total_watch_hours, "
            "d.average_view_duration AS avg_view_duration FROM daily_video_metrics d "
            "JOIN (SELECT video_id, MAX(metric_date) metric_date "
            "FROM daily_video_metrics WHERE channel=? AND metric_date<=? GROUP BY video_id) latest "
            "ON latest.video_id=d.video_id AND latest.metric_date=d.metric_date "
            "WHERE d.channel=?",
            (channel, effective_as_of.isoformat(), channel))
        adv = _db_query(str(db_path),
            "SELECT d.video_id, d.estimated_minutes_watched/60.0 AS advertising_watch_hours, "
            "d.views AS advertising_views, d.average_view_duration AS avg_advertising_view_duration "
            "FROM video_traffic_source_metrics d JOIN (SELECT video_id, MAX(metric_date) metric_date "
            "FROM video_traffic_source_metrics WHERE channel=? AND traffic_source_type='ADVERTISING' "
            "AND metric_date<=? GROUP BY video_id) latest ON latest.video_id=d.video_id "
            "AND latest.metric_date=d.metric_date WHERE d.channel=? "
            "AND d.traffic_source_type='ADVERTISING'",
            (channel, effective_as_of.isoformat(), channel))
        snapshots = _db_query(str(db_path),
            "SELECT s.video_id, s.view_count FROM video_snapshots s JOIN ("
            "SELECT video_id, MAX(captured_at) captured_at FROM video_snapshots "
            "WHERE channel=? AND date(captured_at)<=? GROUP BY video_id) latest "
            "ON latest.video_id=s.video_id AND latest.captured_at=s.captured_at "
            "WHERE s.channel=?",
            (channel, effective_as_of.isoformat(), channel))
        if not snapshots.empty and not dvm.empty:
            dvm = dvm.drop(columns=["view_count"]).merge(
                snapshots, on="video_id", how="left"
            )

    if dvm.empty:
        return []

    df = vids.merge(dvm, on="video_id", how="inner")
    if not adv.empty:
        df = df.merge(adv, on="video_id", how="left")
    else:
        df["advertising_watch_hours"] = float("nan")
        df["advertising_views"] = float("nan")
        df["avg_advertising_view_duration"] = float("nan")

    df["view_count"] = df["view_count"].fillna(0).astype(int)
    df["total_watch_hours"] = df["total_watch_hours"].fillna(0.0)
    df["avg_view_duration"] = df["avg_view_duration"].fillna(0.0)

    has_api_data = not adv.empty

    metrics: list[VideoPromotionMetrics] = []
    for _, row in df.iterrows():
        published: Optional[datetime] = None
        if pd.notna(row.get("published_at")):
            try:
                published = pd.to_datetime(row["published_at"]).to_pydatetime().replace(tzinfo=None)
            except Exception:
                pass

        total_wh = float(row["total_watch_hours"])
        avg_dur = float(row.get("avg_view_duration") or 0)

        if has_api_data and pd.notna(row.get("advertising_watch_hours")):
            # API_ACTUAL: ADVERTISING minutes directly from insightTrafficSourceType
            promo_views = int(row.get("advertising_views") or 0)
            avg_promo_dur = float(row.get("avg_advertising_view_duration") or avg_dur)
            data_source = "API_ACTUAL"
        elif has_api_data:
            # Video had no ADVERTISING traffic in the period — truly zero promotion
            promo_views = 0
            avg_promo_dur = 0.0
            data_source = "API_ACTUAL"
        else:
            # No traffic source data yet — will populate on next fetch
            promo_views = 0
            avg_promo_dur = 0.0
            data_source = "NONE"

        metrics.append(make_metrics(
            video_id=str(row["video_id"]),
            title=str(row.get("title", "")),
            published=published,
            length_seconds=int(row.get("duration_seconds") or 0),
            total_views=int(row["view_count"]),
            promotion_views=promo_views,
            total_watch_hours=total_wh,
            avg_promotion_view_duration_seconds=avg_promo_dur,
            promotion_cost=0.0,
            subscribers=0,
            follow_on_views=0,
            avg_view_duration_seconds=avg_dur,
            data_source=data_source,
            # Pass pre-computed promo watch hours directly via the factory
        ))

    # Override promotion watch hours with the stored value and exclude Shorts from
    # monetization-qualifying hours while retaining them in organic watch hours.
    corrected: list[VideoPromotionMetrics] = []
    for m, (_, row) in zip(metrics, df.iterrows()):
        if m.data_source == "API_ACTUAL" and pd.notna(row.get("advertising_watch_hours")):
            import dataclasses as _dc
            adv_wh = float(row["advertising_watch_hours"])
            organic_wh = max(m.total_watch_hours - adv_wh, 0.0)
            promo_pct = (adv_wh / max(m.total_watch_hours, 1)) * 100
            qualifying_wh = 0.0 if 0 < m.length_seconds <= 180 else organic_wh
            corrected.append(_dc.replace(
                m,
                promotion_watch_hours=adv_wh,
                organic_watch_hours=organic_wh,
                estimated_qualifying_hours=qualifying_wh,
                promotion_percentage=promo_pct,
                promotion_views=int(row.get("advertising_views") or 0),
            ))
        else:
            corrected.append(m)

    return compute_efficiency_scores(corrected)


def _build_real_timeseries(
    db_path: Path,
    channel: str,
    *,
    as_of: date | None = None,
    daily_metrics_are_increments: bool = False,
) -> pd.DataFrame:
    """Aggregate real daily_channel_metrics into weekly time-series.

    Splits each week's total hours into organic vs promotion using the channel-level
    ADVERTISING ratio summed over the same daily-increment window.
    """
    effective_as_of = as_of or date.today()
    window_start = effective_as_of - timedelta(days=364)
    window = (channel, window_start.isoformat(), effective_as_of.isoformat())
    df = _db_query(str(db_path),
        "SELECT metric_date, estimated_minutes_watched "
        "FROM daily_channel_metrics WHERE channel = ? AND metric_date BETWEEN ? AND ? "
        "ORDER BY metric_date",
        window)
    if df.empty:
        return pd.DataFrame()

    df["metric_date"] = pd.to_datetime(df["metric_date"])
    adv = _db_query(str(db_path),
        "SELECT metric_date, video_id, estimated_minutes_watched FROM "
        "video_traffic_source_metrics WHERE channel=? AND traffic_source_type='ADVERTISING' "
        "AND metric_date<=? ORDER BY video_id, metric_date",
        (channel, effective_as_of.isoformat()))
    shorts = _db_query(str(db_path),
        "SELECT d.metric_date, d.video_id, d.estimated_minutes_watched "
        "FROM daily_video_metrics d JOIN videos v ON v.channel=d.channel AND v.video_id=d.video_id "
        "WHERE d.channel=? AND d.metric_date<=? AND v.duration_seconds>0 "
        "AND v.duration_seconds<=180 ORDER BY d.video_id, d.metric_date",
        (channel, effective_as_of.isoformat()))
    short_adv = _db_query(str(db_path),
        "SELECT d.metric_date, d.video_id, d.estimated_minutes_watched "
        "FROM video_traffic_source_metrics d JOIN videos v "
        "ON v.channel=d.channel AND v.video_id=d.video_id WHERE d.channel=? "
        "AND d.metric_date<=? AND d.traffic_source_type='ADVERTISING' "
        "AND v.duration_seconds>0 AND v.duration_seconds<=180 "
        "ORDER BY d.video_id, d.metric_date",
        (channel, effective_as_of.isoformat()))

    if not daily_metrics_are_increments:
        adv = _cumulative_snapshot_deltas(
            adv,
            group_columns=["video_id"],
            value_columns=["estimated_minutes_watched"],
            start=window_start,
        )
        shorts = _cumulative_snapshot_deltas(
            shorts,
            group_columns=["video_id"],
            value_columns=["estimated_minutes_watched"],
            start=window_start,
        )
        short_adv = _cumulative_snapshot_deltas(
            short_adv,
            group_columns=["video_id"],
            value_columns=["estimated_minutes_watched"],
            start=window_start,
        )
    else:
        for frame in (adv, shorts, short_adv):
            if not frame.empty:
                frame["metric_date"] = pd.to_datetime(frame["metric_date"])
                frame.drop(
                    frame[frame["metric_date"] < pd.Timestamp(window_start)].index,
                    inplace=True,
                )

    def _daily_minutes(frame: pd.DataFrame, name: str) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame(columns=["metric_date", name])
        return frame.groupby("metric_date", as_index=False)["estimated_minutes_watched"].sum().rename(
            columns={"estimated_minutes_watched": name}
        )

    df = df.merge(_daily_minutes(adv, "promotion_minutes"), on="metric_date", how="left")
    df = df.merge(_daily_minutes(shorts, "shorts_minutes"), on="metric_date", how="left")
    df = df.merge(_daily_minutes(short_adv, "shorts_promotion_minutes"), on="metric_date", how="left")
    for column in ("promotion_minutes", "shorts_minutes", "shorts_promotion_minutes"):
        df[column] = df[column].fillna(0.0)
    df["total_hours"] = df["estimated_minutes_watched"] / 60.0
    df["promotion_hours"] = df["promotion_minutes"].clip(lower=0) / 60.0
    df["organic_hours"] = (df["total_hours"] - df["promotion_hours"]).clip(lower=0)
    df["shorts_organic_hours"] = (
        df["shorts_minutes"] - df["shorts_promotion_minutes"]
    ).clip(lower=0) / 60.0
    df["qualifying_hours"] = (
        df["organic_hours"] - df["shorts_organic_hours"]
    ).clip(lower=0)
    df["week"] = df["metric_date"].dt.to_period("W").apply(lambda p: p.start_time.date())
    weekly = df.groupby("week", as_index=False).agg(
        organic_hours=("organic_hours", "sum"),
        promotion_hours=("promotion_hours", "sum"),
        qualifying_hours=("qualifying_hours", "sum"),
    )
    return weekly[["week", "organic_hours", "promotion_hours", "qualifying_hours"]]


def _get_qualifying_hours_last_365(
    db_path: Path, channel: str, *, as_of: date | None = None
) -> float:
    effective_as_of = as_of or date.today()
    cutoff = (effective_as_of - timedelta(days=364)).isoformat()
    df = _db_query(str(db_path),
        "SELECT SUM(estimated_minutes_watched)/60.0 AS hrs "
        "FROM daily_channel_metrics WHERE metric_date BETWEEN ? AND ? AND channel = ?",
        (cutoff, effective_as_of.isoformat(), channel))
    if df.empty or pd.isna(df["hrs"].iloc[0]):
        return 0.0
    return float(df["hrs"].iloc[0])


def _get_shorts_watch_hours(
    db_path: Path,
    channel: str,
    *,
    as_of: date | None = None,
    daily_metrics_are_increments: bool = False,
) -> float:
    """Return non-advertising Shorts hours for the trailing-year window.

    Shorts watch time never counts toward the YPP watch-hours requirement, regardless
    of whether it was organic or promoted — but a Short's ADVERTISING-sourced hours are
    already netted out by _get_advertising_watch_hours(). Subtracting a Short's full
    total here on top of that would double-count the overlap for any Short that also
    ran paid promotion, so this returns only each Short's non-ADVERTISING remainder
    (total minus that video's own ADVERTISING hours, floored at 0).

    Increment storage is summed directly; cumulative storage uses boundary-snapshot
    subtraction via _cumulative_window_total (see its docstring for why). Both totals
    use the same explicit inclusive reporting window.
    """
    effective_as_of = as_of or date.today()
    cutoff = (effective_as_of - timedelta(days=364)).isoformat()
    totals = _db_query(str(db_path),
        "SELECT d.metric_date, d.video_id, d.estimated_minutes_watched "
        "FROM daily_video_metrics d JOIN videos v ON v.channel=d.channel "
        "AND v.video_id=d.video_id WHERE d.channel=? AND d.metric_date<=? "
        "AND v.duration_seconds>0 AND v.duration_seconds<=180 "
        "ORDER BY d.video_id, d.metric_date",
        (channel, effective_as_of.isoformat()))
    adv = _db_query(str(db_path),
        "SELECT d.metric_date, d.video_id, d.estimated_minutes_watched "
        "FROM video_traffic_source_metrics d JOIN videos v ON v.channel=d.channel "
        "AND v.video_id=d.video_id WHERE d.channel=? AND d.metric_date<=? "
        "AND d.traffic_source_type='ADVERTISING' AND v.duration_seconds>0 "
        "AND v.duration_seconds<=180 ORDER BY d.video_id, d.metric_date",
        (channel, effective_as_of.isoformat()))
    window_start = effective_as_of - timedelta(days=364)
    if not daily_metrics_are_increments:
        total_minutes = _cumulative_window_total(
            totals,
            group_columns=["video_id"],
            value_column="estimated_minutes_watched",
            window_start=window_start,
            as_of=effective_as_of,
        )
        adv_minutes = _cumulative_window_total(
            adv,
            group_columns=["video_id"],
            value_column="estimated_minutes_watched",
            window_start=window_start,
            as_of=effective_as_of,
        )
    else:
        totals = totals[totals["metric_date"] >= cutoff] if not totals.empty else totals
        adv = adv[adv["metric_date"] >= cutoff] if not adv.empty else adv
        total_minutes = float(totals["estimated_minutes_watched"].sum()) if not totals.empty else 0.0
        adv_minutes = float(adv["estimated_minutes_watched"].sum()) if not adv.empty else 0.0
    return max(total_minutes - adv_minutes, 0.0) / 60.0


def _get_advertising_watch_hours(
    db_path: Path,
    channel: str,
    *,
    as_of: date | None = None,
    daily_metrics_are_increments: bool = False,
) -> tuple[float, bool]:
    """Return (advertising_watch_hours, has_api_data).

    Sums ADVERTISING watch time over the trailing year. Increment storage is summed
    directly; cumulative storage uses boundary-snapshot subtraction via
    _cumulative_window_total (see its docstring for why — not a running delta sum).
    Returns (0.0, False) when video_traffic_source_metrics has no ADVERTISING rows.
    """
    effective_as_of = as_of or date.today()
    cutoff = (effective_as_of - timedelta(days=364)).isoformat()
    df = _db_query(str(db_path),
        "SELECT metric_date, video_id, estimated_minutes_watched "
        "FROM video_traffic_source_metrics WHERE traffic_source_type='ADVERTISING' "
        "AND channel=? AND metric_date<=? ORDER BY video_id, metric_date",
        (channel, effective_as_of.isoformat()))
    if df.empty:
        return 0.0, False
    if daily_metrics_are_increments:
        df = df[df["metric_date"] >= cutoff]
        return float(df["estimated_minutes_watched"].sum()) / 60.0, True
    total_minutes = _cumulative_window_total(
        df,
        group_columns=["video_id"],
        value_column="estimated_minutes_watched",
        window_start=effective_as_of - timedelta(days=364),
        as_of=effective_as_of,
    )
    return total_minutes / 60.0, True


def _get_db_date_range(
    db_path: Path, channel: str, *, as_of: date | None = None
) -> tuple[Optional[str], Optional[str]]:
    effective_as_of = as_of or date.today()
    cutoff = (effective_as_of - timedelta(days=364)).isoformat()
    df = _db_query(str(db_path),
        "SELECT MIN(metric_date) AS earliest, MAX(metric_date) AS latest "
        "FROM daily_channel_metrics WHERE channel = ? AND metric_date BETWEEN ? AND ?",
        (channel, cutoff, effective_as_of.isoformat()))
    if df.empty or pd.isna(df["earliest"].iloc[0]):
        return None, None
    return str(df["earliest"].iloc[0]), str(df["latest"].iloc[0])


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
            "Organic Watch Hours": round(m.organic_watch_hours, 1),
            "Est. Qualifying Hours": round(m.estimated_qualifying_hours, 1),
            "Promotion %": round(m.promotion_percentage, 1),
            "Subscribers": m.subscribers,
            "Follow-on Views": m.follow_on_views,
            "Promotion Cost": round(m.promotion_cost, 2),
            "Cost / Qualifying Hour": round(m.cost_per_qualified_hour, 2),
            "Cost / Subscriber": round(m.cost_per_subscriber, 2),
            "Cost / Follow-on View": round(m.cost_per_follow_on_view, 2),
            "CTR %": round(m.ctr, 2),
            "Avg View Duration": _fmt_duration(int(m.avg_view_duration_seconds)),
            "Watch Time / View": _fmt_duration(
                int(m.total_watch_hours * 3600 / max(m.total_views, 1))
            ),
            "Efficiency Score": round(m.promotion_efficiency_score, 1),
            "Data Source": m.data_source,
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
        hover_data=["Promotion %", "Cost / Qualifying Hour"],
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
            "Est. Qualifying Hours": "Qualifying Watch Hours",
            "Follow-on Views": "Bubble = Follow-on Views",
        },
        title="Promotion Cost · Qualifying Hours · Subscribers (Bubble = Follow-on Views)",
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
    promo = df[(df["Promotion Cost"] > 0) & (df["Cost / Qualifying Hour"] > 0)].copy()
    if promo.empty:
        st.info("No promoted videos with cost data in current filter.")
        return
    worst = promo.nlargest(15, "Cost / Qualifying Hour")[["Video", "Cost / Qualifying Hour", "Promotion Cost", "Est. Qualifying Hours"]].copy()
    worst["short_title"] = worst["Video"].apply(lambda t: t[:50] + "…" if len(t) > 50 else t)
    fig = px.bar(
        worst,
        x="Cost / Qualifying Hour",
        y="short_title",
        orientation="h",
        color="Cost / Qualifying Hour",
        color_continuous_scale="Reds",
        labels={"short_title": "", "Cost / Qualifying Hour": "Cost per Qualifying Hour ($)"},
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
# Projections
# ---------------------------------------------------------------------------

def _render_projections(
    db_path: Path,
    qual_ratio: float,
    channel: str,
    *,
    as_of: date | None = None,
) -> None:
    """Show projected qualifying hours under three rate scenarios."""
    import datetime as _dt
    import numpy as np

    today = as_of or _dt.date.today()

    # Current cumulative qualifying hours over the trailing 365 days — matches YPP's
    # actual rolling-window rule. While the channel is younger than 365 days this equals
    # its full history; once it passes the 1-year mark, an unbounded SUM would keep
    # counting hours YouTube has already rolled off the eligibility window.
    year_cutoff = (today - _dt.timedelta(days=364)).isoformat()
    cur_row = _db_query(str(db_path),
        "SELECT SUM(estimated_minutes_watched)/60.0 AS hrs, "
        "COUNT(*) AS days FROM daily_channel_metrics WHERE channel = ? "
        "AND metric_date BETWEEN ? AND ?",
        (channel, year_cutoff, today.isoformat()))
    current_total = float(cur_row["hrs"].iloc[0] or 0) if not cur_row.empty else 0.0
    channel_days = int(cur_row["days"].iloc[0] or 1) if not cur_row.empty else 1
    current_qualifying = current_total * qual_ratio

    def _avg_rate(lookback_days: int) -> float:
        cutoff = (today - _dt.timedelta(days=lookback_days - 1)).isoformat()
        r = _db_query(str(db_path),
            "SELECT AVG(estimated_minutes_watched)/60.0 AS d "
            "FROM daily_channel_metrics WHERE metric_date BETWEEN ? AND ? AND channel = ?",
            (cutoff, today.isoformat(), channel))
        return float(r["d"].iloc[0] or 0) if not r.empty else 0.0

    rate_30d = _avg_rate(30) * qual_ratio    # recent (last 30 days)
    rate_90d = _avg_rate(90) * qual_ratio    # medium-term (last 90 days)
    rate_life = (current_total / max(channel_days, 1)) * qual_ratio  # trailing-year avg

    if rate_life <= 0:
        st.info("Not enough history to project qualifying hours.")
        return

    st.subheader("Qualifying Hours Projection")

    # Rate comparison table
    rc1, rc2, rc3 = st.columns(3)
    rc1.metric(
        "Conservative (Trailing-Year Avg)",
        f"{rate_life:.1f} qualifying hrs/day",
        delta=f"{rate_life * 30:.0f} hrs / month",
        delta_color="off",
        help=f"Based on {channel_days} days in the trailing-year window. "
             "Best estimate if growth has plateaued.",
    )
    rc2.metric(
        "Moderate (Last 90 Days)",
        f"{rate_90d:.1f} qualifying hrs/day",
        delta=f"{rate_90d * 30:.0f} hrs / month",
        delta_color="off",
        help="Smooths out short-term spikes. A reasonable middle ground.",
    )
    rc3.metric(
        "Optimistic (Last 30 Days)",
        f"{rate_30d:.1f} qualifying hrs/day",
        delta=f"{rate_30d * 30:.0f} hrs / month",
        delta_color="off",
        help="Reflects your most recent growth pace. Could reflect subscriber "
             "flywheel effects OR a temporary spike in promotion spend.",
    )

    # Gap explanation
    if rate_30d > rate_life * 1.1:
        accel_pct = (rate_30d / rate_life - 1) * 100
        st.info(
            f"Your recent rate ({rate_30d:.1f} qualifying hrs/day) is **{accel_pct:.0f}% faster** "
            f"than your trailing-year average ({rate_life:.1f} hrs/day). This likely reflects a mix of "
            f"subscriber growth (more organic reach per video), recent promotion spend, "
            f"and natural channel momentum. The Conservative line accounts for the possibility "
            f"that some of this acceleration is temporary.",
            icon="📈",
        )

    # Scenario milestones — 3,000 hr YPP threshold
    st.markdown("**Days to YPP threshold (3,000 hrs) by scenario**")
    mil1, mil2, mil3 = st.columns(3)
    for col, rate, label in [
        (mil1, rate_life, "Conservative"),
        (mil2, rate_90d, "Moderate"),
        (mil3, rate_30d, "Optimistic"),
    ]:
        if current_qualifying >= _YPP_WATCH_HOURS_THRESHOLD:
            col.metric(label, "Already reached ✅")
        elif rate <= 0:
            col.metric(label, "—")
        else:
            days_left = (_YPP_WATCH_HOURS_THRESHOLD - current_qualifying) / rate
            target = today + _dt.timedelta(days=int(days_left))
            col.metric(
                label,
                f"{int(days_left)} days",
                delta=target.strftime("%b %d, %Y"),
                delta_color="off",
            )

    # Scenario milestones — 4,000 hr 12-month growth target
    st.markdown("**Days to 12-month target (4,000 hrs) by scenario**")
    bon1, bon2, bon3 = st.columns(3)
    for col, rate, label in [
        (bon1, rate_life, "Conservative"),
        (bon2, rate_90d, "Moderate"),
        (bon3, rate_30d, "Optimistic"),
    ]:
        if current_qualifying >= _BONUS_THRESHOLD:
            col.metric(label, "Already reached ✅")
        elif rate <= 0:
            col.metric(label, "—")
        else:
            days_left = (_BONUS_THRESHOLD - current_qualifying) / rate
            target = today + _dt.timedelta(days=int(days_left))
            col.metric(
                label,
                f"{int(days_left)} days",
                delta=target.strftime("%b %d, %Y"),
                delta_color="off",
            )

    # Projection chart — all three scenarios
    future_days = np.arange(0, 366)
    future_dates = [today + _dt.timedelta(days=int(d)) for d in future_days]

    fig = go.Figure()
    scenarios = [
        ("Conservative (Trailing Year)", rate_life, "#F58518", "dash"),
        ("Moderate (90-day)", rate_90d, "#4C78A8", "dot"),
        ("Optimistic (30-day)", rate_30d, "#54A24B", "solid"),
    ]
    for name, rate, color, dash in scenarios:
        fig.add_scatter(
            x=future_dates,
            y=current_qualifying + rate * future_days,
            name=name,
            mode="lines",
            line=dict(color=color, width=2, dash=dash),
        )
    fig.add_hline(
        y=_YPP_WATCH_HOURS_THRESHOLD,
        line=dict(color="#E45756", width=2, dash="dash"),
        annotation_text="YPP 3,000 hr threshold",
        annotation_position="top left",
    )
    fig.add_hline(
        y=_BONUS_THRESHOLD,
        line=dict(color="#F58518", width=2, dash="dash"),
        annotation_text="4,000 hr 12-month target",
        annotation_position="top left",
    )
    for days in [30, 90, 180, 365]:
        fig.add_vline(
            x=str(today + _dt.timedelta(days=days)),
            line=dict(color="rgba(255,255,255,0.15)", width=1, dash="dot"),
        )
    fig.update_layout(
        title="Projected Qualifying Watch Hours — Three Scenarios",
        xaxis_title="Date",
        yaxis_title="Cumulative Qualifying Hours",
        height=400,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption(
        f"Current qualifying hours: **{current_qualifying:,.0f} hrs** (total {current_total:,.0f} hrs × "
        f"{qual_ratio * 100:.0f}% qualifying ratio). All scenarios assume constant promotion spend and "
        f"content cadence. Excludes Shorts (not counted by YouTube for YPP)."
    )


# ---------------------------------------------------------------------------
# Main render function
# ---------------------------------------------------------------------------

def render(
    db_path: Path,
    channel: str,
    as_of: date | None = None,
    empty_message: str | None = None,
    *,
    fixed_data_source: bool = False,
    daily_metrics_are_increments: bool = False,
) -> None:
    effective_as_of = as_of or date.today()
    empty_copy = empty_message or "No qualifying watch-hour data is available."
    st.header("Qualifying Watch Hours")
    st.caption(
        "Estimates YouTube Partner Program qualifying watch hours by excluding "
        "advertising-generated watch time and watch time from Shorts."
    )

    # --- Sidebar: data source & filters ---
    real_metrics = _build_real_metrics(
        db_path,
        channel,
        as_of=effective_as_of,
        daily_metrics_are_increments=daily_metrics_are_increments,
    )
    has_real_data = len(real_metrics) > 0

    uploaded_file = None
    if fixed_data_source:
        source_mode = "Live Data"
        if not has_real_data:
            st.warning(empty_copy)
            return
    else:
        with st.sidebar:
            st.markdown("---")
            st.markdown("**Data Source**")
            default_source = "Live Data" if has_real_data else "Demo Mode"
            source_mode = st.radio(
                "data_source",
                ["Live Data", "Demo Mode", "Upload Promotion CSV"],
                index=["Live Data", "Demo Mode", "Upload Promotion CSV"].index(default_source),
                label_visibility="collapsed",
                key="qwh_source",
            )
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
    if source_mode == "Live Data":
        base_metrics = real_metrics
        ts = _build_real_timeseries(
            db_path,
            channel,
            as_of=effective_as_of,
            daily_metrics_are_increments=daily_metrics_are_increments,
        )
        is_demo = False
    elif source_mode == "Upload Promotion CSV" and uploaded_file is not None:
        base_metrics = _try_load_csv(uploaded_file) or real_metrics or _build_demo_metrics(effective_as_of)
        ts = (_build_real_timeseries(
                  db_path,
                  channel,
                  as_of=effective_as_of,
                  daily_metrics_are_increments=daily_metrics_are_increments,
              )
              if has_real_data else _build_timeseries(base_metrics, effective_as_of))
        is_demo = False
    else:
        base_metrics = _build_demo_metrics(effective_as_of)
        ts = _build_timeseries(base_metrics, effective_as_of)
        is_demo = True

    if is_demo:
        st.warning(
            "**Demo Mode** — showing synthetic data to illustrate all calculations. "
            "Switch to **Live Data** in the sidebar to see real channel numbers.",
            icon="🧪",
        )
    elif not has_real_data:
        st.info(empty_copy)

    # --- YPP progress (live data only) ---
    if source_mode == "Live Data" and has_real_data:
        total_365 = _get_qualifying_hours_last_365(
            db_path, channel, as_of=effective_as_of
        )
        adv_hours, has_adv_data = _get_advertising_watch_hours(
            db_path,
            channel,
            as_of=effective_as_of,
            daily_metrics_are_increments=daily_metrics_are_increments,
        )
        shorts_hours = _get_shorts_watch_hours(
            db_path,
            channel,
            as_of=effective_as_of,
            daily_metrics_are_increments=daily_metrics_are_increments,
        )
        earliest, latest = _get_db_date_range(
            db_path, channel, as_of=effective_as_of
        )
        est_qualifying = max(total_365 - adv_hours - shorts_hours, 0.0) if has_adv_data else max(total_365 - shorts_hours, 0.0)

        st.subheader("YouTube Partner Program Progress")

        if has_adv_data:
            source_label = "SIMULATED" if fixed_data_source else "API_ACTUAL"
            ypp_col1, ypp_col2, ypp_col3, ypp_col4, ypp_col5 = st.columns(5)
            ypp_col1.metric(
                "Total Watch Hours",
                f"{total_365:,.0f} hrs",
                delta=f"{earliest} → {latest}" if earliest else None,
                delta_color="off",
                help="Sum of daily_channel_metrics — includes Shorts and paid promotion.",
            )
            ypp_col2.metric(
                "Promotion (ADVERTISING)",
                f"−{adv_hours:,.0f} hrs",
                delta=source_label,
                delta_color="off",
                help="Sum of insightTrafficSourceType=ADVERTISING minutes watched / 60 "
                     "across all videos in the trailing 365-day window. These hours do not count toward YPP.",
            )
            ypp_col3.metric(
                "Shorts (excluded)",
                f"−{shorts_hours:,.0f} hrs",
                delta_color="off",
                help="Watch hours on videos ≤180s in the trailing 365-day window. Shorts never count "
                     "toward the YPP watch-hours requirement, promoted or not.",
            )
            ypp_col4.metric(
                "Est. Qualifying Hours",
                f"{est_qualifying:,.0f} hrs",
                help="Total watch hours minus ADVERTISING watch hours minus Shorts watch hours.",
            )
            ypp_col5.metric(
                "YPP Threshold",
                f"{_YPP_WATCH_HOURS_THRESHOLD:,} hrs",
                help="YouTube Partner Program requires 3,000 valid public watch hours in the last 365 days.",
            )
            st.progress(min(est_qualifying / _YPP_WATCH_HOURS_THRESHOLD, 1.0))
            if fixed_data_source:
                st.caption(
                    f"Simulated qualifying = {total_365:,.0f} total − {adv_hours:,.0f} "
                    f"promotion − {shorts_hours:,.0f} Shorts = **{est_qualifying:,.0f} hrs**."
                )
            else:
                st.caption(
                    f"Est. qualifying = {total_365:,.0f} total − {adv_hours:,.0f} ADVERTISING − {shorts_hours:,.0f} Shorts "
                    f"= **{est_qualifying:,.0f} hrs** (API_ACTUAL). Verify in **YouTube Studio → Earn** — YouTube's own "
                    "eligibility figure may still differ (e.g. Content ID claims, reused/duplicate content)."
                )

            st.markdown("---")
            st.markdown("**12-Month Growth Target: 4,000 hrs**")
            bon_c1, bon_c2, bon_c3 = st.columns(3)
            bon_c1.metric(
                "Est. Qualifying Hours",
                f"{est_qualifying:,.0f} hrs",
            )
            bon_c2.metric(
                "12-Month Target",
                f"{_BONUS_THRESHOLD:,} hrs",
                help="Internal growth target: reach 4,000 qualifying watch hours to build a stronger catalog buffer above the YPP minimum.",
            )
            _gap_4k = max(_BONUS_THRESHOLD - est_qualifying, 0.0)
            bon_c3.metric(
                "Gap to Target",
                f"{_gap_4k:,.0f} hrs remaining" if _gap_4k > 0 else "Target reached ✅",
            )
            st.progress(min(est_qualifying / _BONUS_THRESHOLD, 1.0))
        else:
            ypp_col1, ypp_col2, ypp_col3, ypp_col4 = st.columns(4)
            ypp_col1.metric(
                "Total Watch Hours (DB)",
                f"{total_365:,.0f} hrs",
                delta=f"covers {earliest} → {latest}" if earliest else None,
                delta_color="off",
                help="Sum of daily_channel_metrics.",
            )
            ypp_col2.metric(
                "Shorts (excluded)",
                f"−{shorts_hours:,.0f} hrs",
                delta_color="off",
                help="Watch hours on videos ≤180s in the trailing 365-day window. Shorts never count "
                     "toward the YPP watch-hours requirement.",
            )
            ypp_col3.metric(
                "Est. Qualifying Hours",
                f"{est_qualifying:,.0f} hrs",
                help="Total watch hours minus Shorts watch hours. No ADVERTISING data yet, "
                     "so promotion watch time is not netted out — this will overstate "
                     "qualifying hours for any promoted channel.",
            )
            ypp_col4.metric(
                "YPP Watch Hours Threshold",
                f"{_YPP_WATCH_HOURS_THRESHOLD:,} hrs",
                help="YouTube Partner Program requires 3,000 valid public watch hours in the last 365 days.",
            )
            st.progress(min(est_qualifying / _YPP_WATCH_HOURS_THRESHOLD, 1.0))
            if fixed_data_source:
                st.info(
                    "The simulated selection contains no promotion watch-time rows.",
                    icon="ℹ",
                )
            else:
                st.info(
                    "ADVERTISING watch time data not yet available. "
                    "Run the fetch job to populate `video_traffic_source_metrics` — "
                    "qualifying hours will then also net out promotion watch time.",
                    icon="ℹ",
                )
                st.caption(
                    "⚠ Est. qualifying above excludes Shorts but not promotion watch time yet. "
                    "For the exact YPP-eligible count, check **YouTube Studio → Earn**."
                )

            st.markdown("---")
            st.markdown("**12-Month Growth Target: 4,000 hrs**")
            bon_c1, bon_c2, bon_c3 = st.columns(3)
            bon_c1.metric("Est. Qualifying Hours", f"{est_qualifying:,.0f} hrs")
            bon_c2.metric(
                "12-Month Target",
                f"{_BONUS_THRESHOLD:,} hrs",
                help="Internal growth target: 4,000 qualifying watch hours within 12 months of channel launch.",
            )
            _gap_4k = max(_BONUS_THRESHOLD - est_qualifying, 0.0)
            bon_c3.metric(
                "Gap to Target",
                f"{_gap_4k:,.0f} hrs remaining" if _gap_4k > 0 else "Target reached ✅",
            )
            st.progress(min(est_qualifying / _BONUS_THRESHOLD, 1.0))

    # --- Sidebar filters ---
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
        if all_campaigns:
            sel_campaigns = st.multiselect("Campaign", all_campaigns, default=all_campaigns, key="qwh_campaigns")
        else:
            sel_campaigns = []
        if all_playlists:
            sel_playlists = st.multiselect("Playlist", all_playlists, default=all_playlists, key="qwh_playlists")
        else:
            sel_playlists = []
        if all_series:
            sel_series = st.multiselect("Series", all_series, default=all_series, key="qwh_series")
        else:
            sel_series = []
        if all_languages:
            sel_languages = st.multiselect("Language", all_languages, default=all_languages, key="qwh_languages")
        else:
            sel_languages = []
        promo_status = st.selectbox(
            "Promotion Status",
            ["All", "Promoted only", "Organic only"],
            key="qwh_promo_status",
        )

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
    total_watch_hrs = sum(m.total_watch_hours for m in metrics)
    total_promo_cost = sum(m.promotion_cost for m in metrics)
    total_cost_per_qual = (
        total_promo_cost / report.estimated_qualifying_hours
        if report.estimated_qualifying_hours > 0 and total_promo_cost > 0
        else 0.0
    )

    st.subheader("Overview")
    ov1, ov2, ov3 = st.columns(3)
    ov4, ov5, ov6 = st.columns(3)

    ov1.metric(
        "Est. Qualifying Hours",
        f"{report.estimated_qualifying_hours:,.1f} hrs",
        help="Organic watch hours minus organic Shorts watch hours",
    )
    ov2.metric(
        "Promotion Watch Hours",
        f"{report.promotion_watch_hours:,.1f} hrs",
        delta=f"-{report.promotion_pct:.1f}% of total" if report.promotion_watch_hours > 0 else "0% — no promotion data",
        delta_color="inverse" if report.promotion_watch_hours > 0 else "off",
    )
    ov3.metric(
        "Total Watch Hours",
        f"{total_watch_hrs:,.1f} hrs",
        help="Promotion watch hours plus organic watch hours",
    )
    ov4.metric(
        "Organic Watch Hours",
        f"{report.organic_watch_hours:,.1f} hrs",
        help="Total watch hours minus promotion watch hours; includes organic Shorts",
    )
    ov5.metric(
        "Avg View Duration",
        _fmt_duration(int(report.avg_organic_view_duration_seconds)),
    )
    ov6.metric(
        "Organic Shorts Excluded",
        f"{max(report.organic_watch_hours - report.estimated_qualifying_hours, 0):,.1f} hrs",
        delta=(
            f"${total_cost_per_qual:,.2f} / qualifying hr"
            if total_cost_per_qual > 0
            else (
                "Cost inputs are not included in this simulation"
                if fixed_data_source
                else "Upload promotion CSV to calculate"
            )
        ),
        delta_color="off",
    )

    if source_mode == "Live Data" and report.promotion_watch_hours == 0:
        st.info(
            (
                "The simulated dataset contains no promotion watch time for this selection."
                if fixed_data_source
                else NO_PROMOTION_DATA_MESSAGE
            ),
            icon="ℹ️",
        )

    # --- Projections (live data only) ---
    if source_mode == "Live Data" and has_real_data and total_watch_hrs > 0:
        # Use the same trailing-year promotion and Shorts exclusions as the YPP panel,
        # rather than applying the all-time per-video mix to recent daily rates.
        _qual_ratio_proj = est_qualifying / max(total_365, 1.0)
        _render_projections(
            db_path, _qual_ratio_proj, channel, as_of=effective_as_of
        )

    # --- Charts ---
    st.subheader("Charts")
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
        "Total Watch Hours", "Promotion Watch Hours", "Organic Watch Hours",
        "Est. Qualifying Hours",
        "Promotion %", "Subscribers", "Follow-on Views",
        "Promotion Cost", "Cost / Qualifying Hour", "Cost / Subscriber", "Cost / Follow-on View",
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
            "Cost / Qualifying Hour": st.column_config.NumberColumn(format="$%.2f"),
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

    # --- Future API connections notice (production only) ---
    if not fixed_data_source:
        with st.expander("Connect Real Promotion Data", expanded=False):
            st.markdown("""
**Upload Promotion CSV** — drop a CSV from Google Ads / YouTube Studio.
Use the **Upload Promotion CSV** option in the sidebar.
Expected columns: `video_id, campaign, cost_usd, views` (paid views).

**YouTube Analytics API** — watch time broken out by traffic source (paid vs organic).
Wire up `services/youtube_analytics.YouTubeAnalyticsAPIAdapter` with OAuth credentials.

**Google Ads API** — campaign spend, paid views, CPV by video.
Wire up `services/google_ads.GoogleAdsAPIAdapter` with a manager account client.
            """)
