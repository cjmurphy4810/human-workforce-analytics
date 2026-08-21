from datetime import date

import pandas as pd
import pytest

from analytics.qualifying_hours import (
    compute_qualifying_hours,
    recompute_with_sim_duration,
)
from models.promotion import make_metrics
import qualifying_watch_hours as report_page


def _metric(video_id: str, *, length: int, total_hours: float, promo_hours: float):
    metric = make_metrics(
        video_id=video_id,
        title=video_id,
        published=None,
        length_seconds=length,
        total_views=100,
        promotion_views=20,
        total_watch_hours=total_hours,
        avg_promotion_view_duration_seconds=promo_hours * 3600 / 20,
        promotion_cost=0,
        subscribers=0,
        follow_on_views=0,
    )
    return metric


def test_report_distinguishes_organic_from_monetization_qualifying_hours():
    long_video = _metric("long", length=600, total_hours=10, promo_hours=2)
    short_video = _metric("short", length=120, total_hours=4, promo_hours=1)

    report = compute_qualifying_hours([long_video, short_video])

    assert report.promotion_watch_hours == pytest.approx(3)
    assert report.organic_watch_hours == pytest.approx(11)
    assert report.estimated_qualifying_hours == pytest.approx(8)


def test_simulation_keeps_shorts_excluded_from_qualifying_hours():
    short_video = _metric("short", length=120, total_hours=4, promo_hours=1)

    [result] = recompute_with_sim_duration([short_video], 90)

    assert result.organic_watch_hours == pytest.approx(3.5)
    assert result.estimated_qualifying_hours == 0


def test_metric_factory_excludes_shorts_from_qualifying_hours():
    short_video = _metric("short", length=120, total_hours=4, promo_hours=1)

    assert short_video.organic_watch_hours == pytest.approx(3)
    assert short_video.estimated_qualifying_hours == 0
    assert short_video.cost_per_qualified_hour == 0


def test_report_table_distinguishes_organic_and_qualifying_cost_labels():
    frame = report_page._to_df(
        [_metric("long", length=600, total_hours=10, promo_hours=2)]
    )

    assert "Organic Watch Hours" in frame.columns
    assert "Est. Qualifying Hours" in frame.columns
    assert "Cost / Qualifying Hour" in frame.columns
    assert "Cost / Organic Hour" not in frame.columns


def test_bubble_chart_labels_qualifying_hours_as_qualifying(monkeypatch):
    captured = []
    monkeypatch.setattr(
        report_page.st,
        "plotly_chart",
        lambda figure, **kwargs: captured.append(figure),
    )
    frame = pd.DataFrame(
        [{
            "Video": "Fixture",
            "Promotion Cost": 10.0,
            "Est. Qualifying Hours": 5.0,
            "Follow-on Views": 2,
            "Subscribers": 1,
        }]
    )

    report_page._chart_bubble(frame)

    assert captured[0].layout.yaxis.title.text == "Qualifying Watch Hours"
    assert "Qualifying" in captured[0].layout.title.text
    assert "Organic" not in captured[0].layout.title.text


def test_public_caption_names_both_qualifying_exclusions(monkeypatch, tmp_path):
    captions = []
    monkeypatch.setattr(report_page.st, "caption", captions.append)

    report_page.render(
        tmp_path / "missing.db",
        "channel-a",
        as_of=date(2026, 8, 14),
        fixed_data_source=True,
    )

    assert captions[0] == (
        "Estimates YouTube Partner Program qualifying watch hours by excluding "
        "advertising-generated watch time and watch time from Shorts."
    )


def test_no_promotion_copy_keeps_shorts_excluded():
    assert report_page.NO_PROMOTION_DATA_MESSAGE == (
        "No promotion data loaded — all non-Shorts watch hours are counting as "
        "qualifying. Upload a Promotion CSV in the sidebar to subtract "
        "promotion-generated hours."
    )


def test_advertising_hours_ignores_declining_cumulative_snapshots(tmp_path):
    """A cumulative per-video ADVERTISING snapshot that dips for many days (observed in
    production: one video fell from 26,487 to 5,779 minutes over two weeks before
    snapping back to exactly 26,487) is a temporary reporting glitch, not a real
    reduction, and must not be treated as a fresh lifetime baseline — that previously
    caused the dip's raw value to be added back in full, wildly inflating
    advertising_watch_hours above total watch hours.
    """
    import sqlite3
    from db import SCHEMA

    db_path = tmp_path / "adv.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    conn.execute(
        "INSERT INTO videos(channel, video_id, title) VALUES ('human_workforce', 'v1', 'v1')"
    )
    # Cumulative snapshots: rises, then a multi-day decline (reporting glitch), then
    # a partial recovery that never regains the prior high within this window.
    for metric_date, minutes in [
        ("2026-08-01", 1000.0),
        ("2026-08-02", 1200.0),
        ("2026-08-03", 600.0),   # glitch begins — not a fresh lifetime baseline
        ("2026-08-04", 650.0),   # still recovering, still below the prior high of 1200
    ]:
        conn.execute(
            "INSERT INTO video_traffic_source_metrics"
            "(metric_date, channel, video_id, traffic_source_type, estimated_minutes_watched) "
            "VALUES (?, 'human_workforce', 'v1', 'ADVERTISING', ?)",
            (metric_date, minutes),
        )
    conn.commit()
    conn.close()

    hours, has_data = report_page._get_advertising_watch_hours(
        db_path, "human_workforce", as_of=date(2026, 8, 4)
    )

    assert has_data
    # Window total = boundary subtraction: running-max at as_of (1200) minus running-max
    # strictly before window_start (0 — this channel is far younger than the trailing
    # 365-day window, so there is no pre-window row at all). = 1200 min = 20 hrs.
    # The old bug summed raw snapshot values on decline days too, giving 850 min (~14.17
    # hrs). A "sum of clipped daily deltas" fix (without boundary subtraction) would
    # instead only count 200 min — it credits growth day-by-day and misses that this
    # video's entire tracked history already sits inside the window.
    assert hours == pytest.approx(1200.0 / 60.0)


def test_advertising_hours_excludes_history_before_the_window(tmp_path):
    """Once a channel's tracked history extends past the trailing-365-day window, only
    growth since the window boundary should count — a pre-window baseline must be
    subtracted out, not included in full.
    """
    import sqlite3
    from db import SCHEMA

    db_path = tmp_path / "adv2.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    conn.execute(
        "INSERT INTO videos(channel, video_id, title) VALUES ('human_workforce', 'v1', 'v1')"
    )
    for metric_date, minutes in [
        ("2024-01-01", 5000.0),  # long before the window — established baseline
        ("2026-08-01", 5000.0),  # window opens ~2025-08-06; unchanged since baseline
        ("2026-08-02", 400.0),   # glitch dip
        ("2026-08-03", 5300.0),  # genuine growth of 300 min within the window
    ]:
        conn.execute(
            "INSERT INTO video_traffic_source_metrics"
            "(metric_date, channel, video_id, traffic_source_type, estimated_minutes_watched) "
            "VALUES (?, 'human_workforce', 'v1', 'ADVERTISING', ?)",
            (metric_date, minutes),
        )
    conn.commit()
    conn.close()

    hours, has_data = report_page._get_advertising_watch_hours(
        db_path, "human_workforce", as_of=date(2026, 8, 3)
    )

    assert has_data
    # end (running-max at as_of) = 5300; start (running-max before window_start) = 5000.
    # 300 min of real in-window growth = 5 hrs — the dip and the pre-window baseline
    # are both excluded correctly.
    assert hours == pytest.approx(300.0 / 60.0)
