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
