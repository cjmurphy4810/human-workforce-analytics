from datetime import date
from unittest.mock import MagicMock, patch

import youtube_client


def _fake_retention_response(rows):
    """Return a mock that mimics analytics_service().reports().query().execute()."""
    service = MagicMock()
    service.reports().query().execute.return_value = {"rows": rows}
    return service


def test_fetch_retention_curve_extracts_25_and_75_exactly():
    """Given exact rows at 0.25 and 0.75, return those values directly."""
    rows = [[round(0.01 * i, 2), 1.0 - 0.01 * i] for i in range(101)]

    with patch("youtube_client.analytics_service") as mock_svc:
        mock_svc.return_value = _fake_retention_response(rows)
        result = youtube_client.fetch_retention_curve(
            video_id="v1", start=date(2026, 1, 1), end=date(2026, 1, 8)
        )

    assert result["video_id"] == "v1"
    assert result["window_start"] == "2026-01-01"
    assert result["window_end"] == "2026-01-08"
    assert abs(result["retention_at_25"] - 0.75) < 1e-6
    assert abs(result["retention_at_75"] - 0.25) < 1e-6


def test_fetch_retention_curve_interpolates_when_target_not_present():
    """If 0.25 lies between 0.24 and 0.26, interpolate linearly."""
    rows = [
        [0.24, 0.80],
        [0.26, 0.70],
        [0.74, 0.40],
        [0.76, 0.30],
    ]
    with patch("youtube_client.analytics_service") as mock_svc:
        mock_svc.return_value = _fake_retention_response(rows)
        result = youtube_client.fetch_retention_curve(
            video_id="v1", start=date(2026, 1, 1), end=date(2026, 1, 8)
        )

    assert abs(result["retention_at_25"] - 0.75) < 1e-6
    assert abs(result["retention_at_75"] - 0.35) < 1e-6


def test_fetch_retention_curve_caps_above_one():
    """audienceWatchRatio can exceed 1.0 due to rewatches; cap at 1.0."""
    rows = [
        [0.25, 1.4],
        [0.75, 0.5],
    ]
    with patch("youtube_client.analytics_service") as mock_svc:
        mock_svc.return_value = _fake_retention_response(rows)
        result = youtube_client.fetch_retention_curve(
            video_id="v1", start=date(2026, 1, 1), end=date(2026, 1, 8)
        )

    assert result["retention_at_25"] == 1.0
    assert result["retention_at_75"] == 0.5


def test_fetch_retention_curve_returns_none_for_empty_response():
    """Videos with too few views return no rows; we return None to signal skip."""
    with patch("youtube_client.analytics_service") as mock_svc:
        mock_svc.return_value = _fake_retention_response([])
        result = youtube_client.fetch_retention_curve(
            video_id="v1", start=date(2026, 1, 1), end=date(2026, 1, 8)
        )

    assert result is None


def _fake_data_service(items):
    """Return a mock that mimics data_service().videos().list().execute()."""
    service = MagicMock()
    service.videos().list().execute.return_value = {"items": items}
    return service


def test_fetch_video_details_includes_privacy_status():
    fake_item = {
        "id": "v1",
        "snippet": {
            "title": "Test Video",
            "description": "A description",
            "publishedAt": "2026-01-01T00:00:00Z",
            "thumbnails": {"high": {"url": "http://thumb.jpg"}},
        },
        "statistics": {"viewCount": "100", "likeCount": "10", "commentCount": "2"},
        "contentDetails": {"duration": "PT10M30S"},
        "status": {"privacyStatus": "private", "publishAt": "2026-06-15T18:00:00Z"},
    }
    with patch("youtube_client.data_service") as mock_svc:
        mock_svc.return_value = _fake_data_service([fake_item])
        result = youtube_client.fetch_video_details(["v1"])
    assert len(result) == 1
    assert result[0]["privacy_status"] == "private"
    assert result[0]["scheduled_at"] == "2026-06-15T18:00:00Z"
    assert result[0]["video_id"] == "v1"
    assert result[0]["view_count"] == 100


def test_fetch_video_details_scheduled_at_is_none_when_not_set():
    fake_item = {
        "id": "v2",
        "snippet": {
            "title": "Unscheduled Video",
            "description": "",
            "publishedAt": "2026-01-01T00:00:00Z",
            "thumbnails": {},
        },
        "statistics": {"viewCount": "0", "likeCount": "0", "commentCount": "0"},
        "contentDetails": {"duration": "PT5M"},
        "status": {"privacyStatus": "private"},
    }
    with patch("youtube_client.data_service") as mock_svc:
        mock_svc.return_value = _fake_data_service([fake_item])
        result = youtube_client.fetch_video_details(["v2"])
    assert result[0]["scheduled_at"] is None


def _fake_views_response(rows):
    service = MagicMock()
    service.reports().query().execute.return_value = {"rows": rows}
    return service


def test_fetch_video_views_in_window_returns_first_row_value():
    """Single video query returns one row [views]; we return it as int."""
    with patch("youtube_client.analytics_service") as mock_svc:
        mock_svc.return_value = _fake_views_response([[1234]])
        views = youtube_client.fetch_video_views_in_window(
            video_id="v1", start=date(2026, 1, 1), end=date(2026, 1, 8)
        )
    assert views == 1234


def test_fetch_video_views_in_window_returns_zero_when_empty():
    """No rows = no recorded views in that window."""
    with patch("youtube_client.analytics_service") as mock_svc:
        mock_svc.return_value = _fake_views_response([])
        views = youtube_client.fetch_video_views_in_window(
            video_id="v1", start=date(2026, 1, 1), end=date(2026, 1, 8)
        )
    assert views == 0


def _fake_geo_response(rows):
    """Return a mock that mimics analytics_service().reports().query().execute()."""
    service = MagicMock()
    service.reports().query().execute.return_value = {"rows": rows}
    return service


def test_fetch_daily_geo_metrics_parses_rows():
    """Each API row [country, views, subs, likes] maps to correct dict keys; metric_date is end."""
    rows = [
        ["IN", 12345, 210, 500],
        ["US", 543, 12, 30],
    ]
    with patch("youtube_client.analytics_service") as mock_svc:
        mock_svc.return_value = _fake_geo_response(rows)
        result = youtube_client.fetch_daily_geo_metrics(
            start=date(2026, 5, 1), end=date(2026, 5, 7)
        )

    assert len(result) == 2
    assert result[0] == {
        "metric_date": "2026-05-07",
        "country_code": "IN",
        "views": 12345,
        "subscribers_gained": 210,
        "likes": 500,
    }
    assert result[1]["country_code"] == "US"
    assert result[1]["views"] == 543


def test_fetch_daily_geo_metrics_returns_empty_list_when_no_rows():
    """Empty API response returns an empty list without error."""
    with patch("youtube_client.analytics_service") as mock_svc:
        mock_svc.return_value = _fake_geo_response([])
        result = youtube_client.fetch_daily_geo_metrics(
            start=date(2026, 5, 1), end=date(2026, 5, 7)
        )
    assert result == []


def test_fetch_daily_geo_metrics_uses_channel_id_when_provided():
    """When channel_id is given, the ids param is 'channel==<id>', not 'channel==MINE'."""
    with patch("youtube_client.analytics_service") as mock_svc:
        mock_svc.return_value = _fake_geo_response([])
        youtube_client.fetch_daily_geo_metrics(
            start=date(2026, 5, 1), end=date(2026, 5, 7),
            channel_id="UCHDU3z8f5_HJzJL1w2J2EaQ",
        )
        call_kwargs = mock_svc.return_value.reports.return_value.query.call_args[1]
        assert call_kwargs["ids"] == "channel==UCHDU3z8f5_HJzJL1w2J2EaQ"
