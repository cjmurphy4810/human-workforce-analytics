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
