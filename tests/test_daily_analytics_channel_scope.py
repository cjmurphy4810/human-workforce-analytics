def test_daily_analytics_loaders_take_channel_param():
    source = open("pages/daily_analytics.py").read()
    assert "def _load_daily(channel: str)" in source
    assert "def _load_video_daily(channel: str)" in source
    assert "def _get_qual_ratio(channel: str)" in source
