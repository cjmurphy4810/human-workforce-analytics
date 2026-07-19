def test_video_render_loaders_take_channel_param():
    source = open("pages/video_render_comparisons.py").read()
    assert "def _load_playlist_videos(channel: str)" in source
    assert "def _load_all_videos(channel: str)" in source
    assert "def _qual_ratio(channel: str)" in source
