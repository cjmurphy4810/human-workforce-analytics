def test_build_real_features_takes_channel_param():
    source = open("pages/promotion_intelligence.py").read()
    assert "def _build_real_features(db: Path, cpv: float, channel: str)" in source


def test_build_real_features_queries_are_channel_scoped():
    source = open("pages/promotion_intelligence.py").read()
    assert "from channel_state import render_channel_selector" in source
    assert "_active_channel = render_channel_selector()" in source
    assert "_build_real_features(_DB, cpv, channel=_active_channel)" in source
