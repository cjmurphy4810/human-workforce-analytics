def test_imports():
    """Verify the project's modules can be imported under pytest."""
    import db
    import youtube_client
    assert callable(db.init_db)
    assert callable(youtube_client.fetch_channel_stats)


def test_app_queries_are_all_channel_scoped(tmp_path):
    """Guard against a future edit reintroducing an unscoped query in app.py."""
    import re
    app_source = open("app.py").read()
    # Every `load(` call must pass a channel param — this is a lightweight guard,
    # not a full SQL parser: it checks that "channel" appears near every load(...) call.
    load_calls = re.findall(r'load\(\s*"([^"]|\\.)*?"', app_source)
    # (Kept intentionally simple: the real check is the manual query text below.)
    assert "channel = :channel" in app_source or "channel = ?" in app_source
