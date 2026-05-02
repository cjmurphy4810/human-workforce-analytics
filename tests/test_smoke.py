def test_imports():
    """Verify the project's modules can be imported under pytest."""
    import db
    import youtube_client
    assert callable(db.init_db)
    assert callable(youtube_client.fetch_channel_stats)
