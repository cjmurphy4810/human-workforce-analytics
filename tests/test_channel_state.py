from channel_state import CHANNELS, DEFAULT_CHANNEL


def test_channels_registry_has_three_entries():
    assert CHANNELS == {
        "human_workforce": "The Human Workforce",
        "club_genius": "Club Genius Stories",
        "kzak": "KZAK Music Videos",
    }


def test_default_channel_is_human_workforce():
    assert DEFAULT_CHANNEL == "human_workforce"
    assert DEFAULT_CHANNEL in CHANNELS
