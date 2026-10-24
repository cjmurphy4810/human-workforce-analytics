from channel_state import CHANNELS, DEFAULT_CHANNEL


def test_channels_registry_includes_the_ai_citizen():
    assert CHANNELS == {
        "human_workforce": "The Human Workforce",
        "club_genius": "Club Genius Stories",
        "kzak": "KZAK Music Videos",
        "techy_chef_tips": "Techy Chef Tips",
        "ai_citizen": "The AI Citizen",
    }


def test_default_channel_is_human_workforce():
    assert DEFAULT_CHANNEL == "human_workforce"
    assert DEFAULT_CHANNEL in CHANNELS
