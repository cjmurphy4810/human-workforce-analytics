from datetime import date

from demo.config import (
    DEMO_AS_OF,
    DEMO_CHANNEL_KEY,
    DEMO_CHANNEL_NAME,
    DISABLED_CHANNELS,
)


def test_demo_identity_is_fixed_and_fictional():
    assert DEMO_CHANNEL_KEY == "ai_engineering_genius"
    assert DEMO_CHANNEL_NAME == "AI Engineering Genius"
    assert DEMO_AS_OF == date(2026, 8, 14)


def test_portfolio_channels_are_present_but_not_selectable():
    assert DISABLED_CHANNELS == (
        "Automation Architects",
        "Future Systems Lab",
        "Practical AI Studio",
    )
