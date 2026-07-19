"""Shared channel registry and sidebar selector for the multi-channel dashboard.

Every report page imports `render_channel_selector()` and uses its return value
to scope every SQL query it issues — no query may read data.db without passing
this value through as the `channel` filter.
"""
import streamlit as st

CHANNELS: dict[str, str] = {
    "human_workforce": "The Human Workforce",
    "club_genius": "Club Genius Stories",
    "kzak": "KZAK Music Videos",
}

DEFAULT_CHANNEL = "human_workforce"

_SESSION_KEY = "active_channel"


def get_active_channel() -> str:
    """Return the currently selected channel key, defaulting to DEFAULT_CHANNEL."""
    return st.session_state.get(_SESSION_KEY, DEFAULT_CHANNEL)


def render_channel_selector() -> str:
    """Render the sidebar channel picker and return the selected channel key.

    Selection is stored in st.session_state so it persists across Streamlit's
    multipage navigation (each page in pages/ calls this at the top of its script).
    """
    if _SESSION_KEY not in st.session_state:
        st.session_state[_SESSION_KEY] = DEFAULT_CHANNEL

    with st.sidebar:
        st.markdown("#### Channel")
        keys = list(CHANNELS.keys())
        current = st.session_state[_SESSION_KEY]
        selected = st.radio(
            "channel",
            keys,
            index=keys.index(current),
            format_func=lambda k: CHANNELS[k],
            label_visibility="collapsed",
            key="_channel_radio",
        )
        st.session_state[_SESSION_KEY] = selected

    return st.session_state[_SESSION_KEY]
