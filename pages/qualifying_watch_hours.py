"""Qualifying Watch Hours — Streamlit page."""
import streamlit as st
from channel_state import render_channel_selector
from db import DB_PATH

st.set_page_config(page_title="Qualifying Watch Hours", layout="wide")

if not st.session_state.get("authenticated"):
    st.switch_page("app.py")
    st.stop()

_active_channel = render_channel_selector()

import qualifying_watch_hours as _qwh
_qwh.render(DB_PATH, _active_channel)
