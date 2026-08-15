from pathlib import Path
import sys

import streamlit as st


PACKAGE_ROOT = str(Path(__file__).resolve().parents[1])
if PACKAGE_ROOT not in sys.path:
    sys.path.insert(0, PACKAGE_ROOT)

from demo.ui import configure_page  # noqa: E402


configure_page("AI Engineering Genius")

pages = {
    "Channel Analytics": [
        st.Page("pages/overview.py", title="Overview", icon="📊", default=True),
        st.Page("pages/daily_analytics.py", title="Daily Analytics", icon="📅"),
        st.Page(
            "pages/qualifying_watch_hours.py",
            title="Qualifying Watch Hours",
            icon="⏱️",
        ),
        st.Page("pages/organic_momentum.py", title="Organic Momentum", icon="🌱"),
        st.Page(
            "pages/promotion_intelligence.py",
            title="Promotion Intelligence",
            icon="📣",
        ),
        st.Page(
            "pages/content_intelligence.py",
            title="Content Intelligence",
            icon="🧠",
        ),
        st.Page(
            "pages/video_render_comparisons.py",
            title="Video Render Comparisons",
            icon="🎬",
        ),
    ]
}

st.navigation(pages, position="sidebar").run()
