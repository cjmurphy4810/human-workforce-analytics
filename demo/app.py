import streamlit as st

from demo.ui import configure_page


configure_page("AI Engineering Genius")

pages = {
    "Channel Analytics": [
        st.Page("demo/pages/overview.py", title="Overview", icon="📊", default=True),
        st.Page("demo/pages/daily_analytics.py", title="Daily Analytics", icon="📅"),
        st.Page(
            "demo/pages/qualifying_watch_hours.py",
            title="Qualifying Watch Hours",
            icon="⏱️",
        ),
        st.Page("demo/pages/organic_momentum.py", title="Organic Momentum", icon="🌱"),
        st.Page(
            "demo/pages/promotion_intelligence.py",
            title="Promotion Intelligence",
            icon="📣",
        ),
        st.Page(
            "demo/pages/content_intelligence.py",
            title="Content Intelligence",
            icon="🧠",
        ),
        st.Page(
            "demo/pages/video_render_comparisons.py",
            title="Video Render Comparisons",
            icon="🎬",
        ),
    ]
}

st.navigation(pages, position="sidebar").run()
