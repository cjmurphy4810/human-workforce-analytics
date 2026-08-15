import streamlit as st

from demo.report_data import DemoDatabaseUnavailable, inspect_demo_database
from demo.ui import (
    configure_page,
    render_database_maintenance,
    render_demo_notice,
    render_demo_sidebar,
)


configure_page("AI Engineering Genius")

pages = {
    "Channel Analytics": [
        st.Page(
            "demo/pages/overview.py", title="Overview", icon="📊", default=True
        ),
        st.Page(
            "demo/pages/daily_analytics.py", title="Daily Analytics", icon="📅"
        ),
        st.Page(
            "demo/pages/qualifying_watch_hours.py",
            title="Qualifying Watch Hours",
            icon="⏱️",
        ),
        st.Page(
            "demo/pages/organic_momentum.py", title="Organic Momentum", icon="🌱"
        ),
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

navigation = st.navigation(pages, position="sidebar")
availability = inspect_demo_database()
if not availability.is_available:
    render_demo_sidebar("Maintenance")
    render_demo_notice()
    render_database_maintenance()
    st.stop()

try:
    navigation.run()
except DemoDatabaseUnavailable:
    render_database_maintenance()
    st.stop()
