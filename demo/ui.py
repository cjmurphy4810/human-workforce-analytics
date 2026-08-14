import streamlit as st

from demo.config import DEMO_CHANNEL_KEY, DEMO_CHANNEL_NAME, DISABLED_CHANNELS


def configure_page(page_title: str) -> None:
    st.set_page_config(
        page_title=f"{page_title} | Channel Analytics Demo",
        page_icon="📊",
        layout="wide",
    )


def render_demo_sidebar(active_page: str) -> str:
    with st.sidebar:
        st.markdown("### Channel Portfolio")
        st.button(DEMO_CHANNEL_NAME, type="primary", use_container_width=True)
        for name in DISABLED_CHANNELS:
            st.button(name, disabled=True, use_container_width=True)
        st.caption("Additional channels can be configured through our consulting service.")
        st.divider()
        st.markdown("**Built for your channel**")
        st.caption(
            "We configure this analytics workspace around your content library, "
            "growth goals, and publishing workflow."
        )
    return DEMO_CHANNEL_KEY


def render_demo_notice() -> None:
    st.info(
        "Demo workspace — AI Engineering Genius and all displayed results are "
        "simulated. Figures illustrate product capabilities, not guaranteed outcomes.",
        icon="🧪",
    )


def render_empty_state(report_name: str) -> None:
    st.warning(f"The simulated {report_name} dataset is temporarily unavailable.")
