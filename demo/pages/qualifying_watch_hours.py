"""Synthetic-data entry point for the qualifying watch-hours report."""

import qualifying_watch_hours as report

from demo.config import DEMO_AS_OF, DEMO_CHANNEL_KEY, DEMO_DB_PATH
from demo.ui import render_demo_notice, render_demo_sidebar


render_demo_sidebar("Qualifying Watch Hours")
render_demo_notice()
report.render(
    DEMO_DB_PATH,
    DEMO_CHANNEL_KEY,
    as_of=DEMO_AS_OF,
    empty_message="The simulated qualifying watch-hour dataset is temporarily unavailable.",
    fixed_data_source=True,
    daily_metrics_are_increments=True,
)
