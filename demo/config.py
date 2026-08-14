from datetime import date
from pathlib import Path

DEMO_CHANNEL_KEY = "ai_engineering_genius"
DEMO_CHANNEL_NAME = "AI Engineering Genius"
DEMO_AS_OF = date(2026, 8, 14)
DEMO_DB_PATH = Path(__file__).parent / "data" / "demo.db"
DISABLED_CHANNELS = (
    "Automation Architects",
    "Future Systems Lab",
    "Practical AI Studio",
)
