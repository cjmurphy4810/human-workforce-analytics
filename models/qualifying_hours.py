"""Summary report model for qualifying watch hours."""
from __future__ import annotations

import dataclasses
from datetime import date
from typing import Optional


@dataclasses.dataclass
class QualifyingHoursReport:
    estimated_qualifying_hours: float
    promotion_watch_hours: float
    organic_watch_hours: float
    promotion_pct: float
    avg_organic_view_duration_seconds: float
    hours_lost_to_promotion: float
    as_of_date: Optional[date] = None
