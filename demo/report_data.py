"""Read-only query boundary for the deterministic public demo database."""

from __future__ import annotations

import sqlite3

import pandas as pd

from demo.config import DEMO_DB_PATH


def query_frame(sql: str, params: dict[str, object]) -> pd.DataFrame:
    """Run a parameterized query against only the packaged demo database."""
    if not DEMO_DB_PATH.exists():
        return pd.DataFrame()
    with sqlite3.connect(DEMO_DB_PATH) as conn:
        try:
            return pd.read_sql_query(sql, conn, params=params)
        except (sqlite3.Error, pd.errors.DatabaseError):
            return pd.DataFrame()
