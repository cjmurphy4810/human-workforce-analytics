"""
Google Ads adapter interfaces for promotion cost data.

Implementations:
  GoogleAdsCSVAdapter   – reads a CSV exported from Google Ads (default path)
  GoogleAdsAPIAdapter   – calls the Google Ads API (requires credentials)
  GoogleAdsStubAdapter  – returns empty data (no Ads account connected)

Expected CSV columns (promotion export):
  video_id, campaign, start_date, end_date, impressions, views (paid),
  cost_usd, avg_cpm, avg_cpv, ctr, subscribers_gained
"""
from __future__ import annotations

from io import StringIO
from pathlib import Path
from typing import Protocol, runtime_checkable

import pandas as pd

_REQUIRED_CSV_COLUMNS = {
    "video_id",
    "campaign",
    "cost_usd",
    "views",
}

_COLUMN_ALIASES = {
    "paid_views": "views",
    "promotion_views": "views",
    "spend": "cost_usd",
    "cost": "cost_usd",
}


@runtime_checkable
class GoogleAdsProvider(Protocol):
    def fetch_campaign_spend(self, date_range: tuple[str, str]) -> pd.DataFrame:
        """Return DataFrame with columns: campaign, date, spend_usd."""
        ...

    def fetch_video_promotion_stats(
        self,
        video_ids: list[str],
        date_range: tuple[str, str],
    ) -> pd.DataFrame:
        """Return DataFrame with columns: video_id, campaign, views, cost_usd, subscribers_gained, ctr."""
        ...


class GoogleAdsCSVAdapter:
    """Loads promotion data from a CSV file exported from Google Ads / YouTube Studio."""

    def __init__(self, csv_path: str | Path | None = None) -> None:
        self._path = Path(csv_path) if csv_path else None
        self._df: pd.DataFrame | None = None

    def load_from_file(self, path: str | Path) -> None:
        self._df = self._parse(pd.read_csv(path))

    def load_from_buffer(self, buffer: StringIO | bytes) -> None:
        self._df = self._parse(pd.read_csv(buffer))

    @staticmethod
    def _parse(df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(columns={k: v for k, v in _COLUMN_ALIASES.items() if k in df.columns})
        df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]
        missing = _REQUIRED_CSV_COLUMNS - set(df.columns)
        if missing:
            raise ValueError(f"Promotion CSV is missing required columns: {missing}")
        if "cost_usd" in df.columns:
            df["cost_usd"] = pd.to_numeric(df["cost_usd"], errors="coerce").fillna(0)
        if "views" in df.columns:
            df["views"] = pd.to_numeric(df["views"], errors="coerce").fillna(0).astype(int)
        return df

    def fetch_campaign_spend(self, date_range: tuple[str, str]) -> pd.DataFrame:
        if self._df is None or self._df.empty:
            return pd.DataFrame()
        df = self._df.copy()
        if "start_date" in df.columns:
            df = df[
                (df["start_date"] >= date_range[0]) & (df["start_date"] <= date_range[1])
            ]
        return df.groupby("campaign", as_index=False)["cost_usd"].sum()

    def fetch_video_promotion_stats(
        self,
        video_ids: list[str],
        date_range: tuple[str, str],
    ) -> pd.DataFrame:
        if self._df is None or self._df.empty:
            return pd.DataFrame()
        df = self._df[self._df["video_id"].isin(video_ids)].copy()
        agg: dict[str, object] = {"cost_usd": "sum", "views": "sum"}
        if "subscribers_gained" in df.columns:
            agg["subscribers_gained"] = "sum"
        if "ctr" in df.columns:
            agg["ctr"] = "mean"
        return df.groupby("video_id", as_index=False).agg(agg)


class GoogleAdsAPIAdapter:
    """
    Calls the Google Ads API.
    Requires a google-ads client initialized with manager account credentials.
    Not yet implemented.
    """

    def __init__(self, client) -> None:
        self._client = client

    def fetch_campaign_spend(self, date_range: tuple[str, str]) -> pd.DataFrame:
        raise NotImplementedError("Google Ads API adapter not yet wired up.")

    def fetch_video_promotion_stats(
        self,
        video_ids: list[str],
        date_range: tuple[str, str],
    ) -> pd.DataFrame:
        raise NotImplementedError("Google Ads API adapter not yet wired up.")


class GoogleAdsStubAdapter:
    """Returns empty DataFrames — use when no Ads account is connected."""

    def fetch_campaign_spend(self, date_range: tuple[str, str]) -> pd.DataFrame:
        return pd.DataFrame(columns=["campaign", "cost_usd"])

    def fetch_video_promotion_stats(
        self,
        video_ids: list[str],
        date_range: tuple[str, str],
    ) -> pd.DataFrame:
        return pd.DataFrame(columns=["video_id", "campaign", "views", "cost_usd", "subscribers_gained", "ctr"])
