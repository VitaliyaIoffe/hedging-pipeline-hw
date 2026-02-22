"""
Data loading and normalization for NASDAQ-100 rebalancing events and daily bars.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final

import pandas as pd

from hedging_pipeline.config import (
    BARS_COLUMN_MAP,
    BARS_DATE_COL,
    BARS_SYMBOL_COL,
    COL_ACTION,
    COL_ANN_DATE,
    COL_EFF_DATE,
    COL_EVENT_TYPE,
    COL_TICKER,
    COL_TRADE_EST_MM,
    EVENTS_OPTIONAL_COLUMNS,
    EVENTS_REQUIRED_COLUMNS,
    EVENTS_SHEET_NAME,
)
from hedging_pipeline.logging_config import logger

_ANN_COL: Final[str] = "ANN DATE AFTER CLOSE"
_EFF_COL: Final[str] = "EFF DATE MORNING OF"
_ADD_COL: Final[str] = "add"
_DEL_COL: Final[str] = "del"
_TYPE_COL: Final[str] = "type"
_TRADE_COL: Final[str] = "TRADE EST MM"


class DataQualityError(Exception):
    """Raised when required columns are missing or data validation fails."""


class EventLoader:
    """Loads and normalizes NASDAQ-100 event Excel and daily bars parquet."""

    def __init__(
        self,
        *,
        events_sheet: None | int | str = None,
        required_event_columns: list[str] | None = None,
        optional_event_columns: list[str] | None = None,
        bars_column_map: dict[str, str] | None = None,
    ) -> None:
        self.events_sheet: None | int | str = (
            events_sheet if events_sheet is not None else EVENTS_SHEET_NAME
        )
        self.required_event_columns: list[str] = required_event_columns or list(
            EVENTS_REQUIRED_COLUMNS
        )
        self.optional_event_columns: list[str] = optional_event_columns or list(
            EVENTS_OPTIONAL_COLUMNS
        )
        self.bars_column_map: dict[str, str] = bars_column_map or dict(BARS_COLUMN_MAP)

    def load_events(self, path: Path | str) -> pd.DataFrame:
        """
        Load the NASDAQ-100 events Excel file and validate required columns.
        Does not normalize to one row per action; use normalize_events() for that.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Events file not found: {path}")

        logger.info("Loading events from %s", path)
        df: pd.DataFrame = pd.read_excel(path, sheet_name=self.events_sheet or 0)

        missing: list[str] = [c for c in self.required_event_columns if c not in df.columns]
        if missing:
            raise DataQualityError(
                f"Events file missing required columns: {missing}. Found: {list(df.columns)}"
            )

        for col in self.optional_event_columns:
            if col not in df.columns:
                logger.warning("Optional column '%s' missing in events file", col)

        df[_ANN_COL] = pd.to_datetime(df[_ANN_COL], errors="coerce")
        df[_EFF_COL] = pd.to_datetime(df[_EFF_COL], errors="coerce")
        null_dates = df[_ANN_COL].isna() | df[_EFF_COL].isna()
        if null_dates.any():
            logger.warning("Dropping %d event rows with invalid dates", int(null_dates.sum()))
            df = df.loc[~null_dates].copy()

        return df

    def normalize_events(self, raw_events: pd.DataFrame) -> pd.DataFrame:
        """
        Transform event table so each row is one stock action (one addition or one deletion).
        """
        rows: list[dict[str, object]] = []
        for _, r in raw_events.iterrows():
            ann = r[_ANN_COL]
            eff = r[_EFF_COL]
            ev_type = str(r[_TYPE_COL]).strip().lower() if pd.notna(r[_TYPE_COL]) else ""
            trade_mm = r[_TRADE_COL] if _TRADE_COL in raw_events.columns else None

            for action, ticker_col in [("add", _ADD_COL), ("del", _DEL_COL)]:
                ticker = r[ticker_col]
                if pd.isna(ticker) or (isinstance(ticker, str) and not ticker.strip()):
                    logger.debug("Skipping row with missing ticker for action %s", action)
                    continue
                ticker_str = str(ticker).strip().upper()
                rows.append(
                    {
                        COL_ANN_DATE: ann,
                        COL_EFF_DATE: eff,
                        COL_TICKER: ticker_str,
                        COL_ACTION: action,
                        COL_EVENT_TYPE: ev_type,
                        COL_TRADE_EST_MM: trade_mm,
                    }
                )

        out: pd.DataFrame = pd.DataFrame(rows)
        if out.empty:
            logger.warning("Normalized events DataFrame is empty")
        return out

    def load_and_normalize_events(self, path: Path | str) -> pd.DataFrame:
        """Load events and return normalized one-row-per-action DataFrame."""
        raw = self.load_events(path)
        return self.normalize_events(raw)

    def load_daily_bars(self, path: Path | str) -> pd.DataFrame:
        """
        Load daily OHLCV parquet and normalize column names to lowercase convention.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Daily bars file not found: {path}")

        logger.info("Loading daily bars from %s", path)
        df = pd.read_parquet(path)

        rename = {k: v for k, v in self.bars_column_map.items() if k in df.columns}
        if not rename:
            raise DataQualityError(
                f"Daily bars columns {list(df.columns)} do not match expected keys {list(self.bars_column_map.keys())}"
            )
        df = df.rename(columns=rename)

        if BARS_DATE_COL not in df.columns or BARS_SYMBOL_COL not in df.columns:
            raise DataQualityError(
                f"Daily bars must have '{BARS_DATE_COL}' and '{BARS_SYMBOL_COL}' after mapping."
            )

        df[BARS_DATE_COL] = pd.to_datetime(df[BARS_DATE_COL], errors="coerce")
        df = df.dropna(subset=[BARS_DATE_COL])
        df[BARS_SYMBOL_COL] = df[BARS_SYMBOL_COL].astype(str).str.strip().str.upper()
        logger.info(
            "Loaded %d bar rows, date range %s to %s",
            len(df),
            df[BARS_DATE_COL].min(),
            df[BARS_DATE_COL].max(),
        )
        return df

    def load_all(
        self,
        events_path: Path | str,
        bars_path: Path | str,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Load and normalize events and load daily bars. Returns (events_df, bars_df)."""
        events = self.load_and_normalize_events(events_path)
        bars = self.load_daily_bars(bars_path)
        return events, bars
