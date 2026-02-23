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


class BaseLoader:
    """Base for file-based data loaders: path resolution and existence check."""

    @staticmethod
    def _resolve_path(path: Path | str) -> Path:
        return Path(path).resolve()

    @staticmethod
    def _ensure_exists(path: Path, label: str = "File") -> None:
        if not path.exists():
            raise FileNotFoundError(f"{label} not found: {path}")

    def load_normalized(self, path: Path | str) -> pd.DataFrame:
        """Load from path and return normalized DataFrame. Override in subclasses."""
        raise NotImplementedError("load_normalized must be implemented by subclass")


class EventsLoader(BaseLoader):
    """Loads and normalizes NASDAQ-100 event Excel to one row per stock action."""

    def __init__(
        self,
        *,
        events_sheet: None | int | str = None,
        required_event_columns: list[str] | None = None,
        optional_event_columns: list[str] | None = None,
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

    def load_events(self, path: Path | str) -> pd.DataFrame:
        """
        Load the NASDAQ-100 events Excel file and validate required columns.
        Does not normalize to one row per action; use normalize_events() for that.
        """
        path = self._resolve_path(path)
        self._ensure_exists(path, "Events file")

        logger.info("Loading events from %s", path)
        df: pd.DataFrame = pd.read_excel(path, sheet_name=self.events_sheet or 0)

        missing: list[str] = [col for col in self.required_event_columns if col not in df.columns]
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
        for _, row in raw_events.iterrows():
            ann = row[_ANN_COL]
            eff = row[_EFF_COL]
            ev_type = str(row[_TYPE_COL]).strip().lower() if pd.notna(row[_TYPE_COL]) else ""
            trade_mm = row[_TRADE_COL] if _TRADE_COL in raw_events.columns else None

            for action, ticker_col in [("add", _ADD_COL), ("del", _DEL_COL)]:
                ticker = row[ticker_col]
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

    def load_normalized(self, path: Path | str) -> pd.DataFrame:
        """Load from path and return normalized one-row-per-action events DataFrame."""
        raw = self.load_events(path)
        return self.normalize_events(raw)

    def load_and_normalize_events(self, path: Path | str) -> pd.DataFrame:
        """Load events and return normalized one-row-per-action DataFrame."""
        return self.load_normalized(path)


class DailyBarsLoader(BaseLoader):
    """Loads daily OHLCV parquet and normalizes column names and dtypes."""

    def __init__(
        self,
        *,
        bars_column_map: dict[str, str] | None = None,
    ) -> None:
        self.bars_column_map: dict[str, str] = bars_column_map or dict(BARS_COLUMN_MAP)

    def load_daily_bars(self, path: Path | str) -> pd.DataFrame:
        """
        Load daily OHLCV parquet and normalize column names to lowercase convention.
        """
        path = self._resolve_path(path)
        self._ensure_exists(path, "Daily bars file")

        logger.info("Loading daily bars from %s", path)
        df = pd.read_parquet(path)

        rename = {
            raw_name: norm_name
            for raw_name, norm_name in self.bars_column_map.items()
            if raw_name in df.columns
        }
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

    def load_normalized(self, path: Path | str) -> pd.DataFrame:
        """Load from path and return normalized daily bars DataFrame."""
        return self.load_daily_bars(path)


class PipelineLoader:
    """
    Composite loader for the pipeline: delegates to EventsLoader and DailyBarsLoader.
    Single entry point for load_all, load_events, load_daily_bars, load_and_normalize_events.
    """

    def __init__(
        self,
        *,
        events_sheet: None | int | str = None,
        required_event_columns: list[str] | None = None,
        optional_event_columns: list[str] | None = None,
        bars_column_map: dict[str, str] | None = None,
    ) -> None:
        self._events_loader = EventsLoader(
            events_sheet=events_sheet,
            required_event_columns=required_event_columns,
            optional_event_columns=optional_event_columns,
        )
        self._bars_loader = DailyBarsLoader(bars_column_map=bars_column_map)

    def load_events(self, path: Path | str) -> pd.DataFrame:
        """Load raw events Excel (no normalization)."""
        return self._events_loader.load_events(path)

    def normalize_events(self, raw_events: pd.DataFrame) -> pd.DataFrame:
        """Normalize raw events to one row per stock action."""
        return self._events_loader.normalize_events(raw_events)

    def load_and_normalize_events(self, path: Path | str) -> pd.DataFrame:
        """Load events and return normalized one-row-per-action DataFrame."""
        return self._events_loader.load_normalized(path)

    def load_daily_bars(self, path: Path | str) -> pd.DataFrame:
        """Load daily bars parquet."""
        return self._bars_loader.load_normalized(path)

    def load_all(
        self,
        events_path: Path | str,
        bars_path: Path | str,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Load and normalize events and load daily bars. Returns (events_df, bars_df)."""
        events = self._events_loader.load_normalized(events_path)
        bars = self._bars_loader.load_normalized(bars_path)
        return events, bars


# Backward compatibility: pipeline and tests use this name
EventLoader = PipelineLoader
