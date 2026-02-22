"""
Price enrichment and QQQ hedge: add returns and excess returns to classified events.
"""

from __future__ import annotations

from typing import Final

import pandas as pd

from hedging_pipeline.config import (
    BARS_CLOSE_COL,
    BARS_DATE_COL,
    BARS_OPEN_COL,
    BARS_SYMBOL_COL,
    COL_ANN_DATE,
    COL_EFF_DATE,
    COL_TICKER,
    HEDGE_SYMBOL,
)
from hedging_pipeline.logging_config import logger

COL_ENTRY_DATE: Final[str] = "entry_date"
COL_EXIT_DATE: Final[str] = "exit_date"
COL_ENTRY_OPEN: Final[str] = "entry_open"
COL_EXIT_CLOSE: Final[str] = "exit_close"
COL_QQQ_ENTRY_OPEN: Final[str] = "qqq_entry_open"
COL_QQQ_EXIT_CLOSE: Final[str] = "qqq_exit_close"
COL_HOLDING_PERIOD_DAYS: Final[str] = "holding_period_trading_days"
COL_STOCK_RETURN: Final[str] = "stock_return"
COL_QQQ_RETURN: Final[str] = "qqq_return"
COL_EXCESS_RETURN: Final[str] = "excess_return"
COL_FIRST_DAY_RETURN: Final[str] = "first_day_return"


def _first_trading_day_after(
    bars: pd.DataFrame,
    symbol: str,
    after_date: pd.Timestamp,
) -> pd.Timestamp | None:
    """First trading day strictly after after_date for symbol."""
    sym_bars = bars[bars[BARS_SYMBOL_COL] == symbol].sort_values(BARS_DATE_COL)
    later = sym_bars[sym_bars[BARS_DATE_COL] > after_date]
    if later.empty:
        return None
    return later[BARS_DATE_COL].iloc[0]


def _trading_day_on_or_before(
    bars: pd.DataFrame,
    symbol: str,
    on_date: pd.Timestamp,
) -> pd.Timestamp | None:
    """Latest trading day <= on_date for symbol (for exit close)."""
    sym_bars = bars[bars[BARS_SYMBOL_COL] == symbol].sort_values(BARS_DATE_COL)
    on_or_before = sym_bars[sym_bars[BARS_DATE_COL] <= on_date]
    if on_or_before.empty:
        return None
    return on_or_before[BARS_DATE_COL].iloc[-1]


def _price_on_date(
    bars: pd.DataFrame,
    symbol: str,
    dt: pd.Timestamp,
    price_col: str,
) -> float | None:
    """Get open or close for symbol on date dt."""
    row = bars[(bars[BARS_SYMBOL_COL] == symbol) & (bars[BARS_DATE_COL] == dt)]
    if row.empty:
        return None
    return float(row[price_col].iloc[0])


def _first_day_return_for_symbol(
    bars: pd.DataFrame,
    symbol: str,
    entry_date: pd.Timestamp,
) -> float | None:
    """Return (close - open) / open on entry_date for symbol."""
    row = bars[(bars[BARS_SYMBOL_COL] == symbol) & (bars[BARS_DATE_COL] == entry_date)]
    if row.empty or row[BARS_OPEN_COL].iloc[0] == 0:
        return None
    o = float(row[BARS_OPEN_COL].iloc[0])
    c = float(row[BARS_CLOSE_COL].iloc[0])
    return (c - o) / o


class PriceEnricher:
    """
    Enriches events with entry/exit dates, prices, holding period,
    stock return, QQQ return, excess return, and first-day return.
    """

    def __init__(self, *, hedge_symbol: str = HEDGE_SYMBOL) -> None:
        self.hedge_symbol: str = hedge_symbol

    def enrich(self, events: pd.DataFrame, bars: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich each event. Entry = first trading day after ann (open);
        exit = last trading day on or before eff (close).
        """
        df = events.copy()
        df[COL_ANN_DATE] = pd.to_datetime(df[COL_ANN_DATE])
        df[COL_EFF_DATE] = pd.to_datetime(df[COL_EFF_DATE])

        entry_dates: list[pd.Timestamp | None] = []
        exit_dates: list[pd.Timestamp | None] = []
        entry_opens: list[float | None] = []
        exit_closes: list[float | None] = []
        qqq_entry_opens: list[float | None] = []
        qqq_exit_closes: list[float | None] = []
        holding_days: list[int | None] = []
        stock_returns: list[float | None] = []
        qqq_returns: list[float | None] = []
        excess_returns: list[float | None] = []
        first_day_returns: list[float | None] = []

        for _, row in df.iterrows():
            ticker = str(row[COL_TICKER])
            ann = row[COL_ANN_DATE]
            eff = row[COL_EFF_DATE]

            entry_d = _first_trading_day_after(bars, ticker, ann)
            exit_d = _trading_day_on_or_before(bars, ticker, eff) if eff is not None else None
            if entry_d is None or exit_d is None or exit_d < entry_d:
                entry_dates.append(None)
                exit_dates.append(None)
                entry_opens.append(None)
                exit_closes.append(None)
                qqq_entry_opens.append(None)
                qqq_exit_closes.append(None)
                holding_days.append(None)
                stock_returns.append(None)
                qqq_returns.append(None)
                excess_returns.append(None)
                first_day_returns.append(None)
                if entry_d is None:
                    logger.debug("No entry date for %s after %s", ticker, ann)
                elif exit_d is None or exit_d < entry_d:
                    logger.debug("No valid exit for %s (eff %s)", ticker, eff)
                continue

            qqq_entry_d = _first_trading_day_after(bars, self.hedge_symbol, ann)
            qqq_exit_d = _trading_day_on_or_before(bars, self.hedge_symbol, eff)
            if qqq_entry_d is None or qqq_exit_d is None:
                qqq_entry_d = qqq_entry_d or entry_d
                qqq_exit_d = qqq_exit_d or exit_d

            entry_open = _price_on_date(bars, ticker, entry_d, BARS_OPEN_COL)
            exit_close = _price_on_date(bars, ticker, exit_d, BARS_CLOSE_COL)
            qqq_entry_open = _price_on_date(bars, self.hedge_symbol, qqq_entry_d, BARS_OPEN_COL)
            qqq_exit_close = _price_on_date(bars, self.hedge_symbol, qqq_exit_d, BARS_CLOSE_COL)

            entry_dates.append(entry_d)
            exit_dates.append(exit_d)
            entry_opens.append(entry_open)
            exit_closes.append(exit_close)
            qqq_entry_opens.append(qqq_entry_open)
            qqq_exit_closes.append(qqq_exit_close)

            sym_bars = bars[
                (bars[BARS_SYMBOL_COL] == ticker)
                & (bars[BARS_DATE_COL] >= entry_d)
                & (bars[BARS_DATE_COL] <= exit_d)
            ]
            holding_days.append(len(sym_bars) if not sym_bars.empty else None)

            stock_ret = (
                (exit_close - entry_open) / entry_open
                if entry_open and exit_close and entry_open != 0
                else None
            )
            qqq_ret = (
                (qqq_exit_close - qqq_entry_open) / qqq_entry_open
                if qqq_entry_open and qqq_exit_close and qqq_entry_open != 0
                else None
            )
            excess_ret = (
                (stock_ret - qqq_ret) if stock_ret is not None and qqq_ret is not None else None
            )

            stock_returns.append(stock_ret)
            qqq_returns.append(qqq_ret)
            excess_returns.append(excess_ret)
            first_day_returns.append(_first_day_return_for_symbol(bars, ticker, entry_d))

        df[COL_ENTRY_DATE] = entry_dates
        df[COL_EXIT_DATE] = exit_dates
        df[COL_ENTRY_OPEN] = entry_opens
        df[COL_EXIT_CLOSE] = exit_closes
        df[COL_QQQ_ENTRY_OPEN] = qqq_entry_opens
        df[COL_QQQ_EXIT_CLOSE] = qqq_exit_closes
        df[COL_HOLDING_PERIOD_DAYS] = holding_days
        df[COL_STOCK_RETURN] = stock_returns
        df[COL_QQQ_RETURN] = qqq_returns
        df[COL_EXCESS_RETURN] = excess_returns
        df[COL_FIRST_DAY_RETURN] = first_day_returns

        missing = int(df[COL_EXCESS_RETURN].isna().sum())
        if missing > 0:
            logger.warning(
                "%d events could not be fully enriched (missing prices or dates)", missing
            )
        return df
