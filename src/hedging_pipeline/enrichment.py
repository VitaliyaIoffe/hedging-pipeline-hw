"""
Price enrichment and pluggable hedge: add returns and excess returns to classified events.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Protocol

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

# Output column names (generic: hedge_*, not qqq_*)
COL_ENTRY_DATE: Final[str] = "entry_date"
COL_EXIT_DATE: Final[str] = "exit_date"
COL_ENTRY_OPEN: Final[str] = "entry_open"
COL_EXIT_CLOSE: Final[str] = "exit_close"
COL_HEDGE_ENTRY_OPEN: Final[str] = "hedge_entry_open"
COL_HEDGE_EXIT_CLOSE: Final[str] = "hedge_exit_close"
COL_HOLDING_PERIOD_DAYS: Final[str] = "holding_period_trading_days"
COL_STOCK_RETURN: Final[str] = "stock_return"
COL_HEDGE_RETURN: Final[str] = "hedge_return"
COL_EXCESS_RETURN: Final[str] = "excess_return"
COL_FIRST_DAY_RETURN: Final[str] = "first_day_return"

# Backward compatibility for code that referenced QQQ-specific names
COL_QQQ_ENTRY_OPEN: Final[str] = COL_HEDGE_ENTRY_OPEN
COL_QQQ_EXIT_CLOSE: Final[str] = COL_HEDGE_EXIT_CLOSE
COL_QQQ_RETURN: Final[str] = COL_HEDGE_RETURN


@dataclass(frozen=True)
class HedgeResult:
    """Result of computing hedge return for one event."""

    return_pct: float | None
    entry_open: float | None
    exit_close: float | None


class HedgeStrategy(Protocol):
    """Protocol for pluggable hedge strategies."""

    def compute(
        self,
        bars: pd.DataFrame,
        entry_date: pd.Timestamp,
        exit_date: pd.Timestamp,
        ann_date: pd.Timestamp,
        eff_date: pd.Timestamp,
    ) -> HedgeResult:
        """Compute hedge return over the same window as the stock. Returns HedgeResult."""
        ...


class SingleBenchmarkHedge:
    """Hedge using a single benchmark symbol (e.g. QQQ) over the same entry/exit window."""

    def __init__(self, symbol: str = HEDGE_SYMBOL) -> None:
        self.symbol: str = symbol

    def compute(
        self,
        bars: pd.DataFrame,
        entry_date: pd.Timestamp,
        exit_date: pd.Timestamp,
        ann_date: pd.Timestamp,
        eff_date: pd.Timestamp,
    ) -> HedgeResult:
        entry_d = _first_trading_day_after(bars, self.symbol, ann_date)
        exit_d = _trading_day_on_or_before(bars, self.symbol, eff_date)
        if entry_d is None or exit_d is None or exit_d < entry_d:
            return HedgeResult(None, None, None)
        # Fallback to stock dates if benchmark has no bar on its own first/last day
        entry_d = entry_d or entry_date
        exit_d = exit_d or exit_date
        entry_open = _price_on_date(bars, self.symbol, entry_d, BARS_OPEN_COL)
        exit_close = _price_on_date(bars, self.symbol, exit_d, BARS_CLOSE_COL)
        if entry_open and exit_close and entry_open != 0:
            ret = (exit_close - entry_open) / entry_open
        else:
            ret = None
        return HedgeResult(ret, entry_open, exit_close)


class NoHedge:
    """No hedge: hedge return is 0, so excess return equals stock return."""

    def compute(
        self,
        bars: pd.DataFrame,
        entry_date: pd.Timestamp,
        exit_date: pd.Timestamp,
        ann_date: pd.Timestamp,
        eff_date: pd.Timestamp,
    ) -> HedgeResult:
        return HedgeResult(0.0, None, None)


def get_hedge_strategy(
    name: str,
    symbol: str | None = None,
) -> HedgeStrategy:
    """Factory: return a HedgeStrategy by name. Used by pipeline/config."""
    if name == "no_hedge":
        return NoHedge()
    if name == "single_benchmark":
        return SingleBenchmarkHedge(symbol=symbol or HEDGE_SYMBOL)
    raise ValueError(f"Unknown hedge strategy: {name}. Use 'single_benchmark' or 'no_hedge'.")


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
    open_price = float(row[BARS_OPEN_COL].iloc[0])
    close_price = float(row[BARS_CLOSE_COL].iloc[0])
    return (close_price - open_price) / open_price


class PriceEnricher:
    """
    Enriches events with entry/exit dates, prices, holding period,
    stock return, hedge return, excess return, and first-day return.
    Uses a pluggable HedgeStrategy (default: SingleBenchmarkHedge(QQQ)).
    """

    def __init__(
        self,
        *,
        hedge_strategy: HedgeStrategy | None = None,
    ) -> None:
        self.hedge_strategy: HedgeStrategy = (
            hedge_strategy if hedge_strategy is not None else SingleBenchmarkHedge()
        )

    def enrich(self, events: pd.DataFrame, bars: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich each event. Entry = first trading day after ann (open);
        exit = last trading day on or before eff (close).
        Hedge return and excess use the configured HedgeStrategy.
        """
        df = events.copy()
        df[COL_ANN_DATE] = pd.to_datetime(df[COL_ANN_DATE])
        df[COL_EFF_DATE] = pd.to_datetime(df[COL_EFF_DATE])

        entry_dates: list[pd.Timestamp | None] = []
        exit_dates: list[pd.Timestamp | None] = []
        entry_opens: list[float | None] = []
        exit_closes: list[float | None] = []
        hedge_entry_opens: list[float | None] = []
        hedge_exit_closes: list[float | None] = []
        holding_days: list[int | None] = []
        stock_returns: list[float | None] = []
        hedge_returns: list[float | None] = []
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
                hedge_entry_opens.append(None)
                hedge_exit_closes.append(None)
                holding_days.append(None)
                stock_returns.append(None)
                hedge_returns.append(None)
                excess_returns.append(None)
                first_day_returns.append(None)
                if entry_d is None:
                    logger.debug("No entry date for %s after %s", ticker, ann)
                elif exit_d is None or exit_d < entry_d:
                    logger.debug("No valid exit for %s (eff %s)", ticker, eff)
                continue

            entry_open = _price_on_date(bars, ticker, entry_d, BARS_OPEN_COL)
            exit_close = _price_on_date(bars, ticker, exit_d, BARS_CLOSE_COL)
            hedge_result = self.hedge_strategy.compute(bars, entry_d, exit_d, ann, eff)

            entry_dates.append(entry_d)
            exit_dates.append(exit_d)
            entry_opens.append(entry_open)
            exit_closes.append(exit_close)
            hedge_entry_opens.append(hedge_result.entry_open)
            hedge_exit_closes.append(hedge_result.exit_close)

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
            hedge_ret = hedge_result.return_pct
            excess_ret = (
                (stock_ret - hedge_ret) if stock_ret is not None and hedge_ret is not None else None
            )

            stock_returns.append(stock_ret)
            hedge_returns.append(hedge_ret)
            excess_returns.append(excess_ret)
            first_day_returns.append(_first_day_return_for_symbol(bars, ticker, entry_d))

        df[COL_ENTRY_DATE] = entry_dates
        df[COL_EXIT_DATE] = exit_dates
        df[COL_ENTRY_OPEN] = entry_opens
        df[COL_EXIT_CLOSE] = exit_closes
        df[COL_HEDGE_ENTRY_OPEN] = hedge_entry_opens
        df[COL_HEDGE_EXIT_CLOSE] = hedge_exit_closes
        df[COL_HOLDING_PERIOD_DAYS] = holding_days
        df[COL_STOCK_RETURN] = stock_returns
        df[COL_HEDGE_RETURN] = hedge_returns
        df[COL_EXCESS_RETURN] = excess_returns
        df[COL_FIRST_DAY_RETURN] = first_day_returns

        missing = int(df[COL_EXCESS_RETURN].isna().sum())
        if missing > 0:
            logger.warning(
                "%d events could not be fully enriched (missing prices or dates)", missing
            )
        return df
