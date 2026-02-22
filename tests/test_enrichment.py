"""Unit tests for PriceEnricher: parametrized and edge cases."""

import pandas as pd
import pytest

from hedging_pipeline.config import HEDGE_SYMBOL
from hedging_pipeline.enrichment import (
    COL_ENTRY_DATE,
    COL_EXCESS_RETURN,
    COL_FIRST_DAY_RETURN,
    COL_HOLDING_PERIOD_DAYS,
    COL_STOCK_RETURN,
    PriceEnricher,
)

# ---------- Parametrized: return math ----------


@pytest.mark.parametrize(
    "entry_open,exit_close,qqq_entry,qqq_exit,expected_stock,expected_excess",
    [
        (100.0, 110.0, 200.0, 208.0, 0.10, 0.06),  # 10% stock, 4% qqq -> 6% excess
        (50.0, 50.0, 100.0, 100.0, 0.0, 0.0),
        (100.0, 90.0, 200.0, 190.0, -0.10, -0.05),  # -10% stock, -5% qqq -> -5% excess
    ],
)
def test_enrich_return_calculation(
    enricher: PriceEnricher,
    entry_open: float,
    exit_close: float,
    qqq_entry: float,
    qqq_exit: float,
    expected_stock: float,
    expected_excess: float,
) -> None:
    events = pd.DataFrame(
        [
            {
                "ann_date": pd.Timestamp("2020-06-01"),
                "eff_date": pd.Timestamp("2020-06-10"),
                "ticker": "T",
                "action": "add",
                "event_type": "adhoc",
                "classification": "adhoc_add",
            }
        ]
    )
    dates = pd.to_datetime(
        [
            "2020-06-02",
            "2020-06-03",
            "2020-06-04",
            "2020-06-05",
            "2020-06-08",
            "2020-06-09",
            "2020-06-10",
        ]
    )
    rows = []
    for i, d in enumerate(dates):
        rows.append(
            {
                "date": d,
                "symbol": "T",
                "open": entry_open if i == 0 else 100.0,
                "close": exit_close if i == len(dates) - 1 else 101.0,
                "volume": 1e6,
            }
        )
        rows.append(
            {
                "date": d,
                "symbol": HEDGE_SYMBOL,
                "open": qqq_entry if i == 0 else 200.0,
                "close": qqq_exit if i == len(dates) - 1 else 201.0,
                "volume": 1e6,
            }
        )
    bars = pd.DataFrame(rows)
    out = enricher.enrich(events, bars)
    assert out[COL_STOCK_RETURN].iloc[0] == pytest.approx(expected_stock, rel=1e-5)
    assert out[COL_EXCESS_RETURN].iloc[0] == pytest.approx(expected_excess, rel=1e-5)


# ---------- Happy path ----------


def test_enrich_adds_columns(
    enricher: PriceEnricher,
    sample_events: pd.DataFrame,
    sample_bars: pd.DataFrame,
) -> None:
    out = enricher.enrich(sample_events, sample_bars)
    assert COL_ENTRY_DATE in out.columns
    assert COL_STOCK_RETURN in out.columns
    assert COL_EXCESS_RETURN in out.columns
    assert out[COL_ENTRY_DATE].iloc[0] == pd.Timestamp("2020-06-02")
    assert out[COL_STOCK_RETURN].iloc[0] == pytest.approx(0.10, rel=1e-5)
    assert out[COL_EXCESS_RETURN].iloc[0] == pytest.approx(0.06, rel=1e-5)


# ---------- Edge cases ----------


def test_enrich_missing_ticker_returns_nan(enricher: PriceEnricher) -> None:
    events = pd.DataFrame(
        [
            {
                "ann_date": pd.Timestamp("2020-06-01"),
                "eff_date": pd.Timestamp("2020-06-10"),
                "ticker": "NONEXISTENT",
                "action": "add",
                "event_type": "adhoc",
                "classification": "adhoc_add",
            }
        ]
    )
    bars = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-06-02"]),
            "symbol": [HEDGE_SYMBOL],
            "open": [100.0],
            "close": [101.0],
            "volume": [1e6],
        }
    )
    out = enricher.enrich(events, bars)
    assert pd.isna(out[COL_EXCESS_RETURN].iloc[0])
    assert pd.isna(out[COL_ENTRY_DATE].iloc[0])


def test_enrich_eff_before_ann_no_entry(enricher: PriceEnricher) -> None:
    """Effective date before announcement: no trading day after ann in range -> NaN."""
    events = pd.DataFrame(
        [
            {
                "ann_date": pd.Timestamp("2020-06-15"),
                "eff_date": pd.Timestamp("2020-06-10"),
                "ticker": "T",
                "action": "add",
                "event_type": "adhoc",
                "classification": "adhoc_add",
            }
        ]
    )
    bars = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-06-02", "2020-06-03", "2020-06-10"]),
            "symbol": ["T", "T", "T"],
            "open": [100.0, 101.0, 102.0],
            "close": [101.0, 102.0, 103.0],
            "volume": [1e6, 1e6, 1e6],
        }
    )
    bars_qqq = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-06-02", "2020-06-03", "2020-06-10"]),
            "symbol": [HEDGE_SYMBOL, HEDGE_SYMBOL, HEDGE_SYMBOL],
            "open": [200.0, 201.0, 202.0],
            "close": [201.0, 202.0, 203.0],
            "volume": [1e6, 1e6, 1e6],
        }
    )
    bars = pd.concat([bars, bars_qqq], ignore_index=True)
    out = enricher.enrich(events, bars)
    # Entry would be first day after 2020-06-15 -> 2020-06-16; we don't have that date. Exit 2020-06-10. So entry_d is None or exit_d < entry_d.
    assert pd.isna(out[COL_EXCESS_RETURN].iloc[0])


def test_enrich_empty_events_returns_empty(
    enricher: PriceEnricher, sample_bars: pd.DataFrame
) -> None:
    events = pd.DataFrame(
        columns=["ann_date", "eff_date", "ticker", "action", "event_type", "classification"]
    )
    out = enricher.enrich(events, sample_bars)
    assert len(out) == 0
    assert COL_EXCESS_RETURN in out.columns


def test_enrich_empty_bars_returns_nan_returns(enricher: PriceEnricher) -> None:
    events = pd.DataFrame(
        [
            {
                "ann_date": pd.Timestamp("2020-06-01"),
                "eff_date": pd.Timestamp("2020-06-10"),
                "ticker": "T",
                "action": "add",
                "event_type": "adhoc",
                "classification": "adhoc_add",
            }
        ]
    )
    bars = pd.DataFrame(columns=["date", "symbol", "open", "close", "volume"])
    out = enricher.enrich(events, bars)
    assert pd.isna(out[COL_EXCESS_RETURN].iloc[0])
    assert pd.isna(out[COL_ENTRY_DATE].iloc[0])


def test_enrich_holding_period_count(
    enricher: PriceEnricher, sample_events: pd.DataFrame, sample_bars: pd.DataFrame
) -> None:
    out = enricher.enrich(sample_events, sample_bars)
    # Entry 2020-06-02, exit 2020-06-10 -> 7 trading days
    assert out[COL_HOLDING_PERIOD_DAYS].iloc[0] == 7


def test_enrich_zero_open_first_day_does_not_crash(enricher: PriceEnricher) -> None:
    """First-day return when open=0 should not divide by zero."""
    events = pd.DataFrame(
        [
            {
                "ann_date": pd.Timestamp("2020-06-01"),
                "eff_date": pd.Timestamp("2020-06-10"),
                "ticker": "T",
                "action": "add",
                "event_type": "adhoc",
                "classification": "adhoc_add",
            }
        ]
    )
    dates = pd.to_datetime(["2020-06-02", "2020-06-10"])
    rows = [
        {"date": dates[0], "symbol": "T", "open": 0.0, "close": 100.0, "volume": 1e6},
        {"date": dates[1], "symbol": "T", "open": 100.0, "close": 110.0, "volume": 1e6},
        {"date": dates[0], "symbol": HEDGE_SYMBOL, "open": 200.0, "close": 201.0, "volume": 1e6},
        {"date": dates[1], "symbol": HEDGE_SYMBOL, "open": 201.0, "close": 208.0, "volume": 1e6},
    ]
    bars = pd.DataFrame(rows)
    out = enricher.enrich(events, bars)
    assert (
        pd.isna(out[COL_FIRST_DAY_RETURN].iloc[0])
        or out[COL_FIRST_DAY_RETURN].iloc[0] != out[COL_FIRST_DAY_RETURN].iloc[0]
    )  # NaN or no inf
    assert COL_EXCESS_RETURN in out.columns
