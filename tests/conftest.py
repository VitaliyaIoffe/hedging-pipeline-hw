"""Shared pytest fixtures and configuration."""

from pathlib import Path

import pandas as pd
import pytest

from hedging_pipeline.classification import EventClassifier
from hedging_pipeline.config import BARS_CLOSE_COL, BARS_OPEN_COL, HEDGE_SYMBOL
from hedging_pipeline.enrichment import PriceEnricher
from hedging_pipeline.loaders import EventLoader
from hedging_pipeline.summary import SummaryStats

# ----- Loaders -----


@pytest.fixture
def loader() -> EventLoader:
    return EventLoader()


@pytest.fixture
def minimal_raw_events_df() -> pd.DataFrame:
    """Raw events table (Excel schema): 2 events, required + optional columns."""
    return pd.DataFrame(
        {
            "ANN DATE AFTER CLOSE": pd.to_datetime(["2020-06-01", "2020-12-10"]),
            "EFF DATE MORNING OF": pd.to_datetime(["2020-06-10", "2020-12-20"]),
            "add": ["TICK1", "TICK2"],
            "del": ["OLD1", "OLD2"],
            "type": ["adhoc", "ANNUAL"],
            "TRADE EST MM": [1.0, 2.0],
        }
    )


@pytest.fixture
def minimal_bars_df() -> pd.DataFrame:
    """Minimal daily bars (normalized schema): date, symbol, open, close, volume."""
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-01-02", "2020-01-02", "2020-01-03"]),
            "symbol": ["QQQ", "AAPL", "QQQ"],
            "open": [100.0, 200.0, 101.0],
            "close": [101.0, 202.0, 102.0],
            "volume": [1_000_000, 2_000_000, 1_100_000],
        }
    )


@pytest.fixture
def events_excel_path(minimal_raw_events_df: pd.DataFrame, tmp_path: Path) -> Path:
    path = tmp_path / "events.xlsx"
    minimal_raw_events_df.to_excel(path, index=False, sheet_name="Sheet1")
    return path


# ----- Classification -----


@pytest.fixture
def classifier() -> EventClassifier:
    return EventClassifier()


# ----- Enrichment -----


@pytest.fixture
def enricher() -> PriceEnricher:
    return PriceEnricher()


@pytest.fixture
def sample_bars() -> pd.DataFrame:
    """Bars for one ticker + QQQ over a short window (enrichment tests)."""
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
    tick_opens = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0]
    tick_closes = [101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 110.0]
    qqq_opens = [200.0, 201.0, 202.0, 203.0, 204.0, 205.0, 206.0]
    qqq_closes = [201.0, 202.0, 203.0, 204.0, 205.0, 206.0, 208.0]
    rows: list[dict] = []
    for i, d in enumerate(dates):
        rows.append(
            {
                "date": d,
                "symbol": "TICK",
                BARS_OPEN_COL: tick_opens[i],
                BARS_CLOSE_COL: tick_closes[i],
                "volume": 1e6,
            }
        )
        rows.append(
            {
                "date": d,
                "symbol": HEDGE_SYMBOL,
                BARS_OPEN_COL: qqq_opens[i],
                BARS_CLOSE_COL: qqq_closes[i],
                "volume": 2e6,
            }
        )
    return pd.DataFrame(rows)


@pytest.fixture
def sample_events() -> pd.DataFrame:
    """One event: ann 2020-06-01, eff 2020-06-10, ticker TICK."""
    return pd.DataFrame(
        [
            {
                "ann_date": pd.to_datetime("2020-06-01"),
                "eff_date": pd.to_datetime("2020-06-10"),
                "ticker": "TICK",
                "action": "add",
                "event_type": "adhoc",
                "trade_est_mm": 1.0,
                "classification": "adhoc_add",
            }
        ]
    )


# ----- Summary -----


@pytest.fixture
def summary_stats() -> SummaryStats:
    return SummaryStats(outlier_std_threshold=2.0)


@pytest.fixture
def enriched_df() -> pd.DataFrame:
    """Enriched-style DataFrame for summary tests."""
    from hedging_pipeline.classification import CLASS_LABEL_COL
    from hedging_pipeline.enrichment import (
        COL_EXCESS_RETURN,
        COL_FIRST_DAY_RETURN,
        COL_HOLDING_PERIOD_DAYS,
    )

    return pd.DataFrame(
        {
            CLASS_LABEL_COL: ["adhoc_add", "adhoc_add", "adhoc_add", "annual_del", "annual_del"],
            COL_EXCESS_RETURN: [0.01, 0.02, 0.03, -0.02, 0.10],
            COL_HOLDING_PERIOD_DAYS: [5, 5, 6, 6, 6],
            COL_FIRST_DAY_RETURN: [0.0, 0.01, -0.01, 0.0, 0.02],
        }
    )
