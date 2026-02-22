"""Performance benchmarks for pipeline stages (run with pytest -m benchmark)."""

import pandas as pd
import pytest

from hedging_pipeline.classification import EventClassifier
from hedging_pipeline.config import HEDGE_SYMBOL
from hedging_pipeline.enrichment import PriceEnricher
from hedging_pipeline.loaders import EventLoader
from hedging_pipeline.summary import SummaryStats

# Scale factors for parameterized benchmarks
BENCHMARK_SCALES = [10, 50, 200]  # events count


def _make_raw_events(n: int) -> pd.DataFrame:
    base = pd.Timestamp("2020-01-01")
    return pd.DataFrame(
        {
            "ANN DATE AFTER CLOSE": [base + pd.Timedelta(days=i * 30) for i in range(n)],
            "EFF DATE MORNING OF": [base + pd.Timedelta(days=i * 30 + 10) for i in range(n)],
            "add": [f"A{i}" for i in range(n)],
            "del": [f"D{i}" for i in range(n)],
            "type": ["adhoc"] * n,
        }
    )


def _make_bars(n_days: int, symbols: list[str]) -> pd.DataFrame:
    base = pd.Timestamp("2020-01-01")
    dates = [base + pd.Timedelta(days=i) for i in range(n_days)]
    rows = []
    for d in dates:
        for sym in symbols:
            rows.append(
                {
                    "date": d,
                    "symbol": sym,
                    "open": 100.0,
                    "close": 101.0,
                    "volume": 1_000_000,
                }
            )
    return pd.DataFrame(rows)


@pytest.mark.benchmark
@pytest.mark.parametrize("n_events", BENCHMARK_SCALES)
def test_bench_normalize_events(benchmark, n_events: int) -> None:
    """Time EventLoader.normalize_events for n raw event rows."""
    loader = EventLoader()
    raw = _make_raw_events(n_events)

    result = benchmark(loader.normalize_events, raw)

    assert len(result) == 2 * n_events


@pytest.mark.benchmark
@pytest.mark.parametrize("n_events", BENCHMARK_SCALES)
def test_bench_classify(benchmark, n_events: int) -> None:
    """Time EventClassifier.classify for n normalized event rows."""
    classifier = EventClassifier()
    raw = _make_raw_events(n_events)
    normalized = EventLoader().normalize_events(raw)
    normalized["event_type"] = normalized["event_type"].replace("", "adhoc")

    result = benchmark(classifier.classify, normalized)

    assert "classification" in result.columns


@pytest.mark.benchmark
@pytest.mark.parametrize("n_events", [5, 20, 50])
def test_bench_enrich(benchmark, n_events: int) -> None:
    """Time PriceEnricher.enrich for n events with bars covering ~1 year."""
    enricher = PriceEnricher()
    symbols = [HEDGE_SYMBOL] + [f"T{i}" for i in range(min(n_events, 20))]
    bars = _make_bars(252, symbols)  # ~1 year trading days
    events = pd.DataFrame(
        {
            "ann_date": [pd.Timestamp("2020-06-01")] * n_events,
            "eff_date": [pd.Timestamp("2020-06-20")] * n_events,
            "ticker": [f"T{i % 20}" for i in range(n_events)],
            "action": ["add"] * n_events,
            "event_type": ["adhoc"] * n_events,
            "classification": ["adhoc_add"] * n_events,
        }
    )

    result = benchmark(enricher.enrich, events, bars)

    assert len(result) == n_events
    assert "excess_return" in result.columns


@pytest.mark.benchmark
@pytest.mark.parametrize("n_rows", [100, 500, 2000])
def test_bench_summary_compute_and_flag(benchmark, n_rows: int) -> None:
    """Time SummaryStats.compute_group_summary + flag_outliers on n enriched rows."""
    from hedging_pipeline.classification import CLASS_LABEL_COL
    from hedging_pipeline.enrichment import COL_EXCESS_RETURN

    stats = SummaryStats()
    import random

    random.seed(123)
    labels = [f"group_{i % 4}" for i in range(n_rows)]
    excess = [random.gauss(0, 0.05) for _ in range(n_rows)]
    df = pd.DataFrame(
        {
            CLASS_LABEL_COL: labels,
            COL_EXCESS_RETURN: excess,
        }
    )

    def run():
        s = stats.compute_group_summary(df)
        o = stats.flag_outliers(df)
        return s, o

    result = benchmark(run)

    assert len(result[0]) <= 4
    assert len(result[1]) == n_rows


@pytest.mark.benchmark
def test_bench_load_all_small(benchmark, tmp_path) -> None:
    """Time full load_all (events Excel + bars parquet) for a small dataset."""
    loader = EventLoader()
    raw = _make_raw_events(20)
    raw.to_excel(tmp_path / "events.xlsx", index=False, sheet_name="Sheet1")
    symbols = [HEDGE_SYMBOL, "AAPL", "T1", "T2"]
    bars = _make_bars(60, symbols)
    bars_csv = bars.rename(
        columns={
            "date": "Date",
            "symbol": "Symbol",
            "open": "open_daily",
            "close": "close_daily",
            "volume": "volume_daily",
        }
    )
    bars_csv.to_parquet(tmp_path / "bars.parquet", index=False)

    def run():
        return loader.load_all(tmp_path / "events.xlsx", tmp_path / "bars.parquet")

    events, bars_out = benchmark(run)
    assert len(events) == 40
    assert len(bars_out) > 0
