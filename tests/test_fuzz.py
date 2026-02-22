"""Fuzz tests using Hypothesis: property-based tests for loaders, classification, enrichment, summary."""

from pathlib import Path

import pandas as pd
from hypothesis import given, settings
from hypothesis import strategies as st

from hedging_pipeline.classification import CLASS_LABEL_COL, EventClassifier
from hedging_pipeline.config import COL_ACTION, COL_EVENT_TYPE, HEDGE_SYMBOL
from hedging_pipeline.enrichment import COL_ENTRY_DATE, COL_EXCESS_RETURN, PriceEnricher
from hedging_pipeline.loaders import EventLoader
from hedging_pipeline.summary import IS_OUTLIER_COL, SummaryStats

# ----- Strategies -----

st_ticker = st.text(
    min_size=1, max_size=6, alphabet=st.characters(whitelist_categories=("Lu", "Nd"))
)

# ----- Loader fuzz -----


@given(n=st.integers(0, 15))
@settings(max_examples=30)
def test_normalize_events_never_crashes(n: int) -> None:
    """Normalize_events accepts 0..n raw event rows and does not crash."""
    loader = EventLoader()
    if n == 0:
        raw_df = pd.DataFrame(
            columns=["ANN DATE AFTER CLOSE", "EFF DATE MORNING OF", "add", "del", "type"]
        )
    else:
        base = pd.Timestamp("2020-01-01")
        raw_df = pd.DataFrame(
            {
                "ANN DATE AFTER CLOSE": [base + pd.Timedelta(days=i * 30) for i in range(n)],
                "EFF DATE MORNING OF": [base + pd.Timedelta(days=i * 30 + 10) for i in range(n)],
                "add": [f"A{i}" for i in range(n)],
                "del": [f"D{i}" for i in range(n)],
                "type": ["adhoc"] * n,
            }
        )
    out = loader.normalize_events(raw_df)
    assert isinstance(out, pd.DataFrame)
    if not out.empty:
        assert "ticker" in out.columns
        assert "action" in out.columns
    assert len(out) <= 2 * n


@given(n=st.integers(1, 10))
@settings(max_examples=20, deadline=None)
def test_load_daily_bars_fuzz_columns(n: int) -> None:
    """Bars with valid column names and varying rows load without crash."""
    import tempfile

    loader = EventLoader()
    dates = pd.date_range("2020-01-01", periods=n, freq="B")
    df = pd.DataFrame(
        {
            "Date": list(dates) * 2,
            "Symbol": ["QQQ"] * n + ["AAPL"] * n,
            "open_daily": [100.0] * (2 * n),
            "close_daily": [101.0] * (2 * n),
            "volume_daily": [1_000_000] * (2 * n),
        }
    )
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        path = Path(f.name)
    try:
        df.to_parquet(path, index=False)
        out = loader.load_daily_bars(path)
        assert len(out) == 2 * n
        assert "symbol" in out.columns
    finally:
        path.unlink(missing_ok=True)


# ----- Classifier fuzz -----


@given(n=st.integers(0, 50))
@settings(max_examples=20)
def test_classify_events_fuzz_length(n: int) -> None:
    """Classify accepts 0..n rows and returns same length."""
    classifier = EventClassifier()
    if n == 0:
        df = pd.DataFrame(columns=[COL_EVENT_TYPE, COL_ACTION])
    else:
        df = pd.DataFrame(
            {
                COL_EVENT_TYPE: ["adhoc"] * n,
                COL_ACTION: ["add"] * n,
            }
        )
    out = classifier.classify(df)
    assert len(out) == n
    assert CLASS_LABEL_COL in out.columns
    if n > 0:
        assert (out[CLASS_LABEL_COL] == "adhoc_add").all()


@given(et=st.sampled_from(["adhoc", "annual", "ANNUAL", ""]), ac=st.sampled_from(["add", "del"]))
def test_classify_single_row_fuzz(et: str, ac: str) -> None:
    """Single row with various type/action produces a label."""
    classifier = EventClassifier()
    df = pd.DataFrame({COL_EVENT_TYPE: [et], COL_ACTION: [ac]})
    out = classifier.classify(df)
    assert len(out) == 1
    assert "_" in out[CLASS_LABEL_COL].iloc[0]
    assert out[CLASS_LABEL_COL].iloc[0].endswith(ac)


# ----- Enricher fuzz -----


@given(n_events=st.integers(0, 5))
@settings(max_examples=15)
def test_enrich_fuzz_event_count(n_events: int) -> None:
    """Enrich with 0 to 5 events and minimal bars does not crash."""
    enricher = PriceEnricher()
    if n_events == 0:
        events = pd.DataFrame(
            columns=["ann_date", "eff_date", "ticker", "action", "event_type", "classification"]
        )
    else:
        events = pd.DataFrame(
            {
                "ann_date": [pd.Timestamp("2020-06-01")] * n_events,
                "eff_date": [pd.Timestamp("2020-06-10")] * n_events,
                "ticker": ["T"] * n_events,
                "action": ["add"] * n_events,
                "event_type": ["adhoc"] * n_events,
                "classification": ["adhoc_add"] * n_events,
            }
        )
    dates = pd.to_datetime(["2020-06-02", "2020-06-03", "2020-06-10"])
    bars = pd.DataFrame(
        [{"date": d, "symbol": "T", "open": 100.0, "close": 101.0, "volume": 1e6} for d in dates]
        + [
            {"date": d, "symbol": HEDGE_SYMBOL, "open": 200.0, "close": 201.0, "volume": 1e6}
            for d in dates
        ]
    )
    out = enricher.enrich(events, bars)
    assert len(out) == n_events
    assert COL_EXCESS_RETURN in out.columns
    assert COL_ENTRY_DATE in out.columns


# ----- Summary fuzz -----


@given(
    n=st.integers(1, 30),
    threshold=st.floats(0.5, 5.0),
)
@settings(max_examples=25)
def test_flag_outliers_fuzz(n: int, threshold: float) -> None:
    """Flag outliers with random excess returns and threshold does not crash."""
    stats = SummaryStats(outlier_std_threshold=threshold)
    import random

    random.seed(42)
    excess = [random.gauss(0, 0.05) for _ in range(n)]
    df = pd.DataFrame(
        {
            CLASS_LABEL_COL: ["g"] * n,
            COL_EXCESS_RETURN: excess,
        }
    )
    out = stats.flag_outliers(df)
    assert len(out) == n
    assert IS_OUTLIER_COL in out.columns
    assert out[IS_OUTLIER_COL].dtype == bool


@given(n_groups=st.integers(1, 5), group_size=st.integers(1, 10))
@settings(max_examples=20)
def test_compute_group_summary_fuzz(n_groups: int, group_size: int) -> None:
    """Group summary with random groups and sizes does not crash."""
    stats = SummaryStats()
    rows = []
    for g in range(n_groups):
        for _ in range(group_size):
            rows.append(
                {
                    CLASS_LABEL_COL: f"group_{g}",
                    COL_EXCESS_RETURN: 0.01 * (g - n_groups / 2),
                }
            )
    df = pd.DataFrame(rows)
    summary = stats.compute_group_summary(df)
    assert len(summary) == n_groups
    assert summary["event_count"].sum() == n_groups * group_size
