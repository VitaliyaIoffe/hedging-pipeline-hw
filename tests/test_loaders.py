"""Unit tests for EventLoader: parametrized, edge cases, and basic flow."""

from pathlib import Path

import pandas as pd
import pytest

from hedging_pipeline.config import (
    COL_ACTION,
    COL_ANN_DATE,
    COL_EFF_DATE,
    COL_EVENT_TYPE,
    COL_TICKER,
)
from hedging_pipeline.loaders import DataQualityError, EventLoader

# ---------- Parametrized: missing required columns ----------


@pytest.mark.parametrize(
    "missing_col",
    [
        "EFF DATE MORNING OF",
        "add",
        "del",
        "type",
    ],
)
def test_load_events_raises_when_required_column_missing(
    loader: EventLoader, tmp_path: Path, missing_col: str
) -> None:
    required = ["ANN DATE AFTER CLOSE", "EFF DATE MORNING OF", "add", "del", "type"]
    cols = {
        c: [pd.Timestamp("2020-06-01")] if "DATE" in c else ["X"]
        for c in required
        if c != missing_col
    }
    df = pd.DataFrame(cols)
    path = tmp_path / "bad.xlsx"
    df.to_excel(path, index=False)
    with pytest.raises(DataQualityError) as exc_info:
        loader.load_events(path)
    assert missing_col in str(exc_info.value)


# ---------- Parametrized: normalize one row per action ----------


@pytest.mark.parametrize(
    "n_events,expected_rows",
    [
        (1, 2),  # 1 event -> 1 add + 1 del
        (2, 4),
        (5, 10),
    ],
)
def test_normalize_events_row_count(loader: EventLoader, n_events: int, expected_rows: int) -> None:
    df = pd.DataFrame(
        {
            "ANN DATE AFTER CLOSE": pd.to_datetime(["2020-06-01"] * n_events),
            "EFF DATE MORNING OF": pd.to_datetime(["2020-06-10"] * n_events),
            "add": [f"A{i}" for i in range(n_events)],
            "del": [f"D{i}" for i in range(n_events)],
            "type": ["adhoc"] * n_events,
        }
    )
    out = loader.normalize_events(df)
    assert len(out) == expected_rows
    assert set(out[COL_ACTION].unique()) == {"add", "del"}


# ---------- Edge cases: load_events ----------


def test_load_events_missing_file(loader: EventLoader) -> None:
    with pytest.raises(FileNotFoundError):
        loader.load_events(Path("/nonexistent/events.xlsx"))


def test_load_events_empty_dataframe(loader: EventLoader, tmp_path: Path) -> None:
    df = pd.DataFrame(columns=["ANN DATE AFTER CLOSE", "EFF DATE MORNING OF", "add", "del", "type"])
    path = tmp_path / "empty.xlsx"
    df.to_excel(path, index=False)
    out = loader.load_events(path)
    assert len(out) == 0


def test_load_events_drops_invalid_dates(loader: EventLoader, tmp_path: Path) -> None:
    """Excel with one invalid date string; loader coerces and drops that row."""
    df = pd.DataFrame(
        {
            "ANN DATE AFTER CLOSE": ["2020-06-01", "not-a-date", "2020-07-01"],
            "EFF DATE MORNING OF": ["2020-06-10", "2020-06-15", "2020-07-10"],
            "add": ["A", "B", "C"],
            "del": ["X", "Y", "Z"],
            "type": ["adhoc", "adhoc", "adhoc"],
        }
    )
    path = tmp_path / "events.xlsx"
    df.to_excel(path, index=False)
    out = loader.load_events(path)
    assert len(out) == 2
    assert out["add"].tolist() == ["A", "C"]


def test_load_events_success(
    loader: EventLoader, minimal_raw_events_df: pd.DataFrame, tmp_path: Path
) -> None:
    path = tmp_path / "events.xlsx"
    minimal_raw_events_df.to_excel(path, index=False, sheet_name="Sheet1")
    df = loader.load_events(path)
    assert len(df) == 2
    assert "ANN DATE AFTER CLOSE" in df.columns
    assert "add" in df.columns
    assert "del" in df.columns
    assert "type" in df.columns


# ---------- Edge cases: normalize_events ----------


def test_normalize_events_one_row_per_action(
    loader: EventLoader, minimal_raw_events_df: pd.DataFrame
) -> None:
    out = loader.normalize_events(minimal_raw_events_df)
    assert len(out) == 4
    assert set(out.columns) >= {COL_ANN_DATE, COL_EFF_DATE, COL_TICKER, COL_ACTION, COL_EVENT_TYPE}
    assert set(out[COL_ACTION].unique()) == {"add", "del"}
    assert set(out[COL_TICKER].unique()) == {"TICK1", "TICK2", "OLD1", "OLD2"}


@pytest.mark.parametrize("blank_value", [None, "", "   ", float("nan")])
def test_normalize_events_skips_blank_ticker(
    loader: EventLoader, minimal_raw_events_df: pd.DataFrame, blank_value: object
) -> None:
    minimal_raw_events_df.loc[1, "add"] = blank_value
    out = loader.normalize_events(minimal_raw_events_df)
    assert len(out) == 3
    assert "TICK2" not in out[COL_TICKER].values


def test_normalize_events_empty_input(loader: EventLoader) -> None:
    df = pd.DataFrame(columns=["ANN DATE AFTER CLOSE", "EFF DATE MORNING OF", "add", "del", "type"])
    out = loader.normalize_events(df)
    assert len(out) == 0
    assert out.columns.tolist() == []


def test_normalize_events_uppercases_ticker(loader: EventLoader) -> None:
    df = pd.DataFrame(
        {
            "ANN DATE AFTER CLOSE": [pd.Timestamp("2020-06-01")],
            "EFF DATE MORNING OF": [pd.Timestamp("2020-06-10")],
            "add": ["abc"],
            "del": ["xyz"],
            "type": ["adhoc"],
        }
    )
    out = loader.normalize_events(df)
    assert out[COL_TICKER].tolist() == ["ABC", "XYZ"]


# ---------- load_and_normalize + daily bars ----------


def test_load_and_normalize_events(
    loader: EventLoader, minimal_raw_events_df: pd.DataFrame, tmp_path: Path
) -> None:
    path = tmp_path / "events.xlsx"
    minimal_raw_events_df.to_excel(path, index=False, sheet_name="Sheet1")
    out = loader.load_and_normalize_events(path)
    assert len(out) == 4
    assert COL_TICKER in out.columns
    assert COL_ACTION in out.columns


def test_load_daily_bars_missing_file(loader: EventLoader) -> None:
    with pytest.raises(FileNotFoundError):
        loader.load_daily_bars(Path("/nonexistent/bars.parquet"))


def test_load_daily_bars_wrong_columns(loader: EventLoader, tmp_path: Path) -> None:
    df = pd.DataFrame({"x": [1], "y": [2]})
    path = tmp_path / "bars.parquet"
    df.to_parquet(path, index=False)
    with pytest.raises(DataQualityError):
        loader.load_daily_bars(path)


def test_load_daily_bars_normalizes_symbol_case(loader: EventLoader, tmp_path: Path) -> None:
    df = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2020-01-02"]),
            "Symbol": ["qqq"],
            "open_daily": [100.0],
            "close_daily": [101.0],
            "volume_daily": [1e6],
        }
    )
    path = tmp_path / "bars.parquet"
    df.to_parquet(path, index=False)
    out = loader.load_daily_bars(path)
    assert out["symbol"].iloc[0] == "QQQ"


def test_load_daily_bars_success(loader: EventLoader, tmp_path: Path) -> None:
    df = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2020-01-02", "2020-01-02", "2020-01-03"]),
            "Symbol": ["QQQ", "AAPL", "QQQ"],
            "open_daily": [100.0, 200.0, 101.0],
            "close_daily": [101.0, 202.0, 102.0],
            "volume_daily": [1_000_000, 2_000_000, 1_100_000],
        }
    )
    path = tmp_path / "bars.parquet"
    df.to_parquet(path, index=False)
    out = loader.load_daily_bars(path)
    assert "date" in out.columns
    assert "symbol" in out.columns
    assert "open" in out.columns
    assert "close" in out.columns
    assert len(out) == 3
    assert out["symbol"].str.isupper().all()


def test_load_daily_bars_drops_null_dates(loader: EventLoader, tmp_path: Path) -> None:
    df = pd.DataFrame(
        {
            "Date": ["2020-01-02", None, "2020-01-03"],
            "Symbol": ["QQQ", "QQQ", "QQQ"],
            "open_daily": [100.0, 101.0, 102.0],
            "close_daily": [101.0, 102.0, 103.0],
            "volume_daily": [1e6, 1e6, 1e6],
        }
    )
    path = tmp_path / "bars.parquet"
    df.to_parquet(path, index=False)
    out = loader.load_daily_bars(path)
    assert len(out) == 2


def test_load_all_returns_correct_shapes(
    loader: EventLoader, minimal_raw_events_df: pd.DataFrame, tmp_path: Path
) -> None:
    minimal_raw_events_df.to_excel(tmp_path / "events.xlsx", index=False, sheet_name="Sheet1")
    bars = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2020-01-02"]),
            "Symbol": ["QQQ"],
            "open_daily": [100.0],
            "close_daily": [101.0],
            "volume_daily": [1e6],
        }
    )
    bars.to_parquet(tmp_path / "bars.parquet", index=False)
    events, bars_out = loader.load_all(tmp_path / "events.xlsx", tmp_path / "bars.parquet")
    assert len(events) == 4
    assert len(bars_out) == 1
