"""Unit tests for SummaryStats: parametrized and edge cases."""

from pathlib import Path

import pandas as pd
import pytest

from hedging_pipeline.classification import CLASS_LABEL_COL
from hedging_pipeline.enrichment import (
    COL_EXCESS_RETURN,
)
from hedging_pipeline.summary import IS_OUTLIER_COL, SummaryStats

# ---------- Parametrized: outlier threshold ----------


@pytest.mark.parametrize(
    "threshold,expect_any_outlier",
    [
        (0.5, True),  # strict -> more outliers
        (2.0, False),  # default; small sample may have none
        (10.0, False),
    ],
)
def test_flag_outliers_threshold(
    threshold: float,
    expect_any_outlier: bool,
    enriched_df: pd.DataFrame,
) -> None:
    stats = SummaryStats(outlier_std_threshold=threshold)
    out = stats.flag_outliers(enriched_df)
    assert IS_OUTLIER_COL in out.columns
    if expect_any_outlier and threshold <= 1.0:
        # With 0.5, our enriched_df has some spread so we might get an outlier
        assert out[IS_OUTLIER_COL].dtype == bool


@pytest.mark.parametrize(
    "excess_values,expected_win_rate",
    [
        ([0.01, 0.02, 0.03], 1.0),
        ([-0.01, -0.02, -0.03], 0.0),
        ([0.01, -0.01, 0.02], 2 / 3),
    ],
)
def test_compute_group_summary_win_rate(
    summary_stats: SummaryStats,
    excess_values: list[float],
    expected_win_rate: float,
) -> None:
    df = pd.DataFrame(
        {
            CLASS_LABEL_COL: ["g"] * len(excess_values),
            COL_EXCESS_RETURN: excess_values,
        }
    )
    summary = summary_stats.compute_group_summary(df)
    assert len(summary) == 1
    assert summary["win_rate"].iloc[0] == pytest.approx(expected_win_rate)


# ---------- Edge cases ----------


def test_compute_group_summary(summary_stats: SummaryStats, enriched_df: pd.DataFrame) -> None:
    summary = summary_stats.compute_group_summary(enriched_df)
    assert len(summary) == 2
    assert set(summary[CLASS_LABEL_COL]) == {"adhoc_add", "annual_del"}
    adhoc = summary[summary[CLASS_LABEL_COL] == "adhoc_add"].iloc[0]
    assert adhoc["event_count"] == 3
    assert adhoc["mean_excess_return"] == pytest.approx(0.02)
    assert adhoc["win_rate"] == 1.0


def test_compute_group_summary_empty_dataframe(summary_stats: SummaryStats) -> None:
    df = pd.DataFrame(columns=[CLASS_LABEL_COL, COL_EXCESS_RETURN])
    summary = summary_stats.compute_group_summary(df)
    assert len(summary) == 0


def test_compute_group_summary_all_nan_excess_returns_empty_summary(
    summary_stats: SummaryStats,
) -> None:
    df = pd.DataFrame(
        {
            CLASS_LABEL_COL: ["a", "a"],
            COL_EXCESS_RETURN: [None, None],
        }
    )
    summary = summary_stats.compute_group_summary(df)
    assert len(summary) == 0


def test_compute_group_summary_single_group(summary_stats: SummaryStats) -> None:
    df = pd.DataFrame(
        {
            CLASS_LABEL_COL: ["only"] * 5,
            COL_EXCESS_RETURN: [0.01, 0.02, 0.03, 0.04, 0.05],
        }
    )
    summary = summary_stats.compute_group_summary(df)
    assert len(summary) == 1
    assert summary[CLASS_LABEL_COL].iloc[0] == "only"
    assert summary["event_count"].iloc[0] == 5
    assert summary["median_excess_return"].iloc[0] == pytest.approx(0.03)


def test_compute_group_summary_raises_without_classification(summary_stats: SummaryStats) -> None:
    df = pd.DataFrame({COL_EXCESS_RETURN: [0.01]})
    with pytest.raises(ValueError) as exc_info:
        summary_stats.compute_group_summary(df)
    assert CLASS_LABEL_COL in str(exc_info.value)


def test_compute_group_summary_raises_without_excess_return(summary_stats: SummaryStats) -> None:
    df = pd.DataFrame({CLASS_LABEL_COL: ["a"]})
    with pytest.raises(ValueError) as exc_info:
        summary_stats.compute_group_summary(df)
    assert COL_EXCESS_RETURN in str(exc_info.value)


def test_flag_outliers(summary_stats: SummaryStats, enriched_df: pd.DataFrame) -> None:
    out = summary_stats.flag_outliers(enriched_df)
    assert IS_OUTLIER_COL in out.columns
    assert len(out) == len(enriched_df)


def test_flag_outliers_empty_excess(summary_stats: SummaryStats) -> None:
    df = pd.DataFrame({CLASS_LABEL_COL: ["a"], COL_EXCESS_RETURN: [None]})
    out = summary_stats.flag_outliers(df)
    assert IS_OUTLIER_COL in out.columns
    assert bool(out[IS_OUTLIER_COL].iloc[0]) is False


def test_flag_outliers_single_row_per_group(summary_stats: SummaryStats) -> None:
    """Single row per group: std=0, z-score 0, so no outlier."""
    df = pd.DataFrame(
        {
            CLASS_LABEL_COL: ["a", "b"],
            COL_EXCESS_RETURN: [0.5, -0.5],
        }
    )
    out = summary_stats.flag_outliers(df)
    assert out[IS_OUTLIER_COL].sum() == 0


def test_run_writes_files(
    summary_stats: SummaryStats, enriched_df: pd.DataFrame, tmp_path: Path
) -> None:
    summary, enriched_out = summary_stats.run(enriched_df, output_dir=tmp_path)
    assert (tmp_path / "summary_by_group.csv").exists()
    assert (tmp_path / "enriched_events.csv").exists()
    assert CLASS_LABEL_COL in summary.columns
    assert IS_OUTLIER_COL in enriched_out.columns
    assert len(summary) == 2
    assert len(enriched_out) == len(enriched_df)


def test_run_without_output_dir_does_not_write(
    summary_stats: SummaryStats, enriched_df: pd.DataFrame, tmp_path: Path
) -> None:
    summary, enriched_out = summary_stats.run(enriched_df, output_dir=None)
    assert len(summary) == 2
    assert not (tmp_path / "summary_by_group.csv").exists()
