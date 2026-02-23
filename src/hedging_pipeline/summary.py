"""
Summary statistics and outlier detection on enriched events.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final

import pandas as pd

from hedging_pipeline.classification import CLASS_LABEL_COL
from hedging_pipeline.config import OUTLIER_STD_THRESHOLD, OUTPUT_ENRICHED_CSV, OUTPUT_SUMMARY_CSV
from hedging_pipeline.enrichment import (
    COL_EXCESS_RETURN,
    COL_FIRST_DAY_RETURN,
    COL_HOLDING_PERIOD_DAYS,
)
from hedging_pipeline.logging_config import logger

IS_OUTLIER_COL: Final[str] = "is_outlier"


class SummaryStats:
    """Computes per-group summary and flags outliers on enriched events."""

    def __init__(self, *, outlier_std_threshold: float = OUTLIER_STD_THRESHOLD) -> None:
        self.outlier_std_threshold: float = outlier_std_threshold

    def compute_group_summary(self, enriched: pd.DataFrame) -> pd.DataFrame:
        """
        One row per classification group: count, mean/median excess return,
        win rate, avg holding period (trading days), avg first-day return.
        """
        if CLASS_LABEL_COL not in enriched.columns:
            raise ValueError(f"Enriched events must have column '{CLASS_LABEL_COL}'")
        if COL_EXCESS_RETURN not in enriched.columns:
            raise ValueError(f"Enriched events must have column '{COL_EXCESS_RETURN}'")

        valid = enriched.dropna(subset=[COL_EXCESS_RETURN])
        if valid.empty:
            return pd.DataFrame()

        def agg_fn(group: pd.DataFrame) -> pd.Series:
            return pd.Series(
                {
                    "event_count": int(len(group)),
                    "mean_excess_return": group[COL_EXCESS_RETURN].mean(),
                    "median_excess_return": group[COL_EXCESS_RETURN].median(),
                    "win_rate": (group[COL_EXCESS_RETURN] > 0).mean(),
                    "avg_holding_period_trading_days": (
                        group[COL_HOLDING_PERIOD_DAYS].mean()
                        if COL_HOLDING_PERIOD_DAYS in group.columns
                        else None
                    ),
                    "avg_first_day_return": (
                        group[COL_FIRST_DAY_RETURN].mean()
                        if COL_FIRST_DAY_RETURN in group.columns
                        else None
                    ),
                }
            )

        return (
            valid.groupby(CLASS_LABEL_COL, dropna=False, group_keys=False)
            .apply(agg_fn, include_groups=False)
            .reset_index()
        )

    def flag_outliers(self, enriched: pd.DataFrame) -> pd.DataFrame:
        """
        Add is_outlier column: True where excess return is more than
        self.outlier_std_threshold standard deviations from the group mean.
        """
        df = enriched.copy()
        if COL_EXCESS_RETURN not in df.columns or CLASS_LABEL_COL not in df.columns:
            df[IS_OUTLIER_COL] = False
            return df

        z_scores = df.groupby(CLASS_LABEL_COL)[COL_EXCESS_RETURN].transform(
            lambda series: (series - series.mean()) / series.std() if series.std() != 0 else 0.0
        )
        df[IS_OUTLIER_COL] = z_scores.abs() > self.outlier_std_threshold
        return df

    def run(
        self,
        enriched: pd.DataFrame,
        *,
        output_dir: Path | str | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Compute summary table and flag outliers; optionally write CSVs to output_dir.
        Returns (summary_df, enriched_with_outliers_df).
        """
        summary = self.compute_group_summary(enriched)
        enriched_with_outliers = self.flag_outliers(enriched)

        if output_dir is not None:
            out = Path(output_dir)
            out.mkdir(parents=True, exist_ok=True)
            summary.to_csv(out / OUTPUT_SUMMARY_CSV, index=False)
            enriched_with_outliers.to_csv(out / OUTPUT_ENRICHED_CSV, index=False)
            logger.info("Wrote %s and %s to %s", OUTPUT_SUMMARY_CSV, OUTPUT_ENRICHED_CSV, out)

        return summary, enriched_with_outliers
