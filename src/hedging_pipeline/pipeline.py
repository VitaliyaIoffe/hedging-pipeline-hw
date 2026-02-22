"""
Pipeline orchestration: load → classify → enrich → (optional) summarize.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from hedging_pipeline.classification import EventClassifier
from hedging_pipeline.config import (
    PipelineConfig,
)
from hedging_pipeline.enrichment import PriceEnricher
from hedging_pipeline.loaders import EventLoader
from hedging_pipeline.logging_config import logger
from hedging_pipeline.summary import SummaryStats


class Pipeline:
    """
    Runs the full pipeline: load & normalize → classify → enrich → optionally summarize.
    """

    def __init__(
        self,
        *,
        config: PipelineConfig | None = None,
        loader: EventLoader | None = None,
        classifier: EventClassifier | None = None,
        enricher: PriceEnricher | None = None,
        summary_stats: SummaryStats | None = None,
    ) -> None:
        self.config: PipelineConfig = config or PipelineConfig()
        self.loader: EventLoader = loader or EventLoader()
        self.classifier: EventClassifier = classifier or EventClassifier()
        self.enricher: PriceEnricher = enricher or PriceEnricher()
        self.summary_stats: SummaryStats = summary_stats or SummaryStats(
            outlier_std_threshold=self.config.outlier_std_threshold,
        )

    def run(
        self,
        *,
        events_path: Path | str | None = None,
        bars_path: Path | str | None = None,
        run_summary: bool = True,
        output_dir: Path | str | None = None,
        outlier_std_threshold: float | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame | None, pd.DataFrame]:
        """
        Run the full pipeline.
        Returns (enriched_events, summary_df | None, enriched_with_outliers).
        If run_summary is False, summary_df is None.
        """
        events_path = Path(events_path) if events_path is not None else self.config.events_path
        bars_path = Path(bars_path) if bars_path is not None else self.config.bars_path
        out_dir = Path(output_dir) if output_dir is not None else self.config.output_dir
        if outlier_std_threshold is not None:
            self.summary_stats.outlier_std_threshold = outlier_std_threshold

        logger.info("Starting pipeline: events=%s, bars=%s", events_path, bars_path)
        events, bars = self.loader.load_all(events_path, bars_path)
        logger.info("Loaded %d normalized events, %d bar rows", len(events), len(bars))

        events = self.classifier.classify(events)
        enriched = self.enricher.enrich(events, bars)

        summary_df: pd.DataFrame | None = None
        if run_summary:
            summary_df, enriched_with_outliers = self.summary_stats.run(
                enriched,
                output_dir=out_dir,
            )
        else:
            enriched_with_outliers = self.summary_stats.flag_outliers(enriched)

        return enriched, summary_df, enriched_with_outliers
