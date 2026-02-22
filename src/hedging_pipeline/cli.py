"""
CLI entry point: run the event processing pipeline.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from hedging_pipeline.config import (
    DEFAULT_BARS_PATH,
    DEFAULT_EVENTS_PATH,
    OUTLIER_STD_THRESHOLD,
    OUTPUT_DIR,
)
from hedging_pipeline.logging_config import logger, setup_logging
from hedging_pipeline.pipeline import Pipeline


def main() -> int:
    """Parse args, configure logging from file, run pipeline."""
    # Configure logging from logging.ini (or env) before parsing so early logs are consistent
    setup_logging()

    parser = argparse.ArgumentParser(description="NASDAQ-100 rebalancing event pipeline")
    parser.add_argument(
        "--events", type=Path, default=DEFAULT_EVENTS_PATH, help="Path to nasdaq_events.xlsx"
    )
    parser.add_argument(
        "--bars", type=Path, default=DEFAULT_BARS_PATH, help="Path to daily_bars.parquet"
    )
    parser.add_argument(
        "--output-dir", type=Path, default=OUTPUT_DIR, help="Directory for output CSVs"
    )
    parser.add_argument(
        "--outlier-std",
        type=float,
        default=OUTLIER_STD_THRESHOLD,
        help="Outlier threshold (std devs)",
    )
    parser.add_argument("--no-summary", action="store_true", help="Skip summary and outlier step")
    parser.add_argument(
        "--logging-config",
        type=Path,
        default=None,
        help="Path to logging.ini or logging.yaml (default: auto-detect)",
    )
    args = parser.parse_args()

    if args.logging_config is not None:
        setup_logging(args.logging_config)

    try:
        pipeline = Pipeline()
        enriched, summary_df, _ = pipeline.run(
            events_path=args.events,
            bars_path=args.bars,
            run_summary=not args.no_summary,
            output_dir=args.output_dir,
            outlier_std_threshold=args.outlier_std,
        )
        logger.info("Pipeline finished. Enriched events: %d rows", len(enriched))
        if summary_df is not None:
            logger.info("Summary groups: %s", list(summary_df["classification"].values))
        return 0
    except Exception as e:
        logger.exception("Pipeline failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
