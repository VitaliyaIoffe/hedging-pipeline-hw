#!/usr/bin/env python3
"""
Optional visualization: summary by group (mean excess return, win rate).
Run after pipeline to plot from output/summary_by_group.csv.
"""

from pathlib import Path

from hedging_pipeline.config import OUTPUT_DIR, OUTPUT_SUMMARY_CSV
from hedging_pipeline.logging_config import logger, setup_logging


def plot_summary(summary_csv: Path, output_path: Path) -> None:
    """Plot mean excess return and win rate by classification (requires matplotlib)."""
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        logger.warning("matplotlib not installed; skipping visualization")
        return

    import pandas as pd

    df = pd.read_csv(summary_csv)
    if df.empty:
        logger.warning("Summary CSV is empty; nothing to plot")
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4))
    labels = df["classification"].astype(str)
    x = range(len(labels))

    ax1.bar(x, df["mean_excess_return"], color="steelblue", edgecolor="navy")
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels, rotation=45, ha="right")
    ax1.set_ylabel("Mean excess return")
    ax1.set_title("Mean excess return by group")
    ax1.axhline(0, color="gray", linestyle="--")

    ax2.bar(x, df["win_rate"], color="seagreen", edgecolor="darkgreen")
    ax2.set_xticks(x)
    ax2.set_xticklabels(labels, rotation=45, ha="right")
    ax2.set_ylabel("Win rate")
    ax2.set_title("Win rate by group")
    ax2.set_ylim(0, 1)
    ax2.axhline(0.5, color="gray", linestyle="--")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    logger.info("Saved plot to %s", output_path)


def main() -> None:
    setup_logging()
    summary_path = OUTPUT_DIR / OUTPUT_SUMMARY_CSV
    if not summary_path.exists():
        logger.warning("Summary file not found at %s; run pipeline first", summary_path)
        return
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    plot_summary(summary_path, OUTPUT_DIR / "summary_plot.png")


if __name__ == "__main__":
    main()
