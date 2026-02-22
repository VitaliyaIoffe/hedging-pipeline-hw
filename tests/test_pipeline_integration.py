"""Integration tests: full Pipeline run."""

from pathlib import Path

import pytest

from hedging_pipeline.config import DEFAULT_BARS_PATH, DEFAULT_EVENTS_PATH
from hedging_pipeline.pipeline import Pipeline


@pytest.mark.skipif(
    not DEFAULT_EVENTS_PATH.exists() or not DEFAULT_BARS_PATH.exists(),
    reason="Data files not present",
)
def test_pipeline_run_produces_output(tmp_path: Path) -> None:
    pipeline = Pipeline()
    enriched, summary, enriched_out = pipeline.run(
        events_path=DEFAULT_EVENTS_PATH,
        bars_path=DEFAULT_BARS_PATH,
        run_summary=True,
        output_dir=tmp_path,
    )
    assert len(enriched) > 0
    assert "excess_return" in enriched.columns
    assert summary is not None
    assert "classification" in summary.columns
    assert "event_count" in summary.columns
    assert "is_outlier" in enriched_out.columns
    assert (tmp_path / "enriched_events.csv").exists()
    assert (tmp_path / "summary_by_group.csv").exists()
