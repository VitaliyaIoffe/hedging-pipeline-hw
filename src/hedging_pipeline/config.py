"""
Pipeline configuration. All paths, thresholds, and mappings are centralized here.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final

# Resolve project root: when developing, package is at repo/src/hedging_pipeline; when installed, use cwd.
_PACKAGE_DIR: Final[Path] = Path(__file__).resolve().parent
_candidate_root: Path = _PACKAGE_DIR.parent.parent
PROJECT_ROOT: Final[Path] = (
    _candidate_root if (_candidate_root / "pyproject.toml").exists() else Path.cwd()
)
DATA_DIR: Final[Path] = PROJECT_ROOT / "data"
DEFAULT_EVENTS_PATH: Path = DATA_DIR / "nasdaq_events.xlsx"
DEFAULT_BARS_PATH: Path = DATA_DIR / "daily_bars.parquet"
OUTPUT_DIR: Path = PROJECT_ROOT / "output"

# Event file
EVENTS_SHEET_NAME: None | int | str = None  # None = first sheet
EVENTS_REQUIRED_COLUMNS: Final[list[str]] = [
    "ANN DATE AFTER CLOSE",
    "EFF DATE MORNING OF",
    "add",
    "del",
    "type",
]
EVENTS_OPTIONAL_COLUMNS: Final[list[str]] = ["TRADE EST MM"]

# Normalized column names (after loading)
COL_ANN_DATE: Final[str] = "ann_date"
COL_EFF_DATE: Final[str] = "eff_date"
COL_TICKER: Final[str] = "ticker"
COL_ACTION: Final[str] = "action"
COL_EVENT_TYPE: Final[str] = "event_type"
COL_TRADE_EST_MM: Final[str] = "trade_est_mm"

# Daily bars
BARS_DATE_COL: Final[str] = "date"
BARS_SYMBOL_COL: Final[str] = "symbol"
BARS_OPEN_COL: Final[str] = "open"
BARS_CLOSE_COL: Final[str] = "close"
BARS_VOLUME_COL: Final[str] = "volume"
BARS_COLUMN_MAP: Final[dict[str, str]] = {
    "Date": BARS_DATE_COL,
    "Symbol": BARS_SYMBOL_COL,
    "open_daily": BARS_OPEN_COL,
    "close_daily": BARS_CLOSE_COL,
    "volume_daily": BARS_VOLUME_COL,
}

# Hedging
HEDGE_SYMBOL: Final[str] = "QQQ"
HEDGE_STRATEGY_DEFAULT: Final[str] = "single_benchmark"  # or "no_hedge"

# Classification
ACTION_ADD: Final[str] = "add"
ACTION_DEL: Final[str] = "del"
EVENT_TYPE_ADHOC: Final[str] = "adhoc"
EVENT_TYPE_ANNUAL: Final[str] = "annual"

# Outlier detection (number of standard deviations from group mean)
OUTLIER_STD_THRESHOLD: Final[float] = 2.0

# Output filenames
OUTPUT_ENRICHED_CSV: Final[str] = "enriched_events.csv"
OUTPUT_SUMMARY_CSV: Final[str] = "summary_by_group.csv"


class PipelineConfig:
    """Mutable config holder for paths, thresholds, and hedge strategy."""

    def __init__(
        self,
        *,
        events_path: Path | None = None,
        bars_path: Path | None = None,
        output_dir: Path | None = None,
        outlier_std_threshold: float | None = None,
        hedge_strategy: str | None = None,
        hedge_symbol: str | None = None,
    ) -> None:
        self.events_path: Path = events_path or DEFAULT_EVENTS_PATH
        self.bars_path: Path = bars_path or DEFAULT_BARS_PATH
        self.output_dir: Path = output_dir or OUTPUT_DIR
        self.outlier_std_threshold: float = outlier_std_threshold or OUTLIER_STD_THRESHOLD
        self.hedge_strategy: str = hedge_strategy or HEDGE_STRATEGY_DEFAULT
        self.hedge_symbol: str = hedge_symbol or HEDGE_SYMBOL
