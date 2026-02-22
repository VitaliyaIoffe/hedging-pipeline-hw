# NASDAQ-100 Rebalancing Event Pipeline

A reusable data processing pipeline for NASDAQ-100 index add/drop events: load and normalize events, classify by type and action, enrich with price and QQQ-hedged returns, and produce summary statistics with optional outlier flagging.

**Layout:** `src/` package, typed OOP API, logging configured via file, [uv](https://github.com/astral-sh/uv) for install/build, GitHub Actions for test, build, and run. **Design:** [DESIGN.md](DESIGN.md) (architecture, data flow, schemas). **Notebook:** [notebooks/data_research.ipynb](notebooks/data_research.ipynb) for data checks and small research (Jupyter included in dev deps).

**Contents:** [Quick start](#quick-start) · [Setup](#setup-uv) · [Logging](#logging) · [Data](#data) · [Running the pipeline](#running-the-pipeline) · [Data exploration notebook](#data-exploration-notebook) · [Optional visualization](#optional-visualization) · [Project structure](#project-structure-src-layout) · [Tests](#tests) · [Code quality](#code-quality-and-coverage) · [CI](#ci-github-actions) · [Design](#design-and-data-schemas) · [Assumptions](#assumptions-and-tradeoffs) · [Improvements](#possible-improvements)

---

## Quick start

```bash
uv sync --dev
uv run run-pipeline --output-dir output   # uses data/nasdaq_events.xlsx, data/daily_bars.parquet by default
uv run pytest tests/ -v
```

---

## Setup (uv)

```bash
# Install uv: https://github.com/astral-sh/uv#installation
uv sync --dev
```

Or with pip:

```bash
pip install -e ".[dev]"
```

## Logging

Logging is configured from **`logging.ini`** in the project root. The CLI loads it automatically; you can override with `--logging-config PATH`. Format and log levels are set in the file. If the file is missing, the process falls back to a default console handler.

## Data

Input files live in **`data/`** (default paths):

- **data/nasdaq_events.xlsx** — NASDAQ-100 add/drop events (required columns: `ANN DATE AFTER CLOSE`, `EFF DATE MORNING OF`, `add`, `del`, `type`)
- **data/daily_bars.parquet** — Daily OHLCV bars for event tickers and QQQ (columns: `Date`, `Symbol`, `open_daily`, `close_daily`, `volume_daily`)
- **data/candidate_assignment.md** — Assignment spec (reference only)

Paths are configurable in `src/hedging_pipeline/config.py` or via `--events` / `--bars`.

## Running the pipeline

```bash
uv run run-pipeline
# or, if installed: python run_pipeline.py
```

Options:

- `--events PATH` — path to events Excel file  
- `--bars PATH` — path to daily bars parquet  
- `--output-dir PATH` — where to write `enriched_events.csv` and `summary_by_group.csv`  
- `--outlier-std N` — outlier threshold in standard deviations (default 2.0)  
- `--no-summary` — skip summary and outlier step  
- `--logging-config PATH` — path to `logging.ini` or `logging.yaml`  

Outputs are written to `output/` by default.

## Data exploration notebook

**`notebooks/data_research.ipynb`** — Jupyter notebook for checking data and small research. It loads events and daily bars, shows unique tickers/actions/event types, runs the pipeline, and displays classification summary and results; extra cells are for ad‑hoc analysis (distributions, filters, plots). Jupyter is included in dev dependencies.

**Run from terminal:**
```bash
uv sync --dev   # includes jupyter
uv run jupyter notebook notebooks/data_research.ipynb
```

**In VS Code / Cursor:** Open `notebooks/data_research.ipynb` and run cells (select the repo’s Python kernel, e.g. the `.venv` that has the package installed).

## Optional visualization

After running the pipeline:

```bash
uv run python visualize.py
```

Requires `matplotlib` (optional extra: `uv sync --extra viz`). Produces `output/summary_plot.png`.

## Project structure (src layout)

```
.
├── .github/
│   └── workflows/
│       └── ci.yml
├── notebooks/
│   └── data_research.ipynb
├── src/
│   └── hedging_pipeline/
│       ├── __init__.py       # Public API
│       ├── classification.py # EventClassifier
│       ├── cli.py            # CLI entry (run-pipeline script)
│       ├── config.py         # Paths, constants, PipelineConfig
│       ├── enrichment.py     # PriceEnricher (returns, QQQ hedge)
│       ├── loaders.py        # EventLoader (load + normalize events, load bars)
│       ├── logging_config.py # setup_logging(config_path)
│       ├── pipeline.py      # Pipeline (orchestrator)
│       └── summary.py        # SummaryStats (group summary, outliers)
├── tests/
│   ├── ...
├── ASSUMPTIONS.md
├── DESIGN.md
├── IMPROVEMENTS.md
├── README.md
├── logging.ini               # Logging config
├── pyproject.toml
├── run_pipeline.py           # Convenience script
├── uv.lock
└── visualize.py              # Optional plot (requires matplotlib)
```

Stages are implemented as classes and are independently testable: `EventLoader`, `EventClassifier`, `PriceEnricher`, `SummaryStats`, `Pipeline`.

## Tests

```bash
uv run pytest tests/ -v
```

- **Unit tests:** parametrized and edge-case coverage for loaders, classification, enrichment, summary, logging config; one integration test (full pipeline when data files exist).
- **Fuzzing (Hypothesis):** property-based tests in `tests/test_fuzz.py` (e.g. normalize/classify/enrich/summary never crash on generated inputs).
- **Benchmarks:** `uv run pytest tests/test_benchmarks.py -m benchmark --benchmark-min-rounds=2` (excluded from default `pytest` via marker; run explicitly when needed).

## Code quality and coverage

- **Coverage:** `uv run pytest tests/ -m "not benchmark" --cov=src/hedging_pipeline --cov-report=term-missing` (HTML: `--cov-report=html`). CI fails if coverage is below 80%.
- **Lint (Ruff):** `uv run ruff check src tests` and `uv run ruff format --check src tests`. To auto-fix: `ruff check --fix`, `ruff format`.

Config: coverage and Ruff in `pyproject.toml`. Static type checking (e.g. Pyright) is not used; see [ASSUMPTIONS.md](ASSUMPTIONS.md) (tradeoffs).

## CI (GitHub Actions)

- **test:** tests (excluding benchmarks) with coverage; fails if coverage &lt; 80%.
- **lint:** Ruff check and format.
- **build:** `uv build` (wheel + sdist).
- **run-pipeline:** runs the pipeline if `data/nasdaq_events.xlsx` and `data/daily_bars.parquet` are present.

## Design and data schemas

See [DESIGN.md](DESIGN.md) for architecture, data flow, and DataFrame schemas (inputs, intermediates, outputs).

## Assumptions and tradeoffs

See [ASSUMPTIONS.md](ASSUMPTIONS.md).

## Possible improvements

See [IMPROVEMENTS.md](IMPROVEMENTS.md) for performance, validation, testing, and tooling ideas.
