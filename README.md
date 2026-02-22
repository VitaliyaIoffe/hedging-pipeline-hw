# NASDAQ-100 Rebalancing Event Pipeline

A reusable data processing pipeline for NASDAQ-100 index add/drop events: load and normalize events, classify by type and action, enrich with price and QQQ-hedged returns, and produce summary statistics with optional outlier flagging.

**Layout:** `src/` package, typed OOP API, logging configured via file, [uv](https://github.com/astral-sh/uv) for install/build, GitHub Actions for test, build, and run. **Design:** [DESIGN.md](DESIGN.md) (architecture, data flow, schemas).

---

## Quick start

```bash
uv sync --dev
uv run run-pipeline --events nasdaq_events.xlsx --bars daily_bars.parquet --output-dir output
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

Place input files either under `data/` or in the project root:

- **nasdaq_events.xlsx** — NASDAQ-100 add/drop events (required columns: `ANN DATE AFTER CLOSE`, `EFF DATE MORNING OF`, `add`, `del`, `type`)
- **daily_bars.parquet** — Daily OHLCV bars for event tickers and QQQ (columns: `Date`, `Symbol`, `open_daily`, `close_daily`, `volume_daily`)

Paths are configurable in `src/hedging_pipeline/config.py` or via CLI.

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

A Jupyter notebook for loading data, running the pipeline, and ad‑hoc research:

```bash
uv sync --dev   # includes jupyter
uv run jupyter notebook notebooks/data_research.ipynb
```

Or from VS Code/Cursor: open `notebooks/data_research.ipynb` and run (ensure the environment has the package: e.g. select kernel from repo venv).

## Optional visualization

After running the pipeline:

```bash
uv run python visualize.py
```

Requires `matplotlib` (optional extra: `uv sync --extra viz`). Produces `output/summary_plot.png`.

## Project structure (src layout)

```
src/hedging_pipeline/
  __init__.py       # Public API
  config.py         # Paths, constants, PipelineConfig
  loaders.py        # EventLoader (load + normalize events, load bars)
  classification.py # EventClassifier
  enrichment.py     # PriceEnricher (returns, QQQ hedge)
  summary.py        # SummaryStats (group summary, outliers)
  pipeline.py       # Pipeline (orchestrator)
  logging_config.py # setup_logging(config_path)
  cli.py            # CLI entry (run-pipeline script)
logging.ini         # Logging config (project root)
run_pipeline.py     # Convenience script
visualize.py        # Optional plot
tests/              # Unit and integration tests
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
- **run-pipeline:** runs the pipeline if `nasdaq_events.xlsx` and `daily_bars.parquet` are present in repo root or `data/`.

## Design and data schemas

See [DESIGN.md](DESIGN.md) for architecture, data flow, and DataFrame schemas (inputs, intermediates, outputs).

## Assumptions and tradeoffs

See [ASSUMPTIONS.md](ASSUMPTIONS.md).

## Possible improvements

See [IMPROVEMENTS.md](IMPROVEMENTS.md) for performance, validation, testing, and tooling ideas.
