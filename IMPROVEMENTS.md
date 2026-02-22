# Improvement ideas

Suggestions to make the pipeline more robust, faster, and easier to maintain. Ordered by impact and effort.

---

## 1. Performance

### 1.1 Enrichment: avoid row-by-row loop

**Where:** `enrichment.py` — `PriceEnricher.enrich()` uses `for _, row in df.iterrows()` and repeated filters on `bars` per event.

**Issue:** `iterrows()` is slow and filtering the full bars DataFrame per symbol/date is O(events × bars) in the worst case.

**Improvements:**

- **Pre-index bars by symbol:** Build once: `bars_by_symbol = bars.groupby(BARS_SYMBOL_COL)` or a dict of symbol → subset. Then for each event, look up only that symbol’s rows and binary-search or filter by date.
- **Vectorize where possible:** For entry/exit dates and returns, consider a merge-based approach: normalize events with a “key” (e.g. ticker + ann_date), merge bars on symbol and date ranges, then aggregate. More complex but scales better for large event sets.
- **Optional:** Use `numba` or `numpy` for the return math if events grow into the thousands.

### 1.2 Loaders: normalize without iterrows

**Where:** `loaders.py` — `normalize_events()` loops `for _, r in raw_events.iterrows()` and builds a list of dicts.

**Improvement:** Use vectorized operations: e.g. melt/stack the add/del columns into long form, then assign ann_date, eff_date, type from the original rows. Fewer Python-level loops and often faster.

---

## 2. Validation and config

### 2.1 Validate PipelineConfig

**Where:** `config.py` — `PipelineConfig` accepts any paths and threshold; no checks.

**Improvements:**

- **Paths:** Optionally validate `events_path.exists()` and `bars_path.exists()` at construction (or at pipeline run), or allow a “strict” mode that raises if missing.
- **Threshold:** Ensure `outlier_std_threshold > 0` (and optionally cap or warn if very large).
- **Optional:** Use **Pydantic** for `PipelineConfig`: coercion (str → Path), validators, and loading from env/YAML for deployment.

### 2.2 Config path resolution at import

**Where:** `config.py` — `DEFAULT_EVENTS_PATH` and `DEFAULT_BARS_PATH` are mutated at import time based on `PROJECT_ROOT` and file existence.

**Issue:** Side effects at import can be surprising and harder to test.

**Improvement:** Resolve defaults in a function (e.g. `get_default_paths()`) or inside `PipelineConfig.__init__`, and keep module-level constants as “template” paths only.

---

## 3. Error handling and robustness

### 3.1 Clearer errors from the pipeline

**Where:** `pipeline.py` — `Pipeline.run()` doesn’t catch exceptions; failures surface as raw `DataQualityError` or `FileNotFoundError`.

**Improvement:** Optionally wrap in a small try/except that adds context (e.g. “Failed during enrichment” or “Events file not found: …”) and either re-raise or return a structured error for the CLI to report.

### 3.2 Enrichment: guard against bad data

**Where:** `enrichment.py` — Return calculations guard against None and zero open; negative prices are not explicitly checked.

**Improvement:** If open/close can be invalid (e.g. negative or zero when not expected), add an explicit check and log a warning or skip the row instead of producing misleading returns.

### 3.3 Summary: groups with no valid excess return

**Where:** `summary.py` — `compute_group_summary()` uses only rows with non-null `excess_return`. A classification that has only NaN excess returns won’t appear in the summary.

**Improvement:** Either document this (already in DESIGN.md) or optionally include such groups with NaN/zero metrics and `event_count = 0` (or total count with “valid” count) so the summary is exhaustive.

---

## 4. Testing

### 4.1 More coverage

- **SummaryStats.run():** Test that when `output_dir` is set, the two CSV files are created and have expected columns and row counts.
- **Enrichment edge cases:** Events with eff_date before entry_date; bars with missing QQQ; duplicate (date, symbol) rows in bars.
- **Loader:** Events Excel with multiple sheets; sheet name vs index in config.

### 4.2 Property / contract tests

- After each stage, assert expected columns exist and dtypes (e.g. `ann_date` is datetime, `excess_return` is float or NaN).
- Optional: use `pandera` or simple asserts on `df.dtypes` and `set(df.columns)` in shared test helpers.

### 4.3 Fixtures and test data

- **Shared fixtures:** A small “canonical” events DataFrame and bars DataFrame (e.g. in `tests/fixtures/` or as constants) used by loader, classifier, enricher, and summary tests to avoid duplication.
- **Integration test:** Run full pipeline on a tiny CSV/parquet fixture and snapshot the summary (or key columns) to catch regressions.

---

## 5. Code quality and tooling

### 5.1 Linting and formatting

- Add **ruff** (or flake8 + isort + black) and run in CI; fix or exclude only where necessary.
- Add **pyright** (or mypy) with strictness that fits the project; fix type issues in config and pipeline.

### 5.2 Dependencies

- In `pyproject.toml`, consider pinning major (or minor) versions for reproducibility (e.g. `pandas>=2.0,<3`), and run `uv lock` so CI and local match.

### 5.3 Docstrings and contracts

- In docstrings for `enrich()`, `classify()`, `compute_group_summary()`, etc., state the **required input columns** and **added/output columns** (or refer to DESIGN.md). Helps avoid misuse when the pipeline is extended.

---

## 6. Features and UX

### 6.1 Output format

- **Parquet output:** Optionally write `enriched_events.parquet` and `summary_by_group.parquet` for downstream tools; keep CSV as default.
- **Summary:** Add a simple “total” row (e.g. across all classifications) for quick sanity checks.

### 6.2 Logging

- Log a one-line summary at the end: e.g. “Enriched N events, M with valid excess return; wrote summary for K groups.” Helps when running in batch or from CI.

### 6.3 Dry run

- CLI flag `--dry-run`: load and validate events and bars, run classification (and optionally enrichment) without writing CSVs. Useful for validating inputs before a full run.

---

## 7. Documentation

- **DESIGN.md:** Already describes flow and schemas; keep it in sync when adding columns or stages.
- **README:** Add a “Troubleshooting” or “Common issues” section (e.g. “No events enriched” → check bars date range and QQQ presence).
- **Docstrings:** Add a short “Raises” section where `DataQualityError`, `ValueError`, or `FileNotFoundError` are raised.

---

## Quick wins (low effort)

1. Add `ruff` and `pyright` to CI and fix reported issues.
2. Validate `outlier_std_threshold > 0` in `SummaryStats` or `PipelineConfig`.
3. Add a test that `SummaryStats.run(output_dir=tmp_path)` creates both CSVs and that summary has a `classification` column.
4. Document in ASSUMPTIONS or DESIGN that summary only includes groups with at least one non-null excess return.
5. Pre-index bars in enrichment: `bars_by_sym = {sym: grp for sym, grp in bars.groupby(BARS_SYMBOL_COL)}` and use it inside the loop instead of filtering the full DataFrame every time.

---

## Larger refactors (if needed later)

- **Pydantic config:** Full settings model with env/file loading.
- **Vectorized enrichment:** Merge-based design for entry/exit and returns.
- **Plugin-style stages:** Abstract base for “loader”, “classifier”, “enricher” so alternative implementations (e.g. different hedge, different bar source) can be swapped without changing the pipeline core.
- **Structured logging:** JSON logs or key-value fields for production monitoring.
