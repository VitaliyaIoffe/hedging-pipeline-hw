# Assumptions and Tradeoffs

## Data and paths

- **Event file layout:** The Excel has one row per rebalancing event with columns for announcement date, effective date, one ticker added (`add`), one ticker deleted (`del`), optional `TRADE EST MM`, and `type` (adhoc / ANNUAL). The pipeline normalizes this into one row per stock action (each add and each del as a separate row).
- **Data location:** Default input paths are `data/nasdaq_events.xlsx` and `data/daily_bars.parquet`. Override with `--events` / `--bars` if needed.
- **Daily bars scope:** Bars are assumed to cover at least the event tickers and QQQ over the relevant dates. Events whose announcement or effective date fall outside the bars’ date range (or whose ticker/QQQ is missing) are enriched with NaN for returns and logged; they remain in the enriched table and are excluded from per-group stats (which use only rows with non-null excess return).
- **Column names:** The parquet is expected to use `Date`, `Symbol`, `open_daily`, `close_daily`, `volume_daily`. These are mapped internally to `date`, `symbol`, `open`, `close`, `volume` for consistency.

## Event timing and returns

- **Entry:** Announcement is “after close” on the announcement date, so the first tradable open is the **first trading day strictly after** the announcement date. Entry price = open on that day.
- **Exit:** The change takes effect “on the morning of” the effective date. We take the **last trading day on or before** the effective date and use its **close** as the exit price. If the effective date is not a trading day, we use the previous trading day’s close so the holding period is well-defined and conservative.
- **Holding period:** Number of trading days from entry date to exit date (inclusive).
- **First-day return:** (Close − Open) / Open on the entry date for the stock only (no hedge).
- **Excess return:** Stock return over the holding window minus QQQ return over the same window. QQQ is used as the hedge for all events; entry/exit for QQQ are aligned with the same “first day after ann” and “on or before eff” logic.

## Classification and summary

- **Classification label:** `{event_type}_{action}` with normalized event_type (`adhoc` / `annual`) and action (`add` / `del`), e.g. `adhoc_add`, `annual_del`.
- **Outliers:** An event is flagged as an outlier if its excess return is more than **k** standard deviations from its **group** mean (by classification). **k** is configurable (`config.OUTLIER_STD_THRESHOLD`, default 2.0). Events with missing excess return are not flagged as outliers.
- **Summary metrics:** Computed only over events with non-null excess return. Win rate = fraction of those with positive excess return.

## Error handling and robustness

- **Missing required columns:** The loader raises `DataQualityError` with a clear message rather than failing later with a generic KeyError.
- **Invalid or missing dates:** Rows with invalid announcement or effective dates are dropped after coercion to datetime; a warning is logged.
- **Missing optional columns:** Optional columns (e.g. `TRADE EST MM`) are logged as missing but do not stop the pipeline.
- **Missing ticker in add/del:** Rows with NaN or blank add/del ticker are skipped in normalization (logged at debug level).

## Tradeoffs

- **No static type checking:** Pyright (and similar strict type checkers) are not used. Satisfying them would require many `cast()` and type narrows around pandas/NumPy (e.g. row scalars, DataFrame indexing), which would complicate the code for little runtime benefit. Type hints remain for readability and IDE support; tradeoff is simpler code over strict static checking.
- **Single hedge:** QQQ is the only hedge; no per-ticker or per-sector hedge. Appropriate for a NASDAQ-100 rebalancing study.
- **No reindexing of returns:** Returns are simple (entry open → exit close). We do not chain daily returns or adjust for corporate actions; the assignment data is assumed to be adjusted as needed.
- **Outlier method:** Z-score within group is simple and configurable; more sophisticated methods (e.g. MAD, isolation forest) could be added later without changing the pipeline interface.
- **Visualization:** Optional and separate (`visualize.py`) so the core pipeline has no plotting dependency.
