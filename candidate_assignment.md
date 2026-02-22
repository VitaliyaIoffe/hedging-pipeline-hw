# Technical Assessment — Software Engineer

## Overview

You are given two data files related to **NASDAQ-100 index rebalancing events** — cases where stocks are added to or removed from the NASDAQ-100 index. Your job is to build a **reusable data processing pipeline** that normalizes the raw event data, enriches it with price data, and produces summary analytics.

Imagine this pipeline will run again next quarter when new events arrive. Design accordingly.

**What we evaluate:** code structure and modularity, correctness, test coverage, error handling, and how well your code would hold up in a production setting.

## Setup

### Provided files

```
data/
├── nasdaq_events.xlsx     # NASDAQ-100 add/drop events
└── daily_bars.parquet     # daily OHLCV bars for event tickers + QQQ
```

Explore the data and understand its structure before writing pipeline code. Pay attention to how the event file is organized.

---

## Task 1: Event Processing Pipeline

Build a pipeline with clearly separated stages: **data loading & normalization → classification → price enrichment → output**.

### Data loading & normalization

Write a loading module that reads both input files into DataFrames. The event file is not in a pipeline-friendly format — you'll need to transform it into a normalized structure where each row represents a single stock action (one addition or one deletion) with its associated dates and metadata.

Your loader should validate that expected columns are present and handle any data quality issues it encounters rather than silently ignoring them or crashing.

### Event classification

Each event has a type and an action. Classify each record with a meaningful label that captures both dimensions.

### Hedging

When trading an index rebalancing event, the stock's price movement includes both the event-specific effect (what we want to measure) and broader market movement (noise). To isolate the event effect, we use a **hedge** — in this case QQQ, the NASDAQ-100 ETF. By computing the stock's return minus QQQ's return over the same period, we get the **excess return**: the portion of the move attributable to the index change rather than the overall market.

Assign QQQ as the hedge instrument for all events and include its return data in your enrichment step.

### Price enrichment

Using the daily bars, enrich each event with relevant price movements. Decide which price points and time windows are most useful given the event structure. The announcement happens after market close on the announcement date — this means the trade can be taken from the following market day's open; the change takes effect on the morning of the effective date.

---

## Task 2: Summary Statistics

Using the output of Task 1, write separate code to produce summary statistics that assess the viability of a trading strategy based on these events.

### Per-group summary

Group events by their classification label and compute metrics such as:
- Number of events
- Average and median excess return (stock return minus hedge return)
- Win rate (fraction of events with positive excess return)
- Average holding period in trading days
- Average first-day return

Consider how performance might differ across event types (adhoc vs annual) and actions (add vs del).

### Outlier detection

Flag events where the excess return is unusually large or small relative to their group. The detection threshold should be configurable.

### Output

- A summary table (one row per group).
- The enriched events DataFrame with an `is_outlier` column.
- A simple visualization is welcome but optional.

---

## Requirements

- Use Python and `pandas`.
- Use Python's `logging` module for diagnostics (not print statements).
- Structure your code so that each stage (loading, classification, enrichment, summarization) is independently callable and testable.
- Configurable parameters (thresholds, mappings, file paths) should not be hardcoded inline.
- Write tests. Aim for meaningful coverage — happy path, edge cases, and at least one test that validates behavior on bad or missing input data.

## Submission

- Send back a zip or git repo with your code, tests, and output files.
- Include a brief note on any assumptions you made or tradeoffs you considered.
- You have **one week** from receiving this assignment.
