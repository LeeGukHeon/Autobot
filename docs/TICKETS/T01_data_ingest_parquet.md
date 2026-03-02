# T01 - CSV to Parquet Standardization + Data Contract v1

## Goal
- Convert raw candle CSV files into a unified partitioned Parquet dataset.
- Enforce one stable data contract for training/backtest/paper/live.
- Generate manifest metadata for reproducibility and incremental ingest.

## Scope Implemented
- Filename parser (`upbit_{QUOTE}_{SYMBOL}_{TF}_full.csv`) with regex and error codes.
- Header-based column mapper with auto-detect and required-column validation.
- Candle Data Contract v1 normalization:
  - `ts_ms`, `open`, `high`, `low`, `close`, `volume_base`, `volume_quote`, `volume_quote_est`
- Partitioned output:
  - `data/parquet/candles_v1/tf=<tf>/market=<market>/part.parquet`
- Manifest output:
  - `data/parquet/candles_v1/_meta/manifest.parquet`
- CLI commands:
  - `python -m autobot.cli data sniff`
  - `python -m autobot.cli data ingest`
  - `python -m autobot.cli data validate`
- Ingest report:
  - `data/parquet/candles_v1/_meta/ingest_report.json`

## Data Contract v1
- `ts_ms`: int64 (UTC epoch ms)
- `open`: float64
- `high`: float64
- `low`: float64
- `close`: float64
- `volume_base`: float64
- `volume_quote`: float64 nullable
- `volume_quote_est`: bool

Timestamp source priority:
1. `timestamp` or `ts_ms`
2. `candle_date_time_utc`
3. `candle_date_time_kst`
4. `datetime | date | time`

## QA Policy
- FAIL:
  - missing required columns (`ts_ms/open/high/low/close/volume_base`)
  - null/parse-failed `ts_ms`
  - null in required OHLCV columns
  - OHLC consistency violation (`high < max(open, close, low)` or `low > min(open, close, high)`)
- WARN:
  - non-monotonic `ts_ms` (`NON_MONOTONIC_FOUND`, `NON_MONOTONIC_SORTED`)
  - duplicate `ts_ms` detected/dropped
  - invalid rows dropped
  - type cast failures
- INFO (default status remains OK):
  - timeframe gaps (`GAPS_FOUND`, configurable via `data.qa.gap_severity`)
  - quote volume estimated (`VOLUME_QUOTE_ESTIMATED`, configurable via `data.qa.quote_est_severity`)
- OHLC violation policy:
  - `data.qa.ohlc_violation_policy=drop_row_and_warn`: invalid rows are dropped and counted in `invalid_rows_dropped` (`WARN`)
  - `data.qa.ohlc_violation_policy=fail`: keep strict `FAIL`

Auto-correction options:
- sort on non-monotonic (configurable)
- dedupe duplicate timestamps with `keep last` (configurable)

## Manifest Columns
- `quote`
- `symbol`
- `market`
- `tf`
- `source_csv_relpath`
- `source_csv_size`
- `source_csv_mtime`
- `ingested_at`
- `rows`
- `min_ts_ms`
- `max_ts_ms`
- `duplicates_dropped`
- `non_monotonic_found`
- `gaps_found`
- `invalid_rows_dropped`
- `ohlc_violations`
- `status`
- `reasons_json`
- `error_message`
- `timestamp_source`
- `timestamp_policy`
- `engine`

## DuckDB Temp Safety Mode (Required)

### Requirement
- If ingest engine is DuckDB, `PRAGMA temp_directory` must be set to D drive.
- Default:
  - `D:/MyApps/Autobot/data/cache/duckdb_tmp`
- Missing temp directory with `fail_if_temp_not_set=true` must fail fast.

### Applied Settings
- `PRAGMA temp_directory='D:/MyApps/Autobot/data/cache/duckdb_tmp';`
- `PRAGMA memory_limit='6GB';`
- `PRAGMA threads=2;`

### Config Keys
- `data.ingest.duckdb.temp_directory`
- `data.ingest.duckdb.memory_limit`
- `data.ingest.duckdb.threads`
- `data.ingest.duckdb.fail_if_temp_not_set`

## CLI

### Sniff
```powershell
python -m autobot.cli data sniff --sample-files 10 --sample-rows 5
```

### Dry Run
```powershell
python -m autobot.cli data ingest --dry-run --limit-files 20
```

### Ingest
```powershell
python -m autobot.cli data ingest --workers 1
```

### Validate
```powershell
python -m autobot.cli data validate
```

## Results / Issues
- Implementation completed in:
  - `autobot/data/*.py`
  - `autobot/cli.py`
  - `config/base.yaml`
  - `docs/CONFIG_SCHEMA.md`
- Fill actual runtime metrics and failed-file samples after first full ingest run.
