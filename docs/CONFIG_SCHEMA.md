# Config Schema

## Common
- `mode`: `backtest | paper | live`
- `timezone`: timezone string (default: `Asia/Seoul`)
- `log_level`: `DEBUG | INFO | WARNING | ERROR`

## Universe
- `universe.quote_currency`: string
- `universe.top_n_by_acc_trade_price_24h`: integer

## Storage
- `storage.raw_dir`: path
- `storage.parquet_dir`: path
- `storage.features_dir`: path
- `storage.backtest_dir`: path
- `storage.paper_dir`: path

## Data
- `data.raw_dir`: path (default: `data/raw`)
- `data.parquet_root`: path (default: `data/parquet`)
- `data.dataset_name`: string (default: `candles_v1`)
- `data.file_pattern`: glob pattern (default: `upbit_*_full.csv`)
- `data.default_compression`: `zstd | snappy | none`
- `data.ingest_workers`: integer
- `data.mode`: `overwrite | skip_unchanged`
- `data.allow_sort_on_non_monotonic`: bool
- `data.allow_dedupe_on_duplicate_ts`: bool
- `data.quote_volume_policy`: `estimate_if_missing | null_if_missing`

### Data Ingest
- `data.ingest.engine`: `duckdb | polars`
- `data.ingest.mode`: `overwrite | skip_unchanged`
- `data.ingest.workers`: integer (default: 1)
- `data.ingest.compression`: `zstd | snappy | none`
- `data.ingest.allow_sort_on_non_monotonic`: bool
- `data.ingest.allow_dedupe_on_duplicate_ts`: bool
- `data.ingest.quote_volume_policy`: `estimate_if_missing | null_if_missing`

### Data Ingest DuckDB
- `data.ingest.duckdb.temp_directory`: string, required when `engine=duckdb`
- `data.ingest.duckdb.memory_limit`: string (default: `6GB`)
- `data.ingest.duckdb.threads`: integer (default: 2)
- `data.ingest.duckdb.fail_if_temp_not_set`: bool (default: `true`)

## Candle Data Contract v1

### Partitioning
- Hive partitions: `tf=<timeframe>/market=<QUOTE-SYMBOL>/part.parquet`

### Columns
- `ts_ms`: `int64` (UTC epoch milliseconds)
- `open`: `float64`
- `high`: `float64`
- `low`: `float64`
- `close`: `float64`
- `volume_base`: `float64`
- `volume_quote`: `float64` nullable
- `volume_quote_est`: `bool`

### Timestamp normalization priority
1. `timestamp` or `ts_ms` (epoch numeric)
2. `candle_date_time_utc` (string -> UTC)
3. `candle_date_time_kst` (string -> KST -> UTC)
4. `datetime | date | time` (string parse -> UTC)

### QA rules
- Fatal:
  - null in `ts_ms`
  - null in `open/high/low/close`
  - any row where `high < low`
- Warning:
  - non-monotonic `ts_ms` (optionally auto-sort)
  - duplicate `ts_ms` (optionally dedupe keep last)
  - timeframe gap(s) detected

### Manifest fields
- `quote`, `symbol`, `market`, `tf`
- `source_csv_relpath`, `source_csv_size`, `source_csv_mtime`
- `ingested_at`, `rows`, `min_ts_ms`, `max_ts_ms`
- `duplicates_dropped`, `non_monotonic_found`, `gaps_found`
- `status` (`OK | WARN | FAIL`), `error_message`
- `timestamp_source`, `timestamp_policy`, `engine`
