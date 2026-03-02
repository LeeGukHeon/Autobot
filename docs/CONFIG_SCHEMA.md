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
- `data.qa.gap_severity`: `info | warn | fail` (default: `info`)
- `data.qa.quote_est_severity`: `info | warn | fail` (default: `info`)

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

## Upbit
- `upbit.base_url`: string (default: `https://api.upbit.com`)

### Upbit Timeout
- `upbit.timeout.connect_sec`: number (default: `3`)
- `upbit.timeout.read_sec`: number (default: `10`)
- `upbit.timeout.write_sec`: number (default: `10`)

### Upbit Auth
- `upbit.auth.access_key_env`: string (default: `UPBIT_ACCESS_KEY`)
- `upbit.auth.secret_key_env`: string (default: `UPBIT_SECRET_KEY`)

### Upbit Rate Limit
- `upbit.ratelimit.enabled`: bool (default: `true`)
- `upbit.ratelimit.ban_cooldown_sec`: integer (default: `60`)
- `upbit.ratelimit.group_defaults.market_rps`: number
- `upbit.ratelimit.group_defaults.candle_rps`: number
- `upbit.ratelimit.group_defaults.trade_rps`: number
- `upbit.ratelimit.group_defaults.ticker_rps`: number
- `upbit.ratelimit.group_defaults.orderbook_rps`: number
- `upbit.ratelimit.group_defaults.exchange_default_rps`: number
- `upbit.ratelimit.group_defaults.order_rps`: number
- `upbit.ratelimit.group_defaults.order_test_rps`: number
- `upbit.ratelimit.group_defaults.order_cancel_all_rps_2s`: number

### Upbit Retry
- `upbit.retry.max_attempts`: integer (default: `3`)
- `upbit.retry.base_backoff_ms`: integer (default: `200`)
- `upbit.retry.max_backoff_ms`: integer (default: `2000`)

### Upbit WebSocket
- `upbit.websocket.public_url`: string
- `upbit.websocket.private_url`: string
- `upbit.websocket.format`: `DEFAULT | SIMPLE | JSON_LIST | SIMPLE_LIST`
- `upbit.websocket.codes_per_connection`: integer
- `upbit.websocket.max_connections`: integer
- `upbit.websocket.keepalive.ping_interval_sec`: number
- `upbit.websocket.keepalive.ping_timeout_sec`: number
- `upbit.websocket.keepalive.allow_text_ping`: bool
- `upbit.websocket.ratelimit.connect_rps`: integer
- `upbit.websocket.ratelimit.message_rps`: integer
- `upbit.websocket.ratelimit.message_rpm`: integer
- `upbit.websocket.reconnect.enabled`: bool
- `upbit.websocket.reconnect.base_delay_ms`: integer
- `upbit.websocket.reconnect.max_delay_ms`: integer
- `upbit.websocket.reconnect.jitter_ms`: integer

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
- FAIL:
  - missing required columns (`ts_ms/open/high/low/close/volume_base`)
  - null or parse-failed `ts_ms`
  - null in required OHLCV fields
  - OHLC consistency violations (`high < max(open, close, low)` or `low > min(open, close, high)`)
- WARN:
  - non-monotonic `ts_ms` (`NON_MONOTONIC_FOUND` / `NON_MONOTONIC_SORTED`)
  - duplicate `ts_ms` found or dropped
  - invalid rows dropped
  - type cast failure rows found
- INFO (status remains OK by default):
  - timeframe gap(s) found (`GAPS_FOUND`) controlled by `data.qa.gap_severity`
  - `volume_quote_est=true` (`VOLUME_QUOTE_ESTIMATED`) controlled by `data.qa.quote_est_severity`

### Manifest fields
- `quote`, `symbol`, `market`, `tf`
- `source_csv_relpath`, `source_csv_size`, `source_csv_mtime`
- `ingested_at`, `rows`, `min_ts_ms`, `max_ts_ms`
- `duplicates_dropped`, `non_monotonic_found`, `gaps_found`
- `invalid_rows_dropped`, `ohlc_violations`
- `status` (`OK | WARN | FAIL`), `reasons_json`, `error_message`
- `timestamp_source`, `timestamp_policy`, `engine`
