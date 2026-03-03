# Config Schema

## Common
- `mode`: `backtest | paper | live`
- `timezone`: timezone string (default: `Asia/Seoul`)
- `log_level`: `DEBUG | INFO | WARNING | ERROR`

## Live
- `live.enabled`: bool
- `live.bot_id`: string

### Live State
- `live.state.db_path`: path (default: `data/state/live_state.db`)
- `live.state.run_lock`: bool (default: `true`)

### Live Startup
- `live.startup.reconcile`: bool (default: `true`)
- `live.startup.unknown_open_orders_policy`: `halt | ignore | cancel` (default: `halt`)
- `live.startup.unknown_positions_policy`: `halt | import_as_unmanaged | attach_default_risk` (default: `halt`)
- `live.startup.allow_cancel_external_orders`: bool (default: `false`)

### Live Sync
- `live.sync.poll_interval_sec`: integer (default: `15`)
- `live.sync.use_private_ws`: bool (default: `false`)
  - `true`면 `live run`에서 private WS(`myOrder`,`myAsset`) 이벤트 기반 동기화를 사용
  - REST polling은 안전망으로 유지(저빈도)

### Live Orders
- `live.orders.identifier_prefix`: string (default: `AUTOBOT`)
- `live.orders` executor request semantics:
  - Upbit submit/replace default limit behavior is `time_in_force` key omission.
  - `GTC` is treated as legacy compatibility input and mapped to omission; it is not forwarded to Upbit.

### Executor Runtime (Env, C++)
- `AUTOBOT_EXECUTOR_DEBUG_TIF_COMPAT`: bool (default: `false`)
  - when `true`, logs one debug line per request for `GTC -> omit` mapping.

### Live Default Risk
- `live.default_risk.sl_pct`: number (default: `2.0`)
- `live.default_risk.tp_pct`: number (default: `3.0`)
- `live.default_risk.trailing_enabled`: bool (default: `false`)

### Live Risk Manager
- `live.risk.enabled`: bool (default: `false`)
- `live.risk.exit_aggress_bps`: number (default: `8.0`)
- `live.risk.timeout_sec`: integer (default: `20`)
- `live.risk.replace_max`: integer (default: `2`)
- `live.risk.default_trail_pct`: number (default: `1.0`)

## Universe
- `universe.quote_currency`: string
- `universe.top_n_by_acc_trade_price_24h`: integer

## Strategy
- `strategy.universe.quote`: string (default: `KRW`)
- `strategy.universe.top_n`: integer (default: `20`)
- `strategy.universe.refresh_sec`: number (default: `60`)
- `strategy.universe.hold_sec`: number (default: `120`)
- `strategy.candidates_v1.enabled`: bool
- `strategy.candidates_v1.momentum_window_sec`: integer (default: `60`)
- `strategy.candidates_v1.min_momentum_pct`: number (default: `0.2`)

## Risk (Paper Runtime)
- `risk.starting_krw`: number (default: `50000`)
- `risk.per_trade_krw`: number (default: `10000`)
- `risk.max_positions`: integer (default: `2`)
- `risk.min_order_krw`: number (default: `5000`)
- `risk.order_timeout_sec`: number (default: `20`)
- `risk.reprice_max_attempts`: integer (default: `2`)
- `risk.cooldown_sec_after_fail`: integer (default: `60`)
- `risk.max_consecutive_failures`: integer (default: `5`)

## Backtest
- `backtest.dataset_name`: string (default: `candles_v1`)
- `backtest.parquet_root`: path (default: `data/parquet`)
- `backtest.tf`: timeframe string (default: `1m`)
- `backtest.from_ts_ms`: int64 nullable
- `backtest.to_ts_ms`: int64 nullable
- `backtest.duration_days`: integer nullable
- `backtest.seed`: integer (default: `0`)

### Backtest Universe
- `backtest.universe.mode`: `static_start | fixed_list` (default: `static_start`)
- `backtest.universe.quote`: string (default: `KRW`)
- `backtest.universe.top_n`: integer (default: `20`)

### Backtest Data
- `backtest.data.dense_grid`: bool (default: `false`)

### Backtest Execution
- `backtest.execution.order_timeout_bars`: integer (default: `5`)
- `backtest.execution.reprice_max_attempts`: integer (default: `1`)
- `backtest.execution.reprice_tick_steps`: integer (default: `1`)
- `backtest.execution.rules_ttl_sec`: integer (default: `86400`)

### Backtest Output
- `backtest.output.root`: path (default: `data/backtest`)

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
- `data.qa.ohlc_violation_policy`: `drop_row_and_warn | fail` (default: `drop_row_and_warn`)

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

## Features
- config file: `config/features.yaml`

### Features Build
- `features.dataset_name`: string (default: `features_v1`)
- `features.input_dataset`: string (default: `candles_v1`)
- `features.float_dtype`: `float32 | float64` (default: `float32`)
- `features.parquet_root`: path override (default: `data.parquet_root` or `storage.parquet_dir`)
- `features.features_root`: path override (default: `storage.features_dir`)

### Features Universe
- `universe.quote`: string (default: `KRW`)
- `universe.mode`: `static_start | fixed_list` (default: `static_start`)
- `universe.top_n`: integer (default: `20`)
- `universe.lookback_days`: integer (default: `7`)
- `universe.fixed_list`: string array (used only when `mode=fixed_list`)

### Features Time Range
- `time_range.start`: `YYYY-MM-DD` (UTC day start)
- `time_range.end`: `YYYY-MM-DD` (UTC day end, inclusive)

### Feature Set v1
- `feature_set_v1.windows.ret`: integer array (default: `[1,3,6,12]`)
- `feature_set_v1.windows.rv`: integer array (default: `[12,36]`)
- `feature_set_v1.windows.ema`: integer array (default: `[12,36]`)
- `feature_set_v1.windows.rsi`: integer (default: `14`)
- `feature_set_v1.windows.atr`: integer (default: `14`)
- `feature_set_v1.windows.vol_z`: integer (default: `36`)
- `feature_set_v1.enable_factor_features`: bool (default: `true`)
- `feature_set_v1.factor_markets`: string array (default: `["KRW-BTC","KRW-ETH"]`)
- `feature_set_v1.enable_liquidity_rank`: bool (default: `false`)

### Label v1
- `label_v1.horizon_bars`: integer (default: `12`)
- `label_v1.thr_bps`: number (default: `15`)
- `label_v1.neutral_policy`: `drop | keep_as_class` (default: `drop`)
- `label_v1.fee_bps_est`: number (default: `10`)
- `label_v1.safety_bps`: number (default: `5`)

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

## CLI: Paper Run
- `python -m autobot.cli paper run --duration-sec 600 --quote KRW --top-n 20`
- Options:
  - `--duration-sec`: integer runtime seconds
  - `--quote`: quote currency
  - `--top-n`: top-N universe size
  - `--print-every-sec`: snapshot print/log interval
  - `--starting-krw`: initial paper cash
  - `--per-trade-krw`: per-order notional target
  - `--max-positions`: max simultaneous positions

## CLI: Backtest Run
- `python -m autobot.cli backtest run --market KRW-BTC --tf 5m --duration-days 7`
- Main options:
  - `--market` / `--markets`
  - `--tf`
  - `--from-ts-ms`, `--to-ts-ms`, `--duration-days`
  - `--quote`, `--top-n`, `--universe-mode`
  - `--dense-grid`
  - `--starting-krw`, `--per-trade-krw`, `--max-positions`, `--min-order-krw`
  - `--order-timeout-bars`, `--reprice-max-attempts`

## CLI: Features
- Build:
  - `python -m autobot.cli features build --tf 5m --quote KRW --top-n 20 --start 2024-01-01 --end 2026-03-01 --feature-set v1 --label-set v1 --workers 1 --fail-on-warn false`
- Validate:
  - `python -m autobot.cli features validate --tf 5m --quote KRW --top-n 20`
- Sample:
  - `python -m autobot.cli features sample --tf 5m --market KRW-BTC --rows 10`
- Stats:
  - `python -m autobot.cli features stats --tf 5m --quote KRW --top-n 20`

## CLI: Live State
- `python -m autobot.cli live status`
- `python -m autobot.cli live reconcile --dry-run`
- `python -m autobot.cli live reconcile --apply`
- `python -m autobot.cli live reconcile --apply --allow-cancel-external`
- `python -m autobot.cli live run --duration-sec 120`
- `python -m autobot.cli live run --allow-cancel-external`
- `python -m autobot.cli live export-state`

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
  - `OHLC_VIOLATIONS` when `data.qa.ohlc_violation_policy=drop_row_and_warn`
- INFO (status remains OK by default):
  - timeframe gap(s) found (`GAPS_FOUND`) controlled by `data.qa.gap_severity`
  - `volume_quote_est=true` (`VOLUME_QUOTE_ESTIMATED`) controlled by `data.qa.quote_est_severity`
- FAIL:
  - `OHLC_VIOLATIONS` when `data.qa.ohlc_violation_policy=fail`

### Manifest fields
- `quote`, `symbol`, `market`, `tf`
- `source_csv_relpath`, `source_csv_size`, `source_csv_mtime`
- `ingested_at`, `rows`, `min_ts_ms`, `max_ts_ms`
- `duplicates_dropped`, `non_monotonic_found`, `gaps_found`
- `invalid_rows_dropped`, `ohlc_violations`
- `status` (`OK | WARN | FAIL`), `reasons_json`, `error_message`
- `timestamp_source`, `timestamp_policy`, `engine`
