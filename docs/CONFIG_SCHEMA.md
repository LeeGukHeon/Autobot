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
- `strategy.micro_gate.enabled`: bool (default: `false`)
- `strategy.micro_gate.mode`: `trade_only | trade_and_book` (default: `trade_only`)
- `strategy.micro_gate.on_missing`: `warn_allow | block | allow` (default: `warn_allow`)
- `strategy.micro_gate.stale_ms`: integer (default: `120000`)
- `strategy.micro_gate.trade.min_trade_events`: integer (default: `1`)
- `strategy.micro_gate.trade.min_trade_coverage_ms`: integer (default: `0`)
- `strategy.micro_gate.trade.min_trade_notional_krw`: number (default: `0`)
- `strategy.micro_gate.book.max_spread_bps`: number (default: `0`, disabled when `0`)
- `strategy.micro_gate.book.min_depth_top5_krw`: number (default: `0`, disabled when `0`)
- `strategy.micro_gate.book.min_book_events`: integer (default: `0`)
- `strategy.micro_gate.book.min_book_coverage_ms`: integer (default: `0`)
- `strategy.micro_gate.live_ws.enabled`: bool (default: `false`)
- `strategy.micro_gate.live_ws.window_sec`: integer (default: `60`)
- `strategy.micro_gate.live_ws.orderbook_topk`: integer (default: `5`)
- `strategy.micro_gate.live_ws.orderbook_level`: integer|string (default: `0`)
- `strategy.micro_gate.live_ws.subscribe_format`: string (default: `DEFAULT`)
- `strategy.micro_gate.live_ws.max_markets`: integer (default: `30`)
- `strategy.micro_gate.live_ws.reconnect.max_per_min`: integer (default: `3`)
- `strategy.micro_gate.live_ws.reconnect.backoff_base_sec`: number (default: `1`)
- `strategy.micro_gate.live_ws.reconnect.backoff_max_sec`: number (default: `32`)
- `strategy.micro_order_policy.enabled`: bool (default: `false`)
- `strategy.micro_order_policy.mode`: `trade_only | trade_and_book` (default: `trade_only`)
- `strategy.micro_order_policy.on_missing`: `static_fallback | conservative | abort` (default: `static_fallback`)
- `strategy.micro_order_policy.tiering.w_notional`: number (default: `1.0`)
- `strategy.micro_order_policy.tiering.w_events`: number (default: `0.5`)
- `strategy.micro_order_policy.tiering.t1`: number (default: `6.0`)
- `strategy.micro_order_policy.tiering.t2`: number (default: `9.0`)
- `strategy.micro_order_policy.tiers.LOW.timeout_ms`: integer (default: `120000`)
- `strategy.micro_order_policy.tiers.LOW.replace_interval_ms`: integer (default: `60000`)
- `strategy.micro_order_policy.tiers.LOW.max_replaces`: integer (default: `1`)
- `strategy.micro_order_policy.tiers.LOW.price_mode`: `PASSIVE_MAKER | JOIN | CROSS_1T` (default: `PASSIVE_MAKER`)
- `strategy.micro_order_policy.tiers.LOW.max_chase_bps`: integer (default: `10`)
- `strategy.micro_order_policy.tiers.MID.timeout_ms`: integer (default: `45000`)
- `strategy.micro_order_policy.tiers.MID.replace_interval_ms`: integer (default: `15000`)
- `strategy.micro_order_policy.tiers.MID.max_replaces`: integer (default: `3`)
- `strategy.micro_order_policy.tiers.MID.price_mode`: `PASSIVE_MAKER | JOIN | CROSS_1T` (default: `JOIN`)
- `strategy.micro_order_policy.tiers.MID.max_chase_bps`: integer (default: `15`)
- `strategy.micro_order_policy.tiers.HIGH.timeout_ms`: integer (default: `15000`)
- `strategy.micro_order_policy.tiers.HIGH.replace_interval_ms`: integer (default: `5000`)
- `strategy.micro_order_policy.tiers.HIGH.max_replaces`: integer (default: `5`)
- `strategy.micro_order_policy.tiers.HIGH.price_mode`: `PASSIVE_MAKER | JOIN | CROSS_1T` (default: `CROSS_1T`)
- `strategy.micro_order_policy.tiers.HIGH.max_chase_bps`: integer (default: `20`)
- `strategy.micro_order_policy.safety.min_replace_interval_ms_global`: integer (default: `1500`)
- `strategy.micro_order_policy.safety.max_replaces_per_min_per_market`: integer (default: `10`)
- `strategy.micro_order_policy.safety.forbid_post_only_with_cross`: bool (default: `true`)
- `strategy.model_alpha_v1.model_ref`: string (default: `latest_v3`)
- `strategy.model_alpha_v1.model_family`: string (default: `train_v3_mtf_micro`)
- `strategy.model_alpha_v1.feature_set`: string (default: `v3`)
- `strategy.model_alpha_v1.selection.top_pct`: number (default: `0.05`)
- `strategy.model_alpha_v1.selection.min_prob`: number (default: `0.58`)
- `strategy.model_alpha_v1.selection.min_candidates_per_ts`: integer (default: `10`)
- `strategy.model_alpha_v1.position.max_positions_total`: integer (default: `3`)
- `strategy.model_alpha_v1.position.cooldown_bars`: integer (default: `6`)
- `strategy.model_alpha_v1.exit.mode`: `hold | risk` (default: `hold`)
- `strategy.model_alpha_v1.exit.hold_bars`: integer (default: `6`)
- `strategy.model_alpha_v1.exit.tp_pct`: number (default: `0.02`)
- `strategy.model_alpha_v1.exit.sl_pct`: number (default: `0.01`)
- `strategy.model_alpha_v1.exit.trailing_pct`: number (default: `0.0`)
- `strategy.model_alpha_v1.execution.price_mode`: `PASSIVE_MAKER | JOIN | CROSS_1T` (default: `JOIN`)
- `strategy.model_alpha_v1.execution.timeout_bars`: integer (default: `2`)
- `strategy.model_alpha_v1.execution.replace_max`: integer (default: `2`)

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
- `backtest.strategy.name`: `candidates_v1 | model_alpha_v1` (default: `candidates_v1`)
- `backtest.strategy.model_ref`: string (default: `latest_v3`)
- `backtest.strategy.model_family`: string (default: `train_v3_mtf_micro`)
- `backtest.strategy.feature_set`: string (default: `v3`)
- `backtest.strategy.model_registry_root`: path (default: `models/registry`)
- `backtest.strategy.model_feature_dataset_root`: path nullable (default: `null`)

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

### Feature Set v2
- config file: `config/features_v2.yaml`
- `features_v2.output_dataset`: string (default: `features_v2`)
- `features_v2.tf`: timeframe string (default: `5m`)
- `features_v2.base_candles_dataset`: `auto | candles_api_v1 | candles_v1 | <path>`
- `features_v2.micro_dataset`: dataset name or path (default: `micro_v1`)
- `features_v2.alignment_mode`: `auto | start | end` (default: `auto`)
- `features_v2.use_precomputed_features_v1`: bool (default: `false`)
- `features_v2.precomputed_features_v1_dataset`: string (default: `features_v1`)
- `features_v2.min_rows_for_train`: integer (default: `5000`)

### Feature Set v2 Micro Filter
- `features_v2.micro_filter.require_micro_available`: bool (default: `true`)
- `features_v2.micro_filter.min_trade_events`: integer (default: `1`)
- `features_v2.micro_filter.min_trade_coverage_ms`: integer (default: `60000`)
- `features_v2.micro_filter.min_book_events`: integer (default: `1`)
- `features_v2.micro_filter.min_book_coverage_ms`: integer (default: `60000`)

### Feature Set v2 Validation
- `features_v2.validation.join_match_warn`: float (default: `0.98`)
- `features_v2.validation.join_match_fail`: float (default: `0.90`)

### Feature Set v3
- config file: `config/features_v3.yaml`
- `features_v3.output_dataset`: string (default: `features_v3`)
- `features_v3.tf`: timeframe string (`5m` only)
- `features_v3.base_candles_dataset`: `auto | candles_api_v1 | candles_v1 | <path>`
- `features_v3.micro_dataset`: dataset name or path (default: `micro_v1`)
- `features_v3.high_tfs`: timeframe array subset of `[15m,60m,240m]`
- `features_v3.high_tf_staleness_multiplier`: float (default: `2.0`)
- `features_v3.one_m_required_bars`: integer (default: `5`)
- `features_v3.one_m_max_missing_ratio`: float (default: `0.2`)
- `features_v3.sample_weight_half_life_days`: float (default: `60`)
- `features_v3.min_rows_for_train`: integer (default: `5000`)
- `features_v3.require_micro_validate_pass`: bool (default: `true`)
- `features_v3.validation.leakage_fail_on_future_ts`: bool (default: `true`)

## Model Training (T14)
- config file: `config/train.yaml`
- `train.registry_root`: path (default: `models/registry`)
- `train.logs_root`: path (default: `logs`)
- `train.model_family`: string (default: `train_v1`)
- `train.tf`: timeframe (default: `5m`)
- `train.quote`: quote filter (default: `KRW`)
- `train.top_n`: integer (default: `20`)
- `train.start`: `YYYY-MM-DD`
- `train.end`: `YYYY-MM-DD`
- `train.task`: `cls` (v1)
- `train.run_baseline`: bool
- `train.run_booster`: bool
- `train.booster_sweep_trials`: integer (default: `15`)
- `train.seed`: integer (default: `42`)
- `train.nthread`: integer (default: `6`)
- `train.batch_rows`: integer (default: `200000`)
- `train.train_ratio`: float (default: `0.70`)
- `train.valid_ratio`: float (default: `0.15`)
- `train.test_ratio`: float (default: `0.15`)
- `train.embargo_bars`: integer (default: `12`)
- `train.baseline_alpha`: float (default: `0.0001`)
- `train.baseline_epochs`: integer (default: `3`)
- `train.fee_bps_est`: float (default: `10.0`)
- `train.safety_bps`: float (default: `5.0`)
- `train.ev_scan_steps`: integer (default: `200`)
- `train.ev_min_selected`: integer (default: `100`)
- `train.gate_min_pr_auc`: float (default: `0.50`)
- `train.gate_min_precision_top5`: float (default: `0.50`)
- `train.gate_max_two_market_bias`: float (default: `0.60`)

## CLI: Model
- Train:
  - `python -m autobot.cli model train --tf 5m --quote KRW --top-n 20 --start 2024-01-01 --end 2026-03-01 --feature-set v1 --label-set v1 --task cls --run-baseline true --run-booster true --booster-sweep-trials 15 --seed 42 --nthread 6`
- Train (v3):
  - `python -m autobot.cli model train --trainer v3_mtf_micro --feature-set v3 --tf 5m --quote KRW --top-n 50 --start 2026-02-24 --end 2026-03-05 --seed 42 --booster-sweep-trials 30`
- Eval:
  - `python -m autobot.cli model eval --model-ref latest --split test --report-csv out.csv`
- List:
  - `python -m autobot.cli model list`
- Show:
  - `python -m autobot.cli model show --model-ref latest`

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
  - `--micro-gate`: `on | off`
  - `--micro-gate-mode`: `trade_only | trade_and_book`
  - `--micro-gate-on-missing`: `warn_allow | block | allow`
  - `--micro-order-policy`: `on | off`
  - `--micro-order-policy-mode`: `trade_only | trade_and_book`
  - `--micro-order-policy-on-missing`: `static_fallback | conservative | abort`

## CLI: Backtest Run
- `python -m autobot.cli backtest run --market KRW-BTC --tf 5m --duration-days 7`
- Main options:
  - `--dataset-name`, `--parquet-root`
  - `--market` / `--markets`
  - `--tf`
  - `--start`, `--end`, `--from-ts-ms`, `--to-ts-ms`, `--duration-days`
  - `--quote`, `--top-n`, `--universe-mode`
  - `--dense-grid`
  - `--starting-krw`, `--per-trade-krw`, `--max-positions`, `--min-order-krw`
  - `--order-timeout-bars`, `--reprice-max-attempts`
  - `--strategy`, `--model-ref`, `--model-family`, `--feature-set`
  - `--entry`, `--top-pct`, `--min-prob`, `--min-cands-per-ts`
  - `--exit-mode`, `--hold-bars`, `--tp-pct`, `--sl-pct`, `--trailing-pct`
  - `--cooldown-bars`, `--max-positions-total`
  - `--execution-price-mode`, `--execution-timeout-bars`, `--execution-replace-max`
  - `--micro-gate`, `--micro-gate-mode`, `--micro-gate-on-missing`
  - `--micro-order-policy`, `--micro-order-policy-mode`, `--micro-order-policy-on-missing`

## CLI: Features
- Build:
  - `python -m autobot.cli features build --tf 5m --quote KRW --top-n 20 --start 2024-01-01 --end 2026-03-01 --feature-set v1 --label-set v1 --workers 1 --fail-on-warn false`
- Build (v2):
  - `python -m autobot.cli features build --feature-set v2 --tf 5m --quote KRW --top-n 20 --start 2026-03-03 --end 2026-03-04 --base-candles auto --micro-dataset micro_v1 --require-micro true --dry-run false`
- Validate:
  - `python -m autobot.cli features validate --tf 5m --quote KRW --top-n 20`
- Validate (v2):
  - `python -m autobot.cli features validate --feature-set v2 --tf 5m --quote KRW --top-n 20`
- Sample:
  - `python -m autobot.cli features sample --tf 5m --market KRW-BTC --rows 10`
- Stats:
  - `python -m autobot.cli features stats --tf 5m --quote KRW --top-n 20`
- Stats (v2):
  - `python -m autobot.cli features stats --feature-set v2 --tf 5m --quote KRW --top-n 20`
- Build (v3):
  - `python -m autobot.cli features build --feature-set v3 --tf 5m --quote KRW --top-n 50 --start 2026-02-24 --end 2026-03-05`
- Validate (v3):
  - `python -m autobot.cli features validate --feature-set v3 --tf 5m --quote KRW --top-n 50`
- Stats (v3):
  - `python -m autobot.cli features stats --feature-set v3 --tf 5m --quote KRW --top-n 50`

## CLI: ModelBt Proxy
- `python -m autobot.cli modelbt run --model-ref latest_v3 --tf 5m --quote KRW --top-n 50 --start 2026-02-24 --end 2026-03-05 --select top_pct --top-pct 0.05 --hold-bars 6 --fee-bps 5`

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
