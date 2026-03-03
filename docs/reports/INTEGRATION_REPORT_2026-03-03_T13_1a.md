# INTEGRATION_REPORT_2026-03-03_T13_1a

## 1) Execution Commands
- `python -m pytest -q`
- `python -m autobot.cli data inventory --dataset candles_v1 --tf 1m,5m,15m,60m,240m --quote KRW --lookback-months 24 --out data/collect/_meta/candle_inventory_report.json`
- `python -m autobot.cli collect plan-candles --base-dataset candles_v1 --out data/collect/_meta/candle_topup_plan.json --lookback-months 24 --tf 1m,5m,15m,60m,240m --market-mode top_n_by_recent_value_est --top-n 50 --max-backfill-days-1m 90`
- `python -m autobot.cli collect candles --plan data/collect/_meta/candle_topup_plan.json --out-dataset candles_api_v1 --workers 1 --dry-run true`
- `python -m autobot.cli collect plan-candles --base-dataset candles_v1 --out data/collect/_meta/candle_topup_plan_btc.json --lookback-months 1 --tf 1m --market-mode fixed_list --markets KRW-BTC --max-backfill-days-1m 30`
- `python -m autobot.cli collect candles --plan data/collect/_meta/candle_topup_plan_btc.json --out-dataset candles_api_v1 --workers 1 --dry-run false --max-requests 3`

## 2) Window / Market Selection
- Inventory snapshot (`candles_v1`, KRW, 24 months):
  - total_pairs: 1079
  - with_data_pairs: 1079
  - average_coverage_pct: 67.48547
- Plan A (`top_n_by_recent_value_est`, top_n=50):
  - selected_markets: 50
  - targets: 358
  - skipped_ranges: 22
- Plan B (`fixed_list=KRW-BTC`, `tf=1m`, 1 month):
  - selected_markets: 1
  - targets: 1
  - target reason: `MISSING_TAIL`
  - coverage_before_pct: 73.12481

## 3) Collection Results
- Dry-run plan execution:
  - discovered/selected/processed: 358 / 358 / 0
  - calls_made: 0
- Live limited execution (`max_requests=3`, KRW-BTC 1m):
  - discovered/selected/processed: 1 / 1 / 1
  - ok/warn/fail: 0 / 1 / 0
  - calls_made/throttled/backoff: 3 / 0 / 0
  - warning reason: `MAX_REQUESTS_BUDGET_REACHED`
  - output parquet: `data/parquet/candles_api_v1/tf=1m/market=KRW-BTC/part-000.parquet`
  - output rows/min_ts_ms/max_ts_ms: 600 / 1772420760000 / 1772456700000

## 4) Validation / Coverage Delta
- `data/collect/_meta/candle_validate_report.json`
  - checked_files: 1
  - ok/warn/fail: 1 / 0 / 0
  - schema_ok: true
  - ohlc_ok: true
- Coverage delta (plan target basis):
  - average_before_pct: 73.12481
  - average_after_pct: 99.997685
  - average_delta_pct: 26.872875

## 5) Fail/Warn Top Reasons
- Collect WARN:
  - `MAX_REQUESTS_BUDGET_REACHED` (intentional due `--max-requests 3`)
  - Note: Upbit minute candles are emitted only when trades occur, so sparse/no-trade windows can validly surface as `NO_ROWS_COLLECTED`.
- Collect FAIL:
  - none
- Validate FAIL:
  - none

## 6) T13.1b / T13.1c Readiness
- T13.1b prerequisite check:
  - REST rate-limit-safe sequential runner exists (`workers_effective=1`, max request budget supported).
  - JSON reports under `data/collect/_meta` are generated.
- T13.1c prerequisite check:
  - No WS collector in this ticket by design.
  - CLI and data layout now separate base dataset (`candles_v1`) and API top-up dataset (`candles_api_v1`) for later merge/promote.
