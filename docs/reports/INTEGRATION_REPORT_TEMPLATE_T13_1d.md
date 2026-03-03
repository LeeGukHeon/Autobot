# INTEGRATION_REPORT_TEMPLATE_T13_1d

## 0) Summary
- Ticket: `T13.1d`
- Scope: raw micro (`REST ticks + WS trade + WS orderbook`) -> `micro_v1` parquet (`1m/5m`) + QA
- Result:
  - aggregate: `<run_id>`
  - validate: `checked=<n> ok=<n> warn=<n> fail=<n>`
  - pytest: `<PASS/FAIL>`

## 1) Executed Commands
- Aggregate:
  - `python -m autobot.cli micro aggregate --tf 1m,5m --start <YYYY-MM-DD> --end <YYYY-MM-DD> --quote KRW --top-n 20 --raw-ticks-root data/raw_ticks/upbit/trades --raw-ws-root data/raw_ws/upbit/quotation --out-root data/parquet/micro_v1 --mode append --chunk-rows 200000`
- Validate:
  - `python -m autobot.cli micro validate --tf 1m,5m --out-root data/parquet/micro_v1 --base-candles candles_v1`
- Stats:
  - `python -m autobot.cli micro stats --tf 1m,5m --out-root data/parquet/micro_v1`
- Test:
  - `python -m pytest -q`

## 2) Coverage / Availability
- Target period/markets/TF:
  - period: `<start> ~ <end>`
  - markets: `<count>`
  - tf: `1m,5m`
- `micro_available_ratio`:
  - overall: `<ratio>`
  - per-market min/max: `<min>/<max>`
- `trade_source` ratio:
  - ws/rest/none: `<ws>/<rest>/<none>`

## 3) Coverage_ms Distribution
- trade coverage (`p50/p90`): `<...>`
- book coverage (`p50/p90`): `<...>`
- partial collection impact assessment:
  - `<comment on MAX_REQUEST_BUDGET + ws/rest split + micro_available semantics>`

## 4) Validation / QA
- validate summary:
  - checked/ok/warn/fail: `<...>`
  - parse_ok_ratio: `<...>`
  - join_match_ratio: `<...>`
- warnings/failures:
  - `<list>`

## 5) Artifacts
- Dataset:
  - `data/parquet/micro_v1/tf=1m/market=*/date=*/part-*.parquet`
  - `data/parquet/micro_v1/tf=5m/market=*/date=*/part-*.parquet`
- Meta:
  - `data/parquet/micro_v1/_meta/manifest.parquet`
  - `data/parquet/micro_v1/_meta/aggregate_report.json`
  - `data/parquet/micro_v1/_meta/validate_report.json`
  - `data/parquet/micro_v1/_meta/spec.json`

## 6) Next Ticket Gate (T13.1e)
- Go/No-Go: `<GO/NO-GO>`
- Reason:
  - `<availability/coverage flags ready?>`
  - `<validation fail=0?>`
  - `<blocking issues?>`
