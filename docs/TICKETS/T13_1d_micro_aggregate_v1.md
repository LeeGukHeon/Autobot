# T13.1d Micro Aggregation v1

## Goal
- Convert raw micro sources (`REST ticks`, `WS trade`, `WS orderbook`) to lookahead-safe bar features for downstream modeling.
- Enforce bar-level availability/coverage flags so partial collection windows are not misinterpreted as true zero.
- Produce `micro_v1` artifacts directly consumable by `T13.1e` and `T14.2`.

## Scope
- Added package: `autobot/data/micro/`
  - `raw_readers.py`: streaming `jsonl.zst` reader + row normalization
  - `trade_aggregator_v1.py`: 1m trade aggregation (`ws` + `rest`) + precedence merge
  - `orderbook_aggregator_v1.py`: 1m orderbook aggregation
  - `merge_micro_v1.py`: trade/book merge + aggregate orchestration
  - `resample_v1.py`: 1m -> 5m resample
  - `store.py`: parquet partition write + manifest/report helpers
  - `validate_micro_v1.py`: schema/range/coverage/join validation + stats
  - `spec_micro_v1.py`: `spec.json` generation
- CLI extension in `autobot/cli.py`:
  - `python -m autobot.cli micro aggregate ...`
  - `python -m autobot.cli micro validate ...`
  - `python -m autobot.cli micro stats ...`
- New config: `config/micro.yaml`
- Tests:
  - `tests/test_micro_alignment_mode.py`
  - `tests/test_micro_trade_merge_precedence.py`
  - `tests/test_micro_orderbook_agg_basic.py`
  - `tests/test_micro_resample_1m_to_5m.py`
  - `tests/test_micro_validate_reports.py`

## Data Contract
- Output root: `data/parquet/micro_v1/`
- Partitioning:
  - `tf=1m/market=<market>/date=<YYYY-MM-DD>/part-*.parquet`
  - `tf=5m/market=<market>/date=<YYYY-MM-DD>/part-*.parquet`
- Meta:
  - `_meta/manifest.parquet`
  - `_meta/aggregate_report.json`
  - `_meta/validate_report.json`
  - `_meta/spec.json`

## Key Policies
- Trade merge precedence: per `(market, bar_ts)` use `ws` first, fallback to `rest`, else `none`.
- Orderbook source: `ws` only.
- Mandatory coverage/availability columns:
  - `trade_events`, `book_events`, `trade_coverage_ms`, `book_coverage_ms`
  - `micro_trade_available`, `micro_book_available`, `micro_available`
- Alignment mode:
  - supports `start|end|auto`
  - `auto` compares match ratio against base candles and writes chosen mode into report.

## Validation Rules
- FAIL:
  - required schema mismatch
  - non-monotonic `ts_ms`
  - high ratio of invalid volume/price values
  - low `join_match_ratio` when overlap exists
- WARN:
  - low `micro_available_ratio`
  - `trade_source=none` saturation
  - short coverage concentration
  - no overlap with base candles in selected window

## Non-goals
- `features_v2` merge (`T13.1e`)
- train pipeline changes (`T14.2`)
- collector-side protocol expansion
