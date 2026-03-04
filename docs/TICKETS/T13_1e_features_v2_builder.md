# T13.1e FeatureSet v2 Builder

## Goal
- Build `features_v2` by joining OHLC(v1-equivalent) + `micro_v1`.
- Keep partial micro collection from degrading training quality via explicit micro coverage filters.
- Reuse `label_v1` while enforcing tail guard (drop last `H` bars per market).

## Scope
- Added modules:
  - `autobot/features/micro_join.py`
  - `autobot/features/feature_set_v2.py`
  - `autobot/features/v2_manifest.py`
  - `autobot/features/pipeline_v2.py`
- CLI extension:
  - `python -m autobot.cli features build --feature-set v2 ...`
  - `python -m autobot.cli features validate --feature-set v2 ...`
  - `python -m autobot.cli features stats --feature-set v2 ...`
- Added config:
  - `config/features_v2.yaml`
- Added tests:
  - `tests/test_features_v2_preflight.py`
  - `tests/test_features_v2_micro_join.py`
  - `tests/test_features_v2_label_tail_guard.py`
  - `tests/test_features_v2_micro_filtering.py`

## Data Contract
- Output root: `data/features/features_v2/`
- Partitioning: `tf=5m/market=<market>/date=<YYYY-MM-DD>/part-000.parquet`
- Meta:
  - `_meta/manifest.parquet`
  - `_meta/feature_spec.json`
  - `_meta/label_spec.json`
  - `_meta/build_report.json`
  - `_meta/validate_report.json`

## Safety Rules
- Preflight is mandatory:
  - Fail when micro period and base candles do not overlap.
  - Fail when label horizon coverage (`max_micro_ts + H*interval`) is missing in candles.
- `label_v1` tail guard:
  - drop last `H` bars per market before training output.
- Micro filter defaults:
  - `m_micro_available == true`
  - `m_trade_events >= 1`, `m_book_events >= 1`
  - `m_trade_coverage_ms >= 60000`, `m_book_coverage_ms >= 60000`
- Join quality is validated by `join_match_ratio`; no-overlap is fail in v2.

## Mode Policy
- Default: Mode A (`candles -> v1-equivalent features recompute -> micro join`).
- Optional: Mode B (`features_v1 -> micro join`) via `use_precomputed_features_v1=true`.

## Out of Scope
- No delete/replace of legacy v1 modules.
- No model training changes in this ticket.
