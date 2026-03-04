# T18: FeatureSet v3 (Multi-TF + Micro Mandatory) + Model Backtest Proxy

## Goal
- Define a stable v3 feature contract that combines:
  - base `5m` OHLCV derived features
  - lookahead-safe `1m/15m/60m/240m` context
  - mandatory `micro_v1` features
- Add `trainer=v3_mtf_micro` and a fast `modelbt` proxy command.

## Scope
- Added modules:
  - `autobot/features/multitf_join_v1.py`
  - `autobot/features/micro_required_join_v1.py`
  - `autobot/features/feature_set_v3.py`
  - `autobot/features/pipeline_v3.py`
  - `autobot/models/train_v3_mtf_micro.py`
  - `autobot/models/modelbt_proxy.py`
- Added config:
  - `config/features_v3.yaml`
- CLI extension:
  - `features build|validate|stats --feature-set v3`
  - `model train --trainer v3_mtf_micro --feature-set v3`
  - `modelbt run ...`

## Core Policy
- Micro is mandatory:
  - rows without micro coverage are dropped
  - build report tracks `rows_dropped_no_micro`
- Effective train window is intersection-driven:
  - report includes `requested_start/end` and `effective_start/end`
- Leakage safety:
  - higher TF joins use backward asof (`<= base ts`)
  - validate checks source timestamps (`src_ts_*`, `one_m_last_ts`) do not exceed base `ts_ms`

## Output Contract (v3)
- Dataset root: `data/features/features_v3`
- Partitioning: `tf=5m/market=<market>/date=<YYYY-MM-DD>/part-000.parquet`
- Meta:
  - `_meta/manifest.parquet`
  - `_meta/feature_spec.json`
  - `_meta/label_spec.json`
  - `_meta/build_report.json`
  - `_meta/validate_report.json`

## DoD Notes
- Build fails with:
  - `NEED_MORE_MICRO_DAYS_OR_LOOSEN_UNIVERSE`
  - when `rows_final < min_rows_for_train`
- `modelbt run` writes:
  - `equity.csv`
  - `summary.json`
  - `trades.csv`
  - `diagnostics.json`
