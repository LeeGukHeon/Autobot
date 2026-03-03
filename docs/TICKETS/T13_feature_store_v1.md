# T13 Feature Store v1 + Labeling v1

## Goal
- Build reproducible, lookahead-safe training data from `candles_v1` for immediate T14 model experiments.
- Establish data contract artifacts: `feature_spec`, `label_spec`, `manifest`, `build_report`, `validate_report`.

## Scope
- New package: `autobot/features/`
  - `feature_spec.py`: config/spec parsing + fingerprint/hash helpers
  - `feature_set_v1.py`: OHLCV trailing features + factor/liquidity feature helpers
  - `labeling_v1.py`: forward-return labels + neutral policy
  - `store.py`: feature manifest schema/normalization I/O
  - `pipeline.py`: `build|validate|sample|stats` orchestration
- CLI extension in `autobot/cli.py`:
  - `python -m autobot.cli features build ...`
  - `python -m autobot.cli features validate ...`
  - `python -m autobot.cli features sample ...`
  - `python -m autobot.cli features stats ...`
- New config: `config/features.yaml`
- Tests:
  - `tests/test_features_v1.py`
  - `tests/test_labeling_v1.py`
  - `tests/test_lookahead_guard.py`

## Data Contract
- Output root: `data/features/features_v1/`
- Partitioning: `tf=<tf>/market=<market>/part-000.parquet`
- Meta:
  - `_meta/feature_spec.json`
  - `_meta/label_spec.json`
  - `_meta/manifest.parquet`
  - `_meta/build_report.json`
  - `_meta/validate_report.json`

## Safety Rules
- Feature calculation uses trailing-only windows.
- Labels use `close[t+h]` and never back-propagate into features.
- Universe `static_start` mode uses `start - lookback_days` historical window only.
- Leakage smoke included in `features validate`.

## Next Ticket Trigger (T13.1)
- If T14 baseline misses threshold (e.g. AUC < 0.55 or Precision@Top5% < 0.60, or cost-adjusted EV < 0),
  create T13.1 for Upbit orderbook/trade collection and microstructure feature expansion.
