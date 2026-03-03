# T14 Model Training & Registry v1

## Goal
- Build leakage-safe train/valid/test pipeline from `features_v1`.
- Run two tracks in one command:
  - Track A: baseline (`SGDClassifier + StandardScaler.partial_fit`)
  - Track B: booster (`XGBoost`, fallback `HistGradientBoostingClassifier`) + short sweep
- Register champion artifact and evaluation outputs for downstream T15 usage.

## Scope
- New package: `autobot/models/`
  - `dataset_loader.py`
  - `split.py`
  - `metrics.py`
  - `train_v1.py`
  - `registry.py`
  - `model_card.py`
- CLI extension in `autobot/cli.py`:
  - `python -m autobot.cli model train ...`
  - `python -m autobot.cli model eval ...`
  - `python -m autobot.cli model list ...`
  - `python -m autobot.cli model show ...`
- New config: `config/train.yaml`
- New tests:
  - `tests/test_model_registry.py`
  - `tests/test_time_split_embargo.py`
  - `tests/test_precision_at_k.py`

## Registry Contract
- Root: `models/registry/<model_family>/<run_id>/`
- Required files:
  - `model.bin`
  - `metrics.json`
  - `thresholds.json`
  - `feature_spec.json`
  - `label_spec.json`
  - `train_config.yaml`
  - `data_fingerprint.json`
  - `leaderboard_row.json`
  - `model_card.md`
- Family pointers:
  - `latest.json`
  - `champion.json`

## Evaluation Policy
- Strict time split only (`shuffle` disabled).
- Embargo around split boundaries: `±horizon_bars`.
- Test set is evaluated once per selected model.
- Primary selection key:
  1. `Precision@Top5%`
  2. `PR-AUC`
  3. `ROC-AUC`

## Threshold Policy
- Auto-generate thresholds on validation split:
  - `top_1pct`
  - `top_5pct`
  - `top_10pct`
  - `ev_opt` (scan-based cost-adjusted optimum)

## T13.1 Trigger Policy
- Raise data expansion recommendation when champion has one or more:
  - low PR-AUC
  - low Precision@Top5%
  - negative EV@Top5%
  - excessive per-market concentration
- Recommended next data:
  - Upbit orderbook WS
  - Upbit trade WS
