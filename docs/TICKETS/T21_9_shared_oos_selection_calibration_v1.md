# T21.9 Shared OOS Selection Calibration v1

- Date: 2026-03-10
- Status: implemented locally

## Goal

Add a compact, auditable out-of-sample calibration artifact that both `paper` and `live`
consume through the same strategy runtime.

## Why

Cross-sectional rank selection is more robust than a global raw threshold, but runtime still
needs a probability-like score with better economic interpretation than the raw booster output.

This ticket adds:

- compact OOS isotonic calibration
- registry persistence
- predictor/runtime loading
- shared paper/live use for rank-based selection metadata

## Scope

- fit isotonic calibration on concatenated walk-forward OOS rows
- persist `selection_calibration.json`
- expose calibration through `ModelPredictor`
- use calibrated selection scores in `ModelAlphaStrategyV1` when selection mode is rank-based
- preserve raw-threshold behavior when manual `min_prob` forces raw mode

## Non-Goals

- cost-aware utility optimization
- live-only execution heuristics
- replacing the order admissibility gate

## Acceptance

- `train_v4_crypto_cs` writes `selection_calibration.json`
- `train_config.yaml` includes `selection_calibration`
- `ModelPredictor.predict_selection_scores()` applies the artifact
- `ModelAlphaStrategyV1` uses calibrated scores for shared rank-policy runtime
- tests cover both fit and runtime consumption

