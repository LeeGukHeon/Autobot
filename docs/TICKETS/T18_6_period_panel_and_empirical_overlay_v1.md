# T18.6 Period Panel And Empirical Overlay v1

## Goal
- Close the two highest-value remaining non-data-limited gaps after `T18.4` and `T18.5`.
- Make `v4` methodology closer to the referenced literature without changing the core deployment contract:
  - `backtest = fixed sanity gate`
  - `paper = learned runtime final gate`
  - runtime adaptation stays in the operational overlay layer

## Scope
In scope:
- persist `per-period` OOS evaluation rows for `v4` walk-forward windows and trial records
- prefer the `per-period differential panel` in `White RC / Hansen SPA`
- replace quantile-only overlay recalibration with an empirical state-score model derived from paper history

Out of scope:
- nested walk-forward threshold re-estimation
- replacing the paper final gate with a full sequential hypothesis framework
- extending the same panel/calibration workflow to `v3`

## Why
Two practical gaps remained even after the earlier methodology work:

1. `RC/SPA` still operated on coarse OOS slices rather than the finest aligned OOS units currently available.
2. runtime overlay calibration still relied too much on bounded quantile heuristics instead of a fitted state-to-outcome model.

## Implementation Notes

### Phase A. Period-Level OOS Panel
- persist `oos_periods` for each walk-forward window
- persist `test_oos_periods` for each trial record
- flatten those rows into `trial_panel`
- make `multiple_testing.py` prefer `oos_periods`, then fallback to `oos_slices`, then fallback to window-level metrics

Expected outcome:
- `White RC / Hansen SPA` operate on a denser aligned differential panel without changing legacy artifact compatibility

### Phase B. Empirical Overlay Calibration
- extend paper summary/smoke outputs with:
  - `operational_regime_score_mean`
  - `operational_breadth_ratio_mean`
  - `operational_max_positions_mean`
- build a fitted paper-history model from observable runtime state:
  - features:
    - `operational_regime_score_mean`
    - `operational_breadth_ratio_mean`
    - `micro_quality_score_mean`
  - target:
    - signed-log `calmar-like` realized performance
- load the fitted coefficients into runtime operational settings
- use the fitted state score in:
  - `risk_multiplier`
  - dynamic `max_positions`

Expected outcome:
- runtime overlay uses empirically fitted state coefficients instead of fixed hand-tuned score blending whenever a calibration artifact exists

## Completion Criteria
- `train_v4_crypto_cs` writes `oos_periods`
- `multiple_testing.py` prefers period keys when present
- paper smoke reports include mean regime/breadth/max-position state
- calibration artifact contains empirical score-model coefficients
- runtime overlay reads and uses those coefficients
- targeted pytest coverage exists for both paths
