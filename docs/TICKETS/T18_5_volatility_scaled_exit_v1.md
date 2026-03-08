# T18.5 Volatility-Scaled Exit v1

## Goal
- Replace fixed `%` risk exits with a learned volatility-scaled exit contract for the `v4` primary lane.
- Keep the current architecture:
  - `backtest = fixed sanity gate`
  - `paper = learned runtime final gate`
  - runtime adaptation happens in the deployed strategy contract, not by daily online retuning

## Literature Direction
- `Stop-loss rules and momentum payoffs in cryptocurrencies` (JBEF 2023)
  - supports explicit stop-loss based downside control improving crypto momentum implementations
- `Cryptocurrency market risk-managed momentum strategies` (FRL 2025)
  - supports volatility-managed risk taking in crypto rather than static risk widths
- `Cryptocurrency momentum has (not) its moments` (FMPM 2025)
  - supports horizon sensitivity rather than a single fixed holding horizon

## Implementation Contract
- `exit.mode` still stays fixed by strategy configuration
- when `exit.mode=hold`
  - runtime may use learned `recommended_hold_bars`
- when `exit.mode=risk`
  - runtime may use learned:
    - volatility estimator choice
    - `tp/sl/trailing` multipliers

## Runtime Formula
- `sigma_bar` from the selected feature:
  - `rv_12`
  - `rv_36`
  - `atr_pct_14 = atr_14 / close`
- `sigma_h = sigma_bar * sqrt(horizon_bars)`
- effective thresholds:
  - `tp_eff = tp_vol_multiplier * sigma_h`
  - `sl_eff = sl_vol_multiplier * sigma_h`
  - `trailing_eff = trailing_vol_multiplier * sigma_h`
- if volatility feature is missing or invalid:
  - fall back to fixed `%` thresholds

## Trainer Path
- `runtime_recommendations.json.exit` now includes:
  - `recommended_hold_bars`
  - `recommended_risk_scaling_mode`
  - `recommended_risk_vol_feature`
  - `recommended_tp_vol_multiplier`
  - `recommended_sl_vol_multiplier`
  - `recommended_trailing_vol_multiplier`
- recommendations are chosen by execution-aware backtest tournament, not static config heuristics

## Status
- implemented
- runtime uses learned risk exit recommendations when `exit.mode=risk`
- trainer writes volatility-scaled exit recommendations to `runtime_recommendations.json`

## Remaining Work
- combine this with later overlay calibration if enough paper history accumulates
- optional future extension:
  - compare additional volatility estimators beyond `rv_12/rv_36/atr_pct_14`
