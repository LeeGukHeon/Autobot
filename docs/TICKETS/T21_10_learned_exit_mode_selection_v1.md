# T21.10 Learned Exit-Mode Selection v1

- Date: 2026-03-10
- Scope: `v4` runtime recommendation artifact, shared paper/live strategy runtime

## Goal
- Replace the current fixed `exit.mode=hold` default with a learned `recommended_exit_mode = hold | risk`.
- Keep the choice non-heuristic by using the same execution-aware tournament already used for runtime recommendation selection.

## Literature Basis
- Crypto stop-loss / downside control:
  - Stop-loss rules and momentum payoffs in cryptocurrencies
  - https://www.sciencedirect.com/science/article/pii/S2214635023000473
- Crypto volatility-managed momentum:
  - Cryptocurrency momentum has (not) its moments
  - https://link.springer.com/article/10.1007/s11408-025-00474-9
- Crypto regime-conditioned momentum:
  - State transitions and momentum effect in cryptocurrency market
  - https://www.sciencedirect.com/science/article/pii/S1544612325016101
- General volatility-managed / crash-aware execution logic:
  - Volatility Managed Portfolios
  - https://www.nber.org/papers/w22208
  - Momentum Crashes
  - https://www.nber.org/papers/w20439

## Method
- Treat `hold` and `risk` as two competing exit policies.
- Evaluate both with the same execution-aware backtest engine.
- Compare them with a versioned execution comparator that:
  - keeps Pareto domination explicit
  - treats realized return plus downside control as the primary utility
  - demotes fill/slippage to implementation tie-breaks
- Persist:
  - `recommended_exit_mode`
  - `recommended_exit_mode_source`
  - `recommended_exit_mode_reason_code`
  - `exit_mode_compare`
- Search `risk` on its own `hold_bars` horizon rather than forcing it to inherit the best `hold` candidate horizon.
- If evidence is not comparable or no clear risk edge exists, keep `hold` but record the insufficiency explicitly.

## Runtime Contract
- `paper` and `live` must read the same learned exit mode.
- `recommended_hold_bars` remains active even when the resolved mode is `risk`, because timeout horizon still matters.
- `recommended_risk_*` values become effective only when the resolved mode is `risk`.

## Acceptance
- `runtime_recommendations.json.exit` contains the learned exit mode and compare document.
- `ModelAlphaSettings.exit` can opt out with `use_learned_exit_mode=false`.
- `paper` and `live` share the same resolved exit mode for a given model run.

## 2026-03-11 Bias-Hardening Update
- `risk` no longer reuses the `hold` winner's `hold_bars`; it now searches its own timeout horizon jointly with volatility feature and TP/SL/trailing multipliers.
- `recommended_hold_bars` now follows the resolved winner, so a selected `risk` policy carries its own timeout horizon into paper/live.
- `recommended_exit_mode_reason_code` now distinguishes:
  - explicit hold edge
  - indeterminate compare
  - insufficient execution evidence

## 2026-03-11 Validation Upgrade
- runtime exit comparison is no longer decided from one summary-only utility.
- each execution backtest run now produces rolling execution windows from:
  - `equity.csv`
  - `fills.jsonl`
- the learned exit selection now uses:
  - rolling-window downside-risk evidence
  - Sortino / lower-partial-moment fold scores
  - exact paired sign-flip validation over aligned folds
- if that validation is not comparable, the contract emits explicit insufficient-evidence status instead of silently treating the compare as a normal hold win.

## 2026-03-11 Trade-Level Extension
- `T21.10` now acts as the global fallback layer.
- trade-level entry-time `hold | risk` resolution moved to:
  - `T21.18` V4 Trade-Level Conviction And Tail-Risk Action Policy v1
- the new layer does not replace the learned global compare artifact.
- it uses that artifact's best `hold` and best `risk` templates as the compact runtime action set, then resolves them conditionally per trade from walk-forward OOS replay evidence.
