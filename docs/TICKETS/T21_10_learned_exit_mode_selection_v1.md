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
- Compare them with the same balanced-pareto execution comparator already used elsewhere.
- Persist:
  - `recommended_exit_mode`
  - `recommended_exit_mode_source`
  - `recommended_exit_mode_reason_code`
  - `exit_mode_compare`
- If evidence is not comparable or `risk` is not decisively better, keep `hold`.

## Runtime Contract
- `paper` and `live` must read the same learned exit mode.
- `recommended_hold_bars` remains active even when the resolved mode is `risk`, because timeout horizon still matters.
- `recommended_risk_*` values become effective only when the resolved mode is `risk`.

## Acceptance
- `runtime_recommendations.json.exit` contains the learned exit mode and compare document.
- `ModelAlphaSettings.exit` can opt out with `use_learned_exit_mode=false`.
- `paper` and `live` share the same resolved exit mode for a given model run.
