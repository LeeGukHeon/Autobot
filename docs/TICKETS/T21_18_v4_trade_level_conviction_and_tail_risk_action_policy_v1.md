# T21.18 V4 Trade-Level Conviction And Tail-Risk Action Policy v1

- Date: 2026-03-11
- Status: landed locally
- Scope:
  - `autobot/models/train_v4_crypto_cs.py`
  - `autobot/models/trade_action_policy.py`
  - `autobot/strategy/model_alpha_v1.py`
  - `autobot/backtest/engine.py`
  - `autobot/paper/engine.py`
  - `autobot/live/model_alpha_runtime.py`

## Goal
- Move `v4` runtime from one global `hold | risk` winner toward a trade-level action policy.
- Keep the method non-heuristic:
  - no direct `score -> TP/SL multiplier` mapping
  - no manual `hold bonus` / `risk penalty` knob
- Make entry sizing depend on learned risk-adjusted edge rather than raw probability alone.

## Literature Basis
- Calibrated signal should feed a decision layer rather than act as a direct control knob:
  - `Trading the FX volatility risk premium with machine learning and alternative data`
  - https://www.sciencedirect.com/science/article/pii/S2405918822000083
- Tail risk should be estimated explicitly, not approximated only with variance:
  - `Learning extreme expected shortfall and conditional tail moments with neural networks. Application to cryptocurrency data`
  - https://www.sciencedirect.com/science/article/abs/pii/S0893608024008323
- Downside-risk scaling is more aligned than plain volatility scaling:
  - `Managing downside risk of low-risk anomaly portfolios`
  - https://www.sciencedirect.com/science/article/pii/S1544612321003883
- Lower-partial-moment regularization is a better fit than return-only tuning for crypto allocation:
  - `Performance-based regularization for downside-risk cryptocurrency portfolios: Evidence from mean-lower partial moment strategies`
  - https://www.sciencedirect.com/science/article/pii/S0927538X26000302
- Citation-heavy anchor papers for risk-managed action selection:
  - `Volatility Managed Portfolios`
  - https://www.nber.org/papers/w22208
  - `Momentum Crashes`
  - https://www.nber.org/papers/w20439

## Why T21.10 Was Not Enough
- `T21.10` learned one run-level `recommended_exit_mode`.
- That is useful as a global fallback, but it is still too coarse for:
  - high-conviction / low-tail-risk entries
  - low-conviction / high-tail-risk entries
- The remaining gap was:
  - entry sizing still depended mainly on raw conviction ramping
  - exit logic inside the strategy still behaved like one global configuration

## Method
### 1. Compact Action Set
- keep runtime action space compact:
  - best `hold` policy from the learned runtime grid
  - best `risk` policy from the learned runtime grid
- do not search a new combinatorial runtime action grid inside `model_alpha_v1`

### 2. OOS Trade Replay Panel
- use walk-forward OOS rows only
- for each OOS row:
  - apply selection calibration to the model score
  - replay the best `hold` policy on the future bar-close path
  - replay the best `risk` policy on the same future bar-close path
- each replay produces:
  - realized net return
  - realized downside LPM
  - transformed downside-aware objective

### 3. Nonparametric Conditional Action Policy
- build a compact conditional table on:
  - calibrated selection score
  - chosen downside proxy feature from the selected risk policy
- aggregate OOS replay results per bin
- choose `hold` or `risk` by:
  - Pareto-style return/downside dominance first
  - downside-aware objective tie-break second

### 4. Risk-Adjusted Entry Weight
- for each comparable bin:
  - compute expected edge
  - compute expected downside deviation
  - compute a risk-budget score from their ratio
- convert that score into the existing sizing envelope using empirical rank, not a hand-set formula

## Runtime Contract
- trainer writes `runtime_recommendations.json.trade_action`
- runtime keeps:
  - global learned exit recommendation from `T21.10` as fallback
  - trade-level action policy as the preferred local layer
- `model_alpha_v1` now:
  - resolves `hold | risk` per candidate row
  - resolves per-trade notional multiplier
  - writes a per-trade `model_exit_plan`
- position bookkeeping now uses the stored plan on fill, so exits are frozen at entry time instead of being recomputed from one mutable global setting

## Implementation Notes
- the first implementation intentionally replays bar-close paths, because that matches the current paper/backtest strategy contract
- it does not claim intrabar `high/low` path realism yet
- if OOS support is missing or too sparse, the artifact stays explicit:
  - `status = skipped`
  - runtime falls back to the prior learned/global logic

## Acceptance
- `runtime_recommendations.json` contains:
  - `trade_action.status`
  - `trade_action.risk_feature_name`
  - compact per-bin action rows
- `decision_surface.json` exposes trade-action policy status under the runtime recommendation contract
- `model_alpha_v1` entry intents carry:
  - `trade_action`
  - trade-level `model_exit_plan`
  - trade-level `notional_multiplier`
- strategy exits now follow the stored entry plan in:
  - backtest
  - paper
  - live runtime bookkeeping

## Regression Coverage
- `tests/test_trade_action_policy.py`
  - OOS replay can produce different `hold` vs `risk` recommendations by conditional bin
- `tests/test_backtest_model_alpha_integration.py`
  - trade-level action policy changes both exit plan and sizing
- `tests/test_train_v4_crypto_cs.py`
  - runtime recommendation artifact and decision surface expose trade-action policy status
- `tests/test_live_model_alpha_runtime.py`
  - live submit metadata still preserves the per-trade model exit plan contract

## 2026-03-12 Conditional Downside Model Update
- The first `v1` implementation still relied too much on a raw single risk feature for the
  `risk` axis, which caused bin collapse and a strong `hold` bias.
- The method was tightened to better match the literature basis:
  - fit a compact contextual action model directly on walk-forward OOS replay rows
  - use calibrated edge plus multi-feature conditional downside inputs:
    - `selection_score`
    - `rv_12`
    - `rv_36`
    - `atr_pct_14`
  - predict action-level expected return, downside LPM, and downside-aware objective
  - keep the final runtime action non-heuristic:
    - prefer OOS Pareto / LPM winner from the matched conditional state bin
    - fall back to contextual predicted metrics only when a matched comparable bin is absent
- This keeps the runtime contract compact while moving the risk state from:
  - raw volatility proxy
  to:
  - learned conditional downside state from OOS replay evidence

## 2026-03-12 Continuous Action-Value Update
- The next refinement removed bin winner selection from the runtime decision path.
- Runtime action choice now uses:
  - direct conditional action-value estimation
  - direct conditional downside-tail proxies
- The compact contextual model now predicts, per action:
  - expected return
  - expected downside LPM
  - expected ES proxy
  - expected CTM proxy
  - expected downside-aware objective
- Runtime now chooses `hold` vs `risk` by:
  - continuous `argmax` on predicted action value
  - with ES-aware tie-break preserved through the objective contract
- Bins remain in the artifact only for:
  - diagnostics
  - auditability
  - stability monitoring
  and are no longer the primary runtime action selector.
