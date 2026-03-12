# T21.19 V4 Direct Conditional Tail-Risk And Continuous Action Value v1

- Date: 2026-03-12
- Status: planned / partial foundation landed
- Scope:
  - `autobot/models/trade_action_policy.py`
  - `autobot/models/train_v4_crypto_cs.py`
  - `autobot/strategy/model_alpha_v1.py`
  - `autobot/live/model_alpha_runtime.py`
  - `autobot/paper/engine.py`
  - `autobot/backtest/engine.py`
  - `autobot/dashboard_server.py`

## Goal
- Move `v4` trade-level action selection from:
  - compact `hold | risk` bin winner selection
  to:
  - direct conditional tail-risk estimation
  - direct continuous action-value comparison
  - state-conditional utility maximization
- Keep the implementation non-heuristic and compact enough for the current Oracle A1 server.

## Why This Ticket Exists
- `T21.18` moved the runtime away from one global exit mode and introduced OOS replay based trade-level action selection.
- That was a valid intermediate step, but it still left three methodology gaps:
  - `risk state` was still too dependent on coarse or collapsed volatility bins
  - `hold` vs `risk` was still explained through compact bins rather than direct state-conditional value estimates
  - tail risk was represented with `LPM/downside deviation` only, rather than a more direct conditional tail-risk estimate

## Literature Basis
- Calibrated signal should feed a contextual decision layer, not act as a direct threshold or multiplier knob:
  - `Trading the FX volatility risk premium with machine learning and alternative data`
  - https://www.sciencedirect.com/science/article/pii/S2405918822000083
- Tail risk should be modeled conditionally and directly, rather than approximated only by unconditional variance:
  - `Learning extreme expected shortfall and conditional tail moments with neural networks. Application to cryptocurrency data`
  - https://www.sciencedirect.com/science/article/abs/pii/S0893608024008323
- Downside-risk control is more appropriate than simple volatility scaling for real portfolio sizing:
  - `Managing downside risk of low-risk anomaly portfolios`
  - https://www.sciencedirect.com/science/article/pii/S1544612321003883
- Lower-partial-moment based regularization is a better fit than return-only tuning for crypto:
  - `Performance-based regularization for downside-risk cryptocurrency portfolios: Evidence from mean-lower partial moment strategies`
  - https://www.sciencedirect.com/science/article/pii/S0927538X26000302
- Risk-managed action comparison should remain utility-based and comparable under drawdown-sensitive metrics:
  - `Volatility Managed Portfolios`
  - https://www.nber.org/papers/w22208
  - `Momentum Crashes`
  - https://www.nber.org/papers/w20439

## Non-Negotiable Methodology Rules
- Do not map `model score -> TP/SL` directly.
- Do not select `hold` because it is simpler or because it has fewer knobs.
- Do not keep `risk` alive via manual bonus weights.
- Do not keep the full replay tensor or duplicate bar path archives.
- Persist enough compact structure to reproduce:
  - fitted conditional tail-risk model
  - fitted continuous action-value model
  - chosen state features
  - validation diagnostics

## Method
### 1. OOS Replay Panel Stays The Label Source
- keep the same walk-forward OOS replay panel from `T21.18`
- for each trade candidate row, replay:
  - `hold` policy outcome
  - `risk` policy outcome
- produce action-level target rows with:
  - realized net return
  - downside LPM
  - downside deviation
  - ES proxy
  - CTM proxy
  - downside-aware utility

### 2. Direct Conditional Tail-Risk Estimation
- fit compact conditional tail-risk models from OOS replay rows
- state inputs should include:
  - calibrated selection score
  - `rv_12`
  - `rv_36`
  - `atr_pct_14`
  - later slices may add compact micro-quality or spread state if stable and already available
- target outputs per action:
  - conditional expected return
  - conditional downside LPM
  - conditional ES proxy
  - conditional CTM proxy
  - conditional downside-aware objective

### 3. Continuous Action Value Selection
- runtime should not need a bin winner to choose the action
- instead:
  - estimate `action_value_hold(x)`
  - estimate `action_value_risk(x)`
  - choose `argmax(action_value_hold, action_value_risk)`
- bins remain only as:
  - audit tables
  - diagnostics
  - stability monitoring

### 4. Continuous Risk Budgeting
- position sizing should be based on:
  - expected edge
  - conditional downside or ES estimate
- this remains compact:
  - keep one scalar `recommended_notional_multiplier`
  - derive it from continuous risk-adjusted score
  - do not keep a giant policy surface artifact

### 5. Runtime Contract
- `runtime_recommendations.json.trade_action` must expose:
  - `conditional_action_model.status`
  - `conditional_action_model.model`
  - state feature list
  - fitted compact coefficients / summaries
  - direct runtime decision source
- runtime trade metadata should expose:
  - expected edge
  - expected downside
  - expected ES
  - expected CTM
  - expected action value
  - chosen action source

## Storage Contract
- This ticket is explicitly allowed to increase storage modestly because the server has enough headroom.
- Still, the implementation must remain compact:
  - no new raw archive lanes
  - no duplicated bar path stores
  - no per-trade full future path dumps
- Storage budget target:
  - low-MB increase per run, not GB-per-run growth
- Allowed artifacts:
  - compact coefficient arrays
  - compact fit diagnostics
  - small validation tables
  - per-trade journal summaries already in place

## Roadmap
### Slice 1. Conditional Downside State
- fit compact conditional downside models from OOS replay rows
- replace raw risk-bin dependence in runtime decision path
- Status:
  - partial foundation landed

### Slice 2. Continuous Action Value
- remove bin winner from the runtime decision path
- use direct predicted action value for `hold` vs `risk`
- Status:
  - partial foundation landed

### Slice 3. Direct ES / CTM Contract
- expose explicit `expected_es` and `expected_ctm` in trade-action runtime decisions
- add trainer diagnostics for:
  - fit quality
  - sample coverage
  - comparable support
- Status:
  - pending hardening

### Slice 4. Certification Consistency
- require certification-lane diagnostics to show:
  - action-value comparability
  - tail-risk support sufficiency
  - state-space coverage
- Status:
  - pending

### Slice 5. Live / Dashboard Observability
- show, per trade:
  - chosen action source
  - expected edge
  - expected downside
  - expected ES
  - expected CTM
  - expected action value
- Status:
  - pending

## Acceptance
- trainer writes a compact conditional-action artifact with direct tail-risk targets
- runtime chooses `hold | risk` from continuous action value, not bin winner
- `decision_surface.json` records:
  - state feature contract
  - tail-risk model contract
  - action-value contract
- runtime / dashboard can explain:
  - why `hold` won
  - why `risk` won
  - whether that came from edge, downside, ES, or objective

## Current Assessment
- The current server has enough free disk to support this compact expansion.
- The real constraint is not storage; it is methodology correctness and OOS support.
- Therefore this ticket explicitly prefers:
  - higher methodological fidelity
  over:
  - prematurely minimizing artifact detail to save a few MB.
