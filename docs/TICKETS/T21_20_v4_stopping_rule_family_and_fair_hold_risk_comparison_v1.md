# T21.20 V4 Stopping-Rule Family And Fair Hold-Risk Comparison v1

- Date: 2026-03-12
- Status: partial foundation landed locally
- Scope:
  - `autobot/models/trade_action_policy.py`
  - `autobot/models/train_v4_crypto_cs.py`
  - `autobot/strategy/model_alpha_v1.py`
  - `autobot/live/model_alpha_runtime.py`
  - `autobot/paper/engine.py`
  - `autobot/backtest/engine.py`
  - `autobot/dashboard_server.py`

## Goal
- Replace the remaining informal `hold` vs `risk` comparison with an explicit stopping-rule family comparison.
- Keep `hold` as a legitimate action, but no longer as a special simpler default.
- Require both `hold` and `risk` to be evaluated under the same:
  - OOS replay source
  - risk-aware welfare objective
  - support and comparability rules

## Why This Ticket Exists
- `T21.19` removed most of the old hold bias caused by bin winners, hidden fallbacks, and policy-layer sizing clamps.
- But one methodology gap still remains:
  - `hold` is still treated as one compact template while `risk` is treated as a richer managed-exit family.
- That asymmetry can create apparent hold superiority even when the real issue is:
  - search-space imbalance
  - evidence imbalance
  - cost-model asymmetry
- This ticket makes the comparison fair by treating both as stopping-rule families rather than:
  - one baseline action
  - one managed action

## Literature Basis
- `Hold until horizon` and `exit on a managed rule` are both stopping rules and should be compared as such:
  - Francis Longstaff, `Valuing Thinly-Traded Assets`
  - https://www.nber.org/papers/w20589
- Sequential treatment / policy families should be compared by welfare, not by one heuristic default:
  - Sukjin Han, `Optimal Dynamic Treatment Regimes and Partial Welfare Ordering`
  - https://arxiv.org/abs/1912.10014
- Risk-aware policy comparison should permit CVaR-style objectives rather than mean-only comparison:
  - Audrey Huang et al., `Off-Policy Risk Assessment in Contextual Bandits`
  - https://arxiv.org/abs/2104.08977
- Selling/exit decisions are a distinct decision problem and should not be treated as a trivial extension of entry:
  - `Selling Fast and Buying Slow`
  - https://www.nber.org/papers/w29076

## Non-Negotiable Methodology Rules
- Do not let `hold` win because it has fewer knobs.
- Do not let `risk` lose because it pays a richer search-complexity penalty than `hold`.
- Do not silently collapse to one preselected hold horizon when multiple admissible hold stopping rules exist.
- Do not silently fall back from a family comparison to one simpler baseline action.
- If support is insufficient, emit:
  - `INSUFFICIENT_EVIDENCE`
  - `NOT_COMPARABLE`
- Keep artifacts compact:
  - selected stopping-rule families
  - comparable support diagnostics
  - OOS family summaries
  - no path explosion archives

## Method
### 1. Define Admissible Stopping-Rule Families
- `hold_family` must be an explicit set of admissible horizon rules, for example:
  - `hold_3`
  - `hold_6`
  - `hold_9`
  - `hold_12`
- `risk_family` must be an explicit set of compact managed-exit rules, for example:
  - fixed `tp/sl`
  - volatility-scaled `tp/sl`
  - trailing-enabled variants
- The family sizes do not need to be identical.
- But both families must be intentionally bounded and explicitly persisted.

### 2. One Shared OOS Replay Panel
- Use the same walk-forward OOS replay source as `T21.18` and `T21.19`.
- For each trade candidate row:
  - replay every admissible hold stopping rule
  - replay every admissible risk stopping rule
- Persist only compact summaries for each stopping rule:
  - return
  - downside LPM
  - downside deviation
  - expected shortfall proxy / direct estimate
  - CTM proxy / direct estimate
  - shared downside-aware utility

### 3. Family-Level Fair Comparison
- First compare rules within each family:
  - best comparable `hold` stopping rule
  - best comparable `risk` stopping rule
- Then compare the best hold and best risk rules under one shared objective.
- The comparison must require:
  - aligned state support
  - aligned OOS periods
  - explicit sample coverage
- If either family lacks support, do not auto-award the other family by default.

### 4. Runtime Contract
- Runtime should consume:
  - best comparable hold stopping rule
  - best comparable risk stopping rule
  - family comparison result
  - decision basis
- Runtime output must expose:
  - `chosen_family`
  - `chosen_rule_id`
  - `hold_family_status`
  - `risk_family_status`
  - `family_compare_status`
  - `chosen_rule_expected_edge`
  - `chosen_rule_expected_es`
  - `chosen_rule_expected_ctm`
  - `chosen_rule_action_value`
- If family comparison support is insufficient:
  - prefer `INSUFFICIENT_EVIDENCE`
  - not silent `hold`

### 5. Diagnostics And Audit
- `decision_surface.json` must record:
  - admissible hold-family contract
  - admissible risk-family contract
  - family-comparison objective
  - support rules
- Dashboard/runtime audit views should explain:
  - whether hold won because of:
    - higher edge
    - lower ES
    - lower CTM
    - better utility
  - or whether the decision was:
    - `INSUFFICIENT_EVIDENCE`
    - `NOT_COMPARABLE`

## Storage Contract
- Allowed:
  - compact family lists
  - compact OOS family summaries
  - per-family comparable support diagnostics
  - selected best-rule metadata
- Not allowed:
  - full raw trajectory duplication for each stopping rule
  - giant candidate-rule archives
  - per-trade path dumps

## Roadmap
### Slice 1. Hold-Family Contract
- version an explicit `hold_family` instead of one implicit hold template
- Status:
  - landed locally

### Slice 2. Risk-Family Compact Symmetry
- bound `risk_family` to a compact comparable rule set
- Status:
  - landed locally

### Slice 3. Family-Level OOS Compare
- compare best hold rule and best risk rule under one shared risk-aware objective
- Status:
  - landed locally as an execution-family compare contract

### Slice 4. Runtime Abstain Contract
- if family support is insufficient, emit `INSUFFICIENT_EVIDENCE` rather than defaulting to hold
- Status:
  - pending

### Slice 5. Observability
- surface rule-family diagnostics in runtime, journal, and dashboard
- Status:
  - partial foundation landed locally

## Acceptance
- `hold` is represented as an explicit stopping-rule family, not one special simple baseline.
- `risk` is represented as an explicit bounded stopping-rule family, not an open-ended richer action set.
- runtime chooses between best comparable family representatives under one shared objective.
- insufficient family support remains explicit and auditable.
- a reviewer can explain why hold won or risk won without appealing to hidden defaults.
