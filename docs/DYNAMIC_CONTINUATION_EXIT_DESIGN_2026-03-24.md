# Dynamic Continuation Exit Design 2026-03-24

Status: proposed design  
Operational authority: design only  
Scope: early profit capture, continuation-value exit logic, integration with current `path_risk` / live risk manager

## Goal

Replace the current weak heuristic:

- tighten TP/SL from path-risk summaries
- optionally trigger `PATH_RISK_CONTINUATION`

with a more explicit decision:

- `exit_now_value_net`
- versus `continue_value_net`

so the system can exit early when:

- current trade is already profitable
- remaining holding time is short
- TP reachability from here is low
- expected continuation value after costs is worse than taking profit now

This is not “randomly take profit early”. It is a state-dependent optimal stopping approximation.

## Why The Current Logic Is Not Enough

Current integration points:

- [autobot/common/path_risk_guidance.py](/d:/MyApps/Autobot/autobot/common/path_risk_guidance.py)
- [autobot/models/exit_path_risk.py](/d:/MyApps/Autobot/autobot/models/exit_path_risk.py)
- [autobot/risk/live_risk_manager.py](/d:/MyApps/Autobot/autobot/risk/live_risk_manager.py)
- [autobot/strategy/model_alpha_v1.py](/d:/MyApps/Autobot/autobot/strategy/model_alpha_v1.py)

Current strengths:

- OOS path summaries already exist
- live and strategy both consume `path_risk`
- `continuation_should_exit` already exists

Current limitations:

1. The continuation rule is still mostly quantile heuristic.
2. It does not explicitly compare `exit now` against `continue`.
3. It does not include enough execution realism on the exit side.
4. It does not model “remaining upside is too small relative to immediate realizable profit”.
5. It does not make `alpha decay` explicit.

So the current controller can tighten risk, but it is not yet a true continuation-value controller.

## Literature Direction

The relevant literature supports three ideas:

1. Trading rules should be chosen from path behavior, not just threshold folklore.
2. Continuation value changes when alpha decays and costs exist.
3. Fill probability / cleanup cost must be state-dependent.

### 1. Optimal trading rule / pathwise stopping

- Carr and López de Prado, *Determining Optimal Trading Rules Without Backtesting*  
  https://ssrn.com/abstract=2658641

Why it matters here:

- stop / take-profit should be chosen as an optimal stopping problem over the path
- not just “fixed TP/SL percentages”

### 2. Hidden costs of stop-loss / naive rule-based exits

- Ma, Morita, Detko, *Re-Examining the Hidden Costs of the Stop-Loss*  
  https://ssrn.com/abstract=1123362

Why it matters here:

- naive static exits distort the path and can destroy expected edge
- early exits must be based on state and expected continuation, not just fear of drawdown

### 3. Alpha decay and multi-period optimal policy

- Ma and Smith, *On the Effect of Alpha Decay and Transaction Costs on the Multi-period Optimal Trading Strategy*  
  https://arxiv.org/abs/2502.04284

Why it matters here:

- once alpha decays and costs exist, myopic holding can be suboptimal
- optimal policy becomes a comparison of continuation value vs action now

### 4. Fill probability / cleanup cost must be state-dependent

- Fabre and Ragel, *Tackling the Problem of State Dependent Execution Probability*  
  https://ssrn.com/abstract=4509063

- Arroyo et al., *Deep Attentive Survival Analysis in Limit Order Books*  
  https://arxiv.org/abs/2306.05479

Why it matters here:

- exit-now value is not “mark-to-market only”
- it depends on whether the exit can actually fill and at what cleanup cost

### 5. State-conditioned execution policies

- Zhang et al., *Towards Generalizable Reinforcement Learning for Trade Execution*  
  https://arxiv.org/abs/2307.11685

Why it matters here:

- execution decisions should be state-conditioned, not fixed rule ladders
- overfitting risk is real, so we should prefer compact state summaries and careful offline contracts

## Target Decision Rule

At each evaluation point for an open position:

### Compute `exit_now_value_net`

This should estimate the realizable value if we close now.

Suggested components:

- `current_return_ratio`
- minus `expected_exit_fee_rate`
- minus `expected_exit_slippage`
- minus `expected_cleanup_cost_from_execution_policy`

So:

`exit_now_value_net ~= current_mark_to_market_return - exit_costs_now`

### Compute `continue_value_net`

This should estimate the value of keeping the position to the remaining horizon.

Suggested components:

- expected remaining terminal return from current state
- remaining upside reachability
- remaining downside / drawdown risk
- alpha decay penalty
- expected eventual exit costs

So:

`continue_value_net ~= E[terminal_return_from_now] - expected_future_exit_costs - alpha_decay_penalty`

### Exit if:

`exit_now_value_net > continue_value_net + margin`

and one of:

- remaining TP hit probability is low
- remaining upside left is small
- continuation downside asymmetry is high
- continuation quantiles have turned unfavorable

## Concrete State Variables

These should drive the decision.

### Already available in our code

From current `path_risk` / live features:

- `remaining_bars`
- `selection_score`
- `risk_feature_value`
- `reachable_tp_q60`
- `bounded_sl_q80`
- `terminal_return_q50`
- `terminal_return_q75`
- `rv_12`
- `rv_36`
- `atr_pct_14`
- spread / depth / trade coverage micro signals

### Add next

Add to path-risk summaries:

- `tp_hit_prob`
  - probability that price reaches target before timeout, from current state/horizon
- `profit_preservation_prob`
  - probability final return remains above a minimum positive floor if held
- `drawdown_from_now_q50/q80/q90`
  - forward MAE from current state
- `continue_edge_q50`
  - median future net terminal value from now
- `continue_edge_q75`
  - upside scenario if held
- `exit_now_edge_proxy`
  - optional learned proxy, but can be runtime-computed initially

## Data Construction Plan

### Stage 1: Extend `exit_path_risk.py`

Current file:

- [autobot/models/exit_path_risk.py](/d:/MyApps/Autobot/autobot/models/exit_path_risk.py)

Current outputs:

- `terminal_return_q50/q75/q90`
- `mfe_q50/q75/q90`
- `mae_abs_q50/q75/q90`
- `reachable_tp_q60`
- `bounded_sl_q80`

Extend to also compute:

- `profit_positive_q`
  - proportion terminal return > 0
- `profit_above_floor_q`
  - proportion terminal return > `profit_floor`
- `tp_hit_prob_at_current_tp`
  - path-wise hit ratio at target level
- `drawdown_from_now_q80`
  - same path sample basis, but explicitly named for runtime use

Important:

- keep this pathwise and OOS only
- do not fit a separate heavy model yet
- avoid introducing a second contract before validating the simpler extension

### Stage 2: Add optional bucket-conditioned continuation summaries

Current selection already supports:

- overall by horizon
- by bucket

That is good. Keep it.

We should extend bucket summaries rather than introducing a separate artifact.

## Runtime Integration Plan

### 1. Keep `path_risk_guidance.py` as the single decision helper

Current file:

- [autobot/common/path_risk_guidance.py](/d:/MyApps/Autobot/autobot/common/path_risk_guidance.py)

This is the right place because both:

- [autobot/risk/live_risk_manager.py](/d:/MyApps/Autobot/autobot/risk/live_risk_manager.py)
- [autobot/strategy/model_alpha_v1.py](/d:/MyApps/Autobot/autobot/strategy/model_alpha_v1.py)

already consume it.

Do not fork separate logic in strategy vs live.

### 2. Replace heuristic continuation capture with explicit value comparison

Current logic in `path_risk_guidance.py`:

- compares current return with quantile-derived anchors
- uses `upside_left_ratio` and `continuation_margin_ratio`

New logic:

Compute:

- `exit_now_value_net`
- `continue_value_net`
- `continuation_gap = continue_value_net - exit_now_value_net`

Then:

- if `continuation_gap < -margin`, set `continuation_should_exit = True`

Suggested initial implementation:

`exit_now_value_net = current_return_ratio - immediate_exit_cost_ratio`

`continue_value_net = terminal_return_q50 - deferred_exit_cost_ratio - alpha_decay_penalty_ratio`

Where:

- `immediate_exit_cost_ratio` comes from current execution recommendation / spread / fee / price mode
- `deferred_exit_cost_ratio` can initially be the same cost basis plus a risk premium
- `alpha_decay_penalty_ratio` should grow as remaining bars shrink and as state weakens

### 3. Trigger priority

Current live order in `live_risk_manager.py`:

- TP
- SL
- PATH_RISK_CONTINUATION
- TRAILING
- TIMEOUT

Recommended order:

- TP
- SL
- CONTINUATION_CAPTURE
- TRAILING
- TIMEOUT

This is acceptable as long as continuation uses a real value comparison.

## Execution-Cost Integration

This is where most “looks good on paper but exits badly” bugs happen.

Use current execution stack:

- [autobot/models/live_execution_policy.py](/d:/MyApps/Autobot/autobot/models/live_execution_policy.py)
- [autobot/live/model_alpha_runtime_execute.py](/d:/MyApps/Autobot/autobot/live/model_alpha_runtime_execute.py)

Design rule:

- `exit_now_value_net` must not assume perfect immediate fill
- it must include:
  - spread
  - fee
  - cleanup cost
  - urgency mode

For the first safe version:

- use conservative deterministic cost proxy from current exit aggressiveness mode
- do not add another learned model yet

For the second version:

- integrate state-dependent fill/cleanup cost from execution contract v2

## Migration Plan

To avoid bugs, do this in slices.

### Slice A: extend artifact only

Files:

- [autobot/models/exit_path_risk.py](/d:/MyApps/Autobot/autobot/models/exit_path_risk.py)
- tests

Add new fields only, keep old ones.

### Slice B: extend `path_risk_guidance.py`

Files:

- [autobot/common/path_risk_guidance.py](/d:/MyApps/Autobot/autobot/common/path_risk_guidance.py)
- tests

Add:

- `exit_now_value_net`
- `continue_value_net`
- `continuation_gap`
- `profit_preservation_prob`

Do not remove old fields yet.

### Slice C: swap trigger to use value comparison

Files:

- [autobot/risk/live_risk_manager.py](/d:/MyApps/Autobot/autobot/risk/live_risk_manager.py)
- [autobot/strategy/model_alpha_v1.py](/d:/MyApps/Autobot/autobot/strategy/model_alpha_v1.py)

Change trigger reason from “quantile threshold heuristic” to “continuation_gap < -margin”.

### Slice D: integrate execution cost realism

Files:

- [autobot/models/live_execution_policy.py](/d:/MyApps/Autobot/autobot/models/live_execution_policy.py)
- [autobot/live/model_alpha_runtime_execute.py](/d:/MyApps/Autobot/autobot/live/model_alpha_runtime_execute.py)
- [autobot/common/path_risk_guidance.py](/d:/MyApps/Autobot/autobot/common/path_risk_guidance.py)

This should be separate because it couples with the exit execution side.

## Safety Constraints

To reduce migration bugs:

1. Keep existing fields and add new ones.
2. Make new logic opt-in behind a versioned payload field first.
3. Keep strategy/live shared through one helper only.
4. Keep `tighten-only` TP/SL behavior until explicit value-comparison trigger is proven.
5. Test with:
   - profitable but low-upside continuation
   - profitable and still-high-upside continuation
   - negative current return
   - no path-risk artifact
   - missing micro / stale micro

## Test Plan

### Unit tests

- `exit_now_value_net` > `continue_value_net` => early exit
- `continue_value_net` > `exit_now_value_net` => hold
- no artifact => no continuation capture
- low `upside_left_ratio` alone should not trigger if `continue_value_net` still dominates

### Integration tests

- [autobot/risk/live_risk_manager.py](/d:/MyApps/Autobot/autobot/risk/live_risk_manager.py)
  - profitable position exits early before timeout if continuation weak
- [autobot/strategy/model_alpha_v1.py](/d:/MyApps/Autobot/autobot/strategy/model_alpha_v1.py)
  - backtest strategy and live risk manager agree on trigger direction

### Acceptance regression

Re-run:

- `runtime_recommendations`
- candidate backtest
- runtime parity

and compare:

- early-profit capture rate
- average giveback after local profit peak
- payout ratio
- drawdown

## What Not To Do

Do not:

- add a brand-new heavy RL controller first
- hard-cap TP/SL heuristically again
- make a separate live-only continuation controller
- use immediate PnL alone without execution cost

## Recommended Next Implementation Order

1. extend `exit_path_risk.py` with continuation-relevant summary fields
2. upgrade `path_risk_guidance.py` to explicit `exit_now_value_net` vs `continue_value_net`
3. wire both strategy and live risk manager to the same new trigger
4. only then integrate execution-cost realism from execution contract v2
