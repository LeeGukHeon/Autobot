# Live Execution Parity Redesign

- Date: 2026-03-20
- Status: Draft
- Scope: `train_v4_crypto_cs`, acceptance/backtest execution checks, `paper alpha --preset live_v4`, candidate live runtime
- Goal: fix the current failure mode where the same model signal is profitable in paper but fails to enter in live because the live order is not filled in time

## 1. Problem Statement

The current system shares the same alpha model and mostly the same live feature provider
between paper and live, but it does **not** share the same execution contract.

Observed failure pattern:

- paper enters and exits profitably on the candidate model
- live candidate emits the same entry signal
- the live order is submitted as a passive maker limit order
- the order is not filled
- the order expires without replacement
- the missed entries and bad remaining exits trigger online execution risk-control halts
- later good signals are skipped by the live breaker

This means the primary gap is not "alpha quality" first. The primary gap is:

- optimistic paper fill semantics
- conservative live entry semantics
- online live breaker escalation after repeated misses/losses

## 2. What Is Shared Today

### 2.1 Shared strategy core

- paper `live_v4` and live runtime both use `ModelAlphaStrategyV1`
- both can use `LiveFeatureProviderV4`
- both use the same model registry family and concrete run id once resolved

Relevant code:

- `autobot/strategy/model_alpha_v1.py`
- `autobot/paper/engine.py`
- `autobot/live/model_alpha_runtime.py`
- `autobot/paper/live_features_v4.py`

### 2.2 Shared canonical exit plan writer

Canonical exit-state source is:

- `intents.meta_json.strategy.meta.model_exit_plan`

Relevant contract:

- `docs/EXIT_STATE_CONTRACT.md`

## 3. Where Paper And Live Diverge Today

### 3.1 Paper fill is optimistic

Paper uses `TouchFillModel`:

- a bid fills whenever `trade_price <= limit_price`
- an ask fills whenever `trade_price >= limit_price`
- immediate fills are marked as taker

This is simpler than actual exchange execution and ignores:

- queue position
- quote update latency
- partial fill risk
- cancellation risk
- non-fill opportunity cost

Relevant code:

- `autobot/paper/fill_model.py`
- `autobot/paper/sim_exchange.py`

### 3.2 Live entry goes through many more gates

Live candidate entry resolution currently includes:

- canary slot checks
- accounts lookup
- chance lookup
- instrument lookup
- exact admissibility snapshot
- trade gate
- micro order policy
- execution action selection
- online risk-control halt
- rollout gate / shadow gate
- executor submit

Relevant code:

- `autobot/live/model_alpha_runtime_execute.py`
- `autobot/live/model_alpha_runtime.py`

### 3.3 Candidate live uses canary-specific execution constraints

Candidate live is not just "live_v4 paper with real orders".

It also uses:

- canary notional cap
- canary timeout cap
- small-account position cap

Relevant code:

- `autobot/live/model_alpha_runtime_execute.py`
- `scripts/install_server_live_runtime_service.ps1`

### 3.4 Live risk manager rewrites exit behavior

Live exit behavior is managed by `LiveRiskManager`, which can apply:

- dynamic micro exit overlay
- tighter TP/SL/trailing/timeout
- exit-order replace control
- breaker escalation on stuck exits

Paper does not run the same protective-order state machine.

Relevant code:

- `autobot/risk/live_risk_manager.py`
- `autobot/common/dynamic_exit_overlay.py`

### 3.5 Acceptance/backtest is not the same contract as live-style runtime

Acceptance intentionally keeps signal breadth fixed for candidate-vs-champion comparability.
This is valid for research comparison, but it means:

- acceptance/backtest
- challenger paper runtime
- champion paper runtime
- candidate live runtime

are not all testing the same execution contract today.

Relevant code:

- `docs/CONFIG_SCHEMA.md`
- `scripts/candidate_acceptance.ps1`
- `autobot/models/train_v4_execution.py`
- `autobot/models/execution_acceptance.py`

## 4. Direct Evidence Collected On Server

### 4.1 Same-model paper run exists

Candidate challenger paper run:

- `paper-20260320-114120-ba5116bd`
- `paper_runtime_model_run_id = 20260320T000911Z-s42-ddd772bf`
- provider: `LIVE_V4`
- micro provider: `LIVE_WS`

This is the same candidate model run that candidate live was using.

### 4.2 Live missed entries are post-submit failures, not pre-submit rejections

For sampled live candidate cases, the entry path was already:

- admissible
- trade gate allowed
- static risk control allowed
- order submitted

but then:

- `fill_fraction = 0`
- `final_state = MISSED`
- `trade_journal.status = CANCELLED_ENTRY`
- close reason often `MAX_REPLACES_REACHED`

### 4.3 Direct same-timestamp mismatches

#### KRW-IP

- live candidate:
  - `submitted_ts_ms = 1773974700000`
  - requested price `1151.0`
  - `final_state = MISSED`
  - `fill_fraction = 0.0`
  - journal close reason `MAX_REPLACES_REACHED`
- challenger paper:
  - same timestamp
  - same market
  - same limit price `1151.0`
  - immediate fill

#### KRW-DOGE

- live candidate:
  - `submitted_ts_ms = 1773977700000`
  - requested price `139.0`
  - `final_state = MISSED`
  - `fill_fraction = 0.0`
- challenger paper:
  - same timestamp
  - same market
  - same limit price `139.0`
  - immediate fill

### 4.4 Live candidate currently escalates to breaker skips

Recent live candidate skipped bid intents are currently dominated by:

- `RISK_CONTROL_MARTINGALE_EVIDENCE`

This means the current system now has two stacked problems:

1. historical missed entries due to live non-fill
2. current new-entry suppression due to online risk-control halt

## 5. Correct Methodology For This Problem

The right methodology is **not** just covariate-shift reweighting.

The core problem is contextual execution under non-fill risk.

The best fit for this codebase is a combined approach:

1. `time-to-fill / fill probability` estimation
2. `expected shortfall / cleanup cost` estimation
3. `pessimistic action selection` from logged execution data

### 5.1 Literature that matches the problem

- fill probability / fill time modeling
  - Arroyo et al., 2023, `Deep Reinforcement Learning for Optimal Placement of Limit Orders`
  - Fabre & Ragel, 2023, interpretable ML for execution / fill prediction
  - Lokin & Yu, 2024, fill probability / execution state modeling
- off-policy evaluation / pessimistic policy learning
  - Wang et al., 2017
  - Su et al., 2020
  - Kuzborskij et al., 2021

### 5.2 What "non-heuristic" means here

We should stop choosing execution behavior via hand rules such as:

- "always passive maker"
- "after timeout, maybe replace"
- "if edge is high, maybe be more aggressive"

Instead we should define:

- state `x`
- action `a`
- utility `U(x, a)`

and learn/action-select from logged execution outcomes.

## 6. Target Architecture

## 6.1 Shared execution contract artifact

Introduce a new artifact family:

- `execution_contract.json`

with three learned components:

1. `fill_model`
  - estimates `P(fill by H | x, a)`
  - or equivalent survival curve over fill time
2. `slippage_model`
  - estimates `E(shortfall_bps | fill, x, a)`
3. `miss_cost_model`
  - estimates expected opportunity loss / cleanup cost if no fill by deadline

The artifact is trained from:

- `live.execution_attempts`
- `trade_journal`
- order lifecycle / cancel outcomes

## 6.2 Shared action space

The action space should be explicit and shared across live and paper:

- `LIMIT_GTC_PASSIVE_MAKER`
- `LIMIT_POST_ONLY`
- `LIMIT_GTC_JOIN`
- `LIMIT_IOC_JOIN`
- `LIMIT_FOK_JOIN`
- `BEST_IOC`
- `BEST_FOK`

The action space already partially exists in:

- `autobot/models/live_execution_policy.py`

## 6.3 Shared utility definition

For each state-action pair:

- `U(x, a) = P(fill <= H | x, a) * (edge_bps - expected_shortfall_bps)`
- `         - (1 - P(fill <= H | x, a)) * miss_cost_bps`

Then choose:

- `argmax_a lower_confidence_bound(U(x, a))`

not:

- a hand-written if/else rule

## 6.4 Paper must stop using touch-fill

Paper should no longer fill just because a trade touched the limit price.

Instead, paper should call the same execution contract used by live:

- same state features
- same action set
- same fill probability model
- same shortfall model
- same miss-cost model

This is the single biggest parity fix.

## 6.5 Candidate live paper lane must support canary parity mode

Current challenger paper is candidate-model pinned, but still not identical to candidate live execution.

We need an explicit candidate-live parity mode that can also mirror:

- canary notional cap
- canary timeout cap
- small-account slot constraints

This does not replace the existing research-friendly champion/challenger lane.
It adds a separate parity lane for diagnosing live execution gaps.

## 7. Required Refactor By Layer

### 7.1 Data / logging layer

Make sure every live execution attempt persists:

- state features used by action selection
- selected action
- candidate actions considered
- fill deadline
- fill / non-fill outcome
- cancel reason
- shortfall
- downstream trade outcome after miss

Current gaps:

- some historical missed attempts have `execution_policy = null`
- some historical rows have `micro_state = null`

### 7.2 Policy training layer

Replace ad-hoc live execution survival summaries with a full execution-contract builder:

- train fill-time model
- train shortfall model
- train miss-cost model
- export diagnostics and calibration curves

### 7.3 Live runtime layer

In live runtime:

- always load the latest `execution_contract`
- score all candidate actions
- choose action by pessimistic expected utility
- record full decision payload into `intents` and `execution_attempts`

### 7.4 Paper runtime layer

In paper runtime:

- run the same action selector
- apply the same canary constraints when in candidate-live parity mode
- replace touch-fill with contract-based execution simulation

### 7.5 Acceptance / backtest layer

Acceptance should keep fixed signal breadth if desired,
but execution should be tested under the same learned execution contract.

That means:

- selection comparability can stay fixed
- execution comparability must stop being a different contract

## 8. Phase Plan

### Phase 0 - Observability hardening

- guarantee `execution_policy` and `micro_state` persistence on every live attempt
- persist action candidates and chosen-action diagnostics
- persist miss-cost labels for later training

### Phase 1 - Execution contract builder

- create `ExecutionContractBuilder`
- fit fill-time / shortfall / miss-cost models from `execution_attempts`
- publish `execution_contract.json`

### Phase 2 - Live runtime migration

- replace current heuristic / fallback execution selection with contract-driven selection
- keep hard safety rails, but remove action-choice heuristics

### Phase 3 - Paper execution parity

- replace touch-fill
- run the same execution contract in paper
- add candidate-live parity paper mode

### Phase 4 - Acceptance/backtest parity

- route execution acceptance through the same execution contract
- keep signal breadth freeze if needed
- stop testing execution with a separate simpler contract

## 9. Validation Criteria

We should consider the redesign successful only if all of the following improve:

- same-model paper/live entry decision agreement
- same-model paper/live fill-rate calibration
- same-model paper/live time-to-fill calibration
- fewer `MISSED` high-edge live entries
- fewer `CANCELLED_ENTRY / MAX_REPLACES_REACHED`
- fewer online risk-control entry halts caused by missed-entry cascades
- paper no longer shows same-timestamp immediate fills where live shows zero fill

## 10. Immediate Design Implication

The next implementation should **not** start from trainer weighting.

It should start from:

- shared execution contract
- shared action selection
- shared fill / non-fill semantics

Only after that should trainer weighting be treated as a secondary calibration layer.
