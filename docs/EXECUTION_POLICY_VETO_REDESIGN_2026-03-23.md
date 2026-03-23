# Execution Policy Veto Redesign 2026-03-23

- Date: 2026-03-23
- Scope: latest `0 fills` candidate failure, current `live_execution_policy` failure mode, research-backed redesign for fill probability, miss-cost, and execution admission control
- Related docs:
  - `docs/FOUNDATIONAL_FAILURE_MODES_2026-03-23.md`
  - `docs/TRAINING_PIPELINE_RESEARCH_COMPARE_2026-03-23.md`
  - `docs/RUNTIME_EXECUTION_FINDINGS_AND_ACTION_PLAN_2026-03-23.md`
  - `docs/DYNAMIC_EXIT_RISK_RESEARCH_2026-03-23.md`

## 1. Bottom Line

The latest candidate did not fail because:

- the model stopped selecting names
- the `23:40` execution-policy refresh timer failed to run
- or exit-path-risk artifacts were missing

It failed because the current execution contract vetoed every entry before order submission.

Implemented first slice on 2026-03-23:

- execution contract now emits longer fill horizons up to `300000 ms` and `600000 ms`
- selector now resolves deadline-aware fill probabilities instead of blindly falling back to `3 second` stats
- selector now uses price-mode/global priors for unseen actions instead of forcing automatic zero-support deadlock
- backtest and paper engines now increment `candidates_aborted_by_policy` on `EXECUTION_POLICY_ABORT`
- `candidate_acceptance.ps1` now treats micro-quality-floor windows as the authoritative usable-history boundary for split-policy consistency and reports the final selected train window length consistently

Not implemented yet:

- continuation-value miss-cost target
- explicit `execution_contract_veto_failure` gate in `candidate_acceptance.ps1`
- full `execution_contract_v2` trainer-side continuation model

The latest governed candidate run was:

- `20260323T032044Z-s42-62d7b1f6`

The challenger-spawn run completed successfully on:

- `2026-03-23 15:10:12 KST`

Its inline execution-policy refresh also completed successfully:

- `rows_total = 176`
- `logs/live_execution_policy/combined_live_execution_policy.json` was refreshed

So the direct failure mode was:

1. the model selected rows
2. execution policy evaluated candidate actions
3. every evaluated action had negative utility
4. every entry was marked `LIVE_EXECUTION_NO_POSITIVE_UTILITY`
5. no orders were submitted
6. acceptance then failed on minimum orders, runtime parity, and trainer-evidence gates

## 2. What We Measured

### 2.1 Candidate backtest did not produce submissions

Latest candidate certification backtest run:

- `data/backtest/runs/backtest-20260323-150715-73be2703ae-29413bd6`

Observed summary and event facts:

- `scored_rows = 3730`
- `selected_rows = 528`
- `candidates_total = 217`
- `orders_submitted = 0`
- `orders_filled = 0`
- `MODEL_ALPHA_SELECTION = 576`
- `EXECUTION_POLICY_ABORT = 217`
- `INTENT_CREATED = 0`

Interpretation:

- selection was alive
- runtime candidate generation was alive
- the execution-policy veto layer killed every candidate before submission

### 2.2 There is also an observability defect

In the same run:

- `events.jsonl` shows `EXECUTION_POLICY_ABORT = 217`
- but `summary.json` still reports `candidates_aborted_by_policy = 0`

Interpretation:

- the diagnosis is still valid because the raw events prove the veto path
- but current backtest summary undercounts execution-policy aborts
- this should be fixed as a reporting correctness bug

### 2.3 The latest acceptance failure chain is consistent with the veto

Latest acceptance result:

- `candidate_run_id = 20260323T032044Z-s42-62d7b1f6`
- `overall_pass = false`
- `backtest_pass = false`
- `runtime_parity_pass = false`

Because submission count was effectively zero, the following were expected downstream:

- `BACKTEST_ACCEPTANCE_FAILED`
- `RUNTIME_PARITY_BACKTEST_FAILED`
- certification minimum-order failure inside trainer-evidence requirements

## 3. How The Current Policy Works

Current code path:

- `autobot/models/live_execution_policy.py`
- `autobot/backtest/engine.py`
- `autobot/paper/engine.py`

Current per-action utility is effectively:

- `utility = p_fill_lcb * (expected_edge - shortfall) - (1 - p_fill_lcb) * miss_cost`

The current design choices behind that utility are:

1. `p_fill_lcb`
   - uses a lower-confidence-bound, not the posterior mean
2. `shortfall`
   - negative shortfall is clipped at `0`
3. `miss_cost`
   - is learned only from `MISSED` and `PARTIAL_CANCELLED` attempts
   - target is `expected_net_edge_bps` or `expected_edge_bps`
4. `state_action` support
   - uses state-action stats only if sample count is at least `3`
   - otherwise falls back to action-level averages
5. unseen actions
   - effectively get `p_fill = 0` and `miss_cost = fallback_expected_edge`
   - so they are automatically negative-utility

## 4. Structural Problems In The Current Contract

### 4.1 Horizon mismatch

This is the most important current modeling defect.

The contract is built with fill horizons:

- `1_000 ms`
- `3_000 ms`
- `10_000 ms`

But the actual execution-policy evaluation in the failed candidate used:

- `deadline_ms = 300000`

Because `300000 ms` statistics do not exist in the contract, the selector falls back to:

- `p_fill_within_default`

and `default` is effectively the `3_000 ms` fill probability.

So the system is evaluating:

- a `5 minute` GTC/JOIN order

with:

- a `3 second` fill proxy

This systematically underestimates fill probability for longer-lived orders and makes the no-trade region much larger than it should be.

This is not a minor tuning issue.
It is a contract-misalignment bug between:

- `execution artifact horizon`
- `backtest/paper/live order timeout semantics`

### 4.2 Miss-cost is not a realized cost model

Current `miss_cost_model` is built only from missed attempts and uses:

- `expected_net_edge_bps`
- fallback `expected_edge_bps`

as the target.

That is not the same as:

- realized opportunity loss
- delayed aggressive execution cost
- alpha remaining after the deadline
- cleanup or chase cost if we decide to continue

So the current miss-cost is structurally biased high whenever:

- expected alpha at submission time was high
- but the system missed the fill

This means the contract is treating:

- "missed a high-alpha opportunity"

as if it were:

- "certainly realized a large cost"

Those are not the same thing.

### 4.3 Sparse buckets are penalized twice

The current system penalizes sparse support in two different ways:

1. sparse state-action buckets fall back to action-level averages
2. the chosen fill probability is then pushed down again by `p_fill_lcb`

So a state-action pair with weak local support is both:

- denied local specificity
- and further hit by a conservative confidence penalty

This is especially harmful with only `176` total attempts in the refreshed contract.

### 4.4 Dead-action lock-in

At refresh time the observed action set was only:

- `BEST_IOC`
- `LIMIT_GTC_JOIN`
- `LIMIT_GTC_PASSIVE_MAKER`

Unseen actions such as:

- `LIMIT_POST_ONLY`
- `LIMIT_IOC_JOIN`
- `LIMIT_FOK_JOIN`
- `BEST_FOK`

have no learned fill support and no miss-cost support.

Current fallback behavior makes those actions automatically negative.

This creates a self-reinforcing loop:

1. an action has no history
2. it gets scored as impossible or strictly bad
3. it is never chosen
4. it never acquires history

This is a structural exploration deadlock.

### 4.5 Coarse state buckets blur materially different situations

Current state buckets are combinations of:

- spread bucket
- depth bucket
- age bucket
- edge bucket

This is useful for human-readable audits, but too coarse for execution valuation.

For example:

- `edge_strong`

still contained candidate states with `expected_edge_bps` around `38.25`.

That is not equivalent to:

- `80 bps`
- `120 bps`
- `200 bps`

Using the same miss-cost and fill priors for all of those within one coarse bucket throws away information exactly where admission decisions are fragile.

### 4.6 The current utility is not a continuation-value model

The current action selector compares:

- immediate fill odds
- immediate shortfall
- miss penalty

But it does not model:

- what alpha remains after a miss
- whether a more aggressive action later is still worthwhile
- whether continuing to wait dominates exiting the entry opportunity

So the current selector is closer to:

- one-shot static scoring

than to:

- dynamic execution control under alpha decay

## 5. Concrete Evidence From The Refreshed Contract

Refreshed contract action-level stats on the server were:

- `BEST_IOC`
  - fill samples: `1`
  - `p_fill_within_default ≈ 0.6667`
  - miss-cost support: none
- `LIMIT_GTC_JOIN`
  - fill samples: `48`
  - `p_fill_within_default = 0.34`
  - `mean_miss_cost_bps ≈ 48.24`
- `LIMIT_GTC_PASSIVE_MAKER`
  - fill samples: `127`
  - `p_fill_within_default ≈ 0.5504`
  - `mean_miss_cost_bps ≈ 57.65`

High miss-cost state-action examples included:

- `spread_mid|depth_shallow|age_fresh|edge_strong|LIMIT_GTC_PASSIVE_MAKER`
  - sample count `5`
  - `mean_miss_cost_bps ≈ 86.92`
- `spread_wide|depth_deep|age_fresh|edge_strong|LIMIT_GTC_PASSIVE_MAKER`
  - sample count `15`
  - `mean_miss_cost_bps ≈ 54.39`

Representative abort examples from the failed candidate:

1. state `spread_mid|depth_shallow|age_fresh|edge_strong`
   - expected edge `≈ 38.25 bps`
   - selected action `LIMIT_GTC_JOIN`
   - `p_fill_deadline = 0.34`
   - `p_fill_deadline_lcb ≈ 0.2525`
   - `miss_cost ≈ 48.24 bps`
   - utility `≈ -26.40 bps`
2. state `spread_mid|depth_shallow|age_fresh|edge_strong`
   - expected edge `≈ 38.25 bps`
   - `LIMIT_GTC_PASSIVE_MAKER` was also negative
   - utility `≈ -27.28 bps`
   - state miss-cost `≈ 86.92 bps`
3. state `spread_mid|depth_shallow|age_fresh|edge_weak`
   - expected edge `≈ 2.22 bps`
   - even `BEST_IOC` was negative
   - utility `≈ -1.94 bps`

Important interpretation:

- even replacing `p_fill_lcb` with the posterior mean would still leave those examples negative
- the bigger issue is not only confidence conservatism
- it is the miss-cost target definition and horizon mismatch

## 6. What Recent Research Actually Suggests

The papers below do not support a fixed-cap or heuristic patch approach.
They support a state-conditional and time-consistent control approach.

### 6.1 Fill probability should be modeled as state-conditional time-to-fill

Relevant direction:

- Deep attentive survival analysis in limit order books
- state-dependent fill probability modeling
- hidden-liquidity / partial-information execution control

Why this matters for Autobot:

- our current contract is already trying to estimate fill probability
- but it collapses the problem to a small fixed horizon table plus an ad hoc LCB
- the literature points toward survival or hazard modeling over relevant deadlines, conditioned on market state

Practical implication:

- `p_fill` should not be a single `3 second` fallback for a `5 minute` order
- it should be a deadline-aware survival estimate

### 6.2 Opportunity cost should be modeled under alpha decay and dynamic control

Relevant direction:

- multi-period optimal trading under alpha decay and transaction costs
- dynamic programming under incomplete information

Why this matters for Autobot:

- the cost of missing a fill is not simply the alpha estimate at submission
- it depends on how much alpha survives after the miss, what later action remains available, and what extra cost a later rescue incurs

Practical implication:

- `miss_cost` should be decomposed into:
  - alpha remaining after deadline
  - rescue execution cost if we continue
  - cleanup or abandonment value if we stop

### 6.3 TP/SL/exit caps should be process-conditioned, not brute-force heuristics

Relevant direction:

- optimal trading rules without backtesting
- recent work on stop-loss/take-profit placement under variance/return trade-offs

Why this matters for Autobot:

- the same principle that made fixed TP/SL caps unattractive also applies here
- we should not replace one bad heuristic with another
- execution admission and exit control should both be learned from path behavior, not hand-tuned thresholds

## 7. Proposed Redesign For Autobot

## 7.1 Replace `execution_contract_v1` with `execution_contract_v2`

The redesign should be factorized into three models, not one scalar heuristic.

### A. Fill-survival model

Goal:

- estimate `P(fill by deadline | state, action, deadline)`

Design:

- use discrete deadlines aligned with runtime semantics:
  - `1s`
  - `3s`
  - `10s`
  - `30s`
  - `60s`
  - `180s`
  - `300s`
  - optionally `600s`
- fit a discrete-time survival or hazard model
- inputs should include:
  - continuous spread
  - continuous depth
  - snapshot age
  - micro quality
  - action
  - price-mode family
  - expected edge as a continuous value
- use hierarchical shrinkage:
  - state-action
  - state-family
  - action
  - global

Why:

- this preserves deadline semantics
- avoids the current `300000 ms` to `3000 ms` fallback bug
- avoids hard failure when one state-action cell is sparse

### B. Continuation-value / miss-cost model

Goal:

- estimate the value of not filling by deadline

Design:

- define miss-cost as:
  - `miss_cost = continue_value_after_deadline - abandon_value_now`
- `continue_value_after_deadline` should include:
  - alpha remaining after the deadline
  - expected execution cost of a follow-up action
  - probability of later fill
- train this from both:
  - filled attempts
  - missed attempts
  - and replay / backtest path outcomes

Important:

- do not train miss-cost only on missed attempts
- do not use `expected_edge_bps` directly as the target

Why:

- the current miss-cost is selected on the worst side of the sample
- it is closer to a pessimistic opportunity label than a realizable economic cost

### C. Admission controller

Goal:

- decide whether any action is worth attempting

Design:

- first rank actions by expected continuation-adjusted value
- then run a separate admission test
- the no-trade decision should be based on:
  - posterior mean value
  - uncertainty margin
  - minimum tradable economic edge

This is different from the current approach, which effectively makes:

- "best action utility <= 0"

the same thing as:

- "no trade"

Why:

- action ranking and trade admission are different decisions
- collapsing them into one score creates brittle no-trade regions

## 7.2 Replace dead-action fallback with hierarchical priors

Current behavior for unseen actions is structurally wrong.

Instead:

- unseen actions should inherit prior estimates from:
  - action family
  - aggressiveness class
  - global state prior

Example:

- `LIMIT_IOC_JOIN`
  - should not have `p_fill = 0`
  - it should inherit from more aggressive join-like actions

This avoids the current exploration deadlock.

## 7.3 Use continuous covariates first, coarse buckets second

Recommended split:

- continuous models for scoring
- coarse buckets only for:
  - audit
  - diagnostics
  - reporting

The current bucket system is still useful, but it should not remain the main economic representation.

## 7.4 Align entry execution and exit control under one continuation framework

This redesign should integrate with the new path-risk work already added on 2026-03-23.

Entry question:

- is the expected value of trying to enter positive?

Exit question:

- is the expected value of continuing to hold positive?

Both should be answered by:

- state-conditional continuation value
- fill / execution feasibility
- alpha decay
- transaction cost

That gives one coherent control framework instead of:

- one heuristic for entry
- another heuristic for TP/SL
- and a third heuristic for timeout

## 8. Immediate Implementation Order

### Phase 0. Observability correctness

Do immediately:

- fix `candidates_aborted_by_policy` reporting in backtest and paper summaries
- add explicit `execution_contract_veto_count`
- add `best_action_value`, `admission_value`, and `admission_reason_code` to event payloads

### Phase 1. Artifact schema upgrade

Add `execution_contract_v2` with:

- deadline-aware fill-survival outputs
- hierarchical support diagnostics
- continuation-value outputs
- action-family priors

### Phase 2. Trainer build path

During refresh / training:

- build fill-survival data using actual order deadlines
- build miss-cost targets from realized continuation outcomes, not raw expected edge
- persist both posterior means and uncertainty summaries

### Phase 3. Runtime selector replacement

Replace current selector with:

1. action value model
2. separate admission controller
3. explicit uncertainty-aware no-trade rule

### Phase 4. Acceptance and reporting changes

Acceptance must treat:

- `selected_rows > 0`
- `orders_submitted = 0`
- `EXECUTION_POLICY_ABORT >> 0`

as:

- execution-contract veto failure

not merely:

- generic model backtest failure

Otherwise the wrong layer gets blamed.

## 9. What Not To Do

Do not:

- add another hard utility offset
- add another fixed cap to `miss_cost`
- relax `p_fill_lcb` without fixing horizon mismatch
- hand-whitelist actions
- lower acceptance standards just to pass a `0 fills` run

Those would all be patches around the contract, not a redesign of the contract.

## 10. Research Sources

Primary sources used for this redesign direction:

- Marcos Lopez de Prado, "Optimal Trading Rules Without Backtesting"
  - https://ssrn.com/abstract=2502613
- James Battle, "A Simple Trading Strategy with a Stop-Loss and Take-Profit Order"
  - https://ssrn.com/abstract=5859402
- Etienne Chevalier, Yadh Hafsi, Vathana Ly Vath, "Optimal Execution under Incomplete Information"
  - https://arxiv.org/abs/2411.04616
- Chutian Ma, Paul Smith, "On the Effect of Alpha Decay and Transaction Costs on the Multi-period Optimal Trading Strategy"
  - https://arxiv.org/abs/2502.04284
- Alvaro Arroyo et al., "Deep attentive survival analysis in limit order books: estimating fill probabilities with convolutional-transformers"
  - repository copy: https://ora.ox.ac.uk/objects/uuid%3Acdab1de2-7576-42e2-abae-ab12371eba76/files/r1n79h621q

## 11. Next Context Handoff

If the next context picks this up, read in this order:

1. `docs/EXECUTION_POLICY_VETO_REDESIGN_2026-03-23.md`
2. `docs/RUNTIME_EXECUTION_FINDINGS_AND_ACTION_PLAN_2026-03-23.md`
3. `docs/DYNAMIC_EXIT_RISK_RESEARCH_2026-03-23.md`
4. `autobot/models/live_execution_policy.py`
5. `autobot/backtest/engine.py`
6. `autobot/paper/engine.py`

And answer this exact question first:

- how should `execution_contract_v2` estimate `P(fill by deadline)` and `continue_value_after_deadline` using the real order timeout horizons already used by backtest/paper/live?
