# V5 Upstream Downstream Alignment Modification Plan 2026-04-08

## 0. Purpose

This document converts the recent baseline and fast-run findings into a concrete modification plan.

The plan focuses on one question:

- how do we improve entry, sizing, risk, and exit behavior without breaking the ownership split that the existing v5 contract is already moving toward?

The answer must respect both sides at once:

- upstream model outputs and training artifacts
- downstream runtime, acceptance, paper, and live consumers


## 1. Research Basis

This plan is based on the current blueprint documents plus direct code-path inspection.

Primary documents:

- `docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md`
- `docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md`
- `docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md`
- `docs/RUNTIME_VIABILITY_AND_RUNTIME_SOURCE_HARDENING_PLAN_2026-04-06.md`

Primary code paths inspected:

- `autobot/models/predictor.py`
- `autobot/models/train_v5_panel_ensemble.py`
- `autobot/models/train_v5_fusion.py`
- `autobot/models/entry_boundary.py`
- `autobot/models/v5_variant_selection.py`
- `autobot/strategy/v5_post_model_contract.py`
- `autobot/strategy/model_alpha_v1.py`
- `autobot/live/model_alpha_runtime_execute.py`
- `autobot/risk/portfolio_budget.py`
- `autobot/models/trade_action_policy.py`
- `autobot/models/execution_risk_control.py`
- `autobot/models/runtime_recommendations.py`
- `autobot/common/path_risk_guidance.py`
- `autobot/risk/liquidation_policy.py`
- `autobot/risk/live_risk_manager.py`
- `scripts/candidate_acceptance.ps1`

Recent observed failure shape used as planning anchor:

- baseline acceptance and runtime parity still fail for economic reasons
- selection policy improved, but the economic result barely moved
- timeout-heavy exit structure remains severe
- fusion variant and runtime input structure remained nearly unchanged between the baseline and fast rerun


## 2. Current System Reconstruction

### 2.1 Upstream Contract Already Exists

The v5 stack already exports the right family of fields:

- `final_rank_score`
- `final_expected_return`
- `final_expected_es`
- `final_tradability`
- `final_uncertainty`
- `final_alpha_lcb`
- `score_mean`
- `score_std`
- `score_lcb`

This contract is present in:

- `autobot/models/predictor.py`
- `autobot/models/train_v5_panel_ensemble.py`
- `autobot/models/train_v5_fusion.py`

This is important because the repo is no longer purely "score only".
The main issue is not that the fields do not exist.
The main issue is that their semantics, provenance, and downstream ownership are not fully aligned yet.


### 2.2 Entry Ownership Is Already Predictor First

The v5 entry contract is explicit:

- entry owner: `predictor_boundary`
- sizing owner: `portfolio_budget_first`
- trade action role: `advisory_only_v1`
- exit owner: `continuation_value_controller`

`resolve_v5_entry_gate(...)` already enforces:

- alpha LCB floor
- tradability threshold
- entry boundary severe-loss risk threshold
- expected net edge positivity
- portfolio budget, breaker, and rollout blockers

This is directionally aligned with the blueprint.


### 2.3 Sizing Is Half-Aligned

`resolve_v5_target_notional(...)` already computes a v5 sizing decision from:

- expected return
- expected ES
- tradability
- uncertainty
- alpha LCB

and then `resolve_portfolio_risk_budget(...)` applies:

- portfolio exposure limits
- cluster limits
- available quote limits
- liquidity haircuts
- data quality haircuts
- recent loss streak haircuts

This is good and already close to the blueprint.

However, there is still a consistency problem:

- `model_alpha_v1.py` still lets runtime `size_ladder` post-process the v5 requested multiplier
- `train_v5_fusion.py` currently treats `trade_action` and `risk_control` as advisory in v5 deploy-readiness
- runtime still consumes some of those layers in a way that can change effective size

So the design language says "portfolio_budget_first", but the runtime still keeps a secondary size clamp path alive.


### 2.4 Exit And Liquidation Are More Advanced Than The Recent Failure Narrative Suggests

The current repo already contains:

- path-risk continuation logic
- dynamic exit overlay
- execution-calibrated protective liquidation tiers
- live protective order replacement logic
- liquidation reports and paper evidence aggregation

This means the problem is not "there is no sell-side risk stack".
The problem is that these layers are not yet consistently reflected in:

- training-time optimization
- fusion variant selection
- acceptance semantics


### 2.5 Acceptance And Runtime Parity Use Different Decision Language

This split is intentional and currently implemented:

- acceptance uses `acceptance_frozen_compare_v1`
- acceptance disables learned selection, exit, hold-bars, risk, trade-action, and learned execution
- runtime parity uses `runtime_deploy_contract_v1`
- runtime parity enables learned selection and learned execution-side decisions

Therefore:

- selection improvements alone cannot rescue acceptance
- runtime-only exit and liquidation improvements alone cannot rescue acceptance
- changing acceptance semantics is possible, but it is not a safe first move


## 3. Confirmed Consistency Gaps

### G1. Upstream fields exist, but some are still heuristic rather than fully provenance-aware

Examples:

- predictor fallback computes `final_tradability` from `1 / (1 + ES + uncertainty)`
- panel contract also derives tradability from `ES + uncertainty`
- fusion can ingest a dedicated tradability expert, but the downstream contract does not yet expose a strong quality or reliability layer for that expert

This matters because the runtime may consume a scalar called `final_tradability` even when its origin is:

- a strong dedicated expert
- a panel proxy
- a fallback heuristic

without enough downstream visibility.


### G2. Fusion runtime recommendations are still mostly panel-seeded

`_build_fusion_runtime_recommendations(...)` inherits:

- `exit`
- `execution`
- `risk_control`
- `trade_action`

from the panel runtime context.

That is acceptable as a temporary bootstrap step.
It is not good enough as the long-term deploy contract for a separate fusion family.

Current problem:

- fusion owns the predictor contract
- panel still largely seeds the downstream decision contract

This makes it hard to reason about true family ownership and evidence.


### G3. Fusion variant selection is still too offline-centric

`v5_variant_selection.py` already checks:

- runtime viability
- runtime deploy contract readiness

But the fusion report still leaves these fields effectively unpopulated:

- `execution_structure_non_regression`
- `paper_non_regression`
- `paired_non_regression`
- `canary_non_regression`

The report shape anticipates richer downstream evidence, but the selection logic still mostly chooses from:

- `test_ev_net_top5`
- `test_precision_top5`
- `test_pr_auc`
- `test_log_loss`

This is exactly where the current misalignment shows up.


### G4. Runtime viability does not yet account for input quality strongly enough

The runtime viability report currently checks:

- rows above alpha floor
- entry gate allowed count

This catches zero-viability candidates well.
It does not yet strongly punish candidates whose allowed rows come from structurally weak expert inputs, such as:

- mostly reduced-context sequence rows
- mostly reduced-context LOB rows
- tradability experts with thin training evidence

The baseline failure suggests we are already beyond the "zero viable rows" problem.
Now we need "weak viable rows" diagnostics, not only "any viable rows".


### G5. Acceptance frozen compare and runtime deploy compare are intentionally divergent

This is not a bug.
But it creates a planning constraint:

- if we improve trade-action, risk-control, or liquidation semantics only in deploy-like lanes, the primary frozen acceptance gate may not move

This means the plan must explicitly separate:

- contract-preserving fixes that improve frozen acceptance economics
- contract-evolution fixes that improve deploy-like runtime quality


### G6. Sell-side intelligence exists, but it is not first-class in trainer selection

The repo already contains:

- timeout-share penalties
- TP-absence penalties
- path-risk continuation logic
- protective liquidation tiers

But these signals are not yet a first-class objective for:

- fusion variant choice
- upstream contract quality ranking
- acceptance-side evidence synthesis

That is why we can have a strong-looking variant that still collapses into:

- near-zero TP share
- extreme timeout share
- poor payoff ratio


### G7. V5 sizing ownership is not completely clean yet

The blueprint says:

- alpha decides what to buy
- tradability says whether it can be executed
- risk decides how much, under what budget, and when to stop

Current runtime still retains a legacy path where:

- v5 requested size can be post-clamped by risk-control size ladder

That may be acceptable as a temporary compatibility bridge.
It is not the clean final ownership boundary.


## 4. Planning Principles

The plan should follow these rules.

### P1. Preserve the ownership split

Do not let `risk_control` silently become a second alpha selector again.

Keep:

- predictor and entry boundary as entry owners
- portfolio budget as hard sizing owner
- trade_action and risk_control as advisory or explicit safety layers
- continuation and liquidation as exit owners


### P2. Keep `acceptance_frozen_compare_v1` stable in the first pass

Do not immediately rewrite the primary acceptance semantics.

Reason:

- current documents explicitly rely on frozen compare
- recent failure diagnosis still points to true economic weakness, not only gate misconfiguration
- changing the gate first would reduce interpretability


### P3. Version contracts rather than mutating meaning in place

If we change semantics for:

- tradability meaning
- sizing authority
- exit/liquidation language
- runtime evidence requirements

we should do it via new or clearly versioned artifact contracts, not by quietly changing old fields.


### P4. Make quality and provenance machine-readable

Every field that materially affects runtime decisions should expose:

- source family
- source mode
- quality or reliability hints
- support-level coverage summaries

This is more valuable now than adding another heuristic threshold.


### P5. Improve frozen acceptance and deploy parity on separate tracks

The repo already has this split.
The plan should respect it rather than blur it.


## 5. Detailed Modification Plan

### Phase 0. Freeze The Current Truth And Add Diagnostics

Goal:

- make the next changes auditable against the current baseline

Changes:

1. extend fusion and acceptance reports with a compact "decision-language summary":
   - entry gate reason counts
   - timeout share
   - TP share
   - liquidation tier counts when available
   - source-quality summary for sequence, LOB, and tradability

2. add a compact "input quality summary" to fusion variant reports:
   - sequence strict-full ratio
   - LOB strict-full ratio
   - tradability train rows
   - tradability selected markets
   - whether tradability is proxy-only or expert-backed

3. expose whether v5 runtime size was modified by:
   - portfolio budget
   - risk-control size ladder
   - operational overlay

Primary files:

- `autobot/models/train_v5_fusion.py`
- `autobot/models/v5_variant_selection.py`
- `autobot/strategy/model_alpha_v1.py`
- `autobot/live/model_alpha_runtime_execute.py`
- `autobot/common/paper_lane_evidence.py`

Reason:

- we need one place to see whether the next candidate improved because of better alpha, better sizing, or better liquidation


### Phase 1. Harden The Upstream Predictor Contract

Goal:

- make downstream consumers see the true strength and quality of the upstream signal

Changes:

1. extend the predictor contract with provenance and quality fields:
   - `final_tradability_source`
   - `final_tradability_source_family`
   - `final_tradability_quality`
   - `auxiliary_support_level`
   - `auxiliary_support_penalty`
   - `sequence_support_level`
   - `lob_support_level`
   - `tradability_evidence_strength`

2. distinguish true tradability expert output from fallback proxy output:
   - dedicated expert-backed tradability remains primary when available
   - panel or generic predictor proxy must be explicitly marked as proxy

3. add a guardrail in fusion training to prevent "tradability is high but return head treats it as negative edge" from remaining silent:
   - write feature-contribution diagnostics into fusion artifacts
   - fail or warn when dominant runtime contribution is directionally inconsistent with the intended contract

4. keep `final_alpha_lcb` explicit, but add companion diagnostics:
   - component contributions to LCB negativity
   - source-quality penalty contribution

Primary files:

- `autobot/models/predictor.py`
- `autobot/models/train_v5_panel_ensemble.py`
- `autobot/models/train_v5_fusion.py`

Why this phase is first:

- downstream alignment is impossible if upstream fields do not expose whether they are strong, weak, proxy, or degraded


### Phase 2. Make Fusion Input Quality First-Class In Variant Selection

Goal:

- stop choosing a fusion variant only because its offline leaderboard looks slightly better

Changes:

1. add a support-aware penalty or constraint to fusion variant selection:
   - penalize candidates whose sequence or LOB coverage is mostly reduced-context
   - penalize candidates whose tradability evidence is too thin

2. upgrade fusion selection evidence from placeholders to real metrics:
   - `execution_structure_non_regression`
   - `paper_non_regression`
   - `paired_non_regression`
   - `canary_non_regression`

3. add a short ablation matrix into the trainer path:
   - panel only
   - panel + sequence
   - panel + LOB
   - panel + sequence + LOB
   - full fusion with tradability
   - full fusion without tradability

4. use the ablation results to decide whether sequence, LOB, or tradability should be:
   - included
   - downweighted
   - excluded

5. strengthen fusion clear-edge logic:
   - non-baseline fusion should beat baseline not only on offline utility
   - it should also not materially regress execution-structure quality

Primary files:

- `autobot/models/v5_variant_selection.py`
- `autobot/models/train_v5_fusion.py`

Expected outcome:

- we stop defaulting to `regime_moe` just because it has a small offline edge when the deploy-quality evidence is weak or ambiguous


### Phase 3. Clean Up Entry And Sizing Ownership

Goal:

- make the runtime behavior match the ownership language already written into the contract

Changes:

1. keep v5 entry allow/deny at the predictor boundary:
   - alpha LCB
   - expected edge
   - tradability threshold
   - severe-loss boundary

2. keep hard sizing ownership in `portfolio_budget.py`

3. for v5 deploy-like lanes, choose one of these two paths explicitly:

Path A:

- risk-control size ladder becomes advisory-only for v5
- no post-clamp after v5 target-notional except portfolio and operational safety

Path B:

- risk-control size ladder remains active
- then its contract readiness must no longer be treated as merely advisory in v5 deploy mode

Recommended choice:

- Path A first

Reason:

- it matches the blueprint
- it reduces hidden secondary ownership
- it improves auditability

4. write a single combined sizing report into strategy meta:
   - requested signal size
   - portfolio clamp
   - advisory clamps
   - final resolved notional

Primary files:

- `autobot/strategy/model_alpha_v1.py`
- `autobot/live/model_alpha_runtime_execute.py`
- `autobot/risk/portfolio_budget.py`
- `autobot/models/execution_risk_control.py`


### Phase 4. Lift Exit And Liquidation Into First-Class Selection Evidence

Goal:

- treat timeout-heavy exit behavior as a core model-quality issue, not only a late runtime artifact

Changes:

1. promote these fields into selection and certification summaries:
   - timeout exit share
   - TP exit share
   - SL exit share
   - payoff ratio
   - liquidation tier distribution
   - expected liquidation cost

2. increase trainer-side sensitivity to timeout-heavy and TP-absent behavior:
   - current penalties exist in `runtime_recommendations.py`
   - they should become selection-visible and acceptance-visible, not only hidden row penalties

3. use path-risk continuation outputs as explicit trainer/runtime evidence:
   - `continue_value_lcb`
   - `exit_now_value_net`
   - `immediate_exit_cost_ratio`
   - `selected_hold_bars`

4. make protective liquidation evidence visible in paper and canary reporting:
   - urgent liquidation share
   - emergency flatten count
   - liquidation cost distribution

Primary files:

- `autobot/models/runtime_recommendations.py`
- `autobot/common/path_risk_guidance.py`
- `autobot/strategy/v5_post_model_contract.py`
- `autobot/risk/liquidation_policy.py`
- `autobot/risk/live_risk_manager.py`
- `autobot/common/paper_lane_evidence.py`
- `autobot/live/candidate_canary_report.py`

Important constraint:

- this phase helps runtime parity and deploy quality immediately
- it does not automatically change frozen acceptance unless we later evolve acceptance semantics


### Phase 5. Strengthen Entry Boundary With Quality-Aware Risk Calibration

Goal:

- keep entry ownership upstream, but make the boundary aware of source quality rather than only raw scalar values

Changes:

1. extend `build_risk_calibrated_entry_boundary(...)` features to include:
   - support-level quality
   - proxy-vs-expert tradability source
   - auxiliary missingness penalties

2. calibrate the entry boundary on explicit certification or walk-forward windows, not on a silently mixed source

3. add diagnostics for:
   - severe-loss-risk drift by support level
   - severe-loss-risk drift by tradability source kind

Primary files:

- `autobot/models/entry_boundary.py`
- `autobot/models/train_v5_fusion.py`


### Phase 6. Evolve Acceptance Only After The Above Is Proven

Goal:

- avoid changing the main gate before the contracts are cleaner

Stage 1:

- keep `acceptance_frozen_compare_v1` unchanged
- use it as the hard "raw model economics" checkpoint

Stage 2:

- add a separate report lane for v5 decision-language quality
- compare frozen acceptance and deploy-like runtime parity side by side

Stage 3:

- only if the paired evidence is strong, consider a new acceptance contract that gives limited credit to:
  - upstream LCB-aware selection
  - continuation-value exit logic
  - execution-calibrated liquidation

Recommended non-goal for the first implementation pass:

- do not rewrite `acceptance_frozen_compare_v1` in place

Primary files later:

- `scripts/candidate_acceptance.ps1`
- `autobot/strategy/model_alpha_evaluation_contract.py`


## 6. Concrete File-Level Change Order

Recommended implementation order:

1. `autobot/models/train_v5_fusion.py`
2. `autobot/models/v5_variant_selection.py`
3. `autobot/models/entry_boundary.py`
4. `autobot/models/predictor.py`
5. `autobot/models/train_v5_panel_ensemble.py`
6. `autobot/strategy/model_alpha_v1.py`
7. `autobot/live/model_alpha_runtime_execute.py`
8. `autobot/models/runtime_recommendations.py`
9. `autobot/common/path_risk_guidance.py`
10. `autobot/risk/liquidation_policy.py`
11. `autobot/risk/live_risk_manager.py`
12. `autobot/common/paper_lane_evidence.py`
13. `autobot/live/candidate_canary_report.py`
14. optional later: `scripts/candidate_acceptance.ps1`

Why this order:

- first fix what the model exports
- then fix how variant selection interprets it
- then fix how runtime consumes it
- only then consider changing acceptance semantics


## 7. Validation Plan

### 7.1 Unit And Contract Tests

Run or extend:

- `tests/test_predictor_contract.py`
- `tests/test_train_v5_panel_ensemble.py`
- `tests/test_train_v5_fusion.py`
- `tests/test_v5_variant_selection.py`
- `tests/test_portfolio_budget.py`
- `tests/test_trade_action_policy.py`
- `tests/test_execution_risk_control.py`
- `tests/test_runtime_recommendations.py`
- `tests/test_exit_path_risk.py`
- `tests/test_live_risk_manager.py`
- `tests/test_live_risk_budget_ledger.py`


### 7.2 Integration Tests

Run or extend:

- `tests/test_backtest_model_alpha_integration.py`
- `tests/test_paper_engine_model_alpha_integration.py`
- `tests/test_live_model_alpha_runtime.py`
- `tests/test_candidate_acceptance_certification_lane.py`
- `tests/test_candidate_acceptance_runtime_coverage.py`
- `tests/test_candidate_acceptance_v5_dependency_inputs.py`


### 7.3 Server Experiment Ladder

Use this order:

1. panel-only and no-tradability ablations
2. monotone GBDT forced fusion
3. support-aware regime MoE rerun
4. fast acceptance lane
5. fast runtime parity lane
6. representative full rerun

For every candidate, compare against the fixed baseline archive.


## 8. Success Criteria

Short-term success:

- fusion reports expose source quality and ownership cleanly
- variant selection no longer relies mainly on offline leaderboard deltas
- v5 runtime sizing ownership becomes auditable and non-ambiguous

Medium-term success:

- acceptance frozen compare improves absolute economics
- timeout exit share materially falls
- TP share becomes non-trivial
- payoff ratio rises toward or above threshold
- runtime parity slippage deterioration shrinks materially

Do not define success as:

- changing the gate so the same weak candidate passes


## 9. Recommended Immediate Next Three Work Items

1. make fusion input quality and tradability evidence first-class in `train_v5_fusion.py` and `v5_variant_selection.py`
2. remove or explicitly reclassify the v5 risk-control size-ladder post-clamp path so sizing ownership is clean
3. promote exit and liquidation evidence into selection-visible metrics before touching acceptance contract semantics


## 10. Summary

The current repo is already much closer to the blueprint than the recent failures might suggest.

The main problem is no longer "missing architecture".
The main problem is that several strong pieces already exist but are not yet aligned under one clean contract:

- upstream predictor semantics
- fusion input quality
- runtime sizing ownership
- exit and liquidation evidence
- acceptance versus deploy-like evaluation language

The right next move is not a gate tweak.
It is a contract-alignment pass.
