# T18.4 Methodology Gap Closure v1

## Goal
- Close the remaining non-data-limited gaps between the current `v4` implementation and the paper methodology direction already adopted in `T18.2` and `T18.3`.
- Keep the current architecture intact:
  - `backtest = fixed sanity gate`
  - `paper = learned runtime final gate`
  - current-market adaptation stays in the runtime operational layer
- Focus only on areas that are still heuristic or simplified even though they can be improved with the data and code structure already available today.

## Scope
This ticket is for the `v4` primary lane only.

In scope:
- correlation-adjusted `effective_trials` for DSR-style sanity checks
- finer-grained `White RC / Hansen SPA` input panel
- data-driven stationary bootstrap block-length selection
- optimizer-based `selection_recommendations`
- operational overlay coefficient calibration loop
- paper final gate statistical refinement

Out of scope:
- resurrecting `v3` as a primary automated lane
- live real-money trading
- new external datasets
- on-chain/news/sentiment feature stacks

## Why This Ticket Exists
Current status is already directionally aligned:
- learned runtime thresholds are used in paper/live
- acceptance is split into fixed backtest sanity and learned-runtime paper final gate
- `v4` trainer writes:
  - `selection_recommendations.json`
  - `walk_forward_report.json`
  - `execution_acceptance_report.json`
  - `promotion_decision.json`
- `walk_forward_report.json` already contains:
  - `SPA-like`
  - `White Reality Check`
  - `Hansen SPA`
  - `stationary bootstrap`
  - sample-dependent null recentering

But some key pieces remain simplified:
- DSR still uses raw `trial_count` instead of correlation-adjusted independent trial count
- RC/SPA still uses `trial x walk-forward-window ev_net` instead of a finer OOS differential panel
- stationary bootstrap block length is still heuristic
- `selection_recommendations` is still rule-based, not objective-optimized
- runtime operational overlay coefficients are still hand-tuned
- paper final gate thresholds are still policy thresholds, not statistically sequenced evidence

## Current Gaps, Exact Interpretation

### 1. DSR `effective_trials` is still approximate
Current code:
- `autobot/models/stat_validation.py`

Current behavior:
- `effective_trials = trial_count`

Gap:
- this is directionally useful but not close to the intended independent-trial interpretation from the DSR literature

Target:
- estimate an `effective_independent_trials` value using correlation structure across trial outcomes
- use that estimate in DSR instead of raw sweep size

### 2. RC/SPA panel is still too coarse
Current code:
- `autobot/models/multiple_testing.py`
- `autobot/models/train_v4_crypto_cs.py`

Current behavior:
- builds a `trial x walk-forward-window ev_net differential matrix`

Gap:
- this is a simplified panel
- it uses one value per window instead of a denser aligned OOS return differential vector

Target:
- move to a finer differential panel built from aligned OOS slices inside each walk-forward window
- keep `trial_panel` persisted and reproducible

### 3. Stationary bootstrap block length is still heuristic
Current code:
- `autobot/models/multiple_testing.py`

Current behavior:
- block length defaults to a bounded `sqrt(T)`-style heuristic

Gap:
- still reasonable, but not close to a data-driven selector

Target:
- add automatic block-length selection from the panel itself
- keep manual override support for debugging/research reproducibility

### 4. `selection_recommendations` is still rules-based
Current code:
- `autobot/models/train_v4_crypto_cs.py`
- `autobot/strategy/model_alpha_v1.py`

Current behavior:
- derives `recommended_top_pct` and `recommended_min_candidates_per_ts` from threshold coverage heuristics

Gap:
- this is still a decision rule, not a validated optimizer

Target:
- derive recommendations by directly optimizing a walk-forward trading objective under constraints
- keep runtime behavior the same: runtime reads recommendations, acceptance stays fixed-profile

### 5. Operational overlay coefficients are still hand-tuned
Current code:
- `autobot/strategy/operational_overlay_v1.py`

Current behavior:
- structure is correct
- coefficients are currently heuristic

Gap:
- no empirical calibration loop yet

Target:
- fit or at least re-estimate coefficients from accumulated paper/runtime history
- keep guardrails and hard caps explicit

### 6. Paper final gate is still policy-threshold based
Current code:
- `scripts/candidate_acceptance.ps1`
- `scripts/paper_micro_smoke.ps1`

Current behavior:
- uses fills/pnl/fallback/micro-quality/rolling-history cutoffs

Gap:
- still a policy gate, not a sequential evidence process

Target:
- retain hard safety floors
- add a paper evidence score or sequential acceptance layer on top

## Execution Plan

### Phase 1. Correlation-Adjusted DSR
Priority: highest

Files:
- `autobot/models/stat_validation.py`
- new helper if needed: `autobot/models/trial_dependence.py`
- tests:
  - `tests/test_stat_validation.py`

Implementation:
- compute a trial outcome vector for the current sweep
  - start with validation/test `ev_net`, top-slice precision, or another stable per-trial scalar already saved
- estimate average correlation across trial outcomes
- convert raw `trial_count` into an `effective_independent_trials` estimate
- feed that into `deflated_sharpe_ratio_estimate`

Acceptance:
- report includes both:
  - `raw_trial_count`
  - `effective_trials`
- DSR remains backward-compatible when only one trial exists

### Phase 2. Finer OOS Differential Panel For RC/SPA
Priority: highest

Files:
- `autobot/models/train_v4_crypto_cs.py`
- `autobot/models/multiple_testing.py`
- tests:
  - `tests/test_multiple_testing.py`
  - `tests/test_train_v4_crypto_cs.py`

Implementation:
- instead of one `ev_net` number per walk-forward window, persist finer aligned OOS slices
- each trial record should include:
  - `window_index`
  - `oos_slice_index`
  - `candidate_metric`
  - `champion_metric`
  - differential
- build the RC/SPA matrix from these aligned OOS slices

Acceptance:
- `trial_panel` persists the finer slice structure
- RC/SPA still works when panels are sparse, but logs explicit insufficiency reasons

### Phase 3. Data-Driven Block Length Selection
Priority: medium-high

Files:
- `autobot/models/multiple_testing.py`
- tests:
  - `tests/test_multiple_testing.py`

Implementation:
- add `auto` mode for block length selection
- estimate dependence strength from the differential panel
- derive a bounded recommended average block length
- keep current manual override path for reproducibility

Acceptance:
- reports include:
  - `bootstrap_method`
  - `average_block_length`
  - `block_length_source`

Status:
- completed
- `multiple_testing.py` now supports:
  - manual override block length
  - explicit `"auto"` mode
  - data-driven dependence-aware selection from the aligned differential panel
- RC/SPA reports now persist:
  - `block_length_source`
  - `block_length_threshold`
  - `block_length_cutoff_lag`
  - `block_length_dependence_strength`
- fallback remains bounded and reproducible for short or weak-dependence panels

### Phase 4. Optimizer-Based `selection_recommendations`
Priority: high

Files:
- `autobot/models/train_v4_crypto_cs.py`
- `autobot/models/research_acceptance.py`
- maybe new helper:
  - `autobot/models/selection_optimizer.py`
- tests:
  - `tests/test_train_v4_crypto_cs.py`
  - new `tests/test_selection_optimizer.py`

Implementation:
- evaluate a grid of:
  - threshold key
  - `top_pct`
  - `min_candidates_per_ts`
  - possibly `max_positions_total`
- objective:
  - walk-forward `ev_net`
  - with coverage/frequency constraints
- output:
  - `selection_recommendations.json`
  - recommendation provenance
  - optimization objective summary

Acceptance:
- recommendation file explicitly states:
  - `objective`
  - `selected_grid_point`
  - `constraint_reasons`
  - `fallback_used=false/true`

Status:
- completed
- `v4` now derives `selection_recommendations.json` from a walk-forward grid search instead of the old coverage heuristic
- optimizer evaluates a bounded grid of:
  - `top_pct`
  - `min_candidates_per_ts`
  for each threshold key
- objective uses the runtime-style selection simulation on walk-forward OOS windows
- runtime contract stays backward-compatible:
  - `recommended_top_pct`
  - `recommended_min_candidates_per_ts`
  are still the fields consumed by `model_alpha_v1`
- provenance is now persisted:
  - `objective`
  - `objective_score`
  - `selected_grid_point`
  - `constraint_reasons`
  - `fallback_used`
- follow-up hardening completed:
  - `recommended_threshold_key` is now selected at the root recommendation document
  - runtime uses the learned threshold key when recommendations are enabled
  - walk-forward summaries and `SPA-like`/`White RC`/`Hansen SPA` no longer hard-code `top_5pct`
  - selection-grid search variants are now included in multiplicity accounting
  - `recommendation_source`
  - `windows_covered`
  - `feasible_window_ratio`

### Phase 5. Overlay Calibration Loop
Priority: medium

Files:
- `autobot/strategy/operational_overlay_v1.py`
- new helper:
  - `autobot/strategy/operational_overlay_calibration.py`
- paper reports / logs readers if needed
- tests:
  - new `tests/test_operational_overlay_calibration.py`

Implementation:
- read accumulated paper/runtime logs
- estimate simple relationships between:
  - regime / micro quality / session
  - realized pnl / drawdown / fill / slippage
- refit:
  - risk multiplier bounds or slope
  - max position scaling
  - execution aggressiveness thresholds

Acceptance:
- calibrated coefficients are persisted to a small JSON artifact
- runtime can read them with a safe fallback to current hardcoded defaults

Status:
- completed
- new helper:
  - `autobot/common/operational_overlay_calibration.py`
- acceptance now writes a runtime calibration artifact from recent paper smoke history:
  - default path: `logs/operational_overlay/latest.json`
- runtime overlay now reads the artifact with safe fallback:
  - `autobot/strategy/operational_overlay_v1.py`
  - `autobot/strategy/model_alpha_v1.py`
- current calibration is intentionally conservative:
  - re-estimates bounded coefficients from recent paper history
  - keeps hardcoded defaults as the fallback contract when history is sparse

Current behavior:
- if enough paper smoke history exists, recent run summaries re-estimate:
  - micro quality thresholds
  - risk multiplier bounds
  - max position scaling
  - execution aggressiveness thresholds
- if history is sparse or missing, runtime keeps the old defaults
- runtime events now expose the calibration artifact path so applied settings are auditable

### Phase 6. Statistical Paper Final Gate
Priority: medium

Files:
- `scripts/candidate_acceptance.ps1`
- `scripts/paper_micro_smoke.ps1`
- maybe new helper doc/report logic

Implementation:
- keep hard safety constraints:
  - catastrophic fallback ratio
  - too few fills
  - catastrophically poor micro quality
- add a paper evidence layer that scores:
  - rolling pnl behavior
  - active window quality
  - run-history consistency
- final decision should be:
  - hard fail
  - statistical hold
  - candidate edge

Acceptance:
- report explicitly separates:
  - `hard_failures`
  - `evidence_score`
  - `final_decision_basis`

Status:
- completed
- `scripts/candidate_acceptance.ps1`
  - now resolves paper final decisions through a two-layer process:
    - hard safety failures
    - evidence-score decision (`candidate_edge` / `statistical_hold` / `insufficient_evidence`)
- hard failures remain:
  - `T15_GATE_FAILED`
  - `MIN_ORDERS_FILLED`
  - `MIN_ACTIVE_WINDOWS`
  - catastrophic micro quality
  - catastrophic fill concentration
- evidence layer now scores:
  - current realized pnl
  - fills
  - micro quality
  - nonnegative rolling windows
  - fill concentration
  - historical nonnegative run ratio
  - historical positive run ratio
  - historical median micro quality
  - historical median fallback quality

Current behavior:
- paper gate report explicitly persists:
  - `hard_failures`
  - `soft_failures`
  - `evidence_score`
  - `evidence_components`
  - `final_decision`
  - `final_decision_basis`
- promote authority remains with paper, but paper is no longer a pure threshold conjunction

## Order To Implement
Implement in this order:
1. Phase 1
2. Phase 2
3. Phase 3
4. Phase 4
3. Phase 3
4. Phase 4
5. Phase 5
6. Phase 6

Reason:
- 1~3 make the statistical testing more paper-like first
- 4 improves runtime learned breadth without changing the acceptance philosophy
- 5~6 refine operational deployment after the validation core is stronger

## Non-Negotiable Guardrails
- Do not re-open `v3` as an automated primary lane.
- Do not move final approval authority back from paper to backtest.
- Do not let online paper re-tune `min_prob/top_pct/min_candidates` every day outside trainer-produced recommendations.
- Do not remove current run-level artifacts while improving them; extend them.

## Minimum Deliverables Before Declaring This Done
- `effective_trials` is no longer raw `trial_count`
- RC/SPA uses finer aligned OOS differential slices
- stationary bootstrap has data-driven block length selection
- `selection_recommendations.json` comes from an explicit objective optimizer
- overlay coefficients can be loaded from calibration artifacts
- paper final gate distinguishes hard safety failure from statistical hold/edge

## Immediate Next Task
- Start with `Phase 1: Correlation-Adjusted DSR`
- only then proceed to finer RC/SPA panel construction

## Progress Notes

### Phase 1 Completed
- `autobot/models/stat_validation.py`
  - now reports both `raw_trial_count` and correlation-adjusted `effective_trials`
  - accepts optional `model_run_dir` so DSR can read persisted sweep artifacts
- `autobot/models/trial_dependence.py`
  - added trial-outcome dependence estimation from persisted booster sweep records
- `scripts/candidate_acceptance.ps1`
  - now passes candidate/champion model artifact directories into `stat_validation`
- tests:
  - `tests/test_stat_validation.py`
  - `tests/test_trial_dependence.py`

Current behavior:
- if persisted sweep records are available, DSR uses `effective_trials` estimated from pairwise outcome correlation
- if only raw trial count is available, behavior remains backward-compatible

Next task:
- `Phase 2: Finer OOS Differential Panel For RC/SPA`

### Phase 2 Completed
- `autobot/models/train_v4_crypto_cs.py`
  - walk-forward windows now persist deterministic aligned `oos_slices`
  - trial records now persist `test_oos_slices`
  - `trial_panel` now includes flattened `oos_slices`
- `autobot/models/multiple_testing.py`
  - RC/SPA now prefers aligned `window_index + slice_index` OOS panels
  - window-level `ev_net` remains as backward-compatible fallback
- tests:
  - `tests/test_multiple_testing.py`
  - `tests/test_train_v4_crypto_cs.py`

Current behavior:
- RC/SPA input is no longer limited to one `ev_net` number per walk-forward window
- aligned OOS slices are used when available
- older artifacts still fall back to window-level differentials

Next task:
- `Phase 3: Data-Driven Block Length Selection`
