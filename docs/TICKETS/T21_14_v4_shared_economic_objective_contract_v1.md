# T21.14 V4 Shared Economic Objective Contract v1

- Date: 2026-03-11
- Status: slice 4 landed locally

## Goal
- Replace the current stack of mixed local objectives with one auditable economic objective contract
  shared across:
  - trainer sweep
  - walk-forward selection
  - execution acceptance
  - promotion comparison

## Literature Basis
- `Machine learning and the cross-section of cryptocurrency returns`
  - https://doi.org/10.1016/j.irfa.2024.103244
- `Using Machines to Advance Better Models of the Crypto Return Cross-Section`
  - https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4986862

## Why
- current `cls` model selection uses one key
- selection optimizer uses `mean_ev_net_selected`
- acceptance uses a separate balanced-pareto utility
- this makes the lane globally heuristic even when each local piece is reasonable

## Scope
In scope:
- define one versioned `economic_objective_profile`
- specify:
  - primary metric order
  - tie-break order
  - admissibility constraints
- apply the same profile to training/search/acceptance

Out of scope:
- adding deep models
- runtime order-routing changes

## Exact Implementation Standard
- objective definition must be versioned and persisted
- no module may silently use a different objective once the profile is active
- if a lane cannot support the shared objective yet, it must declare that explicitly

## Acceptance
- the same run can be explained under one economic objective from fit to promotion
- reviewers can trace why a candidate won or lost without translating between incompatible score systems

## 2026-03-11 Slice 1 Implementation
- added `autobot/models/economic_objective.py` with:
  - `profile_id = v4_shared_economic_objective_v3`
  - explicit contracts for:
    - `trainer_sweep`
    - `walk_forward_selection`
    - `offline_compare`
    - `execution_compare`
    - `promotion_compare`
- trainer now writes `economic_objective_profile.json` in each `v4` run directory
- `train_v4_crypto_cs.py` now:
  - uses the shared trainer-sweep sort key
  - records the objective contract in `metrics.json`
  - surfaces it in `decision_surface.json`
  - writes the objective artifact path into `TrainV4CryptoCsResult`
- `selection_optimizer.py` now exposes:
  - `economic_objective_profile_id`
  - `economic_objective_context`
  - explicit walk-forward tie-break ordering
- `research_acceptance.py` and `execution_acceptance` consumers now emit compare docs annotated with the shared profile id and metric order
- `candidate_acceptance.ps1` now:
  - reads `economic_objective_profile.json`
  - records the profile path/id in acceptance reports and certification provenance
  - drives promotion pareto metric selection from the shared profile instead of a private hardcoded metric list

## Regression Coverage
- `tests/test_economic_objective.py`
  - shared profile structure and trainer EV-first ordering
- `tests/test_selection_optimizer.py`
  - walk-forward optimizer profile metadata
- `tests/test_research_acceptance.py`
  - offline/execution compare profile metadata
- `tests/test_candidate_acceptance_certification_lane.py`
  - acceptance/certification provenance of `economic_objective_profile.json`
- `tests/test_train_v4_crypto_cs.py`
  - trainer artifact and decision-surface exposure

## 2026-03-11 Slice 2 Implementation
- extended `economic_objective_profile.json` so `promotion_compare` now governs:
  - candidate min-order / min-PnL / min-DSR thresholds
  - strict-vs-champion PnL delta threshold
  - drawdown-improvement threshold
  - policy variants for:
    - `strict`
    - `balanced_pareto`
    - `conservative_pareto`
    - `paper_final_balanced`
- `candidate_acceptance.ps1` now resolves promotion thresholds from the shared profile first
- explicit CLI flags still override, but only as a thin wrapper and the override keys are recorded in:
  - acceptance report config
  - train step metadata
  - backtest gate metadata
- `metrics.json` now exposes `promotion_compare` alongside the other objective contexts

## Additional Regression Coverage
- `tests/test_candidate_acceptance_certification_lane.py`
  - profile-governed min-order threshold can fail acceptance
  - explicit CLI override can relax a profile threshold and is recorded
- `tests/test_economic_objective.py`
  - resolved promotion contract exposes policy-variant thresholds and override keys

## 2026-03-11 Slice 3 Implementation
- versioned the shared profile to:
  - `profile_id = v4_shared_economic_objective_v3`
- hardened `execution_compare` so it now declares:
  - primary higher-is-better metrics:
    - `realized_pnl_quote`
  - primary lower-is-better metrics:
    - `max_drawdown_pct`
  - implementation tie-break metrics:
    - `fill_rate`
    - `slippage_bps_mean`
- `compare_v4_profiled_pareto()` now:
  - preserves Pareto domination as the first decision rule
  - uses return-plus-downside utility before execution-friction tie-breaks for runtime exit comparisons
  - exposes primary/implementation utility components in the compare document for audit

## Slice 3 Regression Coverage
- `tests/test_research_acceptance.py`
  - a lower-PnL candidate can still win `execution_compare` when downside improvement is large enough and execution friction is only worse on the tie-break axis
- `tests/test_runtime_recommendations.py`
  - selected `risk` exit now carries its own `hold_bars` into the runtime artifact

## 2026-03-11 Slice 4 Implementation
- versioned the shared profile again to:
  - `profile_id = v4_shared_economic_objective_v3`
- replaced summary-only runtime execution comparison with a validated execution contract:
  - policy: `paired_sortino_lpm_execution_v1`
  - validation method: `rolling_window_sortino_lpm_cv_v1`
  - rolling window size: `60m`
  - contiguous validation folds: `6`
  - minimum active windows: `12`
  - paired decision rule: exact sign-flip test on aligned fold-level Sortino scores
- runtime grid ranking is no longer an O(`n^2`) pairwise tournament.
  - each row is ranked by its own validated downside-risk objective and stability summary
- the final hold-vs-risk decision now requires comparable validated evidence unless the artifact is an older legacy summary-only payload

## Slice 4 Regression Coverage
- `tests/test_execution_validation.py`
  - backtest run artifacts can be converted into rolling-window Sortino/LPM validation folds
- `tests/test_research_acceptance.py`
  - execution compare can pass on validated downside-fold evidence
- `tests/test_train_v4_crypto_cs.py`
  - trainer artifacts still expose the shared profile under the updated versioned contract

## Remaining Work
- `BacktestMinProb` and `BacktestMinCandidatesPerTs` still live outside `promotion_compare`
- that is intentional for now because they shape certification backtest input construction rather than the promotion compare gate itself
