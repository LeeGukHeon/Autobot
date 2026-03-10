# T21.13 V4 Promotion-Eligible Budget Freeze And Scout Split v1

- Date: 2026-03-11
- Status: slice 1 landed locally

## Goal
- Split daily `v4` runs into:
  - `scout` runs for cheap exploration
  - `promotion-eligible` runs for frozen evidence quality
- stop letting shared-server throttling redefine the evidence standard for promotable runs.

## Literature Basis
- `Backtest Overfitting in the Machine Learning Era`
  - https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4686376
- `The Deflated Sharpe Ratio`
  - https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551

## Why
- current search-budget logic can switch:
  - booster sweep trial count
  - runtime recommendation profile
  - effective research breadth
- that is pragmatic for the box, but not acceptable as a moving certification standard

## Current Code Touchpoints
- `autobot/models/search_budget.py`
- `autobot/models/train_v4_crypto_cs.py`
- `autobot/models/experiment_ledger.py`
- `scripts/v4_candidate_acceptance.ps1`
- `scripts/daily_champion_challenger_v4_for_server.ps1`

## Scope
In scope:
- define two run classes:
  - `scout`
  - `promotion_eligible`
- freeze the promotable budget profile
- make acceptance reject promotion-eligible evidence produced under scout throttling

Out of scope:
- bigger hardware
- unrestricted CPCV

## Exact Implementation Standard
- promotable runs must emit a stable budget contract id
- throttled scout runs remain valid for exploration but not for promotion
- experiment ledger must record lane class and budget contract

## Acceptance
- a run can be cheap, or promotable, or both only if it satisfies the frozen promotable profile
- acceptance reports explain when promotion is blocked by scout-only evidence

## 2026-03-11 Slice 1 Implementation
- `autobot/models/search_budget.py` now emits an explicit budget-lane contract:
  - `lane_class_requested`
  - `lane_class_effective`
  - `budget_contract_id`
  - `promotion_eligible_contract`
- the frozen promotable contract is currently:
  - `budget_contract_id = v4_promotion_eligible_budget_v1`
  - `min_booster_sweep_trials = 10`
  - `runtime_recommendation_profile = full`
  - `cpcv_lite_auto_enabled = false`
- `manual_daily` and scout-scoped runs resolve to requested lane `scout`
- `scheduled_daily` requests `promotion_eligible`, but any reduced-budget run is downgraded to effective lane `scout`
- `autobot/models/experiment_ledger.py` now records lane class and promotable-contract satisfaction
- `autobot/models/train_v4_crypto_cs.py` now surfaces the same contract in `decision_surface.json`
- `scripts/v4_candidate_acceptance.ps1` now pins `-RunScope "scheduled_daily"` for the daily promotable lane
- `scripts/candidate_acceptance.ps1` now reads `search_budget_decision.json` and rejects scout-only evidence with:
  - `SCOUT_ONLY_BUDGET_EVIDENCE`
- `scripts/daily_champion_challenger_v4_for_server.ps1` now preserves that reason when challenger spawn is skipped

## Regression Coverage
- `tests/test_search_budget.py`
  - lane resolution and frozen-budget satisfaction
- `tests/test_experiment_ledger.py`
  - ledger persistence of lane and contract state
- `tests/test_candidate_acceptance_certification_lane.py`
  - scout-only evidence rejection with `SCOUT_ONLY_BUDGET_EVIDENCE`
- `tests/test_train_v4_crypto_cs.py`
  - `decision_surface.json` exposure of the search-budget contract

## Remaining Work
- introduce an explicit promotable-run launcher instead of relying only on `scheduled_daily`
- split cheap scout scheduling from frozen promotable scheduling at the orchestration layer so both can coexist intentionally
