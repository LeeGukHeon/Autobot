# T21.16 V4 Comparable Panel And Multiple-Testing Hardening v1

- Date: 2026-03-11
- Status: slice 1 landed locally

## Goal
- Harden `spa_like`, `White RC`, `Hansen SPA`, and `CPCV-lite` into a clean certification-support lane.

## Why
- current comparable panels are much better than before, but still mix real panel rows and
  simplified selection-grid rows
- certification-grade multiple testing should not depend on synthetic flat values or incomplete panel keys

## Current Code Touchpoints
- `autobot/models/train_v4_crypto_cs.py`
- `autobot/models/selection_optimizer.py`
- `autobot/models/multiple_testing.py`
- `autobot/models/cpcv_lite.py`

## Scope
In scope:
- keep full comparable trial keys for selection-search rows
- remove placeholder values that hide missing metrics
- strengthen diagnostics for:
  - insufficient common trials
  - insufficient common windows
  - budget-cut CPCV status

Out of scope:
- large bootstrap counts that exceed the server budget

## Exact Implementation Standard
- no synthetic trial may be invented
- placeholder zeros may not stand in for missing evidence
- reports must explain exactly why a panel is not comparable

## Acceptance
- multiple-testing reports become either:
  - comparable with trustworthy inputs
  - explicitly insufficient
- not silently flattened by placeholder rows

## 2026-03-11 Slice 1 Implementation
- `train_v4_crypto_cs.py`
  - selection-grid trial panels now preserve real `period_results`
  - fake slice expansion and placeholder `precision = 0.0` values were removed
  - walk-forward reports now store `multiple_testing_panel_diagnostics`
- `multiple_testing.py`
  - added explicit panel-alignment diagnostics:
    - candidate trial source mix
    - champion panel source
    - shared/common panel key counts
    - exact insufficiency reasons
  - `White RC` and `Hansen SPA` now include those diagnostics in both:
    - comparable results
    - insufficient results
- `cpcv_lite`
  - summary payloads now surface:
    - `budget_reason`
    - explicit `reasons`
    - `BUDGET_CUT` / skipped-fold / insufficient-comparable-fold diagnostics

## Regression Coverage
- `tests/test_multiple_testing.py`
  - selection-grid period panels are comparable when real period keys exist
  - diagnostics flag missing `selection_grid` period evidence
  - insufficient overlap surfaces exact panel reasons
- `tests/test_train_v4_crypto_cs.py`
  - selection-grid trial panels keep real OOS periods and stop emitting placeholder precision/slices
