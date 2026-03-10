# T21.16 V4 Comparable Panel And Multiple-Testing Hardening v1

- Date: 2026-03-11

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

