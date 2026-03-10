# T21.11 V4 Decision Surface Inventory And Freeze v1

- Date: 2026-03-11
- Status: started locally

## Goal
- Freeze every resolved `v4` methodology decision into an auditable artifact before changing
  promotion logic again.
- Prevent silent regressions caused by CLI defaults, config inheritance, wrapper scripts,
  or runtime fallback behavior.

## Why This Ticket Comes First
- the current `v4` lane is controlled by several layers:
  - CLI
  - YAML defaults
  - training code
  - PowerShell acceptance wrappers
  - runtime artifact consumers
- without a frozen decision surface, later methodology fixes can accidentally change behavior
  without a clean before/after record

## Current Code Touchpoints
- `config/train.yaml`
- `config/strategy.yaml`
- `config/backtest.yaml`
- `autobot/cli.py`
- `autobot/models/train_v4_crypto_cs.py`
- `scripts/candidate_acceptance.ps1`
- `scripts/v4_candidate_acceptance.ps1`
- `scripts/daily_champion_challenger_v4_for_server.ps1`
- `autobot/models/predictor.py`
- `autobot/strategy/model_alpha_v1.py`

## Scope
In scope:
- write a compact `decision_surface.json` per `v4` training run
- persist:
  - resolved trainer/task/model-family inputs
  - data window ownership
  - split/cpcv/factor-selection/search-budget settings
  - execution-acceptance and runtime-recommendation provenance
  - promotion/trainer-evidence provenance
  - known methodology warnings
- add regression tests that assert the artifact exists and surfaces current coupling points

Out of scope:
- changing promotion policy
- changing objective weights
- changing windows
- changing runtime execution behavior

## Exact Implementation Standard
- every field must state the resolved value, not just the raw CLI input
- any hidden fallback must be represented as:
  - source
  - resolved value
  - warning code if it is methodology-sensitive
- the artifact must remain compact and JSON-only
- tests must cover both:
  - execution-acceptance disabled path
  - execution-acceptance enabled path

## Deliverables
- `decision_surface.json` in each `v4` run directory
- high-level decision-surface metadata in `TrainV4CryptoCsResult`
- regression tests for artifact writing and warning emission

## Acceptance
- a reviewer can open one run directory and see how that run made:
  - search-budget decisions
  - factor-selection decisions
  - runtime recommendation decisions
  - promotion-evidence decisions
- the artifact explicitly shows the current methodology shortcuts instead of hiding them

