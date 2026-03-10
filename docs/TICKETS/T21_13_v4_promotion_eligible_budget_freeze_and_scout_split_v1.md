# T21.13 V4 Promotion-Eligible Budget Freeze And Scout Split v1

- Date: 2026-03-11

## Goal
- Split daily `v4` runs into:
  - `scout` runs for cheap exploration
  - `promotion-eligible` runs for frozen evidence quality
- stop letting shared-server throttling redefine the evidence standard for promotable runs.

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

