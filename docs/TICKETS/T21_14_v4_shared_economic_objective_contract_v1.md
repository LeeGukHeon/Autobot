# T21.14 V4 Shared Economic Objective Contract v1

- Date: 2026-03-11

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

