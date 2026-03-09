# T21.7 White/Hansen Comparable Panel v1

## Goal
- Make `White Reality Check` and `Hansen SPA` operate on a genuinely comparable `trial x window` panel instead of frequently falling back to `INSUFFICIENT_COMMON_TRIAL_WINDOWS`.

## Why This Ticket Exists
- The current `v4` path already persists:
  - walk-forward windows
  - trial panels
  - selection-search trials
- But if the persisted panel is too thin, `White/Hansen` become structurally non-comparable even when the rest of the trainer is healthy.
- This is a methodology gap, not a threshold-tuning problem.

## Scope
In scope:
- ensure the persisted selection-search trial panel includes every evaluated grid point, not only the final one per window
- keep the `trial x window` panel aligned across shared `threshold_key/top_pct/min_candidates`
- persist enough trial diversity so `trial_count >= 2` is realistically achievable
- emit compact comparable-panel diagnostics in the walk-forward report

Out of scope:
- weakening `White/Hansen` into advisory-only checks
- forcing manual overrides when the evidence is still structurally insufficient
- large combinatorial search beyond the current Oracle A1 budget

## Exact Implementation Standard
- every window must contribute all valid `selection_optimization.grid_results` rows into the persisted selection-search panel
- selection-search trial ids must stay deterministic and stable
- diagnostics must make it obvious whether insufficiency is caused by:
  - too few common windows
  - too few common trials
  - missing champion panel coverage
- the implementation must not silently invent synthetic trials

## Deliverables
- fixed selection-search trial panel builder
- regression test proving multiple grid trials survive panel materialization
- ticket/documentation updates so future acceptance failures can be interpreted correctly

## Acceptance
- a walk-forward report with multiple grid points per threshold produces `selection_search_trial_count > 1`
- `White/Hansen` no longer become non-comparable merely because the builder dropped all but the last grid row
- no change to runtime contracts or live/paper behavior

## Resource Fit
- CPU: negligible
- RAM: negligible
- Disk: negligible

## Follow-On Path
- after new champion baselines are retrained under the corrected panel contract, re-evaluate whether additional window/trial thickening is still needed
