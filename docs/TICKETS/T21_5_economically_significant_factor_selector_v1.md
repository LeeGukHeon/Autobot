# T21.5 Economically Significant Factor Selector v1

## Goal
- Move factor selection closer to the literature by selecting predictors for both statistical and economic value, not just model fit.

## Reference
- Bakshi, Gao, Zhang, "Using Machines to Advance Better Models of the Crypto Return Cross-Section"
  - https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4986862

## Why This Fits The Current Server
- the project already has a bounded feature universe
- selection over compact factor blocks is cheaper than adding large new raw data families
- it improves interpretability and research discipline without large storage cost

## Scope
In scope:
- define factor blocks with stable identifiers
- estimate block-level usefulness across walk-forward windows
- persist economic-significance diagnostics:
  - OOS incremental edge
  - stability across windows
  - turnover or coverage cost proxy
- allow the trainer to consume a selected factor block set instead of the full block set

Out of scope:
- black-box feature pruning with no provenance
- removing current features without a stored audit trail
- massive wrapper search over all subsets

## Exact Implementation Standard
- factor selection must emit a compact selection artifact with:
  - candidate block universe
  - accepted blocks
  - rejected blocks
  - reason codes
  - OOS incremental contribution summaries
- the selector must report when the sample is too weak to support block pruning
- rejected blocks must remain recoverable by configuration

## Deliverables
- block registry for literature-aligned factors
- economic-significance selection report
- trainer hook to load the selected block set
- tests for deterministic artifact writing and fallback behavior

## Acceptance
- the project can explain why a factor block was included or excluded
- selection is driven by persisted OOS evidence, not silent pruning heuristics
- disk growth stays negligible because only summaries are stored

## Resource Fit
- CPU: low to medium
- RAM: low
- Disk: low

## Follow-On Path
- later extend from block-level to hierarchical factor-family selection without changing the artifact contract
