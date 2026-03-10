# T21.15 V4 Refit-Based Factor Block Certification v1

- Date: 2026-03-11

## Goal
- Replace median-ablation factor-block selection with bounded refit/drop-block certification.

## Literature Basis
- `Using Machines to Advance Better Models of the Crypto Return Cross-Section`
  - https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4986862

## Why
- current selector is compact and pragmatic, but median ablation is only a rough proxy
- with correlated blocks and tree interactions, ablation can misstate true incremental value
- guarded auto-pruning should rely on stronger evidence before it changes the active feature set

## Scope
In scope:
- run bounded drop-block refits on selected windows
- certify block usefulness using incremental OOS economic contribution
- keep history-based guarded auto policy, but feed it refit-based evidence

Out of scope:
- exhaustive subset search
- black-box SHAP-only pruning

## Exact Implementation Standard
- every rejected optional block must have stored refit evidence
- protected base blocks remain non-prunable
- if budget is insufficient, keep full set and emit explicit insufficiency reasons

## Acceptance
- pruning decisions are based on stored refit evidence, not one-pass ablation only
- guarded auto no longer activates from median-ablation summaries alone

