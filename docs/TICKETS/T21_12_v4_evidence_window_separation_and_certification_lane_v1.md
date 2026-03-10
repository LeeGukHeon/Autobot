# T21.12 V4 Evidence Window Separation And Certification Lane v1

- Date: 2026-03-11

## Goal
- Separate:
  - trainer fit window
  - research OOS window
  - promotion-certification window
- stop using train-produced evidence as the direct trainer-evidence gate for promotion.

## Literature Basis
- `Backtest Overfitting in the Machine Learning Era`
  - https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4686376
- `The Deflated Sharpe Ratio`
  - https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551

## Why
- current `execution_acceptance` and runtime recommendation search reuse the training run window
- `candidate_acceptance.ps1` consumes trainer evidence from `promotion_decision.json`
- this is too coupled for a certification-grade promote gate

## Scope
In scope:
- add explicit window roles:
  - `train_window`
  - `research_window`
  - `certification_window`
- generate a dedicated certification artifact, separate from `promotion_decision.json`
- make acceptance consume certification evidence only

Out of scope:
- changing paper soak policy
- changing live rollout policy

## Exact Implementation Standard
- train/search may not write the final certification verdict
- certification must read a concrete model run and evaluate it on a separate window contract
- every report must expose exact date boundaries and provenance
- if certification evidence is missing or overlapping improperly, fail with explicit reason codes

## Acceptance
- promotion can no longer pass because a training run wrote favorable self-evidence
- acceptance reports show:
  - certification artifact path
  - certification window boundaries
  - overlap checks

## 2026-03-11 Slice 1 Implementation
- `scripts/candidate_acceptance.ps1` now separates:
  - `train_window`
  - `research_window`
  - `certification_window`
- the trainer call no longer uses the certification end date directly:
  - train ends on the day before certification starts
- candidate acceptance now writes `certification_report.json` inside each candidate run directory
- trainer now writes `trainer_research_evidence.json`
- trainer evidence for the promote gate is now consumed from the certification artifact, and the certification artifact reads trainer research evidence from `trainer_research_evidence.json` rather than directly from `promotion_decision.json`
- explicit certification failure reasons now include:
  - `MISSING_DECISION_SURFACE`
  - `TRAIN_CERTIFICATION_WINDOW_OVERLAP`
  - `RESEARCH_CERTIFICATION_WINDOW_OVERLAP`

## Remaining Work
- the certification artifact still wraps train-produced research evidence
- a later slice must move the research/offline evidence producer itself into a separately governed certification lane with a stronger OOS contract
