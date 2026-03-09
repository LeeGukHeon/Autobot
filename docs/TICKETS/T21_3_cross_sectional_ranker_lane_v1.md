# T21.3 Cross-Sectional Ranker Lane v1

## Goal
- Add a trainer lane whose objective matches the actual runtime problem more directly: cross-sectional ordering of candidates at each timestamp.

## References
- Bakshi, Gao, Zhang, "Using Machines to Advance Better Models of the Crypto Return Cross-Section"
  - https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4986862
- Fieberg et al., "A Trend Factor for the Cross Section of Cryptocurrency Returns"
  - https://www.cambridge.org/core/journals/journal-of-financial-and-quantitative-analysis/article/trend-factor-for-the-cross-section-of-cryptocurrency-returns/4C1509ACBA33D5DCAF0AC24379148178

## Why This Fits The Current Server
- it reuses the current feature and registry contracts
- it is heavier than the current classifier lane, but still bounded if search breadth is kept small
- it improves methodological alignment without needing new raw data

## Scope
In scope:
- add one bounded ranker lane for `v4`
- use group structure keyed by timestamp or timestamp-bucket
- keep the downstream registry and runtime interfaces unchanged
- compare the ranker under the same acceptance loop used today

Out of scope:
- large trainer family zoo
- deep neural ranking models
- portfolio optimizer replacement at runtime

## Exact Implementation Standard
- objective type must be explicit:
  - `rank:pairwise`
  - or another named ranking objective
- grouping logic must be persisted in artifact metadata
- evaluation must include ranking-native metrics in addition to current trading summaries
- if the lane is disabled due to time budget, the report must say so explicitly

## Deliverables
- one ranking trainer path
- model-card additions for ranking metrics
- fixed-budget search profile for the current A1 server
- compatibility tests proving the same runtime contract still works

## Acceptance
- the new lane can register runs and participate in candidate/champion flow
- ranking metrics are persisted and auditable
- the lane stays within the current daily wall-time budget on the A1 box
- no runtime interface break is introduced

## Resource Fit
- CPU: medium
- RAM: low to medium
- Disk: low

## Follow-On Path
- later compare classifier/regressor/ranker under one shared evidence ledger rather than replacing the current lane blindly
