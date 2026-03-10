# T21.17 V4 Ranker Shadow Lane And Lane Governance v1

- Date: 2026-03-11

## Goal
- Evaluate the `rank` lane under the same frozen methodology contract as the current `cls` lane,
  without replacing the production lane blindly.

## Literature Basis
- `Cross-Sectional Ranker Lane v1`
  - `docs/TICKETS/T21_3_cross_sectional_ranker_lane_v1.md`
- `A Trend Factor for the Cross Section of Cryptocurrency Returns`
  - https://www.cambridge.org/core/journals/journal-of-financial-and-quantitative-analysis/article/trend-factor-for-the-cross-section-of-cryptocurrency-returns/4C1509ACBA33D5DCAF0AC24379148178

## Why
- the runtime problem is cross-sectional ordering at each timestamp
- the current promotable lane still defaults to `task=cls`
- changing the production lane directly before the certification contract is frozen would be too risky

## Scope
In scope:
- add a shadow-governed `rank` evaluation lane
- compare `cls` vs `rank` under the same certification windows and objective contract
- persist lane-level governance metadata

Out of scope:
- replacing production with ranker immediately
- deep ranking model families

## Exact Implementation Standard
- lane identity must be persisted in every relevant artifact
- `rank` may be promotion-eligible only after the earlier `T21.11` to `T21.16` contracts land
- acceptance must explain which lane was evaluated and why

## Acceptance
- `rank` can accumulate evidence without silently hijacking the live lane
- lane changes become explicit governance decisions, not hidden CLI flips

