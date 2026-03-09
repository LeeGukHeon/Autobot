# T21: Literature-Grounded Methodology Tickets For Current Oracle A1 Server

- Date: 2026-03-09
- Target host:
  - Oracle Cloud A1
  - `4 vCPU`
  - `24GB RAM`
  - current shared-server deployment
- Storage posture:
  - follow `T20`
  - no new large raw archive lanes
  - summary-first artifacts only

## Goal
- Raise the current system from a strong paper-first automated stack toward the highest level realistically achievable on the current server.
- Convert paper ideas into versioned contracts, persisted panels, and auditable artifacts instead of adding more heuristic knobs.

## Why A Separate Ticket Family Exists
- `T20` is mostly about storage, daily wall-time, and operational budget control.
- `T21` is about methodology uplift under the same hardware envelope.
- Every `T21` ticket must be:
  - implementable on the current server
  - explicit enough to audit
  - extensible enough to improve later without rewriting the runtime contract

## Non-Negotiable Methodology Rules
- Do not add a paper-inspired heuristic if the paper gives a formal construction that can be encoded directly.
- Persist enough intermediate structure to reproduce the method:
  - factor metadata
  - panel keys
  - fit diagnostics
  - chosen hyperparameters
- If evidence is insufficient, emit `INSUFFICIENT_EVIDENCE` or `NOT_COMPARABLE`.
- Do not silently fall back to an unrelated simpler rule.
- Keep new artifacts compact:
  - summary tables
  - fit reports
  - selected coefficients
  - no full duplicate raw replay copies

## Current-Server Ceiling
With `T20` already in place, the current box can realistically support:

- one primary automated `v4` lane
- one compact research lane at a time
- bounded trainer search
- bounded statistical validation
- compact factor additions from already collected data

It should not be used for:

- full multi-venue raw archives
- large model-zoo sweeps
- deep LOB replay research
- broad alternative-data firehoses

## Ticket Order
1. `T21.1` Exact CTREND Factor Contract v1
2. `T21.2` Compact Order-Flow Panel Contract v1
3. `T21.3` Cross-Sectional Ranker Lane v1
4. `T21.4` CPCV-Lite And PBO Research Lane v1
5. `T21.5` Economically Significant Factor Selector v1
6. `T21.6` Selector History And Guarded Auto-Apply v1

## Intended Outcome
If the `T21` family is completed without violating `T20`, the project should move from:

- current: `strong advanced-individual / small-team paper stack`

to roughly:

- target on this server: `strong small professional-team grade`

It still will not become a full frontier research lab on this hardware, but it can become a much more methodologically serious and auditable system.

## References
- Bakshi, Gao, Zhang, "Using Machines to Advance Better Models of the Crypto Return Cross-Section"
  - https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4986862
- Fieberg et al., "A Trend Factor for the Cross Section of Cryptocurrency Returns"
  - https://www.cambridge.org/core/journals/journal-of-financial-and-quantitative-analysis/article/trend-factor-for-the-cross-section-of-cryptocurrency-returns/4C1509ACBA33D5DCAF0AC24379148178
- "Order Flow and Cryptocurrency Returns"
  - https://www.sciencedirect.com/science/article/pii/S1386418126000029
- Arian, Norouzi, Seco, "Backtest Overfitting in the Machine Learning Era"
  - https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4686376
- Bailey, Lopez de Prado, "The Deflated Sharpe Ratio"
  - https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551
