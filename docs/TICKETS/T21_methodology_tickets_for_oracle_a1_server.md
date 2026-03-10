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
7. `T21.7` White/Hansen Comparable Panel v1
8. `T21.8` Shared Rank Selection Policy v1
9. `T21.9` Shared OOS Selection Calibration v1
10. `T21.10` Learned Exit-Mode Selection v1

## 2026-03-11 Global Audit Update

### Decision-Surface Inventory Completed
The current `v4` methodology flow was re-audited across every code layer that can change
training, evidence, promotion, or runtime behavior.

Entry/config owners:
- `config/train.yaml`
- `config/strategy.yaml`
- `config/backtest.yaml`
- `autobot/cli.py`
  - `model train`
  - `model daily-v4`
  - `paper alpha`
  - `backtest alpha`
  - `live run`

Trainer/research owners:
- `autobot/models/train_v4_crypto_cs.py`
- `autobot/models/split.py`
- `autobot/models/selection_optimizer.py`
- `autobot/models/selection_policy.py`
- `autobot/models/selection_calibration.py`
- `autobot/models/research_acceptance.py`
- `autobot/models/execution_acceptance.py`
- `autobot/models/runtime_recommendations.py`
- `autobot/models/factor_block_selector.py`
- `autobot/models/search_budget.py`
- `autobot/models/experiment_ledger.py`

Promotion/orchestration owners:
- `scripts/candidate_acceptance.ps1`
- `scripts/v4_candidate_acceptance.ps1`
- `scripts/daily_champion_challenger_v4_for_server.ps1`

Runtime-consumption owners:
- `autobot/models/predictor.py`
- `autobot/strategy/model_alpha_v1.py`
- `autobot/live/model_alpha_runtime.py`
- `autobot/paper/engine.py`
- `autobot/backtest/engine.py`

### Global Methodology Gaps Found
- training-time `execution_acceptance` and runtime recommendation search reuse the same
  `options.start/end` window as the trainer run
- `trainer evidence` is consumed from `promotion_decision.json`, so acceptance currently reads
  train-produced evidence rather than a separate certification artifact
- hyperparameter search, walk-forward selection, factor pruning, and acceptance use different
  objectives, so the lane is literature-inspired but still globally heuristic
- factor block selection uses median ablation rather than bounded refit/drop-block certification
- research evidence quality changes with shared-server budget throttling, so promotable runs and
  scout runs are not yet methodology-frozen
- `White/Hansen` comparable panels are improved but still not fully clean enough to be treated as
  a hardened certification lane
- the runtime problem is cross-sectional ordering, but the daily promotable lane still defaults to
  `task=cls`

### Phase 2 Ticket Order
These tickets are the required follow-on family before changing core promotion behavior again.

1. `T21.11` V4 Decision Surface Inventory And Freeze v1
2. `T21.12` V4 Evidence Window Separation And Certification Lane v1
3. `T21.13` V4 Promotion-Eligible Budget Freeze And Scout Split v1
4. `T21.14` V4 Shared Economic Objective Contract v1
5. `T21.15` V4 Refit-Based Factor Block Certification v1
6. `T21.16` V4 Comparable Panel And Multiple-Testing Hardening v1
7. `T21.17` V4 Ranker Shadow Lane And Lane Governance v1

### Implementation Rule
- `T21.11` must land first and write an auditable decision-surface artifact for every `v4` run.
- `T21.12` and `T21.13` must land before any change that makes promotion easier.
- `T21.14` may not change objectives silently; it must publish one shared objective contract.
- `T21.15` may not auto-prune from one-run ablation evidence.
- `T21.16` must keep `INSUFFICIENT_EVIDENCE` and `NOT_COMPARABLE` explicit.
- `T21.17` is shadow-lane only until the earlier tickets freeze the certification contract.

### Implementation Progress
- `T21.11` slice 1 landed:
  - `decision_surface.json` is now written for each `v4` training run
- `T21.12` slice 1 landed:
  - `candidate_acceptance.ps1` now separates train and certification windows
  - each candidate run now gets `certification_report.json`
  - trainer evidence is consumed from the certification artifact rather than directly from `promotion_decision.json`
- `T21.12` slice 2 landed:
  - trainer now writes `trainer_research_evidence.json`
  - certification no longer reads research evidence directly from `promotion_decision.json`
- `T21.13` slice 1 landed locally:
  - `search_budget_decision.json` now records:
    - `lane_class_requested`
    - `lane_class_effective`
    - `budget_contract_id`
    - `promotion_eligible_contract`
  - `decision_surface.json` and the experiment ledger now surface the same budget-lane contract
  - `candidate_acceptance.ps1` now rejects scout-only evidence with:
    - `SCOUT_ONLY_BUDGET_EVIDENCE`
  - the daily `v4` wrapper now pins:
    - `RunScope = scheduled_daily`

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
