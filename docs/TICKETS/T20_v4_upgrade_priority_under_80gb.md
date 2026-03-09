# T20: v4 Upgrade Priority Under 80GB Stable Budget

- Date: 2026-03-09
- Scope: current `v4` paper-first automated lane
- Constraint:
  - stable operating target: `<= 80GB used`
  - hard operating cap: `<= 100GB used`
- Goal:
  - prioritize the next upgrades that raise model quality and operational quality without breaking the single-server storage envelope

## Current Envelope

Current observed server state after legacy `data/raw` removal:

- total disk: `175GB`
- current used: about `67GB`
- dominant variable bucket: `logs/train_v4_execution_backtest`

Implication:

- the system is already viable inside the target envelope
- but large daily compute/search branches can push usage upward quickly
- future work must prefer:
  - more signal per byte
  - more robustness per backtest
  - less artifact fan-out

## Recommended Specs

### Minimum viable

- CPU: `4 vCPU`
- RAM: `16GB`
- Disk: `100GB SSD`
- Network:
  - stable public internet
  - long-lived websocket connectivity
- Notes:
  - keep `booster_sweep_trials` conservative
  - do not widen execution grid aggressively
  - keep retention strict

### Recommended for stable daily operation

- CPU: `8 vCPU`
- RAM: `32GB`
- Disk: `100GB SSD`
- Notes:
  - preferred target for current `v4` daily retrain + acceptance + challenger workflow
  - allows moderate trainer search without pushing completion too far into the day

### Research-friendly but outside the current storage philosophy

- CPU: `12+ vCPU`
- RAM: `64GB`
- Disk: `150GB+` or external archive/object storage
- Notes:
  - only needed if the project expands into heavier model search, multi-source alternative data, or deeper execution simulation artifacts

## Storage Budget Target

Recommended steady-state budget inside the `80GB` target:

- OS + repo + venv + system logs: `25GB to 30GB`
- raw ws + raw ticks + parquet + features + registry + paper reports: `10GB to 15GB`
- general backtest runs: `8GB to 10GB`
- execution backtest runs: `20GB to 25GB`
- slack / transient headroom: `10GB to 15GB`

Operational rule:

- if `execution_backtest` grows past `25GB` in normal days, upgrades should reduce search breadth before adding any new data source

## Priority Backlog

### T20.1 Storage envelope hardening v1

- Priority: `P0`
- Why:
  - the project already fits the target, but only because retention is active and legacy bulk data was removed
  - the remaining risk is silent log/report creep outside the main retention buckets
- Scope:
  - add retention coverage for lightweight operational logs:
    - `logs/model_v4_acceptance`
    - `logs/model_v4_challenger/archive`
    - `logs/paper_micro_smoke`
    - `logs/micro_tiering`
  - add a daily storage budget summary with section deltas
  - add `SOFT_BUDGET_EXCEEDED` and `HARD_BUDGET_EXCEEDED` markers in the cleanup report
- Acceptance:
  - steady-state stays under `80GB` for `7` consecutive days
  - crossing `100GB` becomes operationally unlikely except during short-lived transient windows
- Resource impact:
  - CPU: low
  - RAM: low
  - Disk: strongly positive

### T20.2 Daily SLA budget manager

- Priority: `P0`
- Why:
  - a good daily loop is not just accurate; it must finish on time
  - current cost spikes come from trainer sweep breadth and runtime recommendation grid breadth
- Scope:
  - introduce disk-aware and time-aware search budgets:
    - lower `booster_sweep_trials` when storage or completion time is pressured
    - shrink runtime recommendation grid when a run is already duplicate-prone or late
  - emit a daily `search_budget_decision` report
- Acceptance:
  - `spawn` 95th percentile wall time stays below a defined target window
  - no budget escalation when used disk is above `80GB`
- Resource impact:
  - CPU: positive
  - RAM: neutral
  - Disk: positive

### T20.3 Duplicate candidate diversification

- Priority: `P1`
- Why:
  - duplicate short-circuit now saves time, but it does not create a better next candidate
  - repeated duplicates mean the architecture is waiting on candidate diversity
- Scope:
  - when the base candidate matches champion artifacts exactly:
    - try `1` or `2` fallback seeds only
    - stop immediately if disk is above the soft budget
  - write duplicate lineage and retry reasons into the registry/report
- Acceptance:
  - duplicate candidates no longer consume full downstream acceptance cost
  - fallback retries remain bounded and storage-aware
- Resource impact:
  - CPU: medium
  - RAM: low
  - Disk: low to medium

### T20.4 Ranking-oriented trainer lane

- Priority: `P1`
- Why:
  - the runtime problem is cross-sectional selection
  - classification/regression is workable, but ranking/listwise objectives are a more direct fit
- Scope:
  - add a ranking-capable trainer option for `v4`
  - keep the same feature contract and registry contract
  - compare against the current classifier/regressor under the same acceptance flow
- Acceptance:
  - new trainer can participate in candidate/champion evaluation without changing runtime interfaces
  - no material storage expansion beyond current model artifact size
- Resource impact:
  - CPU: medium
  - RAM: low to medium
  - Disk: low

### T20.5 Stronger OOS evidence without larger datasets

- Priority: `P1`
- Why:
  - the system already has `walk_forward`, `White RC`, and `Hansen SPA`
  - the next gain is better statistical power without collecting heavier raw data
- Scope:
  - improve trial-window comparability rate
  - add a lightweight `CPCV-lite` or nested temporal resampling path for research mode
  - keep production acceptance on the existing simple contract until research evidence is convincing
- Acceptance:
  - `NOT_COMPARABLE` frequency drops materially
  - research mode remains within the storage cap by storing summaries rather than full duplicate run artifacts
- Resource impact:
  - CPU: medium
  - RAM: medium
  - Disk: low

### T20.6 Execution realism uplift v1

- Priority: `P2`
- Why:
  - the current system is strong on operational gates but still not frontier-grade on fill realism
  - existing WS trade/orderbook data should be exploited more fully before adding new data domains
- Scope:
  - queue-aware passive fill proxy
  - latency bucket and stale-book penalty model
  - better paper/backtest calibration from realized paper evidence
- Acceptance:
  - slippage/fill calibration error vs paper evidence improves
  - storage growth remains modest because the system stores calibration summaries, not full raw replay copies
- Resource impact:
  - CPU: medium
  - RAM: medium
  - Disk: low to medium

### T20.7 Paper-history driven auto-tuning loop

- Priority: `P2`
- Why:
  - the architecture already has the right loop shape
  - it still needs a formal way to learn from repeated rejections and repeated champion holds
- Scope:
  - build a compact experiment ledger:
    - rejection reasons
    - duplicate frequency
    - comparable vs non-comparable rate
    - daily wall-time
    - storage cost per run
  - use that ledger to auto-adjust:
    - search budget
    - retry policy
    - selected trainer mode
- Acceptance:
  - the system can explain why it changed its own search budget
  - the ledger stays compact and summarized
- Resource impact:
  - CPU: low
  - RAM: low
  - Disk: low

### T20.8 Alternative data pilot under a strict budget

- Priority: `P3`
- Why:
  - recent literature supports richer information sources
  - but this project cannot afford a large raw-data explosion on a `100GB` box
- Scope:
  - pilot only one compact external signal family:
    - summarized on-chain
    - exchange flow aggregates
    - low-frequency sentiment/news aggregates
  - no raw firehose archive
- Acceptance:
  - monthly storage delta remains bounded
  - the signal must beat the current price+micro baseline on a budget-adjusted basis
- Resource impact:
  - CPU: medium
  - RAM: low to medium
  - Disk: medium

## Recommended Execution Order

1. `T20.1` Storage envelope hardening v1
2. `T20.2` Daily SLA budget manager
3. `T20.3` Duplicate candidate diversification
4. `T20.4` Ranking-oriented trainer lane
5. `T20.5` Stronger OOS evidence without larger datasets
6. `T20.6` Execution realism uplift v1
7. `T20.7` Paper-history driven auto-tuning loop
8. `T20.8` Alternative data pilot under a strict budget

## Not Recommended Yet

Do not prioritize these inside the current `80GB/100GB` envelope:

- full raw multi-venue tick archive
- full raw on-chain history archive
- broad news/social firehose ingest
- deep LOB training pipelines with large replay datasets
- heavy model zoo sweeps across many trainer families on the same box

## One-Line Recommendation

The next best path is:

- first make the current system cheaper and more self-aware
- then improve candidate diversity and objective alignment
- only after that add heavier data or frontier model complexity

## Follow-On

For literature-grounded methodology tickets that still fit the current Oracle A1 box, see:

- `docs/TICKETS/T21_methodology_tickets_for_oracle_a1_server.md`
