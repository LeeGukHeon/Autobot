# INTEGRATED STRONG MODEL SYSTEM ROADMAP 2026-03-25

## 0. Purpose

This document is the execution roadmap that integrates the five blueprint documents written so far.

Before any implementation session begins, the required entry point is:

- [CODEX_MANDATORY_WORK_PRINCIPLES_2026-03-25.md](/d:/MyApps/Autobot/docs/CODEX_MANDATORY_WORK_PRINCIPLES_2026-03-25.md)
- [OCI_SERVER_ACCESS_REFERENCE_2026-03-25.md](/d:/MyApps/Autobot/docs/OCI_SERVER_ACCESS_REFERENCE_2026-03-25.md)
- [NEXT_CONTEXT_MANDATORY_EXECUTION_PROTOCOL_2026-03-25.md](/d:/MyApps/Autobot/docs/NEXT_CONTEXT_MANDATORY_EXECUTION_PROTOCOL_2026-03-25.md)

Unless the user explicitly waives it, completion in this roadmap means:

- local implementation complete
- local verification complete
- commit complete
- push complete
- OCI server accessed directly
- server `git pull --ff-only` complete
- OCI server validation complete
- reflected server state confirmed

- [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
- [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)
- [RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
- [DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md)
- [SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md)

This roadmap is not just an idea list.

It is meant to answer:

- what should be built first
- what depends on what
- which artifacts must exist before the next layer can be trusted
- how to sequence the work within the current codebase and server setup


## 1. The Final Target System

The strongest target system has five strong layers at the same time.

### 1.1 Strong Data Platform

Source:

- [DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md)

Core outcomes:

- raw-to-feature lineage
- data contract registry
- mandatory feature validation
- live feature parity certification
- retention classes


### 1.2 Strong Predictor

Source:

- [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)

Core outcomes:

- `v5_panel`
- `v5_sequence`
- `v5_lob`
- `v5_fusion`
- uncertainty-aware and tradability-aware outputs


### 1.3 Strong Evaluation Ladder

Source:

- [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)

Core outcomes:

- fast research backtest
- stronger certification lane
- paired paper
- canary sequential evidence


### 1.4 Strong Risk And Live Safety

Source:

- [RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md)

Core outcomes:

- portfolio risk budget
- white-box safety contract
- confidence-sequence monitors
- execution-calibrated protective liquidation


### 1.5 Strong Server And Automation Contract

Source:

- [SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md)
- [FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md](/d:/MyApps/Autobot/docs/FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md)

Core outcomes:

- runtime topology report
- pointer consistency contract
- spawn/promote state handoff contract
- pre-flight unit checks
- clean champion/candidate lane ownership


## 2. Core Principle

The system becomes strong only when all five layers are strong together.

`strong data + strong predictor + strong evaluation + strong risk control + strong server contract`

If one of these layers is weak, the whole system remains weak.

Examples:

- strong predictor + weak data contract -> hidden leakage or parity drift
- strong predictor + weak evaluation -> false confidence
- strong evaluation + weak risk control -> live blow-up risk
- strong risk control + weak server contract -> pointer/service drift


## 3. Immediate Reality Constraint

The roadmap must reflect the current server reality.

Confirmed by direct OCI inspection:

- current operating topology is centered on `champion lane + candidate lane`
- `paper-v4-replay` clone/service exists on the server
- but the user explicitly stated: `replay는 안써요`

Therefore:

- replay is not part of the target operating topology
- replay clone/service should be treated as legacy or cleanup candidate
- any future certification replay work should be implemented as an offline certification lane inside the main repo
- it should not be treated as a required live service


## 4. Highest-ROI Priorities

Within the current codebase and approximately 100GB of storage budget, the highest-ROI work is:

### Priority 1

- `data contract registry`
- `feature validate artifact mandatory`
- `runtime topology report`
- `pointer consistency report`

Reason:

- these are the shared foundation for every later improvement


### Priority 2

- `opportunity_log`
- `counterfactual_action_log`
- `paired paper harness`
- `portfolio risk budget engine`

Reason:

- these immediately improve trust in decisions even before the next model is built


### Priority 3

- `v5_panel_ensemble`
- `risk budget ledger`
- `typed breakers`

Reason:

- this is the first point where model quality and live safety both move meaningfully


### Priority 4

- stronger certification lane
- `execution twin`
- `DR-OPE`
- `confidence-sequence canary`

Reason:

- this is where deployment and promotion quality become much more principled


### Priority 5

- `v5_sequence`
- `v5_lob`
- `v5_fusion`
- conformal or risk-calibrated entry boundary

Reason:

- this is the high-ceiling phase after the foundation is already trustworthy


## 5. Recommended Phase Order

## 5.1 Phase 0: Data And Server Foundation

Duration sense:

- 1 to 2 weeks

Primary source documents:

- data/feature platform blueprint
- server/deployment automation blueprint

Main work:

1. add `data contract registry`
2. make feature `validate_report` operationally mandatory
3. add `runtime topology report`
4. add `pointer consistency report`
5. define replay legacy cleanup plan
6. add pre-flight unit and worktree checks

Done when:

- the current server topology can be described by machine-readable artifacts
- data and server drift are visible before training/adoption/promotion runs


## 5.2 Phase 1: Logging And Matched Evaluation Foundation

Duration sense:

- 2 to 4 weeks

Primary source documents:

- evaluation blueprint
- risk/live control blueprint

Main work:

1. add `opportunity_log`
2. add `counterfactual_action_log`
3. add `paired paper`
4. add `portfolio risk budget engine`
5. add `risk budget ledger`

Done when:

- champion and challenger can be compared on the same feed and same clock
- live sizing and entry decisions leave audit-ready budget traces


## 5.3 Phase 2: Stronger Predictor Baseline

Duration sense:

- 3 to 6 weeks

Primary source document:

- training blueprint

Main work:

1. multi-horizon label bundle
2. `train_v5_panel_ensemble`
3. cls/reg/rank ensemble and stacking
4. uncertainty export
5. tradability-aware predictor contract

Done when:

- runtime gets `score_mean`, `score_std`, `score_lcb`-style outputs
- paired paper can compare v4 vs v5_panel on matched opportunities


## 5.4 Phase 3: Stronger Safety And Runtime Control

Duration sense:

- 2 to 4 weeks

Primary source document:

- risk/live control blueprint

Main work:

1. typed breaker taxonomy
2. confidence-sequence monitors
3. execution-calibrated protective liquidation
4. uncertainty-aware portfolio sizing

Done when:

- risk system is no longer mostly local single-trade logic
- live halts become more statistically interpretable


## 5.5 Phase 4: Execution Learning And Off-Policy Evaluation

Duration sense:

- 4 to 8 weeks

Primary source documents:

- evaluation blueprint
- risk/live control blueprint

Main work:

1. add action propensity logging
2. build `execution twin`
3. add `DR-OPE`
4. use OPE in execution policy selection

Done when:

- execution policy changes can be filtered before full live exposure


## 5.6 Phase 5: Advanced Data Shapes

Duration sense:

- 4 to 8 weeks

Primary source documents:

- data/feature platform blueprint
- training blueprint

Main work:

1. add `candles_second_v1`
2. add `ws_candle_v1`
3. add `lob30_v1`
4. add sequence and LOB tensor contracts

Done when:

- the platform can support sequence and LOB experts without breaking v4


## 5.7 Phase 6: Sequence And LOB Experts

Duration sense:

- 6 to 12 weeks

Primary source document:

- training blueprint

Main work:

1. `v5_sequence`
2. `v5_lob`
3. compare them against `v5_panel` through paired paper and certification

Done when:

- sequence or LOB experts show robust matched-opportunity uplift


## 5.8 Phase 7: Fusion And Risk-Calibrated Entry

Duration sense:

- 6 to 10 weeks

Primary source documents:

- training blueprint
- risk/live control blueprint

Main work:

1. `v5_fusion`
2. risk-calibrated entry boundary
3. severe-loss bounded entry contract

Done when:

- entry decisions use alpha, uncertainty, tradability, and portfolio budget together


## 5.9 Phase 8: Sequential Canary Promotion Contract

Duration sense:

- 2 to 4 weeks

Primary source documents:

- evaluation blueprint
- risk/live control blueprint
- server/deployment automation blueprint

Main work:

1. canary confidence sequence artifact
2. promote/abort/continue state machine
3. integration into pointer and service automation

Done when:

- canary is no longer just a threshold list
- promotion becomes a formally bounded runtime contract


## 6. Dependency Rules

### Must Happen Before Predictor Upgrades Matter

- data contract registry
- feature validation enforcement
- opportunity logging
- paired paper


### Must Happen Before OPE Is Credible

- action propensity logging
- candidate action set logging


### Must Happen Before Live Automation Is Trustworthy

- runtime topology report
- pointer consistency checks
- spawn/promote shared state contract


### Must Happen Before Sequence/LOB Work Is Worthwhile

- storage retention classes
- second-candle / lob30 dataset design


## 7. Recommended Practical Build Order

If we want the strongest path with the least wasted effort, the practical order is:

1. `data contract registry`
2. `feature validate artifact mandatory`
3. `runtime topology report`
4. `pointer consistency report`
5. `opportunity_log`
6. `paired paper`
7. `portfolio risk budget`
8. `v5_panel_ensemble`
9. `typed breakers + confidence monitors`
10. `execution twin + DR-OPE`
11. `candles_second_v1 + lob30_v1 + sequence contracts`
12. `v5_sequence`
13. `v5_lob`
14. `v5_fusion`
15. `risk-calibrated entry boundary`
16. `sequential canary promotion`


## 8. What To Avoid

### 8.1 Do Not Start With Fancy Models Before Data And Evaluation Contracts

Without stronger contracts, better models are hard to trust.


### 8.2 Do Not Depend On The Current Replay Service

The currently observed replay clone/service is not part of the intended operating topology.


### 8.3 Do Not Promote Based Only On Aggregate PnL

Use matched-opportunity evidence and bounded live evidence.


### 8.4 Do Not Let Risk Become A Second Alpha Model Again

Alpha ownership and safety ownership must stay separated.


## 9. Final Integrated Recommendation

The strongest realistic path for this codebase is:

`strong data contracts -> matched evaluation -> portfolio-aware risk control -> v5_panel baseline -> execution twin and OPE -> sequence/LOB experts -> fused model -> sequential canary promotion`

That path is strong because it improves:

- what the model predicts
- how we verify it
- how we size and stop it
- how we operate it on the real server

in the right order.


## 10. Source Map

### Data / Feature Platform

- [DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md)

### Mandatory Implementation Start Point

- [NEXT_CONTEXT_MANDATORY_EXECUTION_PROTOCOL_2026-03-25.md](/d:/MyApps/Autobot/docs/NEXT_CONTEXT_MANDATORY_EXECUTION_PROTOCOL_2026-03-25.md)

### Predictor Design

- [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)

### Evaluation / Execution Validation

- [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)

### Risk / Live Safety

- [RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md)

### Server / Deployment Automation

- [SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md)

### Operating Contract

- [FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md](/d:/MyApps/Autobot/docs/FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md)
