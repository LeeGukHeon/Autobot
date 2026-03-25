# NEXT CONTEXT MANDATORY EXECUTION PROTOCOL 2026-03-25

## 0. Purpose

This document is the mandatory starting point for any future implementation context working on this project.

Before reading anything else in this repository, the next context must read:

- [CODEX_MANDATORY_WORK_PRINCIPLES_2026-03-25.md](/d:/MyApps/Autobot/docs/CODEX_MANDATORY_WORK_PRINCIPLES_2026-03-25.md)
- [OCI_SERVER_ACCESS_REFERENCE_2026-03-25.md](/d:/MyApps/Autobot/docs/OCI_SERVER_ACCESS_REFERENCE_2026-03-25.md)

Its job is simple:

- force the next context to read the blueprint documents
- force the work to follow the agreed order
- prevent skipping layers or jumping to attractive later work
- make completion visible through a checklist

This document is intentionally strict.

If a future context starts implementation without first following this protocol, that work should be treated as out of contract.


## 1. Mandatory Reading Order

Before making any code change, the next context must open and read these documents in this exact order.

1. [CODEX_MANDATORY_WORK_PRINCIPLES_2026-03-25.md](/d:/MyApps/Autobot/docs/CODEX_MANDATORY_WORK_PRINCIPLES_2026-03-25.md)
2. [OCI_SERVER_ACCESS_REFERENCE_2026-03-25.md](/d:/MyApps/Autobot/docs/OCI_SERVER_ACCESS_REFERENCE_2026-03-25.md)
3. [NEXT_CONTEXT_MANDATORY_EXECUTION_PROTOCOL_2026-03-25.md](/d:/MyApps/Autobot/docs/NEXT_CONTEXT_MANDATORY_EXECUTION_PROTOCOL_2026-03-25.md)
4. [INTEGRATED_STRONG_MODEL_SYSTEM_ROADMAP_2026-03-25.md](/d:/MyApps/Autobot/docs/INTEGRATED_STRONG_MODEL_SYSTEM_ROADMAP_2026-03-25.md)
5. [DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md)
6. [SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md)
7. [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
8. [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)
9. [RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
10. [FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md](/d:/MyApps/Autobot/docs/FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md)


## 2. Non-Skippable Rules

The next context must obey all of the following.

### Rule 1

Do not jump ahead to a later roadmap item while an earlier item is still incomplete.

### Rule 2

Always work on the first unchecked item in the checklist below, unless that item is impossible due to a concrete blocker discovered from the codebase or server state.

### Rule 3

If blocked, do not silently skip.

Instead:

- document the blocker in the final response
- identify the exact file, artifact, or server state causing the block
- then move only to the minimum prerequisite or unblock task

### Rule 4

Every implementation session must explicitly mention which blueprint document and which roadmap item the work is implementing.

### Rule 5

At the end of each implementation session, this checklist document must be updated if an item is completed or materially advanced.

### Rule 6

Do not treat the observed replay clone or replay service as target architecture.

The user explicitly stated replay is not part of the intended operating path.

### Rule 7

Do not start with model upgrades before:

- data contract visibility
- server topology visibility
- pointer consistency visibility
- feature validation enforcement

are in place.

### Rule 8

Unless the user explicitly waives it, do not mark implementation work complete until all of the following are done:

- local change applied
- local verification performed
- commit created
- push completed
- OCI server accessed directly
- server-side `git pull --ff-only` completed
- server environment checked
- reflected server state confirmed

If only some of those steps were done, say so explicitly and do not call the work complete.


## 3. Canonical Ordered Checklist

The next context must start from the first unchecked item.

### Phase 0: Data And Server Foundation

- [x] 01. Add a machine-readable `data contract registry`
  Required references:
  [DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md)
  Done when:
  a repo-visible artifact exists for dataset lineage and contract identity.
  Current implementation note:
  implemented in `autobot/ops/data_contract_registry.py`, tested locally and on the OCI server, wired into `scripts/candidate_acceptance.ps1`, committed, pushed, server-pulled, and reflected as `data/_meta/data_contract_registry.json` on the OCI server.

- [x] 02. Make `features_v4` validation artifact mandatory in the operational flow
  Required references:
  [DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md)
  Done when:
  `data/features/features_v4/_meta/validate_report.json` is generated or enforced, and acceptance can fail on its absence or invalid status.
  Current implementation note:
  candidate acceptance enforces `features validate`, the main operational wrappers route into `candidate_acceptance.ps1`, server-side stale partitions causing the prior `ctrend_v1_rsi_14` schema failure were archived, and direct OCI validation on 2026-03-25 confirmed `python -m autobot.cli features validate --feature-set v4 ...` now succeeds and generates `data/features/features_v4/_meta/validate_report.json`. Local acceptance tests also verify failure on missing or invalid validate artifacts.

- [x] 03. Add a machine-readable `runtime topology report`
  Required references:
  [SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md)
  Done when:
  current server lane/unit/pointer/runtime state can be summarized by one artifact without manual SSH forensics.
  Current implementation note:
  implemented in `autobot/ops/runtime_topology_report.py`, tested locally and on the OCI server, and reflected as `logs/runtime_topology/latest.json` on the OCI server. Direct server validation on 2026-03-25 confirmed the artifact now includes actual `systemd` service and timer snapshots, git HEAD and dirty worktree state, sibling replay-like path detection, and live failure facts such as `autobot-v4-challenger-spawn.service: failed/failed`, so current lane/unit/pointer/runtime state can be summarized without separate manual SSH forensics.

- [x] 04. Add a machine-readable `pointer consistency report`
  Required references:
  [SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md)
  [FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md](/d:/MyApps/Autobot/docs/FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md)
  Done when:
  invalid steady-state pointer combinations are detectable by artifact and script.
  Current implementation note:
  implemented in `autobot/ops/pointer_consistency_report.py` and `scripts/check_pointer_consistency.ps1`, tested locally and on the OCI server, and reflected as `logs/ops/pointer_consistency/latest.json` on the OCI server. Direct server validation on 2026-03-25 confirmed the checker exits nonzero on the current invalid state and the artifact records concrete violations such as `LATEST_CANDIDATE_WITHOUT_CURRENT_STATE` and `CHAMPION_EQUALS_LATEST_CANDIDATE_NO_TRANSITION_STATE`.

- [x] 05. Add pre-flight checks for server units, pointer resolvability, and dirty worktree state
  Required references:
  [SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md)
  Done when:
  batch scripts can fail fast before expensive train/adoption steps.
  Current implementation note:
  implemented in `scripts/check_server_preflight.ps1` and wired into `scripts/daily_candidate_acceptance_for_server.ps1` and `scripts/daily_champion_challenger_v4_for_server.ps1`. Local and OCI validation on 2026-03-25 confirmed the preflight writes `logs/ops/server_preflight/latest.json`, refreshes `logs/runtime_topology/latest.json` and `logs/ops/pointer_consistency/latest.json`, exits nonzero on dirty worktree, failed unit, and stale pointer/state conditions, and causes both server batch entrypoints to fail closed before train/adoption work begins.

- [ ] 06. Define and apply replay legacy cleanup policy
  Required references:
  [SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md)
  Done when:
  replay clone/service is explicitly classified as legacy and removed from target topology logic.


### Phase 1: Logging And Matched Evaluation Foundation

- [ ] 07. Add `opportunity_log` artifact skeleton across decision paths
  Required references:
  [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  Done when:
  a single opportunity unit can be reconstructed from logs.

- [ ] 08. Add `counterfactual_action_log` or equivalent candidate-action capture
  Required references:
  [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  Done when:
  multiple candidate actions for the same opportunity are recoverable.

- [ ] 09. Implement minimal `paired paper` harness
  Required references:
  [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  Done when:
  champion and challenger can be compared on the same feed and same decision clock.

- [ ] 10. Add `risk_budget_ledger`
  Required references:
  [RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  Done when:
  live sizing and skip reasons are traceable via a budget artifact.

- [ ] 11. Add minimal white-box `portfolio risk budget engine`
  Required references:
  [RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  Done when:
  sizing is no longer purely local per-trade logic.


### Phase 2: Stronger Predictor Baseline

- [ ] 12. Add multi-horizon label bundle
  Required references:
  [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)

- [ ] 13. Implement `train_v5_panel_ensemble`
  Required references:
  [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)

- [ ] 14. Export uncertainty-aware predictor contract
  Required references:
  [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)


### Phase 3: Stronger Risk And Runtime Control

- [ ] 15. Introduce typed breaker taxonomy
  Required references:
  [RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md)

- [ ] 16. Add confidence-sequence based online monitors
  Required references:
  [RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md)

- [ ] 17. Add execution-calibrated protective liquidation policy
  Required references:
  [RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md)


### Phase 4: Execution Learning And OPE

- [ ] 18. Add action propensity logging
  Required references:
  [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)

- [ ] 19. Build `execution twin`
  Required references:
  [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  [RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md)

- [ ] 20. Add `DR-OPE`
  Required references:
  [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)


### Phase 5: Advanced Data Shapes

- [ ] 21. Add `candles_second_v1`
  Required references:
  [DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md)

- [ ] 22. Add `ws_candle_v1`
  Required references:
  [DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md)

- [ ] 23. Add `lob30_v1`
  Required references:
  [DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md)

- [ ] 24. Add sequence and LOB tensor contracts
  Required references:
  [DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md)


### Phase 6: Sequence And LOB Experts

- [ ] 25. Implement `v5_sequence`
  Required references:
  [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)

- [ ] 26. Implement `v5_lob`
  Required references:
  [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)


### Phase 7: Fusion And Risk-Calibrated Entry

- [ ] 27. Implement `v5_fusion`
  Required references:
  [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)

- [ ] 28. Add risk-calibrated or conformal entry boundary
  Required references:
  [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  [RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md)


### Phase 8: Sequential Canary Promotion

- [ ] 29. Add canary confidence-sequence artifact
  Required references:
  [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  [RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md)

- [ ] 30. Integrate promote/abort/continue state machine into automation
  Required references:
  [SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md)
  [FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md](/d:/MyApps/Autobot/docs/FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md)


## 4. Session Start Procedure

Every future implementation session must do this in order.

1. Read the documents in the mandatory reading order.
2. Open this checklist document.
3. Find the first unchecked item.
4. State explicitly in the response:
   - active checklist item number
   - document(s) being followed
   - why no earlier item is being skipped
5. Inspect the relevant local and, if needed, server state.
6. Implement only the active item and its direct prerequisites.
7. Run targeted tests.
8. Unless the user explicitly waives it, perform OCI server validation and reflection for implementation work.
9. Update this checklist if the item is completed.


## 5. Session End Procedure

Every future implementation session must end with:

- active checklist item number
- files changed
- tests run
- whether OCI server validation was performed
- whether commit, push, and server pull were performed
- artifact(s) added or updated
- whether the checklist item is now complete
- the exact next unchecked item


## 6. Server-Specific Notes Confirmed So Far

The next context must remember the following confirmed facts unless a newer direct server inspection disproves them.

- project root: `/home/ubuntu/MyApps/Autobot`
- current runtime is centered on `champion + candidate` lanes
- replay is not target architecture
- `features_v4` build report existed on the server during inspection
- `features_v4` validate report was missing during inspection
- `spawn/promote` handoff showed drift:
  `logs/model_v4_challenger/current_state.json` absent,
  `latest.json` showing `NO_PREVIOUS_CHALLENGER_STATE`


## 7. Canonical Interpretation

This document overrides any future temptation to:

- jump directly to `v5_sequence`
- jump directly to `v5_lob`
- build a flashy model before data/server contracts exist
- ignore server topology drift
- silently skip early checklist items

The correct interpretation is:

- build the foundation first
- then matched evaluation and risk visibility
- then predictor upgrades
- then advanced execution/risk machinery
- then advanced model families


## 8. Source Map

- [INTEGRATED_STRONG_MODEL_SYSTEM_ROADMAP_2026-03-25.md](/d:/MyApps/Autobot/docs/INTEGRATED_STRONG_MODEL_SYSTEM_ROADMAP_2026-03-25.md)
- [DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md)
- [SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md)
- [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
- [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)
- [RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
- [FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md](/d:/MyApps/Autobot/docs/FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md)
