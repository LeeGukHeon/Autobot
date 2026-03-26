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

### Rule 9

For operational checklist items, code existence plus local/server tests are still not sufficient to close the checkbox.

Do not mark an operational item complete unless all of the following are also confirmed:

- the current OCI operating path is actually wired to use the new logic
- the currently running unit, timer, or batch entrypoint is using it in practice
- a live or actively-produced reflected artifact, log, or machine-readable linkage confirms that usage

If an implementation exists but is only manually runnable or smoke-tested, keep the item unchecked and describe it as implemented but not yet operationally wired.

### Rule 10

Once code changes begin for the active item, implement the blueprint-defined scope as completely as reasonably possible in the same session.

Do not stop at a shallow partial patch if the blueprint or checklist item clearly implies additional directly-related required work.

If full blueprint-aligned implementation is difficult, incomplete, or blocked, report that explicitly to the user before closing the session.

That report must state:

- what was implemented
- what required blueprint scope remains
- why it could not be completed in this session
- the exact blocker, risk, or missing prerequisite

### Rule 11

If implementation changes or replaces existing logic, do not leave conflicting legacy branches in place such that the old path can silently override, bypass, or break the new logic.

Legacy paths in the touched area must be explicitly cleaned up, disabled, reconciled, or proven still correct with the new logic.

If the changed logic sits later in the protocol order, it must remain compatible with the earlier prerequisite layers and prior implemented checklist items that feed into it.

Do not change a later layer in a way that breaks the already-established earlier flow, artifacts, contracts, or operational assumptions.

The implementation must also preserve exact consistency for prior numeric and semantic contracts unless an intentional migration is performed and documented.

This includes, where relevant:

- earlier pipeline stages, prerequisite artifacts, and previously completed checklist-item outputs
- units and scales such as ratio, percent-points, bps, counts, and timestamps
- threshold meanings
- pointer meanings
- schema field meanings
- artifact/report values derived from the changed logic

If exact compatibility cannot be preserved in the same session, report that explicitly to the user as a blocker or migration risk before closing the work.

### Rule 12

If a gap, limitation, or partially unsatisfied aspect of the current checklist item is expected to be completed by a later blueprint or checklist item, record that dependency explicitly.

Do not leave such coverage implicit.

The session output and, when relevant, the checklist implementation note must state:

- what is fully covered now
- what is intentionally deferred
- which exact later blueprint or checklist item is expected to complete that deferred part
- why that later item is the correct place for it

Only do this when the later item clearly and directly covers the missing part.

If that coverage is not clear, do not treat it as safely deferred. Report it as an open issue, blocker, or incomplete scope instead.

### Rule 13

Do not arbitrarily downscope a blueprint-defined implementation to only the smallest subset currently consumed by one caller.

If the active checklist item clearly requires a contract expansion such as:

- multiple required columns
- new artifact fields
- new validation expectations
- new machine-readable linkage

then implement that contract as a complete connected slice for the touched path, not as a shallow partial stub.

Do not close an item by saying it is done when only:

- one of several required columns exists
- build writes the new shape but validate/stats/trainer metadata still expect the old shape
- the new contract exists only as an internal value and is not reflected in artifacts or specs

If compatibility with earlier layers requires temporary aliases or transitional fields, that is allowed.

But in that case the session must explicitly preserve both:

- the new canonical contract required by the active item
- the compatibility path needed so earlier implemented layers do not break


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
  implemented in `autobot/ops/runtime_topology_report.py`, tested locally and on the OCI server, and reflected as `logs/runtime_topology/latest.json` on the OCI server. Direct OCI validation confirmed the artifact now includes actual `systemd` service and timer snapshots, git HEAD and dirty worktree state, sibling replay-like path detection, paired-paper topology facts such as `autobot-paper-v4-paired.service`, and replay exclusion status, so current lane/unit/pointer/runtime state can be summarized without separate manual SSH forensics.

- [x] 04. Add a machine-readable `pointer consistency report`
  Required references:
  [SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md)
  [FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md](/d:/MyApps/Autobot/docs/FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md)
  Done when:
  invalid steady-state pointer combinations are detectable by artifact and script.
  Current implementation note:
  implemented in `autobot/ops/pointer_consistency_report.py` and `scripts/check_pointer_consistency.ps1`, tested locally and on the OCI server, and reflected as `logs/ops/pointer_consistency/latest.json` on the OCI server. Direct OCI validation confirmed the checker records concrete violation codes for broken pointer/state combinations and now reports `status=healthy` on the current server after champion/latest_candidate/current_state realignment.

- [x] 05. Add pre-flight checks for server units, pointer resolvability, and dirty worktree state
  Required references:
  [SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md)
  Done when:
  batch scripts can fail fast before expensive train/adoption steps.
  Current implementation note:
  implemented in `scripts/check_server_preflight.ps1` and wired into `scripts/daily_candidate_acceptance_for_server.ps1` and `scripts/daily_champion_challenger_v4_for_server.ps1`. Local and OCI validation confirmed the preflight writes `logs/ops/server_preflight/latest.json`, refreshes `logs/runtime_topology/latest.json` and `logs/ops/pointer_consistency/latest.json`, exits nonzero on dirty worktree, failed unit, stale pointer/state conditions, expected unit-file state mismatches, and missing required state DB paths, and causes both server batch entrypoints to fail closed before train/adoption work begins. Direct OCI validation also confirmed the running server now records paired-only expected states such as `autobot-paper-v4.service=disabled`, `autobot-paper-v4-challenger.service=disabled`, `autobot-paper-v4-paired.service=enabled`, spawn/promote timers `enabled`, replay services `disabled`, and required DB paths `data/state/live_candidate/live_state.db` plus `data/state/live_state.db`.

- [x] 06. Define and apply replay legacy cleanup policy
  Required references:
  [SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md)
  Done when:
  replay clone/service is explicitly classified as legacy and removed from target topology logic.
  Current implementation note:
  defined in [REPLAY_LEGACY_CLEANUP_POLICY_2026-03-25.md](/d:/MyApps/Autobot/docs/REPLAY_LEGACY_CLEANUP_POLICY_2026-03-25.md), reflected in `autobot/ops/runtime_topology_report.py` as `legacy_replay.classification=legacy_excluded_from_target_topology`, and applied in `scripts/install_server_daily_split_challenger_services.ps1` by disabling replay legacy services by default. Direct OCI validation on 2026-03-25 confirmed `autobot-paper-v4-replay.service` was stopped/disabled while the sibling replay clone remained only as a legacy path and no longer blocked or defined target topology logic.


### Phase 1: Logging And Matched Evaluation Foundation

- [x] 07. Add `opportunity_log` artifact skeleton across decision paths
  Required references:
  [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  Done when:
  a single opportunity unit can be reconstructed from logs.
  Current implementation note:
  implemented via `autobot/common/opportunity_log.py` plus strategy-level `StrategyOpportunityRecord` emission in `autobot/strategy/model_alpha_v1.py`, and wired into `autobot/backtest/engine.py`, `autobot/paper/engine.py`, and `autobot/live/model_alpha_runtime.py`. Local validation confirmed `opportunity_log.jsonl` is written for backtest/paper model-alpha runs and `logs/opportunity_log/<unit>/latest.jsonl` is written for live runtime decisions with stable `opportunity_id`, `feature_hash`, `selection_score`, `chosen_action`, and `skip_reason_code` skeleton fields. Direct OCI operational validation confirmed the active `autobot-live-alpha-candidate.service` is writing non-empty live `opportunity_log` artifacts and the paired paper lane is writing `opportunity_log.jsonl` inside `logs/paired_paper/runs/*/{champion,challenger}/runs/paper-*`.

- [x] 08. Add `counterfactual_action_log` or equivalent candidate-action capture
  Required references:
  [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  Done when:
  multiple candidate actions for the same opportunity are recoverable.
  Current implementation note:
  implemented by extending `StrategyOpportunityRecord` with `candidate_actions_json` and wiring `counterfactual_action_log.jsonl` emission through `autobot/common/opportunity_log.py` in `autobot/backtest/engine.py`, `autobot/paper/engine.py`, and `autobot/live/model_alpha_runtime.py`. Local validation confirmed multiple candidate execution actions for the same opportunity are recoverable from the artifact, and live/paper reflected state writes the new counterfactual log alongside `opportunity_log`. Direct OCI operational validation confirmed the active `autobot-live-alpha-candidate.service` is writing non-empty live `counterfactual_action_log` artifacts and the paired paper lane is writing `counterfactual_action_log.jsonl` inside `logs/paired_paper/runs/*/{champion,challenger}/runs/paper-*`.

- [x] 09. Implement minimal `paired paper` harness
  Required references:
  [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  Done when:
  champion and challenger can be compared on the same feed and same decision clock.
  Current implementation note:
  implemented in `autobot/paper/paired_reporting.py`, `autobot/paper/paired_runtime.py`, and `scripts/paired_paper_soak.ps1`, with long-running lifecycle support wired into `scripts/daily_champion_challenger_v4_for_server.ps1` and service install support added via `scripts/install_server_runtime_services.ps1` preset `paired_v4`. The target flow is now `spawn -> paired service start -> long-running one-feed fanout lane -> promote stops/flushes paired service -> paired promotion_decision -> promote verdict`. Direct OCI validation confirmed the dedicated unit `autobot-paper-v4-paired.service` is now `enabled` and `active/running` without immediately exiting, produces `logs/paired_paper/latest.json` with `mode=paired_paper_live_service_v1` and `source_mode=live_ws_fanout_service`, writes paired run directories under `logs/paired_paper/runs/paired-*`, and the real promotion entrypoint consumes paired paper artifacts from the operational path.

- [x] 10. Add `risk_budget_ledger`
  Required references:
  [RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  Done when:
  live sizing and skip reasons are traceable via a budget artifact.
  Current implementation note:
  implemented in `autobot/live/risk_budget_ledger.py` and wired through `autobot/live/model_alpha_runtime.py` plus `autobot/live/model_alpha_runtime_execute.py` so every persisted live strategy intent now appends a machine-readable budget ledger entry and refreshes a latest summary artifact. The ledger initialization path was then hardened so service restarts preserve prior ledger history and rebuild the latest summary if the summary artifact is missing, without changing the existing online risk breaker baseline-reset behavior. Local validation confirmed the new ledger path is created and populated through `tests/test_live_risk_budget_ledger.py`, `tests/test_live_small_account.py`, `tests/test_live_admissibility.py`, targeted `tests/test_live_model_alpha_runtime.py` coverage for live shadow and lookup-failure paths, `tests/test_live_breakers.py` coverage confirming the online risk breaker clear-baseline behavior still holds, and additional ledger tests covering conservative open bid order exposure plus uncertainty-weighted exposure propagation from `model_alpha_v1` strategy metadata. Direct OCI operational validation confirmed the active `autobot-live-alpha-candidate.service` writes persistent `logs/risk_budget_ledger/autobot_live_alpha_candidate_service/latest.jsonl` and `latest.json`, that ledger history survives service restart without truncation, and that the current operating path continues to emit live sizing/skip-reason ledger entries. During the post-deploy observation window no fresh candidate decision arrived to directly witness the newly added open-order exposure in live output, so that addition was validated locally while the restart-preservation change was validated directly on the OCI service. Open issue retained intentionally: the current active predictor/runtime contract does not export a row-level uncertainty scalar, so `uncertainty_weighted_exposure` may remain `null` until a later predictor contract upgrade provides that value.

- [x] 11. Add minimal white-box `portfolio risk budget engine`
  Required references:
  [RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  Done when:
  sizing is no longer purely local per-trade logic.
  Current implementation note:
  implemented in `autobot/risk/portfolio_budget.py` and wired into `autobot/live/model_alpha_runtime_execute.py` so bid entry sizing now passes through a white-box portfolio budget layer after trade-action, size-ladder, and operational-overlay sizing but before trade-gate/admissibility/execution submission. The engine now computes current cash-at-risk from open positions plus open bid orders, applies gross and cluster budget ceilings, available-quote caps, and a recent-loss-streak haircut, then emits explicit `risk_reason_codes`, `position_budget_fraction`, `max_notional_quote`, and cluster budget remaining values through the live meta payload and risk budget ledger. Local validation confirmed the shared live path clamps notional against existing portfolio exposure via `tests/test_portfolio_budget.py`, `tests/test_live_model_alpha_runtime.py`, `tests/test_live_risk_budget_ledger.py`, and preserves the online breaker clear-baseline behavior via `tests/test_live_breakers.py`. Direct OCI operational validation confirmed the active `autobot-live-alpha-candidate.service` produced a fresh `risk_budget_ledger` entry with `portfolio_budget`-driven fields and reason codes such as `PORTFOLIO_AVAILABLE_QUOTE_EXHAUSTED`, `PORTFOLIO_AVAILABLE_QUOTE_CLAMP`, and `PORTFOLIO_RECENT_LOSS_STREAK_HAIRCUT`, proving the current running path is no longer sizing purely from local per-trade logic. Open limitation retained explicitly: until a later predictor contract exports row-level uncertainty, the portfolio budget confidence haircut may remain a no-op in live entries where `uncertainty` is null.


### Phase 2: Stronger Predictor Baseline

- [x] 12. Add multi-horizon label bundle
  Required references:
  [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  Done when:
  `features_v4` carries a machine-readable multi-horizon label contract instead of only a single-horizon target.
  Current implementation note:
  implemented in `autobot/features/labeling_v2_crypto_cs.py`, `autobot/features/labeling_v3_crypto_cs.py`, and `autobot/features/pipeline_v4.py` by preserving the existing `label_v2` compatibility path while adding an explicit `label_v3` residualized multi-horizon bundle. `features_v4` can now build either `label_set=v2` or `label_set=v3`; `v2` continues to emit canonical raw multi-horizon `y_reg_net_h3/h6/h12/h24` and `y_rank_cs_h3/h6/h12/h24` columns plus compatibility aliases `y_reg_net_12`, `y_rank_cs_12`, and `y_cls_topq_12`, while `v3` emits a machine-readable residualized target bundle with `y_reg_resid_btc_h*`, `y_reg_resid_eth_h*`, `y_reg_resid_leader_h*`, `y_rank_resid_leader_h*`, and `y_cls_resid_leader_topq_h*` columns. The contract is now reflected in `label_spec.json` through `label_set_version`, `label_bundle_version`, `multi_horizon_bars`, `training_default_columns`, and canonical column-family lists, and `validate_features_dataset_v4`, `features_stats_v4`, `train_v4_core`, `train_v4_crypto_cs`, persisted trainer `train_config.yaml`, standard `autobot.cli` `--label-set v3` parsing, and the local `autobot_center.ps1` v4 train/build wizard path now consume or expose that machine-readable contract rather than hard-coded label names alone. Local validation confirmed both the compatibility and canonical residualized paths via `tests/test_cli_alpha_shortcuts.py`, `tests/test_labeling_v2_crypto_cs.py`, `tests/test_labeling_v3_crypto_cs.py`, `tests/test_pipeline_v4_label_v2.py`, `tests/test_train_v4_crypto_cs.py`, `tests/test_train_v4_crypto_cs_label_v3.py`, and `tests/test_model_compare_v4.py`. Deferred explicitly to later items: actual multi-task consumption of the extra horizons belongs to `#13 Implement train_v5_panel_ensemble`, and uncertainty-aware predictor outputs that build on the richer `label_v3` contract belong to `#14 Export uncertainty-aware predictor contract`.

- [x] 13. Implement `train_v5_panel_ensemble`
  Required references:
  [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  Done when:
  a registry-saving trainer exists that learns classifier / ranker / multi-horizon regression heads together and stacks them into a final panel score with uncertainty metadata.
  Current implementation note:
  implemented in `autobot/models/train_v5_panel_ensemble.py` and wired into `autobot/cli.py` plus `autobot/models/__init__.py` as trainer `v5_panel_ensemble`. The new trainer reuses the current `features_v4` backbone and `label_v3` residualized contract, fits classifier / ranker / residualized regression heads together, builds walk-forward OOF base predictions, fits a stacked logistic meta-model, derives isotonic selection calibration from those OOF rows, and persists a registry model bundle with `panel_ensemble_contract.json` describing the standardized `final_rank_score` and uncertainty field. The trainer also writes the existing v4-style support and governance backbone artifacts through the current registry/persistence path so the new family still produces `walk_forward_report.json`, `selection_recommendations.json`, `selection_policy.json`, `selection_calibration.json`, `promotion_decision.json`, `trainer_research_evidence.json`, and `decision_surface.json` under a `train_v5_panel_ensemble` run directory. Standard entrypoints now expose this trainer through `autobot.cli model train --trainer v5_panel_ensemble --feature-set v4 --label-set v3`, and the local `autobot_center.ps1` wizard can invoke the same path. Local validation confirmed parser wiring, ensemble contract persistence, and regression compatibility via `tests/test_cli_alpha_shortcuts.py`, `tests/test_train_v5_panel_ensemble.py`, `tests/test_train_v4_crypto_cs.py`, `tests/test_model_compare_v4.py`, and `tests/test_pipeline_v4_label_v2.py`. Deferred explicitly to later items: `#14 Export uncertainty-aware predictor contract` still owns runtime-side exposure of `score_mean`, `score_std`, and `score_lcb` to the predictor/runtime contract even though `#13` now trains and stores the ensemble-side score/uncertainty backbone.

- [x] 14. Export uncertainty-aware predictor contract
  Required references:
  [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  Done when:
  the predictor/runtime boundary can expose `score_mean`, `score_std`, and `score_lcb` instead of only a single scalar score.
  Current implementation note:
  implemented by extending `autobot/models/predictor.py` with `predict_score_contract()` and uncertainty-aware `predict_uncertainty()` support, wiring `autobot/strategy/model_alpha_v1.py` to append `score_mean`, `score_std`, and `score_lcb` to scored runtime rows, and updating `autobot/models/train_v5_panel_ensemble.py` to persist `predictor_contract.json` alongside the panel ensemble contract and train config. The new `v5_panel_ensemble` model bundle now carries runtime-callable score mean and uncertainty behavior, `ModelPredictor` can load the exported `predictor_contract`, and downstream paths that already read uncertainty-like fields such as the live risk budget ledger can now consume real `score_std` from the trained predictor path rather than only ad hoc row metadata. Local validation confirmed the new contract via `tests/test_predictor_contract.py`, `tests/test_train_v5_panel_ensemble.py`, `tests/test_cli_alpha_shortcuts.py`, `tests/test_train_v4_crypto_cs.py`, `tests/test_model_compare_v4.py`, and `tests/test_pipeline_v4_label_v2.py`. Deferred explicitly: using `score_lcb` as the active selection/policy comparison criterion remains a later policy change beyond the raw predictor contract export itself.


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
