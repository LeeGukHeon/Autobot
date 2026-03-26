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
  implemented in `autobot/ops/data_contract_registry.py`, tested locally and on the OCI server, wired into `scripts/candidate_acceptance.ps1`, committed, pushed, server-pulled, and reflected as `data/_meta/data_contract_registry.json` on the OCI server. The registry now goes beyond raw/micro/feature roots and carries blueprint-aligned `live` and `runtime` contract layers as well, including explicit `validation_status`, `retention_class`, and `coverage_window` metadata plus source linkage from raw WS and feature datasets into the live feature plane and runtime state DB entries.

- [x] 02. Make `features_v4` validation artifact mandatory in the operational flow
  Required references:
  [DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md)
  Done when:
  `data/features/features_v4/_meta/validate_report.json` is generated or enforced, and acceptance can fail on its absence or invalid status.
  Current implementation note:
  candidate acceptance now enforces both `features validate` and a sampled `live_feature_parity_report` in the operational flow, so the gate fails not only on missing/invalid `validate_report` but also when the live feature provider cannot reconstruct sampled `features_v4` rows without missing-column hard gates or row-level parity mismatches. The main operational wrappers still route through `candidate_acceptance.ps1`, server-side stale partitions causing the prior `ctrend_v1_rsi_14` schema failure were archived, and the acceptance flow now records a dedicated `features_live_parity` step alongside `features_validate` to cover the blueprint-required `missing feature parity pass` condition rather than only `fail_files/schema_ok/leakage_smoke`.

- [x] 03. Add a machine-readable `runtime topology report`
  Required references:
  [SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md)
  Done when:
  current server lane/unit/pointer/runtime state can be summarized by one artifact without manual SSH forensics.
  Current implementation note:
  implemented in `autobot/ops/runtime_topology_report.py`, tested locally and on the OCI server, and reflected as `logs/runtime_topology/latest.json` on the OCI server. Direct OCI validation confirmed the artifact now includes actual `systemd` service and timer snapshots, git HEAD and dirty worktree state, sibling replay-like path detection, paired-paper topology facts such as `autobot-paper-v4-paired.service`, replay exclusion status, and an explicit `target_topology` plus `topology_health` contract so current lane/unit/pointer/runtime state can be summarized and compared against the intended two-lane architecture without separate manual SSH forensics.

- [x] 04. Add a machine-readable `pointer consistency report`
  Required references:
  [SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md)
  [FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md](/d:/MyApps/Autobot/docs/FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md)
  Done when:
  invalid steady-state pointer combinations are detectable by artifact and script.
  Current implementation note:
  implemented in `autobot/ops/pointer_consistency_report.py` and `scripts/check_pointer_consistency.ps1`, tested locally and on the OCI server, and reflected as `logs/ops/pointer_consistency/latest.json` on the OCI server. The checker now covers not only family/global pointer alignment and candidate-unit activity but also the operating-contract-required `current_state.json` handoff fields such as `candidate_run_id`, `champion_run_id_at_start`, `started_ts_ms`, and `lane_mode`, so challenger handoff state that is present but structurally incomplete is emitted as explicit machine-readable violations rather than being treated as loosely acceptable.

- [x] 05. Add pre-flight checks for server units, pointer resolvability, and dirty worktree state
  Required references:
  [SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/SERVER_OPERATIONS_AND_DEPLOYMENT_AUTOMATION_BLUEPRINT_2026-03-25.md)
  Done when:
  batch scripts can fail fast before expensive train/adoption steps.
  Current implementation note:
  implemented in `scripts/check_server_preflight.ps1` and wired into `scripts/daily_candidate_acceptance_for_server.ps1` and `scripts/daily_champion_challenger_v4_for_server.ps1`. Local and OCI validation confirmed the preflight writes `logs/ops/server_preflight/latest.json`, refreshes `logs/runtime_topology/latest.json` and `logs/ops/pointer_consistency/latest.json`, exits nonzero on dirty worktree, failed unit, stale pointer/state conditions, expected unit-file state mismatches, and missing required state DB paths, and causes both server batch entrypoints to fail closed before train/adoption work begins. The preflight now also escalates relevant `pointer_consistency_report` and `runtime_topology_report` violations into its own blocking checks rather than treating those refreshed reports as informational-only side artifacts, so structurally invalid challenger handoff or topology drift is fail-closed without relying on duplicate bespoke checks in each wrapper.

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
  implemented via `autobot/common/opportunity_log.py` plus strategy-level `StrategyOpportunityRecord` emission in `autobot/strategy/model_alpha_v1.py`, and wired into `autobot/backtest/engine.py`, `autobot/paper/engine.py`, and `autobot/live/model_alpha_runtime.py`. The log contract now goes beyond a pure decision-time skeleton: backtest and paper runs backfill `realized_outcome_json` from `trades.csv` using intent-linked realized outcome summaries, and live runtime backfills the same field from `trade_journal` after journal recomputation so current logs can carry both the decision slice and the realized outcome slice for the same opportunity. Local validation confirmed `opportunity_log.jsonl` is written for backtest/paper model-alpha runs and `logs/opportunity_log/<unit>/latest.jsonl` is written for live runtime decisions with stable `opportunity_id`, `feature_hash`, `selection_score`, `chosen_action`, `skip_reason_code`, and realized outcome back-links where outcome evidence exists.

- [x] 08. Add `counterfactual_action_log` or equivalent candidate-action capture
  Required references:
  [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  Done when:
  multiple candidate actions for the same opportunity are recoverable.
  Current implementation note:
  implemented by extending `StrategyOpportunityRecord` with `candidate_actions_json` and wiring `counterfactual_action_log.jsonl` emission through `autobot/common/opportunity_log.py` in `autobot/backtest/engine.py`, `autobot/paper/engine.py`, and `autobot/live/model_alpha_runtime.py`. The counterfactual log now preserves explicit action payloads, propensities, chosen/no-trade behavior policy fields, and the same intent-linked `realized_outcome_json` backfill used by `opportunity_log`, so the chosen behavior slice and its observed outcome can be read from the same machine-readable family rather than being split between disconnected artifacts.

- [x] 09. Implement minimal `paired paper` harness
  Required references:
  [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  Done when:
  champion and challenger can be compared on the same feed and same decision clock.
  Current implementation note:
  implemented in `autobot/paper/paired_reporting.py`, `autobot/paper/paired_runtime.py`, and `scripts/paired_paper_soak.ps1`, with long-running lifecycle support wired into `scripts/daily_champion_challenger_v4_for_server.ps1` and service install support added via `scripts/install_server_runtime_services.ps1` preset `paired_v4`. The target flow is now `spawn -> paired service start -> long-running one-feed fanout lane -> promote stops/flushes paired service -> paired promotion_decision -> promote verdict`. The paired report no longer treats matched PnL as a plain aggregate run-summary delta only; it now derives matched opportunity realized PnL coverage from intent-linked trade outcomes, while still reporting aggregate realized PnL separately, so matched fill/slippage/no-trade deltas and matched PnL all come from the same opportunity-linked evidence family rather than mixing matched and aggregate semantics.

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
  implemented in `autobot/risk/portfolio_budget.py` and wired into `autobot/live/model_alpha_runtime_execute.py` so bid entry sizing now passes through a white-box portfolio budget layer after trade-action, size-ladder, and operational-overlay sizing but before trade-gate/admissibility/execution submission. The engine now computes current cash-at-risk from open positions plus open bid orders, applies gross and cluster budget ceilings, available-quote caps, recent-loss-streak haircut, and when explicitly available from the predictor/runtime contract also applies `expected_return`, `expected_es`, `tradability`, and `alpha_lcb` haircuts instead of only local exposure heuristics. It emits explicit `risk_reason_codes`, `position_budget_fraction`, `max_notional_quote`, cluster budget remaining values, and the new risk-input fields through the live meta payload and risk budget ledger. Local validation confirmed the shared live path clamps notional against existing portfolio exposure and preserves the online breaker clear-baseline behavior via `tests/test_portfolio_budget.py`, `tests/test_live_model_alpha_runtime.py`, `tests/test_live_risk_budget_ledger.py`, and `tests/test_live_breakers.py`. Current explicit limitation retained: these richer haircut inputs are only active when the runtime strategy meta actually carries the newer `final_*` predictor outputs, so older v4/v3-style runs remain backward-compatible rather than being heuristically over-interpreted as having true tradability or alpha-LCB estimates.


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
  implemented in `autobot/models/train_v5_panel_ensemble.py` and wired into `autobot/cli.py` plus `autobot/models/__init__.py` as trainer `v5_panel_ensemble`. The trainer reuses the current `features_v4` backbone and `label_v3` residualized contract, fits classifier / ranker / residualized regression heads together, builds walk-forward OOF base predictions, fits a stacked logistic meta-model, derives isotonic selection calibration from those OOF rows, and persists a registry model bundle with `panel_ensemble_contract.json` describing the standardized `final_rank_score`, uncertainty contract, and a machine-readable distributional contract for multi-horizon `q10/q50/q90` return quantiles plus expected-shortfall proxies derived from walk-forward regression members. The exported predictor/panel contracts now also standardize `final_expected_return`, `final_expected_es`, `final_tradability`, and `final_alpha_lcb` as canonical runtime fields for the primary horizon, so downstream runtime layers no longer have to infer those from ad hoc distributional payloads alone. The trainer also writes the existing v4-style support and governance backbone artifacts through the current registry/persistence path, including `walk_forward_report.json`, `selection_recommendations.json`, `selection_policy.json`, `selection_calibration.json`, `promotion_decision.json`, `trainer_research_evidence.json`, `decision_surface.json`, and the family-level experiment ledger plus latest experiment summary. Standard entrypoints now expose this trainer through `autobot.cli model train --trainer v5_panel_ensemble --feature-set v4 --label-set v3`, and the local `autobot_center.ps1` wizard can invoke the same path. Local validation confirmed parser wiring, ensemble contract persistence, extended predictor-contract persistence, and regression compatibility via `tests/test_cli_alpha_shortcuts.py`, `tests/test_train_v5_panel_ensemble.py`, `tests/test_predictor_contract.py`, `tests/test_train_v4_crypto_cs.py`, `tests/test_model_compare_v4.py`, and `tests/test_pipeline_v4_label_v2.py`. Current OCI audit note on 2026-03-26: no server-origin `train_v5_panel_ensemble` family run has yet been materialized in `models/registry/`, so implementation correctness is validated by local/server tests and artifact contract generation rather than by an already-produced automated server run. Remaining limitation retained explicitly: the trainer still uses one primary classifier/ranker lane plus multi-horizon regression heads rather than a fully separate horizon-specific classifier/ranker family for every horizon, so a deeper trainer redesign would still be needed to claim the complete north-star multi-task architecture from the broader blueprint.

- [x] 14. Export uncertainty-aware predictor contract
  Required references:
  [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  Done when:
  the predictor/runtime boundary can expose `score_mean`, `score_std`, and `score_lcb` instead of only a single scalar score.
  Current implementation note:
  implemented by extending `autobot/models/predictor.py` with `predict_score_contract()`, uncertainty-aware `predict_uncertainty()`, and distributional predictor accessors, wiring `autobot/strategy/model_alpha_v1.py` to append `final_rank_score`, `final_uncertainty`, `score_mean`, `score_std`, and `score_lcb` to scored runtime rows, and updating `autobot/models/train_v5_panel_ensemble.py` to persist `predictor_contract.json` alongside the panel ensemble contract and train config. The exported selection contract now supports score-source aware behavior so the trainer can compare `score_mean` versus `score_lcb` at walk-forward selection-policy time, persist the chosen `score_source` in `selection_policy.json`, and let `ModelPredictor.predict_selection_scores()` calibrate the matching source-specific score path. The `v5_panel_ensemble` model bundle now carries runtime-callable score mean, canonical `final_rank_score` alias, uncertainty behavior, and machine-readable multi-horizon quantile / expected-shortfall proxy metadata; `ModelPredictor` can load the exported `predictor_contract`, and downstream paths that already read uncertainty-like fields such as the live risk budget ledger can now consume real `score_std` from the trained predictor path rather than only ad hoc row metadata. Local validation confirmed the new contract via `tests/test_predictor_contract.py`, `tests/test_selection_contract_score_source.py`, `tests/test_train_v5_panel_ensemble.py`, `tests/test_cli_alpha_shortcuts.py`, `tests/test_train_v4_crypto_cs.py`, `tests/test_model_compare_v4.py`, and `tests/test_pipeline_v4_label_v2.py`. Current OCI audit note on 2026-03-26: the server does not yet have an automatically produced `v5_panel_ensemble` run to exercise this contract in the scheduled lane, so current server validation remains contract-level and test-level rather than scheduled-lane runtime adoption.


### Phase 3: Stronger Risk And Runtime Control

- [x] 15. Introduce typed breaker taxonomy
  Required references:
  [RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  Current implementation note:
  implemented by adding `autobot/live/breaker_taxonomy.py` as the machine-readable SSOT for live breaker reason typing and recovery semantics and wiring it through `autobot/live/breakers.py`, `autobot/live/state_store.py`, `autobot/live/rollout.py`, `autobot/live/daemon.py`, `autobot/live/model_alpha_runtime.py`, `autobot/ops/runtime_topology_report.py`, `autobot/ops/live_execution_override_audit.py`, and `autobot/dashboard_server.py`. The live breaker report, persisted breaker state/events, rollout status payload, runtime-topology snapshot, live execution override audit, and dashboard live DB summary now expose `taxonomy_version`, per-reason typed metadata, `reason_types`, `primary_reason_type`, `reason_type_counts`, and `clear_policies`, so current active reasons are explicitly classified into blueprint-aligned families such as `INFRA`, `STATE_INTEGRITY`, `STATISTICAL_RISK`, and `OPERATIONAL_POLICY`. Runtime recovery paths that clear stale runtime/pointer/rollout/online-risk reasons now select clearable codes through taxonomy `clear_policy` rather than only hard-coded reason lists. Local validation confirmed the contract through `tests/test_live_breakers.py`, `tests/test_live_rollout.py`, `tests/test_live_state_store.py`, `tests/test_runtime_topology_report.py`, `tests/test_live_execution_override_audit.py`, plus targeted daemon/risk/runtime regression coverage. Deferred explicitly to later items: stronger statistical monitor families and new confidence-sequence evidence generation belong to `#16 Add confidence-sequence based online monitors`, while protective liquidation policy families remain part of `#17 Add execution-calibrated protective liquidation policy`.

- [x] 16. Add confidence-sequence based online monitors
  Required references:
  [RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  Current implementation note:
  implemented by adding `autobot/risk/confidence_monitor.py` and extending `autobot/models/execution_risk_control.py`, `autobot/live/model_alpha_runtime_execute.py`, `autobot/live/model_alpha_runtime.py`, `autobot/live/breaker_taxonomy.py`, and `autobot/live/risk_budget_ledger.py`. The current running live path now computes and writes `logs/live_risk_confidence_sequence/<unit>/latest.json` as a machine-readable monitor artifact containing time-uniform rate-style confidence monitors for nonpositive return rate, severe loss rate, execution miss rate, and expected-vs-realized edge-gap breach rate, plus an explicit feature-divergence monitor slot. The runtime merges triggered confidence-sequence reasons into the online halt contract, writes the artifact on startup and during subsequent runtime activity, and classifies the new monitor reason codes through the typed breaker taxonomy so live breaker semantics remain machine-readable. Local validation confirmed the contract through `tests/test_live_confidence_monitor.py`, `tests/test_execution_risk_control.py`, `tests/test_live_breakers.py`, targeted `tests/test_live_model_alpha_runtime.py`, `tests/test_live_risk_budget_ledger.py`, and related predictor/runtime regression tests. Direct OCI operational validation confirmed the active `autobot-live-alpha-candidate.service` now writes `logs/live_risk_confidence_sequence/autobot_live_alpha_candidate_service/latest.json` with `artifact_version=1`, real current `run_id`, active triggered reason codes such as `RISK_CONTROL_NONPOSITIVE_RATE_CS_BREACH`, `RISK_CONTROL_SEVERE_LOSS_RATE_CS_BREACH`, `EXECUTION_MISS_RATE_CS_BREACH`, and `RISK_CONTROL_EDGE_GAP_CS_BREACH`, and the expected monitor families in the reflected artifact. Open limitation retained explicitly: the `paper_live_feature_divergence_rate` slot is present in the contract but currently remains `source_unavailable` because the current runtime path does not yet produce a dedicated paper/live feature-divergence source artifact.

- [ ] 17. Add execution-calibrated protective liquidation policy
  Required references:
  [RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  Current implementation note:
  implemented in `autobot/risk/liquidation_policy.py` and wired into `autobot/risk/live_risk_manager.py` so protective exits now resolve an explicit liquidation policy tier with execution semantics separated from normal alpha-entry execution. The policy currently emits machine-readable tier decisions such as `soft_exit`, `normal_protective`, `urgent_defensive`, and `emergency_flatten`, uses stop-breach magnitude, elapsed trigger time, current spread/depth/trade-imbalance plus breaker action to choose `ord_type`, `time_in_force`, `price_mode`, timeout, and replace budget, and writes a latest `protective_liquidation_report.json` beside the active state DB whenever the protective path actually submits or replaces an order. Local validation confirmed the behavior through `tests/test_live_risk_manager.py`, including `best/ioc` emergency flatten for severe stop-breach conditions and explicit report emission on the local state path. Direct OCI validation has now produced reflected candidate artifacts at `data/state/live_candidate/protective_liquidation_report.json`, first for `KRW-ETH` and then again post-fix for `KRW-SOL`, proving the current OCI path exercised the protective liquidation policy in practice. The `KRW-ETH` event showed the timeline root cause for the active canary breaker: `REPEATED_REPLACE_REJECTS` was armed during pre-fill protective replace attempts, not after the final fill, and state/journal updates continued afterward because `HALT_NEW_INTENTS` still allows protective-order management and runtime state refresh. The replace-path incompatibility was then patched by making the direct REST execution gateway fall back from `cancel_and_new` to `cancel -> fresh submit` for `ioc/fok` protective replacements, because Upbit rejected `POST /v1/orders/cancel_and_new` with `status=403 error=not_supported_ord_type` for that protective order lineage. The post-fix `KRW-SOL` live event completed without re-arming the breaker, but it was a `phase=submit` / `replace_attempt=0` protective liquidation and therefore did not yet exercise the new live replace fallback path itself. Keep this item unchecked until a post-fix live protective replace event is observed on the current OCI path without re-arming the repeated replace-reject breaker. This is an explicit operational blocker discovered from the live artifact, not silent scope reduction.


### Phase 4: Execution Learning And OPE

- [x] 18. Add action propensity logging
  Required references:
  [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  Current implementation note:
  implemented by extending `autobot/backtest/strategy_adapter.py`, `autobot/common/opportunity_log.py`, and `autobot/strategy/model_alpha_v1.py` so the shared opportunity/counterfactual logging contract now records explicit behavior-policy action propensities rather than only candidate utilities. `StrategyOpportunityRecord` now carries `decision_outcome`, `chosen_action`, `chosen_action_propensity`, `no_trade_action_propensity`, and machine-readable behavior-policy identity/support fields, while `candidate_actions_json` and `counterfactual_action_log` rows now include per-action `propensity` values plus an explicit `NO_TRADE` action. The current `model_alpha_v1` path logs the full deterministic execution-stage action set for each opportunity, including skip/no-trade cases, so backtest/paper/live artifacts can distinguish the realized outcome (`intent_created` vs `skip`) from the logged behavior action and its propensity support. Local validation confirmed the contract through `tests/test_backtest_model_alpha_integration.py`, `tests/test_paper_engine_model_alpha_integration.py`, `tests/test_paired_paper_reporting.py`, `tests/test_paired_paper_runtime.py`, and targeted `tests/test_live_model_alpha_runtime.py`; direct OCI validation confirmed the active candidate live path now writes fresh `opportunity_log` and `counterfactual_action_log` rows with `chosen_action_propensity`, `no_trade_action_propensity`, and explicit `NO_TRADE` candidate actions. Open limitation retained explicitly: the current live `opportunity_log` row captures the behavior-policy action/propensity slice before later runtime veto layers such as portfolio-budget or execution-gate skips, so a downstream live no-trade veto is not yet backlinked into that same opportunity row. Deferred explicitly to later items: `#19 Build execution twin` and `#20 Add DR-OPE` consume this new logged propensity contract, while the current policy support remains `deterministic_no_exploration` until later methodology work introduces broader safe-lane action support.

- [x] 19. Build `execution twin`
  Required references:
  [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  [RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  Current implementation note:
  implemented in `autobot/models/execution_twin.py` and wired through `autobot/models/live_execution_policy.py` plus `autobot/live/execution_policy_refresh.py`. The current refresh path now emits a machine-readable `execution_twin` contract alongside the existing fill and miss-cost models, using private-lane derived `execution_attempts` rows enriched with linked `trade_journal` and `order_lineage` evidence to estimate state/action-conditioned `P(first_fill <= t)`, `P(full_fill <= t)`, partial-fill probability, replace probability, cancel probability, mean time-to-first-fill, mean time-to-full-fill, tail execution downside summaries, and the stronger blueprint-aligned survival / hazard / queue-reactive calibration slice. The twin now exposes `model_form=hazard_survival_queue_reactive_v1`, explicit first/full-fill survival curves with interval hazard probabilities, queue-reactive action and price-mode stats keyed by spread/depth/coverage/quality buckets, and richer chain/fill-fraction summaries. The live submission path was also tightened so execution attempts preserve `operational_overlay.micro_quality_score` even when `micro_state` omits it, allowing the current live path to populate the new queue-reactive buckets rather than collapsing quality to `null`. Local validation confirmed the connected slice through `tests/test_live_execution_policy.py`, `tests/test_live_execution_policy_refresh.py`, `tests/test_data_contract_registry.py`, `tests/test_runtime_topology_report.py`, `tests/test_check_server_preflight.py`, `tests/test_paired_paper_reporting.py`, `tests/test_paired_paper_runtime.py`, `tests/test_portfolio_budget.py`, `tests/test_live_risk_budget_ledger.py`, `tests/test_predictor_contract.py`, and `tests/test_train_v5_panel_ensemble.py`. Open limitation retained explicitly: the current queue-reactive slice is still bucketed by spread/depth/coverage/quality proxies rather than a full per-price queue-state simulator, so a stronger order-queue-conditioned calibration layer would still belong to later deeper methodology work.

- [x] 20. Add `DR-OPE`
  Required references:
  [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)
  Current implementation note:
  implemented by adding `autobot/models/offpolicy_evaluation.py` plus `autobot.cli model ope-execution`, and wiring automatic `execution_ope_report.json` generation into `autobot/backtest/engine.py` and `autobot/paper/engine.py` after realized outcomes are backfilled into `opportunity_log` and `counterfactual_action_log`. The DR-OPE slice now consumes the existing behavior-policy propensity logging from `#18`, uses direct predicted-utility baselines from counterfactual action payloads with `execution_twin`-based fallback estimates where needed, and emits per-policy `dm / ips / dr` estimates, support-rate diagnostics, effective sample size, and action-availability coverage for logged, greedy-utility, no-trade, and deterministic action policies. The paired-paper matched report remains a separate artifact, but backtest/paper run summaries now expose `execution_ope_report_path` so execution-policy candidate evaluation can be filtered before full live exposure using a machine-readable OPE artifact rather than only aggregate run summaries. Local validation confirmed the connected slice through `tests/test_offpolicy_evaluation.py`, `tests/test_backtest_model_alpha_integration.py`, `tests/test_paper_engine_model_alpha_integration.py`, `tests/test_paired_paper_reporting.py`, `tests/test_paired_paper_runtime.py`, `tests/test_live_model_alpha_runtime.py`, `tests/test_portfolio_budget.py`, `tests/test_predictor_contract.py`, and `tests/test_train_v5_panel_ensemble.py`. Open limitation retained explicitly: because current behavior support remains `deterministic_no_exploration`, off-support target actions still fall back to the direct-model term rather than receiving a full unbiased importance correction, so broader safe-lane exploration remains future methodology work rather than part of this item.


### Phase 5: Advanced Data Shapes

- [x] 21. Add `candles_second_v1`
  Required references:
  [DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md)

  Current implementation note:
  implemented by extending the existing candle collection stack rather than creating a disconnected side path. `autobot/upbit/public.py` now exposes the official second-candle REST endpoint, `autobot/data/collect/upbit_candles_client.py` now supports both `1s` and minute candle pagination through one range-fetch contract, `autobot/data/schema_contract.py` now recognizes `1s`, and `autobot/data/collect/plan_candles.py` now supports blueprint-aligned second-layer planning with a separate `market_source_dataset` plus explicit `1s` recent-window caps so `data/parquet/candles_second_v1` can be built in parallel without depending on preexisting second-candle inventory. `autobot/data/collect/candles_collector.py` and `autobot.cli collect candles` now preserve separate second-layer collect/validate artifact paths such as `candle_second_collect_report.json` and `candle_second_validate_report.json` instead of overwriting the minute-layer reports, while `autobot/data/ingest_csv_to_parquet.py` now treats `1s` candle gaps as sparse-by-construction rather than integrity failures because Upbit does not emit a second candle when no trade occurs in that second. Local validation covered the connected slice through `tests/test_candle_plan.py`, `tests/test_upbit_candles_client.py`, `tests/test_upbit_public_candles.py`, and `tests/test_candle_collect_validate.py`, including second-layer planning from a separate market-source dataset, endpoint dispatch, sparse-second validation, and dataset-specific artifact naming. Direct OCI validation then confirmed the live server can generate a `1s` smoke plan with `market_source_dataset=candles_api_v1`, collect real second candles into `data/parquet/candles_second_v1`, emit `data/collect/_meta/candle_second_collect_report.json` and `data/collect/_meta/candle_second_validate_report.json`, write manifest rows with `source=upbit_api_seconds` and `tf=1s`, and expose `candles_second_v1` through the machine-readable data contract registry.

- [x] 22. Add `ws_candle_v1`
  Required references:
  [DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md)

  Current implementation note:
  implemented as a separate websocket-candle parquet layer rather than folding candle streams into the raw `ws_public` trade/orderbook store. `autobot/data/collect/plan_ws_candles.py` now creates machine-readable `ws_candle_plan.json` artifacts with blueprint-aligned market selection, explicit websocket candle timeframe sets, and snapshot/realtime subscription flags; `autobot/data/collect/ws_candle_collector.py` now subscribes to official Upbit websocket candle stream types such as `candle.1s`, `candle.1m`, `candle.3m`, `candle.5m`, `candle.10m`, `candle.15m`, `candle.30m`, `candle.60m`, and `candle.240m`, normalizes the payload into the shared candle schema, keeps the latest update per `(market, tf, ts_ms)` because the same candle can be re-sent multiple times, and persists the result into `data/parquet/ws_candle_v1`. `autobot/data/collect/validate_ws_candles.py` now validates the resulting dataset with websocket-candle sparse semantics instead of treating no-trade intervals as integrity failures, and `autobot.cli` exposes `collect plan-ws-candles` plus `collect ws-candles` so the dataset can be planned, collected, and validated without disturbing the existing raw `ws_public` pipeline. Local validation covered `tests/test_schema_contract.py`, `tests/test_ws_candle_plan.py`, `tests/test_ws_candle_collect_validate.py`, `tests/test_ws_public_plan.py`, `tests/test_ws_public_collect_validate.py`, and `tests/test_ws_public_ops.py`, including latest-update dedupe and sparse validation semantics. Direct OCI validation then confirmed the current server can generate `data/collect/_meta/ws_candle_plan_smoke.json`, collect real websocket candle traffic into `data/parquet/ws_candle_v1`, emit `data/collect/_meta/ws_candle_collect_report.json` plus `data/collect/_meta/ws_candle_validate_report.json`, write manifest rows with `source=upbit_ws_candle` for both `1s` and `1m`, and expose `ws_candle_v1` through the machine-readable data contract registry.

- [x] 23. Add `lob30_v1`
  Required references:
  [DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md)

  Current implementation note:
  implemented as a dedicated 30-level orderbook parquet layer instead of relying on the raw `ws_public` JSONL store. `autobot/data/collect/plan_lob30.py` now builds machine-readable `lob30_plan.json` artifacts with blueprint-aligned market selection, explicit `requested_depth=30`, `orderbook_level=0`, and request codes such as `KRW-BTC.30`; `autobot/data/collect/lob30_collector.py` now subscribes to the official Upbit websocket orderbook stream with 30-depth request codes, normalizes each snapshot into a fixed 30-level row, keeps the latest row per `(market, ts_ms)`, and persists the result into market/date-partitioned `data/parquet/lob30_v1`. `autobot/data/collect/lob30_writer.py`, `autobot/data/collect/lob30_manifest.py`, and `autobot/data/collect/validate_lob30.py` now provide the storage contract, build manifest, and validator for the compressed certification store, including checks that `requested_depth=30`, `levels_present>=30`, `level=0`, top-of-book is not crossed, and level prices are monotonic in the expected ask/bid directions. `autobot.cli` now exposes `collect plan-lob30` and `collect lob30` so the layer can be planned, collected, validated, and reflected separately from the existing raw websocket pipeline. Local validation covered `tests/test_lob30_plan.py`, `tests/test_lob30_collect_validate.py`, plus the adjacent `ws_public` regression suite. Direct OCI validation then confirmed the server can generate `data/collect/_meta/lob30_plan_smoke.json`, collect live 30-level orderbook snapshots into `data/parquet/lob30_v1`, emit `data/collect/_meta/lob30_collect_report.json` plus `data/collect/_meta/lob30_validate_report.json`, write parquet rows with `requested_depth=30` and `levels_present=30`, and expose `lob30_v1` through the machine-readable data contract registry.

- [x] 24. Add sequence and LOB tensor contracts
  Required references:
  [DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md)

  Current implementation note:
  implemented by adding `autobot/data/collect/sequence_tensor_store.py` plus `autobot.cli collect tensors`, so the newly added `candles_second_v1`, `ws_candle_v1`, and `lob30_v1` layers are no longer isolated datasets but can be transformed into machine-readable sequence and LOB tensor contracts under `data/parquet/sequence_v1`. The builder now writes `sequence_tensor_contract.json`, `lob_tensor_contract.json`, a manifest parquet, build/validate reports, and compressed per-anchor `.npz` tensor caches that contain padded sequence tensors for observed second candles, 1-minute websocket candles, 1-minute micro rows, and a `T x L x C` LOB tensor with `L=30` and per-level channels `relative_price_bps`, `bid_size`, `ask_size`, `normalized_depth_share`, and `event_delta`, plus the blueprint-aligned global LOB channels `spread_bps`, `total_depth`, `trade_imbalance`, `tick_size`, and `relative_tick_bps`. The current contract is coverage-aware rather than fail-open: manifest rows and validate reports carry per-modality coverage ratios and masks, so missing upstream slices remain explicit machine-readable warnings instead of silently shrinking tensor shapes. Local validation covered `tests/test_sequence_tensor_store.py` plus the adjacent `candles/ws_candle/lob30` regression suites. Direct OCI validation then confirmed the server can build `data/parquet/sequence_v1/_meta/sequence_tensor_contract.json`, `data/parquet/sequence_v1/_meta/lob_tensor_contract.json`, manifest and validate artifacts, and at least one reflected tensor cache with shapes such as `second_tensor=(4,4)`, `minute_tensor=(1,4)`, `micro_tensor=(1,7)`, `lob_tensor=(1,30,5)`, and `lob_global_tensor=(1,5)`. The current reflected warning on the server is `PARTIAL_MICRO_CONTEXT`, which is an explicit data-availability warning because the current server snapshot lacked matching `micro_v1` rows for that same anchor minute, not a silent scope reduction or contract failure.


### Phase 6: Sequence And LOB Experts

- [x] 25. Implement `v5_sequence`
  Required references:
  [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)

  Current implementation note:
  implemented in `autobot/models/train_v5_sequence.py` and wired through `autobot/models/__init__.py` plus `autobot.cli model train --trainer v5_sequence`. The new trainer consumes `sequence_v1` tensor caches rather than row-wise tabular features, supports blueprint-aligned backbone families `patchtst`, `timemixer`, and `tft`, and exposes pretraining-mode choices `ts2vec_like`, `timemae_like`, or `none` before supervised fine-tuning. The current supervised head emits multi-horizon return quantiles for horizons `3/6/12/24` minutes, primary directional probability, uncertainty derived from quantile spread, and a learned regime embedding, while the registry bundle now persists `sequence_model_contract.json` plus `predictor_contract.json` so the sequence expert writes machine-readable output contracts rather than only opaque weights. The trainer also writes the usual registry backbone artifacts such as `metrics.json`, `thresholds.json`, `leaderboard_row.json`, `selection_recommendations.json`, `selection_policy.json`, `runtime_recommendations.json`, `walk_forward_report.json`, `promotion_decision.json`, and `artifact_status.json`, so the new family fits the existing registry/pointer layout instead of bypassing it. Local validation covered `tests/test_train_v5_sequence.py`, `tests/test_cli_alpha_shortcuts.py`, `tests/test_sequence_tensor_store.py`, `tests/test_predictor_contract.py`, and `tests/test_train_v5_panel_ensemble.py`; direct OCI validation confirmed the same server-side suite passes under the current `.venv` after adding `torch`, so the trainer path is reflected on the real server environment as code, dependency, and artifact-contract behavior. Open note retained explicitly: the current OCI `sequence_v1` dataset still contains only sparse smoke-built anchors and therefore is not yet rich enough to justify a meaningful server-origin production training run, so current server validation is test-level and contract-level rather than evidence from an already materialized scheduled `train_v5_sequence` family run.

- [x] 26. Implement `v5_lob`
  Required references:
  [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)

  Current implementation note:
  implemented in `autobot/models/train_v5_lob.py` and wired through `autobot/models/__init__.py` plus `autobot.cli model train --trainer v5_lob`. The trainer consumes the `sequence_v1` cache plus `candles_second_v1` / `ws_candle_v1` short-horizon closes to build blueprint-aligned LOB labels, supports the explicit backbone families `deeplob`, `bdlob`, and `hlob`, and trains a short-horizon orderbook expert whose primary outputs are `micro_alpha_1s`, `micro_alpha_5s`, `micro_alpha_30s`, and `micro_uncertainty`. The implementation also preserves the broader blueprint target structure by training auxiliary heads for `micro_alpha_60s`, `five_min_alpha`, and `adverse_excursion_30s`, then exporting those decisions in `lob_model_contract.json` and `predictor_contract.json` rather than hiding them in opaque weights. The registry bundle follows the existing artifact contract backbone, writing `metrics.json`, `thresholds.json`, `leaderboard_row.json`, `selection_recommendations.json`, `selection_policy.json`, `runtime_recommendations.json`, `walk_forward_report.json`, `promotion_decision.json`, `artifact_status.json`, and the dedicated LOB contracts so the new expert family fits the same registry/pointer system instead of forking it. Local validation covered `tests/test_train_v5_lob.py`, `tests/test_train_v5_sequence.py`, `tests/test_cli_alpha_shortcuts.py`, `tests/test_sequence_tensor_store.py`, and `tests/test_predictor_contract.py`; direct OCI validation confirmed the same suite passes on the server and an additional synthetic server smoke run under `/tmp/autobot_v5_lob_smoke` produced a real reflected `train_v5_lob` run directory with `lob_model_contract.json`, `predictor_contract.json`, `backbone_family=deeplob`, the expected short-horizon outputs, and the auxiliary targets present. Open note retained explicitly: like `v5_sequence`, the current production OCI data plane still lacks a rich naturally accumulated `sequence_v1` history for a meaningful server-origin research run, so current server validation is synthetic-run and contract-level rather than evidence from an already scheduled real-data `train_v5_lob` family run.


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
