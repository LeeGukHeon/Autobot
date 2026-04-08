# 00:20 Nightly Cycle Trace, Step 4

## 0. Purpose

This document traces `Step 4 = governed candidate acceptance` in the same spirit as Step 1, Step 2, and Step 3.

The target scope here is:

- outer wrapper:
  - `scripts/v5_governed_candidate_acceptance.ps1`
- core orchestrator:
  - `scripts/candidate_acceptance.ps1`

The goal of this document is:

1. freeze the true Step 4 entrypoint and parameter contract
2. trace the real execution phases in `candidate_acceptance.ps1`
3. record which upstream artifacts from Step 1~3 are consumed
4. record which downstream artifacts and promotion pointers are produced
5. compare the current implementation against:
   - `docs/RUNTIME_VIABILITY_AND_RUNTIME_SOURCE_HARDENING_PLAN_2026-04-06.md`
   - `docs/DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md`
   - `docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md`

Important scope note:

- this is a detailed Step 4 trace draft
- it already covers the main acceptance phases and contracts
- but Step 4 is large enough that conservative completion judgment should stay open until the later phase helpers and live reruns are rechecked again


## 1. True Entrypoint

The direct Step 4 wrapper is:

- `scripts/v5_governed_candidate_acceptance.ps1`

Its role is narrow:

- resolve project root / python exe
- define known runtime units
- define v5 governed defaults
- invoke `scripts/candidate_acceptance.ps1`

`v5_governed_candidate_acceptance.ps1` currently pins:

- `ModelFamily = train_v5_fusion`
- `Trainer = v5_fusion`
- `DependencyTrainers = v5_panel_ensemble, v5_sequence, v5_lob, v5_tradability`
- `FeatureSet = v4`
- `LabelSet = v3`
- `Task = cls`
- `RunScope = scheduled_daily`
- `CandidateModelRef = latest_candidate`
- `ChampionModelRef = champion`
- `PaperFeatureProvider = live_v5`
- `PromotionPolicy = paper_final_balanced`
- `TrainerEvidenceMode = required`

This means Step 4 is not a generic acceptance lane in current nightly usage.
The actual nightly route is the fully governed v5 fusion acceptance route.


## 2. Candidate Acceptance Parameter Surface

`scripts/candidate_acceptance.ps1` has a very wide parameter surface.

The major parameter groups are:

### 2.1 Core roots and scripts

- `PythonExe`
- `ProjectRoot`
- `BatchDate`
- `DailyPipelineScript`
- `TrainSnapshotCloseReportPath`
- `PaperSmokeScript`
- `OutDir`

### 2.2 Window and ramp controls

- `TrainLookbackDays`
- `BacktestLookbackDays`
- `TrainLookbackRampEnabled`
- `TrainLookbackRampMicroRoot`
- `TrainLookbackRampMinMarketsPerDate`
- `TrainDataQualityFloorDate`
- `TrainStartFloorDate`

### 2.3 Split-policy controls

- `SplitPolicyHistoricalSelectorEnabled`
- `SplitPolicyCandidateHoldoutDays`
- `SplitPolicyMinHistoricalAnchors`
- `SplitPolicyMaxNewAnchorEvaluationsPerRun`
- `SplitPolicyHistoryBoosterSweepTrials`
- `SplitPolicyHistoryRunScope`

### 2.4 Train / registry identity

- `Tf`
- `Quote`
- `TrainTopN`
- `FeatureParityTopN`
- `BacktestTopN`
- `ModelFamily`
- `Trainer`
- `DependencyTrainers`
- `FeatureSet`
- `LabelSet`
- `Task`
- `RunScope`
- `CandidateModelRef`
- `ChampionModelRef`
- `ChampionModelFamily`

### 2.5 Backtest and paper gates

- `BacktestTopPct`
- `BacktestMinProb`
- `BacktestMinCandidatesPerTs`
- `HoldBars`
- `BacktestRuntimeParityEnabled`
- `BacktestMinPayoffRatio`
- `BacktestMaxLossConcentration`
- `ExecutionStructureMinClosedTrades`
- `PaperSoakDurationSec`
- `PaperMicroProvider`
- `PaperFeatureProvider`
- `PaperUseLearnedRuntime`
- `PaperWarmupSec`
- `PaperWarmupMinTradeEventsPerMarket`
- `PaperMaxFallbackRatio`
- `PaperMinOrdersSubmitted`
- `PaperMinOrdersFilled`
- `PaperMinRealizedPnlQuote`
- `PaperMinMicroQualityScoreMean`
- `PaperMinActiveWindows`
- `PaperMinNonnegativeWindowRatio`
- `PaperMaxFillConcentrationRatio`
- `PaperMinPayoffRatio`
- `PaperMaxLossConcentration`
- `PaperEvidenceEdgeScore`
- `PaperEvidenceHoldScore`
- `PaperHistoryWindowRuns`
- `PaperHistoryMinCompletedRuns`
- `PaperHistoryMinNonnegativeRunRatio`
- `PaperHistoryMinPositiveRunRatio`
- `PaperHistoryMinMedianMicroQualityScore`
- `PaperMinTierCount`
- `PaperMinPolicyEvents`

### 2.6 Promotion and runtime controls

- `PromotionPolicy`
- `TrainerEvidenceMode`
- `BacktestAllowStabilityOverride`
- `BacktestChampionPnlTolerancePct`
- `BacktestChampionMinDrawdownImprovementPct`
- `BacktestChampionMaxFillRateDegradation`
- `BacktestChampionMaxSlippageDeteriorationBps`
- `BacktestChampionMinUtilityEdgePct`
- `OverlayCalibrationArtifactPath`
- `OverlayCalibrationWindowRuns`
- `OverlayCalibrationMinReports`
- `RestartUnits`
- `KnownRuntimeUnits`
- `AutoRestartKnownUnits`

### 2.7 High-level mode switches

- `EnableVariantMatrixSelection`
- `SkipDailyPipeline`
- `SkipPaperSoak`
- `SkipReportRefresh`
- `SkipPromote`
- `SkipChampionCompare`
- `DryRun`


## 3. Step 4 Top-Level Initialization

The top-level initialization order in `candidate_acceptance.ps1` is:

1. resolve paths
2. resolve effective batch date
3. resolve current `data_platform_ready_snapshot_id`
4. load `train_snapshot_close` contract
5. resolve train-window ramp
6. build initial report skeleton
7. write `train_snapshot_close_preflight`
8. if preflight fails, exit early

That means Step 4 is structurally anchored on Step 3.

It is not “train first, inspect data later”.
It is explicitly “data-close contract first, acceptance later”.


## 4. Upstream Contract From Step 3

Step 4 consumes Step 3 through:

- `Resolve-TrainSnapshotCloseContract`
- `Assert-TrainingCriticalCoverageWindow`
- `Resolve-TrainWindowRamp`

The fields it reads from `train_snapshot_close_latest.json` include:

- `batch_date`
- `snapshot_id`
- `snapshot_root`
- `overall_pass`
- `deadline_met`
- `source_freshness`
- `micro_date_coverage_counts`
- `coverage_window`
- `train_window`
- `certification_window`
- `training_critical_start_date`
- `training_critical_end_date`
- `features_v4_effective_end`
- `coverage_window_source`
- `refresh_argument_mode`

This makes Step 4 the first place where:

- Step 1 freshness
- Step 2 freshness
- Step 3 snapshot close

become real gating input for model training / certification / promotion.


## 5. High-Level Phase Map

The actual Step 4 flow inside `candidate_acceptance.ps1` is:

1. `train_snapshot_close_preflight`
2. window ramp / split-policy resolution
3. features build window resolution
4. data contract registry refresh
5. features validate
6. live feature parity
7. feature dataset certification
8. private execution label store build/validate
9. dependency trainers
10. dependency runtime export chain
11. dependency runtime contract alignment checks
12. candidate train
13. runtime dataset coverage preflight
14. runtime viability preflight
15. runtime deploy-contract preflight
16. acceptance backtest candidate/champion
17. runtime parity backtest candidate/champion
18. paper candidate
19. fusion variant selection evidence
20. overall gate resolution
21. latest-candidate pointer update
22. promote
23. optional report refresh
24. acceptance report write

This is not a single train-and-compare script.
It is a full orchestration layer that mutates:

- reports
- pointers
- run artifact status
- runtime units


## 6. Preflight And Window Logic

### 6.1 `train_snapshot_close_preflight`

Step 4 first builds:

- `train_snapshot_close_preflight`

This phase checks:

- close report exists
- `batch_date` matches expected batch date
- `overall_pass == true`
- `snapshot_id` matches current ready snapshot pointer
- `deadline_met == true`
- close coverage window contains:
  - train window
  - certification window

If this fails, Step 4 exits immediately with:

- `failure_stage = data_close`

This matches the `04-06` hardening intent very closely.

### 6.2 Train window ramp

`Resolve-TrainWindowRamp` uses:

- batch date
- requested train / backtest lookback
- micro coverage counts
- quality floor dates

to derive:

- effective train window
- effective certification window
- whether ramp is active
- whether train window had to be shrunk

If micro coverage is insufficient and no valid comparable window exists, Step 4 can fail before candidate train.

This makes Step 4 not purely a model gate.
It is also a dataset sufficiency gate.

### 6.3 Split-policy selector

If `SplitPolicyHistoricalSelectorEnabled` is on, Step 4 can:

- evaluate alternative holdout windows
- write split-policy history
- choose between:
  - `promotion_strict`
  - `bootstrap_latest_inclusive`

and then rebuild features for the selected window.

This is a large policy branch inside Step 4 itself.


## 7. Feature / Registry / Certification Subchain

### 7.1 `Invoke-FeaturesBuildAndLoadReport`

This helper has two modes:

- mutable rebuild:
  - actually run `python -m autobot.cli features build ...`
- frozen close contract mode:
  - if trainer is `v5_fusion`
  - and `train_snapshot_close` is pass
  - and `features_v4_effective_end` is available
  - then it may reuse the already-frozen Step 3 features contract instead of rebuilding

This is a very important Step 3 -> Step 4 contract bridge.

The source mode values are:

- `mutable_features_build`
- `train_snapshot_close_frozen_features`

Important nuance:

- even in `train_snapshot_close_frozen_features` mode, the helper reads the current project-root
  `data/features/features_v4/_meta/build_report.json`
- it does **not** resolve the build report from the immutable ready snapshot root

So Step 4 currently treats “frozen features” as:

- frozen enough for train-window reuse decision

not as:

- “all feature contract reads come from the immutable snapshot tree”

### 7.2 `Invoke-FeaturesValidateAndLoadReport`

This wraps:

- `python -m autobot.cli features validate ...`

and requires:

- `checked_files > 0`
- `fail_files == 0`
- `schema_ok == true`
- `leakage_smoke == PASS`

If not usable:

- Step 4 fails before training

### 7.3 `Invoke-DataContractRegistryAndLoadReport`

This wraps:

- `python -m autobot.ops.data_contract_registry`

and requires:

- registry path resolved
- contract count > 0

If missing:

- Step 4 fails before training

### 7.4 `Invoke-LiveFeatureParityAndLoadReport`

This wraps:

- `python -m autobot.ops.live_feature_parity_report`

and requires:

- report path exists
- sampled pairs > 0
- `acceptable == true`
- `status == PASS`

If not:

- Step 4 fails before train

### 7.5 `Invoke-FeatureDatasetCertificationAndLoadReport`

This wraps:

- `python -m autobot.ops.feature_dataset_certification`

and requires:

- `pass == true`
- `status == PASS`

### 7.6 `Invoke-PrivateExecutionLabelStoreAndLoadReport`

This is required when:

- trainer is `v5_fusion`
- or trainer is `v5_tradability`
- or dependency trainers include `v5_tradability`

It wraps:

- `python -m autobot.ops.private_execution_label_store`

and requires:

- build report path exists
- `rows_written_total > 0`
- validate `pass == true`
- validate `status == PASS`


## 7.7 Mutable vs frozen contract split inside Step 4

This is one of the most important newly confirmed Step 4 findings.

`candidate_acceptance.ps1` sets:

- `AUTOBOT_DATA_PLATFORM_READY_SNAPSHOT_ID`

around native command execution.

Because of that, CLI/model commands that honor snapshot resolution can train or export against frozen snapshot dataset roots.

However, several Step 4 preflight helpers still read mutable current-root artifacts directly:

- `Invoke-FeaturesBuildAndLoadReport`
  - frozen mode still inspects current `features_v4/_meta/build_report.json`
- `Invoke-FeaturesValidateAndLoadReport`
  - current `features_v4/_meta/validate_report.json`
- `Invoke-DataContractRegistryAndLoadReport`
  - current `data/_meta/data_contract_registry.json`
- `Invoke-LiveFeatureParityAndLoadReport`
  - current `features_v4/_meta/live_feature_parity_report.json`
- `Invoke-FeatureDatasetCertificationAndLoadReport`
  - current `features_v4/_meta/feature_dataset_certification.json`
- `Invoke-PrivateExecutionLabelStoreAndLoadReport`
  - current `private_execution_v1` build/validate reports

At the same time, later train/export commands can resolve frozen dataset roots from the ready snapshot through `autobot.cli`.

So current Step 4 mixes:

- mutable current-root preflight artifacts
- frozen snapshot-root training inputs

This is a real contract mismatch candidate.

It does not prove immediate incorrect behavior in every run, but it is not a clean “all reads come from the same frozen close” design.

Current implementation status:

- first hardening slice implemented on `2026-04-08`
- Step 4 now prefers snapshot-root artifacts for:
  - features validate
  - data contract registry
  - live feature parity
  - feature dataset certification
  - private execution label store
- `features build` frozen mode also now prefers snapshot-root `build_report.json`
- current root remains fallback when the snapshot artifact is absent

So Step 4 is now closer to:

- frozen snapshot first

than to:

- mutable current root first

but it is still not fully proven end-to-end until a representative rerun is observed.


## 8. Dependency Trainer Phase

### 8.1 `Invoke-DependencyTrainerChain`

This function iterates `DependencyTrainers`.

For each dependency trainer it:

1. resolves model family
2. derives dependency run scope
3. checks whether an existing reusable run is available
4. if reusable, records reuse
5. otherwise runs either:
   - `python -m autobot.cli model train ...`
   - or `train-variant-matrix` for `v5_sequence` / `v5_lob` when enabled

Returned fields include:

- `trainer`
- `model_family`
- `run_scope`
- `exit_code`
- `run_dir`
- `run_id`
- `data_platform_ready_snapshot_id`
- `reused`
- `required_artifacts_complete`
- `tail_mode`
- variant metadata fields when applicable

### 8.2 Dependency reuse contract

Dependency reuse is not “latest run wins”.

It requires:

- matching trainer/model family
- matching train window
- matching execution eval window
- matching expected snapshot id
- required artifacts complete

This is one of the stronger reuse contracts in the current system.

The actual required artifacts are:

- `train_config.yaml`
- `artifact_status.json`
- `expert_prediction_table.parquet`

Plus trainer-specific provenance artifacts for some trainers:

- `v5_sequence`
  - `sequence_pretrain_contract.json`
  - `sequence_pretrain_report.json`
  - `domain_weighting_report.json`
- `v5_lob`
  - `lob_backbone_contract.json`
  - `lob_target_contract.json`
  - `domain_weighting_report.json`
- `v5_tradability`
  - `tradability_model_contract.json`
  - `domain_weighting_report.json`


## 9. Dependency Runtime Export Phase

### 9.1 `Invoke-DependencyRuntimeExportChain`

This takes dependency results and builds runtime export tables for:

- panel
- sequence
- lob
- tradability

It constructs a certification window export root and expects:

- export path
- metadata path
- snapshot id match
- coverage dates
- `window_timezone = Asia/Seoul`
- row count > 0

### 9.2 Runtime export alignment gate

`Test-DependencyRuntimeExportContractAlignment` checks:

- snapshot id consistency
- certification window coverage
- anchor alignment
- common runtime universe consistency

Potential failure codes include:

- `*_RUNTIME_SNAPSHOT_MISMATCH`
- `*_RUNTIME_WINDOW_GAP`
- `*_RUNTIME_ANCHOR_GAP`
- `*_RUNTIME_UNIVERSE_MISMATCH`

This is one of the most important downstream-contract checks in Step 4.


## 10. Candidate Train Phase

The candidate train itself happens after:

- data-close preflight
- feature contract checks
- dependency trainer checks
- dependency runtime export checks

This means current Step 4 is structurally “fail closed before train” in many places.

For `v5_fusion`, the train phase also records:

- `fusion_dependency_inputs`
- `fusion_dependency_runtime_inputs`
- `common_runtime_universe_id`
- `fusion_provenance`
- `sequence_variant_name`
- `lob_variant_name`
- `fusion_variant_name`

This is much closer to the `04-06` source-lineage intent than a simple train wrapper would be.


## 11. Post-Train Preflights

For `v5_fusion`, Step 4 then runs three critical post-train gates.

### 11.1 Runtime dataset coverage preflight

Functions:

- `Resolve-CandidateRuntimeDatasetCoverage`
- `Test-CandidateRuntimeDatasetCertificationCoverage`

This loads:

- `fusion_runtime_input_contract.json`
- runtime dataset summary

and checks:

- runtime dataset exists
- manifest exists
- data files exist
- rows > 0
- certification coverage dates are present
- `window_timezone = Asia/Seoul`

Failure stage:

- `acceptance_gate`

### 11.2 Runtime viability preflight

Functions:

- `Resolve-CandidateRuntimeViabilityArtifact`
- `Test-CandidateRuntimeViability`

This reads:

- `runtime_viability_report.json`

and surfaces:

- `alpha_lcb_floor`
- `runtime_rows_total`
- `mean_final_expected_return`
- `mean_final_expected_es`
- `mean_final_uncertainty`
- `mean_final_alpha_lcb`
- `alpha_lcb_positive_count`
- `rows_above_alpha_floor`
- `entry_gate_allowed_count`
- `estimated_intent_candidate_count`

If fail:

- `failure_stage = runtime_viability`

This is directly aligned with the `2026-04-06` fail-fast direction.

### 11.3 Runtime deploy contract preflight

Functions:

- `Resolve-CandidateRuntimeDeployContractArtifact`
- `Test-CandidateRuntimeDeployContract`

This reads:

- `runtime_deploy_contract_readiness.json`

and can fail before backtest / paper if deploy-readiness is not satisfied.


## 12. Backtest / Runtime Parity / Paper

### 12.1 Acceptance backtest

`Invoke-OrReuse-AcceptanceBacktest` and `Invoke-OrReuse-AcceptanceStatValidation` implement cached backtests.

Backtest contract (`acceptance` preset) includes:

- `evaluation_contract_id = acceptance_frozen_compare_v1`
- `evaluation_contract_role = frozen_compare`
- raw-threshold selection mode
- all learned execution policy toggles disabled
- `micro-order-policy off`
- hold-style exit mode

This is where Step 4 explicitly enforces the “frozen compare” profile.

Step 4 also wraps acceptance backtest with a cache layer:

- `candidate_acceptance_backtest_contract_v1`
- cache keyed by contract hash
- cache root under model family registry

So backtest in Step 4 is not always fresh compute.
It can reuse an exact previous result when the acceptance backtest contract is identical.

### 12.2 Runtime parity backtest

`runtime_parity` preset instead uses:

- `evaluation_contract_id = runtime_deploy_contract_v1`
- `evaluation_contract_role = deploy_runtime`
- learned runtime policy toggles enabled
- `micro-order-policy on`
- `micro-order-policy-mode trade_only`
- `micro-order-policy-on-missing static_fallback`

So Step 4 intentionally runs **two different backtest contracts**:

- frozen acceptance compare
- deploy-like runtime parity

### 12.3 Paper candidate

If `SkipPaperSoak` is not set, Step 4 invokes:

- `scripts/paper_micro_smoke.ps1`

and then evaluates:

- orders submitted / filled
- realized pnl
- micro quality
- active windows
- fill concentration
- history-based paper evidence
- execution structure gate

### 12.4 Promote

Promotion only happens if:

- overall pass
- not shadow-only lane
- promotion allowed by lane governance
- not skipped by flag

Then Step 4 runs:

- `python -m autobot.cli model promote --model-ref <candidate_run_id> --model-family <ModelFamily>`

After promote it can restart runtime units.

Before promote, Step 4 can also:

- update `latest_candidate` pointers when overall pass is true
- mark run-level `artifact_status.json`

So even without promote, Step 4 can still mutate candidate pointer state.


## 13. Current Live Server Observation

Latest stored v5 acceptance artifact on the OCI server:

- `logs/model_v5_acceptance/latest.json`
- mtime:
  - `2026-04-06T17:15:14Z`

Top-level result:

- `batch_date = 2026-04-05`
- `candidate_run_id = 20260406T171500Z-s42-67abadc6`
- `overall_pass = false`
- `failure_stage = fusion_train`
- `failure_code = UNHANDLED_EXCEPTION`

Important successful pre-train / pre-backtest phases in that artifact:

- `train_snapshot_close_preflight.pass = true`
- `features_build.exit_code = 0`
- `data_contract_registry.exit_code = 0`
- `features_validate.fail_files = 0`
- `features_live_parity.status = PASS`
- `feature_dataset_certification.pass = true`
- `private_execution_label_store.pass = true`
- `dependency_trainers.count = 4`
- `dependency_runtime_exports.count = 4`
- `runtime_dataset_coverage_preflight.pass = true`
- `runtime_viability_preflight.pass = true`
- `runtime_deploy_contract_preflight.pass = true`

The observed failure was not a gate failure.
It was an unhandled exception during backtest invocation.

Observed exception payload:

- backtest command:
  - `python -m autobot.cli backtest alpha ... --micro-order-policy off ...`
- error:
  - `autobot: error: unrecognized arguments: --micro-order-policy off`

So the current stored live artifact indicates a real CLI contract mismatch between:

- `candidate_acceptance.ps1` backtest invocation
- the server-side `autobot.cli backtest alpha` parser state at that time

This should be treated as a real Step 4 bug candidate until revalidated under current server code.

### 13.1 Revalidation against current server code

Current server code now accepts the same acceptance backtest CLI shape.

Representative manual replay on the OCI server with the historical failing argument pattern:

- `python -m autobot.cli backtest alpha --preset acceptance ... --micro-order-policy off ...`

Current observed result:

- command parses successfully
- backtest runs successfully
- no parser rejection for `--micro-order-policy off`

This means the stored latest acceptance artifact from `2026-04-06` is best interpreted as:

- a historical artifact produced under an older server code state
- not necessarily a current parser bug in the now-deployed code

So the right conservative reading is:

- the live latest artifact still records a real historical failure
- but the exact parser mismatch is **not** currently reproduced on the server after the latest code updates

### 13.2 Failure-stage labeling is currently coarse

The top-level `catch` block in `candidate_acceptance.ps1` chooses a default failure stage.

For exceptions, the fallback logic is:

- if message looks like dependency runtime export:
  - `runtime_export`
- elseif message looks like `FUSION_RUNTIME_*` or trainer is `v5_fusion`:
  - `fusion_train`
- elseif message looks like `TRAIN_SNAPSHOT_CLOSE*`:
  - `data_close`
- else:
  - `acceptance_gate`

That means for `v5_fusion`, many late-phase exceptions can still be labeled as:

- `failure_stage = fusion_train`

even when the actual failing subphase was:

- acceptance backtest
- runtime parity backtest
- paper soak
- another post-train step

So the current failure-stage field is useful but not fully precise.

This is a real Step 4 observability weakness.

### 13.3 Current downstream pointer/artifact state on the server

For the latest stored failed v5 acceptance run:

- run id:
  - `20260406T171500Z-s42-67abadc6`
- `models/registry/train_v5_fusion/latest.json`
  - points to that run id
- `models/registry/train_v5_fusion/latest_candidate.json`
  - not present
- `artifact_status.json`
  - `status = acceptance_incomplete`
  - `acceptance_completed = false`
  - `candidate_adoptable = false`
  - `candidate_adopted = false`
  - `promoted = false`

This matches the intended downstream contract:

- train completed
- acceptance failed
- candidate pointer not adopted
- champion pointer unchanged

Important nuance:

- `latest.json` is not the same contract as `latest_candidate.json`
- a run can become the latest trained run before acceptance passes
- Step 4 only writes `latest_candidate` when overall acceptance succeeds

This is why downstream consumers must distinguish:

- `latest` = latest trained run
- `latest_candidate` = latest acceptance-adopted candidate

and should not silently treat them as interchangeable.


## 14. Alignment With 2026-04-06 Hardening Plan

### 14.1 Areas that align well

`docs/RUNTIME_VIABILITY_AND_RUNTIME_SOURCE_HARDENING_PLAN_2026-04-06.md` expects:

- `train_snapshot_close_preflight`
- dependency trainer/export resolution
- runtime viability as first-class fusion artifact
- runtime deploy contract readiness
- fail-fast before later backtest/paper stages when those preflights fail

Current Step 4 code clearly aligns with that direction:

- `train_snapshot_close_preflight` exists and can hard fail
- runtime dataset coverage preflight exists
- runtime viability preflight exists
- runtime deploy contract preflight exists
- those can all exit before paper/promote

### 14.2 Areas not yet fully aligned

The `04-06` plan emphasizes:

- fail-fast with rich diagnostics
- not falling through into later confusing failures

Current live latest artifact instead shows:

- preflights all passed
- then a backtest CLI argument mismatch triggered
  - `failure_stage = fusion_train`
  - `failure_code = UNHANDLED_EXCEPTION`

This is not the intended clean fail-fast style.
It is a concrete execution-path bug / contract mismatch.


## 15. Alignment With Data / Training Blueprints

### 15.1 Data and feature blueprint

`docs/DATA_AND_FEATURE_PLATFORM_BLUEPRINT_2026-03-25.md` wanted:

- stronger artifact-level certification
- stronger lineage / registry integration
- acceptance / promotion integration for those artifacts

Current Step 4 does use:

- data contract registry
- live feature parity
- feature dataset certification
- train snapshot close contract

So Step 4 is much closer to the blueprint than a plain train/backtest wrapper.

### 15.2 Training blueprint

`docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md` emphasizes:

- final certification window separation
- not reusing training windows too loosely in runtime acceptance

Current Step 4 does explicitly maintain:

- train window
- certification window
- backtest/runtime parity window
- research window

That is a good structural match.

However, Step 4 is still very large and imperative, so while the conceptual contract is aligned, the implementation remains operationally fragile.


## 16. Upstream / Downstream Contract Position

### 16.1 Upstream dependence

Step 4 depends strongly on:

- Step 3 snapshot close
- feature contract artifacts
- private execution label store
- dependency trainer outputs
- dependency runtime exports

So Step 4 is the first place where all earlier data and feature layers become one acceptance graph.

### 16.2 Downstream impact

Step 4 directly affects:

- candidate run status
- latest candidate pointers
- promotion decision
- champion pointer update
- runtime unit restart

Therefore a Step 4 contract mismatch is downstream-critical.
It is no longer “observation only”.


## 17. Conservative Findings

### F1. Step 4 structurally matches the hardened acceptance direction

Current judgment:

- true

Reason:

- strong pre-train and post-train preflights already exist
- runtime viability / deploy checks are first-class

### F2. Step 4 still mixes mutable and frozen contract reads

Observed:

- training/export commands can honor `AUTOBOT_DATA_PLATFORM_READY_SNAPSHOT_ID`
- several preflight helpers still read current mutable root artifacts directly

Current judgment:

- historically real
- partially hardened now
- still needs representative rerun validation to confirm the mixed-read risk is gone in practice

### F3. Latest live Step 4 artifact showed a historical backtest CLI contract mismatch

Observed:

- latest live acceptance artifact failed with:
  - `UNHANDLED_EXCEPTION`
  - backtest parser rejecting `--micro-order-policy off`

Current judgment:

- historically real
- now revalidated as **not reproduced on current server code**
- should be treated as a stale latest-artifact symptom unless reproduced again

### F4. Failure-stage labeling is too coarse for v5_fusion exceptions

Observed:

- default exception mapping can still collapse many late-phase failures into `fusion_train`

Current judgment:

- real observability weakness
- not necessarily a behavioral blocker, but it makes operational diagnosis less precise

### F5. `latest` vs `latest_candidate` meaning remains easy to misread

Observed:

- failed acceptance run still became `latest`
- but did not become `latest_candidate`

Current judgment:

- intended contract
- but operationally easy to misread without explicit documentation
- downstream dashboards and runbooks must keep this distinction visible

### F6. Step 4 is operationally complex enough that completion should be conservative

Reason:

- `candidate_acceptance.ps1` is very large
- many phases mutate reports and run artifacts
- many early exits exist
- several phase helpers call other scripts / CLI contracts

Current judgment:

- do not mark Step 4 complete yet


## 18. Current Conservative Status

Step 4 is **not closed** yet.

What is already strong:

- entrypoint mapping
- parameter surface
- main phase ordering
- upstream Step 3 contract consumption
- downstream mutation points
- blueprint / `04-06` alignment framing
- latest live failure observation

What still needs more work before a conservative close:

1. deeper read-through of:
   - dependency runtime export tail
   - candidate train artifact expectations
   - runtime parity / paper / promote late phases
2. resolve whether mutable-root preflight helpers should be switched to snapshot-root reads for strict frozen acceptance
3. tighter mapping of Step 4 report fields to downstream dashboards / runtime units / promotion pointers
4. decide whether failure-stage precision should be hardened for late-phase exceptions
5. decide whether stale historical `latest.json` acceptance artifacts should be superseded by a new representative rerun before declaring Step 4 closed

So Step 4 should currently be treated as:

- `trace draft: strong`
- `completion judgment: still open`


## 19. Representative Rerun Archive Baseline

The representative Step 4 rerun performed on `2026-04-08` should be kept as the
current baseline artifact set for further model / gate / execution analysis.

Server archive location:

- `/home/ubuntu/MyApps/Autobot/logs/analysis/acceptance_full_rerun_20260408`
- `/home/ubuntu/MyApps/Autobot/logs/analysis/acceptance_full_rerun_20260408.tgz`

What is preserved there:

- full rerun acceptance logs:
  - `acceptance_latest.json`
  - `acceptance_latest.md`
  - `v5_candidate_acceptance_20260408-121605.json`
  - `v5_candidate_acceptance_20260408-121605.md`
  - `common_runtime_universe.json`
- candidate fusion run artifacts for:
  - `20260407T214609Z-s42-96ff5673`
- champion comparison artifacts for:
  - `20260328T090226Z-s42-d2b602aa`

Current working rule:

- further Step 4 improvement analysis should continue against this archived rerun
  baseline first
- do not rely only on moving `latest.json` pointers when investigating why the
  candidate failed
- use this archive as the stable evidence set for:
  - model-side diagnosis
  - gate-side diagnosis
  - execution/runtime-parity diagnosis

This does **not** mean Step 4 is closed.

It means:

- the current rerun result is now frozen enough to serve as the canonical
  analysis baseline while further fixes are explored.
