# 00:20 Nightly Cycle Trace, Step 3

## 0. Purpose

This document traces `Step 3 = train_snapshot_close` from the real `00:20` nightly chain entrypoint down through:

- `scripts/close_v5_train_ready_snapshot.ps1`
- `scripts/refresh_data_platform_layers.ps1`
- `scripts/refresh_current_features_v4_contract_artifacts.ps1`
- `autobot.ops.data_platform_snapshot`
- the immediate downstream consumer contract in `scripts/candidate_acceptance.ps1`

The goal is the same as Step 1 and Step 2:

1. freeze the true execution path
2. document the real argument and artifact contracts
3. compare repo code with the current OCI runtime state
4. identify real operating issues conservatively

Important scope note:

- This document is about `Step 3` itself.
- It stops at the point where `candidate_acceptance.ps1` consumes the Step 3 artifact.
- It does not fully trace the whole acceptance lane yet.


## 1. True Entrypoint

Inside the live `00:20` chain, `Step 3` is invoked by:

- outer wrapper: `scripts/daily_champion_challenger_v5_for_server.ps1`
- concrete worker: `scripts/close_v5_train_ready_snapshot.ps1`

The wrapper calls it after:

1. `candles_api_refresh`
2. `raw_ticks_daily`

The actual argument shape passed by the wrapper is:

- `-ProjectRoot <resolvedProjectRoot>`
- `-PythonExe <resolvedPythonExe>`
- `-BatchDate <resolvedBatchDate>`
- `-SkipDeadline`
- optional `-DryRun`

This matters because current nightly topology does **not** use Step 3 as an independent timer-owned stage.
It is currently a chain-owned stage.


## 2. Current Topology

Repo still contains a standalone installer:

- `scripts/install_server_train_snapshot_close_service.ps1`

Standalone defaults there are:

- service: `autobot-v5-train-snapshot-close.service`
- timer: `autobot-v5-train-snapshot-close.timer`
- `OnCalendar = *-*-* 00:05:00`
- lock file: `/tmp/autobot-v5-train-snapshot-close.lock`

But on the current OCI server:

- `autobot-train-snapshot-close.timer` does not exist
- `autobot-train-snapshot-close.service` does not exist

Current live path is therefore:

- `autobot-v5-challenger-spawn.timer`
- `daily_champion_challenger_v5_for_server.ps1`
- `close_v5_train_ready_snapshot.ps1`

So Step 3 should be documented as a `00:20` chain-owned step, not as an independently scheduled unit.


## 3. Close Script Parameter Surface

`scripts/close_v5_train_ready_snapshot.ps1` takes:

- project and interpreter:
  - `ProjectRoot`
  - `PythonExe`
- core operating date:
  - `BatchDate`
- output summaries:
  - `SummaryPath`
  - `RefreshSummaryPath`
  - `FeatureRefreshSummaryPath`
- subordinate script overrides:
  - `DataPlatformRefreshScript`
  - `FeatureContractRefreshScript`
- upstream freshness sources:
  - `CandlesSummaryPath`
  - `TicksSummaryPath`
- micro coverage probe:
  - `MicroRoot`
  - `Tf`
- training window controls:
  - `FeatureTopN`
  - `TrainLookbackDays`
  - `BacktestLookbackDays`
  - `TrainingCriticalStartDate`
  - `TrainingCriticalEndDate`
- freshness thresholds:
  - `MaxCandlesSummaryAgeMinutes`
  - `MaxTicksSummaryAgeMinutes`
- runtime switches:
  - `SkipDeadline`
  - `DryRun`

Default artifacts:

- `data/collect/_meta/train_snapshot_close_latest.json`
- `data/collect/_meta/train_snapshot_training_critical_refresh_latest.json`
- `data/features/features_v4/_meta/nightly_train_snapshot_contract_refresh.json`


## 4. Core Helper Semantics

### 4.1 `Resolve-BatchDateValue`

If `BatchDate` is explicitly passed, it must be `yyyy-MM-dd`.

If not passed, default is:

- local server date minus 1 day

Because the live server timezone is `Asia/Seoul`, standalone default behavior is effectively:

- `batch_date = yesterday in KST`

### 4.2 `Resolve-DateWindowForTrainingCriticalRefresh`

This helper decides the training-critical refresh window.

Two modes exist:

- explicit:
  - both `TrainingCriticalStartDate` and `TrainingCriticalEndDate` are passed
  - source = `explicit_window`
- derived:
  - start = `batch_date - (train_lookback_days + backtest_lookback_days - 1)`
  - end = `batch_date`
  - source = `batch_date_plus_train_and_backtest_lookback`

With current defaults:

- `TrainLookbackDays = 30`
- `BacktestLookbackDays = 8`

the default coverage span is:

- total `38` days inclusive

### 4.3 `Resolve-TrainSnapshotWindowContract`

This helper derives three windows:

- `train_window`
- `certification_window`
- `coverage_window`

For batch date `D`:

- `certification_window`
  - start = `D - (backtest_days - 1)`
  - end = `D`
- `train_window`
  - end = `D - backtest_days`
  - start = `train_end - (train_days - 1)`
- `coverage_window`
  - copied from training-critical refresh start/end

The important detail is that `coverage_window` is intentionally wider than `train_window`.

### 4.4 `Get-SourceFreshnessResult`

This helper reads a summary JSON and checks:

- file exists
- `generated_at_utc` or `generated_at` parses
- age is within max age threshold
- every embedded `steps[*].exit_code == 0`
- if `BatchDateValue` is given:
  - `payload.batch_date == expected batch date`
  - else fallback to `validate_dates`
  - else fallback to step name `validate_raw_ticks_<batchDate>`

Current Step 3 behavior:

- candles freshness:
  - age and step exit only
  - no batch-date enforcement
- ticks freshness:
  - age, step exit, and batch-date coverage

### 4.5 Deadline semantics

Close script computes:

- `deadlineDate = batch_date + 1 day + 20 minutes`

This `DateTime` is local-server based because `ParseExact()` returns unspecified-kind and the later `.ToUniversalTime()` conversion uses server local timezone.

On the current KST server this means:

- deadline = `00:20 KST on D+1`

This explains the current topology decision:

- standalone timer default = `00:05 KST`
- live nightly wrapper passes `-SkipDeadline`

The current chain runs around `00:20 KST`, so deadline enforcement would be right on the boundary and could flap.
Current live chain intentionally disables it.


## 5. Top-Level Execution Sequence

`close_v5_train_ready_snapshot.ps1` executes in this order:

1. resolve paths and dates
2. build `trainingCriticalWindow`
3. build `windowContract`
4. evaluate Step 1 freshness
5. evaluate Step 2 freshness
6. if both pass:
   - run `refresh_data_platform_layers.ps1 -Mode training_critical -SkipPublishReadySnapshot`
7. if still no failure:
   - run `refresh_current_features_v4_contract_artifacts.ps1`
8. if still no failure:
   - run `python -m autobot.ops.data_platform_snapshot publish`
9. write `train_snapshot_close_latest.json`
10. exit `0` on success, `2` on any failure reason


## 6. What Step 3 Actually Gates On

`overall_pass` only requires:

- upstream freshness passed
- subordinate scripts exited `0`
- snapshot publish exited `0`
- deadline passed if deadline enforcement was enabled

It does **not** directly gate on:

- warning counts inside micro validate
- warning counts inside sequence validate
- warning counts inside features validate
- support-level quality distributions

That means Step 3 is closer to:

- `contract close / publish gate`

than to:

- `all warnings must be zero`


## 7. Subscript A: `refresh_data_platform_layers.ps1`

### 7.1 High-level role

This script refreshes derived data-platform layers below features:

- `candles_second_v1`
- `lob30_v1`
- `micro_v1`
- `sequence_v1`
- `private_execution_v1`
- registry/retention artifacts

### 7.2 Important parameter defaults

- `Mode = full | training_critical | runtime_rich`
- `TopN = 50`
- `TensorMaxMarkets = 20`
- `TensorMaxAnchorsPerMarket = 64`
- `TensorRecentDates = 2`
- `MicroRecentDates = 2`
- `SkipPublishReadySnapshot` supported

### 7.3 Current Step 3 call shape

Close script calls it as:

- `-Mode training_critical`
- explicit tensor window:
  - `-TensorStartDate <trainingCriticalStartDate>`
  - `-TensorEndDate <trainingCriticalEndDate>`
- explicit micro window:
  - `-MicroStartDate <trainingCriticalStartDate>`
  - `-MicroEndDate <trainingCriticalEndDate>`
- `-SkipPublishReadySnapshot`

So in live Step 3:

- this script is a data refresh worker only
- snapshot publish is intentionally deferred to the parent close script

### 7.4 `training_critical` mode step list

The script builds these step groups:

- candles second:
  - `plan_candles_second`
  - `collect_candles_second`
- lob30:
  - `plan_lob30`
  - `collect_lob30`
- micro:
  - `aggregate_micro_current_window`
  - `validate_micro_current_window`
- sequence tensors:
  - `collect_sequence_tensors`
  - `collect_sequence_tensors_prev1`
  - ...
  - one per explicit tensor date
- metadata:
  - `refresh_private_execution_label_store`
  - `refresh_data_contract_registry`
  - `refresh_dataset_retention_registry`

Current live server run for batch date `2026-04-05` produced:

- `47` total training-critical steps
- `38` explicit tensor dates
- all `47` exit codes were `0`

### 7.5 Effective tensor market count

When `Mode == training_critical` and explicit tensor markets are not passed:

- `effectiveTensorMaxMarkets = max(TensorMaxMarkets, TopN)`

With defaults:

- configured `TensorMaxMarkets = 20`
- `TopN = 50`
- effective = `50`

This matches the live summary:

- `tensor_max_markets_effective = 50`

### 7.6 Step tolerances inside refresh script

Two explicit fail-soft behaviors exist:

1. `collect_sequence_tensors`
   - if command exits non-zero
   - but `sequence_v1/_meta/build_report.json` shows `built_anchors > 0`
   - script tolerates the partial build and rewrites exit to `0`

2. `collect_ws_candles`
   - only relevant in `runtime_rich` or `full`
   - if collect wrote rows and validate has no fail files, partial ws-candle collect can be tolerated

Current Step 3 path does not execute `collect_ws_candles`, but the tensor partial-build tolerance is part of the active Step 3 code path.


## 8. Python Contracts Inside `refresh_data_platform_layers`

### 8.1 `plan_lob30.py`

`generate_lob30_collection_plan()`:

- validates market mode
- uses candle inventory / recent value estimate
- optional active-market filtering
- writes `lob30_plan.json`

Key plan contract:

- runtime policy:
  - websocket endpoint
  - `requested_depth = 30`
  - `orderbook_level = 0`
  - dedupe policy = `latest_by_market_ts_ms`

### 8.2 `lob30_collector.py`

`collect_lob30_from_plan()`:

- loads websocket settings
- enforces:
  - depth must be 30
  - orderbook_level must be 0
- subscribes to orderbook stream
- normalizes rows
- writes parquet partitions under `lob30_v1`
- appends manifest
- writes:
  - `lob30_collect_report.json`
  - `lob30_v1/_meta/build_report.json`

Important summary behavior:

- `failures` only blocks CLI exit when non-empty
- validate is a separate step after collect

### 8.3 `validate_lob30.py`

Checks per parquet part:

- required 30-level columns exist
- requested depth is exactly 30
- levels_present is at least 30
- grouped `level == 0`
- no crossed top-of-book
- monotonic level ordering

Status policy:

- `WARN` if no rows
- `FAIL` on schema / structural book violations

### 8.4 `merge_micro_v1.py`

`aggregate_micro_v1()` merges:

- raw ws trades
- raw REST ticks
- raw ws orderbook

into:

- `micro_v1`
- tf `1m` and `5m`

Important runtime behavior:

- `alignment_mode` can be `auto|start|end`
- in `auto`, it detects alignment once from base candles and locks the mode
- Step 3 uses:
  - `--mode overwrite`
  - explicit start/end window

Artifacts:

- `micro_v1/_meta/aggregate_report.json`
- `micro_v1/_meta/manifest.parquet`

### 8.5 `validate_micro_v1.py`

This is the main structural check for Step 3 micro data.

It validates:

- required columns
- ts monotonicity
- negative volume ratio
- nonpositive price ratio
- micro availability ratio
- trade/book coverage length
- join-match ratio vs base candles

Important rule:

- join-match ratio below warn threshold increments warn count
- below fail threshold increments fail count

Current defaults seen in code:

- join warn threshold = `0.98`
- join fail threshold = `0.90`

Current live server latest:

- `checked_files = 9046`
- `ok_files = 9035`
- `warn_files = 11`
- `fail_files = 0`
- `join_match_ratio_start = 0.998293...`
- `join_match_ratio_end = 0.858971...`
- chosen `alignment_mode = start`

This explains why micro validate currently passes overall even though some detailed warnings remain.

### 8.6 `sequence_tensor_store.py`

`build_sequence_tensor_store()` builds:

- per-anchor cached `.npz`
- `manifest.parquet`
- `build_report.json`
- `validate_report.json`
- `sequence_tensor_contract.json`
- `lob_tensor_contract.json`

Support levels:

- `strict_full`
- `reduced_context`
- `structural_invalid`

Build policy:

- anchor tensors with partial context become `WARN`
- only structural missing/bad cache causes `FAIL`

Validate policy:

- bad shape or missing cache = `FAIL`
- partial coverage = `WARN`

Current live server latest:

- build:
  - `discovered_anchors = 1545`
  - `built_anchors = 1545`
  - `ok_anchors = 0`
  - `warn_anchors = 1545`
  - `fail_anchors = 0`
- validate:
  - `checked_files = 110240`
  - `ok_files = 1307`
  - `warn_files = 108933`
  - `fail_files = 0`
- support-level counts:
  - `strict_full = 1307`
  - `reduced_context = 108933`
  - `structural_invalid = 0`

This means current Step 3 treats sequence/lob tensor reduced-context coverage as usable, not fatal.

This is consistent with downstream trainers:

- `support_level_weight(strict_full) = 1.0`
- `support_level_weight(reduced_context) = 0.5`
- `support_level_weight(other) = 0.0`

and strict eval prefers strict-full rows but falls back to full set when strict rows are insufficient.

So current sequence warning volume is real, but not automatically an operating failure.

### 8.7 `private_execution_label_store.py`

This step reads runtime state DBs and builds:

- `private_execution_v1`
- manifest
- label contract
- build report
- validate report

Validate policy:

- `FAIL` if rows empty
- `FAIL` if required columns missing
- `FAIL` if required key fields are null

Current live server latest:

- `rows_written_total = 273`
- build `status = PASS`
- validate `pass = true`

### 8.8 `data_contract_registry.py` and `dataset_retention_registry.py`

These are metadata registry builders.

`data_contract_registry.py` enumerates:

- raw ws
- raw ticks
- parquet datasets
- micro datasets
- feature datasets
- live contracts
- runtime contracts

It infers:

- status
- validation status
- source roots
- source contract ids
- source run ids
- retention class

`dataset_retention_registry.py` maps layers/contracts to:

- `hot`
- `warm`
- `cold`
- `archive`
- `scratch`

These are used later by lineage and certification.


## 9. Subscript B: `refresh_current_features_v4_contract_artifacts.ps1`

### 9.1 High-level role

This script refreshes the contract-bearing feature layer around `features_v4`.

It can optionally:

- rebuild micro
- validate micro
- build features
- validate features
- build live parity report
- rebuild registry / retention / lineage / certification

### 9.2 Current Step 3 call shape

Close script calls it as:

- `-StartDate <trainingCriticalStartDate>`
- `-EndDate <trainingCriticalEndDate>`
- `-TopN <FeatureTopN>`
- `-UseTopNUniverse`
- `-RequireExplicitWindow`
- `-SkipMicroRefresh`
- `-SkipMicroValidate`

So current Step 3 intentionally assumes:

- micro was already refreshed/validated by the prior `training_critical` refresh
- this phase should rebuild features and feature contracts only

### 9.3 Current live step list

Current live feature refresh summary shows exactly `7` steps:

1. `features_v4_build_contract_window`
2. `features_v4_validate_contract_window`
3. `features_v4_live_parity_contract_window`
4. `refresh_data_contract_registry`
5. `refresh_dataset_retention_registry`
6. `refresh_raw_to_feature_lineage_report`
7. `refresh_feature_dataset_certification`

All `7` exited `0`.


## 10. Python Contracts Inside Feature Refresh

### 10.1 `pipeline_v4.py` feature build

`build_features_dataset_v4()`:

- requires `feature_set = v4`
- requires `tf = 5m`
- resolves:
  - base candles root
  - micro root
  - explicit start/end
  - top-n universe
- if `require_micro_validate_pass = true`, it asserts micro validate pass for the requested range
- discovers micro market windows
- selects markets using `_select_v3_universe_markets()` from `pipeline_v3`
- builds per-market feature tables
- applies label contract
- writes partitions, manifest, feature spec, label spec, build report

Important detail:

- feature universe selection is quality-weighted on discovered micro windows and base-candle trade value
- it is **not** the same contract as Step 1/2 active-market filter

Observed live server nuance:

- current `features_v4` build report selected `50` markets
- that selected set includes `KRW-FLOW`
- Step 1/2 active-market filter had previously dropped `KRW-FLOW`

This is a real contract difference between:

- Step 1/2 source collection universe
- Step 3 feature-build universe

It is not yet proven to be an immediate operating bug, but it is a real universe mismatch.

### 10.2 `pipeline_v4.py` feature validate

`validate_features_dataset_v4()`:

- loads feature manifest
- selects top-n markets from manifest
- loads per-market partitions
- checks:
  - required columns
  - null ratios
  - ts monotonicity
  - leakage
  - staleness

Current live server latest:

- `checked_files = 50`
- `ok_files = 49`
- `warn_files = 1`
- `fail_files = 0`

The current warning market is:

- `KRW-AAVE`

Reason:

- `NO_ROWS`
- file selected in manifest, but zero feature rows remained after build / label contract

Current Step 3 treats this as non-fatal because validate fail count is still zero.

### 10.3 `live_feature_parity_report.py`

This is the strongest feature-contract gate inside Step 3.

It:

- samples offline rows from `features_v4`
- reconstructs live-feature rows through `LiveFeatureProviderV4`
- compares offline vs live columns
- requires:
  - sampled pairs > 0
  - compared pairs == sampled pairs
  - no hard-gate missing columns
  - all sampled pairs pass within tolerance

Current live server latest:

- `sampled_pairs = 20`
- `compared_pairs = 20`
- `passing_pairs = 20`
- `hard_gate_fail_count = 0`
- `acceptable = true`
- `status = PASS`

### 10.4 `feature_dataset_certification.py`

This builds the final pass/fail certification for `features_v4`.

It requires:

- build report present and pass-like
- validate report present and fail-free
- live parity report present and `PASS`
- feature contract registry entry present
- live feature contract registry entry present

Current live server latest:

- `pass = true`
- `status = PASS`

### 10.5 `raw_to_feature_lineage_report.py`

This is registry-derived lineage only.

It records:

- feature contract entry
- feature source contracts
- live feature contract entry
- live source contracts

It does not itself gate pass/fail.


## 11. Snapshot Publish

### 11.1 Publish contract

After both subordinate scripts succeed, close script runs:

- `python -m autobot.ops.data_platform_snapshot publish --project-root <root>`

Snapshot layout includes:

- `candles_second_v1`
- `ws_candle_v1`
- `lob30_v1`
- `sequence_v1`
- `private_execution_v1`
- `candles_api_v1`
- `features_v4`

Publish requirements:

- every dataset root must exist
- validate report must exist
- validate report must not be failed
- for `features_v4`:
  - `feature_dataset_certification.json` must exist and pass
  - `raw_to_feature_lineage_report.json` must exist

### 11.2 Real immutability problem

`autobot.ops.data_platform_snapshot` claims to be an immutable ready-snapshot publisher.

But the implementation copies files using:

- `_copytree_hardlink()`
- first choice = `os.link(source, target)`
- fallback = `shutil.copy2()`

This means snapshot files are hardlinked to source files whenever the filesystem allows it.

That would only be safe if all later writes replaced source files atomically without mutating the inode.
Current codebase does not satisfy that requirement consistently.

Examples:

- `feature_dataset_certification.py` writes with `path.write_text(...)`
- many build / validate scripts write directly to target paths
- parquet writers can overwrite existing source files in-place

### 11.3 Live server evidence that snapshot is not immutable

Current live close summary:

- `snapshot_id = 20260406T102824Z`
- close summary `generated_at_utc = 2026-04-06T10:28:29Z`

But on the live server:

- source and snapshot `feature_dataset_certification.json` share the same inode
- source and snapshot `features_v4/_meta/validate_report.json` share the same inode
- source and snapshot `raw_to_feature_lineage_report.json` share the same inode

Observed inode evidence:

- `feature_dataset_certification.json`
  - source inode = `3908539`
  - snapshot inode = `3908539`
- `validate_report.json`
  - source inode = `3898247`
  - snapshot inode = `3898247`

Observed mtime evidence:

- snapshot publish time:
  - `2026-04-06T10:28:28Z`
- current source and snapshot feature certification mtime:
  - `2026-04-06T14:43:18Z`
- current source and snapshot feature validate mtime:
  - `2026-04-06T14:39:55Z`

This means the supposed immutable snapshot copy moved forward with later source writes.

That is a real Step 3 bug.


## 12. Why The Snapshot Bug Is Operationally Real

This is not just a metadata purity issue.

Training commands actually resolve dataset roots from the ready snapshot when
`AUTOBOT_DATA_PLATFORM_READY_SNAPSHOT_ID` is set.

Relevant runtime path:

- `scripts/candidate_acceptance.ps1`
  - wraps native commands and sets `AUTOBOT_DATA_PLATFORM_READY_SNAPSHOT_ID`
- `autobot.cli`
  - `_resolve_data_platform_ready_dataset_root()`
  - uses snapshot payload `datasets[*].dataset_root`
- model train commands such as:
  - `v5_panel_ensemble`
  - `v5_sequence`
  - `v5_lob`
  - `v5_tradability`
  can therefore load dataset roots from the snapshot

So if the snapshot files keep moving after publish, then:

- `snapshot_id` no longer guarantees frozen data content
- provenance becomes weaker than the contract name implies
- same `snapshot_id` can observe later source mutations

This is the most important real Step 3 finding so far.


## 13. Current Live Step 3 Run

Latest live server close summary currently shows:

- `policy = v5_train_snapshot_close_v1`
- `batch_date = 2026-04-05`
- `generated_at_utc = 2026-04-06T10:28:29Z`
- `overall_pass = true`
- `failure_reasons = []`
- `snapshot_id = 20260406T102824Z`

Upstream freshness embedded in close summary:

- candles:
  - generated `2026-04-06T07:42:42Z`
  - age `5.04` min at close time
  - pass `true`
- ticks:
  - generated `2026-04-06T07:45:39Z`
  - age `2.08` min at close time
  - `batch_covered = true`
  - pass `true`

Training-critical refresh summary:

- `mode = training_critical`
- `start_date = 2026-02-27`
- `end_date = 2026-04-05`
- `window_source = explicit_date_range`
- `steps_total = 47`

Feature refresh summary:

- `start = 2026-02-27`
- `end = 2026-04-05`
- `refresh_argument_mode = explicit_date_range`
- `steps_total = 7`

Close summary window contract:

- `train_window = 2026-02-27 .. 2026-03-28`
- `certification_window = 2026-03-29 .. 2026-04-05`
- `coverage_window = 2026-02-27 .. 2026-04-05`

Micro coverage counts recorded in close summary:

- `41` distinct date keys


## 14. Conservative Findings

### F1. Immutable ready snapshot contract is currently false

Severity:

- real operating/provenance issue

Why:

- snapshot publisher hardlinks source files
- later in-place source writes mutate the snapshot
- training commands can use snapshot dataset roots directly

Current judgment:

- real bug
- not theoretical

### F2. Deadline is intentionally disabled in live chain

Severity:

- topology nuance, not a current bug

Why:

- live wrapper passes `-SkipDeadline`
- chain runs near the derived `00:20 KST` boundary

Current judgment:

- not a bug by itself
- but Step 3 "deadline contract" is not active in current nightly chain

### F3. Step 3 success tolerates large warning surfaces

Examples from current live run:

- micro validate warnings exist
- sequence build warnings are `1545 / 1545`
- sequence validate warnings are `108933 / 110240`
- features validate has `1` warning
- Step 3 still passes because fail counts are zero

Current judgment:

- currently looks intentional
- downstream sequence/lob trainers explicitly support `reduced_context`
- not enough evidence yet to call it a bug

### F4. Step 3 feature universe is not the same contract as Step 1/2 active-market filtering

Observed nuance:

- live `features_v4` selected set includes `KRW-FLOW`
- Step 1/2 active market filter had dropped `KRW-FLOW`

Current judgment:

- real contract difference
- not yet proven to be an immediate operating bug


## 15. Alignment With Blueprint / 2026-04-06 Intent

The intended contract direction from the platform documents is:

- stronger unified lineage
- stronger artifact-level certification
- frozen acceptance / compare semantics

That means Step 3 ready snapshot should behave like:

- a frozen provenance anchor
- a stable dataset-root resolution target for downstream training

and not like:

- a mutable alias over the current working tree

Because of that, the correct implementation direction is:

- independent copied snapshot files
- no shared hardlinks between source tree and snapshot tree
- snapshot id must continue to mean frozen content, not just a named pointer

This is the implementation direction now applied in `autobot.ops.data_platform_snapshot`.


## 16. Conservative Completion Check

This Step 3 pass has already covered:

- true nightly entrypoint
- live topology
- close-script helper semantics
- window contract and freshness contract
- both subordinate PowerShell scripts
- Python build/validate/certification/lineage/snapshot entry points
- live OCI latest artifact shapes
- actual downstream acceptance consumption contract
- real server evidence for snapshot mutability

What is still not safe to call closed:

- Step 3 implementation status

Reason:

- a real bug was found in the ready snapshot immutability contract

So the conservative judgment is:

- `Step 3 trace quality`: already strong
- `Step 3 implementation health`: **not closed yet**
- blocking finding:
  - `F1 immutable ready snapshot contract is false`


## 17. Final Judgment For Step 3

Step 3 is **not** ready for a clean completion judgment yet.

Not because the main chain is currently failing.
Current live run is passing.

It is not closed because a real, code-backed, live-server-confirmed issue exists:

- `data_platform_snapshot publish` does not actually produce immutable snapshots under current write behavior

That issue is material enough that Step 3 should remain open until it is either:

1. fixed in code, or
2. explicitly downgraded in the operating contract from "immutable snapshot" to a weaker pointer/frozen-best-effort contract
