# 00:20 Nightly End-to-End Consistency Audit

## 0. 목적

이 문서는 현재 `00:20` 야간 체인을 문서와 실제 코드 기준으로 다시 묶어서:

1. 입수 데이터부터 `latest_candidate/champion/live` 소비자까지의 실제 순서를 하나로 고정하고
2. 각 단계의 생성자-소비자 계약을 artifact 중심으로 정리하고
3. 서버/service/wrapper/script/CLI 각 레이어의 default 우선순위를 분리하고
4. "옵션이 빠졌을 때 무엇이 없어야 정상인가", "어디서 skip되고 어디서 fail해야 하는가", "어디가 실제 미스매치인가"를 보수적으로 문서화한다.

이 문서는 `2026-04-10` 시점의 repo 코드 기준이다.


## 1. 참조 문서

- `docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md`
- `docs/RUNTIME_VIABILITY_AND_RUNTIME_SOURCE_HARDENING_PLAN_2026-04-06.md`
- `docs/WIKI_0020_NIGHTLY_CYCLE_TRACE_STEP1_2026-04-07.md`
- `docs/WIKI_0020_NIGHTLY_CYCLE_TRACE_STEP2_2026-04-07.md`
- `docs/WIKI_0020_NIGHTLY_CYCLE_TRACE_STEP3_2026-04-07.md`
- `docs/WIKI_0020_NIGHTLY_CYCLE_TRACE_STEP4_2026-04-07.md`
- `docs/WIKI_0020_POST_ACCEPTANCE_HANDOFF_2026-04-08.md`


## 2. 실제 진입점

현재 `00:20` 체인의 진짜 입구는 acceptance script가 아니라 아래다.

1. `scripts/install_server_daily_v5_split_challenger_services.ps1`
2. `scripts/install_server_daily_split_challenger_services.ps1`
3. `autobot-v5-challenger-spawn.timer`
4. `autobot-v5-challenger-spawn.service`
5. `scripts/daily_champion_challenger_v5_for_server.ps1 -Mode spawn_only`

실제 중요한 contract는 다음 네 층이다.

- scheduler:
  - `OnCalendar=*-*-* 00:20:00`
- service:
  - `ExecStart`에 `ProjectRoot`, `PythonExe`, `Mode`, `ModelFamily`, `ChampionUnitName` 등을 직접 주입
- lock:
  - `/tmp/autobot-v5-nightly-train-chain.lock`
  - `flock -n`
  - busy 시 `exit 0`
- wrapper:
  - pre-chain 3단계 수행 후 기존 split wrapper로 위임

즉 현재 체인은 "00:20 timer -> service -> lock wrapper -> v5 wrapper -> pre-chain -> v4 split wrapper -> governed acceptance -> handoff" 구조다.


## 3. 기본값 우선순위

이 체인은 한 레이어의 default만 보면 틀리기 쉽다. 현재 우선순위는 아래다.

1. systemd service가 직접 주입한 값
2. wrapper script가 채우는 값
3. 하위 PowerShell step script가 채우는 값
4. 최종 `autobot.cli` 또는 Python module parser default

핵심 예시는 아래와 같다.

| 구간 | service/wrapper에서 결정되는 값 | step script default | raw CLI default | 실제 00:20 해석 |
| --- | --- | --- | --- | --- |
| Step 1 candles | `BatchDate` 계산만 하고 child로는 안 넘김 | `LookbackMonths=3`, `TopN=50`, `MaxBackfillDays1m=3`, `MaxRequests=120` | `plan-candles`는 `lookback_months=24`, `max_backfill_days_1m=90`; `collect candles`는 `dry-run=true` 기본 | nightly는 script-layer default가 우선이고, 시간 의미는 rolling UTC planner window |
| Step 2 raw ticks | wrapper가 `BatchDate=어제(KST)` 전달 | `TopN=50`, `DaysAgo=1`, `MaxPagesPerTarget=50` | `plan-ticks`/`collect ticks`의 `top_n=20`, `max_pages_per_target=500` | nightly는 `BatchDate -> days_ago` 변환 후 script-layer default로 실행 |
| Step 3 snapshot close | wrapper가 `BatchDate`와 `-SkipDeadline` 전달 | `TrainLookbackDays=30`, `BacktestLookbackDays=8`, freshness max age 120분 | 없음 | nightly는 deadline을 의도적으로 비활성화 |
| Step 4 acceptance | v5 governed wrapper가 trainer/feature/label/runtime gate 값을 강하게 고정 | `candidate_acceptance.ps1` 자체 default 존재 | 각 `autobot.cli` subcommand default 존재 | nightly는 generic acceptance가 아니라 governed v5 fusion lane |

따라서 현재 서버 동작을 해석할 때는 CLI help의 default만 보면 안 되고, 반드시 service -> wrapper -> step script -> CLI 순서로 따라가야 한다.


## 4. 실제 전체 순서

### 4.1 Step 1: `candles_api_refresh`

실행 경로:

- `scripts/daily_champion_challenger_v5_for_server.ps1`
- `scripts/run_candles_api_refresh.ps1`
- `python -m autobot.cli collect plan-candles`
- `python -m autobot.cli collect candles`

생성물:

- `data/collect/_meta/candle_topup_plan_auto.json`
- `data/collect/_meta/candle_validate_report.json`
- `data/collect/_meta/candles_api_refresh_latest.json`

소비자:

- 직접 소비자는 `scripts/close_v5_train_ready_snapshot.ps1`

중요 계약:

- wrapper는 `resolvedBatchDate`를 계산하지만 Step 1 child에는 넘기지 않는다.
- Step 1은 batch-date 기반 refresh가 아니라 rolling UTC inventory window 기반 source refresh다.
- close script는 Step 1 freshness를 볼 때 `batch_date`를 강제하지 않는다.
- 따라서 Step 1의 의미는 "오늘 00:20 배치용 캔들"보다 "지금 시점 기준 fresh한 candle source summary"에 가깝다.

absence/skip contract:

- `CandlesRefreshScript` path가 없으면 Step 1은 실패가 아니라 skip된다.
- skip 자체를 machine-readable artifact로 남기지는 않는다.
- 이후 Step 3에서 freshness summary가 stale/missing이면 그때 `CANDLES_API_REFRESH_STALE_OR_MISSING`로 실패한다.


### 4.2 Step 2: `raw_ticks_daily`

실행 경로:

- `scripts/daily_champion_challenger_v5_for_server.ps1`
- `scripts/run_raw_ticks_daily.ps1`
- `python -m autobot.cli collect plan-ticks`
- `python -m autobot.cli collect ticks --mode daily`
- `python -m autobot.cli collect ticks validate --date <UTC_DATE>`

생성물:

- `data/raw_ticks/upbit/_meta/ticks_plan_daily_auto.json`
- `data/raw_ticks/upbit/_meta/ticks_daily_latest.json`
- raw partitions:
  - `data/raw_ticks/upbit/trades/date=<UTC_DATE>/market=<MARKET>/part-*.jsonl.zst`

소비자:

- 직접 소비자는 `scripts/close_v5_train_ready_snapshot.ps1`
- 간접 소비자는 `micro aggregate`, `features_v4`, `private_execution_v1`, runtime dataset chain

중요 계약:

- wrapper는 `BatchDate = local yesterday`를 넘긴다.
- step script는 `BatchDate -> days_ago`를 계산한다.
- 그러나 실제 target date와 `validate_dates`는 `UTC now - days_ago`로 계산한다.
- 즉 Step 2 summary에는 동시에
  - `batch_date = local KST 기준 날짜`
  - `validate_dates = UTC 기준 날짜`
  가 기록된다.

이 구간의 실제 미스매치는 아래다.

1. producer는 `batch_date`와 `validate_dates`를 같이 기록한다.
2. Step 3 consumer인 `Get-SourceFreshnessResult`는 `batch_date`가 있으면 그것만 먼저 비교한다.
3. 그래서 `batch_date=2026-04-06`, `validate_dates=['2026-04-05']` 같은 local/UTC 혼재가 있어도 Step 3는 `batch_covered=true`로 통과할 수 있다.

즉 현재 구조는 "실제 수집/검증된 UTC target date"보다 "summary top-level batch_date 문자열"을 더 강하게 믿는다.

absence/skip contract:

- `RawTicksDailyScript` path가 없으면 Step 2도 실패가 아니라 skip된다.
- Step 3는 summary freshness만 보고 실패 여부를 정한다.
- 기존 fresh summary가 남아 있으면 이번 run이 Step 2를 실제로 돌리지 않았더라도 downstream이 그 사실을 즉시 모를 수 있다.


### 4.3 Step 3: `train_snapshot_close`

실행 경로:

- `scripts/daily_champion_challenger_v5_for_server.ps1`
- `scripts/close_v5_train_ready_snapshot.ps1`
- `scripts/refresh_data_platform_layers.ps1 -Mode training_critical -SkipPublishReadySnapshot`
- `scripts/refresh_current_features_v4_contract_artifacts.ps1`
- `python -m autobot.ops.data_platform_snapshot publish`

생성물:

- `data/collect/_meta/train_snapshot_training_critical_refresh_latest.json`
- `data/features/features_v4/_meta/nightly_train_snapshot_contract_refresh.json`
- `data/collect/_meta/train_snapshot_close_latest.json`
- `data/_meta/data_platform_ready_snapshot.json`
- `data/snapshots/data_platform/<snapshot_id>/...`

현재 close script가 실제로 gate하는 것:

- Step 1 summary freshness
- Step 2 summary freshness
- training-critical refresh exit code
- feature contract refresh exit code
- snapshot publish exit code
- deadline check

snapshot publish가 요구하는 것:

- dataset root 존재
- validate report 존재 및 failure 없음
- `features_v4`는 추가로
  - `feature_dataset_certification.json` pass
  - `raw_to_feature_lineage_report.json` 존재

중요 계약:

- live chain은 wrapper에서 항상 `-SkipDeadline`를 넘긴다.
- 즉 현재 `00:20` 운영 경로에서는 deadline은 사실상 enforced되지 않는다.
- Step 3는 `training_critical` refresh 내부 publish를 꺼두고, close script 맨 끝에서 snapshot publish를 한 번 더 단일 지점에서 수행한다.
- `refresh_data_platform_layers.ps1`의 training-critical mode는 `sequence_v1` 생성 시 `--skip-existing-ready true`를 사용한다.
- Step 3 summary는 `snapshot_id`, `coverage_window`, `train_window`, `certification_window`, `micro_date_coverage_counts`, `features_v4_effective_end`를 남기고 Step 4가 이 값을 hard gate로 읽는다.

absence/skip contract:

- `TrainSnapshotCloseScript` path가 없으면 pre-chain wrapper는 Step 3을 skip할 수 있다.
- 하지만 Step 4는 `train_snapshot_close_latest.json`을 hard preflight로 읽기 때문에, summary가 없거나 mismatch면 `failure_stage = data_close`로 조기 종료된다.


### 4.4 Step 4: `governed candidate acceptance`

실행 경로:

- `scripts/daily_champion_challenger_v4_for_server.ps1`
- `scripts/v5_governed_candidate_acceptance.ps1`
- `scripts/candidate_acceptance.ps1`

현재 nightly에서 실제로 고정되는 lane 값:

- `ModelFamily = train_v5_fusion`
- `Trainer = v5_fusion`
- `DependencyTrainers = v5_panel_ensemble, v5_sequence, v5_lob, v5_tradability`
- `FeatureSet = v4`
- `LabelSet = v3`
- `RunScope = scheduled_daily`
- `PromotionPolicy = paper_final_balanced`
- `TrainerEvidenceMode = required`

high-level phase:

1. `train_snapshot_close_preflight`
2. train window ramp / split policy
3. features build/validate/parity/certification
4. private execution label store
5. dependency trainer chain
6. dependency runtime export chain
7. candidate train
8. runtime dataset coverage preflight
9. runtime viability preflight
10. runtime deploy-contract preflight
11. acceptance backtest
12. runtime parity backtest
13. paper soak
14. fusion evidence / variant selection
15. `latest_candidate` update
16. `model promote`
17. runtime unit restart
18. artifact status update

Step 4의 핵심 snapshot contract:

- `train_snapshot_close_latest.json`의 `snapshot_id`가 현재 `data_platform_ready_snapshot.json`과 일치해야 한다.
- dependency trainers는 `data_platform_ready_snapshot_id`가 모두 같아야 재사용 가능하다.
- `train_v5_fusion.py`는 expert 입력의 `data_platform_ready_snapshot_id`가 모두 같은 non-empty 값이 아니면 실패한다.
- `train_v5_fusion.py`는 runtime input bundle에서도 공통 runtime universe와 runtime viability를 다시 계산한다.

현재 snapshot read 정책:

- `candidate_acceptance.ps1`는 snapshot artifact를 우선 사용한다.
- 우선 사용 대상:
  - `features_v4/_meta/build_report.json`
  - `validate_report.json`
  - `live_feature_parity_report.json`
  - `feature_dataset_certification.json`
  - `data/_meta/data_contract_registry.json`
  - `private_execution_v1/_meta/build_report.json`
  - `private_execution_v1/_meta/validate_report.json`
- snapshot artifact가 없을 때만 current root fallback으로 내려간다.

즉 현재 Step 4는 완전한 "mutable root only"는 아니지만, 엄밀한 "snapshot only"도 아니다.

pointer/status contract:

- `overall_pass=true`여도 dry-run이면 pointer를 바꾸지 않는다.
- `overall_pass=false`면 `latest_candidate`는 업데이트되지 않는다.
- `SkipPaperSoak=true`면 promote는 하지 않는다.
- `SkipPromote=true`면 champion은 바뀌지 않는다.
- restart는 promote 성공 후에만 실행된다.
- run artifact status는 마지막에
  - `champion`
  - `candidate_adopted`
  - `candidate_adoptable`
  - `acceptance_completed`
  중 하나로 정리된다.


### 4.5 Post-acceptance handoff

실행 경로:

- `scripts/daily_champion_challenger_v4_for_server.ps1`
- `scripts/adopt_v5_candidate_for_server.ps1`
- `scripts/adopt_v4_candidate_for_server.ps1`
- `scripts/install_server_runtime_services.ps1`
- `autobot/models/registry.py`
- `autobot/paper/engine.py`
- `autobot/live/model_handoff.py`
- `autobot/live/daemon.py`

실제 handoff가 하는 일:

1. `latest_candidate` pointer 재기록
2. paired paper runtime install/start
3. `current_state.json` 기록
4. candidate target unit restart
5. artifact_status를 `candidate_adopted`로 승격

중요 pointer semantics:

- `latest`:
  - latest trained run
- `latest_candidate`:
  - acceptance-adopted candidate
- `champion`:
  - production/paper champion

중요 consumer semantics:

- champion paper preset:
  - `RuntimeModelRef = champion`
  - `BootstrapRefs = latest_candidate, latest`
- candidate paper preset:
  - `RuntimeModelRef = latest_candidate`
  - bootstrap 없음
- live daemon:
  - `latest_candidate`를 요청했는데 pointer가 없으면
  - 이전 pinned run이 있으면
  - `previous_pinned_run` fallback으로 살아남을 수 있다

즉 정상 흐름에서는 `latest_candidate`를 먼저 쓰고 candidate target을 restart해야 한다. 그렇지 않으면 candidate paper/canary/live가 새 후보가 아닌 이전 pinned run에 남을 수 있다.


### 4.6 단계별 summary/report schema와 실제 consumer 읽기 범위

#### Step 1 summary: `candles_api_refresh_latest.json`

생성 key:

- `policy`
- `generated_at_utc`
- `project_root`
- `python_exe`
- `meta_dir`
- `plan_path`
- `validate_report_path`
- `steps`

consumer가 실제로 강하게 읽는 것:

- Step 3 `Get-SourceFreshnessResult`
  - `generated_at_utc|generated_at`
  - `steps[*].exit_code`

consumer가 읽지 않는 것:

- batch date 관련 key
  - 애초에 없음
- plan window 의미
- selected/processed target 수

판정:

- Step 1 summary는 freshness contract이지 batch contract가 아니다.


#### Step 2 summary: `ticks_daily_latest.json`

생성 key:

- `policy`
- `generated_at_utc`
- `project_root`
- `python_exe`
- `raw_root`
- `meta_dir`
- `plan_path`
- `batch_date`
- `days_ago`
- `validate_dates`
- `steps`

consumer가 실제로 강하게 읽는 것:

- Step 3 `Get-SourceFreshnessResult`
  - `generated_at_utc|generated_at`
  - `steps[*].exit_code`
  - `batch_date` 우선
  - 없으면 `validate_dates`
  - 그것도 없으면 `validate_raw_ticks_<batchDate>` step name

판정:

- Step 2는 producer가 richer metadata를 남기지만 current consumer는 그중 일부만 선택적으로 읽는다.
- 여기서 local/UTC 혼재가 가려진다.


#### Step 3 summary: `train_snapshot_close_latest.json`

생성 key:

- `policy`
- `generated_at_utc`
- `batch_date`
- `source_freshness`
- `training_critical_steps`
- `training_critical_refresh`
- `feature_contract_refresh`
- `micro_date_coverage_counts`
- `train_window`
- `certification_window`
- `coverage_window`
- `training_critical_start_date`
- `training_critical_end_date`
- `features_v4_effective_start`
- `features_v4_effective_end`
- `snapshot_id`
- `snapshot_root`
- `snapshot_summary_path`
- `pointer_path`
- `deadline_enforced`
- `deadline_met`
- `overall_pass`
- `failure_reasons`

consumer가 실제로 강하게 읽는 것:

- Step 4 `Resolve-TrainSnapshotCloseContract`
  - `batch_date`
  - `snapshot_id`
  - `overall_pass`
  - `deadline_met`
  - `source_freshness`
  - `micro_date_coverage_counts`
  - `coverage_window`
  - `train_window`
  - `certification_window`
  - `features_v4_effective_end`

판정:

- Step 3 summary는 `00:20` 체인의 핵심 SSOT에 가장 가깝다.
- Step 4 전체가 사실상 이 문서를 첫 gate로 삼는다.


#### Step 4 acceptance report

가장 중요한 step bucket:

- `train_snapshot_close_preflight`
- `features_validate`
- `live_feature_parity`
- `feature_dataset_certification`
- `private_execution_label_store`
- `dependency_trainers`
- `dependency_runtime_export`
- `train`
- `runtime_dataset_coverage_preflight`
- `runtime_viability_preflight`
- `runtime_deploy_contract_preflight`
- `backtest_candidate`
- `backtest_runtime_parity_candidate`
- `paper_candidate`
- `fusion_variant_selection`
- `update_latest_candidate`
- `promote`

판정:

- Step 4 report는 단순 pass/fail 문서가 아니라 downstream forensic root다.
- 현재 0-fill/미채택 이슈를 보려면 사실상 여기서부터 다시 내려가야 한다.


### 4.7 fusion trainer 산출물 묶음과 downstream 연결

`train_v5_fusion.py`가 현재 쓰는 핵심 run artifact는 아래다.

- `fusion_model_contract.json`
- `predictor_contract.json`
- `entry_boundary_contract.json`
- `fusion_input_contract.json`
- `fusion_runtime_input_contract.json`
- `runtime_recommendations.json`
- `runtime_viability_report.json`
- `runtime_deploy_contract_readiness.json`
- `promotion_decision.json`
- `execution_acceptance_report.json`
- `trainer_research_evidence.json`
- `economic_objective_profile.json`
- `lane_governance.json`
- `decision_surface.json`
- `artifact_status.json`

주요 소비자:

- predictor loader:
  - `predictor_contract.json`
  - `entry_boundary_contract.json`
- Step 4 runtime preflight:
  - `fusion_runtime_input_contract.json`
  - `runtime_viability_report.json`
  - `runtime_deploy_contract_readiness.json`
- variant selection:
  - `runtime_viability_report.json`
  - `runtime_deploy_contract_readiness.json`
  - `runtime_recommendations.json`
- backtest/paper/live:
  - predictor bundle + runtime recommendations + strategy contracts

중요 nuance:

- `finalize_v5_expert_family_run()`는 `publish_family_latest=true`일 때만 `latest`를 갱신한다.
- fusion path에서는 이 값이 현재
  - `runtime_viability_pass`
  - `runtime_deploy_contract_ready`
  둘 다 true일 때만 켜진다.
- 즉 fusion `latest`는 pure train-complete pointer가 아니라, 이미 runtime viability/deploy readiness를 한번 통과한 run만 가리키는 쪽에 더 가깝다.


### 4.8 `orders_filled=0` 디버깅 ladder

현재 코드 기준으로는 아래 순서로 좁혀가는 것이 가장 안전하다.

#### L1. feature row 자체가 없는가

증거:

- backtest/paper `selection_per_ts`
- strategy `skipped_missing_features_rows`
- live feature provider `last_build_stats().skip_reasons`

대표 원인:

- `NO_BASE_FEATURE_ROW`
- `V5_FEATURE_VALUES_UNAVAILABLE`
- `MISSING_V5_FEATURE_COLUMNS`
- `NO_FEATURE_ROWS_AT_TS`
- `NO_ACTIVE_MARKET_FEATURE_ROWS`


#### L2. feature row는 있는데 entry gate에서 intent가 0인가

증거:

- `intent_created_count`
- opportunity log의 `skip_reason_code`
- `entry_decision.reason_codes`
- `runtime_viability_report.top_entry_gate_reason_codes`

대표 원인:

- `ENTRY_GATE_ALPHA_LCB_NOT_POSITIVE`
- `ENTRY_GATE_EDGE_NOT_POSITIVE`
- `ENTRY_BOUNDARY_TRADABILITY_BELOW_THRESHOLD`
- `ENTRY_GATE_PORTFOLIO_BUDGET_BLOCKED`
- `ENTRY_GATE_BREAKER_ACTIVE`
- `ENTRY_GATE_ROLLOUT_BLOCKED`


#### L3. intent는 생겼는데 submit 전 policy layer에서 0인가

증거:

- `candidates_aborted_by_policy`
- `micro_order_policy_report.json`
- event log:
  - `MICRO_ORDER_POLICY_ABORT`
  - `EXECUTION_POLICY_ABORT`
  - `PORTFOLIO_BUDGET_BLOCKED`
  - `TRADE_GATE_BLOCKED`

대표 원인:

- `ZERO_VOLUME`
- micro policy abort
- execution policy utility skip
- portfolio budget rejection
- trade gate rejection


#### L4. submit는 됐는데 fill이 0인가

증거:

- `orders_submitted > 0`
- `orders_filled = 0`
- `orders_canceled`
- `replaces_total`
- `aborted_timeout_total`
- `dust_abort_total`
- `fill_events_total = 0`

대표 원인:

- price mode/timeout mismatch
- replace loop
- cross/passive policy가 실거래성에 안 맞음
- micro policy는 allow했지만 fill utility가 실제로 낮음


#### L5. live만의 별도 분기인가

증거:

- `submitted_intents_total`
- `shadow_intents_total`
- `skipped_intents_total`
- `rollout_order_emission_allowed`
- `halted_reasons`

대표 원인:

- submit path 자체가 shadow
- executor 미연결
- admissibility snapshot/build 실패
- breaker halt

판정:

- 이 ladder를 건너뛰고 마지막 `orders_filled=0`만 보면, 위에서 artifact를 안 만들고 있는 문제와 아래에서 체결을 못 받는 문제를 섞어 읽게 된다.


### 4.9 dependency trainer 재사용 계약과 runtime export 연결

Step 4에서 dependency trainer는 "같은 trainer family니까 재사용"이 아니라 아래가 맞아야 재사용된다.

- `trainer`
- `model_family`
- `feature_set`
- `label_set`
- `task`
- `run_scope`
- `tf`
- `quote`
- `top_n`
- `train start/end`
- `execution_acceptance_eval start/end`
- `seed`
- `data_platform_ready_snapshot_id`
- `artifact_status.core_saved`
- `artifact_status.support_artifacts_written`
- `artifact_status.expert_prediction_table_complete`

추가 trainer-specific rule:

- `v5_panel_ensemble`
  - main trainer가 fusion이 아니면 `dependency_expert_only` 요구 여부까지 맞아야 한다.

runtime export 쪽에서 추가로 맞춰야 하는 것:

- dependency runtime export 결과의 snapshot id 일치
- 공통 runtime market universe 존재
- 각 expert runtime input window 정합성

대표 failure code:

- `SNAPSHOT_ID_MISMATCH`
- `PANEL_FULL_RUNTIME_CONTRACT_REQUIRED`
- `PANEL_DEPENDENCY_EXPERT_ONLY_REQUIRED`
- `COMMON_RUNTIME_UNIVERSE_EMPTY`

판정:

- 이 계약 때문에 Step 4는 "전날 학습 결과가 있으니 그냥 재사용"이 아니라 "같은 snapshot close에 묶인 expert chain만 재사용" 구조다.
- 따라서 Step 3 snapshot close가 흔들리면 dependency trainer 재사용부터 깨지고, 그 여파가 fusion input/runtime viability까지 전파된다.


## 5. 생성자-소비자 계약 매트릭스

| 생성자 | 핵심 산출물 | 직접 소비자 | 꼭 맞아야 하는 것 | 없을 때 정상 동작 |
| --- | --- | --- | --- | --- |
| `run_candles_api_refresh.ps1` | `candles_api_refresh_latest.json` | `close_v5_train_ready_snapshot.ps1` | fresh timestamp, `steps[*].exit_code==0` | path 없으면 wrapper skip, 이후 close가 stale/missing으로 실패해야 함 |
| `run_raw_ticks_daily.ps1` | `ticks_daily_latest.json` | `close_v5_train_ready_snapshot.ps1` | fresh timestamp, `steps[*].exit_code==0`, `batch_date` string | path 없으면 wrapper skip, 이후 close가 stale/missing으로 실패해야 함 |
| `refresh_data_platform_layers.ps1` | training-critical datasets + summary | `close_v5_train_ready_snapshot.ps1` | exit code 0, required validate reports 존재 | 실패 시 Step 3는 `TRAINING_CRITICAL_REFRESH_FAILED` |
| `refresh_current_features_v4_contract_artifacts.ps1` | build/validate/parity/certification reports | `close_v5_train_ready_snapshot.ps1`, `candidate_acceptance.ps1` | `features_v4` contract artifacts usable | 실패 시 Step 3는 `FEATURE_CONTRACT_REFRESH_FAILED` |
| `autobot.ops.data_platform_snapshot publish` | snapshot tree + ready pointer | `candidate_acceptance.ps1`, dependency trainers, fusion trainer | validate/certification/lineage 모두 준비 | 실패 시 Step 3 전체 실패 |
| dependency trainers/export | expert prediction tables + `data_platform_ready_snapshot_id` | `train_v5_fusion.py` | snapshot id 동일, required artifact complete | mismatch면 재사용 금지 또는 Step 4 실패 |
| `train_v5_fusion.py` | `runtime_recommendations.json`, `fusion_runtime_input_contract.json`, `runtime_viability_report.json`, `runtime_deploy_contract_readiness.json` | backtest/paper/live acceptance gates, variant selection | same snapshot id, runtime universe non-empty, viability/deploy readiness | 실패 시 candidate는 `latest`만 될 수 있어도 `latest_candidate`는 안 됨 |
| `candidate_acceptance.ps1` | acceptance report, `latest_candidate`, artifact_status, optional champion promote | adopt script, paper/live runtime | `overall_pass`, lane governance, promote flags | fail 시 `latest_candidate`/`champion`은 그대로여야 함 |
| `adopt_v4_candidate_for_server.ps1` | paired paper start, candidate target restart, `current_state.json` | candidate paper/canary/live | `artifact_status` 필수 필드 true, `latest_candidate` 먼저 기록 | candidate target 없으면 restart step은 `NO_CANDIDATE_TARGET_UNITS` |


## 6. 옵션 부재 시 생략되어야 하는 것

### 6.1 service -> wrapper arg omission

`scripts/install_server_daily_split_challenger_services.ps1::Build-ExecStart`는 아래 인자를 비어 있으면 아예 붙이지 않는다.

- `-ChampionCompareModelFamily`
- `-AcceptanceScript`
- `-RuntimeInstallScript`
- `-CandidateAdoptionScript`
- `-PairedPaperModelFamily`
- `-PromotionTargetUnits`

추가 규칙:

- `-CandidateTargetUnits`는
  - `ModeName == "spawn_only"` 이고
  - 값이 있을 때만
  service command에 붙는다.
- `promote_only` path에는 candidate target unit 인자가 원천적으로 안 붙는다.

즉 옵션이 비어 있으면 downstream wrapper에서 그 계약이 "없음"으로 읽혀야 한다. 빈 문자열이 의미를 갖는 것이 아니라 아예 arg omission이 의미를 갖는다.


### 6.2 v5 wrapper step omission

`daily_champion_challenger_v5_for_server.ps1`는 pre-chain step path가 없으면 아래를 skip한다.

- Step 1 candles
- Step 2 raw ticks
- Step 3 close

이때 즉시 실패 report를 남기는 것은 "step 실행 후 실패" 케이스뿐이다.
path 자체가 없어서 아예 실행하지 않은 경우는 pre-chain wrapper failure report조차 없다.


### 6.3 Step 4 omission rules

`candidate_acceptance.ps1`에서 아래는 없거나 false여야 정상이다.

- `overall_pass=false`
  - `steps.update_latest_candidate.updated` 없어야 한다
  - `steps.promote.promoted` 없어야 한다
  - `champion` pointer는 바뀌면 안 된다
- `SkipPromote=true`
  - `steps.promote.reason = SKIPPED_BY_FLAG`
  - `restart_units`는 비어야 한다
- `SkipPaperSoak=true`
  - `steps.paper_candidate`는 skip
  - `steps.promote.reason = SKIPPED_PAPER_SOAK_REQUIRES_MANUAL_PROMOTE`
- `DryRun=true`
  - pointer update 없음
  - artifact_status mutation 없음
  - `gates.overall_pass = null`


### 6.4 candidate handoff omission rules

`adopt_v4_candidate_for_server.ps1`에서:

- `CandidateRunId`가 없으면 adoption 자체가 실패해야 한다.
- `artifact_status` 필수 필드가 하나라도 false면 adoption은 blocked되어야 한다.
- `CandidateTargetUnits`가 없으면
  - `restart_candidate_targets.attempted = false`
  - `reason = NO_CANDIDATE_TARGET_UNITS`
  이어야 한다.


### 6.5 runtime install omission rules

`scripts/install_server_runtime_services.ps1`는 champion preset에 대해서만 pointer 부재를 강하게 막는다.

- `live_v5`/`offline_v5`
  - champion pointer가 없으면 install/start를 막거나 bootstrap을 요구
- `candidate_v5`
  - default source가 `latest_candidate`
  - bootstrap 강제 없음

따라서 candidate lane은 "설치 스크립트가 candidate pointer 존재를 보장한다"가 아니라 "adopt step이 pointer를 먼저 만들고 그 다음 restart한다"가 현재 운영 계약이다.


## 7. 확정된 정합성 이슈

### F1. Step 1은 batch-date driven step이 아니다

사실:

- wrapper는 `BatchDate`를 계산한다.
- Step 1 child에는 전달하지 않는다.
- Step 1 planner는 current UTC 기반 rolling inventory window를 사용한다.
- Step 3도 Step 1에 대해 batch-date를 검증하지 않는다.

판정:

- 의도적일 수는 있지만 strict batch chain 관점에서는 계약 차이가 있다.
- Step 1은 `D 배치 데이터 생성`이라기보다 `rolling source refresh`다.


### F2. Step 2는 local batch date와 UTC target date를 동시에 쓰고, Step 3는 그 차이를 가린다

사실:

- Step 2 summary는 `batch_date`와 `validate_dates`를 같이 남긴다.
- `validate_dates`는 UTC 기준이다.
- Step 3 freshness consumer는 `batch_date`가 있으면 그 값만 우선 비교한다.

판정:

- 실제 producer-consumer mismatch다.
- 현재 consumer는 "무슨 UTC 날짜를 수집/검증했는가"보다 "summary에 적힌 batch_date 문자열"을 더 강하게 신뢰한다.
- `00:20 KST`에서는 이 차이가 가장 잘 드러난다.


### F3. pre-chain step path 부재와 lock-busy는 silent skip에 가깝다

사실:

- systemd lock은 busy 시 `exit 0`이다.
- pre-chain step path가 없으면 wrapper는 skip한다.
- 이 skip들은 현재 machine-readable skip artifact가 약하다.

판정:

- 운영 관찰성 문제다.
- "이번 run에서 안 돌았다"와 "돌았지만 실패했다"를 구분하기 어렵다.


### F4. Step 4는 snapshot-first지만 아직 strict snapshot-only는 아니다

사실:

- snapshot root가 있으면 frozen artifact를 먼저 읽는다.
- snapshot artifact가 없으면 current root fallback으로 내려간다.

판정:

- `04-06` hardening 방향에는 맞다.
- 다만 strict frozen acceptance를 주장하려면 대표 rerun으로 mixed-read가 실제 사라졌는지 더 확인해야 한다.


### F5. `latest`, `latest_candidate`, `champion`은 의도적으로 다르다

사실:

- train 성공 시 `latest`는 바뀔 수 있다.
- acceptance 실패 시 `latest_candidate`는 비어 있을 수 있다.
- promote 전까지 `champion`은 안 바뀐다.

판정:

- 버그가 아니라 계약이다.
- candidate-facing lane, dashboard, runbook이 `latest`를 candidate 의미로 읽으면 안 된다.


### F6. candidate runtime는 pointer가 없을 때 이전 pinned run으로 남을 수 있다

사실:

- live daemon은 `latest_candidate`가 안 풀리면 `previous_pinned_run` fallback을 허용한다.

판정:

- restart 안정성에는 유리하다.
- 하지만 "현재 pointer가 없다"와 "현재 runtime이 새 후보를 정확히 가리킨다"를 동일시하면 안 된다.


### F7. historical backtest CLI mismatch는 artifact에는 남아 있지만 현재 parser bug로 단정하면 안 된다

사실:

- 과거 artifact에는 `--micro-order-policy off` parser mismatch가 기록되어 있다.
- 현재 코드 기준 parser는 그 인자를 받는다.

판정:

- historical failure로는 유효하다.
- current reproducible bug로는 확인되지 않았다.


### F8. v5 unit인데 service description이 아직 V4로 기록된다

사실:

- generic installer의 description 문자열은 여전히 `Autobot V4 Challenger ...`다.

판정:

- 동작 blocker는 아니다.
- 하지만 운영 로그/대시보드 가독성 관점의 관찰성 mismatch다.


### F9. `orders_filled=0`는 하나의 문제가 아니라 최소 5단계 차단 결과다

사실:

- `ModelAlphaStrategyV1.on_ts()`는 먼저 feature row가 있어야 하고, 그 다음 `final_expected_return`, `final_expected_es`, `final_tradability`, `final_uncertainty`, `final_alpha_lcb`를 predictor contract에서 붙인다.
- 그 뒤 `resolve_v5_entry_gate()`가 `ENTRY_GATE_ALPHA_LCB_NOT_POSITIVE`, `ENTRY_GATE_EDGE_NOT_POSITIVE`, `ENTRY_GATE_PORTFOLIO_BUDGET_BLOCKED`, `ENTRY_GATE_BREAKER_ACTIVE`, `ENTRY_GATE_ROLLOUT_BLOCKED` 등으로 intent 자체를 막을 수 있다.
- intent가 생겨도 backtest/paper는 다시 `portfolio_budget`, `trade_gate`, `micro_order_policy`, `execution_policy`에서 submit 직전 차단될 수 있다.
- live는 여기에 추가로 `chance/accounts/instruments/admissibility`, `rollout order_emission_allowed`, `executor_gateway` 유무가 더 붙는다.
- 실제 `orders_filled` 카운터는 backtest/paper에서 `update.orders_with_fill`이 생긴 뒤에야 증가한다.

판정:

- 따라서 `orders_filled=0`만 보고 execution/fill 문제로 단정하면 안 된다.
- 최소한 아래를 분리해서 봐야 한다.
  - `intent_created_count = 0`
  - `intent_created_count > 0 && orders_submitted = 0`
  - `orders_submitted > 0 && orders_filled = 0`


### F10. strategy runtime row는 predictor feature columns 외에도 별도 숨은 필드 계약을 가진다

사실:

- `ModelPredictor`는 `train_config.feature_columns` 또는 `dataset_root` spec에서 predictor 입력 컬럼을 읽는다.
- 하지만 `ModelAlphaStrategyV1.on_ts()`는 predictor 입력 외에 아래 필드를 row에서 직접 읽는다.
  - base/runtime:
    - `market`, `close`, `model_prob`, `model_prob_raw`, `score_mean`, `score_std`, `score_lcb`
  - predictor post-contract:
    - `final_expected_return`, `final_expected_es`, `final_tradability`, `final_uncertainty`, `final_alpha_lcb`
  - micro/state quality:
    - `m_trade_events`, `m_book_events`, `m_trade_coverage_ms`, `m_book_coverage_ms`
    - `m_trade_imbalance`, `m_spread_proxy`
    - `m_depth_bid_top5_mean`, `m_depth_ask_top5_mean`, `m_depth_top5_notional_krw`
    - `m_best_bid_price`, `m_best_ask_price`, `m_best_bid_notional_krw`, `m_best_ask_notional_krw`
    - `m_micro_available`, `m_micro_book_available`, `m_trade_volume_base`, `m_trade_max_ts_ms`, `m_book_max_ts_ms`

판정:

- 이 계약은 "학습 feature columns"와 "실행 decision columns"가 완전히 같지 않다는 뜻이다.
- 따라서 dataset build/feature provider가 predictor 입력 컬럼만 맞추고 위 보조 필드를 놓치면, 모델은 돌아도 strategy gate/operational overlay/logging이 비정상화될 수 있다.


### F11. `LiveFeatureProviderV5`는 행을 0으로 채우는 것이 아니라 조건 미충족 시 row 자체를 버린다

사실:

- `LiveFeatureProviderV5.build_frame()`는 `LiveFeatureProviderV4Native`의 base row를 먼저 요구한다.
- 그 다음 `FUSION` 모드에서는
  - panel predictor
  - sequence predictor
  - lob predictor
  - online second/micro/lob payload
  를 모두 묶어 `feature_values`를 만든다.
- 실패 시 skip reason이 분리된다.
  - `NO_BASE_FEATURE_ROW`
  - `V5_FEATURE_VALUES_UNAVAILABLE`
  - `MISSING_V5_FEATURE_COLUMNS`
- 코드상 missing column은 `0.0`으로 한번 채우지만, `missing_columns`가 하나라도 있으면 그 row는 최종적으로 `continue`되어 frame에 들어가지 않는다.

판정:

- 현재 `LIVE_V5`는 "부족한 feature를 0으로 대체하고 계속 간다"가 아니라 "row contract가 완성되지 않으면 row를 폐기한다"가 실제 동작이다.
- 그 결과 아래쪽 strategy에서는
  - `frame is None/empty`
  - `NO_FEATURE_ROWS_AT_TS`
  - `NO_ACTIVE_MARKET_FEATURE_ROWS`
  로만 보일 수 있다.
- 즉 live에서 `orders_filled=0`가 보일 때는 predictor나 주문엔진보다 먼저 `feature_provider.last_build_stats().skip_reasons`를 봐야 한다.


### F12. backtest/paper/live consumer는 `candidate.meta`의 중간 계약을 연쇄적으로 소비한다

사실:

- `ModelAlphaStrategyV1`가 entry intent를 만들 때 meta에 아래 문서를 같이 넣는다.
  - `entry_decision`
  - `sizing_decision`
  - `trade_action`
  - `risk_control_static`
  - `size_ladder_static`
  - `execution_decision`
  - `exec_profile`
  - `model_exit_plan`
  - `liquidation_policy`
- backtest/paper 엔진은 여기서 다시 아래를 읽는다.
  - `decision_contract_version`
  - `sizing_decision.target_notional_quote`
  - `force_volume`
  - `exec_profile`
  - `execution_decision`
  - `entry_price`, `entry_ts_ms`, `qty`
- live submit path는 한 단계 더 가서 strategy meta를 감싼 `meta_payload.strategy.meta`에서
  - `entry_decision.expected_net_edge_bps`
  - `trade_action`
  - `sizing_decision`
  - `liquidation_policy`
  를 다시 읽어 admissibility, risk-control, emission 결정을 보강한다.

판정:

- 현재 체인은 "최종 score만 있으면 된다"가 아니라 "중간 decision contract들이 끝까지 살아 있어야 한다"가 맞다.
- 따라서 Step 4 train/export가 `runtime_recommendations`만 만들고 `entry_boundary_contract`, `predictor_contract`, `trade_action`, `execution`, `risk_control` 중 일부를 비어 있게 두면 downstream은 parse는 되어도 submit/fill 쪽에서 약해지거나 0으로 수렴할 수 있다.


### F13. live는 `intent_created`와 `submitted` 사이에 별도 shadow/skip 계층이 있다

사실:

- live summary는 다음 카운터를 분리한다.
  - `intent_created_count`
  - `shadow_intents_total`
  - `submitted_intents_total`
  - `skipped_intents_total`
- `model_alpha_runtime_execute`는 실제 submit 전에 아래 이유로 `SKIPPED` 또는 `SHADOW`를 남길 수 있다.
  - `ACCOUNTS_LOOKUP_FAILED`
  - `CHANCE_LOOKUP_FAILED`
  - `MISSING_INSTRUMENTS`
  - `ADMISSIBILITY_SNAPSHOT_BUILD_FAILED`
  - `REJECTED_ADMISSIBILITY`
  - `MISSING_EXECUTOR_GATEWAY`
  - `ENTRY_GATE_ROLLOUT_BLOCKED`
  - `ENTRY_GATE_BREAKER_ACTIVE`
- `order_emission_allowed=false`이면 intent는 버리지 않고 `SHADOW`로 기록된다.

판정:

- live에서 `orders_filled=0`를 backtest/paper와 같은 방식으로 읽으면 안 된다.
- live는 먼저
  - `intent_created_count`
  - `submitted_intents_total`
  - `rollout_order_emission_allowed`
  - `halted_reasons`
  - `live_runtime_health.model_pointer_divergence`
  를 함께 봐야 한다.


### F14. 현재 체인에서 0-fill/미채택 문제를 보려면 확인 순서 자체가 계약이다

현재 코드 기준 최소 확인 순서는 아래다.

1. Step 3 close:
  - `data/collect/_meta/train_snapshot_close_latest.json`
  - `overall_pass`, `snapshot_id`, `source_freshness`, `coverage_window`
2. Step 4 preflight:
  - acceptance report의 `steps.train_snapshot_close_preflight`
  - `steps.runtime_dataset_coverage_preflight`
  - `steps.runtime_viability_preflight`
  - `steps.runtime_deploy_contract_preflight`
3. fusion candidate artifacts:
  - `fusion_runtime_input_contract.json`
  - `runtime_viability_report.json`
  - `runtime_deploy_contract_readiness.json`
  - `predictor_contract.json`
  - `entry_boundary_contract.json`
4. acceptance result:
  - `steps.backtest_candidate`
  - `steps.backtest_runtime_parity_candidate`
  - `steps.paper_candidate`
  - `steps.update_latest_candidate`
  - `steps.promote`
5. runtime consumers:
  - backtest/paper `opportunity_log.jsonl`
  - `micro_order_policy_report.json`
  - live `logs/opportunity_log/<lane>/latest.jsonl`
  - live runtime health checkpoint
  - `latest_candidate_adoption.json`

판정:

- 이 순서를 건너뛰고 마지막 `orders_filled`, `latest_candidate`, `champion`만 보면 원인 계층을 잘못 짚게 된다.
- 즉 현재 00:20 체인은 "어디서 실패했는가"뿐 아니라 "어느 artifact가 먼저 비정상화됐는가"를 순서대로 보는 것이 필수다.


### F15. predictor/entry-boundary 계약 파일은 실제 소비되지만 promotion completeness에서는 직접 강제되지 않는다

사실:

- `train_v5_fusion.py`는 `predictor_contract.json`, `entry_boundary_contract.json`, `fusion_runtime_input_contract.json`를 쓴다.
- `ModelPredictor.load_predictor_from_registry()`는 `predictor_contract.json`, `entry_boundary_contract.json`를 먼저 읽고, 없으면 `train_config` fallback을 시도한다.
- 그러나 `train_v5_fusion`의 `train_config`에는 이 두 payload 자체가 직접 내장되지 않는다.
- 동시에 `autobot.models.registry.ensure_run_completeness()`의 promotion required artifact 목록에는
  - `predictor_contract.json`
  - `entry_boundary_contract.json`
  - `fusion_runtime_input_contract.json`
  이 없다.

판정:

- 현재 이 셋은 "실제로는 downstream가 읽는 계약"인데 "promotion completeness가 명시적으로 보장하는 필수물"은 아니다.
- 즉 지금 당장 재현된 장애라고 단정하긴 어렵지만, 파일 유실/부분 복구 상황에서는 의미가 달라질 수 있는 guard gap 후보로 보는 것이 맞다.


### F16. Step 4의 runtime 관련 실패는 이름이 비슷하지만 실제 끊기는 층이 다르다

현재 코드 기준으로 최소한 아래는 구분해서 읽어야 한다.

- `COMMON_RUNTIME_UNIVERSE_EMPTY`
  - dependency runtime export 또는 fusion runtime input 공통 시장 교집합이 비었을 때
- `CANDIDATE_RUNTIME_DATASET_CERTIFICATION_WINDOW_EMPTY`
  - runtime dataset root/manifest/data file/row가 비었을 때
- `CANDIDATE_RUNTIME_DATASET_CERTIFICATION_WINDOW_GAP`
  - runtime dataset window가 certification window를 덮지 못할 때
- `FUSION_RUNTIME_ALPHA_LCB_ZERO_VIABILITY`
  - `rows_above_alpha_floor <= 0`
- `FUSION_RUNTIME_ENTRY_GATE_ZERO_VIABILITY`
  - `entry_gate_allowed_count <= 0`
- `FUSION_RUNTIME_DEPLOY_CONTRACT_NOT_READY`
  - deploy-grade required component readiness가 미달일 때

판정:

- 이 이름들은 모두 "runtime 쪽 문제"처럼 보이지만 실제로는
  - input universe mismatch
  - runtime dataset build mismatch
  - predictor viability mismatch
  - deploy contract readiness mismatch
  로 층이 다르다.
- 따라서 Step 4 report에서는 `failure_stage`만 볼 게 아니라 위 code와 해당 artifact path를 같이 읽어야 한다.


## 8. 현재 기준 청사진

### 8.1 의도된 정상 경로

1. `00:20` spawn timer가 v5 wrapper를 띄운다.
2. Step 1이 rolling candle source를 fresh하게 만든다.
3. Step 2가 raw ticks를 batch-aware로 수집하고 validate한다.
4. Step 3이 training-critical datasets와 features contract를 닫고 immutable ready snapshot을 publish한다.
5. Step 4가 그 snapshot을 기준으로 dependency trainers와 fusion candidate를 재구성하고 viability/deploy readiness를 검사한다.
6. acceptance/backtest/runtime parity/paper를 통과하면 `latest_candidate`를 갱신한다.
7. promote가 허용되면 champion을 바꾸고 runtime unit을 재시작한다.
8. adopt step이 paired paper/canary/live candidate handoff를 마무리한다.


### 8.2 현재 체인에서 가장 중요한 hard guarantees

- Step 4는 Step 3 snapshot close 없이 정상 진행되면 안 된다.
- fusion 입력들은 같은 `data_platform_ready_snapshot_id`를 가져야 한다.
- `latest_candidate`는 `overall_pass` 이전에 쓰이면 안 된다.
- `champion`은 promote 이전에 바뀌면 안 된다.
- candidate target restart는 pointer update 이후여야 한다.


### 8.3 지금 남아 있는 핵심 보강 포인트

1. Step 2의 `batch_date` vs `validate_dates`를 하나의 operating-date contract로 통일할지 결정해야 한다.
2. Step 3 freshness가 raw ticks의 실제 UTC target date를 보도록 강화할지 결정해야 한다.
3. pre-chain skip과 lock-busy를 machine-readable summary로 남길지 결정해야 한다.
4. Step 4 snapshot fallback을 완전히 제거할지, 아니면 fallback 허용을 운영 계약으로 명시할지 결정해야 한다.
5. candidate runtime consumer가 `latest_candidate` 부재 시 stale fallback에 들어갔는지 dashboard/runbook에서 더 명확히 드러내야 한다.


## 9. 결론

현재 `00:20` 체인은 큰 흐름으로는 다음 청사진에 맞게 정렬되어 있다.

- source refresh
- raw ticks batch 수집
- train-ready snapshot close
- snapshot-driven governed acceptance
- pointer/runtime handoff

하지만 세부 정합성은 아직 완전히 닫히지 않았다.
특히 현재 가장 실제적인 mismatch는 아래 둘이다.

1. Step 1이 batch-date chain이 아니라 rolling UTC refresh라는 점
2. Step 2가 local `batch_date`와 UTC `validate_dates`를 혼용하고, Step 3 consumer가 그 차이를 가린다는 점

그 외 `latest`/`latest_candidate`/`champion` 분리는 의도된 비대칭이므로 bug로 취급하면 안 되고, 오히려 downstream consumer 문서에 강하게 새겨야 하는 운영 계약이다.
