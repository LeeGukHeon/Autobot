# RUNTIME VIABILITY AND RUNTIME SOURCE HARDENING PLAN 2026-04-06

## 0. Purpose

Status update as of `2026-04-06` / `HEAD=d5609b2`:

- `5.1 Runtime Source Contract Hardening`: implemented
- `5.2 Runtime Viability Metrics`: implemented
- `5.3 Variant Selection Hard Constraint`: implemented
- `5.4 Acceptance Fail-Early`: implemented
- `5.5 Backtest / Paper / Live parity checks`: implemented
- `7. Server Validation Plan`: representative OCI rerun completed; latest result now fails at `runtime_viability` with rich diagnostics instead of falling through to late backtest/paper failure
- follow-up hardening after `d5609b2`: `runtime_deploy_contract_readiness.json` is now a first-class fusion artifact, fusion default-eligibility now requires both runtime viability and deploy-contract readiness, and acceptance now fail-closes at `failure_stage = runtime_contract` when a fusion candidate inherits `dependency_expert_only` panel runtime stubs instead of deploy-grade execution/exit docs

Current remaining work after this plan:

- monitor and improve candidate quality so viable fusion candidates are produced more often
- investigate OCI-only broad-suite instability observed in a handful of backtest/paper sizing tests under the full server bundle

이 문서는 현재 `v5` 학습/acceptance 체인에서 드러난 두 가지 핵심 문제를 하나의 실행 계획으로 묶어 정리한다.

1. `runtime_export gap`
2. `offline metric은 좋아 보이지만 runtime/live에서 실제로 거래가 0건인 candidate selection`

이 문서의 목표는 다음과 같다.

- `features_v4`의 의미를 바꾸지 않고 runtime/export source를 분리한다.
- `offline winner`가 `runtime viable candidate`와 다를 수 있는 현재 구조를 fail-closed로 막는다.
- acceptance가 오래 걸리는 backtest/paper 단계에 들어가기 전에 `runtime viability zero` 후보를 조기 차단한다.
- 실제 업비트 자동매매를 전제로, 급한 임시 우회가 아니라 training/backtest/paper/live 전 경로의 앞뒤 정합성을 우선한다.

이 문서는 구현 문서다. 구현자는 이 문서만 읽고도 함수 단위 수정 위치, 추가 artifact, 테스트와 서버 검증 순서를 그대로 따라갈 수 있어야 한다.


## 1. Current State

### 1.1 Current failure classes

현재 repo와 OCI 서버에서 확인된 핵심 실패 축은 아래 두 가지다.

1. `runtime_export gap`
2. `runtime viability zero candidate`

두 문제는 서로 관련되어 있지만 동일한 문제는 아니다.

- `runtime_export gap`은 `training-serving skew / runtime source mismatch` 축이다.
- `orders=0 despite strong offline metrics`는 `predict-then-optimize mismatch` 축이다.

현재 시스템은 이 둘을 동시에 겪고 있다.


### 1.2 Current completed acceptance result

현재 최신 완료 acceptance artifact는 아래다.

- `logs/model_v5_acceptance/v5_candidate_acceptance_20260406-002044.json`

최종 상태:

- `overall_pass = false`
- `failure_stage = acceptance_gate`
- `failure_code = BACKTEST_ACCEPTANCE_FAILED`

최종 reasons:

- `BACKTEST_ACCEPTANCE_FAILED`
- `RUNTIME_PARITY_BACKTEST_FAILED`
- `TRAINER_EVIDENCE_REQUIRED_FAILED`
- `PAPER_FINAL_GATE_HARD_FAILURE`

즉 최신 완료 run은 더 이상 초반 `PANEL_RUNTIME_WINDOW_GAP`에서만 즉시 종료되지 않는다. 실제로 train -> acceptance backtest -> runtime parity backtest -> paper smoke 단계까지 진입한 뒤 최종 gate에서 실패한다.


### 1.3 Current failed candidate

최신 완료 실패 candidate:

- `20260405T124415Z-s42-eeb0d5d7`

이 candidate는 아래 세 경로 모두에서 `orders_submitted=0`, `orders_filled=0`가 반복된다.

- acceptance backtest
- runtime parity backtest
- paper smoke

직접 원인은 strategy-level entry gate다.

- `ENTRY_GATE_ALPHA_LCB_NOT_POSITIVE`

실제 runtime dataset 기준 확인값:

- `rows_above_floor = 0 / 21244`
- `alpha_lcb_floor = 0.0`
- `final_expected_return mean = -0.0134`
- `final_expected_es mean = 0.0321`
- `final_uncertainty mean = 0.0170`
- `final_alpha_lcb mean = -0.0625`

즉 이 candidate는 runtime row 전체에서 `alpha_lcb`가 floor 위로 올라오는 경우가 없다.


### 1.4 Comparison candidate

비교용으로, 실제 주문이 나왔던 candidate는 아래다.

- `20260405T032807Z-s42-47affbc7`

핵심 비교값:

- `alpha_lcb_floor = -0.0179`
- `rows_above_floor = 213 / 963`

이 후보도 `alpha_lcb` 자체가 전반적으로 강한 것은 아니었지만, 적어도 runtime row 중 일부는 entry floor 위에 존재했다. 따라서 `intent_created -> orders_submitted` 경로가 실제로 열렸다.


### 1.5 Current code-path diagnosis

현재 `orders=0`은 execution/fill logic 이전에 결정된다.

실제 호출 흐름:

1. `scripts/v5_governed_candidate_acceptance.ps1`
2. `scripts/candidate_acceptance.ps1`
3. `autobot.cli backtest alpha --preset acceptance`
4. `autobot/backtest/engine.py::run_backtest_sync`
5. `autobot/backtest/engine.py::BacktestRunEngine.run`
6. `autobot/backtest/engine.py::_run_model_alpha_cycle`
7. `autobot/strategy/model_alpha_v1.py::ModelAlphaStrategyV1.on_ts`
8. `autobot/strategy/v5_post_model_contract.py::resolve_v5_entry_gate`
9. entry allowed일 때만 intent 생성
10. 그 다음에야 `trade gate -> order submit -> fill -> slippage -> time-to-fill`

따라서 현재 핵심 문제는 fill-rate 로직, execution policy, replace/cancel logic이 아니라 그보다 앞단의 `runtime viability 없는 fusion candidate selection`이다.


### 1.6 Current runtime-export state

현재 관찰된 중요한 사실:

- `v5_governed_candidate_acceptance`는 더 이상 초반 `PANEL_RUNTIME_WINDOW_GAP`에서만 고정적으로 죽지 않는다.
- 그러나 `features_v4`는 여전히 training labeled artifact 성격이 강하고, runtime/export source와 training source의 경계가 코드에서 완전히 분리되어 있지 않다.
- panel runtime export는 본질적으로 labeled `features_v4`를 직접 재사용하지 않아야 하며, 같은 feature contract를 공유하는 별도 runtime source를 봐야 한다.


### 1.7 Why this is dangerous for real Upbit trading

이 문제는 단순 연구 실패가 아니라 실거래 위험으로 이어진다.

- offline metric이 좋다는 이유만으로 runtime candidate를 promote하면 실거래에서 `trade count = 0` 또는 `viability = 0` 상태가 발생할 수 있다.
- 이런 candidate는 paper/live에서도 실제로 거래를 만들지 못하므로, 운영 상태는 살아 있어도 decision quality는 0이 된다.
- 더 나쁘게는 일부 future patch가 gate를 느슨하게 만들 경우, viability를 확인하지 않은 채 위험한 candidate를 실제 주문 경로로 밀어 넣을 수 있다.

따라서 현재 구조는 “조금 더 좋은 metric”보다 “runtime viability hard fail”이 우선이다.


## 2. External References

### 2.1 Data layer references

- TFX User Guide  
  <https://www.tensorflow.org/tfx/guide>
- TensorFlow Transform guide  
  <https://www.tensorflow.org/tfx/guide/transform>
- TensorFlow Data Validation guide  
  <https://www.tensorflow.org/tfx/guide/tfdv>
- TFX Data Validation basic tutorial  
  <https://www.tensorflow.org/tfx/tutorials/data_validation/tfdv_basic>
- TensorFlow Transform tutorial  
  <https://www.tensorflow.org/tfx/tutorials/transform/simple>
- Feast point-in-time joins  
  <https://docs.feast.dev/getting-started/concepts/point-in-time-joins>

이 레퍼런스들이 말하는 핵심은 아래와 같다.

- training과 serving은 같은 transform contract를 써야 한다.
- serving/runtime feature는 point-in-time correct 해야 한다.
- schema skew와 feature skew는 별도 validation gate로 막아야 한다.
- 학습용 historical retrieval과 runtime serving surface는 계약은 공유하되 artifact는 분리될 수 있다.


### 2.2 System risk references

- Hidden Technical Debt in Machine Learning Systems  
  <https://papers.nips.cc/paper/5656-hidden-technical-debt-in-machine-learning-systems.pdf>

이 논문이 직접적으로 설명하는 위험:

- strict abstraction boundary erosion
- undeclared consumers
- training-serving skew
- hidden coupling

현재 repo의 `features_v4 training artifact`와 `runtime/export consumer`의 혼재는 이 범주에 정확히 들어간다.


### 2.3 Selection / decision mismatch references

- Decision-Focused Learning: Foundations, State of the Art, Benchmark and Future Opportunities  
  <https://arxiv.org/abs/2307.13565>
- DFF: Decision-Focused Fine-tuning for Smarter Predict-then-Optimize with Limited Data  
  <https://arxiv.org/abs/2501.01874>
- Feasibility-Aware Decision-Focused Learning for Predicting Parameters in the Constraints  
  <https://arxiv.org/abs/2510.04951>
- Decision-focused predictions via pessimistic bilevel optimization: a computational study  
  <https://arxiv.org/abs/2312.17640>

이 문헌들이 공통으로 말하는 핵심:

- prediction metric이 좋아도 downstream decision quality는 나쁠 수 있다.
- `predict-then-optimize` 구조에서는 선택 단계 또는 학습 단계에 decision quality / feasibility / regret를 직접 반영해야 한다.
- 모든 문제를 곧바로 end-to-end DFL로 바꾸는 것보다, constrained selection과 feasibility-aware filtering을 먼저 넣는 단계적 접근이 유효하다.


### 2.4 Interpretation for this repository

이 repo에 가장 적합한 해석은 아래다.

- `runtime_export gap` 문제는 `training-serving skew / runtime source mismatch`다.
- `orders=0 despite strong offline metrics` 문제는 `predict-then-optimize mismatch`다.
- 현재 repo는 full end-to-end DFL을 당장 넣는 것보다, `runtime source consistency + runtime viability constrained selection`을 먼저 넣는 것이 더 안전하다.


## 3. Current Code Paths And Exact Decision Points

### 3.1 Training and variant selection

관련 파일:

- `autobot/models/train_v5_fusion.py`
- `autobot/models/v5_variant_selection.py`

핵심 함수:

- `train_v5_fusion.py::train_and_register_v5_fusion`
- `train_v5_fusion.py::V5FusionEstimator.predict_panel_contract`
- `train_v5_fusion.py::_fit_reg_head`
- `train_v5_fusion.py::_fit_binary_head`
- `v5_variant_selection.py::run_v5_fusion_variant_matrix`
- `v5_variant_selection.py::_collect_variant_run_record`
- `v5_variant_selection.py::_selection_key_from_leaderboard`
- `v5_variant_selection.py::_select_fusion_winner`
- `v5_variant_selection.py::_has_clear_fusion_edge`

현재 상태:

- fusion winner 선택은 `test_ev_net_top5`, `test_precision_top5`, `test_pr_auc`, `test_log_loss` 위주다.
- runtime viability는 현재 variant selection key에 포함되어 있지 않다.
- `runtime_recommendations`에는 `fusion_candidate_default_eligible = true`, `fusion_evidence_reason_code = OFFLINE_SELECTION_ONLY` 같은 offline-only 상태가 반영될 수 있다.

즉 현재 selection은 “runtime에서 실제로 거래 가능한지”보다 “offline leaderboard가 좋아 보이는지”를 먼저 본다.


### 3.2 Predictor and entry gate

관련 파일:

- `autobot/models/predictor.py`
- `autobot/strategy/v5_post_model_contract.py`
- `autobot/strategy/model_alpha_v1.py`

핵심 함수:

- `predictor.py::ModelPredictor.predict_score_contract`
- `train_v5_fusion.py::V5FusionEstimator.predict_panel_contract`
- `v5_post_model_contract.py::resolve_v5_entry_gate`
- `model_alpha_v1.py::_resolve_v5_strategy_expected_edge_bps`
- `model_alpha_v1.py::ModelAlphaStrategyV1.on_ts`

현재 상태:

- `final_alpha_lcb = final_expected_return - final_expected_es - final_uncertainty`
- `resolve_v5_entry_gate`는 `alpha_lcb <= alpha_lcb_floor`이면 entry를 차단한다.
- strategy는 gate를 통과해야만 `intent_created`를 만든다.

따라서 `rows_above_floor = 0`이면 downstream order engine은 아무리 정상이어도 결과가 `orders=0`으로 고정된다.


### 3.3 Backtest / paper / live consumers

관련 파일:

- `autobot/backtest/engine.py`
- `autobot/paper/engine.py`
- `autobot/live/model_alpha_runtime.py`
- `autobot/paper/live_features_v5.py`

핵심 함수:

- `backtest/engine.py::run_backtest_sync`
- `backtest/engine.py::_run_model_alpha_cycle`
- `backtest/engine.py::_try_submit_candidate`
- `paper/engine.py::run_live_paper_sync`
- `paper/engine.py::_try_submit_candidate`
- `live/model_alpha_runtime.py::run_live_model_alpha_runtime`
- `paper/live_features_v5.py::LiveFeatureProviderV5.build_frame`

현재 상태:

- backtest/paper/live 모두 `model_alpha_v1`의 `entry_decision_payload`를 기반으로 intent를 만든다.
- 따라서 세 경로의 `orders=0`이 동시에 나온다면, 대부분 원인은 upstream entry viability에 있다.
- fill-rate, slippage, replace/cancel, micro gate는 그 이후 단계다.


### 3.4 Acceptance orchestration

관련 파일:

- `scripts/v5_governed_candidate_acceptance.ps1`
- `scripts/candidate_acceptance.ps1`

핵심 단계:

- `train_snapshot_close_preflight`
- dependency trainer/export resolution
- candidate train
- `backtest_candidate`
- `backtest_runtime_parity_candidate`
- `paper_candidate`
- `fusion_variant_selection`
- promote

현재 상태:

- 최신 completed acceptance는 이미 backtest/paper 단계까지 진입한다.
- 즉 현재 핵심 실패는 orchestration bug가 아니라 candidate viability 문제다.
- 다만 `candidate_acceptance.ps1`에는 PowerShell inline expression/assignment 패턴이 많아서, 최근 실제 문법 오류가 있었고 앞으로도 parse-safe 작성 규칙을 강제해야 한다.


## 4. Exact Root Cause Analysis For The Latest Failure

### 4.1 The direct mechanism

최신 실패 candidate:

- `20260405T124415Z-s42-eeb0d5d7`

runtime dataset direct calculation:

- `rows_above_floor = 0 / 21244`
- `alpha_lcb_floor = 0.0`

따라서 strategy-level entry gate에서 모든 row가 차단된다.


### 4.2 Why alpha is negative

실제 mean terms:

- `final_expected_return mean = -0.0134`
- `final_expected_es mean = 0.0321`
- `final_uncertainty mean = 0.0170`
- `final_alpha_lcb mean = -0.0625`

즉 `return`이 이미 음수이고, `es`와 `uncertainty`는 양수라서 `alpha_lcb`가 전부 음수가 된다.


### 4.3 Why return is low

실패 candidate의 linear fusion `return_model`에서 runtime feature 평균 기준 큰 기여도를 보면:

- `panel_final_tradability`가 가장 큰 음수 기여
- `panel_score_mean / final_rank_score`는 소폭 양수 기여
- `panel_score_lcb`는 다시 음수 기여

즉 현재 학습된 linear return head는 runtime에서 높은 `panel_final_tradability`를 오히려 낮은 `final_expected_return`과 연결해 사용하고 있다.

이건 parser bug나 execution bug가 아니라 실제 학습된 mapping 문제다.


### 4.4 Why this candidate was still selected

현재 `_select_fusion_winner()`는 아래를 우선 본다.

- `test_ev_net_top5`
- `test_precision_top5`
- `test_pr_auc`
- `test_log_loss`

하지만 아래는 보지 않는다.

- `rows_above_alpha_floor`
- `alpha_lcb_positive_count`
- `entry_gate_allowed_count`
- `intent_created_count`
- `orders_submitted`

즉 offline leaderboard는 좋아 보여도 runtime에서는 거래 0건인 candidate가 winner가 될 수 있다.


## 5. Hardening Plan

### 5.1 Runtime Source Contract Hardening

Implementation status:

- completed
- runtime source rebuild no longer mutates `features_v4` semantics
- panel runtime source contract / runtime-only source lineage path is now active in code and exercised in tests

목표:

- `features_v4` 의미를 training contract로 유지
- runtime/export는 training labeled parquet와 분리된 runtime source를 사용

수정 대상:

- `autobot/features/pipeline_v4.py`
- `autobot/models/train_v5_panel_ensemble.py`

구현 원칙:

- `feature_spec.json`과 `label_spec.json`을 source-of-truth로 사용
- `base_candles_root`, `micro_root`, `high_tfs`, `one_m_*`, `sample_weight` 정책을 공유
- runtime window는 `Asia/Seoul` operating-date 기준으로 절단
- runtime source dataset은 train-config의 실제 label alias를 유지
- runtime source는 market별 flat parquet를 사용해 date-partition pruning mismatch를 피함
- runtime-only source에서만 high-tf tail 유도 허용
- training `features_v4` artifact 의미는 절대 바꾸지 않음

함수 단위 변경 대상:

- `pipeline_v4.py::build_runtime_feature_frame_v4_from_contract`
- `pipeline_v4.py::_parse_runtime_operating_date_to_ts_ms`
- `pipeline_v4.py::_derive_high_tf_close_candles_from_base`
- `train_v5_panel_ensemble.py::_load_panel_inference_dataset_window`
- `train_v5_panel_ensemble.py::_write_panel_runtime_source_dataset`

추가 artifact:

- `runtime_source_contract.json`
  - `source_dataset_root`
  - `requested_window`
  - `requested_markets`
  - `selected_markets`
  - `rows`


### 5.2 Runtime Viability Metrics

Implementation status:

- completed
- `runtime_viability_report.json` is now a first-class fusion artifact
- `runtime_recommendations.json`, `promotion_decision.json`, train report, acceptance artifact, and dashboard all surface the same viability summary fields
- added rich diagnostics:
  - `mean_final_expected_return`
  - `mean_final_expected_es`
  - `mean_final_uncertainty`
  - `mean_final_alpha_lcb`
  - `top_entry_gate_reason_codes`
  - `sample_rows`

목표:

- fusion candidate가 runtime에서 실제로 거래 가능한지 수치화
- offline metric과 분리된 별도 viability artifact 생성

수정 대상:

- `autobot/models/train_v5_fusion.py`
- `autobot/models/predictor.py`
- `autobot/strategy/v5_post_model_contract.py`

추가 artifact:

- `runtime_viability_report.json`

필수 필드:

- `alpha_lcb_floor`
- `runtime_rows_total`
- `alpha_lcb_positive_count`
- `rows_above_alpha_floor`
- `rows_above_alpha_floor_ratio`
- `expected_return_positive_count`
- `entry_gate_allowed_count`
- `entry_gate_allowed_ratio`
- `estimated_intent_candidate_count`
- `generation_window`
- `common_runtime_universe_id`

계산 규칙:

- 반드시 fusion `runtime_feature_dataset` + actual predictor output 기준
- acceptance에서 재계산 가능해야 함
- train artifact와 acceptance artifact 양쪽에 남아야 함


### 5.3 Variant Selection Hard Constraint

Implementation status:

- completed
- zero-viability fusion candidates are rejected before offline score comparison wins are allowed to matter
- family `latest` pointer is now published only for viable fusion runs
- rejection reason codes are reflected in variant reporting

목표:

- offline metric이 좋아도 runtime에서 거래가 0건이면 winner가 되지 못하게 함

수정 대상:

- `autobot/models/v5_variant_selection.py`
- `autobot/models/train_v5_fusion.py`

함수 단위 변경:

- `_collect_variant_run_record`
  - `runtime_viability_report.json` 로드
  - `selection_key` 외에 `runtime_viability` payload 추가
- `_validate_variant_contracts`
  - 기존 artifact 존재 체크 외에 아래 hard fail 추가
  - `rows_above_alpha_floor == 0`
  - `alpha_lcb_positive_count == 0`
- `_select_fusion_winner`
  - 비교 순서를 변경
  - 1단계: runtime viability hard pass
  - 2단계: 그 안에서 offline metric 비교
- baseline `linear`도 동일 규칙 적용

`fusion_variant_report.json` 필수 확장 필드:

- `runtime_viability_pass`
- `runtime_viability_report_path`
- `runtime_viability_summary`
- `offline_winner_variant_name`
- `default_eligible_variant_name`
- `rejection_reasons`

새 reason code:

- `FUSION_RUNTIME_ALPHA_LCB_ZERO_VIABILITY`
- `FUSION_RUNTIME_ENTRY_GATE_ZERO_VIABILITY`


### 5.4 Acceptance Fail-Early

Implementation status:

- completed
- acceptance now reads `runtime_viability_report.json` immediately after candidate train
- zero-viability candidates fail at `failure_stage = runtime_viability`
- backtest / runtime parity / paper are not started for those candidates
- acceptance report now preserves rich viability diagnostics instead of dropping them

목표:

- runtime viability가 0인 후보를 backtest/paper까지 오래 태우지 않음

수정 대상:

- `scripts/candidate_acceptance.ps1`
- 필요 시 `scripts/v5_governed_candidate_acceptance.ps1`

함수/step 계획:

- candidate train 직후 `runtime_viability_report` 로드
- hard fail 조건
  - `rows_above_alpha_floor == 0`
  - `entry_gate_allowed_count == 0`
- 이 경우 즉시 종료
  - `failure_stage = runtime_viability`
  - `failure_code = FUSION_RUNTIME_ALPHA_LCB_ZERO_VIABILITY`
- backtest/paper는 실행하지 않음
- acceptance report에 새 step 추가
  - `runtime_viability_preflight`
- PowerShell에서는 inline `if (...) {}`를 값 표현식으로 사용하지 않는다.
  중간 변수로만 받는다.


### 5.5 Backtest / Paper / Live parity checks

Implementation status:

- completed
- opportunity log rows now consistently expose:
  - `alpha_lcb_floor`
  - `final_alpha_lcb`
  - `expected_net_edge_bps`
  - `reason_codes`
- backtest / paper / live summaries now expose `intent_created_count`
- `LIVE_V5` feature-provider build stats now include runtime source lineage
- orphan cleanup helper added:
  - `autobot/ops/paper_alpha_process_guard.py`
  - `scripts/cleanup_stale_live_v5_paper_alpha_processes.ps1`

목표:

- 같은 candidate에 대해 backtest/paper/live가 같은 `entry gate` 이유를 남기게 함

수정 대상:

- `autobot/strategy/model_alpha_v1.py`
- `autobot/backtest/engine.py`
- `autobot/paper/engine.py`
- `autobot/live/model_alpha_runtime.py`
- `autobot/paper/live_features_v5.py`

필수 보강:

- opportunity log에 항상 아래를 남기도록 보장
  - `alpha_lcb_floor`
  - `final_alpha_lcb`
  - `expected_net_edge_bps`
  - `reason_codes`
- `intent_created_count`를 summary/report level에서도 직접 집계 가능하게 함
- backtest/paper/live가 같은 `entry_decision_payload` field contract를 사용하도록 강제
- orphan `paper alpha --duration-sec 0 --preset live_v5` 프로세스 정리 절차를 운영 runbook에 추가


## 6. Tests

### 6.1 Existing mandatory local suites

- `tests/test_pipeline_v4_runtime_source.py`
- `tests/test_train_v5_panel_ensemble.py`
- `tests/test_train_v5_fusion.py`
- `tests/test_candidate_acceptance_runtime_coverage.py`
- `tests/test_candidate_acceptance_v5_dependency_inputs.py`
- `tests/test_candidate_acceptance_dependency_reuse.py`
- `tests/test_candidate_acceptance_certification_lane.py`
- `tests/test_backtest_model_alpha_integration.py`
- `tests/test_paper_engine_model_alpha_integration.py`
- `tests/test_paper_live_feature_provider_v5.py`
- `tests/test_paired_paper_runtime.py`


### 6.2 New required tests

- fusion variant selection이 `rows_above_alpha_floor == 0` 후보를 탈락시키는 테스트
- acceptance가 `runtime_viability_preflight`에서 fail-fast 하는 테스트
- runtime viability summary가 `runtime_feature_dataset`과 predictor output으로 계산되는 테스트
- panel runtime source dataset이 실제 train-config label alias를 쓰는 테스트
- same candidate에 대해 backtest/paper opportunity log reason이 `ENTRY_GATE_ALPHA_LCB_NOT_POSITIVE`로 일치하는 테스트


## 7. Server Validation Plan

Current status:

- completed for the hardening scope
- OCI was updated through `d5609b2`
- representative governed acceptance rerun with `BatchDate 2026-04-04` now lands in `runtime_viability` with full diagnostics, which is the intended fail-fast behavior for zero-viability candidates
- latest observed representative OCI result:
  - `failure_stage = runtime_viability`
  - `failure_code = FUSION_RUNTIME_ALPHA_LCB_ZERO_VIABILITY`
  - rich mean / reason / sample diagnostics present in both acceptance artifact and candidate fusion artifacts

순서 고정:

1. panel runtime export 수동 재생성
2. metadata 확인
3. representative fusion candidate train
4. representative acceptance run

서버 확인 항목:

- panel runtime export metadata
  - `coverage_end_date == certification_end`
- fusion `runtime_viability_report.json`
  - `rows_above_alpha_floor`
  - `entry_gate_allowed_count`
- acceptance latest artifact
  - `runtime_export gap`이 아니라
  - `runtime_viability` hard fail 또는 실제 trade generation 중 하나로 귀결
- stale `paper alpha --duration-sec 0 --preset live_v5` orphan process 없음


## 8. Execution Discipline

이 작업은 업비트 실거래 전제 작업이다.

따라서 다음 원칙을 강제한다.

- fail-open 금지
- offline metric만 보고 promote 금지
- runtime viability zero candidate는 early hard fail
- training artifact 의미와 runtime artifact 의미를 혼용 금지
- power shell script는 parse-safe pattern만 사용
- 기능 추가보다 경계 정합성과 rollback 가능성을 우선

절대 해서는 안 되는 것:

- `features_v4` training contract를 unlabeled runtime artifact처럼 느슨하게 바꾸기
- acceptance가 느리다는 이유로 runtime viability hard fail을 제거하기
- 실거래 candidate를 `orders=0` 상태로 paper/live까지 계속 밀기


## 9. Assumptions

- 현재 가장 위험한 문제는 execution bug보다 `runtime viability 없는 후보 selection`이다.
- full end-to-end DFL은 지금 단계에서 하지 않는다.
- 먼저 `runtime source consistency`와 `runtime viability constrained selection`을 넣는다.
- 이 문서의 계획은 fail-closed default를 전제로 한다.
- 실거래 전제이므로 metric 상향보다 `runtime viability + parity + rollback`이 우선이다.
