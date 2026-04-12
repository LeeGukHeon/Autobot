# V5 Training Data Dependency Inventory 2026-04-12

## 목적

이 문서는 `00:20` v5 학습/검증 체인이 실제로 어떤 데이터를 필요로 하는지, 각 데이터의 producer/consumer가 누구인지, 서버 현재 상태가 어떤지, 그리고 무엇을 정상화해야 하는지를 한 눈에 보기 위한 운영용 인벤토리다.

관련 상위 감사 문서:

- `docs/WIKI_0020_END_TO_END_CONSISTENCY_AUDIT_2026-04-10.md`
- `docs/RUNTIME_VIABILITY_AND_RUNTIME_SOURCE_HARDENING_PLAN_2026-04-06.md`

## 범위

이 문서는 `train_v5_panel_ensemble -> train_v5_sequence -> train_v5_lob -> train_v5_tradability -> train_v5_fusion -> candidate_acceptance` 체인에 실제로 필요한 데이터만 다룬다.

정리 기준:

- `원천 source`: 외부에서 직접 들어오는 데이터
- `파생 dataset`: 학습 전에 만들어져 있어야 하는 내부 데이터셋
- `expert/runtime artifact`: dependency trainer가 후속 trainer에 넘기는 산출물

## 필수 데이터 인벤토리

| 데이터 / 계약 | 주 producer | 주 consumer | 필수성 | 서버 현재 상태 | 정상화 액션 |
| --- | --- | --- | --- | --- | --- |
| `candles_api_v1` | `scripts/run_candles_api_refresh.ps1` | `raw_ticks_daily`, `features_v4`, `ws_candle_v1` plan, `candles_second_v1` plan | 필수 | 주기 갱신 중. `candles_api_refresh_latest.json` 최신 | 유지. freshness만 계속 확인 |
| `raw_ticks/upbit/trades` | `scripts/run_raw_ticks_daily.ps1`, `autobot-raw-ticks-backfill.timer` | `micro_v1`, ticks validate, snapshot close freshness | 필수 | daily latest는 `2026-04-10`, backfill latest는 `2026-04-11` | daily와 backfill 역할을 분리 문서화. stale 경보 기준 명확화 |
| `raw_ws/upbit/public` | `autobot-ws-public.service` | live runtime, `ws_candle_v1`, health reports | 필수 | service active. 다만 `ws_public_health.json` 필드명이 구 소비자 기대와 다름 | health consumer schema 정렬 필요 |
| `micro_v1` | `autobot.cli micro aggregate/validate` via `refresh_data_platform_layers.ps1` | `features_v4`, snapshot close, live parity, micro-dependent runtime | 필수 | nightly chain 의존이 큼. standalone health는 약함 | independent freshness/report 추가 |
| `candles_second_v1` | `refresh_data_platform_layers.ps1` training-critical | `v5_sequence`, `v5_lob` | 필수 | 독립 timer 없음. nightly refresh 의존 | standalone freshness/report 또는 dashboard surfacing 필요 |
| `ws_candle_v1` | `refresh_data_platform_layers.ps1` runtime-rich | `v5_sequence`, `v5_lob`, 일부 live/runtime fallback | 중요 | build report 존재하나 최신성이 약함 | runtime-rich chain 건강상태 명시 필요 |
| `lob30_v1` | `refresh_data_platform_layers.ps1` training-critical | `sequence_v1` upstream context, `v5_lob` 간접 의존 | 중요 | build report 존재하나 독립 건강 신호 약함 | validate/freshness를 별도 surfacing |
| `sequence_v1` | `autobot.cli collect tensors` via `refresh_data_platform_layers.ps1` | `v5_sequence`, `v5_lob` | 필수 | nightly chain 의존. `--skip-existing-ready true` 사용 | stale cache/coverage gap을 별도 보고서로 노출 |
| `private_execution_v1` | `autobot.ops.private_execution_label_store` | `v5_tradability` | 필수 | snapshot close / acceptance가 강하게 소비 | build/validate freshness를 운영 대시보드에 노출 필요 |
| `features_v4` | `refresh_current_features_v4_contract_artifacts.ps1` + feature build/validate/certification chain | `v5_panel_ensemble`, acceptance feature parity/certification, predictor contract | 필수 | snapshot close가 hard gate로 읽음 | build/validate/certification/parity 4종 상태를 독립 health로 유지해야 함 |
| `train_snapshot_close_latest.json` | `scripts/close_v5_train_ready_snapshot.ps1` | `candidate_acceptance.ps1`, dependency trainers, runtime topology/dashboard | 필수 | 최신 close는 `batch_date=2026-04-10`, `overall_pass=true` | close service failed 원인 복구 필요 |
| `data_platform_ready_snapshot.json` | `autobot.ops.data_platform_snapshot publish` | 모든 v5 trainer, acceptance | 필수 | snapshot pointer는 사용 중 | stale publish 경로와 failed refresh service 복구 필요 |

## Trainer별 직접 입력

### `train_v5_panel_ensemble`

직접 입력:

- `features_v4` snapshot root
- `label_spec`, `feature_spec`, `feature_dataset_certification`
- execution acceptance용 eval dataset window

의미:

- panel은 v5 체인에서 유일하게 `features_v4`를 직접 학습 입력으로 쓴다.
- 따라서 `features_v4` build/validate/certification/parity가 모두 살아 있어야 이후 dependency 전체가 안정적이다.

### `train_v5_sequence`

직접 입력:

- `sequence_v1`
- sibling source:
  - `candles_second_v1`
  - `ws_candle_v1`
  - `candles_api_v1`
  - `candles_v1` fallback

의미:

- `sequence_v1`만 있으면 끝이 아니라, short-horizon/minute context source가 같이 살아 있어야 한다.
- 현재 체계는 이 대부분을 `training_critical refresh` 안에서 만든다.

### `train_v5_lob`

직접 입력:

- `sequence_v1`
- sibling source:
  - `candles_second_v1`
  - `ws_candle_v1`
  - `candles_api_v1`
  - `candles_v1` fallback

의미:

- 이름은 `lob`지만 실제 anchor/source contract는 `sequence_v1` family와 강하게 묶여 있다.
- 그래서 `sequence tensor` stale은 `lob` 품질 문제로도 내려온다.

### `train_v5_tradability`

직접 입력:

- `panel expert_prediction_table.parquet`
- `sequence expert_prediction_table.parquet`
- `lob expert_prediction_table.parquet`
- `private_execution_v1`

의미:

- tradability는 raw feature dataset이 아니라 upstream expert output과 private execution label을 결합한다.
- 따라서 `private_execution_v1`가 nightly close 산출물로만 취급되면 안 되고, 별도 freshness 대상이어야 한다.

### `train_v5_fusion`

직접 입력:

- `panel/sequence/lob/tradability expert_prediction_table.parquet`
- 각 dependency의 runtime export table
- `data_platform_ready_snapshot_id`

의미:

- fusion은 raw source를 직접 읽지 않는 대신 dependency artifact 정합성에 100% 의존한다.
- 따라서 upstream expert/runtime export가 stale이거나 snapshot mismatch이면 fusion은 겉보기엔 돌더라도 실제 compare/runtime parity에서 깨진다.

## 수집/생성 경로별 현재 판단

### 살아 있는 경로

- `autobot-candles-api-refresh.timer`
- `autobot-ws-public.service`
- `autobot-private-ws-archive.service`
- `autobot-raw-ticks-backfill.timer`

이 경로들은 "원천 source를 완전히 죽이지는 않는다"는 의미에서 살아 있다.

### 취약하거나 실패한 경로

- `autobot-data-platform-refresh.service` failed
- `autobot-v5-train-snapshot-close.service` failed

이 둘은 "학습 파생 dataset / close contract"를 독립적으로 건강하게 유지하지 못한다는 뜻이다.

### nightly chain 의존이 큰 경로

- `candles_second_v1`
- `lob30_v1`
- `sequence_v1`
- `private_execution_v1`
- `features_v4` contract refresh 일부

즉 지금은 source 일부는 상시로 돌지만, 학습에 직접 필요한 파생물 다수는 여전히 `00:20` 체인에 크게 의존한다.

## 현재 가장 중요한 정상화 타깃

### 1. 파생 dataset health를 독립적으로 보이게 만들기

대상:

- `sequence_v1`
- `candles_second_v1`
- `lob30_v1`
- `ws_candle_v1`
- `private_execution_v1`
- `features_v4`

필요한 것:

- latest build/validate timestamp
- 최근 성공/실패 여부
- coverage window
- selected market count / row count

### 2. failed service 복구

대상:

- `autobot-data-platform-refresh.service`
- `autobot-v5-train-snapshot-close.service`

이 둘이 실패 상태인 한, nightly chain은 돌아도 운영자가 "독립 수집/생성 경로가 건강하다"고 볼 수 없다.

### 3. WS health schema 소비자 정렬

현재 `ws_public_health.json`은 예전 기대 필드 대신 아래 계열을 쓴다.

- `last_rx_ts_ms`
- `subscribed_markets_count`

즉 consumer가 아직 아래 구 키를 기대하면 비정상처럼 보일 수 있다.

- `trade_last_rx_ts_ms`
- `orderbook_last_rx_ts_ms`
- `subscribed_market_count`

### 4. `train_snapshot_close`를 source freshness summary 이상으로 격상

`train_snapshot_close_latest.json`은 현재:

- snapshot id
- coverage/train/certification window
- source freshness
- `features_v4_effective_end`

를 묶어서 Step 4의 사실상 root contract 역할을 한다.

즉 이 파일은 단순 리포트가 아니라 `v5 nightly training root contract`로 취급해야 한다.

## 추천 실행 순서

1. `autobot-data-platform-refresh.service` failed 원인 복구
2. `autobot-v5-train-snapshot-close.service` failed 원인 복구
3. `ws_public_health.json` consumer schema 정렬
4. 파생 dataset health를 dashboard/ops artifact로 독립 surfacing
5. 그 다음에야 `00:20` 의존을 줄일지, 계속 chain-owned로 둘지 결정

## 한 줄 결론

지금 v5 학습에 필요한 데이터는 "원천 source + 파생 dataset + dependency artifact" 3층으로 나뉜다.

현재 서버는:

- 원천 source 일부는 상시 수집 중
- 하지만 학습에 직접 필요한 파생 dataset 다수는 여전히 nightly chain 의존
- 독립 refresh/close service는 실패 상태

따라서 다음 정상화 작업의 본질은 "더 많이 수집"이 아니라, "학습 필수 파생 dataset을 독립 health contract로 끌어올리는 것"이다.
