# DATA AND FEATURE PLATFORM BLUEPRINT 2026-03-25

## 0. Executive Summary

이 문서는 현재 코드베이스와 실제 OCI 서버 상태를 함께 기준으로 `데이터 수집 -> micro 집계 -> feature dataset -> live feature provider`까지의 데이터/피처 플랫폼을 분석하고, 더 강한 시스템을 만들기 위한 개선 방향을 정리한 문서다.

이 문서는 아래 문서들과 직접 연결된다.

- [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
- [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)
- [RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md)

핵심 결론은 다음이다.

- 현재 데이터 플랫폼은 이미 꽤 강하다.
- public WS 수집, REST top-up, micro 집계, feature dataset, live feature provider가 하나의 계열로 이어져 있다.
- 하지만 아직 `데이터 계약이 강력한 SSOT로 완전히 닫혀 있다`고 보긴 어렵다.

가장 중요한 약점은:

1. dataset lineage와 freshness contract가 흩어져 있다.
2. feature build와 live feature provider의 parity가 강하지만 완전한 artifact-level certification은 부족하다.
3. feature dataset validate artifact가 일관되게 남지 않는 흔적이 있다.
4. 수집/집계/feature build를 아우르는 unified data contract registry가 없다.
5. 향후 `1s candle`, `30-level orderbook`, `sequence/LOB tensors`를 수용할 플랫폼 설계가 아직 없다.

즉, 강한 모델과 강한 평가의 선행조건은 결국 강한 데이터 플랫폼이다.


## 1. Confirmed Server-Backed Findings

이번 문서 작성 과정에서 OCI 서버에 실제 접속해 아래를 확인했다.

OCI server access reference:

- [OCI_SERVER_ACCESS_REFERENCE_2026-03-25.md](/d:/MyApps/Autobot/docs/OCI_SERVER_ACCESS_REFERENCE_2026-03-25.md)

### 1.1 Confirmed Remote Host

- host: `ubuntu@168.107.44.206`
- project root: `/home/ubuntu/MyApps/Autobot`

### 1.2 Confirmed Remote Repository State

- current HEAD: `745cfc62d77dfd643298dd1845eec05bad0ac284`
- remote: `origin git@github.com:LeeGukHeon/Autobot.git`
- working tree had untracked files at inspection time:
  - `docs/reports/CANDIDATE_CANARY_REPORT_2026-03-13.md`
  - `scripts/restore_promote_override_once.sh`

### 1.3 Confirmed Remote Storage Topology

상위 용량 분포:

- `data`: 약 `8.9G`
- `models`: 약 `12G`
- `logs`: 약 `1.2G`

주요 하위 용량:

- `data/parquet`: 약 `4.7G`
- `data/raw_ws`: 약 `2.9G`
- `data/paper`: 약 `529M`
- `data/backtest`: 약 `465M`
- `data/features`: 약 `189M`
- `data/state`: 약 `170M`
- `models/registry`: 약 `12G`

이 구조는 "원천 raw + aggregated parquet + runtime state + registry"가 모두 서버 내에 공존하는 운영형 플랫폼이라는 점을 보여 준다.


## 2. Current Platform Topology

### 2.1 Confirmed Data Roots On The Server

실서버에서 확인된 주요 루트:

- `data/raw_ws/upbit`
- `data/raw_ticks/upbit`
- `data/parquet/candles_api_v1`
- `data/parquet/candles_v1`
- `data/parquet/micro_v1`
- `data/features/features_v4`
- `data/paper/runs`
- `data/backtest/runs`
- `data/state/live`
- `data/state/live_candidate`


### 2.2 Current Dataset Families

실제 확인된 dataset family:

- candles:
  - `candles_api_v1`
  - `candles_v1`
- micro:
  - `micro_v1`
  - `micro_v1_daily_smoke`
- features:
  - `features_v1`
  - `features_v2`
  - `features_v3`
  - `features_v3_ac*`
  - `features_v4`

이건 좋은 점도 있고 위험도 있다.

좋은 점:

- 실험 흔적과 production data가 같이 남아 있어 forensic이 가능하다.

위험:

- dataset naming/versioning governance가 더 엄격하지 않으면 오래 갈수록 혼동이 생길 수 있다.


## 3. Current Code Structure

### 3.1 Raw Collection

핵심 파일:

- [ws_public_collector.py](/d:/MyApps/Autobot/autobot/data/collect/ws_public_collector.py)
- [ticks_collector.py](/d:/MyApps/Autobot/autobot/data/collect/ticks_collector.py)
- [candles_collector.py](/d:/MyApps/Autobot/autobot/data/collect/candles_collector.py)
- [plan_ws_public.py](/d:/MyApps/Autobot/autobot/data/collect/plan_ws_public.py)
- [plan_ticks.py](/d:/MyApps/Autobot/autobot/data/collect/plan_ticks.py)
- [plan_candles.py](/d:/MyApps/Autobot/autobot/data/collect/plan_candles.py)

현재 구조는:

- plan 생성
- collect 실행
- checkpoint / manifest / validate report 갱신

형태다.

이건 수집 계층으로서는 꽤 좋다.


### 3.2 Micro Aggregation

핵심 파일:

- [merge_micro_v1.py](/d:/MyApps/Autobot/autobot/data/micro/merge_micro_v1.py)
- [trade_aggregator_v1.py](/d:/MyApps/Autobot/autobot/data/micro/trade_aggregator_v1.py)
- [orderbook_aggregator_v1.py](/d:/MyApps/Autobot/autobot/data/micro/orderbook_aggregator_v1.py)
- [resample_v1.py](/d:/MyApps/Autobot/autobot/data/micro/resample_v1.py)
- [validate_micro_v1.py](/d:/MyApps/Autobot/autobot/data/micro/validate_micro_v1.py)

현재 micro layer는:

- WS trade
- REST trade fallback
- WS orderbook
- 1m / 5m aggregate
- date alignment detection

까지 이미 포함한다.


### 3.3 Feature Build

핵심 파일:

- [pipeline_v4.py](/d:/MyApps/Autobot/autobot/features/pipeline_v4.py)
- [feature_set_v4_live_base.py](/d:/MyApps/Autobot/autobot/features/feature_set_v4_live_base.py)
- [feature_set_v4.py](/d:/MyApps/Autobot/autobot/features/feature_set_v4.py)
- [labeling_v2_crypto_cs.py](/d:/MyApps/Autobot/autobot/features/labeling_v2_crypto_cs.py)

현재 build는:

- 5m base
- 1m densify
- 15m/60m/240m asof join
- mandatory micro join
- spillover/breadth
- periodicity
- trend/volume
- order flow panel
- interactions
- sample weight
- cross-sectional labels

를 수행한다.


### 3.4 Live Feature Provider

핵심 파일:

- [live_features_online_core.py](/d:/MyApps/Autobot/autobot/paper/live_features_online_core.py)
- [live_features_v4_native.py](/d:/MyApps/Autobot/autobot/paper/live_features_v4_native.py)
- [live_features_v4_common.py](/d:/MyApps/Autobot/autobot/paper/live_features_v4_common.py)

현재 구조는:

- 실시간 ticker ingestion
- 1m online candle state
- parquet bootstrap
- micro translation
- requested v4 feature projection

으로 되어 있어, offline feature contract를 live로 가져오려는 방향이 좋다.


## 4. What The Server Artifacts Say

### 4.1 WS Public Health

실서버 `data/raw_ws/upbit/_meta/ws_public_health.json`에서 확인된 상태:

- `connected = true`
- `run_id = 20260318T150944Z`
- `subscribed_markets_count` 존재
- `last_rx_ts_ms.trade/orderbook` 존재
- reconnect / refresh counters 존재

이건 WS public daemon이 단순히 raw 파일만 쓰는 게 아니라 health contract를 형성하고 있음을 보여 준다.


### 4.2 Micro Aggregate Reports Exist

실서버 `data/parquet/micro_v1/_meta/aggregate_report.json`에서 확인:

- alignment detection
- parse ratios
- markets count
- rows_written_total
- micro_available_ratio

즉 micro는 단순 파생 데이터가 아니라 운영 보고서가 있는 production artifact다.


### 4.3 Micro Validate Report Exists

실서버 `data/parquet/micro_v1/_meta/validate_report.json`이 존재하고, 파일 수/상세/coverage 성격의 진단이 들어 있다.

이는 강점이다.


### 4.4 Features V4 Build Report Exists, But Validate Report Missing

실서버에서 확인한 내용:

- `data/features/features_v4/_meta/build_report.json` 존재
- `data/features/features_v4/_meta/validate_report.json` 없음

이건 매우 중요한 관찰이다.

의미:

- feature build는 artifact를 남기고 있지만
- validate artifact는 운영 경로에서 누락될 수 있다.

이건 이후 `데이터 플랫폼 SSOT` 문서에서 반드시 보완해야 할 부분이다.


### 4.5 Feature Build Quality Signal

실서버 features_v4 build report에서 확인된 패턴:

- `rows_dropped_no_micro`가 여러 시장에서 존재
- 다수 시장이 `MICRO_MANDATORY_DROPS_PRESENT` warning
- 일부 시장은 `one_m_synth_ratio`가 의미 있게 존재

해석:

- 현재 feature platform은 micro mandatory 철학을 지키고 있다.
- 동시에 일부 시장에서는 data availability / densify / rescue 비율이 꽤 중요한 품질 축이다.

즉 향후 강한 모델을 만들 때도 이 quality signal을 first-class metadata로 써야 한다.


## 5. Current Strengths

### 5.1 The Platform Is Already Contract-Heavy

현재 플랫폼은 단순 script 모음이 아니라:

- manifest
- checkpoint
- collect report
- validate report
- aggregate report
- feature spec
- label spec

를 남긴다.

이건 매우 좋은 방향이다.


### 5.2 Live/Offline Feature Parity Is A First-Class Concern

현재 live feature provider가:

- parquet bootstrap
- same feature projection
- missing column hard gate

를 갖는 점은 강하다.


### 5.3 Data Quality Is Visible

micro validate, feature build report, ws health 모두가 남는다.

이건 strong research/evaluation system의 필수 전제다.


## 6. Current Weaknesses

### 6.1 No Unified Data Contract Registry

현재는 각 레이어별 `_meta` artifact는 좋지만,

- raw
- micro
- feature
- live provider

를 한 번에 연결하는 unified lineage registry는 없다.

즉 다음 질문에 한 번에 답하기 어렵다.

- 어떤 raw WS run이 어떤 micro run을 만들었고
- 어떤 micro run이 어떤 features_v4 run의 기반이었으며
- live runtime은 그 중 어느 contract를 소비하는가


### 6.2 Feature Validation Is Not Operationally Mandatory Enough

실서버에서 `features_v4 validate_report`가 없는 건 중요한 신호다.

즉:

- build는 돌지만
- validate artifact가 정책적으로 반드시 남는 것은 아니다.


### 6.3 Dataset Namespace Sprawl

실제 서버에는:

- `features_v3_ac`
- `features_v3_ac_baseline`
- `features_v3_ac_conly`
- backup datasets

같은 실험성 dataset이 남아 있다.

이 자체가 나쁜 것은 아니지만, 다음이 없으면 위험하다.

- status
- owner
- purpose
- retention class


### 6.4 Live Feature Provider Certification Is Still Implicit

현재는 테스트가 많고 구조도 좋지만,

- offline dataset row
- live-built row

를 일정 샘플에 대해 artifact-level로 비교하는 정식 certification report는 약하다.


### 6.5 The Platform Is Not Yet Ready For V5 Data Shapes

앞 문서에서 제안한:

- second candles
- WS candles
- 30-level orderbook
- sequence tensors
- LOB tensors

를 수용하려면 dataset contract 자체가 한 단계 올라가야 한다.


## 7. Stronger Methodology

## 7.1 Create A Data Contract Registry

새 artifact:

- `data/_meta/data_contract_registry.json`

또는 더 나은 방식:

- `data/_meta/data_contract_registry.parquet`

레코드 예시:

- contract_id
- layer: raw/ws/micro/feature/live/runtime
- dataset_name
- run_id
- source_run_ids
- source_paths
- coverage window
- validation status
- retention class


## 7.2 Make Validate Reports Mandatory Promotion Inputs

향후 규칙:

- feature dataset는 `build_report`만으로 충분하지 않다.
- 반드시 `validate_report`가 있어야 한다.

promotion/acceptance에서 확인해야 할 것:

- feature validate artifact 존재
- leakage smoke pass
- stale rows budget within threshold
- missing feature parity pass


## 7.3 Add Live Feature Parity Certification

새 artifact:

- `live_feature_parity_report.json`

내용:

- sampled ts/market pairs
- offline row hash
- live-built row hash
- column-level diff summary
- missing column rates
- acceptable tolerance 여부

이건 paired paper와 live divergence monitoring의 선행조건이다.


## 7.4 Add Data Quality Budgeting

강한 모델/평가/리스크 시스템은 모두 data quality budget를 공유해야 한다.

예:

- market별 `rows_dropped_no_micro`
- one_m synth ratio
- ws parse error ratio
- micro available ratio
- feature missing ratio

이걸:

- training sample weight
- universe selection
- paired paper inclusion
- live risk budget

에 모두 반영할 수 있어야 한다.


## 7.5 Introduce V5 Data Layers Without Breaking V4

권장 신규 dataset:

- `data/parquet/candles_second_v1`
- `data/parquet/ws_candle_v1`
- `data/parquet/lob30_v1`
- `data/parquet/sequence_v1`

핵심 원칙:

- `v4`는 계속 production baseline
- `v5`용 데이터는 병렬 layer로 추가
- 기존 `features_v4` 경로를 깨지 않는다


## 7.6 Add Retention Classes

각 dataset/artifact에 retention class를 붙인다.

예:

- `hot`
- `warm`
- `cold`
- `archive`
- `scratch`

특히 실서버에서는:

- raw WS = hot/warm
- micro/features = warm/cold
- lineage/meta/report = cold
- temporary backups = archive or purge candidate


## 7.7 Make Data Health A First-Class Runtime Input

현재 health/report는 mostly operator-facing이다.

다음 단계에서는 runtime도 직접 써야 한다.

예:

- WS stale -> breaker
- micro coverage poor -> risk haircut
- feature parity broken -> rollout halt
- dataset validate missing -> acceptance fail


## 8. Recommended New Artifacts

권장 추가 artifact:

- `data_contract_registry.json`
- `dataset_retention_registry.json`
- `live_feature_parity_report.json`
- `feature_dataset_certification.json`
- `raw_to_feature_lineage_report.json`


## 9. Recommended Implementation Order

### Step 1

- data contract registry
- retention class metadata

### Step 2

- feature validate artifact mandatory
- acceptance/promotion check integration

### Step 3

- live feature parity certification

### Step 4

- v5 data layers (`1s`, `ws_candle`, `lob30`, `sequence`)

### Step 5

- quality-budget integration into model/risk/runtime


## 10. Strongest Combined View

세 개의 기존 문서와 합치면 데이터 플랫폼의 역할은 명확하다.

- predictor는 좋은 신호를 만들고
- evaluation ladder는 그 신호를 검증하고
- risk/live control은 그 신호를 안전하게 집행한다

그리고 데이터 플랫폼은:

- 이 세 축 모두가 믿을 수 있는 공통 입력 계약

을 제공해야 한다.

즉 강한 시스템의 첫 번째 토대는 결국 데이터 플랫폼이다.


## 11. References

### Internal Code

- [ws_public_collector.py](/d:/MyApps/Autobot/autobot/data/collect/ws_public_collector.py)
- [merge_micro_v1.py](/d:/MyApps/Autobot/autobot/data/micro/merge_micro_v1.py)
- [trade_aggregator_v1.py](/d:/MyApps/Autobot/autobot/data/micro/trade_aggregator_v1.py)
- [orderbook_aggregator_v1.py](/d:/MyApps/Autobot/autobot/data/micro/orderbook_aggregator_v1.py)
- [pipeline_v4.py](/d:/MyApps/Autobot/autobot/features/pipeline_v4.py)
- [feature_set_v4_live_base.py](/d:/MyApps/Autobot/autobot/features/feature_set_v4_live_base.py)
- [live_features_online_core.py](/d:/MyApps/Autobot/autobot/paper/live_features_online_core.py)
- [live_features_v4_native.py](/d:/MyApps/Autobot/autobot/paper/live_features_v4_native.py)

### Related Blueprints

- [TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
- [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)
- [RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
