# TF 5m Global Impact Map

## Purpose

이 문서는 `5m -> 1m` 전체 전환을 시작하기 전에, 현재 `tf=5m`가 전역에서 어디서 정의되고 어디로 전파되는지 정리한 시작 기준 문서다.

원칙:

- `source -> propagation -> consumer -> validation -> ops/test`
- 설정 하나를 바꾸기 전에 전역 사용처를 먼저 닫는다.

## 1. Source

### 1.1 Primary config source

- [config/strategy.yaml](/d:/MyApps/Autobot/config/strategy.yaml)
  - `strategy.model_alpha_v1.tf`
  - 현재 기본값: `5m`

### 1.2 Secondary config/default sources

- [config/train.yaml](/d:/MyApps/Autobot/config/train.yaml)
  - training defaults `tf: 5m`
- [config/features_v4.yaml](/d:/MyApps/Autobot/config/features_v4.yaml)
  - `tf: 5m`
- [config/features_v3.yaml](/d:/MyApps/Autobot/config/features_v3.yaml)
  - `tf: 5m`
- [config/features_v2.yaml](/d:/MyApps/Autobot/config/features_v2.yaml)
  - `tf: 5m`
- [config/backtest.yaml](/d:/MyApps/Autobot/config/backtest.yaml)
  - backtest default `tf: 5m`

### 1.3 Non-source but related

- [config/base.yaml](/d:/MyApps/Autobot/config/base.yaml)
  - `live.strategy.decision_interval_sec = 1.0`
  - 이 값은 loop cadence이고 `tf` source는 아니다

## 2. Propagation

### 2.1 CLI defaults loader

- [autobot/cli.py](/d:/MyApps/Autobot/autobot/cli.py)
  - `_paper_defaults(...)`
  - `model_alpha_v1` 설정을 읽어 defaults dict로 만든다
  - 여기서 `tf = str(model_alpha_cfg.get("tf", "5m"))`

### 2.2 Live strategy defaults

- [autobot/cli.py](/d:/MyApps/Autobot/autobot/cli.py)
  - `_live_strategy_defaults(...)`
  - `_paper_defaults(...)`를 재사용한다

### 2.3 Live runtime settings construction

- [autobot/cli.py](/d:/MyApps/Autobot/autobot/cli.py)
  - `_build_live_model_alpha_runtime_settings(...)`
  - `tf=str(strategy_defaults.get("tf", "5m"))`

### 2.4 Paper/backtest settings construction

- [autobot/cli.py](/d:/MyApps/Autobot/autobot/cli.py)
  - paper run settings 생성부
  - backtest run settings 생성부
  - `args.tf or defaults["tf"]` 패턴이 매우 많이 퍼져 있음

## 3. Runtime Consumers

### 3.1 Live runtime

- [autobot/live/model_alpha_runtime.py](/d:/MyApps/Autobot/autobot/live/model_alpha_runtime.py)
  - `LiveModelAlphaRuntimeSettings.tf = "5m"`
  - `interval_ms = _interval_ms_from_tf(settings.tf)`
  - `decision_ts_ms = floor(ticker.ts_ms / interval_ms) * interval_ms`
  - 실시간 WS를 받아도 실제 모델 decision cadence는 `tf` bucket 기준

### 3.2 Paper runtime

- [autobot/paper/engine.py](/d:/MyApps/Autobot/autobot/paper/engine.py)
  - `PaperRunSettings.tf`
  - `_interval_ms_from_tf(settings.tf)`
  - live/paper parity 경로도 동일한 cadence 가정

### 3.3 Strategy layer

- [autobot/strategy/model_alpha_v1.py](/d:/MyApps/Autobot/autobot/strategy/model_alpha_v1.py)
  - `interval_ms`를 기반으로 timeout, replace interval, cooldown, hold/exit timing을 계산

### 3.4 Micro snapshot and gating

- [autobot/strategy/micro_snapshot.py](/d:/MyApps/Autobot/autobot/strategy/micro_snapshot.py)
  - tf 기반 fallback tolerance
- [autobot/strategy/micro_gate_v1.py](/d:/MyApps/Autobot/autobot/strategy/micro_gate_v1.py)
  - tf aware gating payload

## 4. Feature/Data Consumers

### 4.1 features_v4 pipeline

- [autobot/features/pipeline_v4.py](/d:/MyApps/Autobot/autobot/features/pipeline_v4.py)
  - 현재 `features_v4`는 `5m`만 지원
  - 명시적으로 `only 5m` 제한이 있음

### 4.2 Live feature providers

- [autobot/paper/live_features_v4_native.py](/d:/MyApps/Autobot/autobot/paper/live_features_v4_native.py)
- [autobot/paper/live_features_v5.py](/d:/MyApps/Autobot/autobot/paper/live_features_v5.py)
- [autobot/paper/live_features_online_core.py](/d:/MyApps/Autobot/autobot/paper/live_features_online_core.py)
- [autobot/paper/live_features_multitf_base.py](/d:/MyApps/Autobot/autobot/paper/live_features_multitf_base.py)

현재 특징:

- 실시간 원천은 `1s`, `1m`, micro, lob를 다 보지만
- 최종 decision contract는 여전히 `5m` 중심

### 4.3 Data layers already available

- `candles_second_v1` : `1s`
- `ws_candle_v1` : `1s`, `1m`
- `micro_v1` : `1m`, `5m`
- `lob30_v1`
- `sequence_v1`

즉 데이터 재료는 `1m` 전환에 충분히 가까움

## 5. Validation / Acceptance Consumers

- [scripts/candidate_acceptance.ps1](/d:/MyApps/Autobot/scripts/candidate_acceptance.ps1)
  - `--tf $Tf`
  - acceptance / runtime_parity backtest 모두 tf를 직접 전달
- [scripts/v5_governed_candidate_acceptance.ps1](/d:/MyApps/Autobot/scripts/v5_governed_candidate_acceptance.ps1)
- [scripts/v5_fusion_fast_validation.ps1](/d:/MyApps/Autobot/scripts/v5_fusion_fast_validation.ps1)
- [scripts/v5_fusion_runtime_parity_fast_validation.ps1](/d:/MyApps/Autobot/scripts/v5_fusion_runtime_parity_fast_validation.ps1)

즉 `1m` 전환은 live만이 아니라 acceptance/runtime parity fast path도 같이 봐야 한다.

## 6. Training Consumers

- [autobot/models/train_v5_panel_ensemble.py](/d:/MyApps/Autobot/autobot/models/train_v5_panel_ensemble.py)
- [autobot/models/train_v5_sequence.py](/d:/MyApps/Autobot/autobot/models/train_v5_sequence.py)
- [autobot/models/train_v5_lob.py](/d:/MyApps/Autobot/autobot/models/train_v5_lob.py)
- [autobot/models/train_v5_tradability.py](/d:/MyApps/Autobot/autobot/models/train_v5_tradability.py)
- [autobot/models/train_v5_fusion.py](/d:/MyApps/Autobot/autobot/models/train_v5_fusion.py)

특히 위험한 지점:

- `features_v4`가 현재 `5m` 전용 제약을 갖고 있음
- `fusion_runtime_input_contract`, `runtime_parity`, `live provider`의 cadence 정합성을 같이 유지해야 함

## 7. Ops / Dashboard

- [autobot/dashboard_server.py](/d:/MyApps/Autobot/autobot/dashboard_server.py)
  - current activity
  - runtime topology
  - training acceptance
  - live lane summaries
- [autobot/dashboard_assets/dashboard.js](/d:/MyApps/Autobot/autobot/dashboard_assets/dashboard.js)

`tf`가 직접 노출되는 곳은 적지만, stage naming / runtime status / validation artifact interpretation에 영향을 줌

## 8. Tests

전역적으로 `5m`를 전제한 테스트가 매우 많다.

주요 군:

- `tests/test_live_model_alpha_runtime.py`
- `tests/test_paper_engine_*`
- `tests/test_backtest_model_alpha_integration.py`
- `tests/test_candidate_acceptance_*`
- `tests/test_train_v5_*`
- `tests/test_features_*`

즉 `1m` 전환은 테스트 범위도 넓다.

## 9. What This Means

`5m -> 1m` 전환의 제일 시작점은 아래 3개다.

1. [config/strategy.yaml](/d:/MyApps/Autobot/config/strategy.yaml)
2. [autobot/cli.py](/d:/MyApps/Autobot/autobot/cli.py)
3. [autobot/live/model_alpha_runtime.py](/d:/MyApps/Autobot/autobot/live/model_alpha_runtime.py)

하지만 실제 full-path 전환을 하려면 아래도 같이 풀어야 한다.

- [autobot/features/pipeline_v4.py](/d:/MyApps/Autobot/autobot/features/pipeline_v4.py)
- [autobot/paper/engine.py](/d:/MyApps/Autobot/autobot/paper/engine.py)
- [scripts/candidate_acceptance.ps1](/d:/MyApps/Autobot/scripts/candidate_acceptance.ps1)
- `tests/` 전반

## 10. Immediate Recommendation

전체 전환 첫 코드 단계는 다음 순서가 가장 안전하다.

1. `config/strategy.yaml` / `cli.py` / `live runtime`에 `1m` 경로를 옵션으로 추가
2. `features_v4`의 `5m only` 제약을 어떻게 다룰지 결정
3. `paper/acceptance/runtime parity`에 `1m validation lane` 추가
4. 그 후 기본값 전환 여부 판단

