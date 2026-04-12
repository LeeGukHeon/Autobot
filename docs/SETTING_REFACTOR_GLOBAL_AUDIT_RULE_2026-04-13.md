# Setting Refactor Global Audit Rule

## Purpose

이 문서는 앞으로 모든 `설정/타임프레임/운영 모드` 리팩토링의 시작 기준을 정의한다.

핵심 원칙은 하나다.

`설정 하나를 바꾸기 전에, 전역 검색으로 실제 사용처를 모두 찾고, source -> propagation -> runtime -> validation -> dashboard -> server contract까지 닫은 뒤 수정한다.`

이 규칙은 특히 아래 같은 변경에 강제 적용한다.

- `tf` 변경 (`5m -> 1m`, `1m -> tick-aware` 등)
- live/paper/backtest preset 변경
- rollout mode / pointer source / model family source 변경
- state DB path / unit name / timer name 변경
- dataset root / snapshot pointer / contract id 변경

## Mandatory Rule

세팅을 바꾸는 리팩토링은 아래 순서를 거치지 않으면 시작하지 않는다.

1. `definition source`를 찾는다.
   예:
   - config 기본값
   - CLI builder
   - systemd env override
   - script wrapper arg

2. 전역 검색으로 `모든 참조`를 찾는다.
   최소 검색 범위:
   - `scripts/`
   - `autobot/`
   - `tests/`
   - `docs/`

3. 각 참조를 아래 6개 bucket 중 어디에 속하는지 분류한다.
   - source
   - propagation
   - runtime consumer
   - validation / acceptance
   - operations / dashboard
   - tests / docs

4. 변경 전, `영향 맵`을 문서나 작업 메모에 적는다.

5. 수정은 `source -> propagation -> consumer -> tests` 순서로 한다.

6. 서버 반영 전, 최소한 아래를 다시 확인한다.
   - preflight 영향
   - pointer/state 영향
   - live/paper/backtest contract 영향
   - dashboard 표시 영향

## Required Audit Template

세팅 변경 작업은 최소한 아래 형식으로 시작한다.

### 1. Setting

- name:
- current value:
- target value:

### 2. Definition Sources

- config:
- cli:
- env/systemd:
- wrapper script:

### 3. Runtime Consumers

- live:
- paper:
- backtest:
- training:
- acceptance:

### 4. Ops / Display

- dashboard:
- preflight:
- reports:

### 5. Tests

- existing tests that already cover it:
- tests that must be added:

## First Starting Point For 1m Transition

현재 `5m -> 1m` 전체 전환의 최초 시작점은 아래 두 군데다.

1. [autobot/cli.py](/d:/MyApps/Autobot/autobot/cli.py)
   - `_build_live_model_alpha_runtime_settings()`
   - 여기서 live runtime의 `tf`가 처음 확정된다.

2. [autobot/live/model_alpha_runtime.py](/d:/MyApps/Autobot/autobot/live/model_alpha_runtime.py)
   - `interval_ms = _interval_ms_from_tf(settings.tf)`
   - `decision_ts_ms = floor(ticker.ts_ms / interval_ms) * interval_ms`
   - 여기서 실제 live decision cadence가 `tf` 버킷으로 고정된다.

즉 `1m` 전환의 첫 코드 단위는:

- [autobot/cli.py](/d:/MyApps/Autobot/autobot/cli.py)
- [autobot/live/model_alpha_runtime.py](/d:/MyApps/Autobot/autobot/live/model_alpha_runtime.py)
- [autobot/live/model_alpha_runtime_bootstrap.py](/d:/MyApps/Autobot/autobot/live/model_alpha_runtime_bootstrap.py)

## Global Search Checklist For tf Refactor

`tf` 변경 작업은 최소 아래 문자열을 전역 검색한다.

- `tf`
- `decision_interval_sec`
- `_interval_ms_from_tf`
- `paper_live_candles_dataset`
- `backtest alpha`
- `runtime_parity`
- `paired_runtime`
- `LiveModelAlphaRuntimeSettings`
- `PaperRunSettings`
- `ModelAlphaSettings`
- `candles_second_v1`
- `ws_candle_v1`
- `micro_v1`
- `lob30_v1`

## Rule For Completion

세팅 리팩토링은 아래 3개가 모두 만족되기 전까지 완료로 보지 않는다.

1. 전역 사용처 인벤토리가 문서화되어 있을 것
2. consumer 계층까지 실제 코드가 연결되어 있을 것
3. 관련 운영 상태와 대시보드 표시까지 정합성이 확인될 것

