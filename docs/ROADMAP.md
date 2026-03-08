# Upbit AutoBot 프로젝트 시작 로드맵/설계서

- Version: v1.0
- Date: 2026-03-02
- Platform: Windows
- Root: `D:\MyApps\Autobot`

목적: 데이터 학습 -> 백테스트 -> 라이브 페이퍼런 -> 실거래까지 하나의 일관된 구조(인터페이스/데이터 계약/리스크 관리)로 연결되는 업비트 자동매매 시스템을 구축합니다.

## 0) 핵심 원칙 (절대 규칙)

### API 규칙 절대 준수
- 업비트 REST API Rate Limit 그룹별 제한을 준수한다.
- `Remaining-Req` 헤더로 잔여 호출량을 추적하고 스로틀링/백오프를 자동 적용한다.

### 하드코딩 최소화
- 최소 주문금액, 수수료율, 주문 가능 타입, 호가단위(틱)는 API 조회 후 캐시해서 사용한다.
- `GET /v1/orders/chance`를 주문 검증의 SSOT로 사용한다.
- `GET /v1/orderbook/instruments`의 `tick_size`를 가격 라운딩에 사용한다.

### 시장가 ord_type 금지
- 실거래는 기본 `limit`만 사용.
- 긴급 실행은 `limit + IOC` 또는 `best + IOC/FOK` 설계.

### 전략/학습/실행/리스크 분리
- 전략이 바뀌어도 실행엔진/리스크/데이터 파이프라인은 재사용.
- Backtest/Paper/Live는 동일 인터페이스, `ExecutionGateway`만 교체.

### 소액(5만원)에서도 동작
- KRW 최소 주문 5,000원과 수수료/틱/체결실패를 반영해 주문 성립 가능한 수량/금액만 거래.

## 1) 목표/범위

### 1.1 목표
- (A) 데이터 학습 파이프라인 자동화
- (B) 백테스트 엔진(수수료/틱/지정가 체결모델/부분체결/미체결)
- (C) 라이브 페이퍼런(실시간 신호 + 가상 체결)
- (D) 실거래(지정가 기반 + 취소/재주문/타임아웃)
- (E) 리스크 관리(TP/SL/트레일링/일일손실제한/킬스위치)

### 1.2 비목표
- 초단타 HFT 최적화
- 타 거래소 멀티연동
- 레버리지/선물

## 2) 업비트 제약/정책

### 2.1 Rate Limit
- Quotation REST: 그룹별 초당 최대 10회 (IP)
- Exchange default: 초당 최대 30회 (계정)
- order 그룹: 초당 최대 8회 (계정)
- order-cancel-all: 2초당 1회
- 초과 시 429, 지속 시 418

### 2.2 최소 주문/틱
- KRW 최소 주문 5,000 KRW
- 틱 정책은 변경 가능하므로 `orderbook/instruments`로 확인

### 2.3 주문 타입
- 지정가: `ord_type=limit`, `time_in_force=ioc|fok|post_only`
- 최유리 지정가: `ord_type=best`, `time_in_force=ioc|fok`
- SMP 옵션: `smp_type`

### 2.4 수수료/주문가능정보
- `orders/chance`의 `bid_fee`, `ask_fee`, `market.*.min_total`, `market.max_total`, 지원 타입을 참조

## 3) 사용 데이터

### 3.1 보유
- 전 기간: 273개 코인 5m/15m/60m/240m CSV
- 최근 2년: 123개 코인 1m CSV

### 3.2 초기 활용
- 멀티타임프레임 시그널 학습/백테스트
- 1분봉은 체결모델 고도화/실행 보조지표

### 3.3 선택 데이터
- 실시간 orderbook/trade WS
- 마켓 상태 히스토리

### 3.4 PC 제약(6C/12T, 16GB)
- 전체 CSV 일괄 pandas 로드 금지
- CSV -> Parquet
- 코인x타임프레임 배치 처리
- 병렬 n_jobs 4~6 제한

## 4) 전체 아키텍처

### 4.1 모듈 분리
- Data Layer
- Feature & Label Layer
- Model Layer
- Strategy Layer
- Risk Layer
- Execution Layer
- Modes: Backtest/Paper/Live Gateway

핵심: Strategy/Risk 공통, ExecutionGateway 교체로 모드 전환.

## 5) 프로젝트 디렉터리 구조

```text
D:\MyApps\Autobot
├─ README.md
├─ docs\
│  ├─ ROADMAP.md
│  ├─ ADR\
│  ├─ TICKETS\
│  ├─ CHANGE_POLICY.md
│  └─ CONFIG_SCHEMA.md
├─ config\
│  ├─ base.yaml
│  ├─ upbit.yaml
│  ├─ strategy.yaml
│  ├─ risk.yaml
│  ├─ model.yaml
│  └─ secrets.example.env
├─ data\
│  ├─ raw\
│  ├─ parquet\
│  ├─ features\
│  ├─ backtest\
│  └─ paper\
├─ models\
│  ├─ registry\
│  └─ artifacts\
├─ logs\
├─ autobot\
│  ├─ __init__.py
│  ├─ cli.py
│  ├─ common\
│  ├─ data\
│  ├─ features\
│  ├─ models\
│  ├─ strategy\
│  ├─ risk\
│  ├─ execution\
│  ├─ upbit\
│  └─ backtest\
├─ python\
│  ├─ requirements.txt
│  └─ autobot\   # deprecated mirror (optional sync target)
└─ cpp\
   ├─ CMakeLists.txt
   ├─ third_party\
   └─ src\
      ├─ backtest_core\
      └─ bindings\
```

패키지 SSOT 정책:
- 메인 개발/실행 패키지는 루트 `autobot/`를 사용한다.
- `python/autobot/`는 기본 실행 경로가 아니며, 필요 시 동기화 스크립트로만 갱신한다.

## 6) 기술 스택/설치
- Python 3.11, VS Build Tools, CMake/Ninja
- 필수 라이브러리: pydantic, pyyaml, numpy/pandas, polars/pyarrow, duckdb, httpx, websockets, tenacity, loguru, orjson
- ML: scikit-learn, lightgbm
- C++ 확장(선택): pybind11

## 7) 설정 중심 설계
- 모든 환경/파라미터는 `config/*.yaml`
- 민감정보는 `.env`
- `mode`, `universe`, `execution`, `risk`, `upbit` 섹션 표준화

## 8) 업비트 연동 설계
- JWT 인증(access_key, nonce, query_hash)
- POST는 JSON body
- 주문 전 검증: orders/chance + orderbook/instruments + 최소주문 + 잔고/locked + 수수료
- 그룹별 RateLimitManager 필수

## 9) 거래대금 상위 20 스캔
- KRW 마켓 ticker `acc_trade_price_24h` 기준 Top20
- 초저가/거래정지/유의 종목 필터

## 10) 리스크 관리
- min_order_krw = max(5000, config.min_order_krw, orders/chance.min_total)
- TP/SL/Trailing
- 긴급청산은 `best + ioc`
- 연속 실패/일일손실 초과 시 거래중단

## 11) 주문 실패 대응
- 상태머신: CREATED -> SUBMITTED -> OPEN -> PARTIAL -> FILLED
- 재호가 정책(타임아웃/틱 조정/횟수 제한)

## 12) 백테스트 설계
- 룩어헤드/누수 방지
- Walk-forward
- 지정가 체결모델, 보수적 fill 옵션, 수수료/틱 반영
- 산출: 트레이드 로그 + 성과 지표 + 리포트

## 13) 라이브 페이퍼런 설계
- 실시간 신호는 라이브와 동일
- 주문만 가상체결
- WS 기반 권장

## 14) 자가판단(메타)
- 레짐/유동성/신뢰도/최근성능/드리프트를 입력으로 risk-on/off 결정
- 출력: `risk_multiplier`, `allowed_strategies`, `max_positions_override`

## 15) ML 파이프라인
- LightGBM 기반 v1
- 워크포워드 검증
- 모델 레지스트리 버전 관리
- 승인된 모델만 실거래 반영

## 16) 레거시 삭제/신규 삽입 정책
- 티켓에 `[DELETE]`와 `[ADD]`를 필수 포함
- 코드 삭제는 가능하되 변경이력은 ADR에 보존

## 17) 개발/테스트/운영
- Unit/Integration/Paper 계층 테스트
- 의사결정 이벤트 로깅 + SQLite 복구 가능성 확보
- API 키 보안(출금권한 비활성/IP 제한)

## 17.1) 현재 운영 로드맵 (2026-03 기준)

### 모델 승급 운영
- `train_v3_mtf_micro`는 새 학습 결과를 즉시 champion으로 승급하지 않고 `candidate`로 등록한다.
- 기본 운영 모델은 `champion_v3`, 최신 실험 결과는 `latest_candidate_v3`로 분리한다.
- 매일 `00:10` 데이터 수집 이후 `candidate acceptance`를 실행한다.
- acceptance 기본 흐름은 `daily pipeline -> candidate train -> backtest sanity gate -> 3h paper final gate -> pass 시 promote -> 활성 runtime restart`다.

### 승급 판단 정책
- 승급 판단은 `backtest = sanity gate`, `paper = final gate` 원칙으로 운영한다.
- 기본 정책은 `paper_final_balanced`다.
- backtest는 후보 모델의 최소 거래 가능성과 학습/실행 증빙을 확인하는 sanity filter 역할만 한다.
- 최종 promote 여부는 3시간 paper soak가 결정한다.
  - 운영 gate:
    - fallback ratio
    - tier diversity
    - policy events
  - 성능 gate:
    - 최소 fills
    - 최소 realized pnl
- 후보-챔피언의 offline compare 지표는 계속 리포트에 남기지만, promote의 직접 결정권은 paper 쪽에 둔다.

### 즉시 다음 개선 항목
- 현재 `DSR-style` sanity check를 유지하고, 다음 단계로 `Reality Check / SPA` 계열 검정을 승급 게이트에 추가한다.
- 단일 backtest window 대신 rolling walk-forward acceptance를 도입한다.
- paper soak 리포트를 운영 검증용과 성능 해석용으로 분리한다.
- promote 후 활성 `paper/live` 서비스 자동 재시작을 기본 운영 루프로 유지한다.

## 17.3) 운영 레이어 v1 (T18.3)

### 원칙
- 알파 selection은 learned output을 유지한다.
- backtest는 하나의 fixed compare profile로 candidate-vs-champion sanity gate만 수행한다.
- paper/live current-market adaptation은 운영 레이어에서 처리한다.

### 구현 상태
- `rolling paper evidence` 반영
  - paper final gate는 단순 end-of-run pnl만이 아니라 rolling window evidence도 본다.
- `paper history evidence` 반영
  - 최근 run들의 비음수 비율, 양수 비율, 중앙 micro quality도 paper final gate에 포함한다.
- `risk multiplier` 반영
  - regime score와 micro feature quality를 바탕으로 sizing을 보정한다.
- `dynamic max_positions` 반영
  - breadth/regime quality에 따라 effective slot count를 조정한다.
- `execution aggressiveness` v2 반영
  - runtime은 micro quality와 session 상태에 따라 `price_mode`뿐 아니라 `timeout/replace/max_replaces/max_chase_bps`까지 조정한다.
- `micro quality composite` 반영
  - spread, depth, coverage, snapshot age를 합성 점수화한다.
  - 품질이 극단적으로 낮으면 runtime에서 진입을 차단하고, paper final gate는 평균 quality 하한도 본다.
- `DSR-style sanity check` 반영
  - backtest sanity gate는 `equity.csv` 기반의 lightweight `deflated_sharpe_ratio_est`를 기록하고 최소 하한을 확인한다.

### 설계 문서
- 상세 내용은 `docs/TICKETS/T18_3_operational_runtime_overlay_v1.md`를 따른다.

## 17.2) 코인 연구 정렬 알파 vNext (T18.2)

### 왜 지금 이 작업을 하는가
- 현재 운영 baseline은 `feature_set=v3`, `label_set=v1`, `trainer=train_v3_mtf_micro`, `strategy=model_alpha_v1`로 안정화되어 있다.
- 하지만 현재 학습 목표는 `12 x 5m` 이후 종가 기반 이진분류이고, 실제 운용은 cross-sectional selection + execution overlay 구조다.
- 즉 데이터가 부족하다기보다 `학습 목표`, `피처 계약`, `실제 선택 로직` 사이의 미스매치가 다음 병목이다.

### 방향성
- 최근 코인 단면 연구와 가장 잘 맞는 방향은 다음 5개다.
- `label`을 절대 방향 이진분류 중심에서 `순이익/상대순위` 중심으로 재설계한다.
- `cross-coin spillover`와 `market breadth`를 feature contract에 추가한다.
- 가격 + 거래량 기반 `trend aggregate` 피처를 추가한다.
- 코인 24/7 시장의 `hour-of-day`, `day-of-week`, `overlap` 주기성을 피처에 반영한다.
- `liquidity x risk x momentum` 상호작용을 소수의 명시적 피처로 추가한다.

### 레거시 정리 원칙
- 운영 baseline인 `v3/v1/train_v3_mtf_micro/model_alpha_v1`은 freeze한다.
- 기존 `v3`나 `label_v1` 의미를 in-place로 바꾸지 않는다.
- 연구용 새 계약은 새 버전(`v4`, `label_v2`, 새 trainer wrapper`)으로 추가한다.
- 단, 구현은 copy-paste가 아니라 공통 빌딩 블록 추출 후 thin wrapper 방식으로 진행한다.
- `model_alpha_v1`은 점수 기반 selection/portfolio/runtime handoff 계약이 유지되는 한 그대로 재사용한다.

### 실행 순서
- 1단계: `label_v2` 설계 및 offline dataset contract 확정 `완료`
- 2단계: `feature_set_v4`에 spillover/trend/periodicity/interaction pack 추가 `사실상 완료`
  - 현재는 `spillover + breadth + periodicity + trend-volume + interaction` pack까지 반영됨
- 3단계: 새 trainer와 rolling acceptance 구축 `완료`
  - 현재는 `v4 trainer + anchored walk-forward evidence + execution-aware backtest acceptance`까지 반영됨
  - `model_alpha_v1` backtest 경로도 이제 `feature_set=v4`를 허용함
- 4단계: `LIVE_V4` parity와 paper preset 연결 `진행 중`
  - `LIVE_V4` paper provider 추가
  - `model_alpha_v1` paper 경로도 `feature_set=v4`를 허용함
  - `paper alpha`는 `live_v4`, `candidate_v4`, `offline_v4` preset을 지원함
  - runtime은 이제 `selection_recommendations.json`을 우선 읽어 learned selection breadth를 사용함
    - learned runtime knobs:
      - `recommended_top_pct`
      - `recommended_min_candidates_per_ts`
    - `min_prob`는 계속 registry threshold를 사용함
  - runtime preset의 lane 숫자는 이제 fallback 값이다:
    - `live_v3`: fallback `top_pct=0.10`, fallback `min_candidates_per_ts=3`, `min_prob -> registry`
    - `live_v4/candidate_v4`: fallback `top_pct=0.50`, fallback `min_candidates_per_ts=1`, `min_prob -> registry`
  - acceptance stays on one shared fixed compare profile across `v3` and `v4`:
    - `top_pct=0.50`
    - `min_prob=0.00`
    - `min_candidates_per_ts=1`
    - `paper_max_fallback_ratio=0.20`
    - `paper_min_orders_filled=2`
    - `paper_min_realized_pnl_quote=0.0`
  - generic `candidate_acceptance.ps1` defaults now match that same shared compare profile, so direct invocation no longer falls back to old legacy `0.52/0.10/3` values
  - ad-hoc OCI paper helper now defers to `paper alpha --preset` runtime defaults instead of hardcoding selection cutoff values
  - `LIVE_V4`는 요청된 `v4` 컬럼이 하나라도 빠지면 zero-fill로 점수를 내지 않고 `MISSING_V4_FEATURE_COLUMNS` hard gate로 빈 frame을 반환함
  - `LIVE_V3`는 그대로 유지하고, 실거래 서비스는 아직 `v4`로 올리지 않음
- 5단계: paper soak 검증 통과 시 champion 승급 경로 연결 `진행 중`
  - 공통 `candidate_acceptance.ps1` 실행기 추가
  - `v3_candidate_acceptance.ps1`, `v4_candidate_acceptance.ps1` wrapper 분리
  - `v4`도 이제 `train -> candidate/champion backtest -> paper soak -> promote` 루프를 같은 계약으로 탈 수 있음
  - `v4_candidate_acceptance.ps1`는 trainer가 미리 만든 `walk-forward + execution_acceptance` 증빙을 `required` gate로 읽음
  - trainer는 이제 `selection_recommendations.json`도 함께 저장한다
    - runtime은 이 learned recommendation을 우선 사용하고
    - acceptance는 의도적으로 사용하지 않는다
  - trainer 내부 `execution_acceptance`도 이제 wrapper와 동일한 `top_n/top_pct/min_prob/min_candidates/hold_bars` compare override를 받아 같은 고정 기준으로 비교함
  - lane별 acceptance는 같은 fixed compare profile을 공유한다
    - 공통: `paper_final_balanced`, `top_pct=0.50`, `min_prob=0.0`, `min_candidates=1`, `paper_max_fallback_ratio=0.20`, `paper_min_orders_filled=2`, `paper_min_realized_pnl_quote=0.0`
    - 차이는 trainer evidence만 남긴다
      - `v3`: `trainer_evidence=ignore`
      - `v4`: `trainer_evidence=required`
  - `v4`의 완화 기준은 임시 bootstrap 설정으로 취급한다
    - 되돌림 트리거: `v4` usable history가 대략 `14일+` 확보되고, 최근 rolling paper fallback ratio가 안정적으로 `0.10` 아래에 머무를 때
    - 그 시점에는 fallback runtime breadth(`top_pct/min_candidates`)와 `paper_max_fallback_ratio`를 다시 조이는 작업을 우선순위로 둔다
  - 운영 스크립트는 `v4` 선택지를 노출하지만 기본 rollout preset은 아직 `v3`에 둠
  - 현재 운영 기본은 `00:10` shared orchestrator다
    - `daily_micro_pipeline_for_server.ps1`를 한 번만 돌려 raw/micro/report를 갱신한다
    - 그 다음 `features_v3`와 `features_v4`를 같은 batch date로 갱신한다
    - 마지막에 `v3`와 `v4` acceptance를 병렬 fan-out 한다
    - 상시 paper는 `v3`와 `v4`를 lane별 systemd unit으로 동시에 유지할 수 있다
      - `v3`: `autobot-paper-alpha.service` -> `paper alpha --preset live_v3`
      - `v4`: `autobot-paper-v4.service` -> `paper alpha --preset live_v4`
    - 승급 시에는 각 lane이 자기 runtime unit만 자동 재기동한다
      - `v3` acceptance -> `autobot-paper-alpha.service`
      - `v4` acceptance -> `autobot-paper-v4.service`
    - `live_v4` lane은 fresh install에서 `champion_v4`가 비어 있으면 최신 candidate/latest run으로 bootstrap promote를 한 번 수행한 뒤 기동한다
    - orchestrator는 각 child lane stdout의 고유 `report=` 경로를 우선 읽고, lane-global `latest.json`은 신선한 경우에만 fallback으로 사용한다
    - `paper_micro_smoke`는 주문 0건 구간을 `fallback_ratio=1.0`로 오해하지 않고 `min_orders_submitted` 실패와 분리해서 기록한다
    - 수동 `paper alpha --preset live_v4` 실행 시 `champion_v4` 포인터가 없으면 `latest_candidate_v4`, 그다음 `latest_v4`로 안전 fallback 한다
  - 별도 지연 실행용 `autobot-daily-v4-accept.timer`는 fallback 운영 경로로만 남긴다
  - 단, 실제 `v4` champion promote와 runtime rollout은 아직 운영 적용 전

### 설계 문서
- 상세 설계와 구현/삭제 기준은 `docs/TICKETS/T18_2_crypto_alpha_research_alignment_v1.md`를 따른다.

## 18) 단계별 티켓 로드맵
- T00: Repo Bootstrap
- T01: CSV -> Parquet + Schema
- T02: Upbit REST + JWT + RateLimiter
- T03: orders/chance + instruments cache
- T04: Top20 scanner + candidate generator
- T05: Backtest v1
- T06: Live StateStore + Reconcile v1
- T06.1: Live Ops completion (apply cancel + polling daemon + intents wiring)
- T07: Private WS sync v1 (`myOrder`/`myAsset`)
- T08: Live execution v1 (order manager + state machine)
- T09: ML pipeline v1
- T10: Meta risk-on/off
- T18.2: Crypto alpha vNext (`label_v2`, `feature_set_v4`, rolling acceptance, legacy freeze/cleanup)

## 19) 첫 작업 지시문 요약
- 디렉터리 구조 생성
- `docs/ROADMAP.md` 저장
- `docs/ADR/0001-initial-architecture.md` 작성
- `config/*.yaml`, `secrets.example.env` 생성
- `autobot/cli.py` 스켈레톤 + `--help`
- `python/requirements.txt` 초안 작성
- `data/models/logs` gitignore 적용
- 하드코딩 금지, 인터페이스 기반 설계

## 체크리스트
- 체결(Execution)과 신호(Alpha) 분리
- 틱/최소주문/지원타입 사전 검증
- 과최적화보다 현실 체결 모델 우선
- 소액(5만원)에서 수수료/스프레드 영향 반영
- RateLimit 위반 방지(무한 재시도 금지)
