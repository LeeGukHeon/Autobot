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

## 18) 단계별 티켓 로드맵
- T00: Repo Bootstrap
- T01: CSV -> Parquet + Schema
- T02: Upbit REST + JWT + RateLimiter
- T03: orders/chance + instruments cache
- T04: Top20 scanner + candidate generator
- T05: Backtest v1
- T06: Risk v1
- T07: Paper v1
- T08: Live execution v1
- T09: ML pipeline v1
- T10: Meta risk-on/off

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
