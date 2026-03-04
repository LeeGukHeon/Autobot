# T13.1c-ops - WS Public Ops v1 (24/7 수집 운영화 + Book Coverage 확보) + Daily Micro Pipeline

- Ticket ID: `T13.1c-ops`
- Root: `D:\MyApps\Autobot`
- SSOT: `autobot/`

## 최우선 목표
1. `T15.1` INCONCLUSIVE 원인(표본 부족 + micro missing + orderbook 부족) 해소
2. trade-only(REST)에서 WS orderbook 포함 micro로 전환
3. `T15.1` 재평가 전 아래 조건 충족
- `MICRO_MISSING_FALLBACK` 비율 `< 10%`
- tier가 `MID only`가 아닌 상태(2개 이상 tier 관측)
- replace/cancel/timeout 이벤트 관측

## 배경/제약
- Upbit WS는 실시간 스트림이며 과거 backfill API가 아니다.
- REST `trades/ticks`는 `days_ago=1..7` 범위에서 조회 가능.
- WS idle timeout(120초) 대응 keepalive 필요.
- orderbook `level`은 기본 `0` 유지.
- Origin 헤더 임의 설정 금지(WS/Quotation rate-limit footgun 회피).

## Scope
### 포함
1. WS public 수집기를 운영(ops)으로 24/7에 가깝게 안정화
- trade + orderbook 동시 수집
- topN 주기 갱신(기본 15분) + subscribe 폭주 방지
- 장애/재연결/keepalive/백오프
- 파일 롤링/manifest/checkpoint/retention purge
- health snapshot 생성

2. Daily pipeline(무료 데이터 only)
- ticks daily 누적(`days_ago=1`)
- micro aggregate(REST trade + WS trade + WS orderbook)
- validate/stats + 일일 리포트 생성

3. T15.1 재평가용 런북 제공
- 최소 6시간 paper run 또는 14~30일 backtest로 표본 확보

### 제외
- 실거래 루프 완성
- T14.x 모델 학습 재개
- WS 과거 backfill

## 구현 항목
### CLI
1. `collect ws-public daemon`
- 입력: `quote`, `top-n`, `refresh-sec`, `duration-sec(옵션)`, `retention-days`, `downsample-hz`, `max-markets`, `format`
- 동작: plan 계산 -> WS 수집 -> 주기 갱신(diff subscribe) -> health snapshot -> purge

2. `collect ws-public status`
- health snapshot + 마지막 run summary 출력

3. `collect ws-public purge --retention-days N`
- 오래된 파티션 삭제 + purge report 기록

### 운영 스크립트
1. `scripts/ws_public_daemon.ps1`
- 재시작 루프
- stdout/stderr 로그 롤링
- 종료코드/마지막 실행시각 기록

2. `scripts/daily_micro_pipeline.ps1`
- ticks daily -> micro aggregate -> micro validate/stats -> `docs/reports/DAILY_MICRO_REPORT_YYYY-MM-DD.md`

### 설정
- `config/micro.yaml`에 `collect.ws_public`, `ticks.daily` 운영 블록 추가

## 실행 런북
### WS daemon
```bat
cd /d D:\MyApps\Autobot
python -m autobot.cli collect ws-public daemon --quote KRW --top-n 50 --refresh-sec 900 --retention-days 30 --duration-sec 21600
```

운영 주의:
- 절전/네트워크 절전 비활성화
- Origin 헤더 임의 설정 금지
- orderbook level은 `0` 유지

### ticks daily
```bat
python -m autobot.cli collect ticks --mode daily --quote KRW --top-n 50 --days-ago 1 --rate-limit-strict true --workers 1
```

### micro aggregate/validate
```bat
python -m autobot.cli micro aggregate --start 2026-03-04 --end 2026-03-04 --quote KRW --top-n 50
python -m autobot.cli micro validate
python -m autobot.cli micro stats
```

## DoD
1. Ops 수집 DoD
- 6시간 이상 안정 실행
- `reconnect_count <= 3`
- `parse_drop == 0`
- health `last_rx_ts_ms` 지속 갱신
- manifest/checkpoint/runs_summary 갱신
- retention purge 동작 확인

2. Book coverage DoD
- 3일 누적 후 `book_available_ratio >= 0.20`(top20 기준 목표)
- `trade_source_ws_ratio > 0.20` 목표

3. T15.1 재평가 준비 DoD
- `MICRO_MISSING_FALLBACK < 10%`
- tier 분포 2개 이상 관측

## 결과 보고서
- `docs/reports/INTEGRATION_REPORT_<YYYY-MM-DD>_T13_1c_ops.md`
- 필수 항목:
- daemon 실행 설정/시간
- trade/orderbook rows, downsample drop, bytes/day
- health 요약(reconnect/last_rx)
- micro aggregate/validate 결과
- `book_available_ratio`, `trade_source_ws_ratio` 변화
- 다음 티켓 1개 제안
