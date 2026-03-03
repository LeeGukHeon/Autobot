# INTEGRATION_REPORT_2026-03-04_T13_1c

## 0) Summary
- Ticket: `T13.1c`
- Scope: Upbit Public WS `trade + orderbook` collector v1 (plan/run/validate/stats + raw rotation + manifest/meta)
- Result:
  - `collect plan-ws-public` 구현 및 실행 완료
  - 120초 smoke + 30분 soak 실수집 완료
  - validate PASS (`checked=16, fail=0, parse_ok_ratio=1.0`)
  - reconnect 폭주/레이트리밋 위반 없음 (`reconnect_count=0`)

## 1) Implemented
- CLI:
  - `collect plan-ws-public`
  - `collect ws-public run`
  - `collect ws-public validate`
  - `collect ws-public stats`
- Collector stack:
  - plan: `autobot/data/collect/plan_ws_public.py`
  - runtime collector: `autobot/data/collect/ws_public_collector.py`
  - rotating raw writer: `autobot/data/collect/ws_public_writer.py`
  - manifest/checkpoint: `autobot/data/collect/ws_public_manifest.py`, `autobot/data/collect/ws_public_checkpoint.py`
  - validate/stats: `autobot/data/collect/validate_ws_public.py`, `autobot/data/collect/ws_public_stats.py`
- exports:
  - `autobot/data/collect/__init__.py`
- tests:
  - `tests/test_ws_public_plan.py`
  - `tests/test_ws_public_collector_utils.py`
  - `tests/test_ws_public_writer.py`
  - `tests/test_ws_public_collect_validate.py`

## 2) Executed Commands
- Plan:
  - `python -m autobot.cli collect plan-ws-public --base-dataset candles_v1 --quote KRW --market-mode top_n_by_recent_value_est --top-n 20 --channels trade,orderbook --format DEFAULT --orderbook-topk 5 --orderbook-level 0 --orderbook-min-write-interval-ms 200 --out data/raw_ws/upbit/_meta/ws_public_plan.json`
- Smoke run (120s):
  - `python -m autobot.cli collect ws-public run --plan data/raw_ws/upbit/_meta/ws_public_plan.json --raw-root data/raw_ws/upbit/quotation --meta-dir data/raw_ws/upbit/_meta --duration-sec 120 --rate-limit-strict true`
- Soak run (1800s):
  - `python -m autobot.cli collect ws-public run --plan data/raw_ws/upbit/_meta/ws_public_plan.json --raw-root data/raw_ws/upbit/quotation --meta-dir data/raw_ws/upbit/_meta --duration-sec 1800 --rate-limit-strict true`
- Validate/Stats:
  - `python -m autobot.cli collect ws-public validate --raw-root data/raw_ws/upbit/quotation --meta-dir data/raw_ws/upbit/_meta`
  - `python -m autobot.cli collect ws-public stats --raw-root data/raw_ws/upbit/quotation --meta-dir data/raw_ws/upbit/_meta`
- Test:
  - `python -m pytest -q`

## 3) Run Results
### 3.1 Smoke (Run ID: `20260303T153218Z`, 120s)
- codes: `20`, channels: `trade+orderbook`
- received:
  - trade: `1,129`
  - orderbook: `8,876`
- written:
  - trade: `1,129`
  - orderbook: `6,118`
- dropped:
  - orderbook interval downsample: `2,758`
  - parse error: `0`
- reconnect/keepalive:
  - reconnect: `0`
  - ping sent: `0`
  - pong rx: `0`

### 3.2 Soak (Run ID: `20260303T153439Z`, 1800s)
- received:
  - trade: `18,082`
  - orderbook: `125,601`
- written:
  - trade: `18,082`
  - orderbook: `85,722`
- dropped:
  - orderbook interval downsample: `39,879`
  - parse error: `0`
- reconnect/keepalive:
  - reconnect: `0`
  - ping sent: `0`
  - pong rx: `0`
- files/bytes:
  - files_written (run): `14`
  - bytes_written (run): `4,589,607`

## 4) Validation / Stats
- Validate (latest):
  - checked/ok/warn/fail: `16 / 16 / 0 / 0`
  - parse_ok_ratio: `1.0`
  - zero_rows_markets_warn: `0`
- Downsampling applied ratio:
  - `0.31750543` (from latest collect_report)
- Time coverage (latest accumulated dataset):
  - trade: 20 markets, global min/max `1772551475568 ~ 1772553880808`
  - orderbook: 20 markets, global min/max `1772551930707 ~ 1772553880879`
- Manifest (all runs accumulated):
  - parts: `16`
  - rows_total: `111,051`
  - bytes_total: `4,911,184`
  - by_channel:
    - trade: `19,211`
    - orderbook: `91,840`

## 5) Artifacts
- Plan:
  - `data/raw_ws/upbit/_meta/ws_public_plan.json`
- Reports:
  - `data/raw_ws/upbit/_meta/ws_collect_report.json` (latest run)
  - `data/raw_ws/upbit/_meta/ws_validate_report.json`
  - `data/raw_ws/upbit/_meta/ws_runs_summary.json`
  - `data/raw_ws/upbit/_meta/retention_report.json`
- Meta:
  - `data/raw_ws/upbit/_meta/ws_manifest.parquet`
  - `data/raw_ws/upbit/_meta/ws_checkpoint.json`
- Raw:
  - `data/raw_ws/upbit/quotation/trade/date=*/hour=*/part-*.jsonl.zst`
  - `data/raw_ws/upbit/quotation/orderbook/date=*/hour=*/part-*.jsonl.zst`

## 6) Design Coverage (T13.1c)
- WS plan 생성(top_n, channels, safety): `DONE`
- 단일 연결 + 단일 subscribe 메시지(복합 type): `DONE`
- Origin 헤더 미사용: `DONE`
- keepalive + reconnect(backoff+jitter, max_reconnect_per_min): `DONE`
- rate-limit-safe 메시지 전송(5/s, 100/min): `DONE`
- trade/orderbook 정규화 저장: `DONE`
- orderbook topk=5 + interval downsample + price/spread/size-change 예외저장: `DONE`
- jsonl.zst 회전 저장(시간 파티션, rotate_sec/max_bytes): `DONE`
- ws_manifest/ws_collect_report/ws_validate_report/ws_runs_summary/ws_checkpoint: `DONE`
- validate/stats CLI: `DONE`
- 오프라인 단위/통합 테스트: `DONE`

## 7) DoD Check
- 120초 smoke에서 trade/orderbook 수신 + 파일 생성: `DONE`
- validate PASS: `DONE`
- rate-limit 위반/폭주 reconnect 없음: `DONE`
- 30분 soak 권장 실행: `DONE`
- pytest PASS: `DONE` (`120 passed`)
- integration report 작성: `DONE`

## 8) Next (T13.1d 연계)
- T13.1d 집계 시 `(market, date)` 단위 tick/micro time coverage(`min_ts~max_ts`)를 저장하고,
  coverage 미포함 구간 bar는 `micro_available=0`으로 강제하는 DoD를 유지해야 함.
