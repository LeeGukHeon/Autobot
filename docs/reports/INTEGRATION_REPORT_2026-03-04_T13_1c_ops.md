# INTEGRATION_REPORT_2026-03-04_T13_1c_ops

## 0) Summary
- Ticket: `T13.1c-ops`
- Scope: WS public ops v1 (daemon/status/purge) + daily micro pipeline scripts + config wiring
- Result: `IMPLEMENTED` (code/scripts/config/doc completed)
- Runtime status: `NOT EXECUTED` (24/7 수집/실데이터 KPI는 운영 실행 후 갱신 필요)

## 1) Implemented Items
### 1.1 CLI (`collect ws-public`)
- Added `daemon`
- Added `status`
- Added `purge --retention-days N`
- Extended `run` with keepalive controls:
  - `--keepalive-mode`
  - `--keepalive-interval-sec`
  - `--keepalive-stale-sec`

### 1.2 Collector Ops Layer
- Added daemon path with:
  - KRW quote topN REST market refresh
  - periodic subscribe diff apply
  - keepalive mode/message-frame-auto-off
  - reconnect/backoff guard
  - health snapshot write (`ws_public_health.json`)
  - retention purge report (`retention_report.json`, `ws_purge_report.json`)
- Added status loader (`load_ws_public_status`)
- Added purge API (`purge_ws_public_retention`)

### 1.3 Scripts
- Added `scripts/ws_public_daemon.ps1`
  - restart loop
  - per-run stdout/stderr rolling logs
  - last run status JSON
- Added `scripts/daily_micro_pipeline.ps1`
  - ticks daily
  - micro aggregate (target date)
  - micro validate/stats
  - report output: `docs/reports/DAILY_MICRO_REPORT_YYYY-MM-DD.md`

### 1.4 Config
- Updated `config/micro.yaml`
  - `micro.raw_ws_root` -> `data/raw_ws/upbit/public`
  - added `collect.ws_public` ops block
  - added `ticks.daily` block

### 1.5 Ticket Doc
- Added `docs/TICKETS/T13_1c_ops_ws_public_ops_v1.md`

## 2) Key Output Paths
- health snapshot: `data/raw_ws/upbit/_meta/ws_public_health.json`
- collect report: `data/raw_ws/upbit/_meta/ws_collect_report.json`
- runs summary: `data/raw_ws/upbit/_meta/ws_runs_summary.json`
- retention/purge report:
  - `data/raw_ws/upbit/_meta/retention_report.json`
  - `data/raw_ws/upbit/_meta/ws_purge_report.json`

## 3) Commands
### WS daemon
```bat
python -m autobot.cli collect ws-public daemon --quote KRW --top-n 50 --refresh-sec 900 --retention-days 30 --downsample-hz 1 --max-markets 60 --duration-sec 21600
```

### Status / Purge
```bat
python -m autobot.cli collect ws-public status
python -m autobot.cli collect ws-public purge --retention-days 30
```

### Daily pipeline
```powershell
powershell -ExecutionPolicy Bypass -File scripts/daily_micro_pipeline.ps1 -TopN 50
```

## 4) Tests
- Added: `tests/test_ws_public_ops.py`
- Suggested run:
  - `python -m pytest -q tests/test_ws_public_ops.py tests/test_ws_public_collector_utils.py tests/test_ws_public_collect_validate.py`

## 5) KPI Tracking (to be filled after daemon run)
- book_available_ratio: `TBD`
- trade_source_ws_ratio: `TBD`
- reconnect_count(6h): `TBD`
- parse_drop: `TBD`
- MICRO_MISSING_FALLBACK ratio (T15.1 rerun): `TBD`
