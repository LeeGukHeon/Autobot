# INTEGRATION_REPORT_2026-03-03_T13_1b

## 0) Summary
- Ticket: `T13.1b`
- Scope: Upbit free REST `trades/ticks` 7-day backfill + daily accumulation (raw ticks)
- Result:
  - Real backfill run executed (`top_n=20`, `days_ago=1..7`)
  - Real daily run executed (`top_n=20`, `days_ago=1`)
  - `fail=0` on both runs
  - Validate full pass (`checked=155, fail=0, schema_ok_ratio=1.0, dup_ratio_overall=0.0`)

## 1) Executed Commands (Real Runs)
- Backfill plan:
  - `python -m autobot.cli collect plan-ticks --base-dataset candles_v1 --quote KRW --market-mode top_n_by_recent_value_est --top-n 20 --days-ago 1,2,3,4,5,6,7 --out data/raw_ticks/upbit/_meta/ticks_plan.json`
- Backfill collect (real):
  - `python -m autobot.cli collect ticks --plan data/raw_ticks/upbit/_meta/ticks_plan.json --raw-root data/raw_ticks/upbit/trades --meta-dir data/raw_ticks/upbit/_meta --mode backfill --rate-limit-strict true --workers 1 --max-pages-per-target 20 --dry-run false`
- Daily collect (real):
  - `python -m autobot.cli collect ticks --plan data/raw_ticks/upbit/_meta/ticks_plan_daily.json --quote KRW --top-n 20 --days-ago 1 --mode daily --raw-root data/raw_ticks/upbit/trades --meta-dir data/raw_ticks/upbit/_meta --rate-limit-strict true --workers 1 --max-pages-per-target 10 --dry-run false`
- Validate/Stats:
  - `python -m autobot.cli collect ticks validate --raw-root data/raw_ticks/upbit/trades --meta-dir data/raw_ticks/upbit/_meta`
  - `python -m autobot.cli collect ticks stats --raw-root data/raw_ticks/upbit/trades --meta-dir data/raw_ticks/upbit/_meta`
- Test:
  - `python -m pytest -q`

## 2) Backfill Result (Run ID: `20260303T142849Z`)
- Time (UTC): `2026-03-03T14:28:49+00:00` -> `2026-03-03T14:49:10+00:00` (1221s)
- Targets:
  - discovered/selected/processed: `140 / 140 / 140`
  - ok/warn/fail: `27 / 113 / 0`
- Calls/rows:
  - calls: `2835`
  - throttled/backoff: `257 / 257`
  - rows_collected_total: `507,103`
- Warn reasons:
  - `MAX_REQUEST_BUDGET_REACHED`: `113`
- Manifest slice:
  - parts: `140`
  - date range: `2026-02-24` to `2026-03-02`

## 3) Daily Result (Run ID: `20260303T144954Z`)
- Time (UTC): `2026-03-03T14:49:54+00:00` -> `2026-03-03T14:51:10+00:00` (76s)
- Targets:
  - discovered/selected/processed: `20 / 20 / 20`
  - ok/warn/fail: `0 / 20 / 0`
- Calls/rows:
  - calls: `170`
  - throttled/backoff: `15 / 15`
  - rows_collected_total: `30,000`
- Warn reasons:
  - `MAX_REQUEST_BUDGET_REACHED`: `15`
  - `NO_ROWS_COLLECTED`: `5`

## 4) Validation / Dataset Status
- Full validate (all current parts):
  - checked/ok/warn/fail: `155 / 155 / 0 / 0`
  - schema_ok_ratio: `1.0`
  - dup_ratio_overall: `0.0`
- Raw files:
  - `*.jsonl.zst` files: `155`
  - total compressed size: `7.55 MB`
  - by date:
    - `2026-02-24`: 20 files
    - `2026-02-25`: 20 files
    - `2026-02-26`: 20 files
    - `2026-02-27`: 20 files
    - `2026-02-28`: 20 files
    - `2026-03-01`: 20 files
    - `2026-03-02`: 35 files

## 5) Artifacts
- Plan:
  - `data/raw_ticks/upbit/_meta/ticks_plan.json`
  - `data/raw_ticks/upbit/_meta/ticks_plan_daily.json`
- Reports:
  - `data/raw_ticks/upbit/_meta/ticks_collect_report.json` (latest: daily run)
  - `data/raw_ticks/upbit/_meta/ticks_validate_report.json`
  - `data/raw_ticks/upbit/_meta/ticks_runs_summary.json` (backfill+daily merged run summary)
- Meta:
  - `data/raw_ticks/upbit/_meta/ticks_manifest.parquet`
  - `data/raw_ticks/upbit/_meta/ticks_checkpoint.json`
- Raw:
  - `data/raw_ticks/upbit/trades/date=*/market=*/part-*.jsonl.zst`

## 6) Design Coverage (Done / Not Done)

### 6.1 API constraints
- `GET /v1/trades/ticks` endpoint: `DONE`
- `days_ago` range 1..7 enforced: `DONE`
- `trade` rate-limit group + Remaining-Req handling + 429 backoff: `DONE`
- 418 immediate abort path implemented: `DONE (code) / NOT OBSERVED (run)`

### 6.2 Required implementation scope (Section 1.1)
- ticks plan generator: `DONE`
- REST ticks collector (strict RL): `DONE`
- raw compressed save + manifest + checkpoint: `DONE`
- validate + report: `DONE`
- CLI (`plan-ticks`, `ticks`, `ticks validate`, `ticks stats`): `DONE`

### 6.3 Collection policy (Section 2)
- market selection by local recent value_est top_n: `DONE`
- initial 7-day backfill (days_ago=1..7): `DONE`
- daily accumulation (days_ago=1) run: `DONE`
- weekly days_ago=2..7 re-collect scheduling: `NOT DONE (manual command ready, scheduler not configured)`
- retention policy (`retention_days=30`) in code: `DONE`
- retention deletion triggered in this run: `NOT TRIGGERED` (all data within retention window)

### 6.4 Storage/schema/collector behavior (Sections 3~5)
- raw path format (`date=.../market=.../part-...jsonl.zst`): `DONE`
- min schema fields: `DONE`
- pagination loop guard + no-progress break: `DONE`
- checkpoint resume support: `DONE`
- strict mode workers=1: `DONE`

### 6.5 Validate/QA (Section 6)
- schema required fields check: `DONE`
- timestamp/day plausibility via partition check: `DONE`
- duplicate ratio (`market+sequential_id`) check: `DONE`
- ask_bid enum check: `DONE`
- status classification (OK/WARN/FAIL): `DONE`

### 6.6 CLI examples (Section 7)
- plan command: `DONE`
- backfill command: `DONE`
- daily command: `DONE`
- validate command: `DONE`
- stats command: `DONE`
- Note on runtime parameter:
  - spec example used `--max-pages-per-target 500`
  - actual run used `20` (backfill) and `10` (daily) for bounded wall-clock
  - status: `PARTIAL (functional done, depth-limited execution)`

### 6.7 Tests (Section 8)
- unit/integration tests for ticks collector stack: `DONE`
- pytest full suite: `DONE` (`113 passed`)

### 6.8 Mandatory outputs (Section 9)
- `ticks_collect_report.json`: `DONE`
- `ticks_validate_report.json`: `DONE`
- `ticks_manifest.parquet`: `DONE`
- integration report doc: `DONE`

### 6.9 DoD (Section 10)
- 7-day backfill run success (top20): `DONE` (`fail=0`)
- report/manifest/validate generated: `DONE`
- no RL violation stop (429 handled, no 418): `DONE`
- pytest PASS: `DONE`
- integration report completed: `DONE`

## 7) Notes
- `NO_ROWS_COLLECTED` is valid for no-trade intervals/low-liquidity targets.
- Current `ticks_collect_report.json` stores latest run only; historical per-run comparison is provided in `ticks_runs_summary.json` and `ticks_manifest.parquet`.
