# INTEGRATION_REPORT_2026-03-04_T16

## 0) Summary
- Ticket: `T16`
- Scope: `Autobot Control Center v1 (Windows Menu Launcher + One-Click Runs)`
- Root: `D:\MyApps\Autobot`
- Result: `PASS`
- Notes:
  - 메뉴형 실행기(`scripts/autobot_center.ps1`) + 더블클릭 엔트리(`scripts/AutobotCenter.cmd`) 추가
  - 요구 DoD 최소 항목 `1,2,4,5,8` 실제 실행 검증 완료
  - 로그는 `logs/ops_center/*.log`에 저장됨

## 1) Added Files
- `scripts/AutobotCenter.cmd`
- `scripts/autobot_center.ps1`
- `docs/RUNBOOK_CONTROL_CENTER.md`
- `docs/reports/INTEGRATION_REPORT_2026-03-04_T16.md`

## 2) Executed Menu Items (DoD)
아래는 Control Center의 단일 실행 모드(`-RunSelection`)로 검증한 결과입니다.

### 2.1 Menu 1) Status Dashboard
- launcher command:
  - `powershell -NoProfile -ExecutionPolicy Bypass -File scripts/autobot_center.ps1 -ProjectRoot D:\MyApps\Autobot -RunSelection 1 -UseDefaults`
- exit code: `0`
- invoked command (inside menu):
  - `python -m autobot.cli --help` (preflight)
- log:
  - `logs/ops_center/20260304-232905_preflight_menu1_status.log`
- output highlights:
  - latest daily report: `docs/reports/DAILY_MICRO_REPORT_2026-03-03.md`
  - ws health snapshot: `connected=True`, `subscribed=50`
  - scheduler tasks: `Autobot_WS_Public_Daemon`, `Autobot_Daily_Micro_Pipeline` not found in this environment

### 2.2 Menu 2) Run Daily Pipeline Now
- launcher command:
  - `powershell -NoProfile -ExecutionPolicy Bypass -File scripts/autobot_center.ps1 -ProjectRoot D:\MyApps\Autobot -RunSelection 2 -UseDefaults`
- exit code: `0`
- invoked commands (inside menu):
  - `python -m autobot.cli --help` (preflight)
  - `powershell.exe -NoProfile -ExecutionPolicy Bypass -File D:\MyApps\Autobot\scripts\daily_micro_pipeline.ps1`
- logs:
  - `logs/ops_center/20260304-233107_preflight_menu2_daily.log`
  - `logs/ops_center/20260304-233110_menu2_daily_pipeline.log`
- generated/updated artifacts:
  - `docs/reports/DAILY_MICRO_REPORT_2026-03-03.md` (updated)
  - `data/collect/_meta/candle_validate_report.json`
  - `data/raw_ticks/upbit/_meta/ticks_validate_report.json`
  - `data/parquet/micro_v1/_meta/validate_report.json`

### 2.3 Menu 4) Backtest Wizard
- launcher command:
  - `powershell -NoProfile -ExecutionPolicy Bypass -File scripts/autobot_center.ps1 -ProjectRoot D:\MyApps\Autobot -RunSelection 4 -UseDefaults`
- exit code: `0`
- invoked commands (inside menu):
  - `python -m autobot.cli --help` (preflight)
  - `python -m autobot.cli backtest run --tf 5m --quote KRW --top-n 20 --duration-days 8`
- logs:
  - `logs/ops_center/20260304-232924_preflight_menu4_backtest.log`
  - `logs/ops_center/20260304-232927_menu4_backtest_wizard.log`
- generated artifacts:
  - `data/backtest/runs/backtest-20260304-232947-4c4c153dda`
  - `data/backtest/runs/backtest-20260304-232947-4c4c153dda/summary.json`

### 2.4 Menu 5) Paper Wizard
- launcher command:
  - `powershell -NoProfile -ExecutionPolicy Bypass -File scripts/autobot_center.ps1 -ProjectRoot D:\MyApps\Autobot -RunSelection 5 -UseDefaults -PaperDurationSecOverride 20`
- exit code: `0`
- invoked commands (inside menu):
  - `python -m autobot.cli --help` (preflight)
  - `python -m autobot.cli paper run --duration-sec 20 --quote KRW --top-n 20`
- logs:
  - `logs/ops_center/20260304-233034_preflight_menu5_paper.log`
  - `logs/ops_center/20260304-233037_menu5_paper_wizard.log`
- generated artifacts:
  - `data/paper/runs/paper-20260304-233039`
  - `data/paper/runs/paper-20260304-233039/summary.json`

### 2.5 Menu 8) Open Outputs
- launcher command:
  - `powershell -NoProfile -ExecutionPolicy Bypass -File scripts/autobot_center.ps1 -ProjectRoot D:\MyApps\Autobot -RunSelection 8 -UseDefaults`
- exit code: `0`
- invoked command (inside menu):
  - `python -m autobot.cli --help` (preflight)
- log:
  - `logs/ops_center/20260304-232914_preflight_menu8_outputs.log`
- opened outputs:
  - `data/paper/runs`
  - `data/backtest/runs`
  - `docs/reports`
  - `logs`

## 3) Failure / Error Notes
- Menu execution failures: `None`
- Non-blocking environment note:
  - Scheduled tasks not registered in this machine context, so Dashboard에서 task 상태는 `not found`로 표시됨.
  - 기능적으로는 예외 처리되어 Control Center 실행은 정상 종료됨.

## 4) Safety Checklist
- live trade one-click 메뉴: `미포함`
- quarantine 동작: `Y/N` 이중 확인 없이는 실행되지 않도록 구현
- purge/delete 계열 자동 실행: `미포함`

## 5) Legacy Change Policy
- 기존 CLI/파이프라인 스크립트 삭제 없음
- 기존 실행 경로를 감싸는 UX 레이어(`cmd + menu ps1`)만 추가
