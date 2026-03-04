# RUNBOOK_CONTROL_CENTER

## 1) 개요
`Autobot Control Center v1`는 Windows에서 더블클릭으로 실행하는 메뉴형 실행기입니다.  
기존 CLI/스케줄러/산출물 구조를 바꾸지 않고, 실행/상태확인/결과열람 UX만 추가합니다.

- 엔트리포인트: `scripts/AutobotCenter.cmd`
- 메인 스크립트: `scripts/autobot_center.ps1`
- 실행 로그: `logs/ops_center/*.log`

안전 원칙:
- 기본 제공 기능은 `backtest / paper / collect / validate / model` 중심입니다.
- 라이브 실거래 원클릭 기능은 포함하지 않습니다.
- 파일 이동/삭제 성격의 동작(예: quarantine)은 반드시 확인 프롬프트를 거칩니다.

## 2) 실행 방법 (더블클릭)
1. 탐색기에서 `D:\MyApps\Autobot\scripts\AutobotCenter.cmd` 더블클릭
2. 메뉴에서 `1~9` 번호 입력
3. 작업 종료 후 안내에 따라 Enter 입력 시 메뉴로 복귀

참고:
- `AutobotCenter.cmd`는 현재 위치와 무관하게 `scripts` 위치를 기준으로 프로젝트 루트를 자동 계산합니다.
- 내부적으로 아래 형태로 PowerShell을 호출합니다.

```bat
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\autobot_center.ps1
```

### 2.1 자동 실행(검증/운영 스크립트용, 선택)
대화형 입력 없이 특정 메뉴를 1회 실행할 수 있습니다.

예시:
```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/autobot_center.ps1 -RunSelection 4 -UseDefaults
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/autobot_center.ps1 -RunSelection 5 -UseDefaults -PaperDurationSecOverride 20
```

## 3) 메뉴 기능과 실제 호출 커맨드
모든 메뉴 실행 전 공통으로:
- 현재 ROOT 경로 출력
- `python -m autobot.cli --help` preflight 체크

### 1) Status Dashboard
- 최신 `DAILY_MICRO_REPORT_*.md` 경로/요약 표시
- `data/raw_ws/upbit/_meta/ws_public_health.json` 기반 WS 상태(connected/lag/subscribed) 표시
- 스케줄러 태스크 상태 표시
  - `Autobot_WS_Public_Daemon`
  - `Autobot_Daily_Micro_Pipeline`

### 2) Run Daily Pipeline Now
호출:
```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/daily_micro_pipeline.ps1
```
완료 후:
- 최신 DAILY 리포트 파일을 자동으로 엽니다(가능 시).

### 3) Start/Stop WS Public Daemon Task
호출:
```powershell
schtasks.exe /Run /TN Autobot_WS_Public_Daemon
schtasks.exe /End /TN Autobot_WS_Public_Daemon
```

### 4) Backtest Wizard
입력(기본값):
- tf=`5m`, quote=`KRW`, top_n=`20`, days=`8`

호출:
```powershell
python -m autobot.cli backtest run --tf <tf> --quote <quote> --top-n <top_n> --duration-days <days>
```
완료 후:
- `data/backtest/runs/<run_id>` 자동 열기

### 5) Paper Wizard
입력(기본값):
- duration_sec=`600` (또는 3600 권장), quote=`KRW`, top_n=`20`

호출:
```powershell
python -m autobot.cli paper run --duration-sec <duration_sec> --quote <quote> --top-n <top_n>
```
완료 후:
- `data/paper/runs/<run_id>` 자동 열기

### 6) Model Train Wizard
입력:
- trainer: `v1` 또는 `v2_micro`
- 기간: 최근 N일(최소 8일, 기본 30일)
- tf/quote/top_n

호출:
```powershell
python -m autobot.cli model train --trainer <trainer> --model-family <family> --tf <tf> --quote <quote> --top-n <top_n> --start <YYYY-MM-DD> --end <YYYY-MM-DD>
python -m autobot.cli model eval --model-ref champion --model-family <family> --split test
```
완료 후:
- `models/registry/<family>` 자동 열기
- 학습 run의 `metrics.json` 자동 열기(존재 시)

### 7) Validate/Doctor
실행 항목:
```powershell
python -m autobot.cli collect ws-public validate --raw-root data/raw_ws/upbit/public --meta-dir data/raw_ws/upbit/_meta --quarantine-corrupt <true|false> --min-age-sec 300
python -m autobot.cli collect ws-public stats --raw-root data/raw_ws/upbit/public --meta-dir data/raw_ws/upbit/_meta
python -m autobot.cli micro validate --out-root data/parquet/micro_v1
python -m autobot.cli micro stats --out-root data/parquet/micro_v1
python -m autobot.cli data validate --parquet-dir data/parquet/candles_api_v1
```
실패 시:
- 콘솔에 report 경로 힌트 출력
- 상세 로그는 `logs/ops_center/*.log`에서 확인

### 8) Open Outputs
빠른 열기 대상:
- `data/paper/runs`
- `data/backtest/runs`
- `docs/reports`
- `logs`

### 9) Exit
- 메뉴 종료

## 4) 산출물/로그 경로
- 운영 로그: `logs/ops_center/*.log`
- 백테스트 결과: `data/backtest/runs/*`
- 페이퍼 결과: `data/paper/runs/*`
- 모델 레지스트리: `models/registry/*`
- 데일리 리포트: `docs/reports/DAILY_MICRO_REPORT_*.md`

## 5) 자주 발생하는 에러와 대응
### (1) `python` 명령 실패 / 모듈 인식 실패
증상:
- preflight에서 `python -m autobot.cli --help` 실패

대응:
1. 프로젝트 루트가 `D:\MyApps\Autobot`인지 확인
2. 사용 중인 Python에서 패키지 import 가능한지 확인
   - `python -m autobot.cli --help`
3. 필요 시 기존 파이프라인 방식처럼 `PYTHONPATH`에 `python/site-packages` 포함

### (2) Task Scheduler 관련 오류
증상:
- task not found / access denied

대응:
1. 태스크 등록 여부 확인
   - `scripts/register_scheduled_tasks.ps1`
2. 권한 이슈 시 관리자 권한 PowerShell에서 재실행

### (3) 실행 정책(ExecutionPolicy) 오류
증상:
- `.ps1` 실행이 차단됨

대응:
- 본 실행기는 `-ExecutionPolicy Bypass`로 호출되므로, 직접 실행 시에도 같은 옵션 사용

### (4) 결과 폴더 자동 열기 실패
증상:
- run 완료 후 폴더가 열리지 않음

대응:
1. 해당 경로가 실제 생성되었는지 확인
2. 상세 원인은 `logs/ops_center/*.log` 확인

## 6) 운영 팁
- 메뉴 기반 실행은 UX 레이어이며, 기존 CLI 스크립트는 그대로 유지됩니다.
- 문제 재현/분석 시 `logs/ops_center`의 해당 시각 로그 파일부터 확인하는 것이 가장 빠릅니다.

## Update 2026-03-05 (T18 compatibility)
- Model Train Wizard defaults to `v3_mtf_micro`.
- Supported trainers in Control Center: `v1`, `v2_micro`, `v3_mtf_micro`.
- Trainer to feature/model mapping:
  - `v1` -> `--feature-set v1 --model-family train_v1`
  - `v2_micro` -> `--feature-set v2 --model-family train_v2_micro`
  - `v3_mtf_micro` -> `--feature-set v3 --model-family train_v3_mtf_micro`
- New menu shortcut `M`: ModelBT Proxy Wizard (defaults: `latest_v3`, `train_v3_mtf_micro`).
- Added `Run-Command` native stderr warning tolerance so sklearn warnings do not cause false failure.
