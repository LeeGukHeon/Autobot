# 00:20 Nightly Cycle Trace, Step 1

## 0. Purpose

이 문서는 `00:20` 야간 사이클의 **진짜 입구**부터 실제 코드 기준으로 다시 추적한 위키다.

이번 문서의 목표는 두 가지다.

1. `00:20` 전체 체인의 실제 entrypoint가 무엇인지 명확히 고정한다.
2. 전체 순서를 먼저 잡되, **1번 단계만** 함수/인자/변수/분기/산출물 단위로 아주 세세하게 기록한다.

중요:

- 이 문서는 `2026-04-07` 시점의 코드 기준이다.
- 이 문서는 **Step 1만 깊게 판독**한 문서다.
- Step 2 이후는 이번 파일에서는 **예고 수준**으로만 적는다.
- 아직 안 읽은 구간은 “안 읽었다”고 분명히 적는다.


## 1. True Entrypoint

`00:20` 전체 체인의 실질 entrypoint는 아래 timer/service 조합이다.

- timer installer:
  - `scripts/install_server_daily_v5_split_challenger_services.ps1`
- 실제 timer/service 파일 생성기:
  - `scripts/install_server_daily_split_challenger_services.ps1`
- 실제 wrapper:
  - `scripts/daily_champion_challenger_v5_for_server.ps1`

확인된 사실:

- `autobot-v5-challenger-spawn.timer`
  - `OnCalendar=*-*-* 00:20:00`
- `autobot-v5-challenger-spawn.service`
  - wrapper script `daily_champion_challenger_v5_for_server.ps1`
  - `-Mode spawn_only`
  - shared lock file:
    - `/tmp/autobot-v5-nightly-train-chain.lock`

### 1.1 Observed server unit values

아래 값은 `2026-04-07`에 실제 서버에서 `systemctl cat/show`로 직접 확인한 관측값이다.

observed unit metadata:

- service:
  - `autobot-v5-challenger-spawn.service`
- timer:
  - `autobot-v5-challenger-spawn.timer`
- unit description:
  - `Autobot V4 Challenger Spawn`
- service user:
  - `ubuntu`
- working directory:
  - `/home/ubuntu/MyApps/Autobot`
- environment:
  - `PYTHONUNBUFFERED=1`
- timer calendar:
  - `OnCalendar=*-*-* 00:20:00`
- timer persistence:
  - `Persistent=true`
- host timezone:
  - `Asia/Seoul (KST, +0900)`

즉 현재 실서버 기준 `00:20`은
**Asia/Seoul wall-clock 00:20**으로 읽어도 된다.

observed concrete exec path:

- outer shell:
  - `/bin/bash -lc ...`
- lock wrapper:
  - `flock -n /tmp/autobot-v5-nightly-train-chain.lock`
- powershell binary:
  - `/snap/powershell/342/opt/powershell/pwsh`
- script:
  - `/home/ubuntu/MyApps/Autobot/scripts/daily_champion_challenger_v5_for_server.ps1`

observed arguments explicitly injected by the service:

- `-ProjectRoot /home/ubuntu/MyApps/Autobot`
- `-PythonExe /home/ubuntu/MyApps/Autobot/.venv/bin/python`
- `-Mode spawn_only`
- `-ModelFamily train_v5_fusion`
- `-ChampionUnitName autobot-paper-v5.service`
- `-ChallengerUnitName ""`
- `-ChampionCompareModelFamily train_v5_fusion`
- `-CandidateAdoptionScript /home/ubuntu/MyApps/Autobot/scripts/adopt_v5_candidate_for_server.ps1`
- `-PairedPaperModelFamily train_v5_fusion`
- `-PromotionTargetUnits autobot-live-alpha.service`
- `-CandidateTargetUnits autobot-live-alpha-canary.service`

즉 서버 `00:20` 시작점에는 이미 아래 값들이 default가 아니라 **service가 직접 주입하는 값**으로 고정되어 있다.

- project root
- python exe
- mode
- model family
- champion compare family
- adoption script
- promotion/candidate target units

반대로 아래 값들은 service가 직접 넘기지 않고 wrapper 내부 default로 채워진다.

- `BatchDate`
- `CandlesRefreshScript`
- `RawTicksDailyScript`
- `TrainSnapshotCloseScript`
- `PairedPaperPreset`
- `PairedPaperFeatureProvider`
- `PairedPaperMicroProvider`
- `PairedPaperWarmupSec`
- `PairedPaperWarmupMinTradeEventsPerMarket`
- `ExecutionContractMinRows`

entrypoint를 만드는 실제 installer 동작도 중요하다.

- `scripts/install_server_daily_v5_split_challenger_services.ps1`
  - `scripts/install_server_daily_split_challenger_services.ps1`를 호출한다.
- `scripts/install_server_daily_split_challenger_services.ps1`
  - `Build-ExecStart`를 통해 wrapper command를 만든다.
  - 이 command는 `Build-FlockWrappedExecStart`를 통해 shared lock으로 감싸진다.
  - legacy timer/service disable 목록도 같이 적용한다.

즉 `00:20` 입구는 단순 timer 이름만의 문제가 아니라:

- spawn timer
- spawn service
- flock-wrapped wrapper exec

이 셋이 같이 입구 contract를 이룬다.

즉 `00:20` 체인은 “acceptance script”가 직접 입구가 아니다.

진짜 입구는:

1. `autobot-v5-challenger-spawn.timer`
2. `autobot-v5-challenger-spawn.service`
3. `scripts/daily_champion_challenger_v5_for_server.ps1 -Mode spawn_only`

### 1.2 Exact installer/default chain for Step 1

위 세 줄만으로는 부족하다.
`Step 1`에서 중요한 것은 **어느 값이 어디서 결정되어 wrapper에 꽂히는가**다.

#### stage A. v5 installer default

`scripts/install_server_daily_v5_split_challenger_services.ps1`는 아래 기본값을 먼저 고정한다.

- `ModelFamily = train_v5_fusion`
- `ChampionUnitName = autobot-paper-v5.service`
- `PromotionTargetUnits = autobot-live-alpha.service`
- `CandidateTargetUnits = autobot-live-alpha-canary.service`
- `PromoteOnCalendar = *-*-* 00:10:00`
- `SpawnOnCalendar = *-*-* 00:20:00`
- `LockFile = /tmp/autobot-v5-nightly-train-chain.lock`
- `WrapperScript` default:
  - `scripts/daily_champion_challenger_v5_for_server.ps1`
- `CandidateAdoptionScript` default:
  - `scripts/adopt_v5_candidate_for_server.ps1`

그리고 이 값을 generic installer인
`scripts/install_server_daily_split_challenger_services.ps1`로 그대로 넘긴다.

#### stage B. generic installer arg injection

generic installer의 `Build-ExecStart`는 service `ExecStart`를 조립할 때 아래 인자를 **항상** 넣는다.

- `-ProjectRoot`
- `-PythonExe`
- `-Mode`
- `-ModelFamily`
- `-ChampionUnitName`
- `-ChallengerUnitName`

아래 인자는 조건부로 붙는다.

- `-ChampionCompareModelFamily`
- `-AcceptanceScript`
- `-RuntimeInstallScript`
- `-CandidateAdoptionScript`
- `-PairedPaperModelFamily`
- `-PromotionTargetUnits`
- `-CandidateTargetUnits`
  - 단, 이 값은 `ModeName == "spawn_only"`일 때만 service command에 들어간다

즉 `00:20` spawn path는 promote path보다 인자가 하나 더 많다.

- spawn path:
  - candidate target units가 들어간다
- promote path:
  - candidate target units가 service command에 직접 들어가지 않는다

#### stage C. flock wrapper semantics

generic installer는 조립한 PowerShell command를 곧바로 service에 넣지 않고
`Build-FlockWrappedExecStart`로 한 번 더 감싼다.

이 wrapper의 실제 의미:

- 최종 실행은 `/bin/bash -lc ...`
- lock file이 비어 있지 않으면 `flock -n <lock>` 사용
- lock을 이미 다른 실행이 잡고 있으면:
  - busy message를 echo하고
  - **exit 0**으로 끝낸다

이건 매우 중요하다.

- `00:20` timer overlap은 hard fail이 아니라 **silent-ish skip** 성격이다
- operator가 실패 유닛만 보면 "실행 안 됨"을 놓칠 수 있다
- 반대로 concurrency로 state를 깨는 위험은 줄인다

즉 `Step 1` 이전에도 이미 다음 contract가 들어가 있다.

- unit timer schedule
- injected args
- nonblocking flock skip behavior

#### stage D. wrapper-internal default fill

service가 넘기지 않은 값들은 `daily_champion_challenger_v5_for_server.ps1` 내부에서 채워진다.

특히 `Step 1`에 직접 관련된 default:

- `CandlesRefreshScript`
  - `scripts/run_candles_api_refresh.ps1`
- `ProjectRoot`
  - 빈 값이면 `PSScriptRoot`의 parent
- `PythonExe`
  - Linux에서는 `<project>/.venv/bin/python`
- `BatchDate`
  - 빈 값이면 `yesterday`
  - 단, 이 값은 `Step 1`에는 안 넘긴다

즉 `Step 1` 시작 시점의 value chain은 실제로 아래처럼 읽어야 한다.

1. installer default 결정
2. service `ExecStart` arg 조립
3. flock nonblocking wrapper 부착
4. v5 wrapper가 비어 있는 arg를 자체 default로 채움
5. 그 후에야 `candles_api_refresh` child script call 여부가 결정됨


## 2. Full Cycle Order Preview

지금 코드 기준으로 `00:20` 체인의 순서는 아래다.

1. `autobot-v5-challenger-spawn.timer`
2. `autobot-v5-challenger-spawn.service`
3. flock-wrapped `ExecStart`
4. `scripts/daily_champion_challenger_v5_for_server.ps1 -Mode spawn_only`
5. `candles_api_refresh`
6. `raw_ticks_daily`
7. `train_snapshot_close`
8. delegated split wrapper call
   - `scripts/daily_champion_challenger_v4_for_server.ps1`
9. governed acceptance wrapper
   - default: `scripts/v5_governed_candidate_acceptance.ps1`
10. actual acceptance worker
   - `scripts/candidate_acceptance.ps1`
11. acceptance 내부:
   - `train_snapshot_close_preflight`
   - feature/data/runtime-pretrain checks
   - dependency trainer chain
   - dependency runtime export chain
   - fusion trainer
   - runtime dataset / runtime viability / runtime deploy contract preflight
   - backtest / runtime parity / paper
   - adoption / challenger-or-paired-paper start branch

중요한 점:

- 이 체인은 병렬 체인이 아니라 **순차 체인**이다.
- `daily_champion_challenger_v5_for_server.ps1`는 먼저 pre-chain 1~3을 직접 돈다.
- 그 다음 old split wrapper인 `daily_champion_challenger_v4_for_server.ps1`에 위임한다.
- 위임 시에는 `-SkipDailyPipeline:$true`, `-SkipFeatureContractRefresh:$true`를 넘긴다.
- `v4` wrapper는 다시 default acceptance entry로 `v5_governed_candidate_acceptance.ps1`를 잡고,
  그 스크립트가 실제로 `candidate_acceptance.ps1`를 `v5_fusion + dependency trainers` 설정으로 호출한다.

즉 `00:20` 스폰 체인은 실제로 아래처럼 읽어야 한다.

- `00:20 spawn timer`
- `v5 wrapper`
- `candles refresh`
- `raw ticks daily`
- `train snapshot close`
- `v4 split wrapper`
- `v5 governed acceptance`
- `candidate_acceptance`

이번 문서에서는 여기서도 **오직 Step 1만 깊게 판다**.
다른 단계는 위 순서를 고정하기 위한 preview로만 사용한다.

## 3. Step 1 Definition

이번 문서에서 깊게 다루는 Step 1은 다음이다.

- step name:
  - `candles_api_refresh`
- wrapper file:
  - `scripts/daily_champion_challenger_v5_for_server.ps1`
- concrete worker script:
  - `scripts/run_candles_api_refresh.ps1`

이 단계의 역할:

- 최신 top-N KRW universe 기준으로 candle top-up plan을 만든다.
- 실제 candle collect를 수행한다.
- collect 과정에서 validate report까지 같이 만든다.
- 다음 단계인 `train_snapshot_close`가 freshness check할 수 있는 summary를 남긴다.


## 4. Step 1 Entrance Condition

`scripts/daily_champion_challenger_v5_for_server.ps1` 기준 Step 1은 아래 조건에서만 실행된다.

### 4.1 Outer mode gate

아래 조건이 먼저 필요하다.

- `if ($Mode -ne "promote_only")`

즉 `00:20 spawn_only`에서는 실행되고,
`00:10 promote_only`에서는 실행되지 않는다.

### 4.2 Script existence gate

그 다음 아래 조건이 필요하다.

- `if (Test-Path $resolvedCandlesRefreshScript)`

기본 resolver:

- function:
  - `Resolve-DefaultCandlesRefreshScript`
- resolved path:
  - `scripts/run_candles_api_refresh.ps1`

중요한 누락 없는 해석:

- 이 조건이 `false`이면 Step 1은 **실패가 아니라 skip** 된다.
- 즉 wrapper는 `candles_api_refresh`를 안 돌리고 다음 단계로 진행한다.
- 따라서 “존재하지 않으면 즉시 fail”은 아니다.


## 5. Step 1 Wrapper Inputs

`scripts/daily_champion_challenger_v5_for_server.ps1`가 Step 1에 넘기는 인자는 매우 적다.

실제 `candlesArgs`:

- `-ProjectRoot <resolvedProjectRoot>`
- `-PythonExe <resolvedPythonExe>`
- optional:
  - `-DryRun`

즉 wrapper는 여기서

- 날짜를 넘기지 않는다
- quote/top-n/market-mode를 넘기지 않는다
- dataset 이름도 넘기지 않는다

추가로 중요:

- wrapper는 `resolvedBatchDate`를 계산하지만 Step 1에는 **넘기지 않는다**
- 즉 Step 1은 batch-date-aware step이 아니다

이 값들은 전부 **step script 내부 default**에 맡긴다.


## 6. Step 1 Wrapper Variables

Step 1과 직접 연결되는 wrapper 변수는 아래다.

### 6.1 `resolvedProjectRoot`

결정 규칙:

- `ProjectRoot` 파라미터가 비어 있으면
  - `Split-Path -Path $PSScriptRoot -Parent`
- 아니면
  - `GetFullPath(ProjectRoot)`

역할:

- 모든 다음 상대 경로의 기준 root

### 6.2 `resolvedPythonExe`

결정 규칙:

- `PythonExe` 파라미터가 비어 있으면
  - Windows: `C:\Python314\python.exe`
  - Linux: `<project>/.venv/bin/python`
- 아니면 입력값 그대로

역할:

- step script가 내부 CLI 호출에 쓸 python path

중요한 디버그 포인트:

- wrapper default와 worker script default가 완전히 같지는 않다
- wrapper는 Windows에서:
  - `C:\Python314\python.exe`
- worker script `run_candles_api_refresh.ps1`는 직접 호출될 때:
  - `Resolve-DefaultPythonExe -Root <project>`
  - 즉 보통 `<project>\\.venv\\Scripts\\python.exe`

실제 `00:20` 체인에서는 wrapper가 `PythonExe`를 child에 직접 넘기므로
worker 자체 default는 잘 안 드러난다.
하지만 로컬 수동 디버그에서는

- wrapper 통해 실행할 때
- worker를 직접 단독 실행할 때

python 해석이 달라질 수 있다.

### 6.3 `resolvedPwshExe`

Linux에서는:

- 우선 `Get-Command pwsh`
- 없으면 `"pwsh"`

역할:

- `Invoke-CheckedScript`가 child PowerShell script를 실행할 binary

### 6.4 `resolvedCandlesRefreshScript`

결정 규칙:

- `CandlesRefreshScript` 파라미터가 비어 있으면
  - `scripts/run_candles_api_refresh.ps1`
- 아니면 입력값

역할:

- Step 1 실제 script path

### 6.5 `resolvedBatchDate`

결정 규칙:

- `Resolve-BatchDateValue -DateText $BatchDate`

역할:

- wrapper 전체에서는 이후 Step 2/3에 사용된다
- 하지만 **Step 1에는 전달되지 않는다**


## 7. Step 1 Wrapper Call Shape

실제 호출은 helper `Invoke-CheckedScript`를 탄다.

helper 역할:

- `pwsh -NoProfile -ExecutionPolicy Bypass -File <script> ...`
  형태로 child script 실행
- step name 출력
- child exit code가 0이 아니면 즉시 throw

Step 1의 실제 call:

- script:
  - `resolvedCandlesRefreshScript`
- args:
  - `-ProjectRoot`
  - `-PythonExe`
  - optional `-DryRun`
- step name:
  - `candles_api_refresh`

추가로 중요한 실행 성질:

- 이 호출은 `Invoke-CheckedScript`를 통해 **동기/blocking** 으로 수행된다.
- 즉 Step 1이 끝나기 전에는 Step 2로 가지 않는다.


## 8. Step 1 Failure Behavior In Wrapper

Step 1이 실패하면 wrapper는 즉시 중단한다.

실패 시 행동:

1. `Write-V5WrapperFailureReport`
2. failure stage:
   - `data_close`
3. failure code:
   - `CANDLES_API_REFRESH_FAILED`
4. failure report path:
   - `data/collect/_meta/candles_api_refresh_latest.json`
5. 그 후 `throw`

즉 Step 1 실패 시:

- raw ticks
- snapshot close
- acceptance

는 시작되지 않는다.

반대로 말하면:

- Step 1이 **skip** 되면 다음 단계는 계속 갈 수 있다.
- Step 1이 **실행되었다가 실패**하면 다음 단계는 가지 않는다.


## 9. Step 1 Script Parameters

이제 실제 worker script `scripts/run_candles_api_refresh.ps1`를 본다.

### 9.1 Parameter list

이 스크립트의 파라미터는 아래다.

- `ProjectRoot`
- `PythonExe`
- `MetaDir`
  - default: `data/collect/_meta`
- `SummaryPath`
  - default: `data/collect/_meta/candles_api_refresh_latest.json`
- `Quote`
  - default: `KRW`
- `MarketMode`
  - default: `top_n_by_recent_value_est`
- `TopN`
  - default: `50`
- `BaseDataset`
  - default: `candles_api_v1`
- `OutDataset`
  - default: `candles_api_v1`
- `PlanPath`
  - default: `data/collect/_meta/candle_topup_plan_auto.json`
- `LookbackMonths`
  - default: `3`
- `Tf`
  - default: `1m,5m,15m,60m,240m`
- `MaxBackfillDays1m`
  - default: `3`
- `Markets`
  - default: `@()`
- `Workers`
  - default: `1`
- `MaxRequests`
  - default: `120`
- `RateLimitStrict`
  - default: `true`
- `DryRun`

이 script는 helper를 위해 아래 external utility도 사용한다.

- `systemd_service_utils.ps1`
  - `Resolve-DefaultProjectRoot`
  - `Resolve-DefaultPythonExe`
  - `Quote-ShellArg`
  - `Join-DelimitedStringArray`


## 10. Step 1 Script Derived Variables

실제로 중요한 파생 변수는 아래다.

### 10.1 `resolvedProjectRoot`

- empty면 default project root
- 아니면 full path

### 10.2 `resolvedPythonExe`

- empty면 default python exe
- 아니면 입력값

### 10.3 `resolvedMetaDir`

- `MetaDir`를 project root 기준 절대경로로 변환

### 10.4 `resolvedSummaryPath`

- summary json 최종 출력 위치

### 10.5 `resolvedPlanPath`

- candle plan json 최종 출력 위치

### 10.6 `serializedMarkets`

- `Markets` 배열을 delimiter string으로 직렬화
- 비어 있으면 market filter arg 자체를 안 넣는다

### 10.7 `validateReportPath`

- 항상:
  - `<meta_dir>/candle_validate_report.json`

### 10.8 Hidden default time window semantics

`Step 1`은 batch-date-aware가 아니므로,
실제 collect window는 `BatchDate`가 아니라 planner 내부 default time window로 결정된다.

핵심 helper:

- `default_inventory_window(lookback_months, end_ts_ms=None)`

현재 기본 규칙:

- `lookback_months=3`이면 `lookback_days = 90`
- `end_ts_ms`가 비어 있으면
  - `datetime.now(timezone.utc) - 1 day`
  - 그리고 `second=0`, `microsecond=0`으로만 truncate
- 즉 end는
  - "`어제의 같은 시각(UTC, 초 단위 절삭)`"
  - 이지
  - "`어제 00:00 UTC`"도 아니고
  - "`BatchDate 23:59:59`"도 아니다

이건 매우 중요하다.

- Step 1 plan/validate window는 **rolling UTC window**다
- Step 2 raw ticks처럼 특정 `batch_date` 커버리지 계약을 직접 들고 있지 않다
- 따라서 Step 1 freshness는 "최근에 성공했는가"와 더 가깝고,
  "특정 날짜를 확실히 덮었는가"와는 다르다


## 11. Step 1 Internal Branches

Step 1의 내부 분기는 단순하다.

### 11.1 Dry-run branch

helper `Invoke-ProjectPythonStep` 내부:

- `if ($DryRun)`

이면 실제 python을 실행하지 않고:

- `step`
- `command`
- `exit_code = 0`
- `dry_run = true`

만 남긴다.

그리고 중요:

- 이 경우 실제 `plan-candles` / `collect candles` CLI는 실행되지 않는다.
- 하지만 최종 summary json은 작성된다.

### 11.2 Markets branch

`serializedMarkets`가 비어 있지 않으면:

- plan 단계에만 `--markets <csv>`를 추가한다.

비어 있으면:

- quote / market-mode / top-n universe를 그대로 사용한다.

### 11.3 Hard-fail branch

각 python step은 helper에서:

- stdout/stderr 출력
- nonzero exit면 throw

즉 plan 단계나 collect 단계 어느 하나라도 실패하면 script 전체가 실패한다.

추가로 중요한 결과:

- failure가 나면 summary write 블록까지 도달하지 못한다.
- 즉 `candles_api_refresh_latest.json`은 **이전 성공본이 남아 있을 수 있다**
- wrapper 쪽 freshness 판단은 다음 run에서 이 stale summary를 보게 된다.

### 11.4 Inventory selection branch that Step 1 depends on

planner는 local candle 상태를 `build_candle_inventory()`로 읽는다.

여기서 중요한 hidden rule:

- manifest에서 같은 `(market, tf)`에 대해
  - `latest_any`
  - `latest_ok_or_warn`
  를 따로 추적한다
- 최종 선택은
  - **최신 `OK/WARN` row with rows>0 and ts span**
  - 가 있으면 그걸 우선 사용
  - 없으면 최신 row 어떤 상태든 사용

즉 매우 중요한 해석:

- 최신 manifest row가 `FAIL`이어도
- 과거 `OK/WARN + rows>0` row가 남아 있으면
- inventory는 그 과거 성공 row를 현재 local coverage 근거로 사용할 수 있다

이건 planner가 완전히 비어 버리지 않도록 하는 fail-soft 성격이지만,
반대로 "가장 최근 수집 실패"를 Step 1 inventory layer가 약하게 반영하는 지점이기도 하다.

### 11.5 Active-market filter branch

market selection은 `resolve_active_quote_markets()`에 의존한다.

현재 규칙:

- override가 있으면 override 사용
- `enabled=false`면 active filter 자체를 끔
- `enabled=true`이면 Upbit `markets(is_details=True)` 호출
  - repo code path:
    - `UpbitPublicClient.markets()`
    - `GET /v1/market/all?isDetails=true`
- 그런데 이 호출이 예외나 invalid payload면:
  - `active_markets = None`
  - meta status는 `unavailable` 또는 `invalid_payload`
  - 그리고 `filter_markets_by_active_set()`는
    **필터를 포기하고 원래 candidate list를 그대로 통과**시킨다

즉 active-market resolution은 현재 **fail-open**이다.

- active market API가 깨져도 Step 1 plan은 계속 만들어진다
- 이 경우 inactive/delisted candidate가 더 쉽게 plan에 남을 수 있다

### 11.6 Paging / retry / truncation branch

collect 단계의 REST fetch는 `UpbitCandlesClient.fetch_candles_range()`를 탄다.

핵심 규칙:

- settings source:
  - `load_upbit_settings(config_dir)`
- 현재 config 기준:
  - `base_url = https://api.upbit.com`
  - timeout: `connect=3s`, `read=10s`, `write=10s`
  - retry: `max_attempts=3`
  - retry backoff: `200ms -> 2000ms cap`
  - REST candle rate-limit group default: `10 rps`
- page size는 `200`
- minute tf endpoint:
  - `GET /v1/candles/minutes/{tf_min}`
- second tf endpoint:
  - `GET /v1/candles/seconds`
- `to` cursor를 뒤로 밀며 pagination
- `max_requests`에 도달하면
  - `truncated_by_budget = true`
  - hard fail이 아니라 collect result에 warning성 신호로 남긴다
- page earliest timestamp가 이전 earliest와 진전이 없으면
  - `loop_guard_triggered = true`
  - 무한 paging을 막고 종료한다
- `RateLimitError`
  - cooldown + jitter 후 retry
- `NetworkError` / `ServerError`
  - exponential-ish backoff 후 retry

즉 Step 1 collect는 단순 fetch 한 번이 아니라:

- pagination
- retry
- rate-limit backoff
- budget truncation
- loop-guard

를 다 포함한 fetch contract다


## 12. Step 1 Actual Python Calls

Step 1은 정확히 두 번의 project python call을 한다.

### 12.1 Plan step

step name:

- `plan_candles_api`

CLI:

- `python -m autobot.cli collect plan-candles`

핵심 args:

- `--base-dataset candles_api_v1`
- `--parquet-root data/parquet`
- `--out <resolvedPlanPath>`
- `--lookback-months 3`
- `--tf 1m,5m,15m,60m,240m`
- `--quote KRW`
- `--market-mode top_n_by_recent_value_est`
- `--top-n 50`
- `--max-backfill-days-1m 3`
- optional `--markets ...`

실제 Python routing:

1. `python -m autobot.cli`
2. `main()`
3. `_handle_collect_command()`
4. `_handle_collect_plan_candles()`
5. `generate_candle_topup_plan()`

#### 12.1.a Handler normalization

`_handle_collect_plan_candles()`에서 실제로 일어나는 값 정규화:

- `parquet_root`
  - `data/parquet`
- `base_dataset`
  - `candles_api_v1`
- `tf_set`
  - csv를 lower-case tuple로 파싱
  - 비면 기본값 `("1m","5m","15m","60m","240m")`
- `quote`
  - upper-case `KRW`
- `market_mode`
  - lower-case `top_n_by_recent_value_est`
- `top_n`
  - 최소 1로 clamp
- `max_backfill_days_1m`
  - 최소 1로 clamp
- `resolve_active_markets`
  - **항상 `true`**

즉 PowerShell이 넘긴 문자열이 그대로 쓰이는 것이 아니라,
CLI handler가 한 번 더 normalize한 뒤 `CandlePlanOptions`로 바꾼다.

여기서 특히 중요한 기본 결합:

- `BaseDataset = candles_api_v1`
- `OutDataset = candles_api_v1`
- `market_source_dataset`
  - wrapper/worker에서 따로 안 넘기므로
  - planner 내부에서는 사실상 `base_dataset`과 동일

즉 Step 1 기본 경로는

- "기존 `candles_api_v1` 상태를 읽고"
- "같은 `candles_api_v1`를 다시 top-up"

하는 자기참조 구조다.

#### 12.1.b `generate_candle_topup_plan()`가 실제로 하는 일

이 함수는 아래 순서로 움직인다.

1. `market_mode`, `tf_set`, `quote`, `lookback_months` 정규화
2. `default_inventory_window()`로 inventory window 계산
3. `build_candle_inventory()`로 base dataset inventory 로드
4. `build_candle_inventory()`로 market source dataset inventory 로드
5. `_select_markets()`로 market universe 확정
6. 각 `(market, tf)`에 대해 missing range를 target으로 변환
7. plan json을 disk에 기록

inventory helper의 hidden semantics도 중요하다.

- `build_candle_inventory()`는 dataset root의 parquet를 바로 스캔하지 않고
  - `manifest.parquet`를 기준으로 inventory를 만든다
- coverage는 실제 row count가 아니라
  - `min_ts_ms`, `max_ts_ms`와 requested window의 overlap으로 계산된다
- missing range는
  - `MISSING_FRONT`
  - `MISSING_TAIL`
  - `NO_LOCAL_DATA`
  성격으로 만들어진다

여기서 가장 중요한 추가 해석:

- `build_candle_inventory()`의 `missing_ranges`는
  - **front/tail 결손만 계산**
  - 내부 hole을 직접 열거하지 않는다
- 그리고 Step 1 collect manifest schema 자체에는
  - `gaps_found`
  - per-gap 위치 정보
  가 없다

즉 planner는 현재 구조상:

- "partition span 밖의 결손"에는 민감하지만
- "span 안쪽의 내부 gap"에는 둔감하다

이건 곧:

- validation이 내부 gap을 `WARN/FAIL`로 볼 수는 있어도
- 다음 Step 1 plan이 그 내부 gap을 명시적 backfill target으로 자동 복구하지는 못할 수 있다

즉 planner의 결손 판단은 "모든 gap을 row-by-row 직접 스캔"이 아니라
manifest 기반 span reasoning이 섞여 있다.

여기서 중요한 default 의미:

- `market_mode = top_n_by_recent_value_est`
  - `estimate_recent_value_by_market(... lookback_days=30 ...)`를 사용
- 그 후 `_finalize_market_selection()`에서
  - `resolve_active_quote_markets()`
  - `filter_markets_by_active_set()`
  를 통해 **active KRW market filter**를 다시 건다

즉 현재 Step 1 plan은 단순히 "parquet에 있는 market"이 아니라
아래 두 조건을 같이 본다.

- recent value estimate ranking
- active market filter

추가로 `top_n_by_recent_value_est`의 hidden fallback:

- preferred tf 순서:
  - `1s,1m,3m,5m,10m,15m,30m,60m,240m`
- 이 순서대로 value estimate를 시도하다가
  - 첫 non-empty tf를 채택한다
- 모든 estimate가 비면
  - fallback으로 inventory market alphabetical selection을 쓴다

그런데 현재 Step 1 기본값에서는
`market_source_dataset = candles_api_v1`이므로,
아예 초기 dataset이 비어 있거나 매우 빈약하면 아래가 동시에 일어날 수 있다.

- value estimate 비어 있음
- inventory fallback market list도 비어 있음
- 따라서 `selected_markets = 0`
- 결국 `targets = 0`

즉 현재 Step 1 기본 contract는
**완전 공백 상태를 스스로 bootstrap하는 경로가 아니다**.

자동 bootstrap이 되려면 적어도 하나가 필요하다.

- 기존 `candles_api_v1` coverage
- `fixed_list` / explicit `Markets`
- 별도 `market_source_dataset`

#### 12.1.c Target range construction semantics

target 생성 시 핵심 규칙:

- inventory entry가 아예 없으면
  - synthetic missing range를 만들고 reason은 `NO_INVENTORY_ENTRY`
- `1m` tf는
  - `max_backfill_days_1m = 3` 제한이 적용된다
- 그보다 오래된 1m 결손은
  - target으로 안 들어가고
  - `skipped_ranges`에 `OUTSIDE_1M_BACKFILL_LIMIT`로 남는다
- `5m/15m/60m/240m`는 이 Step 1 script에서 별도 recent-cap을 두지 않는다
- 각 target은
  - `need_from_ts_ms`
  - `need_to_ts_ms`
  - `reason`
  - `estimated_bars`
  - `max_calls_budget_hint`
  를 가진다

즉 Step 1 plan은 "모든 missing candle을 무조건 메운다"가 아니라
**최근성 제한이 들어간 실행 계획**이다.

#### 12.1.d Plan-step success meaning

이 step이 `exit_code = 0`이라는 것은 아래만 뜻한다.

- parser/handler 실행이 예외 없이 끝남
- plan file write가 끝남

이 값이 아래를 뜻하지는 않는다.

- 반드시 target이 1개 이상 있다
- 반드시 새 row가 다음 collect에서 쓰일 것이다
- skipped range가 없다

즉 `targets = 0`이어도 plan step 자체는 성공할 수 있다.

### 12.2 Collect step

step name:

- `collect_candles_api`

CLI:

- `python -m autobot.cli collect candles`

핵심 args:

- `--plan <resolvedPlanPath>`
- `--out-dataset candles_api_v1`
- `--parquet-root data/parquet`
- `--collect-meta-dir <resolvedMetaDir>`
- `--validate-report <validateReportPath>`
- `--workers 1`
- `--dry-run false`
- `--max-requests 120`
- `--rate-limit-strict true`

중요한 미묘점:

- `run_candles_api_refresh.ps1` 자체의 `-DryRun`은
  - 이 collect CLI의 `--dry-run true`로 번역되지 않는다
- 대신 PowerShell helper `Invoke-ProjectPythonStep`가
  - **Python 호출 자체를 생략**한다

즉 현재 Step 1 경로에서 collect CLI가 실제로 실행되는 경우,
그 collect CLI는 항상 `--dry-run false`로 돈다.

실제 Python routing:

1. `python -m autobot.cli`
2. `main()`
3. `_handle_collect_command()`
4. `_handle_collect_candles()`
5. `collect_candles_from_plan()`
6. `validate_candles_api_dataset()`

#### 12.2.a Handler normalization

`_handle_collect_candles()`에서 실제로 일어나는 정규화:

- `out_dataset`
  - `candles_api_v1`
- `collect_meta_dir`
  - `data/collect/_meta`
- `dry_run`
  - `_parse_bool_arg("false")` -> `false`
- `workers`
  - 최소 1로 clamp
- `max_requests`
  - `120`
- `rate_limit_strict`
  - `_parse_bool_arg("true")` -> `true`

그리고 validation severity는 CLI handler가 config default를 그대로 읽는다.

현재 `config/base.yaml` 기준:

- `data.qa.gap_severity = info`
- `data.qa.quote_est_severity = info`
- `data.qa.ohlc_violation_policy = drop_row_and_warn`

즉 현재 Step 1 기본 경로에서는:

- gap 발견
- volume quote estimate 사용
- OHLC violation

중 일부가 기본적으로 hard fail이 아니라
`WARN/info` 쪽으로 흘러가도록 설정되어 있다.

#### 12.2.b `collect_candles_from_plan()`가 실제로 하는 일

이 함수는 아래 순서로 움직인다.

1. plan file load
2. `targets` 목록 추출
3. dataset root 생성
4. manifest path 계산
5. target별 API fetch
6. parquet partition write
7. manifest append
8. collect report write
9. build report write

중요한 실행 계약:

- `dataset_root = data/parquet/candles_api_v1`
- manifest path:
  - `data/parquet/candles_api_v1/_meta/manifest.parquet`
- `rate_limit_strict = true`이면
  - `effective_workers = 1`로 강제
- `max_requests`가 non-null이어도
  - serial path를 강제한다
- `max_requests`는
  - target count budget이 아니라
  - **REST request count budget**이다

즉 wrapper가 `-Workers 1`을 넘겨서 직렬화하는 것뿐 아니라,
`rate_limit_strict=true` 자체도 병렬 collect를 막는 방향으로 작동한다.

writer semantics도 중요하다.

- partition path는 항상:
  - `tf=<tf>/market=<market>/part-000.parquet`
- incoming rows는 schema 정규화 후
  - required OHLCV/ts null row를 버린다
- 기존 parquet가 있으면
  - **해당 partition dir의 모든 parquet 파일**을 읽어 합친 뒤
  - incoming과 다시 합친다
- dedupe rule:
  - `ts_ms` 기준 unique
  - `keep="last"`
- 최종 결과는 `part-000.parquet`로 다시 쓴다

즉 동일 `ts_ms` 충돌 시에는
**나중에 들어온 row가 기존 row를 덮는다**.

#### 12.2.c Per-target status semantics

target 하나를 collect할 때 상태는 아래처럼 결정된다.

- `OK`
  - rows가 있고
  - loop-guard / budget warning이 없음
- `WARN`
  - `NO_ROWS_COLLECTED`
  - `PAGING_LOOP_GUARD_TRIGGERED`
  - `MAX_REQUESTS_BUDGET_REACHED`
  - `DELISTED_OR_INACTIVE_MARKET`
- `FAIL`
  - `COLLECT_EXCEPTION`

즉 `WARN`은 실제로 꽤 넓다.

- row가 0개여도 `WARN`
- 시장이 이미 비활성/상장폐지여도 `WARN`
- request budget에 걸려 잘려도 `WARN`

#### 12.2.d Collect-step exit code semantics

CLI handler는 아래 규칙으로 종료한다.

- `collect_options.dry_run == true`
  - 즉시 `0`
- 아니면 validation까지 수행한 뒤
  - `collect_summary.fail_targets > 0` 이면 `2`
  - `validate_summary.fail_files > 0` 이면 `2`
  - 그 외는 `0`

즉 아래는 **exit 0**으로 통과 가능하다.

- `warn_targets > 0`
- `validate warn_files > 0`
- `selected_targets = 0`
- `processed_targets = 0`

이 점은 Step 1 downstream contract에서 매우 중요하다.

추가로 field semantics도 조심해야 한다.

- `selected_targets`는
  - `max_requests <= 0`인 특수 case를 제외하면
  - 거의 `discovered_targets`와 같게 남는다
- request budget에 의해 실제 처리 대상이 중간에서 잘려도
  - `selected_targets`는 줄지 않을 수 있다
- 이런 경우 실제 진도는
  - `processed_targets`
  - `calls_made`
  - detail별 `truncated_by_budget`
  를 봐야 알 수 있다

### 12.3 Hidden validate phase inside collect step

`Step 1`은 surface상 두 개의 Python call만 있지만,
두 번째 call 내부에는 실제로 validation phase가 숨어 있다.

`validate_candles_api_dataset()`의 핵심 동작:

1. `data/parquet/candles_api_v1/tf=*/market=*/*.parquet` 전체 스캔
2. 각 part file에 대해 schema / OHLC validation
3. file별 `OK/WARN/FAIL` 산출
4. `coverage_delta` 계산
   - plan의 `base_dataset`
   - API-collected dataset
   를 같은 window에서 비교
5. report json write

scope 해석에서 특히 중요:

- collect phase는
  - 현재 plan의 `targets`만 처리한다
- 그러나 validate phase는
  - 현재 target subset이 아니라
  - `candles_api_v1` dataset 아래 **전체 parquet file**을 스캔한다

즉 Step 1 validate는
"이번 run이 만진 파일만 검사"가 아니다.

따라서:

- 오래된 다른 market/tf partition의 문제
- 이번 run target에 포함되지 않은 stale/corrupt file

도 Step 1 전체 exit code에 영향을 줄 수 있다.

validate report 핵심 top-level 필드:

- `checked_files`
- `ok_files`
- `warn_files`
- `fail_files`
- `schema_ok`
- `ohlc_ok`
- `coverage_delta`
- `details`

validation reason mapping을 더 정확히 적으면 아래다.

- 즉시 `FAIL`:
  - `MISSING_REQUIRED_COLUMNS`
  - `TS_NULL_FOUND`
  - `TIMESTAMP_PARSE_FAILED`
  - `REQUIRED_VALUE_NULL_FOUND`
- `OHLC_VIOLATIONS`
  - 현재 policy가 `drop_row_and_warn`이면 `WARN`
  - 아니면 `FAIL`
- `NON_MONOTONIC_FOUND`
  - `WARN`
- `DUPLICATES_FOUND` / `DUPLICATES_DROPPED`
  - `WARN`
- `INVALID_ROWS_DROPPED`
  - `WARN`
- `TYPE_CAST_FAILURE_ROWS`
  - `WARN`
- `GAPS_FOUND`
  - `gap_severity`에 따름
- `VOLUME_QUOTE_ESTIMATED`
  - `quote_est_severity`에 따름

그리고 `1s` tf는 special-case다.

- `_count_gaps()`가 `1s`에서는 항상 `0`
- 즉 second candle sparse gap은 설계상 정상으로 본다

추가로 `coverage_delta` 해석도 조심해야 한다.

- `_coverage_delta()`는 `before/after` coverage를
  - `min_ts_ms` / `max_ts_ms`
  - overlap span
  기반으로 계산한다
- 즉 coverage delta 역시
  - internal hole 개수
  - hole 위치
  를 직접 반영하지 않는다

따라서:

- coverage delta가 좋아 보여도
- 내부 sparse hole이 완전히 사라졌다는 뜻은 아니다

여기서도 중요한 것은:

- `warn_files`는 Step 1 전체 실패를 직접 만들지 않는다
- `fail_files > 0`일 때만 collect CLI가 `exit 2`를 내고 wrapper가 hard fail한다

### 12.4 What the wrapper actually sees back

`daily_champion_challenger_v5_for_server.ps1`는 Python 내부의 rich report를 직접 파싱하지 않는다.
wrapper가 직접 보는 것은 오직:

- child PowerShell script exit code
- stdout/stderr text

그리고 `run_candles_api_refresh.ps1` summary에는 다음만 남는다.

- step 이름
- command string
- exit code
- dry_run 여부
- output preview

즉 아래 정보는 `Step 1` summary top-level에는 없다.

- `warn_targets`
- `fail_targets`
- `coverage_delta`
- `schema_ok`
- `ohlc_ok`
- 새로 몇 row가 실제로 써졌는지
- active market filter resolution status
- value-est tf fallback 결과
- inventory latest-row fallback 여부

그 정보는 plan/collect/validate 개별 artifact를 열어야만 보인다.

### 12.5 Official Upbit contract relevant to Step 1

Step 1이 직접 기대는 외부 Upbit 공식 계약은 대부분 REST quotation 쪽이다.

#### 12.5.a Pair list endpoint

공식 문서:

- [페어 목록 조회](https://docs.upbit.com/kr/reference/list-trading-pairs)

현재 공식 계약상:

- endpoint:
  - `GET https://api.upbit.com/v1/market/all`
- `is_details` 지원
- rate limit:
  - 초당 최대 `10`회
  - `마켓 그룹`
  - IP 단위

repo-local Step 1 대응:

- `resolve_active_quote_markets()`는 이 endpoint를 사용한다
- 구현상 `isDetails=true`를 넣는다
- 하지만 현재 Step 1 planner는 응답 중
  - `market`
  만 실질적으로 사용하고,
  `market_warning`, `market_event` 같은 세부 필드는 직접 활용하지 않는다

#### 12.5.b Minute candle endpoint

공식 문서:

- [분(Minute) 캔들 조회](https://docs.upbit.com/kr/reference/list-candles-minutes)
- [캔들 API CSV 가이드](https://docs.upbit.com/kr/docs/how-to-download-candle-data)

현재 공식 계약상:

- endpoint:
  - `GET https://api.upbit.com/v1/candles/minutes/{unit}`
- supported unit:
  - `1, 3, 5, 10, 15, 30, 60, 240`
- 캔들은 해당 시간대에 **체결이 있을 때만 생성**
- 체결이 없으면 응답에 해당 캔들이 없다
- candle API는 호출당 최대 `200`개 반환
- rate limit:
  - 초당 최대 `10`회
  - `캔들 그룹`
  - IP 단위

repo-local Step 1 대응:

- local default tf set:
  - `1m,5m,15m,60m,240m`
  - 공식 지원 unit의 부분집합
- planner/validator가 "missing candle == 무조건 데이터 누락"으로 단순 해석하면 안 되는 이유가
  - 바로 이 공식 계약 때문이다
- collector는 page size `200`을 고정 사용하므로
  - 공식 최대 반환량과 정합하다

#### 12.5.c Second candle endpoint

공식 문서:

- [초(Second) 캔들 조회](https://docs.upbit.com/kr/reference/list-candles-seconds)

현재 공식 계약상:

- endpoint:
  - `GET https://api.upbit.com/v1/candles/seconds`
- 최근 `3개월` 이내 데이터만 제공
- 체결이 없으면 캔들이 생성되지 않음
- 조회 가능 기간을 넘으면
  - 빈 리스트
  - 또는 요청 count보다 적은 개수
  가 올 수 있음
- rate limit:
  - 초당 최대 `10`회
  - `캔들 그룹`
  - IP 단위

repo-local Step 1 대응:

- 현재 default tf set에는 `1s`가 없다
- 그러나 planner/client 코드는 `1s`를 지원한다
- 따라서 Step 1이 나중에 `1s`를 포함하도록 바뀌면
  - 공식 `3개월` 제약이 즉시 중요해진다

#### 12.5.d Global rate-limit and Remaining-Req contract

공식 문서:

- [요청 수 제한(Rate Limits)](https://docs.upbit.com/kr/kr/reference/rate-limits)
- [REST API 연동 Best Practice](https://docs.upbit.com/kr/docs/rest-api-best-practice)

현재 공식 계약상:

- rate limit은 초 단위
- 같은 그룹 API끼리 요청 수를 공유
- `Remaining-Req` 응답 헤더로 잔여 요청량을 확인
- `429`는 초당 제한 초과
- `429` 상태에서 계속 밀면 `418` 차단으로 escalates

repo-local Step 1 대응:

- `UpbitHttpClient`는 request 전에 local limiter acquire
- 응답 후 `Remaining-Req`를 parse해서 limiter state를 갱신
- `429`는 retry + cooldown 등록
- `418`은 ban cooldown + global cooldown 등록
- `market`/`candle` group default rps는 현재 config에서 둘 다 `10`

local limiter의 실제 세부 동작:

- `observe_remaining_req(sec=0)`이면
  - 해당 group token을 `0`으로 만들고
  - 최소 `1초` cooldown을 건다
- `register_429(group, attempt)`는
  - `2^(attempt-1)` 기반으로
  - `1초`에서 `8초` 사이 cooldown을 건다
- `register_418(group, cooldown)`는
  - 해당 group뿐 아니라
  - global cooldown까지 같이 올린다

즉 Step 1 throttling은 단순 sleep loop가 아니라
`header-driven sync + 429 cooldown + 418 ban cooldown`
구조다.

즉 현재 repo-local limiter는 공식 문서가 요구하는
`group-aware throttling + Remaining-Req tracking + 429/418 handling`
방향과 정합하다.

#### 12.5.e Origin-header special limit

공식 문서:

- [Origin 헤더 제한 강화 공지](https://docs.upbit.com/kr/kr/changelog/origin_rate_limit)

현재 공식 계약상:

- Quotation REST와 Public WebSocket은
  - `Origin` 헤더가 붙은 경우
  - 강화된 제한
  - `10초당 1회`
  가 적용될 수 있다

repo-local Step 1 해석:

- 현재 `UpbitHttpClient` 기본 헤더는
  - `Accept: application/json`
  만 설정하고
  `Origin`은 직접 넣지 않는다
- 따라서 기본 실행에서는 이 특수 제한이 직접 걸릴 가능성이 낮다
- 다만 reverse proxy / browser-like 환경 / 외부 wrapper가 `Origin`을 주입하면
  실제 체감 rate limit은 문서의 기본 10 rps보다 더 강해질 수 있다

#### 12.5.f WebSocket candle contract is not a direct Step 1 dependency

공식 문서:

- [캔들 WebSocket](https://docs.upbit.com/kr/reference/websocket-candle)
- [WebSocket 연동 Best Practice](https://docs.upbit.com/kr/docs/websocket-best-practice)

현재 공식 계약상 relevant facts:

- candle WS 전송 주기:
  - `1초`
- 체결이 없으면 실시간 candle stream이 발생하지 않을 수 있음
- 같은 `candle_date_time` 데이터가 여러 번 전송될 수 있음
- connection limit:
  - 초당 `5`
- message send limit:
  - 초당 `5`, 분당 `100`

Step 1 해석:

- Step 1은 직접 WebSocket을 사용하지 않는다
- 따라서 WS 규칙은 Step 1 실행 contract 그 자체는 아니다
- 다만 나중에 Step 1 REST candle layer와 WS candle layer를 비교할 때
  - "무체결 시 미생성"
  - "동일 candle_date_time 중복 전송 가능"
  같은 공식 WS 규칙을 혼동하면 안 된다


## 13. Step 1 Outputs

이 단계가 직접 남기는 핵심 산출물:

- plan file
  - `data/collect/_meta/candle_topup_plan_auto.json`
- collect report
  - `data/collect/_meta/candle_collect_report.json`
- dataset build report
  - `data/parquet/candles_api_v1/_meta/build_report.json`
- dataset manifest
  - `data/parquet/candles_api_v1/_meta/manifest.parquet`
- parquet partitions
  - `data/parquet/candles_api_v1/tf=*/market=*/*.parquet`
- validate report
  - `data/collect/_meta/candle_validate_report.json`
- summary
  - `data/collect/_meta/candles_api_refresh_latest.json`

### 13.1 Plan file contract

plan file에는 단순 target 목록만 있는 것이 아니다.

핵심 top-level 필드:

- `version`
- `generated_at`
- `base_dataset`
- `base_dataset_root`
- `window`
- `filters`
- `market_selection`
- `constraints`
- `selected_markets`
- `targets`
- `skipped_ranges`
- `summary`

이 중 Step 1 의미를 해석할 때 중요한 필드:

- `filters.quote`
- `filters.tf_set`
- `filters.market_mode`
- `filters.top_n`
- `filters.max_backfill_days_1m`
- `market_selection.active_market_filter`
- `targets[*].reason`
- `skipped_ranges[*].reason`

즉 Step 1이 실제로 "무엇을 수집하려 했는지"는 summary가 아니라
**plan file이 가장 잘 말한다**.

### 13.2 Collect/build/validate artifact contract

collect report에서 중요한 필드:

- `dry_run`
- `workers_requested`
- `workers_effective`
- `discovered_targets`
- `selected_targets`
- `processed_targets`
- `ok_targets`
- `warn_targets`
- `fail_targets`
- `calls_made`
- `throttled_count`
- `backoff_count`
- `failures`
- `details`

build report에서 중요한 필드:

- `dataset_name`
- `dataset_root`
- `manifest_file`
- `collect_report_file`
- `source_mode = external_upbit_rest_candle_api`
- `summary.processed_targets`
- `summary.ok_targets`
- `summary.warn_targets`
- `summary.fail_targets`
- `summary.calls_made`

validate report에서 중요한 필드:

- `checked_files`
- `ok_files`
- `warn_files`
- `fail_files`
- `schema_ok`
- `ohlc_ok`
- `coverage_delta`
- `details[*].status_reasons`

즉 Step 1 상세 진단은 실제로 아래 순서로 읽는 것이 가장 정확하다.

1. `candle_topup_plan_auto.json`
2. `candle_collect_report.json`
3. `candle_validate_report.json`
4. `candles_api_refresh_latest.json`

### 13.3 Summary payload contract

summary payload 주요 필드:

- `policy = candles_api_refresh_v1`
- `generated_at_utc`
- `project_root`
- `python_exe`
- `meta_dir`
- `plan_path`
- `validate_report_path`
- `steps`

각 `steps[*]` 항목에는 실제로 아래 필드가 들어간다.

- `step`
- `command`
- `exit_code`
- `dry_run`
- `output_preview`

즉 다음 단계에서 Step 1을 판단할 때는 보통 이 summary를 본다.

빠지지 말아야 할 해석:

- 이 summary에는 top-level `pass`가 없다.
- 이 summary에는 top-level `batch_date`도 없다.
- freshness/유효성 판단은 다음 단계가
  - `generated_at_utc`
  - `steps[*].exit_code`
를 조합해서 한다.

### 13.4 Exact downstream consumer: `train_snapshot_close`

`scripts/close_v5_train_ready_snapshot.ps1`는 Step 1을 `Get-SourceFreshnessResult()`로 읽는다.

`candles_api_refresh` 쪽 호출 shape:

- `SummaryFile = data/collect/_meta/candles_api_refresh_latest.json`
- `MaxAgeMinutes = 120`
- `BatchDateValue`
  - **전달하지 않음**

즉 Step 3가 Step 1 summary에서 실제로 읽는 것은 아래다.

- `generated_at_utc`
  - 없으면 fallback으로 `generated_at`
- `steps`
- 각 step의 `exit_code`

그리고 pass 규칙은 사실상 아래다.

- summary file exists
- generated time parse 가능
- age minutes <= 120
- `steps[*].exit_code`가 모두 `0`

여기서 **읽지 않는 것**도 분명히 적어야 한다.

- `plan_path`
- `validate_report_path`
- `steps[*].dry_run`
- `policy`
- `Quote`
- `MarketMode`
- `TopN`
- `OutDataset`
- `selected_markets`
- `targets`
- `warn_targets`
- `coverage_delta`

즉 Step 3가 Step 1에 대해 보장받는 것은
**"최근 120분 안에 exit code 0으로 끝난 summary가 있다"** 수준이지,
semantic contract 전체를 재검증하는 것은 아니다.

### 13.5 Immediate consequences of the current contract

현재 코드 기준, Step 1 downstream pass는 아래를 보장하지 않는다.

- 이번 run에서 실제로 새 candle row가 써졌다
- warning이 하나도 없었다
- validation이 완전히 clean했다
- wrapper가 의도한 `KRW / top_n_by_recent_value_est / top_n=50 / candles_api_v1` 조합으로 실행되었다
- dry-run artifact가 아니었다

### 13.6 Observed live server artifact snapshot on 2026-04-07

실서버에서 직접 확인한 최신 Step 1 artifact snapshot:

- summary:
  - `data/collect/_meta/candles_api_refresh_latest.json`
  - `generated_at_utc = 2026-04-06T18:03:59.2757654Z`
- plan:
  - `generated_at = 2026-04-06T18:03:06+00:00`
  - `selected_markets = 49`
  - `targets = 212`
  - `skipped_ranges = 32`
  - `filters.top_n = 50`
  - `market_selection.value_est_tf = 1m`
  - `market_selection.active_market_filter.status = resolved`
  - `market_selection.active_market_filter.dropped_count = 1`
  - `market_selection.active_market_filter.dropped_sample = ['KRW-FLOW']`
- collect report:
  - `dry_run = false`
  - `discovered_targets = 212`
  - `selected_targets = 212`
  - `processed_targets = 112`
  - `ok_targets = 93`
  - `warn_targets = 19`
  - `fail_targets = 0`
  - `calls_made = 120`
- validate report:
  - `checked_files = 250`
  - `ok_files = 250`
  - `warn_files = 0`
  - `fail_files = 0`
  - `schema_ok = true`
  - `ohlc_ok = true`

summary `steps` 관측값도 중요하다.

- `plan_candles_api.exit_code = 0`
- `collect_candles_api.exit_code = 0`
- `collect_candles_api.output_preview`에는 실제로 아래 요약이 들어 있다
  - `discovered=212 selected=212 processed=112 ok=93 warn=19 fail=0 calls=120 throttled=8 backoff=8`

즉 실서버 최신 실제 run이 아래 사실을 이미 증명한다.

- request budget에 의해 전체 target을 다 처리하지 못해도
- warn target이 남아 있어도
- validate fail이 없으면
- Step 1 summary는 `exit_code = 0`으로 남을 수 있다
- `top_n = 50`이어도
  active market filter/value-est 결과에 따라
  실제 `selected_markets`는 더 작을 수 있다

이건 위 문서의 contract 해석이 단순 추론이 아니라
**실서버 artifact로도 확인된 사실**이라는 뜻이다


## 14. Step 1 Does Not Do

이 단계는 아래를 하지 않는다.

- batch-date freshness 자체를 판정하지 않음
- snapshot publish 하지 않음
- micro / sequence / feature refresh 하지 않음
- acceptance 시작하지 않음

이 역할은 다음 단계들로 넘어간다.


## 15. Step 2+ Preview Only

이번 문서에서 아직 깊게 안 읽은 다음 단계는 아래다.

### Step 2. `raw_ticks_daily`

file:

- `scripts/run_raw_ticks_daily.ps1`

역할 preview:

- `BatchDate`를 기준으로 `days_ago`를 계산
- tick collect plan 생성
- raw tick collect
- validation per-date 수행
- `ticks_daily_latest.json` 생성

### Step 3. `train_snapshot_close`

file:

- `scripts/close_v5_train_ready_snapshot.ps1`

역할 preview:

- Step 1/2 freshness 확인
- `training_critical` refresh
- `features_v4` contract refresh
- data platform snapshot publish
- `train_snapshot_close_latest.json` 기록

### Step 4. Delegated challenger wrapper

file:

- `scripts/daily_champion_challenger_v4_for_server.ps1`

역할 preview:

- v5 wrapper가 pre-chain을 끝낸 뒤, old split wrapper에 위임
- 이때 `-SkipDailyPipeline:$true`
- `-SkipFeatureContractRefresh:$true`

### Step 5. governed acceptance wrapper

file:

- `scripts/v5_governed_candidate_acceptance.ps1`

역할 preview:

- `candidate_acceptance.ps1`에 v5 default contract를 꽂아주는 thin wrapper
- `ModelFamily = train_v5_fusion`
- `Trainer = v5_fusion`
- `DependencyTrainers = v5_panel_ensemble,v5_sequence,v5_lob,v5_tradability`
- `PromotionPolicy = paper_final_balanced`

### Step 6. `candidate_acceptance`

file:

- `scripts/candidate_acceptance.ps1`

역할 preview:

- train_snapshot_close preflight
- feature parity
- dependency trainers
- dependency runtime exports
- fusion train
- runtime viability / runtime deploy contract preflight
- backtest / runtime parity / paper


## 16. Step 1 Alignment And Risk Notes

### 16.1 Alignment with the broader blueprint

`TRAINING_MODEL_STRENGTHENING_BLUEPRINT_2026-03-25` 기준 큰 데이터 경로는 아래다.

1. raw candles / raw ws / raw ticks 수집
2. micro 집계
3. feature dataset
4. train / compare / acceptance

이 관점에서 Step 1은:

- raw candle top-up
- raw candle validation
- raw candle freshness summary publish

만 담당한다.

즉 Step 1은 청사진과 모순되지 않는다.
오히려 `raw candles` 레이어의 가장 앞단 operational entry로 읽는 것이 맞다.

### 16.2 Alignment with `2026-04-06` hardening document

`RUNTIME_VIABILITY_AND_RUNTIME_SOURCE_HARDENING_PLAN_2026-04-06.md`의 핵심은

- runtime source consistency
- runtime viability hard fail
- acceptance fail-early

다.

이 기준에서 Step 1의 위치는 분명하다.

- Step 1은 runtime viability gate가 아니다
- Step 1은 acceptance gate도 아니다
- Step 1은 그보다 앞단의 **source freshness gate 재료**를 만드는 단계다

따라서 아래는 정합하다.

- `2026-04-06` 문서가 말하는 주요 fail-early 포인트:
  - `candidate_acceptance` 내부의 `train_snapshot_close_preflight`, `runtime_viability_preflight`
- 현재 Step 1의 역할:
  - 그 preflight가 볼 upstream candle freshness summary를 만든다

즉 Step 1은 `0406` 문서의 핵심 해결책 그 자체는 아니지만,
그 해결책이 기대하는 upstream source contract 일부를 담당한다.

### 16.3 Current Step 1 contract weaknesses / bug-like candidates

아래는 현재 코드 기준 Step 1 범위에서 명시적으로 적어둘 가치가 있는 약점들이다.

1. `dry_run` summary가 real freshness check를 속일 수 있다.
   - `run_candles_api_refresh.ps1 -DryRun`은 실제 Python 실행 없이도
     `candles_api_refresh_latest.json`를 쓴다.
   - 이 summary의 `steps[*].exit_code`는 `0`이다.
   - `train_snapshot_close`는 `steps[*].dry_run`을 보지 않는다.
   - 따라서 최근 120분 안의 dry-run summary는 real run freshness gate를 통과시킬 수 있다.

2. downstream freshness check가 Step 1 parameter contract를 검증하지 않는다.
   - `train_snapshot_close`는 아래를 읽지 않는다.
     - `Quote`
     - `MarketMode`
     - `TopN`
     - `OutDataset`
     - `plan filters`
   - 즉 수동 실행이나 다른 파라미터 조합으로 만들어진 recent summary도
     freshness gate는 통과할 수 있다.

3. `WARN`은 Step 1 전체 실패를 만들지 않는다.
   - `NO_ROWS_COLLECTED`
   - `MAX_REQUESTS_BUDGET_REACHED`
   - `PAGING_LOOP_GUARD_TRIGGERED`
   - `DELISTED_OR_INACTIVE_MARKET`
   - validate-side `WARN`
   는 모두 `exit 0`에 남을 수 있다.

4. `targets = 0`이어도 Step 1 success가 가능하다.
   - plan step은 zero-target plan도 성공
   - collect step도 zero selected/processed target 상태로 성공 가능
   - 따라서 "Step 1 success"는 "이번 run에서 새 candle을 실제로 썼다"와 동치가 아니다.

5. timer overlap은 hard fail이 아니라 skip이다.
   - flock busy면 service는 `exit 0`
   - concurrency safety에는 유리하지만
   - operator가 실패 유닛만 볼 경우 00:20 작업 누락을 놓칠 수 있다

6. Step 1 summary는 rich diagnostic를 축약한다.
   - top-level `pass` 없음
   - top-level `batch_date` 없음
   - warn/fail detail count 없음
   - coverage delta 없음
   - semantic drift 검사용 field 없음

7. active-market resolution은 fail-open이다.
   - Upbit market-all 호출이 실패해도
   - planner는 active filter를 포기하고 candidate list를 그대로 사용한다.

8. inventory는 최신 `FAIL` manifest row보다 과거 `OK/WARN + rows>0` row를 우선 채택할 수 있다.
   - 따라서 planner의 local coverage view는
     "가장 최근 시도"보다
     "최근에 성공적으로 span을 가진 상태"
     에 더 가깝다.

9. Step 1 plan window는 rolling UTC window다.
   - `BatchDate`와 직접 연결되지 않는다.
   - 따라서 "오늘/어제 배치 기준 완전성"과
     "최근 90일 rolling window top-up"은 개념적으로 다르다.

10. writer는 partition dir의 모든 parquet를 읽어 병합하지만,
    최종 write는 `part-000.parquet` 하나에만 다시 쓴다.
    - historical stray parquet가 있으면 현재 동작 해석이 더 복잡해질 수 있다.

11. planner는 internal gap을 본질적으로 잘 못 잡을 수 있다.
    - inventory는 manifest span 기반이다
    - `_missing_ranges()`는 front/tail만 계산한다
    - collect manifest에는 per-gap metadata가 없다
    - 따라서 내부 hole 자동 복구는 현재 Step 1 contract의 강한 보장사항이 아니다

12. validation의 `coverage_delta`도 gap-aware가 아니라 span-aware다.
    - 따라서 `coverage_delta`를 "실질 완전성"으로 과신하면 안 된다

13. 로컬 수동 디버그 시 wrapper와 worker의 Windows default python이 다를 수 있다.
    - wrapper default: `C:\Python314\python.exe`
    - worker direct default: `<project>\\.venv\\Scripts\\python.exe`

14. 현재 Step 1 기본값은 self-bootstrap에 약하다.
    - `base_dataset = candles_api_v1`
    - `out_dataset = candles_api_v1`
    - `market_source_dataset` unset
    - 따라서 기존 `candles_api_v1`가 비어 있으면
      `top_n_by_recent_value_est`가 market을 못 고를 수 있다

15. 현재 QA severity 기본값 자체가 fail-open 쪽이다.
    - `gap_severity = info`
    - `quote_est_severity = info`
    - `ohlc_violation_policy = drop_row_and_warn`
    - 따라서 데이터 품질 이상이 있어도
      collect validate는 기본적으로 `WARN/OK`로 끝날 여지가 크다

16. validate scope는 current target subset보다 넓다.
    - collect는 `plan.targets` 기준
    - validate는 dataset 전체 parquet scan
    - 따라서 이번 run과 직접 무관한 오래된 partition 문제도 Step 1 실패 원인이 될 수 있다

위 1~16은 아직 "즉시 코드 수정해야 할 확정 버그"라고 단정하기보다,
현재 Step 1 contract가 fail-closed하지 않은 지점을 표시한 것이다.


## 17. Step 1 Ticket Breakdown

이제 위 약점 목록을 실제 수정 티켓 단위로 묶는다.

중요한 원칙:

- 이 티켓들은 **테스트베이스 구현보다 코드 자체의 실제 contract 정리**를 우선한다.
- 즉 구현자는 먼저
  - `00:20 timer -> service -> wrapper -> step script -> python -> downstream consumer`
  실제 코드를 따라가서 contract를 고친 뒤,
  그 다음에 테스트를 그 contract에 맞게 보강해야 한다.
- 테스트가 현재 동작과 다를 경우에도
  - 먼저 live entrypoint / code path / artifact contract를 기준으로 진실을 확정하고
  - 그 뒤 테스트를 수정한다.
- 금지:
  - 테스트를 먼저 맞추기 위해 contract를 흐리게 바꾸는 것
  - Step 1 의미를 확정하기 전에 mock-heavy 테스트만 늘리는 것
  - `warn` / `success` / `freshness` 의미를 코드상 정리하지 않은 채 테스트 expectation만 바꾸는 것

### 17.1 Ticket S1-T01: Step 1 Summary Contract Hardening

문제:

- `dry_run` summary도 freshness 통과 가능
- parameter drift가 있어도 downstream이 모름
- summary가 semantic contract를 너무 적게 담음

우선 수정 대상:

- `scripts/run_candles_api_refresh.ps1`
- `scripts/daily_champion_challenger_v5_for_server.ps1`
- `scripts/close_v5_train_ready_snapshot.ps1`

현재 구현 상태:

- 현재는 `skip / 보류`
- 이유:
  - 지금 단계에서 너무 filter/gate 중심으로 흐를 위험이 큼
  - 지금 목표는 먼저 "현재 Step 1 수집이 실제로 잘 되고 있는지"를 관측/판독하는 것임

핵심 수정 방향:

- `candles_api_refresh_latest.json`에 최소한 아래를 명시
  - `dry_run`
  - `quote`
  - `market_mode`
  - `top_n`
  - `base_dataset`
  - `out_dataset`
  - `selected_markets`
  - `targets`
  - `processed_targets`
  - `warn_targets`
  - `fail_targets`
  - `request_budget_hit`
  - `summary_contract_version`
- `train_snapshot_close`는
  - `dry_run=false`
  - expected parameter contract match
  를 검사하도록 강화

수정 후 정합성 체크:

- `00:20` 자동 run에서 새 summary가 생성되는가
- `dry_run` artifact는 freshness fail이 되는가
- manual override parameter로 만든 summary는 expected contract mismatch로 fail되는가
- 기존 downstream artifact reader가 새 field 추가로 깨지지 않는가

### 17.2 Ticket S1-T02: Success / Warning / Completion Semantics Hardening

문제:

- `warn_targets > 0`이어도 success
- `targets = 0`이어도 success
- request budget truncation이어도 success
- `selected_targets`와 실제 처리량 해석이 혼동됨

우선 수정 대상:

- `autobot/data/collect/candles_collector.py`
- `autobot/cli.py`
- `scripts/run_candles_api_refresh.ps1`
- `scripts/close_v5_train_ready_snapshot.ps1`

현재 구현 상태:

- 현재는 `skip / 보류`
- 이유:
  - 이 티켓도 현재는 filter/gate 비중이 큼
  - 지금은 completion semantics를 더 세게 정의하기보다
    실제 수집 상태를 먼저 보는 쪽이 우선임

핵심 수정 방향:

- `processed_targets < discovered_targets` + `truncated_by_budget`를 explicit failure 또는 blocking warning으로 승격할지 결정
- `targets = 0` 상황을
  - 정상 no-op
  - suspicious no-op
  - hard fail
  중 어떤 contract로 볼지 명확히 고정
- `selected_targets` 의미를 field rename 또는 별도 field 추가로 분리
  - 예: `planned_targets`, `processed_targets`, `skipped_by_budget_targets`

수정 후 정합성 체크:

- live server 최신 artifact 같은 `processed < discovered` 사례가 새 규칙에서 어떻게 분류되는지 설명 가능해야 함
- wrapper failure code가 실제 의미와 맞는가
- `train_snapshot_close`가 Step 1 incomplete 상태를 놓치지 않는가

### 17.3 Ticket S1-T03: Planner Market-Source Contract Hardening

문제:

- `market_source_dataset` unset
- `base_dataset == out_dataset == candles_api_v1`
- 초기 공백 상태에서 self-bootstrap이 약함
- active-market resolution이 fail-open

우선 수정 대상:

- `scripts/run_candles_api_refresh.ps1`
- `autobot/cli.py`
- `autobot/data/collect/plan_candles.py`
- `autobot/data/collect/active_markets.py`

현재 구현 상태:

- 현재는 `skip / 보류`
- `candles_v1`를 기본 market source로 쓰는 시도는 롤백함
- 이유:
  - 현재 OCI 서버 기준 `candles_v1`의 실제 data max timestamp가 `2026-02-22`로 stale함
  - `data/raw`도 현재 서버에 없고, `candles_v1` 자동 ingest path도 활성 운영 흔적이 약함
  - 따라서 Step 1 기본 market source로 쓰면 최신 market activity source로 부적절함
- 즉 현재는 다시
  - `base_dataset == candles_api_v1`
  - `market_source_dataset` unset
  상태로 돌아와 있음

핵심 수정 방향:

- `market_source_dataset`를 explicit contract field로 노출
- 완전 공백 초기화 시 사용할 bootstrap source를 명시
  - `fixed_list`
  - 별도 **current** market source dataset
  - fallback policy
- 중요한 제약:
  - `candles_v1`처럼 stale/retired 성격의 dataset는 기본 source로 쓰지 말 것
- active-market resolution 실패 시
  - fail-open 유지
  - fail-closed 전환
  - degraded mode with explicit artifact flag
  중 하나로 contract를 고정

수정 후 정합성 체크:

- empty `candles_api_v1` 상태에서도 market selection이 의도대로 되는가
- Upbit market-all 호출 실패 시 artifact에 degraded/failure가 명시되는가
- top-N requested와 selected count 차이가 operator에게 설명 가능한가

### 17.4 Ticket S1-T04: Inventory / Gap Model Hardening

목적:

- Step 1이 "무엇을 수집해야 하는지"를 더 진실하게 판단하게 만드는 것
- planner가 실제 누락을 놓치지 않게 만드는 것
- validate가 말하는 데이터 문제와 planner가 보는 결손 모델을 서로 맞추는 것

왜 이 티켓이 중요한가:

- 지금 Step 1의 핵심 문제는
  - 결과를 너무 느슨하게 통과시키는 filter 문제도 있지만
  - 그보다 먼저
    "planner가 실제 결손을 정확히 표현하지 못한다"
  는 점에 더 가깝다
- inventory가 manifest span만 보고,
  planner가 front/tail만 보고,
  validate는 internal gap을 따로 볼 수 있으면
  Step 1 전체가 같은 현실을 보지 않게 된다
- 그러면:
  - planner는 "문제 없다"고 생각하고
  - validate는 "hole이 있다"고 말하거나
  - 반대로 validate는 대충 통과하지만 planner는 실제 필요한 복구를 못 할 수 있다

즉 `T04`는 단순히 strict filter를 추가하는 티켓이 아니라:

- Step 1의 결손 인식 모델
- 복구 대상 모델
- completeness 해석 모델

을 서로 정렬하는 티켓이다

현재 구현 상태:

- 현재는 `skip / 보류`
- 이유:
  - 이 티켓은 Step1 내부의 결손/복구 모델을 설계하는 작업이라 범위가 커짐
  - 지금 목표는 결손 복구 설계보다
    "현재 Step1 수집이 실제로 잘 되고 있는지"를 먼저 보는 것임
  - 특히 micro/ws/orderbook 계열은 캔들처럼 단순 backfill이 안 되므로
    지금 단계에서 결손 모델 설계를 크게 건드리면 범위가 쉽게 퍼짐

문제:

- inventory는 manifest span 기반
- internal gap을 직접 복구 대상으로 못 잡음
- `coverage_delta`도 gap-aware가 아님
- 최신 `FAIL`보다 과거 `OK/WARN` row를 채택할 수 있음

우선 수정 대상:

- `autobot/data/inventory.py`
- `autobot/data/collect/plan_candles.py`
- `autobot/data/collect/validate_candles_api.py`
- 필요시 `autobot/data/collect/candle_manifest.py`

핵심 수정 방향:

- internal gap metadata를 manifest 또는 별도 report에 남길지 결정
- planner가 front/tail뿐 아니라 internal hole도 target화할지 결정
- inventory latest-row 선택 정책을
  - latest attempt 우선
  - latest successful span 우선
  중 어느 쪽인지 명시적으로 고정
- `coverage_delta`와 별도로 gap-aware completeness metric 추가 검토

수정 후 정합성 체크:

- 내부 hole가 있을 때 다음 Step 1 plan이 실제로 복구 target을 생성하는가
- 최신 실패 이후에도 planner가 과거 span을 계속 믿는지 여부가 의도와 일치하는가
- validate report와 planner decision이 서로 모순되지 않는가
- `collect -> validate -> 다음 plan`이 같은 결손 모델을 공유하는가

### 17.5 Ticket S1-T05: Writer / Dataset Scope Contract Hardening

문제:

- writer가 partition dir 전체 parquet를 읽고 다시 `part-000.parquet` 하나로 덮음
- stray parquet 존재 시 의미가 복잡함
- validate는 current target이 아니라 dataset 전체를 검사

우선 수정 대상:

- `autobot/data/collect/candle_writer.py`
- `autobot/data/collect/validate_candles_api.py`
- `autobot/data/collect/candles_collector.py`

핵심 수정 방향:

- partition dir에 `part-000.parquet` 외 stray file이 있으면
  - fail
  - quarantine
  - merge 후 정리
  중 하나로 정책 고정
- validate scope를
  - dataset 전체
  - 이번 run touched partitions only
  - dual report
  중 어느 쪽으로 갈지 결정

수정 후 정합성 체크:

- 이전 stale partition이 Step 1 실패를 일으킬 때 operator가 원인을 바로 알 수 있는가
- write 후 manifest/build/validate가 같은 dataset reality를 보고 있는가
- dedupe `keep=last`가 의도와 맞는가

### 17.6 Ticket S1-T06: Time Window And Batch-Date Contract Hardening

문제:

- Step 1은 rolling UTC window
- `BatchDate`와 직접 연결되지 않음
- 그러나 후속 단계는 batch-date 관점을 더 강하게 가짐

우선 수정 대상:

- `scripts/run_candles_api_refresh.ps1`
- `autobot/data/inventory.py`
- `scripts/close_v5_train_ready_snapshot.ps1`

핵심 수정 방향:

- Step 1을 계속 rolling UTC source-refresh로 둘지
- 아니면 batch-date-aware freshness contract를 추가할지 결정
- 적어도 artifact에
  - requested window start/end
  - window timezone / interpretation
  를 명시적으로 고정

수정 후 정합성 체크:

- operator가 `2026-04-06 batch`와 `rolling 90-day window`를 혼동하지 않게 되었는가
- Step 2 raw ticks와 Step 1 candle freshness semantics가 의도적으로 다른지 설명 가능한가
- `train_snapshot_close`가 두 레이어를 잘 조합하는가

### 17.7 Ticket S1-T07: Operational Skip / Lock Observability Hardening

문제:

- flock busy면 `exit 0`
- timer overlap skip이 실패처럼 드러나지 않음

우선 수정 대상:

- `scripts/systemd_service_utils.ps1`
- `scripts/install_server_daily_split_challenger_services.ps1`
- `scripts/daily_champion_challenger_v5_for_server.ps1`
- 필요시 dashboard/runtime topology 쪽

핵심 수정 방향:

- lock-busy skip을 machine-readable artifact로 남길지 결정
- wrapper latest report에
  - `skipped_by_lock`
  - `lock_file`
  - `skip_reason`
  같은 필드 추가 검토

수정 후 정합성 체크:

- timer overlap 시 operator가 skip을 실패와 구분할 수 있는가
- dashboard / latest report / journal 중 최소 하나에서 분명히 보이는가

### 17.8 Ticket S1-T08: QA Severity Policy Reconciliation With 2026-04-06 Philosophy

문제:

- 현재 Step 1 default QA severity는 관대함
- 04-06 문서는 fail-closed 철학을 더 강하게 요구

우선 수정 대상:

- `config/base.yaml`
- `autobot/cli.py`
- `scripts/run_candles_api_refresh.ps1`
- `scripts/close_v5_train_ready_snapshot.ps1`

핵심 수정 방향:

- `gap_severity`, `quote_est_severity`, `ohlc_violation_policy`를
  - Step 1 전용 stricter profile로 분리할지
  - 전체 default를 강화할지
  결정
- 최소한 nightly `00:20` path는
  - interactive/manual path와 다른 stricter contract를 가질 수 있음

수정 후 정합성 체크:

- stricter policy 적용 시 live server가 과도하게 fail하는지
- fail이 늘더라도 그것이 operator에게 더 설명 가능해지는지
- 04-06 문서의 `source consistency / fail-early` 철학과 실제 Step 1이 더 가까워졌는지

### 17.9 Recommended Implementation Order

우선순위는 아래가 맞다.

1. `S1-T01` summary/downstream contract
2. `S1-T02` success/completion semantics
3. `S1-T03` planner market-source contract
4. `S1-T08` QA severity reconciliation
5. `S1-T04` inventory/gap model
6. `S1-T05` writer/validate scope
7. `S1-T06` time-window contract
8. `S1-T07` lock observability

이 순서를 권장하는 이유:

- 먼저 downstream이 Step 1 결과를 잘못 믿는 문제를 막아야 한다
- 그 다음 Step 1 자체가 success를 어떻게 선언할지 정해야 한다
- 그 뒤에 planner/data model을 더 깊게 바꿔야 rollback과 정합성 설명이 가능하다


## 18. Current Confidence Boundary

이 문서로 **확실히 말할 수 있는 것**:

- `00:20` 전체 체인의 진짜 입구는 `autobot-v5-challenger-spawn.timer -> ...spawn.service -> daily_champion_challenger_v5_for_server.ps1`
- Step 1은 `candles_api_refresh`
- Step 1은 wrapper에서 hard fail되는 pre-chain step이다
- Step 1 script의 인자/파생 변수/분기/산출물은 위와 같다
- Step 1의 Python entry surface와 hidden validate phase까지는 이번 문서에서 열었다
- `train_snapshot_close`가 Step 1 summary를 어떤 필드만 읽는지까지는 이번 문서에서 확정했다
- Step 1 범위의 fail-open 약점 후보도 위와 같이 특정할 수 있다
- 실서버 timezone / live unit file / latest Step 1 artifact까지 대조했다
- Step 1에 직접 relevant한 Upbit 공식 외부 계약도 함께 반영했다

이 문서로 **아직 말하면 안 되는 것**:

- Step 2 이후 전체 체인이 구현 의도와 완전히 정합하다는 주장
- dependency trainer 내부 / fusion handoff / runtime resolver 전체 검증 완료 주장
- Upbit가 문서에 적지 않은 비공개 내부 동작까지 완전히 예측 가능하다는 주장
- 향후 Upbit 공식 정책 변경 후에도 이 문서가 자동으로 최신이라는 주장

그건 다음 문서에서 단계별로 계속 잘라서 읽어야 한다.


## 19. Step 1 Completion Status

현재 판단 기준으로 Step 1은 여기서 닫는다.

완료로 보는 이유:

- 실제 `00:20 -> service -> wrapper -> Step 1` entrypoint가 고정되었다
- Step 1 worker / Python entry / artifact / downstream consumer를 코드 기준으로 열었다
- 실서버 timezone / live unit file / 실제 artifact 상태를 대조했다
- warning 원인도 실서버 artifact 기준으로 분류했다
- 현재 시점에 즉시 구현해야 할 Step 1 수정 티켓은 없다고 판단했다

즉 Step 1은:

- 문서화 완료
- 현재 운영 상태 판독 완료
- 구현 티켓은 전부 `skip / 보류`

상태로 닫는다.


## 20. Step 2 Handoff

다음 단계는 `Step 2 = raw_ticks_daily`다.

현재 코드 기준 handoff 사실만 먼저 고정:

- Step 1 다음 단계:
  - `scripts/run_raw_ticks_daily.ps1`
- 호출 주체:
  - `scripts/daily_champion_challenger_v5_for_server.ps1`
- wrapper가 넘기는 핵심 인자:
  - `-ProjectRoot`
  - `-PythonExe`
  - `-BatchDate`
  - optional `-DryRun`

Step 2의 중요한 차이:

- Step 1은 batch-date-aware step이 아니었지만
- Step 2는 `BatchDate`를 직접 받아
  `days_ago`와 `validate_dates`를 계산한다

즉 Step 2부터는:

- 운영 날짜 해석
- 로컬 날짜 vs UTC 날짜
- `days_ago=1..7` 제한
- validate 대상 날짜 집합

이 핵심 contract가 된다.

다음 문서/다음 단계에서는 이 `Step 2`를
Step 1과 같은 방식으로

- 진입점
- wrapper inputs
- 변수
- 분기
- Python routing
- 산출물
- downstream consumer

순서로 다시 깊게 판독한다.
