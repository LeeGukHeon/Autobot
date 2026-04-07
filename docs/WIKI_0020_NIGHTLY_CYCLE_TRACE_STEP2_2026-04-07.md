# 00:20 Nightly Cycle Trace, Step 2

## 0. Purpose

이 문서는 `00:20` 야간 사이클의 두 번째 단계인 `raw_ticks_daily`를
실제 코드 기준으로 다시 추적한 위키다.

이번 문서의 목표:

1. `Step 2`의 실제 entrypoint와 wrapper call shape를 고정한다.
2. `scripts/run_raw_ticks_daily.ps1`와 그 아래 Python path를
   함수/인자/변수/분기/산출물 단위로 세세하게 기록한다.
3. `BatchDate -> days_ago -> validate_dates` 계약이 실제로 어떻게 계산되는지 분명히 적는다.
4. 실서버 latest artifact를 기준으로 현재 Step 2가 어떻게 동작하는지 관측값을 남긴다.

중요:

- 이 문서는 `2026-04-07` 시점의 코드와 실서버 관측 기준이다.
- 범위는 `Step 2`에 집중한다.
- Step 1은 이미 완료 판정 후 닫힌 상태로 전제한다.
- Step 3 이후는 이번 문서에서는 preview 수준으로만 다룬다.


## 1. True Entrypoint

`Step 2`의 직접 실행 주체는 아래 wrapper call이다.

- outer wrapper:
  - `scripts/daily_champion_challenger_v5_for_server.ps1`
- concrete worker:
  - `scripts/run_raw_ticks_daily.ps1`

wrapper가 실제로 하는 일:

1. `resolvedBatchDate` 계산
2. `resolvedRawTicksDailyScript` 존재 여부 확인
3. 아래 인자로 child PowerShell script 실행

- `-ProjectRoot <resolvedProjectRoot>`
- `-PythonExe <resolvedPythonExe>`
- `-BatchDate <resolvedBatchDate>`
- optional `-DryRun`

즉 Step 2는 Step 1과 다르게
wrapper가 계산한 `BatchDate`를 실제로 직접 받는다.

### 1.1 Current topology note

현재 v5 split challenger 운영 경로에서는
Step 2가 standalone timer owner가 아니라
`00:20` chain-owned step으로 읽히는 것이 맞다.

근거:

- v5 split installer는 disable legacy timer 목록에
  - `autobot-raw-ticks-daily.timer`
  를 포함한다
- standalone raw-ticks installer 기본값은
  - `OnCalendar = *-*-* 00:00:00`
  - lock file = `/tmp/autobot-raw-ticks-daily.lock`
- 실서버 확인값:
  - `autobot-raw-ticks-daily.timer`
    - `is-enabled = disabled`
    - `is-active = inactive`
    - unit file은 남아 있음

즉 현재 Step 2는
"독립 00:00 ticks timer"
가 아니라
"00:20 nightly chain 내부 raw ticks step"
으로 해석하는 것이 더 맞다.


## 2. Role

Step 2의 역할은:

- `BatchDate`를 기준으로 Upbit REST `trades/ticks`의 `days_ago` 요청 범위를 계산
- top-N KRW market 기준 raw ticks collection plan 생성
- raw ticks를 date/market partition으로 수집
- 날짜별 validate 수행
- Step 3가 freshness와 batch coverage를 확인할 수 있는 summary를 남김

즉 Step 2는 Step 1처럼 rolling source-refresh 느낌이 아니라,
상대적으로 더 강한 `batch-date-aware daily collection` 단계다.


## 3. Wrapper Entrance Condition

`scripts/daily_champion_challenger_v5_for_server.ps1` 기준 Step 2는 아래 조건에서만 실행된다.

### 3.1 Outer mode gate

- `if ($Mode -ne "promote_only")`

즉 `spawn_only`에서는 실행되고,
`promote_only`에서는 실행되지 않는다.

### 3.2 Script existence gate

- `if (Test-Path $resolvedRawTicksDailyScript)`

기본 resolved path:

- `scripts/run_raw_ticks_daily.ps1`

이 조건이 false이면:

- Step 2는 실패가 아니라 skip
- wrapper는 다음 단계로 진행


## 4. Wrapper Inputs To Step 2

wrapper가 실제로 Step 2에 넘기는 값:

- `ProjectRoot`
- `PythonExe`
- `BatchDate`
- optional `DryRun`

Step 1과 다른 점:

- `BatchDate`가 실제로 child script에 전달된다
- 따라서 Step 2는 wrapper-level operating date contract를 직접 소비한다

wrapper가 넘기지 않는 값:

- `Quote`
- `TopN`
- `DaysAgo`
- `DaysAgoCsv`
- `RawRoot`
- `MetaDir`
- `Workers`
- `MaxPagesPerTarget`
- `RateLimitStrict`

이 값들은 모두 Step 2 script 내부 default에 맡긴다.


## 5. Step 2 Script Parameters

`scripts/run_raw_ticks_daily.ps1` parameter list:

- `ProjectRoot`
- `PythonExe`
- `BatchDate`
- `SummaryPath`
  - default: `data/raw_ticks/upbit/_meta/ticks_daily_latest.json`
- `PlanPath`
  - default: `data/raw_ticks/upbit/_meta/ticks_plan_daily_auto.json`
- `Quote`
  - default: `KRW`
- `TopN`
  - default: `50`
- `DaysAgo`
  - default: `1`
- `DaysAgoCsv`
  - default: `""`
- `RawRoot`
  - default: `data/raw_ticks/upbit/trades`
- `MetaDir`
  - default: `data/raw_ticks/upbit/_meta`
- `Workers`
  - default: `1`
- `MaxPagesPerTarget`
  - default: `50`
- `RateLimitStrict`
  - default: `true`
- `DryRun`


## 6. Step 2 Derived Variables

### 6.1 `resolvedProjectRoot`

- empty면 default project root
- 아니면 full path

### 6.2 `resolvedPythonExe`

- empty면 default python exe
- 아니면 입력값

### 6.3 `resolvedSummaryPath`

- `ticks_daily_latest.json` 최종 출력 위치

### 6.4 `resolvedPlanPath`

- `ticks_plan_daily_auto.json` 최종 출력 위치

### 6.5 `resolvedRawRoot`

- raw ticks compressed jsonl partition root
- 기본:
  - `data/raw_ticks/upbit/trades`

### 6.6 `resolvedMetaDir`

- ticks collect/validate 메타 디렉터리
- 기본:
  - `data/raw_ticks/upbit/_meta`

### 6.7 `resolvedBatchDate`

- empty면 local yesterday
- 아니면 입력값 trim

중요:

- 여기서 쓰는 `Get-Date`는 local timezone 기준이다
- 현재 OCI host timezone은 `Asia/Seoul (KST, +0900)`로 확인됐다
- 즉 nightly actual operating date는 server local date 기준으로 해석된다

### 6.8 `daysAgoSpec`

이 변수는 Step 2 핵심 계약이다.

계산 helper:

- `Resolve-DaysAgoSpec(SingleDay, DaysAgoCsvText, BatchDateText)`

우선순위:

1. `BatchDateText`가 있으면
   - `todayLocal - batchDate`
   - 그 차이를 일수로 계산
   - 최소 1로 clamp
2. 아니면 `DaysAgoCsv`
3. 아니면 `DaysAgo`

즉 wrapper가 `BatchDate`를 넘기는 현재 nightly 경로에서는
`DaysAgo` / `DaysAgoCsv` 기본값보다
`BatchDate`가 우선한다.

### 6.9 `daysAgoValues`

- `daysAgoSpec`를 comma split
- trim
- int cast
- 최소 1 clamp
- unique sort

즉 최종적으로는 integer list가 된다.

### 6.10 `validateDates`

이 변수도 매우 중요하다.

계산식:

- `(Get-Date).ToUniversalTime().AddDays(-1 * daysAgo).ToString("yyyy-MM-dd")`

즉:

- `days_ago`는 local-date delta로 계산될 수 있지만
- `validate_dates`는 **UTC 날짜 문자열**로 계산된다

이건 Step 2의 가장 중요한 time-contract 포인트 중 하나다.

빠지지 말아야 할 해석:

- server local timezone이 `Asia/Seoul`
- nightly 실행 시각이 `00:20 KST`
인 현재 구조에서는
`BatchDate = local yesterday`와
`UTC now - days_ago`가
보통 같은 날짜로 수렴할 가능성이 높다.

하지만 이건 구현상 **항상 같은 기준을 쓰는 것**은 아니다.

- `days_ago` 계산은 local date 기준
- `validate_dates` 계산은 UTC date 기준

즉 실행 시각/타임존 조건이 달라지면
batch date와 validate date가 엇갈릴 수 있다.

현재 운영 topology에서 가장 중요한 concrete example:

- 실행 시각:
  - `2026-04-07 00:20 KST`
- 이때 UTC:
  - `2026-04-06 15:20 UTC`
- default `BatchDate`:
  - local yesterday
  - `2026-04-06`
- `days_ago`:
  - `todayLocal(2026-04-07) - batchDate(2026-04-06) = 1`
- `target_date` / `validate_dates`:
  - `UTC now - 1 day`
  - `2026-04-05`

즉 현재 Step 2 nightly contract는 실제로:

- local operating date:
  - `batch_date = 2026-04-06`
- UTC raw tick target date:
  - `target_date = 2026-04-05`

를 동시에 다룬다.

간단한 시간 시나리오 비교:

- `00:20 KST`
  - `validate_date = 2026-04-05`
- `08:00 KST`
  - 여전히 UTC는 전날 밤이라
  - `validate_date = 2026-04-05`
- `12:00 KST`
  - UTC date가 local과 같아지며
  - `validate_date = 2026-04-06`

즉 local/UTC 혼재는
특히 현재 `00:20` 실행 시각에서 가장 뚜렷하게 드러난다.


## 7. Step 2 Internal Branches

### 7.1 Dry-run branch

`Invoke-ProjectPythonStep()` 내부:

- `if ($DryRun)`

이면 실제 Python 호출을 생략하고:

- `step`
- `command`
- `exit_code = 0`
- `dry_run = true`
- `output_preview = ""`

만 남긴다.

### 7.2 DaysAgo resolution branch

`Resolve-DaysAgoSpec()`는 세 갈래다.

- `BatchDate` 우선
- 없으면 `DaysAgoCsv`
- 둘 다 없으면 `DaysAgo`

nightly path에서는 사실상
`BatchDate -> days_ago` 경로만 사용된다.

### 7.3 Validate fan-out branch

Step 2는 Python 호출이 정확히 두 번만 있는 것이 아니다.

실행 순서:

1. `plan_raw_ticks_daily`
2. `collect_raw_ticks_daily`
3. `validate_raw_ticks_<date>` for each validate date

즉 `daysAgoValues` 길이에 따라 validate step 개수가 늘어난다.


## 8. Step 2 Actual Calls

### 8.1 Plan step

step name:

- `plan_raw_ticks_daily`

CLI:

- `python -m autobot.cli collect plan-ticks`

핵심 args:

- `--parquet-root data/parquet`
- `--base-dataset candles_api_v1`
- `--out <resolvedPlanPath>`
- `--quote KRW`
- `--market-mode top_n_by_recent_value_est`
- `--top-n 50`
- `--days-ago <daysAgoSpec>`

중요:

- Step 2 planner는 base dataset으로 `candles_api_v1`를 본다
- 즉 tick daily market universe는 현재 Step 1이 갱신하는 캔들 레이어를 기준으로 선택된다

### 8.2 Collect step

step name:

- `collect_raw_ticks_daily`

CLI:

- `python -m autobot.cli collect ticks`

핵심 args:

- `--plan <resolvedPlanPath>`
- `--mode daily`
- `--quote KRW`
- `--top-n 50`
- `--days-ago <daysAgoSpec>`
- `--raw-root <resolvedRawRoot>`
- `--meta-dir <resolvedMetaDir>`
- `--rate-limit-strict true`
- `--workers 1`
- `--max-pages-per-target 50`
- `--dry-run false`

### 8.3 Validate step(s)

step name shape:

- `validate_raw_ticks_<YYYY-MM-DD>`

CLI:

- `python -m autobot.cli collect ticks validate`

핵심 args:

- `--date <validateDate>`
- `--raw-root <resolvedRawRoot>`
- `--meta-dir <resolvedMetaDir>`


## 9. Python Routing

### 9.1 `plan-ticks`

`autobot.cli` path:

1. `main()`
2. `_handle_collect_command()`
3. `_handle_collect_plan_ticks()`
4. `generate_ticks_collection_plan()`

핵심 handler normalization:

- `base_dataset`
- `quote`
- `market_mode`
- `top_n`
- `days_ago`
- `resolve_active_markets = true`

`_parse_days_ago_csv()`는:

- `1..7` 범위만 허용
- 중복 제거
- sort

즉 Step 2 Python layer에서도 `days_ago`는 강하게 bounded 된다.

### 9.2 `collect ticks`

`autobot.cli` path:

1. `main()`
2. `_handle_collect_command()`
3. `_handle_collect_ticks()`
4. `collect_ticks_from_plan()`
5. `validate_ticks_raw_dataset()` if not dry-run

여기서도 중요한 normalization:

- `mode = daily`
- `default_days_ago = (1,)` for daily mode
- `days_ago` 다시 `_parse_days_ago_csv()`로 정규화
- `base_dataset`는 default로 `candles_v1`지만
  current Step 2 wrapper call은 explicit plan을 넘기므로
  collect phase는 사실상 plan 파일을 주 소비자로 사용한다

helper `_load_or_build_plan()` 의미:

- `plan_path`가 이미 존재하면 그 JSON을 그대로 사용
- 없으면 `TicksPlanOptions`로 즉석 build

즉 current nightly path에서는:

- wrapper-level `plan_raw_ticks_daily`가 먼저 파일을 기록하고
- collect는 그 same-run plan file을 읽는 구조다

### 9.4 `target_date` actual derivation

`collect_ticks_from_plan()` 내부 target date는
각 detail마다 `_expected_target_date(days_ago)`로 다시 계산한다.

계산식:

- `datetime.now(timezone.utc).date() - timedelta(days=days_ago)`

즉 collect 단계의 target date는
`BatchDate`를 직접 읽지 않고
**UTC current date + days_ago**
조합으로 다시 계산한다.

즉 현재 nightly path에서는:

- wrapper-level plan step이 먼저 파일을 기록하고
- collect는 그 same-run plan file을 읽는 구조다

### 9.3 `ticks validate`

`autobot.cli` path:

1. `main()`
2. `_handle_collect_command()`
3. `_handle_collect_ticks()`
4. `validate_ticks_raw_dataset(date_filter=<validateDate>)`


## 10. Ticks Plan Logic

`generate_ticks_collection_plan()` 핵심:

- `market_mode` validation
- `days_ago` normalize
- `end_ts_ms = now(UTC)` if omitted
- `_select_markets()`
- selected market x each days_ago value로 targets 생성

plan constraints:

- endpoint:
  - `/v1/trades/ticks`
- `days_ago_supported_range = [1, 7]`
- `count_per_request = 200`
- rate-limit policy:
  - `rest_trade_group_rps = 10`
  - `remaining_req_enforced = true`

즉 plan 자체가 external API contract를 명시적으로 artifact에 남긴다.

market selection logic:

- base dataset inventory는 `build_candle_inventory()` 기반
- `top_n_by_recent_value_est`면 `estimate_recent_value_by_market()` 사용
- active market filter도 적용

여기서 active market filter semantics는
Step 1과 같은 helper를 재사용한다.

- `resolve_active_quote_markets()`
- `filter_markets_by_active_set()`

즉 active market resolution 실패 시
현재 구조는 기본적으로 fail-open 쪽에 가깝다.

- active market API가 unavailable이면
  active filter를 포기하고 candidate list를 그대로 통과시킬 수 있다

즉 Step 2 planner는 raw ticks 자체가 아니라
**캔들 inventory/value estimate**를 이용해 tick collection universe를 고른다.


## 11. Ticks Collect Logic

`collect_ticks_from_plan()` 핵심:

1. existing plan load or build
2. targets 추출
3. checkpoint load
4. target별 `fetch_trades_ticks()`
5. raw ticks `.jsonl.zst` partition write
6. manifest append
7. checkpoint update
8. retention prune
9. collect report write

### 11.1 Daily mode semantics

`TicksCollectOptions.mode = daily`일 때:

- Python handler default `days_ago`는 `(1,)`
- Step 2 wrapper도 nightly path에서 사실상 `days_ago=1`을 기대한다

중요:

- 이 단계는 기본적으로 "어제 하루"를 daily 대상처럼 다루지만
- raw ticks API는 `days_ago` 상대 지정 방식이라
- Step 1처럼 절대 시각 범위를 직접 요청하는 구조는 아니다

즉 Step 2가 실제로 하는 일은
"batch_date의 raw ticks를 절대 날짜로 직접 지정 수집"이 아니라
"batch_date로부터 도출한 days_ago에 대응하는 UTC target date를 상대 조회"
에 더 가깝다.

### 11.2 Checkpoint semantics

이 단계의 핵심은 checkpoint다.

checkpoint path:

- `data/raw_ticks/upbit/_meta/ticks_checkpoint.json`

key shape:

- `market|target_date`

checkpoint entry 주요 필드:

- `market`
- `days_ago`
- `target_date`
- `last_cursor`
- `last_success_ts_ms`
- `pages_collected`
- `updated_at_ms`

즉 Step 2는 단순 daily snapshot이 아니라
**resume-aware collector**다.

이 점은 Step 1과 구조적으로 다르다.

- Step 1 candle API는 partition merge/update 중심
- Step 2 ticks daily는 `target_date + cursor` resume 중심

즉 동일 `days_ago=1` 실행이라도
이미 같은 target_date에 대해 충분히 수집됐으면
0 row가 나오는 것이 이상하지 않을 수 있다.

실서버 checkpoint 실제 예시:

- `KRW-0G|2026-04-05`
  - `last_cursor = 17753472093450000`
  - `pages_collected = 20`
- `KRW-AQT|2026-04-05`
  - `last_cursor = 17753472058480000`
  - `pages_collected = 3`
- `KRW-BTC|2026-04-05`
  - `last_cursor = 17754299547860000`
  - `pages_collected = 100`

즉 최신 warning target 상당수는
"처음 수집"이 아니라
"이미 checkpoint가 있는 재실행 target"
이다.

추가로 매우 중요한 점:

- `collect_ticks_from_plan()`는 result를 받은 뒤
  `NO_ROWS_COLLECTED` warning이어도
  `update_ticks_checkpoint()`를 호출한다
- 즉 `last_success_ts_ms`와 `updated_at_ms`는
  "새 row를 실제로 썼다"는 뜻이 아니라
  "이 target 처리 루프가 예외 없이 끝났다"에 더 가깝다
- `pages_collected`도 latest run value가 아니라
  기존 checkpoint 값에 이번 run 값을 더하는 **누적값**이다

즉 checkpoint 해석 시:

- `pages_collected = 100`
  는 "이번 run에서 100 page"
  가 아니라
  "누적 100 page"
  일 수 있다

이건 Step 2 운영 해석에서 매우 중요하다.

checkpoint update behavior의 핵심:

- `NO_ROWS_COLLECTED` warning이어도
  `update_ticks_checkpoint()`가 호출된다
- 즉 `last_success_ts_ms`는
  "새 tick row를 실제로 썼다"
  보다
  "이 target 처리 루프가 예외 없이 끝났다"
  에 더 가깝다

따라서 checkpoint는 strict completeness ledger라기보다
resume cursor/state ledger로 읽는 것이 맞다.

### 11.3 Per-target status semantics

collect detail status:

- `OK`
- `WARN`
  - `NO_ROWS_COLLECTED`
  - `PAGING_LOOP_GUARD_TRIGGERED`
  - `MAX_REQUEST_BUDGET_REACHED`
  - `DELISTED_OR_INACTIVE_MARKET`
- `FAIL`
  - `RATE_LIMIT_EXCEPTION`
  - `RATE_LIMIT_BANNED_418`
  - `COLLECT_EXCEPTION`

### 11.4 Resume / no-new-data interpretation

현재 실서버 최신 artifact에서 매우 중요한 관측:

- `warn_targets = 47 / 49`
- 그중 `NO_ROWS_COLLECTED = 41`
- 다수 warning detail에 이미 `start_cursor`가 들어 있음
- `pages_collected = 0`
- `truncated_by_budget = false`

이 패턴은 단순 missing-data보다
`checkpoint cursor 이후 새 row가 없는 상태`
일 가능성이 높다.

즉 Step 2의 `NO_ROWS_COLLECTED`는 항상 bad signal이 아니라,
이미 해당 UTC date/market target에 대해 수집이 진행돼
**새 체결이 더 없어서 0 row인 resume/no-new-data**
일 수도 있다.

이 점은 Step 2를 해석할 때 매우 중요하다.

실서버 latest warn sample들이 바로 이 패턴을 보여준다.

- `KRW-0G`
- `KRW-ADA`
- `KRW-AERGO`
- `KRW-API3`
- `KRW-AQT`

등은 공통적으로:

- `start_cursor` present
- `pages_collected = 0`
- `rows_fetched_raw = 0`
- `rows_fetched_unique = 0`
- `truncated_by_budget = false`

즉 이 warning은
"collector가 고장"이라기보다
"resume 이후 더 수집할 tick이 없었다"
로 읽는 것이 더 자연스럽다.

실서버 latest `NO_ROWS_COLLECTED` 41건 전체를 다시 분류한 결과도
이 해석을 강하게 지지한다.

- total: `41`
- `with_start_cursor = 41`
- `with_checkpoint_entry = 41`
- `pages_collected_zero = 41`
- `rows_zero = 41`
- `without_start_cursor = 0`

즉 latest run 기준 `NO_ROWS_COLLECTED`는
적어도 이번 41건에 한해
"체크포인트 없는 신규 수집 실패"
보다
"기존 checkpoint가 있는 resume/no-new-data"
로 읽는 것이 훨씬 자연스럽다.

단, 여기서도 아직 말하면 안 되는 것:

- 모든 `NO_ROWS_COLLECTED`가 무조건 정상이라는 주장

왜냐하면 현재 artifact만으로는

- 진짜 no-new-data
- 잘못된 cursor
- date 해석 mismatch

를 완전히 구분하지는 못하기 때문이다.

### 11.5 Request budget semantics

`UpbitTicksClient.fetch_trades_ticks()`는:

- `max_pages`
- `max_requests`
둘 다 지원한다.

현재 Step 2 wrapper는:

- `max-pages-per-target = 50`
- global `max_requests`는 wrapper에서 직접 안 넘김

최신 서버 artifact에서는

- `MAX_REQUEST_BUDGET_REACHED = 6`
- 이 warning target들은
  - `pages_collected = 50`
  - `calls_made = 55`
  - `truncated_by_budget = true`

즉 여기서의 budget은
실질적으로 per-target `max_pages_per_target=50` 제약에 먼저 걸린 케이스로 읽는 게 맞다.

실제 target 예시:

- `KRW-BERA`
- `KRW-BTC`
- `KRW-ETH`
- `KRW-KITE`
- `KRW-SOL`
- `KRW-XRP`

공통 shape:

- `rows_fetched_unique = 10000`
- `pages_collected = 50`
- `calls_made = 55`
- `truncated_by_budget = true`

즉 이 warning은
"0 row/no data"가 아니라
"활발한 시장이라 per-target page budget에서 잘렸다"
는 뜻이다.

### 11.6 Selected vs processed semantics

`collect_ticks_from_plan()`에서도
`selected_targets`는 거의 `discovered_targets`와 같이 움직인다.

- `selected_targets = discovered_targets`
- `max_requests <= 0`인 특수 case만 `selected_targets = 0`

따라서 실제 진도 해석은:

- `processed_targets`
- `warn_targets`
- `fail_targets`
- `calls_made`
- `rows_collected_total`
- `no_rows_targets_count`

를 같이 봐야 한다.

실서버 latest run에서는:

- `selected_targets = 49`
- `processed_targets = 49`

이라 selected/processed mismatch는 없었다.

즉 이번 latest run에서 noisy함의 원인은
미처리 target이 아니라
warning-heavy target mix였다.


## 12. Tick Raw Storage / Manifest / Validate

### 12.1 Raw partition writer

`write_ticks_partitions()`:

- output format:
  - compressed `jsonl.zst`
- partition root:
  - `raw_root/date=<UTC_DATE>/market=<MARKET>/part-<run_id>.jsonl.zst`
- partitioning key:
  - `timestamp_ms`의 UTC date
  - market
- collision 시:
  - `part-<run_id>-0001.jsonl.zst`
  형태로 증가

중요:

- Step 1 candle layer는 parquet merge/update였지만
- Step 2 raw ticks는 append-style compressed jsonl partition이다
- writer는 기존 part를 merge하지 않고
  같은 run의 새 part file을 추가하는 방식이다

즉 Step 2 raw storage는
"latest snapshot overwrite"
가 아니라
"immutable raw append accumulation"
에 더 가깝다.

### 12.2 Ticks manifest

manifest path:

- `data/raw_ticks/upbit/_meta/ticks_manifest.parquet`

주요 컬럼:

- `run_id`
- `date`
- `market`
- `days_ago`
- `rows`
- `min_ts_ms`
- `max_ts_ms`
- `dup_ratio`
- `status`
- `reasons_json`
- `calls_made`
- `pages_collected`
- `part_file`
- `error_message`
- `collected_at_ms`

manifest interpretation:

- status/reasons는 해당 part write 시점의 결과를 남긴다
- later run이 같은 date/market을 다시 수집해도
  기존 row를 덮지 않고 새 row를 append한다

즉 Step 2 manifest도 current-state SSOT라기보다
run-by-run raw collection ledger 성격이 강하다.

### 12.3 Ticks validate

`validate_ticks_raw_dataset()` 핵심:

- date filter 있으면 그 UTC date partition만 검사
- 없으면 전체 raw_root 검사
- 각 row에서 schema validation
- `days_ago`는 `1..7`이어야 함
- duplicate ratio 계산

status rules:

- `FAIL`
  - `READ_OR_PARSE_EXCEPTION`
  - `SCHEMA_INVALID`
- `WARN`
  - `NO_ROWS_COLLECTED`
  - `PARTITION_MISMATCH`
  - `DUP_RATIO_HIGH`
- `OK`
  - 그 외

중요:

- collect command 내부 validate는 `date_filter=None`
  - 즉 raw root 전체 validate
- wrapper의 explicit validate fan-out는
  - `date_filter=<validateDate>`
  - 즉 날짜별 validate

즉 Step 2는
하나의 collect command 내부 full validate와
wrapper-level date-specific validate가
둘 다 존재한다.

이중 validate 구조의 해석:

- collect 내부 full validate:
  - raw root 전체 health
- wrapper-level validate fan-out:
  - 이번 nightly batch가 겨냥한 UTC date 확인

즉 downstream summary에는 날짜 fan-out step이 남지만,
실제 raw root 전체 validate도 collect 내부에서 이미 한 번 돌고 있다.

테스트가 보장하는 현재 contract:

- offline fixture collect + validate success
- `/v1/trades/ticks` 404는 `DELISTED_OR_INACTIVE_MARKET` warning 처리
- checkpoint key는 `market|target_date`를 지원

즉 date-aware checkpoint key와 delisted warning path는
현재 test coverage에도 일부 반영돼 있다.

validate interpretation 핵심:

- collect report의 warning과
- validate report의 warning/fail

은 서로 다른 층위다.

예를 들어 latest run에서는:

- collect:
  - `warn_targets = 47`
- validate:
  - `warn_files = 0`
  - `fail_files = 0`

즉 Step 2에서는
"API fetch semantics noisy"
와
"현재 raw files schema/dup integrity clean"
이 동시에 성립할 수 있다.


## 13. Summary Artifact

`ticks_daily_latest.json` top-level fields:

- `policy = raw_ticks_daily_v1`
- `generated_at_utc`
- `project_root`
- `python_exe`
- `raw_root`
- `meta_dir`
- `plan_path`
- `batch_date`
- `days_ago`
- `validate_dates`
- `steps`

즉 Step 2 summary는 Step 1보다 강하게
`batch_date`, `days_ago`, `validate_dates`를 top-level에 남긴다.

중요한 한계:

- 이 summary는 top-level `run_id`를 남기지 않는다
- `started_at`, `finished_at`도 없다

즉 summary만으로는
"현재 latest `ticks_collect_report.json`와 정확히 같은 실행인지"
를 강하게 연결하기 어렵다.

중요한 한계:

- 이 summary는 top-level `run_id`를 남기지 않는다
- `started_at`, `finished_at`도 없다

즉 summary만으로는
"현재 latest `ticks_collect_report.json`와 정확히 같은 실행인지"
를 강하게 연결하기 어렵다.


## 14. Downstream Consumer

`scripts/close_v5_train_ready_snapshot.ps1`는 Step 2를 `Get-SourceFreshnessResult()`로 읽는다.

Step 2 쪽 호출 shape:

- `SummaryFile = data/raw_ticks/upbit/_meta/ticks_daily_latest.json`
- `MaxAgeMinutes = 120`
- `BatchDateValue = <resolved batch date>`

Step 2 summary에서 downstream이 실제로 보는 것:

- `generated_at_utc`
- `steps[*].exit_code`
- `batch_date`
  또는
- `validate_dates`
  또는
- `validate_raw_ticks_<batchDate>` step 존재 여부

즉 Step 2는 Step 1과 달리
batch-date coverage가 downstream contract에 직접 걸린다.


## 15. Official Upbit Contract Relevant To Step 2

직접 관련 공식 문서:

- [최근 체결 내역 조회](https://docs.upbit.com/kr/kr/reference/recent-trades-history)
- [요청 수 제한(Rate Limits)](https://docs.upbit.com/kr/kr/reference/rate-limits)

현재 공식 계약상 중요한 사실:

- endpoint:
  - `GET https://api.upbit.com/v1/trades/ticks`
- 지정한 페어의 최근 체결 목록 조회
- revision history에 `조회 기간 7일 확대 지원` 명시
- `days_ago` 지원 범위:
  - `1..7`
- trade group rate limit:
  - 초당 최대 `10`회
  - IP 단위
- `Remaining-Req` 헤더로 잔여 요청 수 확인
- `429`
  - 초당 제한 초과
- `418`
  - 과도 반복 시 차단

repo-local Step 2 대응:

- `_parse_days_ago_csv()`와 `UpbitTicksClient`가 `1..7`를 강제
- page size는 `200`
- local rate limiter는 `trade` group `10 rps` 전제
- `Remaining-Req`, `429`, `418` 처리도 공통 Upbit client가 담당


## 16. Observed Live Server Snapshot On 2026-04-07

실서버 latest artifact:

- `ticks_daily_latest.json`
  - `generated_at_utc = 2026-04-06T07:45:39.9479274Z`
  - `batch_date = 2026-04-05`
  - `days_ago = [1]`
  - `validate_dates = ['2026-04-05']`
  - `steps_count = 3`
  - top-level `run_id` 없음

- `ticks_plan_daily_auto.json`
  - `selected_markets = 49`
  - `targets = 49`
  - `days_ago_count = 1`

- `ticks_collect_report.json`
  - `run_id = 20260406T203011Z`
  - `discovered_targets = 49`
  - `selected_targets = 49`
  - `processed_targets = 49`
  - `ok_targets = 2`
  - `warn_targets = 47`
  - `fail_targets = 0`
  - `calls_made = 442`
  - `rows_collected_total = 71485`
  - `no_rows_targets_count = 41`

- `ticks_validate_report.json`
  - `checked_files = 57`
  - `ok_files = 57`
  - `warn_files = 0`
  - `fail_files = 0`
  - `schema_ok_ratio = 1.0`
  - `dup_ratio_overall = 0.0`

추가 해석:

- `checked_files = 57`가 `targets = 49`보다 큰 이유는
  validate가 target 수를 세는 것이 아니라
  raw root 아래 실제 `.jsonl.zst` part file 개수를 검사하기 때문이다
- raw ticks writer는 append part 방식이므로
  target 수와 file 수가 1:1일 필요가 없다

warning reason breakdown in latest collect report:

- `NO_ROWS_COLLECTED = 41`
- `MAX_REQUEST_BUDGET_REACHED = 6`

중요한 현재 해석:

- Step 2 collect는 매우 noisy하다
- 하지만 validate는 clean하다
- 특히 많은 `NO_ROWS_COLLECTED`는
  `start_cursor`가 이미 있는 상태에서 `pages_collected = 0`이라
  실패라기보다 `resume/no-new-data` 성격일 가능성이 높다
- `MAX_REQUEST_BUDGET_REACHED`는
  활발 시장에서 `max_pages_per_target=50`에 걸린 case로 읽는 것이 더 자연스럽다
- 독립 `autobot-raw-ticks-daily.timer`는 현재 disabled/inactive라
  latest Step 2는 chain-owned 수집으로 보는 해석이 맞다

추가로 매우 중요한 관측:

- `ticks_daily_latest.json`의 embedded `collect_raw_ticks_daily.output_preview`
  와
- 현재 파일 `ticks_collect_report.json`

내용이 서로 다를 수 있다.

실서버 현재 관측에서는:

- summary embedded preview는
  - `ok=41 warn=8 fail=0 calls=1098 rows=187126`
- 현재 `ticks_collect_report.json` latest는
  - `ok=2 warn=47 fail=0 calls=442 rows=71485`

즉 Step 2 해석 시:

- `ticks_daily_latest.json`는 "summary를 쓴 그 실행의 요약"
- `ticks_collect_report.json`는 "현재 latest collect report file"

로 따로 봐야 한다.

둘을 같은 run이라고 단정하면 안 된다.

drift가 생길 수 있는 현재 코드 근거:

- `run_raw_ticks_daily.ps1`는 `ticks_daily_latest.json`를 쓴다
- 반면 `python -m autobot.cli collect ticks ...` direct 호출은
  `ticks_collect_report.json`만 갱신하고
  `ticks_daily_latest.json`는 갱신하지 않는다
- `daily_micro_pipeline.ps1`와 `daily_micro_pipeline_for_server.ps1`는
  direct `collect ticks` command를 사용한다

즉 현재 topology에서는
chain-owned Step 2 summary 이후에도
다른 경로가 latest collect report를 덮을 수 있다.

추가 관측:

- historical 문서에는 `ticks_runs_summary.json`가 언급되지만
- 현재 repo code search 기준 Step 2 raw ticks path에서
  이 파일을 갱신하는 active code path는 보이지 않는다

즉 현재 운영 해석에서는

- `ticks_daily_latest.json`
- `ticks_collect_report.json`
- `ticks_validate_report.json`
- `ticks_manifest.parquet`
- `ticks_checkpoint.json`

이 주 artifact들이고,
`ticks_runs_summary.json`는 historical artifact로 남아 있을 가능성을 염두에 두는 편이 안전하다.

Step 3 입구 해석상 의미:

- `train_snapshot_close`는 Step 2 latest collect report를 직접 읽지 않고
  `ticks_daily_latest.json` summary를 본다
- 따라서 Step 3가 이해하는 Step 2 상태와
  operator가 최신 `ticks_collect_report.json`를 보고 이해하는 Step 2 상태가
  서로 다를 수 있다

즉 Step 3 입구 전까지의 해석에서
가장 조심해야 하는 함정 중 하나가
바로 이 `summary-vs-latest-report drift`다.

현재 repo/test contract와의 연결:

- `close_v5_train_ready_snapshot.ps1`는
  `ticks_daily_latest.json` summary만 읽는다
- candidate acceptance 관련 seed contract들도
  대부분 `source_freshness.raw_ticks_daily = { pass, batch_date, batch_covered }`
  형태의 summary-level 요약만 가정한다

즉 현재 downstream consumer 계층은
Step 2 collect report latest drift를 직접 흡수하지 않고
summary-level batch coverage만 신뢰하는 구조에 더 가깝다.


## 17. Current Confidence Boundary

이 문서로 확실히 말할 수 있는 것:

- Step 2는 Step 1과 달리 batch-date-aware 단계다
- `BatchDate -> days_ago -> validate_dates` 계약이 실제로 존재한다
- `plan-ticks -> collect ticks -> validate ticks` fan-out path가 실제 실행 구조다
- `close_v5_train_ready_snapshot`는 Step 2에 대해 batch coverage를 직접 본다
- latest 실서버 Step 2는 수집 자체는 동작하지만 collect warning이 매우 많다
- 그 warning은 현재 artifact shape상 상당 부분 `resume/no-new-data` 또는 `per-target page budget`과 연결된다
- Step 2에는 local-date batch contract와 UTC validate-date contract가 혼재한다
- Step 3 입구 해석은 summary-level batch coverage에 크게 의존하고,
  latest collect report 상태와는 drift할 수 있다

이 문서로 아직 말하면 안 되는 것:

- Step 2 warning 47건이 전부 정상이라고 단정하는 주장
- Step 2가 전체 raw tick completeness를 강하게 보장한다는 주장
- Step 3 이후 전체 정합성이 이미 충분하다는 주장
- local/UTC date 혼재와 summary/report drift가 실제 운영에서 harmless하다고 단정하는 주장

그건 다음 판독과 이후 단계에서 계속 확인해야 한다.


## 18. Conservative Completion Check

현재 시점에서 마지막으로 남았던 두 의문:

1. local `BatchDate` vs UTC `validate_dates`
2. `ticks_daily_latest.json` vs `ticks_collect_report.json` drift

에 대한 보수적 판단은 아래와 같다.

### 18.1 Local/UTC date mixed contract

판단:

- 구현상으로는 latent risk가 맞다
- 하지만 현재 실제 운영 시각인 `00:20 KST` path에서는
  latest observed artifact가
  - `batch_date = 2026-04-05`
  - `validate_dates = ['2026-04-05']`
  - `validate_raw_ticks_2026-04-05`
  로 맞물려 있어
  즉시 모순이 드러난 상태는 아니다

즉:

- 설계상 완전히 아름답다고 말할 수는 없지만
- 현재 `00:20` 운영 해석을 막는 blocker라고도 보기 어렵다

### 18.2 Summary/report drift

판단:

- 운영 해석상 주의점은 맞다
- 하지만 Step 3 consumer는 애초에 `ticks_daily_latest.json` summary를 읽도록 설계돼 있고,
  current downstream tests도 summary-level `source_freshness.raw_ticks_daily`
  구조를 전제로 한다
- 즉 이것은
  "Step 2 내부가 이해 불가능한 상태"
  보다는
  "operator가 latest collect report를 볼 때 혼동할 수 있는 상태"
  에 더 가깝다

즉:

- 지금 문서화/판독 관점에서는 blocker가 아니다
- 다만 이후 운영 가시성 개선이 필요할 수 있다

### 18.3 Final judgment for Step 2

현재 기준으로 Step 2는:

- 진입점
- wrapper call
- `BatchDate -> days_ago -> validate_dates`
- Python routing
- planner / collect / validate / checkpoint / writer / manifest / summary
- downstream consumer
- 실서버 latest artifact 해석

까지 열려 있다.

그리고 지금 남은 불확실성은
즉시 추가 구현 없이도
문서/운영 판독 수준에서 관리 가능한 범위로 좁혀졌다.

따라서 보수적으로 보더라도
**Step 2는 여기서 완료 판정 가능**하다.
