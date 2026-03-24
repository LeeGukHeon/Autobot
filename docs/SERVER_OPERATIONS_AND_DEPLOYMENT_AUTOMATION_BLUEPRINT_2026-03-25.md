# SERVER OPERATIONS AND DEPLOYMENT AUTOMATION BLUEPRINT 2026-03-25

## 0. Executive Summary

이 문서는 실제 OCI 서버에 접속해서 확인한 runtime/deployment topology를 바탕으로, 현재 운영 자동화/배포/서비스 관리 구조를 분석하고 더 강한 운영 계약으로 가기 위한 설계 방향을 정리한 문서다.

이 문서는 아래 문서와 직접 연결된다.

- [FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md](/d:/MyApps/Autobot/docs/FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md)
- [INTEGRATED_STRONG_MODEL_SYSTEM_ROADMAP_2026-03-25.md](/d:/MyApps/Autobot/docs/INTEGRATED_STRONG_MODEL_SYSTEM_ROADMAP_2026-03-25.md)
- [OCI_SERVER_ACCESS_REFERENCE_2026-03-25.md](/d:/MyApps/Autobot/docs/OCI_SERVER_ACCESS_REFERENCE_2026-03-25.md)

Unless the user explicitly waives it, implementation-complete should be interpreted as:

- local code change complete
- local verification complete
- commit complete
- push complete
- direct OCI server access complete
- server `git pull --ff-only` complete
- direct OCI server validation complete
- reflected service or artifact state confirmed on the server

핵심 결론:

- 현재 서버 운영은 이미 systemd 기반 자동화 토대가 있다.
- 하지만 문서 상 목표 상태와 실서버 상태는 아직 다르다.
- 가장 중요한 운영 리스크는 `pointer/service/timer/state contract drift`다.

특히 이번 직접 접속에서 확인된 중요한 사실은 아래다.

1. `autobot-paper-v4.service`, `autobot-live-alpha-candidate.service`, `autobot-ws-public.service`, `autobot-dashboard.service`는 실제로 활성 상태다.
2. `autobot-v4-challenger-spawn.service`는 `oneshot`인데도 장시간 `activating (start)` 상태로 실행 중이었다.
3. `autobot-paper-v4-challenger.service`는 unit file은 있으나 실제 활성 상태가 아니었다.
4. `logs/model_v4_challenger/current_state.json`은 없었고, `latest.json`은 `promote_only`가 `NO_PREVIOUS_CHALLENGER_STATE`로 끝난 기록을 담고 있었다.
5. `champion`, `latest`, `latest_candidate`가 모두 같은 run id를 가리키고 있었다.
6. 서버에는 `Autobot_replay_627dacf` clone과 `autobot-paper-v4-replay.service`가 살아 있었지만, 사용자 확인에 따라 이 경로는 현재 운영 대상이 아니라 legacy/stale path로 취급해야 한다.

즉 현재 서버는 "완전자동 steady-state"라기보다:

- 자동화 조각은 존재하지만
- 상태기계가 아직 완전히 닫히지 않았고
- 운영 잔재와 임시 흐름이 같이 남아 있는

상태에 가깝다.


## 1. Confirmed Remote Findings

### 1.1 Host And Project Root

실제 SSH 접속으로 확인:

- host: `ubuntu@168.107.44.206`
- hostname: `instance-20260305-2024`
- project root: `/home/ubuntu/MyApps/Autobot`


### 1.2 Remote Repository State

실제 확인:

- HEAD: `745cfc62d77dfd643298dd1845eec05bad0ac284`
- remote: `origin git@github.com:LeeGukHeon/Autobot.git`
- worktree untracked:
  - `docs/reports/CANDIDATE_CANARY_REPORT_2026-03-13.md`
  - `scripts/restore_promote_override_once.sh`

운영 의미:

- server worktree is not perfectly clean
- deployment contract는 `git pull --ff-only`였지만, local untracked drift가 존재한다


### 1.3 MyApps Root Topology

실서버 `/home/ubuntu/MyApps`에서 확인:

- `Autobot`
- `Autobot_replay_627dacf`

중요:

- replay clone은 존재하지만, 사용자 확인에 따라 현재 운영 대상이 아니다.
- 따라서 이 clone은 target architecture가 아니라 cleanup/review candidate로 봐야 한다.


### 1.4 Confirmed Services

실제 `systemctl list-units 'autobot*' --type=service --all` 결과 기준:

- active:
  - `autobot-dashboard.service`
  - `autobot-live-alpha-candidate.service`
  - `autobot-paper-v4.service`
  - `autobot-paper-v4-replay.service`
  - `autobot-ws-public.service`
- inactive:
  - `autobot-live-alpha.service`
  - `autobot-live-execution-policy.service`
  - `autobot-storage-retention.service`
  - `autobot-v4-challenger-promote.service`
- activating:
  - `autobot-v4-challenger-spawn.service`

이 중 replay service는 current target path가 아니라 legacy/stale path로 취급해야 한다.


### 1.5 Confirmed Unit Files

실제 unit file 존재:

- `autobot-paper-v4.service`
- `autobot-paper-v4-challenger.service`
- `autobot-paper-v4-replay.service`
- `autobot-live-alpha.service`
- `autobot-live-alpha-candidate.service`
- `autobot-ws-public.service`
- `autobot-v4-challenger-spawn.service`
- `autobot-v4-challenger-promote.service`
- `autobot-live-execution-policy.service`
- `autobot-storage-retention.service`
- `autobot-v4-rank-shadow.service`

즉 unit file 자체는 꽤 많이 정비돼 있다.


### 1.6 Confirmed Timers

실제 timer 설정:

- `autobot-v4-challenger-promote.timer`
  - `OnCalendar=*-*-* 00:10:00`
- `autobot-v4-challenger-spawn.timer`
  - `OnCalendar=*-*-* 00:20:00`
- `autobot-live-execution-policy.timer`
  - `23:40:00`
- `autobot-storage-retention.timer`
  - `06:30:00`

운영 의미:

- promote가 spawn보다 먼저 돈다
- 이는 "전날 challenger promote 후 새 challenger spawn" 철학이라면 맞을 수 있다
- 하지만 state machine이 이를 전제로 매우 명확해야 한다


### 1.7 Confirmed Pointer State

실제 registry pointer:

- `train_v4_crypto_cs/champion.json`
- `train_v4_crypto_cs/latest.json`
- `train_v4_crypto_cs/latest_candidate.json`
- global `latest.json`
- global `latest_candidate.json`

모두 run id:

- `20260324T081937Z-s42-a6d6b2a5`

즉 현재는:

- champion
- latest
- latest_candidate

가 모두 동일 run을 가리킨다.

이는 정상 steady-state가 아니라 transitional/overloaded state다.


### 1.8 Confirmed Challenger State File Status

실제 확인:

- `logs/model_v4_challenger/current_state.json` 없음
- `logs/model_v4_challenger/latest.json` 존재

`latest.json` 내용상:

- mode=`promote_only`
- `NO_PREVIOUS_CHALLENGER_STATE`
- `challenger_previous={}`
- `challenger_next={}`

즉 현재 challenger orchestration은 intended state machine을 완전히 만족하지 못한 상태다.


### 1.9 Confirmed Spawn Service Runtime

실제 `systemctl status autobot-v4-challenger-spawn.service` 기준:

- `Active: activating (start)` 상태
- 1시간 이상 지속
- 메모리 약 `2.9G`
- 내부적으로:
  - `daily_champion_challenger_v4_for_server.ps1`
  - `v4_governed_candidate_acceptance.ps1`
  - `python -m autobot.cli model train ...`

이건 중요한 운영 신호다.

possible interpretations:

- long-running scheduled daily job로 의도된 것일 수 있음
- 하지만 `oneshot` 서비스 계약과의 긴장이 있음
- timer overlapping / stuck perception / operator confusion 가능성이 큼


## 2. Current Topology Reconstruction

### 2.1 Champion Paper

실제 unit:

- [autobot-paper-v4.service](/etc/systemd/system/autobot-paper-v4.service)

주요 특징:

- working dir: `/home/ubuntu/MyApps/Autobot`
- preset: `live_v4`
- runtime role: `champion`
- pinned model ref empty
- command:
  - `python -m autobot.cli paper alpha --duration-sec 0 --preset live_v4`

해석:

- champion paper는 pointer 기반 champion resolution에 의존한다.


### 2.2 Candidate Live

실제 unit:

- [autobot-live-alpha-candidate.service](/etc/systemd/system/autobot-live-alpha-candidate.service)

환경:

- `AUTOBOT_LIVE_MODEL_REF_SOURCE=latest_candidate_v4`
- `AUTOBOT_LIVE_MODEL_FAMILY=train_v4_crypto_cs`
- rollout mode: `canary`
- sync mode: `private_ws`
- state DB: `data/state/live_candidate/live_state.db`

해석:

- candidate live는 latest_candidate pointer에 직접 연결된다.
- 따라서 adoption/pointer semantics가 특히 중요하다.


### 2.3 WS Public Daemon

실제 unit:

- `autobot-ws-public.service`

command:

- `python -m autobot.cli collect ws-public daemon --quote KRW --top-n 50 --refresh-sec 900 --retention-days 14 --duration-sec 0`

해석:

- public market data plane이 독립 서비스로 운용된다.
- 이는 매우 좋다.


### 2.4 Challenger Spawn/Promote Flow

실제 oneshot unit:

- `autobot-v4-challenger-spawn.service`
- `autobot-v4-challenger-promote.service`

둘 다:

- `daily_champion_challenger_v4_for_server.ps1`
- mode만 `spawn_only` / `promote_only`

을 사용한다.

즉:

- orchestration은 script-centric
- systemd는 thin wrapper

형태다.


### 2.5 Legacy Replay Path

실제 활성 상태:

- `autobot-paper-v4-replay.service`
- `/home/ubuntu/MyApps/Autobot_replay_627dacf`

하지만 사용자 명시:

- `replay는 안써요`

따라서 이 경로는:

- current target architecture 아님
- docs/roadmap의 core path 아님
- cleanup or archival review 대상


## 3. Strengths Of The Current Ops Structure

### 3.1 systemd First-Class Runtime

서비스/타이머가 분리되어 있고, unit file 수준에서:

- working directory
- environment
- runtime role
- rollout target

이 명시된다.

이건 좋다.


### 3.2 Distinct Candidate State DB

candidate live가:

- `data/state/live_candidate/live_state.db`

를 별도로 쓰는 점은 매우 좋다.

champion과 candidate 상태가 뒤섞일 위험을 줄여 준다.


### 3.3 Challenger Logs Persist

`logs/model_v4_challenger` 아래에:

- daily loop reports
- archive
- manual override archive

가 남아 있어 forensic이 가능하다.


### 3.4 Timer-Based Automation Exists

manual-only 운영이 아니라 적어도 scheduled orchestration 토대는 이미 있다.


## 4. Current Weaknesses

### 4.1 Documented Target And Actual State Diverge

문서상 target:

- challenger paper
- canary live
- champion paper
- champion live

가 정확한 역할 분리를 가져야 한다.

실제 상태는 inspection 시점 기준:

- champion/live/latest/latest_candidate overloaded
- challenger state file 없음
- candidate target units 비어 있는 recent latest.json

즉 state machine drift가 있다.


### 4.2 Pointer Semantics Are Operationally Too Fragile

현재 포인터 상태가 모두 같은 run을 가리키는 것은 강한 신호다.

이건:

- trainer
- acceptance
- adoption
- promotion

이 아직 완전히 분리된 semantics로 작동하지 않았거나,
혹은 temporary manual path가 잔류했음을 의미한다.


### 4.3 oneshot Service Long-Running Ambiguity

`spawn.service`가 장시간 `activating (start)`인 것은 나쁜 smell이다.

문제:

- operator가 stuck으로 오해할 수 있음
- timer overlap handling 복잡
- systemd semantics와 actual job duration이 어긋남

권장:

- long-running batch orchestrator면 `Type=simple`
- short job면 `oneshot`
- 둘 중 하나로 명확히 정리해야 한다.


### 4.4 Current Challenger State Contract Is Not Closed

`current_state.json`이 없고, latest report가 `NO_PREVIOUS_CHALLENGER_STATE`로 끝난 것은:

- spawn/promote state handoff contract가 아직 robust하지 않음을 뜻한다.


### 4.5 Replay/Legacy Path Should Not Stay Ambiguous

실서버에는 replay clone/service가 남아 있지만 현재 사용자 기준 운영 대상이 아니다.

이 상태로 두면:

- dashboard/operator confusion
- unit ownership ambiguity
- storage and mental overhead

가 생긴다.


### 4.6 Worktree Cleanliness Is Not Enforced

서버 worktree가 clean하지 않으면:

- deployment audit
- rollback confidence
- `git pull --ff-only` discipline

이 약해진다.


## 5. Stronger Methodology

## 5.1 Make The Server Topology Explicitly Two-Lane Only

현재 replay는 안 쓰므로 target topology는 명확히:

- champion lane
- candidate lane

만 둔다.

구체:

- champion paper
- champion live
- candidate paper
- candidate live
- shared data plane
- dashboard
- housekeeping timers

replay clone/service는 target topology에서 제거한다.


## 5.2 Introduce A Deployment State Registry

새 artifact:

- `logs/runtime_topology/latest.json`

내용:

- expected units
- active units
- pointer bindings
- state DB paths
- rollout modes
- current lane run ids
- topology health status

이걸 만들면 "문서상 구조"와 "실서버 구조"를 기계적으로 비교할 수 있다.


## 5.3 Convert Script-Centric Automation Into Contract-Centric Automation

현재는 systemd가 thin wrapper고 PowerShell script가 실질 state machine이다.

이건 나쁘진 않지만 다음이 필요하다.

- step contract
- idempotency contract
- checkpoint contract
- failure class contract

즉 script가 길더라도 내부 상태는 machine-readable state artifact로 남아야 한다.


## 5.4 Enforce Pointer Transition Rules

강한 운영 계약은 아래를 강제해야 한다.

- trainer는 `latest`만 갱신
- acceptance/adoption만 `latest_candidate` 갱신
- promote만 `champion` 갱신

그리고:

- `champion == latest_candidate`는 temporary exception only
- 정상 steady-state에서는 warning or policy violation


## 5.5 Introduce Pre-Flight Unit Contract Checks

spawn/promote 실행 전에 자동 체크:

- required unit files exist
- expected enabled/disabled state
- state DB path exists
- registry pointers resolvable
- worktree clean or approved dirty state
- no stale current_state mismatch

실패 시:

- train 돌리기 전에 fast fail


## 5.6 Split Batch Orchestration Into Observable Steps

권장 step artifact:

- `step_01_data_plane.json`
- `step_02_feature_build.json`
- `step_03_train.json`
- `step_04_acceptance.json`
- `step_05_adopt_candidate.json`
- `step_06_promote.json`
- `step_07_restart_units.json`

이건 PowerShell script를 없애자는 뜻이 아니라,
script 내부 step을 분리된 checkpoint artifact로 남기자는 뜻이다.


## 5.7 Treat Replay As Cleanup Candidate

사용자 요구에 따라:

- replay clone/service는 current architecture에서 제외
- 관련 unit과 clone은:
  - archive
  - disable
  - or remove

중 하나로 정리해야 한다.

중요:

- 이건 event replay methodology와는 별개다.
- 향후 offline certification lane이 필요하면 main repo 안의 dataset/engine으로 구현하면 된다.
- 별도 long-running replay service를 운영 topology의 일부로 둘 필요는 없다.


## 5.8 Promote And Spawn Need A Stronger Shared State Contract

현재 문제는 `promote_only`가 `NO_PREVIOUS_CHALLENGER_STATE`로 끝난다는 점이다.

강한 구조에서는:

- spawn가 candidate state를 생성
- promote가 그 exact state를 consume
- promote 성공 시 state clear
- 실패 시 explicit stale state + recovery action

이 보장되어야 한다.


## 5.9 Add Remote Runtime Audit Reports

권장 artifact:

- `server_runtime_topology_report.json`
- `pointer_consistency_report.json`
- `systemd_unit_contract_report.json`
- `deployment_cleanliness_report.json`

이건 dashboard와 acceptance 모두가 읽을 수 있어야 한다.


## 6. Recommended New Documents / Artifacts

권장 artifact:

- `logs/runtime_topology/latest.json`
- `logs/runtime_topology/history/*.json`
- `logs/ops/deployment_contract/latest.json`
- `logs/ops/pointer_consistency/latest.json`

권장 code/script:

- `scripts/report_server_runtime_topology.ps1`
- `scripts/check_pointer_consistency.ps1`
- `scripts/check_systemd_contracts.ps1`


## 7. Recommended Implementation Order

### Step 1

- runtime topology report
- pointer consistency report

### Step 2

- pre-flight unit contract checks
- worktree cleanliness check

### Step 3

- spawn/promote shared state contract hardening

### Step 4

- replay legacy cleanup

### Step 5

- script step checkpoint decomposition


## 8. Strongest Combined View

앞선 predictor/evaluation/risk 문서들과 합치면, 운영 자동화 문서의 역할은 명확하다.

- predictor가 강해도
- evaluation이 강해도
- risk control이 강해도

서버 운영 계약이 약하면 전체 시스템은 약해진다.

특히 이 저장소는:

- pointer semantics
- state DB
- systemd timers
- rollout modes

가 모두 실제 PnL과 연결되므로 ops contract는 first-class architecture다.


## 9. Final Recommendation

현재 실서버 상태를 기준으로 가장 중요한 운영 작업은 아래다.

1. `runtime topology report` 추가
2. `pointer consistency contract` 강제
3. `spawn/promote state handoff` 정비
4. `oneshot long-running ambiguity` 제거
5. `replay legacy cleanup`
6. `worktree cleanliness policy` 도입

이 여섯 가지가 되면 문서상 목표와 실서버 상태 사이의 거리가 크게 줄어든다.


## 10. References

### Internal Documents

- [FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md](/d:/MyApps/Autobot/docs/FULLY_AUTOMATED_CHAMPION_CANDIDATE_OPERATING_CONTRACT_2026-03-24.md)
- [INTEGRATED_STRONG_MODEL_SYSTEM_ROADMAP_2026-03-25.md](/d:/MyApps/Autobot/docs/INTEGRATED_STRONG_MODEL_SYSTEM_ROADMAP_2026-03-25.md)
- [BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/BACKTEST_PAPER_LIVE_STRENGTHENING_BLUEPRINT_2026-03-25.md)
- [RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md](/d:/MyApps/Autobot/docs/RISK_AND_LIVE_CONTROL_STRENGTHENING_BLUEPRINT_2026-03-25.md)
