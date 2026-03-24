# Fully Automated Champion Candidate Operating Contract 2026-03-24

Status: proposed refactor contract
Operational authority: intended future SSOT for v4 full automation
Scope: Oracle OCI server access, Git deployment, training, candidate generation, acceptance, paper, canary, champion live, pointers, artifacts, promotion, rollback, and retention

## 1. Purpose

This document defines the target operating contract for `train_v4_crypto_cs` so that:

- the daily loop is fully automated
- pointer meanings are exact and non-overlapping
- paper, canary, and champion each have one clear role
- promotion decisions never depend on ambiguous or incomplete state
- evidence artifacts remain auditable after the fact
- manual intervention follows the same state machine as automation

This document is intentionally strict.
If code, service state, or docs disagree, the refactor should move code and services toward this contract rather than weakening the contract.

## 2. Verified Environment

### 2.1 Local Git

Local workspace:

- `d:\MyApps\Autobot`

Local Git remote:

- `origin https://github.com/LeeGukHeon/Autobot.git`

Primary branch:

- `main`

### 2.2 Oracle OCI Server

Server host:

- `ubuntu@168.107.44.206`

Server project root:

- `/home/ubuntu/MyApps/Autobot`

SSH private key on the local machine:

- `C:\Users\Administrator\Desktop\OCI_SSH_KEY\ssh-key-2026-03-05.key`

Local Windows PowerShell login command:

```powershell
& 'C:\Windows\System32\OpenSSH\ssh.exe' -i "C:\Users\Administrator\Desktop\OCI_SSH_KEY\ssh-key-2026-03-05.key" ubuntu@168.107.44.206
```

Server Git remote currently verified on 2026-03-24:

- `origin git@github.com:LeeGukHeon/Autobot.git`

Server HEAD verified on 2026-03-24:

- `c8f102ea4e9ca90a0d73c454f515afe1d7d98c9f`

Deployment model today:

1. local commit
2. local `git push origin main`
3. server `git pull --ff-only origin main`

Canonical server pull command:

```powershell
& 'C:\Windows\System32\OpenSSH\ssh.exe' -i "C:\Users\Administrator\Desktop\OCI_SSH_KEY\ssh-key-2026-03-05.key" ubuntu@168.107.44.206 "cd /home/ubuntu/MyApps/Autobot && git pull --ff-only origin main && git rev-parse HEAD"
```

## 3. Verified Runtime Snapshot On 2026-03-24

Verified family pointers on the server:

- `champion.json`
  - `run_id = 20260324T081937Z-s42-a6d6b2a5`
- `train_v4_crypto_cs/latest_candidate.json`
  - `run_id = 20260324T081937Z-s42-a6d6b2a5`
- `train_v4_crypto_cs/latest.json`
  - `run_id = 20260324T081937Z-s42-a6d6b2a5`
- global `latest_candidate.json`
  - `run_id = 20260324T081937Z-s42-a6d6b2a5`
- global `latest.json`
  - `run_id = 20260324T081937Z-s42-a6d6b2a5`

Verified service states on the server:

- `autobot-paper-v4.service`
  - active
  - start timestamp `2026-03-24 19:12:56 KST`
- `autobot-paper-v4-challenger.service`
  - inactive
- `autobot-live-alpha.service`
  - inactive
- `autobot-live-alpha-candidate.service`
  - active
  - start timestamp `2026-03-24 19:15:32 KST`

This means the current server is in a temporary non-steady-state where:

- champion and canary point to the same run
- challenger paper is not active
- champion live is not active

This is acceptable only as a temporary operator state.
It must not be the steady-state target of automation.

## 4. Exact Pointer Semantics

The refactor must make these meanings explicit and enforce them everywhere.

### 4.1 Family Pointer Aliases

Within `train_v4_crypto_cs`:

- `latest_v4` -> family `latest`
- `latest_candidate_v4` -> family `latest_candidate`
- `champion_v4` -> family `champion`

These aliases are routing convenience only.
They are not separate state.

### 4.2 Pointer Definitions

#### `latest`

Meaning:

- newest completed trainer artifact for the family

Allowed to point to:

- a run that completed training and trainer-side runtime artifact generation

Must not mean:

- acceptance-passed candidate
- canary-adopted candidate
- live-promoted champion

Allowed consumers:

- research tooling
- dashboards
- forensic comparison tools

Forbidden consumers:

- champion live
- canary live
- promotion decisions

Mutation rule:

- written only by trainer after trainer runtime/governance artifacts are fully persisted

#### `latest_candidate`

Meaning:

- the candidate currently adopted into challenger paper and canary live

Allowed to point to:

- a run that passed the required offline candidate gate for canary adoption

Allowed to be absent:

- yes
- especially between successful promotion of the previous candidate and successful spawn of the next candidate

Must not mean:

- newest training artifact regardless of gate outcome
- champion

Allowed consumers:

- `autobot-paper-v4-challenger.service`
- `autobot-live-alpha-candidate.service`
- dashboard actions that explicitly mean "adopt next candidate"

Mutation rule:

- written only by successful candidate adoption logic
- never written by raw trainer completion alone
- manual promote must not silently mutate it unless the operation explicitly includes candidate adoption

#### `champion`

Meaning:

- the current production model approved for champion paper and champion live

Allowed consumers:

- `autobot-paper-v4.service`
- `autobot-live-alpha.service`
- promotion compare baselines

Mutation rule:

- written only by promote logic after promote readiness checks pass

#### `logs/model_v4_challenger/current_state.json`

Meaning:

- transient pending challenger state between `spawn_only` and the next `promote_only`

Must contain:

- `candidate_run_id`
- `champion_run_id_at_start`
- `started_ts_ms`
- lane metadata

Must not be treated as:

- a general registry pointer
- a replacement for `latest_candidate`

Mutation rule:

- created by successful spawn/adopt of a new challenger
- removed by successful promote-only completion or explicit rollback/cleanup

## 5. Service Semantics

### 5.1 Champion Paper

Unit:

- `autobot-paper-v4.service`

Model source:

- `champion_v4`

Purpose:

- continuous paper reference lane for the current champion
- promotion baseline for `paper_lane_evidence`

### 5.2 Challenger Paper

Unit:

- `autobot-paper-v4-challenger.service`

Model source:

- pinned run id through `AUTOBOT_PAPER_MODEL_REF_PINNED`

Purpose:

- continuous paper reference lane for the current candidate under evaluation

### 5.3 Champion Live

Unit:

- `autobot-live-alpha.service`

Model source:

- `champion_v4`

Purpose:

- actual production live trading

### 5.4 Canary Live

Unit:

- `autobot-live-alpha-candidate.service`

Model source:

- `latest_candidate_v4`

Purpose:

- real-market validation lane for the next candidate
- not a duplicate champion lane

### 5.5 Daily One-Shot Orchestrator

Unit/script:

- `autobot-v4-challenger-spawn.service`
- `scripts/daily_champion_challenger_v4_for_server.ps1`

Modes:

- `promote_only`
- `spawn_only`
- `combined`

Target design:

- production timers should run `promote_only` and `spawn_only` separately
- `combined` is only for explicit manual recovery or development

## 6. Current Code-Level Flow

### 6.1 Training

Entrypoints:

- `python -m autobot.cli model train --trainer v4_crypto_cs ...`
- `autobot/models/train_v4_crypto_cs.py`

Current high-level flow:

1. prepare dataset and split contracts
2. fit primary booster
3. build thresholds and leaderboard row
4. `save_run(..., publish_pointers=False)`
5. write support artifacts:
   - walk-forward
   - cpcv lite
   - factor block selection
   - search budget decision
6. build trainer-side `execution_acceptance`
7. build trainer-side `runtime_recommendations`
8. build trainer-side `promotion_decision`
9. build `trainer_research_evidence`
10. persist runtime/governance artifacts
11. update family/global `latest` for `scheduled_daily`
12. append experiment ledger and write train report

Important code:

- `autobot/models/train_v4_crypto_cs.py`
- `autobot/models/train_v4_execution.py`
- `autobot/models/train_v4_governance.py`
- `autobot/models/train_v4_persistence.py`
- `autobot/models/registry.py`

### 6.2 Acceptance

Primary scripts:

- `scripts/v4_governed_candidate_acceptance.ps1`
- `scripts/v4_promotable_candidate_acceptance.ps1`
- `scripts/candidate_acceptance.ps1`

Current acceptance flow:

1. run daily pipeline if not skipped
2. run trainer
3. resolve candidate run id
4. read trainer artifacts from candidate run dir
5. run acceptance candidate/champion backtest on frozen compare profile
6. run runtime parity backtest
7. optionally run paper soak
8. compute `overall_pass`
9. if `overall_pass`, write `latest_candidate`
10. if not `SkipPromote` and `overall_pass`, promote immediately

Important code:

- `Resolve-RunDirFromText`
- `Invoke-BacktestAndLoadSummary`
- `Update-LatestCandidatePointers`
- `New-CertificationArtifact`

### 6.3 Promote-Only

Current code:

- `scripts/daily_champion_challenger_v4_for_server.ps1`
- `autobot/common/paper_lane_evidence.py`

Current promote-only flow:

1. read `current_state.json`
2. identify previous candidate run id and champion run id at start
3. aggregate challenger/champion paper lane runs
4. compare via `paper_lane_evidence`
5. if decision says promote:
   - run `python -m autobot.cli model promote`
   - restart champion paper
   - restart allowed promotion target units

### 6.4 Spawn-Only

Current flow:

1. refresh execution policy contract from state DBs
2. run governed candidate acceptance with `-SkipPaperSoak -SkipPromote`
3. if candidate run id exists and acceptance gate permits:
   - install or update challenger paper unit pinned to the candidate run id
   - write `current_state.json`
   - if `overall_pass`, restart configured canary target units

## 7. Current Bugs And Consistency Gaps

These are not optional improvements.
They are concrete contract issues.

### 7.1 Incomplete Run Promotion Is Possible

Problem:

- `promote_run_to_champion()` in `autobot/models/registry.py` requires only `leaderboard_row.json`
- it does not require:
  - `execution_acceptance_report.json`
  - `runtime_recommendations.json`
  - `promotion_decision.json`
  - `decision_surface.json`
  - acceptance certification completion

Impact:

- a partially completed or manually interrupted run can become champion

### 7.2 `latest_candidate` Contract Is Split Across Code Paths

Problem:

- trainer updates `latest`
- acceptance updates `latest_candidate`
- manual promote updates `champion`
- none of those mutations are transactionally linked

Impact:

- champion and canary can diverge accidentally
- canary can remain stale after manual promote
- current code and tests disagree on intended meaning

### 7.3 Trainer Tests Currently Disagree With Trainer Code

Observed on 2026-03-24:

- targeted run:
  - `python -m pytest tests/test_train_v4_crypto_cs.py -k "latest_candidate or registers_candidate_without_auto_promotion or split_policy_history_scope_keeps_latest_pointers_clean"`
- result:
  - 2 failed

Reason:

- tests expect trainer to publish `latest_candidate`
- trainer code currently does not

This is a real contract mismatch and must be resolved explicitly.

### 7.4 Acceptance Run Resolution Can Fall Back To The Wrong Pointer

Current fallback order in `candidate_acceptance.ps1`:

1. parse candidate run dir from trainer stdout
2. fallback to `latest`
3. fallback to `latest_candidate`

Impact:

- if stdout parse fails during overlapping runs, acceptance can attach itself to the wrong run

Target rule:

- acceptance must never use `latest` to identify the fresh candidate run for the same invocation

### 7.5 Evidence Cleanup Removes Referenced Backtest Run Dirs

Current trainer cleanup:

- `train_v4_execution.py` removes run directories reported inside execution/runtime artifacts

Impact:

- `execution_acceptance_report.json` may keep `run_dir` strings that no longer exist
- postmortem compare and forensic debugging become much harder

Target rule:

- no artifact may store a path that automation immediately deletes

### 7.6 Code Pull Does Not Automatically Refresh Long-Running Champion Services

Problem:

- a server `git pull` updates disk state only
- long-running paper/live units keep old code in memory until restart

Impact:

- acceptance/backtest may use new code while champion paper/live still run old code

Target rule:

- service restart obligations must be explicit and tied to pointer changes or code deployment

## 8. Target Automation Model

The target loop is a two-stage daily cycle:

1. evaluate yesterday's candidate for promotion
2. build and adopt today's next candidate

### 8.1 Stage A: 23:40 Refresh

Action:

- run `scripts/refresh_live_execution_policy.ps1`

Inputs:

- `data/state/live_state.db`
- `data/state/live_candidate/live_state.db`

Outputs:

- `logs/live_execution_policy/combined_live_execution_policy.json`
- `logs/live_execution_policy/latest_refresh.json`

Contract:

- refresh must complete before any daily promote or spawn stage

### 8.2 Stage B: 00:10 Promote-Only

Action:

- evaluate the candidate in `current_state.json`

Evidence source:

- paper lane evidence first
- canary live evidence as optional guard or tie-break

If promote succeeds:

1. verify candidate run completeness
2. mutate `champion`
3. archive promote cutover artifact
4. restart `autobot-paper-v4.service`
5. restart `autobot-live-alpha.service` if enabled and armed
6. stop `autobot-paper-v4-challenger.service`
7. stop `autobot-live-alpha-candidate.service`
8. clear `latest_candidate`
9. clear `current_state.json`

If promote fails:

1. keep current champion
2. stop challenger paper and canary only if the candidate is explicitly rejected and should not continue collecting evidence
3. otherwise keep them running until the configured evidence window ends
4. clear or preserve `current_state.json` according to decision type, but never leave ambiguous stale state

### 8.3 Stage C: 00:20 Spawn-Only

Action:

- train the next candidate
- run offline gate
- adopt the new candidate only if offline gate passes

If candidate adoption succeeds:

1. write `latest_candidate`
2. install or update challenger paper pinned to the run id
3. restart `autobot-paper-v4-challenger.service`
4. restart `autobot-live-alpha-candidate.service`
5. write fresh `current_state.json`

If candidate adoption fails:

1. do not touch `latest_candidate`
2. do not restart challenger paper
3. do not restart canary
4. write a complete negative acceptance artifact

## 9. Exact Steady-State Meaning Of Each Lane

### Champion Paper

Role:

- promotion baseline
- continuous shadow of current champion under paper conditions

### Challenger Paper

Role:

- primary promotion judge for the next candidate

### Canary Live

Role:

- real-market validation lane for the next candidate
- secondary evidence, never the only promotion judge

### Champion Live

Role:

- actual production trading

Steady-state rule:

- champion live and canary live should normally run different model refs
- if they point to the same run, the system is in a temporary transition state, not steady state

## 10. Artifact Contract

### 10.1 Required Per-Run Trainer Artifacts

Every promoted or adoptable run must have:

- `leaderboard_row.json`
- `metrics.json`
- `thresholds.json`
- `selection_recommendations.json`
- `selection_policy.json`
- `selection_calibration.json`
- `walk_forward_report.json`
- `execution_acceptance_report.json`
- `runtime_recommendations.json`
- `promotion_decision.json`
- `trainer_research_evidence.json`
- `economic_objective_profile.json`
- `lane_governance.json`
- `decision_surface.json`
- `certification_report.json`

### 10.2 New Required Completeness Artifact

The refactor should add a new file in every run dir:

- `artifact_status.json`

It must include:

- `run_id`
- `status`
- `core_saved`
- `support_artifacts_written`
- `execution_acceptance_complete`
- `runtime_recommendations_complete`
- `governance_artifacts_complete`
- `acceptance_completed`
- `candidate_adoptable`
- `candidate_adopted`
- `promoted`
- `updated_at_utc`

This file becomes the promote/adopt readiness gate.

### 10.3 Acceptance Artifacts

Acceptance must always write:

- lane report json
- lane report markdown
- candidate run `certification_report.json`

Acceptance must record:

- candidate backtest run dir and summary path
- champion backtest run dir and summary path
- runtime parity candidate run dir and summary path
- runtime parity champion run dir and summary path
- paper smoke report path if paper is evaluated

### 10.4 Promotion Evidence Artifacts

Promote-only must always write:

- `logs/model_v4_challenger/latest.json`
- archive copy under `logs/model_v4_challenger/archive/`
- if promotion succeeds:
  - `logs/model_v4_challenger/latest_promote_cutover.json`
  - archive copy under `logs/model_v4_challenger/promote_cutover_archive/`

### 10.5 Retention Rule

The refactor should stop immediate deletion of execution backtest run dirs.

Required behavior:

- preserve trainer execution-acceptance and runtime-recommendation backtest run dirs for a retention window
- if storage pressure requires cleanup, use a separate retention job
- never delete a run dir while a stored artifact still references it

## 11. Required Refactor Decisions

### 11.1 Pointer Ownership

Final decision:

- `latest` is trainer-owned
- `latest_candidate` is candidate-adoption-owned
- `champion` is promote-owned

Trainer must not update `latest_candidate`.
Acceptance and dashboard helper logic must treat `latest_candidate` as the adopted canary/challenger candidate only.

### 11.2 Manual Promote Semantics

`python -m autobot.cli model promote` must not remain a naked pointer mutation.

Required new behavior:

- either:
  - make `model promote` enforce run completeness and only mutate `champion`
  - and keep orchestration in a higher-level script
- or:
  - add a separate higher-level command such as `model promote-v4-cutover`

Required cutover behavior:

- verify run completeness
- promote champion
- write cutover artifact
- restart champion paper
- restart champion live if enabled
- optionally stop or clear candidate services

### 11.3 Candidate Adoption Semantics

Add a single explicit candidate-adoption operation that:

- verifies candidate readiness for canary adoption
- writes `latest_candidate`
- restarts challenger paper
- restarts canary live
- writes `current_state.json`

Dashboard action `adopt_latest_candidate` should call this same path, not a separate special case.

## 12. Required Code Changes By File

### `autobot/models/train_v4_crypto_cs.py`

Must change:

- stop any hidden expectation that trainer publishes `latest_candidate`
- write `artifact_status.json`
- mark runtime/governance completion only after all runtime artifacts are persisted

Must not change:

- `latest` remaining trainer-owned for `scheduled_daily`

### `autobot/models/registry.py`

Must change:

- add completeness verification helper
- add promote guard that rejects incomplete runs
- keep `promote_run_to_champion()` from succeeding on partial runs

### `scripts/candidate_acceptance.ps1`

Must change:

- never use `latest` as the fallback for fresh candidate identification
- require run-specific stdout/run_dir or explicit candidate pointer semantics
- persist acceptance evidence paths and run dirs robustly
- update `artifact_status.json`
- candidate adoption logic should be separable from promotion logic

### `scripts/daily_champion_challenger_v4_for_server.ps1`

Must change:

- production automation should use separate timers for `promote_only` and `spawn_only`
- successful promote must clear candidate state and optionally clear `latest_candidate`
- successful spawn must explicitly adopt the new candidate
- code deployment vs service restart obligations must be explicit

### `autobot/common/paper_lane_evidence.py`

Must change:

- preserve enough evidence metadata to support forensic replay
- keep comparison output tied to exact run ids and run time windows

### `scripts/install_server_runtime_services.ps1`

Must change:

- document and enforce role-specific model sources
- champion units must never default to `latest_candidate`
- candidate units must never default to `champion`

### `autobot/dashboard_server.py`

Must change:

- route manual operator actions through the same adoption and promotion state machine as automation

## 13. Required Tests

The refactor is not complete unless the following contract tests exist and pass.

### Pointer Contract

- trainer updates `latest` but not `latest_candidate`
- candidate adoption updates `latest_candidate`
- promote updates `champion` only when completeness passes
- candidate adoption and promotion do not silently update unrelated pointers

### Promote Safety

- incomplete run promotion is rejected
- manual promotion of incomplete run does not mutate `champion`
- successful promote writes cutover artifacts

### Acceptance Resolution

- acceptance resolves the fresh run from stdout first
- if stdout run id is missing, it must not use family `latest` as a fallback for the same invocation

### Candidate Adoption

- successful candidate adoption restarts challenger paper and canary live
- failed adoption does not mutate `latest_candidate`

### Retention

- execution evidence artifacts do not reference deleted run dirs

### Daily Cycle

- promote-only and spawn-only together preserve a consistent state machine
- no stale `current_state.json` remains after successful promote-only

## 14. Non-Negotiable Invariants

These are hard rules.

1. No trading service may use `latest`.
2. `latest_candidate` means adopted candidate only.
3. `champion` means production champion only.
4. No incomplete run may be promoted.
5. No promote operation may leave champion/candidate pointers in an ambiguous state.
6. No artifact may reference a run dir that automation has already deleted.
7. Champion paper and champion live must restart after champion pointer change.
8. Challenger paper and canary live must restart after `latest_candidate` change.
9. Manual operator paths must reuse the automated state machine.
10. If automation cannot prove correctness, it must fail closed rather than guess.

## 15. Recommended End-State Cycle

Daily target:

1. `23:40`
   - refresh execution policy and live execution evidence
2. `00:10`
   - `promote_only`
   - evaluate previous candidate using paper first and canary as guard
3. immediately after
   - `spawn_only`
   - train, evaluate, and adopt the next candidate if offline gate passes
4. rest of day
   - champion live trades
   - champion paper baselines
   - challenger paper collects promotion evidence
   - canary live collects reality evidence

This is the best balance of:

- full automation
- operational safety
- promotion evidence quality
- real-market validation
- reproducible performance comparison

## 16. Manual Operations Still Allowed

Manual operations may still exist, but they must be wrappers around the same state machine.

Required manual actions:

- reconnect to server
- pull latest code
- inspect service state
- re-run `promote_only`
- re-run `spawn_only`
- explicit candidate adoption
- explicit champion cutover

They must not mutate pointers directly without completeness and service-transition checks.

## 17. Immediate Refactor Priority

Order:

1. pointer contract and `artifact_status.json`
2. incomplete-run promote guard
3. candidate adoption state machine
4. acceptance fallback cleanup
5. evidence retention fix
6. split `promote_only` and `spawn_only` timers into the canonical daily cycle

## 18. Current Known Sharp Edge To Carry Into The Refactor

As of 2026-03-24, the current server state was operator-adjusted so that:

- `champion`
- `latest`
- `latest_candidate`

all point to the same run `20260324T081937Z-s42-a6d6b2a5`.

This must be treated as a temporary manual state, not proof that the current contracts are correct.

