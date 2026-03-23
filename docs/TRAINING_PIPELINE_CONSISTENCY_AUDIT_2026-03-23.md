# Training Pipeline Consistency Audit 2026-03-23

Status: current analysis
Operational authority: yes
Scope: `23:40` execution refresh, `00:10` challenger spawn, `train -> runtime recommendation -> acceptance -> spawn`

## Read This First

This audit was performed after the following operational fixes were already applied:

- candidate/live startup now re-evaluates stale online risk halts instead of dying immediately
- manual stale local positions can now be reconciled and manual closes can be inferred from exchange closed orders
- rejected candidates no longer overwrite `latest_candidate` or auto-restart candidate live
- execution-policy veto failures are surfaced explicitly in acceptance
- shadow timers were disabled on OCI

Related documents:

- [PROGRAM_RUNBOOK.md](/d:/MyApps/Autobot/docs/PROGRAM_RUNBOOK.md)
- [FOUNDATIONAL_FAILURE_MODES_2026-03-23.md](/d:/MyApps/Autobot/docs/FOUNDATIONAL_FAILURE_MODES_2026-03-23.md)
- [TRAINING_PIPELINE_RESEARCH_COMPARE_2026-03-23.md](/d:/MyApps/Autobot/docs/TRAINING_PIPELINE_RESEARCH_COMPARE_2026-03-23.md)
- [RUNTIME_EXECUTION_FINDINGS_AND_ACTION_PLAN_2026-03-23.md](/d:/MyApps/Autobot/docs/RUNTIME_EXECUTION_FINDINGS_AND_ACTION_PLAN_2026-03-23.md)
- [EXECUTION_POLICY_VETO_REDESIGN_2026-03-23.md](/d:/MyApps/Autobot/docs/EXECUTION_POLICY_VETO_REDESIGN_2026-03-23.md)

## Pipeline Map

The primary `00:10` path is:

1. `autobot-v4-challenger-spawn.timer`
2. [`daily_champion_challenger_v4_for_server.ps1`](/d:/MyApps/Autobot/scripts/daily_champion_challenger_v4_for_server.ps1)
3. `refresh_live_execution_policy.ps1`
4. [`v4_governed_candidate_acceptance.ps1`](/d:/MyApps/Autobot/scripts/v4_governed_candidate_acceptance.ps1)
5. [`candidate_acceptance.ps1`](/d:/MyApps/Autobot/scripts/candidate_acceptance.ps1)
6. `python -m autobot.cli model train --trainer v4_crypto_cs`
7. [`train_v4_crypto_cs.py`](/d:/MyApps/Autobot/autobot/models/train_v4_crypto_cs.py)
8. internal `execution_acceptance` / `runtime_recommendations`
9. acceptance backtest / runtime parity / trainer evidence gates
10. `start_challenger` and optionally `restart_candidate_targets`

## Findings

### 1. Training-time execution evaluation does not use the same window as acceptance certification

Severity: high

The internal execution evaluation created during training still uses the full training request window:

- [`train_v4_execution.py`](/d:/MyApps/Autobot/autobot/models/train_v4_execution.py)
  - `run_execution_acceptance_v4(...)` passes:
    - `start_ts_ms = parse_date_to_ts_ms(options.start)`
    - `end_ts_ms = parse_date_to_ts_ms(options.end, end_of_day=True)`
  - `build_runtime_recommendations_v4(...)` passes the same window

This means:

- training-time `execution_acceptance.json`
- training-time `runtime_recommendations.json`

are optimized over the train command range, not the strict certification range later used by [`candidate_acceptance.ps1`](/d:/MyApps/Autobot/scripts/candidate_acceptance.ps1).

By contrast, acceptance uses:

- `certification_start_date`
- `effectiveBatchDate`

for candidate/champion backtests and runtime parity.

Result:

- the runtime contract and recommendation artifacts in the run directory are not directly comparable to the acceptance decision that follows
- a candidate can look good in trainer-internal execution evaluation and still fail immediately on certification parity, or the reverse

Why this matters:

- this is a true contract mismatch, not just an optimization detail
- it can distort execution-policy learning, runtime recommendation selection, and later root-cause analysis

Recommended fix:

1. pass the selected certification window into `train_v4_crypto_cs`
2. build internal `execution_acceptance` and `runtime_recommendations` on the certification window, not the train window
3. record that window explicitly inside the emitted artifacts

### 2. Governed acceptance still depends on stale rank-shadow governance files

Severity: medium

[`v4_governed_candidate_acceptance.ps1`](/d:/MyApps/Autobot/scripts/v4_governed_candidate_acceptance.ps1) still reads:

- `logs/model_v4_rank_shadow_cycle/latest_governance_action.json`

and uses that file to pick the acceptance script.

Current OCI state is safe because:

- shadow timer is disabled
- the last governance file currently points to `cls_primary`
- missing governance action already defaults to `v4_promotable_candidate_acceptance.ps1`

But the structural issue remains:

- a stale file can still steer the governed lane even when shadow automation is disabled

Recommended fix:

1. if rank-shadow timer is disabled, ignore the governance artifact and default to promotable lane
2. alternatively, reject governance artifacts older than the current batch date or beyond a freshness threshold

### 3. `latest` still advances before acceptance completes

Severity: medium

[`train_v4_crypto_cs.py`](/d:/MyApps/Autobot/autobot/models/train_v4_crypto_cs.py) updates:

- family `latest`
- global `latest`

for `scheduled_daily` runs before acceptance finishes.

This is no longer fatal for candidate/live because:

- `latest_candidate` is now acceptance-gated
- candidate live binds `latest_candidate_v4`
- main live binds `champion_v4`

But it still creates a semantic split:

- `latest` means newest training artifact
- `latest_candidate` means acceptance-passed candidate

That is workable, but only if every downstream consumer respects the distinction.

Current risk:

- fallback code or tooling that treats `latest_v4` as “best current candidate” will still be wrong

Recommended fix:

1. keep the distinction, but document it aggressively
2. audit any remaining `latest_v4` consumers and ensure they are non-promotive

### 4. Acceptance is now consistent about quality floor, but the trainer itself still relies on caller-prepared dates

Severity: medium

[`candidate_acceptance.ps1`](/d:/MyApps/Autobot/scripts/candidate_acceptance.ps1) now applies:

- `train_data_quality_floor_date`
- split-policy strict window selection
- historical selector filtering by quality floor

This is good.

But the trainer core itself does not independently know the certification semantics. It only sees the requested `start` and `end`.

Result:

- acceptance owns the “true” windows
- trainer-internal execution/risk artifacts still depend on what acceptance chose to pass in

This is mostly the same root cause as Finding 1, but from an ownership perspective:

- the training artifact contract is still partially implicit in the wrapper layer

Recommended fix:

1. serialize the selected train/certification windows into the run contract before internal evaluation
2. make trainer-internal execution/risk builders consume that explicit contract

### 5. Runtime recommendation artifacts do not make their evaluation window obvious enough

Severity: medium

[`runtime_recommendations.py`](/d:/MyApps/Autobot/autobot/models/runtime_recommendations.py) emits rich output, but the artifact does not prominently expose:

- exact evaluation start/end
- whether the window is train, certification, or another lane
- the window provenance source

This makes post-mortem analysis harder, especially when comparing:

- trainer-internal recommendation output
- acceptance runtime parity results

Recommended fix:

Add explicit metadata to `runtime_recommendations.json`:

- `evaluation_window.start_ts_ms`
- `evaluation_window.end_ts_ms`
- `evaluation_window.label`
- `evaluation_window.source`

## Findings Not Marked As Pipeline Bugs

### A. Shadow disable is operationally safe right now

Current OCI state:

- `autobot-v4-rank-shadow.timer`: disabled
- `autobot-v4-rank-shadow.service`: disabled
- `autobot-live-alpha-replay-shadow.service`: disabled

This does not break `00:10` today because the governed wrapper still resolves to the promotable `cls` lane.

### B. Session refresh does work without restart

Live runtime verification showed:

- `last_sync` advances during a single live session
- stale candidate `KRW-ORDER` local position was eventually cleared in-session

So the intended periodic refresh contract is alive.

## Recommended Next Fix Order

1. Fix Finding 1
   - align trainer-internal execution evaluation and runtime recommendation windows with certification
2. Fix Finding 2
   - make governed acceptance ignore stale shadow governance when shadow is disabled
3. Fix Finding 5
   - write explicit evaluation-window provenance into runtime artifacts
4. Re-run one `23:40 -> 00:10` cycle and compare:
   - trainer-internal execution artifacts
   - acceptance backtest
   - runtime parity

## Suggested Next Context Prompt

Continue from `TRAINING_PIPELINE_CONSISTENCY_AUDIT_2026-03-23.md`.

Priority:

1. patch `train_v4_execution.py` so internal `execution_acceptance` and `runtime_recommendations` use the selected certification window rather than `options.start/end`
2. patch `v4_governed_candidate_acceptance.ps1` so stale rank-shadow governance is ignored when shadow is disabled
3. add evaluation-window provenance into `runtime_recommendations.json`
