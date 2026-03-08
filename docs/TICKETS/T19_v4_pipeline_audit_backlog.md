# T19: v4 Pipeline Audit Backlog

- Date: 2026-03-08
- Scope: current automated `v4` lane only
- Goal: split start-to-end audit work into executable tickets

## Automation Boundary

Current automated `v4` path is:

1. `scripts/daily_micro_pipeline.ps1` / `scripts/daily_micro_pipeline_for_server.ps1`
   - collect candles / ticks / ws-public
   - build `micro_v1`
   - validate micro / ws artifacts
2. `scripts/v4_candidate_acceptance.ps1`
   - wrapper over `scripts/candidate_acceptance.ps1`
   - `autobot.cli model train --trainer v4_crypto_cs`
   - trainer-internal walk-forward / execution acceptance / runtime recommendations
   - external backtest sanity gate
   - optional paper soak
   - optional promote + runtime restart
3. `scripts/daily_champion_challenger_v4_for_server.ps1`
   - compare previous challenger vs current champion paper lane
   - promote previous challenger when paper evidence passes
   - restart champion lane
   - train next candidate and install challenger unit

Important boundary:

- The default daily automation currently ends at `paper` runtime promotion / restart.
- `autobot.live.*` and C++ executor paths exist, but they are not the default end of the daily `v4` loop.

## Validation Snapshot

Python-side regression checks executed during this audit:

- `python -m pytest -q tests/test_pipeline_v4_label_v2.py tests/test_train_v4_crypto_cs.py tests/test_paper_live_feature_provider_v4.py tests/test_model_compare_v4.py tests/test_runtime_recommendations.py tests/test_live_daemon.py`
- `python -m pytest -q tests/test_cli_alpha_shortcuts.py tests/test_paper_engine_model_alpha_integration.py tests/test_paper_live_ws_provider_selection.py tests/test_paper_lane_evidence.py tests/test_execution_acceptance.py`

Result:

- 40 tests passed
- current highest risk is orchestration / runtime branch behavior, not the already-tested core trainer path

## Priority Tickets

### T19.1 Fix LIVE_WS provider gating bug for `micro_gate`-only runtime

- Severity: High
- Evidence:
  - [engine.py](/d:/MyApps/Autobot/autobot/paper/engine.py#L2440)
  - [engine.py](/d:/MyApps/Autobot/autobot/paper/engine.py#L2471)
- Problem:
  - `_resolve_micro_snapshot_provider()` allows `LIVE_WS` only when `micro_order_policy.enabled=True`.
  - If `micro_gate.enabled=True`, `paper_micro_provider=live_ws`, and policy is off, runtime silently rejects live WS and falls back offline.
- Observed audit reproduction:
  - `micro_gate.enabled=True`
  - `micro_order_policy.enabled=False`
  - `paper_micro_provider='live_ws'`
  - actual provider became `OfflineMicroSnapshotProvider`
  - decision logged as `LIVE_WS_REJECTED_POLICY_OFF`
- Risk:
  - runtime config does not match actual data source
  - micro gate quality can degrade or behave differently from operator intent
- Acceptance:
  - `micro_gate` alone can legally drive `LIVE_WS`
  - add regression test for `micro_gate=on`, `micro_order_policy=off`, `paper_micro_provider=live_ws`

### T19.2 Pin acceptance to the concrete candidate run id

- Severity: High
- Evidence:
  - [candidate_acceptance.ps1](/d:/MyApps/Autobot/scripts/candidate_acceptance.ps1#L1407)
  - [candidate_acceptance.ps1](/d:/MyApps/Autobot/scripts/candidate_acceptance.ps1#L1727)
- Problem:
  - after training, script resolves `$candidateRunId`
  - but backtest and paper soak still run with `$CandidateModelRef` alias, usually `latest_candidate_v4`
- Risk:
  - concurrent training or manual promote can move the pointer mid-run
  - acceptance may evaluate or promote a different model than the one just trained
- Acceptance:
  - once training returns a concrete run id, every downstream step uses that run id
  - report explicitly records `candidate_run_id_used_for_backtest` and `candidate_run_id_used_for_paper`

### T19.3 Replace filesystem-diff run discovery with stdout/run-id based resolution

- Severity: Medium
- Evidence:
  - [paper_micro_smoke.ps1](/d:/MyApps/Autobot/scripts/paper_micro_smoke.ps1#L204)
  - [paper_micro_smoke.ps1](/d:/MyApps/Autobot/scripts/paper_micro_smoke.ps1#L280)
  - [candidate_acceptance.ps1](/d:/MyApps/Autobot/scripts/candidate_acceptance.ps1#L883)
- Problem:
  - `paper_micro_smoke.ps1` and acceptance backtest helpers infer the new run directory by snapshotting `data/*/runs` before/after execution
  - fallback behavior picks the most recent run if no new directory is found
- Risk:
  - concurrent runs can attach the report reader to the wrong run directory
  - a successful process can be graded against another run's `summary.json`
- Acceptance:
  - parse CLI JSON stdout and read `run_dir` directly
  - remove "latest directory wins" fallback for acceptance-critical paths

### T19.4 Make runtime bootstrap independent from caller working directory

- Severity: Medium
- Evidence:
  - [install_server_runtime_services.ps1](/d:/MyApps/Autobot/scripts/install_server_runtime_services.ps1#L105)
  - [install_server_runtime_services.ps1](/d:/MyApps/Autobot/scripts/install_server_runtime_services.ps1#L127)
- Problem:
  - service installer bootstraps `champion` with `python -m autobot.cli model promote ...`
  - script does not `Set-Location $resolvedProjectRoot` before that call
- Risk:
  - fresh install can fail when invoked outside repo root unless package is already installed into the venv
  - service bootstrap behavior differs from runtime behavior, which does set `WorkingDirectory`
- Acceptance:
  - bootstrap commands run from repo root explicitly
  - add a smoke check that installer works when launched from a non-repo cwd

### T19.5 Split v4 contract from v3 private helper reuse

- Severity: Medium
- Evidence:
  - [pipeline_v4.py](/d:/MyApps/Autobot/autobot/features/pipeline_v4.py#L40)
  - [pipeline_v4.py](/d:/MyApps/Autobot/autobot/features/pipeline_v4.py#L263)
- Problem:
  - `pipeline_v4.py` imports many `_private` helpers from `pipeline_v3.py`
  - v4 behavior is coupled to v3 internal implementation details rather than a shared public utility layer
- Risk:
  - future v3 cleanup can break v4 silently
  - v4-specific invariants are hard to audit because they are spread across v3 internals
- Acceptance:
  - move shared helpers into an explicit shared module
  - leave v4-only invariants and reports inside `pipeline_v4.py`

### T19.6 Add script-level regression harness for orchestration paths

- Severity: Medium
- Evidence:
  - no direct regression coverage found for:
    - `scripts/candidate_acceptance.ps1`
    - `scripts/daily_champion_challenger_v4_for_server.ps1`
    - `scripts/install_server_runtime_services.ps1`
    - `scripts/daily_micro_pipeline_for_server.ps1`
- Problem:
  - most Python core modules are tested
  - orchestration logic carrying promote / restart / report-path / pointer behavior is effectively untested
- Risk:
  - highest-value operational branches regress without signal
- Acceptance:
  - add a lightweight PowerShell smoke harness or Python wrapper tests for:
    - candidate pinning
    - report path resolution
    - bootstrap champion fallback
    - restart target selection

### T19.7 Document and enforce the real terminal point of the automated v4 lane

- Severity: Medium
- Evidence:
  - [daily_champion_challenger_v4_for_server.ps1](/d:/MyApps/Autobot/scripts/daily_champion_challenger_v4_for_server.ps1#L328)
  - [install_server_runtime_services.ps1](/d:/MyApps/Autobot/scripts/install_server_runtime_services.ps1#L144)
  - [cli.py](/d:/MyApps/Autobot/autobot/cli.py#L3956)
- Problem:
  - daily automation promotes and restarts paper units by default
  - live daemon / executor event paths are present but not first-class in the daily loop
- Risk:
  - operators can assume "v4 automation" means live-trading automation, which is not true by default
  - promotion side effects for future live units are optional and easy to misconfigure
- Acceptance:
  - document default terminal point as `paper`
  - define an explicit promote-to-live hook contract before wiring executor/live into the daily lane

### T19.8 Clean legacy platform branches in server-only scripts

- Severity: Low
- Evidence:
  - [daily_micro_pipeline_for_server.ps1](/d:/MyApps/Autobot/scripts/daily_micro_pipeline_for_server.ps1#L53)
- Problem:
  - server script still carries Windows compatibility logic
  - Windows `PYTHONPATH` append uses `:` instead of `;`
- Risk:
  - low runtime impact on current Linux server target
  - but misleading cross-platform branch can fail silently when reused
- Acceptance:
  - either remove Windows branch from server-only script, or fix separator and add a minimal smoke check

## Recommended Execution Order

1. `T19.1`
2. `T19.2`
3. `T19.3`
4. `T19.4`
5. `T19.6`
6. `T19.5`
7. `T19.7`
8. `T19.8`

## First Ticket Recommendation

Start with `T19.1`.

- It is a real runtime behavior bug, not just structure debt.
- It is locally reproducible.
- It is small enough to fix and verify quickly.
- It directly affects paper-lane decision quality under a supported configuration shape.
