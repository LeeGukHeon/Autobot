# Replay Plan 2026-03-21

- Worktree: `d:\MyApps\Autobot_replay_627dacf`
- Branch: `replay/pre_refactor_627dacf`
- Base commit: `627dacf`
- Intent:
  - keep the codebase before the large March 20 refactor/modularization wave
  - reintroduce later fixes and selected functionality in small, reviewable bundles
  - avoid destabilizing `main` while we validate the simpler pre-refactor state machine

## 1. What This Base Already Includes

`627dacf` is already after these major additions:

- `1c90558` gate 00:10 challenger spawn on fresh execution contract
- `85638f5` shared execution contract for live/paper parity
- `2511432` live candidate domain reweighting
- `2d737cd` canary trade-history execution-attempt backfill
- `75f61bc` pooled live/canary execution refresh
- `4cc1446` refresh output naming fix
- `e72a2d6` automated daily live execution policy refresh
- `c6e82b2` live fill-hazard execution policy
- `f64da7c` staged execution frontier and fill-time evidence
- `b302e5b` live entry timeout tuning
- `5a51cfd` canary timeout cap
- `d1570fd` risk control safety layer redesign
- `b28ed3a` support-aware sizing / execution structure gates

That means this replay branch is not a pre-feature branch.
It is specifically a `pre-refactor / pre-modularization` branch.

## 2. Commits To Exclude From First Replay Pass

These are the commits we intentionally do **not** want in the first replay pass because they are mainly refactor, modularization, cleanup, or dashboard polish:

- `cc12534` Refactor lifecycle startup and shared trade artifacts
- `9320d29` Extract candidate acceptance common helpers
- `b3a1fdf` Extract candidate acceptance evidence helpers
- `a647f1d` Extract candidate acceptance runtime helpers
- `fb9506a` Extract candidate acceptance window helpers
- `8031376` Extract candidate acceptance reporting helpers
- `cd7c2e6` Remove v4 candidate acceptance alias script
- `086dcce` Remove unreferenced utility scripts
- `da096c7` Remove unused local utility scripts
- `e498d1c` Remove v3 parallel acceptance path
- `10a78d1` Remove live_v3 preset surface
- `0c06068` Make local center default to v4 only
- all dashboard-only commits unless needed for operator visibility

## 3. Commits To Reapply First

These are the highest-value bugfixes that should likely be replayed on top of `627dacf` even if we avoid the refactor commits:

### 3.1 Execution / acceptance correctness

1. `eec6d48` Fix backtest execution contract snapshot init
2. `948f149` Skip execution contract when learned execution is off
3. `ff81a4a` Use runtime parity for certification backtests
4. `0b38d24` Rebuild live micro state during execution backfill

### 3.2 Live malformed-input hardening

5. `9600467` Ignore malformed public ws micro events
6. `e55b3ca` Guard malformed public ticker events
7. `443a473` Capture public ws malformed payload context

### 3.3 Breaker / reconcile correctness

8. `34d33bf` Auto-clear recovered stuck risk exit breaker
9. `18cde26` Use verified close evidence in reconcile
10. `8f258e5` Normalize live pnl pct for online risk control
11. `25ff9ec` Recover stale live public ws breaker
12. `9117d36` Handle best orders in direct gateway
13. `4fe5b01` Audit live runtime state machine regressions

## 4. Optional Replay Group

These are helpful, but not required to validate the simpler runtime:

- `903cf6f` Add runtime artifact rebuild script
- `6199baa` Add skip-train candidate acceptance rerun mode
- `b025380` Reuse split policy windows in skip-train acceptance

## 5. Suggested Replay Order

Recommended `git cherry-pick` order on this branch:

```powershell
git cherry-pick eec6d48
git cherry-pick 948f149
git cherry-pick ff81a4a
git cherry-pick 0b38d24
git cherry-pick 9600467
git cherry-pick e55b3ca
git cherry-pick 443a473
git cherry-pick 34d33bf
git cherry-pick 18cde26
git cherry-pick 8f258e5
git cherry-pick 25ff9ec
git cherry-pick 9117d36
git cherry-pick 4fe5b01
```

After that, run the live/runtime regression bundle:

```powershell
python -m pytest -q tests/test_live_model_alpha_runtime.py tests/test_live_rollout.py tests/test_live_reconcile.py tests/test_direct_execution_gateway.py tests/test_live_public_ticker_guards.py
```

## 6. Why This Order

- The first four commits stabilize execution-contract behavior in training/backtest/acceptance.
- The next three harden malformed input and add observability.
- The next four repair state-machine closure / breaker recovery / reconcile behavior.
- The last two fix runtime classification and direct order-submit behavior.

## 7. Success Criteria

The replay branch is worth keeping only if:

1. candidate live no longer crashes on malformed public data
2. local stale positions are removed reliably after verified closes
3. stuck exit breaker does not persist after exit completion
4. best-order submit path no longer crashes direct REST execution
5. the simpler pre-refactor topology produces fewer lifecycle regressions than `main`

## 8. Current Next Step

Do **not** switch `main`.

Use this branch/worktree to replay the bugfix bundle above and compare:

- lifecycle stability
- breaker frequency
- operator recoverability
- candidate runtime profitability

against current `main`.

## 9. Current Replay Status

The replay branch has already reapplied the following commits:

- `01d65cd` Fix backtest execution contract snapshot init
- `ac596be` Skip execution contract when learned execution is off
- `6f620d9` Use runtime parity for certification backtests
- `3341c3b` Rebuild live micro state during execution backfill
- `57bf5c8` Ignore malformed public ws micro events
- `3c806e1` Guard malformed public ticker events
- `45b9191` Capture public ws malformed payload context
- `ed817bd` Auto-clear recovered stuck risk exit breaker
- `7848e90` Use verified close evidence in reconcile
- `04b9b70` Normalize live pnl pct for online risk control
- `4ae35a3` Recover stale live public ws breaker
- `80954f1` Handle best orders in direct gateway
- `c2d1980` Audit live runtime state machine regressions

Additional replay-only support commits:

- `6932195` Add replay plan for pre-refactor branch
- `3b672f4` Replay certification preset and trailing exit fixes
- `78614ec` Restore data support surface for replay branch

## 10. Validation Run On Replay Branch

Validated successfully on `replay/pre_refactor_627dacf`:

```powershell
python -m pytest -q tests/test_backtest_model_alpha_integration.py tests/test_paper_engine_model_alpha_integration.py tests/test_candidate_acceptance_certification_lane.py tests/test_execution_attempts_backfill.py
```

- result: `66 passed`

```powershell
python -m pytest -q tests/test_live_model_alpha_runtime.py tests/test_live_public_ticker_guards.py tests/test_live_rollout.py
```

- result: `71 passed`

```powershell
python -m pytest -q tests/test_live_daemon.py tests/test_live_breakers.py tests/test_live_risk_manager.py tests/test_daily_champion_challenger_spawn_handling.py tests/test_t23_2_server_script_contracts.py tests/test_dashboard_server.py
```

- result: `90 passed`

Server-side replay worktree validation on OCI (`/home/ubuntu/MyApps/Autobot_replay_627dacf`) using the shared main `.venv`:

```bash
/home/ubuntu/MyApps/Autobot/.venv/bin/python -m pytest -q tests/test_live_model_alpha_runtime.py tests/test_live_rollout.py tests/test_live_reconcile.py tests/test_direct_execution_gateway.py tests/test_live_public_ticker_guards.py tests/test_backtest_model_alpha_integration.py tests/test_paper_engine_model_alpha_integration.py tests/test_candidate_acceptance_certification_lane.py tests/test_execution_attempts_backfill.py tests/test_live_daemon.py tests/test_live_breakers.py tests/test_live_risk_manager.py tests/test_daily_champion_challenger_spawn_handling.py tests/test_t23_2_server_script_contracts.py tests/test_dashboard_server.py
```

- result: `265 passed`

```powershell
python -m pytest -q tests/test_live_model_alpha_runtime.py tests/test_live_rollout.py tests/test_live_reconcile.py tests/test_direct_execution_gateway.py tests/test_live_public_ticker_guards.py tests/test_backtest_model_alpha_integration.py tests/test_paper_engine_model_alpha_integration.py tests/test_candidate_acceptance_certification_lane.py tests/test_execution_attempts_backfill.py
```

- result: `175 passed`

## 11. Replay Branch Caveat

`627dacf` did not contain a fully tracked `autobot.data` support surface in git.

To make this replay branch boot and test correctly, the following support files had to be restored into the branch:

- `autobot/data/__init__.py`
- `autobot/data/column_mapper.py`
- `autobot/data/duckdb_utils.py`
- `autobot/data/filename_parser.py`
- `autobot/data/ingest_csv_to_parquet.py`
- `autobot/data/inventory.py`
- `autobot/data/manifest.py`
- `autobot/data/schema_contract.py`

This means the replay branch is currently a practical validation branch, not yet a historically pure reconstruction.

## 12. Next Recommended Step

The next useful action is not more blind cherry-picking.

It is one of:

1. run local live/paper smoke commands on this replay branch
2. deploy the replay branch to an isolated server path and compare breaker frequency versus `main`
3. replay a smaller optional group only after smoke results look better than `main`

## 13. Cutover Checklist

If we decide the replay branch is better than `main`, the switch should be a controlled cutover, not a merge.

### 13.1 Preconditions

- replay branch includes latest required hotfixes
- replay paper smoke has recent `LIVE_FEATURES_BUILT` / `MODEL_ALPHA_SELECTION` activity
- no unresolved replay-only boot issues remain
- current production `main` head is recorded for rollback

### 13.2 Server Preparation

1. record current production head in `/home/ubuntu/MyApps/Autobot`
2. stop candidate/challenger-only units first
3. keep champion paper/live state DB snapshots before replacing code
4. confirm timer state before switch

### 13.3 Recommended Switch Shape

1. fast-forward a dedicated cutover branch from `replay/pre_refactor_627dacf`
2. update `/home/ubuntu/MyApps/Autobot` to that cutover branch
3. restart only:
   - `autobot-paper-v4.service`
   - `autobot-paper-v4-challenger.service`
   - `autobot-live-alpha-candidate.service`
4. verify:
   - latest paper run writes events
   - no immediate breaker arm
   - no malformed submit/runtime loop crash
5. only then resume full timer-driven cycle

### 13.4 Rollback Trigger

Rollback immediately if any of these happen after cutover:

- `LIVE_RUNTIME_LOOP_FAILED`
- `LIVE_PUBLIC_WS_STREAM_FAILED`
- `LOCAL_POSITION_MISSING_ON_EXCHANGE`
- `RISK_EXIT_STUCK_MAX_REPLACES`
- paper/live no longer create intents under conditions where current `main` does

### 13.5 Rollback Method

- return `/home/ubuntu/MyApps/Autobot` to recorded production head
- restart paper/candidate units
- restore timer state
- preserve replay worktree for later comparison

## 14. Latest Replay Delta

Latest replay-only sync after initial validation:

- `f9692c7` Clip learned trade-action sizing multipliers

Replay revalidation after this delta:

```powershell
python -m pytest -q tests/test_trade_action_policy.py tests/test_paper_engine_model_alpha_integration.py tests/test_backtest_model_alpha_integration.py tests/test_live_model_alpha_runtime.py
```

- result: `101 passed`
