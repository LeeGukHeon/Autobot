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
