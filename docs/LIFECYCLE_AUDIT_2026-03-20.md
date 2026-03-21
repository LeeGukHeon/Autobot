# Lifecycle Audit 2026-03-20

- Date: 2026-03-20
- Scope: collect -> feature/label -> train -> acceptance/backtest -> paper -> live candidate/main -> risk/breaker -> recovery/continuity
- Goal: verify that the implemented lifecycle logic is coherent end to end, identify overlapping/legacy logic, and define cleanup priorities without breaking safety-critical behavior

Post-incident follow-up:

- [LIVE_RUNTIME_POSTMORTEM_2026-03-21.md](d:/MyApps/Autobot/docs/LIVE_RUNTIME_POSTMORTEM_2026-03-21.md)

## 1. Top-Level Lifecycle Map

Autobot currently behaves as one production-style pipeline split into these layers:

1. Data collection and microstructure aggregation
2. Feature/label dataset generation
3. Training and registry artifact generation
4. Acceptance / champion-challenger orchestration
5. Backtest runtime
6. Paper runtime
7. Live runtime
8. Risk / breaker / rollout / continuity

Current production lane is `v4`:

- trainer: `train_v4_crypto_cs`
- shared model pointers:
  - `champion_v4`
  - `latest_candidate_v4`
- always-on primary paper:
  - `autobot-paper-v4.service`
- candidate/challenger paper:
  - `autobot-paper-v4-challenger.service`
- candidate live:
  - `autobot-live-alpha-candidate.service`
- main live:
  - `autobot-live-alpha.service`

High-level reference:

- [PROGRAM_RUNBOOK.md](d:/MyApps/Autobot/docs/PROGRAM_RUNBOOK.md)
- [ROADMAP.md](d:/MyApps/Autobot/docs/ROADMAP.md)

## 2. Data Plane

### 2.1 Collection layers

- candles
  - `autobot/data/collect/plan_candles.py`
  - `autobot/data/collect/candles_collector.py`
  - `autobot/data/collect/validate_candles_api.py`
- raw ticks
  - `autobot/data/collect/plan_ticks.py`
  - `autobot/data/collect/ticks_collector.py`
  - `autobot/data/collect/validate_ticks.py`
- raw public websocket
  - `autobot/data/collect/plan_ws_public.py`
  - `autobot/data/collect/ws_public_collector.py`
  - `autobot/data/collect/validate_ws_public.py`
- `micro_v1`
  - `autobot/data/micro/merge_micro_v1.py`
  - `autobot/data/micro/validate_micro_v1.py`

### 2.2 Feature/label layers

- v4 primary feature path:
  - `autobot/features/pipeline_v4.py`
  - `autobot/features/feature_set_v4.py`
- v3/v2 remain present and actively loadable:
  - `autobot/features/pipeline_v3.py`
  - `autobot/features/pipeline_v2.py`
- dataset loading contract:
  - `autobot/models/dataset_loader.py`

### 2.3 Data-plane findings

- `micro_v1` is upstream for both paper/live execution-related logic and training datasets. This is not optional background work.
- v2/v3/v4 all still contain legacy partition/file fallbacks (`part.parquet`, nested date partitions, cleanup helpers). These are compatibility paths, not obvious delete-now targets.
- v4 still contains explicit bootstrap label warmup logic from v3-era assumptions. This looks intentional, but it is a candidate for later simplification once the label path is fully stabilized.

## 3. Training / Registry / Orchestration

### 3.1 Trainer entrypoints

- v4 path:
  - [cli.py](d:/MyApps/Autobot/autobot/cli.py):2332
  - [cli_train_v4_helpers.py](d:/MyApps/Autobot/autobot/cli_train_v4_helpers.py):19
  - [train_v4_crypto_cs.py](d:/MyApps/Autobot/autobot/models/train_v4_crypto_cs.py)
- v3/v2 remain CLI-visible:
  - [cli.py](d:/MyApps/Autobot/autobot/cli.py):2407
  - [cli.py](d:/MyApps/Autobot/autobot/cli.py):2454

### 3.2 v4 trainer lifecycle

The v4 trainer currently does:

1. dataset load and split
2. search budget application
3. main fit
4. walk-forward / CPCV-lite
5. execution acceptance
6. runtime recommendations
7. governance / promotion artifacts

Relevant files:

- `autobot/models/train_v4_core.py`
- `autobot/models/search_budget.py`
- `autobot/models/train_v4_execution.py`
- `autobot/models/execution_acceptance.py`
- `autobot/models/train_v4_artifacts.py`

### 3.3 Daily orchestration

Production schedule currently observed:

- 23:40 `autobot-live-execution-policy.timer`
- 23:50 `autobot-v4-challenger-promote.timer`
- 00:10 `autobot-v4-challenger-spawn.timer`
- 04:40 `autobot-v4-rank-shadow.timer`

Installer/source files:

- `scripts/install_server_live_execution_policy_service.ps1`
- `scripts/install_server_daily_split_challenger_services.ps1`
- `scripts/daily_champion_challenger_v4_for_server.ps1`
- `scripts/install_server_rank_shadow_service.ps1`

### 3.4 Training/orchestration findings

- `candidate_acceptance.ps1` remains the monolithic acceptance/orchestration core. The v3/v4 wrapper family mostly rewrites arguments.
- `daily_candidate_acceptance_for_server.ps1` still exists as a secondary/manual path and therefore remains a drift risk.
- The new shared execution contract now enters:
  - 23:40 refresh
  - live runtime checkpoint load
  - paper runtime artifact load
  - backtest / execution acceptance artifact load
  - 00:10 pre-refresh / freshness gate
- Naming is legacy now:
  - checkpoint name: `live_execution_policy_model`
  - artifact filename: `combined_live_execution_policy.json`
  even though they now carry a broader `execution_contract`.

## 4. Backtest / Paper Lifecycle

### 4.1 Paper runtime

- entrypoint:
  - [engine.py](d:/MyApps/Autobot/autobot/paper/engine.py#L3302)
- primary engine:
  - [engine.py](d:/MyApps/Autobot/autobot/paper/engine.py#L92)
- strategy path:
  - [engine.py](d:/MyApps/Autobot/autobot/paper/engine.py#L1683)
  - [engine.py](d:/MyApps/Autobot/autobot/paper/engine.py#L1536)
  - [engine.py](d:/MyApps/Autobot/autobot/paper/engine.py#L1891)
- exchange/fill path:
  - [sim_exchange.py](d:/MyApps/Autobot/autobot/paper/sim_exchange.py#L109)
  - [fill_model.py](d:/MyApps/Autobot/autobot/paper/fill_model.py#L14)

### 4.2 Backtest runtime

- entrypoint:
  - [engine.py](d:/MyApps/Autobot/autobot/backtest/engine.py#L2501)
- primary engine:
  - [engine.py](d:/MyApps/Autobot/autobot/backtest/engine.py#L92)
- strategy path:
  - [engine.py](d:/MyApps/Autobot/autobot/backtest/engine.py#L1966)
  - [engine.py](d:/MyApps/Autobot/autobot/backtest/engine.py#L1240)
  - [engine.py](d:/MyApps/Autobot/autobot/backtest/engine.py#L1359)
- exchange/fill path:
  - [exchange.py](d:/MyApps/Autobot/autobot/backtest/exchange.py#L12)
  - `CandleFillModel`

### 4.3 Backtest/paper findings

- paper and backtest now load the same execution contract artifact on startup.
- paper and backtest now apply the same contract-driven action selection for `model_alpha_v1` bid entries.
- paper still is not full live parity:
  - it is no longer allowed to immediate-fill `PASSIVE_MAKER`
  - but it still uses ticker-touch semantics after submission
- backtest still uses candle-touch semantics after the next bar.
- `PaperExecutionGateway` and `BacktestExecutionGateway` duplicate a large amount of execution logic and are clear consolidation candidates.

## 5. Live Runtime / State / Recovery

### 5.1 Main live lifecycle

- strategy runtime entry:
  - [model_alpha_runtime.py](d:/MyApps/Autobot/autobot/live/model_alpha_runtime.py#L135)
- startup sync:
  - [model_alpha_runtime.py](d:/MyApps/Autobot/autobot/live/model_alpha_runtime.py#L592)
- predictor bind:
  - [model_alpha_runtime.py](d:/MyApps/Autobot/autobot/live/model_alpha_runtime.py#L658)
- feature provider / strategy / risk manager build:
  - [model_alpha_runtime.py](d:/MyApps/Autobot/autobot/live/model_alpha_runtime.py#L674)
  - [model_alpha_runtime.py](d:/MyApps/Autobot/autobot/live/model_alpha_runtime.py#L690)
  - [model_alpha_runtime.py](d:/MyApps/Autobot/autobot/live/model_alpha_runtime.py#L705)
- execution resolution:
  - [model_alpha_runtime_execute.py](d:/MyApps/Autobot/autobot/live/model_alpha_runtime_execute.py#L492)
- order submit path:
  - [model_alpha_runtime_execute.py](d:/MyApps/Autobot/autobot/live/model_alpha_runtime_execute.py#L994)

### 5.2 Persistence surfaces

- authoritative store:
  - [state_store.py](d:/MyApps/Autobot/autobot/live/state_store.py#L232)
- tables:
  - `intents`
  - `risk_plans`
  - `trade_journal`
  - `execution_attempts`
  - `checkpoints`
  - `breaker_state`
  - `breaker_events`
  - `order_lineage`

### 5.3 Live findings

- The sync/bootstrap stack is split across:
  - `model_alpha_runtime.py`
  - `daemon.py`
  - `model_alpha_runtime_bootstrap.py`
  with overlapping responsibilities.
- `trade_journal`, `closed_order_backfill`, and `reconcile` all repair lifecycle state after the fact. This is robust but ownership is blurred.
- Candidate/main live split is still driven mostly by service installer environment, not one typed deployment contract.
- Historical rows in `intents` / `execution_attempts` still require compatibility handling because metadata richness differs across versions.

## 6. Risk / Breakers / Rollout / Continuity

### 6.1 Risk/rollout stack

- breaker contract:
  - [breakers.py](d:/MyApps/Autobot/autobot/live/breakers.py)
- rollout contract:
  - [rollout.py](d:/MyApps/Autobot/autobot/live/rollout.py)
- reconcile:
  - [reconcile.py](d:/MyApps/Autobot/autobot/live/reconcile.py)
- risk manager:
  - [live_risk_manager.py](d:/MyApps/Autobot/autobot/risk/live_risk_manager.py)
- dynamic overlay:
  - [dynamic_exit_overlay.py](d:/MyApps/Autobot/autobot/common/dynamic_exit_overlay.py)

### 6.2 Risk/continuity findings

- `daemon.py` and `model_alpha_runtime.py` duplicate startup lifecycle orchestration and should eventually be unified.
- `attach_default_risk` in reconcile is a compatibility/continuity branch, not the primary model-managed path.
- `model_alpha_projection.py` overlaps conceptually with reconcile and risk-plan syncing and is a cleanup candidate after behavior is proven elsewhere.
- `LIVE_BREAKER_ACTIVE` is used in rollout status but explicitly stripped before breaker arming; the breaker-side mapping now looks like a defensive legacy entry.
- Breaker reasons are merge-based and can persist longer than operators expect unless the precise clear path is hit.

## 7. Cross-Cutting Invariants To Preserve

The following invariants appear safety-critical:

1. reconcile must happen before model bind
2. risk resume must happen before rollout/start
3. `HALT_NEW_INTENTS` must still allow protective exits
4. rollout status and breaker state must remain distinguishable
5. canary constraints must not silently diverge across gate layers
6. `intents`, `trade_journal`, `execution_attempts`, and `orders` must remain recoverable after restart

## 8. Legacy / Cleanup Candidates

### 8.1 High-value cleanup candidates

1. Unify duplicated startup lifecycle orchestration
   - `autobot/live/daemon.py`
   - `autobot/live/model_alpha_runtime.py`
2. Unify execution gateways
   - `autobot/paper/engine.py`
   - `autobot/backtest/engine.py`
3. Rename legacy execution-contract storage names
   - checkpoint: `live_execution_policy_model`
   - artifact: `combined_live_execution_policy.json`
4. Clarify ownership of post-trade repair
   - `reconcile.py`
   - `closed_order_backfill.py`
   - `trade_journal.py`
5. Reduce script duplication
   - `candidate_acceptance.ps1`
   - manual/secondary daily wrappers

### 8.2 Keep-for-now compatibility branches

These look legacy-like but should not be removed yet:

- `attach_default_risk` reconcile fallback
- bootstrap-only split-policy branches
- v2/v3 trainer CLI entrypoints
- candidate/main live installer split
- historical metadata compatibility handling in `intents` / `execution_attempts`

## 9. Repo-Wide Drift Surfaces

### 9.1 Secondary/manual paths that still exist

These are not the primary OCI production path anymore, but they are still present and therefore should be treated as compatibility/manual surfaces, not assumed-dead code:

- daily acceptance wrappers:
  - `scripts/daily_candidate_acceptance_for_server.ps1`
- legacy/manual installers:
  - `scripts/install_server_daily_acceptance_service.ps1`
 - v3-visible runtime surfaces:
   - CLI `--trainer` still exposes `v2_micro` / `v3_mtf_micro`
   - runtime feature/provider internals for `v3` still exist behind code/test surfaces, even though the public `live_v3` paper preset surface has been removed
- local Windows scheduling helpers:
  - `scripts/register_scheduled_tasks.ps1`
  - `scripts/unregister_scheduled_tasks.ps1`
  - `scripts/autobot_center.ps1` still has `schtasks` fallback logic
- package compatibility mirror:
  - `python/autobot/` remains intentionally minimized, but ADR/runbook still keep it as a compatibility surface

### 9.2 Naming drift that blocks blind cleanup

- The actual payload is now `execution_contract`, but important storage names still use `execution_policy` terminology:
  - checkpoint name: `live_execution_policy_model`
  - artifact path: `logs/live_execution_policy/combined_live_execution_policy.json`
  - refresh payload policy string: `live_execution_policy_refresh_v1`
  - runtime/order metadata field name: `execution_policy`
- Main live DB path is still dual-surfaced:
  - config/CLI/live installer default to `data/state/live_state.db`
  - dashboard also supports `data/state/live/live_state.db`

### 9.3 Deploy/ops cleanup preconditions

- `install_server_daily_split_challenger_services.ps1` explicitly disables:
  - `autobot-daily-micro.timer`
  - `autobot-daily-v4-accept.timer`
  so deleting the old units/scripts before operators stop relying on them removes fallback/rollback paths.
- Unit content changes are installer-driven on OCI. A `git pull` alone is not sufficient when a systemd unit contract changes.
- Service names are embedded across scripts, tests, dashboard logic, and docs. Renaming or deleting units requires a coordinated migration, not a repo-only cleanup.
- DB path cleanup requires coordinated changes across config defaults, live installers, dashboard DB discovery, and tests.

## 10. Test Gaps

Missing or weakly-covered integration areas:

- service start -> reconcile -> resume -> runtime bind -> signal -> submit -> fill -> close
- restart with active breaker + open position + active risk plan + pending order
- execution-contract provenance consistency between:
  - DB checkpoint
  - filesystem artifact
  - acceptance/backtest report
  - paper runtime startup metadata
- 23:50 promote using contract freshness/provenance
- rank-shadow interaction with the new execution contract

Additional coverage note:

- coverage is strong for script contracts and component-level runtime behavior:
  - `tests/test_t23_2_server_script_contracts.py`
  - `tests/test_daily_champion_challenger_spawn_handling.py`
  - `tests/test_live_model_alpha_runtime.py`
  - `tests/test_t23_2_continuity_pack.py`
- however, those checks are still split by layer; there is not yet one production-style test that proves the full timer -> refresh -> spawn/promote -> reconcile/resume -> submit/fill/close chain under breaker and rollout state together

## 11. Recommended Cleanup Order

### Phase A - Audit / provenance hardening

- persist execution-contract provenance everywhere it is consumed
- add cross-surface freshness/hash checks

### Phase B - Startup lifecycle consolidation

- extract a shared startup/reconcile/resume/model-bind/rollout pipeline
- remove duplicated orchestration between `daemon.py` and `model_alpha_runtime.py`

### Phase C - Execution gateway consolidation

- extract a shared execution-gateway base for paper/backtest
- keep only venue/fill-model differences in thin adapters

### Phase D - State ownership cleanup

- define single-owner write rules for:
  - `trade_journal`
  - `execution_attempts`
  - `risk_plans`
  - post-trade repair

### Phase E - Naming cleanup

- migrate `execution_policy` names to `execution_contract` names once consumers are updated

## 12. Bottom Line

The system is already coherent enough to operate, but it is not yet clean.

The biggest cleanup opportunities are not single dead functions. They are:

- duplicated lifecycle orchestration
- duplicated execution gateway logic
- blurred persistence ownership
- legacy naming that no longer matches actual behavior

This means the right next step is not blind deletion. It is:

- lifecycle audit documentation
- provenance hardening
- then targeted consolidation, one ownership boundary at a time
