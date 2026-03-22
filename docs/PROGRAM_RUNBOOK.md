# AUTOBOT PROGRAM RUNBOOK

- Version: 2026-03-23
- Scope: current `autobot/` codebase, server automation scripts, and observed OCI runtime state on 2026-03-23
- Purpose: this is the single operational reference for architecture, lifecycle, services, deployment, recovery, and current sharp edges
- Docs index:
  - `docs/README.md`
- Current analysis:
  - `docs/FOUNDATIONAL_FAILURE_MODES_2026-03-23.md`
  - `docs/TRAINING_PIPELINE_RESEARCH_COMPARE_2026-03-23.md`
  - `docs/RUNTIME_EXECUTION_FINDINGS_AND_ACTION_PLAN_2026-03-23.md`

## 1. What This Program Is

Autobot is a single-repo Upbit KRW spot trading platform with one shared architecture across:

- market data collection
- microstructure aggregation
- feature generation
- model training and registry management
- candidate acceptance / challenger orchestration
- paper runtime
- live runtime
- dashboard / operations

Current primary family:

- trainer: `train_v4_crypto_cs`
- runtime strategy: `ModelAlphaStrategyV1`
- main aliases:
  - `champion_v4`
  - `latest_v4`
  - `latest_candidate_v4`

This is not a notebook-only research repo.
It already behaves like a production-style beta trading platform with persistent state, restart-safe recovery, candidate/champion separation, rollout controls, breakers, and operator-facing diagnostics.

## 2. Truth Hierarchy

When sources disagree, use this order:

1. current code
2. current OCI runtime state
3. this file
4. `docs/CONFIG_SCHEMA.md`
5. current dated analysis docs in `docs/`
6. older root docs, ADRs, tickets, and reports

Important doc rule:

- `docs/README.md` is the top-level docs guide
- historical design notes are useful background, but they are not the operational SSOT

## 3. Source Of Truth

Main package SSOT:

- root `autobot/`

Important top-level directories:

- `autobot/`
  - real application package
- `config/`
  - runtime and training defaults
- `scripts/`
  - server installers, scheduled loops, acceptance wrappers, helpers
- `docs/`
  - runbooks, current analysis, ADRs, tickets, reports
- `models/registry/`
  - trained model runs and family pointers
- `data/`
  - parquet datasets, raw WS/ticks, paper outputs, state DBs
- `logs/`
  - acceptance, challenger, rollout, execution-policy, audit, reports
- `tests/`
  - regression and contract coverage

Important note:

- `python/autobot/` is not the development SSOT
- root `autobot/` is the real package

## 4. Repo Map By Responsibility

- `autobot/data`
  - candles, ticks, ws-public collection, micro merge/validate
- `autobot/features`
  - v1/v2/v3/v4 feature pipelines and label contracts
- `autobot/models`
  - dataset loading, trainer, registry, runtime recommendations, governance, execution policy
- `autobot/backtest`
  - candle-based simulation
- `autobot/paper`
  - public-data paper runtime and simulated exchange
- `autobot/live`
  - reconcile, daemon, rollout, runtime, state store, journal, execution attempts, breakers
- `autobot/risk`
  - live risk manager
- `autobot/strategy`
  - strategy policy, micro gate/order policy, operational overlay
- `autobot/upbit`
  - REST and WS clients, auth, rate-limit handling
- `autobot/dashboard_server.py`
  - operational backend

## 5. End-To-End Lifecycle

### 5.1 Data Lifecycle

The data plane has four main layers.

1. Candles

- planner: `autobot/data/collect/plan_candles.py`
- collector: `autobot/data/collect/candles_collector.py`
- validator: `autobot/data/collect/validate_candles_api.py`
- storage: `data/parquet/candles_*`

2. Raw ticks

- planner: `autobot/data/collect/plan_ticks.py`
- collector: `autobot/data/collect/ticks_collector.py`
- validator: `autobot/data/collect/validate_ticks.py`
- storage: `data/raw_ticks/upbit/trades`

3. Raw public WS

- planner: `autobot/data/collect/plan_ws_public.py`
- collector: `autobot/data/collect/ws_public_collector.py`
- validator: `autobot/data/collect/validate_ws_public.py`
- storage: `data/raw_ws/upbit/public`
- health/meta: `data/raw_ws/upbit/_meta`

4. `micro_v1`

- builder: `autobot/data/micro/merge_micro_v1.py`
- validator: `autobot/data/micro/validate_micro_v1.py`
- storage: `data/parquet/micro_v1`

Operational rule:

- the daily micro pipeline is upstream for v3/v4 feature builds, acceptance, paper micro overlays, and live freshness decisions
- it is not optional background work

### 5.2 Feature Lifecycle

Feature families:

- legacy: v1
- micro-aware: v2
- multi-TF + micro mandatory: v3
- current primary lane: v4

Current primary feature contract:

- pipeline: `autobot/features/pipeline_v4.py`
- feature set: `autobot/features/feature_set_v4.py`

Feature datasets write:

- partitioned parquet
- `_meta/manifest.parquet`
- `_meta/feature_spec.json`
- `_meta/label_spec.json`
- `_meta/build_report.json`

Runtime and training depend on those artifacts rather than implicit column guessing.

### 5.3 Training Lifecycle

Main entry:

- `python -m autobot.cli model train --trainer v4_crypto_cs ...`

Primary implementation:

- `autobot/models/train_v4_crypto_cs.py`

High-level flow:

1. load feature dataset through `autobot/models/dataset_loader.py`
2. build train/valid/test plus walk-forward splits
3. run booster sweep and primary fit
4. compute thresholds and leaderboard row
5. save the core run into the registry
6. compute walk-forward, factor-block, runtime recommendations, execution acceptance, promotion evidence, and lane governance
7. persist runtime / governance artifacts
8. append experiment ledger summary
9. emit train report into `logs/`

Important run artifacts used later by paper/live/dashboard:

- `train_config.yaml`
- `thresholds.json`
- `selection_recommendations.json`
- `selection_policy.json`
- `selection_calibration.json`
- `runtime_recommendations.json`
- `execution_acceptance_report.json`
- `promotion_decision.json`
- `trainer_research_evidence.json`
- `economic_objective_profile.json`
- `lane_governance.json`
- `decision_surface.json`

### 5.4 Registry And Pointers

Registry root:

- `models/registry/<model_family>/<run_id>/`

Family pointers:

- `latest.json`
- `latest_candidate.json`
- `champion.json`

Important behavior:

- `save_run()` always advances `latest`
- v4 advances `latest_candidate` automatically for `run_scope=scheduled_daily`
- promotion is pointer mutation, not code mutation
- manual promote can later overwrite a run's `promotion_decision.json`

Observed pointer state on 2026-03-23:

- `latest = latest_candidate = champion = 20260322T093201Z-s42-da19a911`
- current `champion.json` includes `promotion_mode = manual`

Operational interpretation:

- current champion status is not the same thing as trainer-evidence pass
- use current analysis docs before assuming the current champion is a fully evidence-passing champion

### 5.5 Acceptance / Champion-Challenger Lifecycle

Main acceptance wrappers:

- `scripts/candidate_acceptance.ps1`
- `scripts/v4_scout_candidate_acceptance.ps1`
- `scripts/v4_promotable_candidate_acceptance.ps1`
- `scripts/v4_rank_shadow_candidate_acceptance.ps1`
- `scripts/v4_governed_candidate_acceptance.ps1`

Main orchestration wrapper:

- `scripts/daily_champion_challenger_v4_for_server.ps1`

Split daily loop on server:

- `23:50` previous challenger promotion window
- `00:10` new challenger spawn window
- `04:40` rank-shadow cycle

Typical daily flow:

1. refresh daily data / micro
2. train candidate
3. run certification backtest
4. optionally run bootstrap-only or paper-soak paths
5. install or update the challenger paper unit pinned to the candidate run
6. next day promotion window decides whether the challenger becomes champion
7. if promoted, restart the configured champion target units

### 5.6 Paper Lifecycle

Paper entry:

- `python -m autobot.cli paper alpha ...`

Primary engine:

- `autobot/paper/engine.py`

Paper characteristics:

- consumes real public market data
- uses shared `ModelAlphaStrategyV1`
- uses registry-backed predictor
- simulates fills through `PaperExecutionGateway`
- writes run artifacts under `data/paper/runs`
- produces summary data used by acceptance and dashboard

Observed current OCI paper units on 2026-03-23:

- `autobot-paper-v4.service`
- `autobot-paper-v4-replay.service`

Historical / optional paper units still referenced by scripts and older docs:

- `autobot-paper-v4-challenger.service`
- `autobot-paper-alpha.service`

Do not assume those units exist on the current OCI host without checking `systemctl`.

### 5.7 Live Lifecycle

Live startup has two cooperating layers.

Layer A: continuity / control / sync

- `autobot/live/daemon.py`

Responsibilities:

- startup reconcile
- runtime contract binding
- ws-public freshness check
- rollout evaluation
- breaker evaluation
- cancel/apply actions
- private WS or polling sync

Layer B: model runtime

- `autobot/live/model_alpha_runtime.py`

Responsibilities:

- load predictor from the runtime contract
- build the live feature provider
- run `ModelAlphaStrategyV1`
- generate intents
- run admissibility, trade gate, micro policy, and execution policy
- submit orders or shadow intents
- supervise open orders
- backfill journal / closed orders / risk plans
- restore state after restart

Primary live units:

- `autobot-live-alpha.service`
- `autobot-live-alpha-candidate.service`

Observed auxiliary / debug live unit on OCI:

- `autobot-live-alpha-replay-shadow.service`

Current rollout commands:

- `python -m autobot.cli live rollout status`
- `python -m autobot.cli live rollout arm`
- `python -m autobot.cli live rollout disarm`
- `python -m autobot.cli live rollout test-order`
- `python -m autobot.cli live breaker status`
- `python -m autobot.cli live breaker arm`
- `python -m autobot.cli live breaker clear`

### 5.8 Dashboard Lifecycle

Dashboard entry:

- `python -m autobot.dashboard_server`

Backend:

- `autobot/dashboard_server.py`

Frontend:

- `autobot/dashboard_assets/`

Dashboard pulls from:

- systemd unit state
- registry pointers and recent model artifacts
- challenger / acceptance / rank-shadow logs
- paper summaries
- live state DBs
- ws-public health metadata
- Upbit public/private API for account and ticker-derived summaries

Operational note:

- the dashboard is not a pure local-file reader
- it has credential and rate-limit implications

## 6. State And Artifact SSOT

Important live DB tables in `autobot/live/state_store.py`:

- `positions`
- `orders`
- `intents`
- `risk_plans`
- `trade_journal`
- `execution_attempts`
- `checkpoints`
- `breaker_state`
- `breaker_events`
- `order_lineage`
- `run_locks`

Important log / artifact roots:

- `logs/model_v4_acceptance`
- `logs/model_v4_challenger`
- `logs/model_v4_rank_shadow_cycle`
- `logs/live_execution_policy`
- `logs/live_execution_override_audit`
- `logs/live_rollout`
- `logs/operational_overlay`

Important rollout artifacts:

- `logs/live_rollout/latest.json`
- scoped rollout files such as `latest.autobot_live_alpha.service.json`
- test-order artifacts
- arm / disarm archives

Important execution-policy / audit artifacts:

- `logs/live_execution_policy/combined_live_execution_policy.json`
- `logs/live_execution_policy/latest_refresh.json`
- `logs/live_execution_override_audit/latest.json`
- `logs/live_execution_override_audit/latest.md`

Important checkpoints in `data/state/*/live_state.db`:

- `live_runtime_contract`
- `live_rollout_status`
- `live_execution_policy_model`

Important DB-path caveat:

- code, dashboard, and server units still support both:
  - legacy main DB: `data/state/live_state.db`
  - canonical-style main DB: `data/state/live/live_state.db`
- candidate DB currently uses:
  - `data/state/live_candidate/live_state.db`

## 7. OCI Runtime Topology

Canonical server repo root:

- `/home/ubuntu/MyApps/Autobot`

Operator access pattern:

- use the local helper `Desktop/connect_oci.bat`
- use `git push` locally, then `git pull origin main` on OCI
- if unit content changes, rerun installer scripts; `git pull` alone is not enough

Systemd is the real scheduler on OCI.

Observed on 2026-03-23:

- cron for `ubuntu` was empty
- deployed server Git SHA matched:
  - `86ce4cafb51aa7ae3466d4840c8526892ed7e96a`

### 7.1 Continuous Services

- `autobot-paper-v4.service`
- `autobot-paper-v4-replay.service`
- `autobot-live-alpha.service`
- `autobot-live-alpha-candidate.service`
- `autobot-ws-public.service`
- `autobot-dashboard.service`
- `autobot-storage-retention.service`
  - timer-driven oneshot

Historical or optional units still referenced by scripts:

- `autobot-paper-v4-challenger.service`
- `autobot-live-alpha-replay-shadow.service`

### 7.2 Timers

- `autobot-live-execution-policy.timer`
  - `23:40`
- `autobot-v4-challenger-promote.timer`
  - `23:50`
- `autobot-v4-challenger-spawn.timer`
  - `00:10`
- `autobot-v4-rank-shadow.timer`
  - `04:40`
- `autobot-storage-retention.timer`
  - `06:30`

### 7.3 Observed OCI State On 2026-03-23

Observed current service state:

- `autobot-paper-v4.service`: active
- `autobot-paper-v4-replay.service`: active
- `autobot-live-alpha.service`: inactive
- `autobot-live-alpha-candidate.service`: active
- `autobot-ws-public.service`: active
- `autobot-dashboard.service`: active
- `autobot-v4-rank-shadow.service`: failed
- `autobot-live-alpha-replay-shadow.service`: failed
- `autobot-v4-challenger-spawn.service`: activating / start-running when inspected

Observed current registry / governance state:

- `latest = latest_candidate = champion = 20260322T093201Z-s42-da19a911`
- `champion.json` records `promotion_mode = manual`

Observed rollout status after the 2026-03-23 candidate runtime restart:

- mode: `canary`
- `breaker_clear = true`
- `start_allowed = true`
- `order_emission_allowed = true`

Important caveat:

- `logs/live_rollout/latest.json` may be stale relative to the runtime DB checkpoint `live_rollout_status`
- prefer the CLI rollout status or DB checkpoint over a single static file

### 7.4 Current Execution Diagnostic Snapshot

Observed from the latest execution override audit on 2026-03-23:

- run-level recommendation:
  - `recommended_price_mode = JOIN`
- recent final execution attempts:
  - `169` total
  - `119` filled
  - `50` missed
- final submit mode mix:
  - `PASSIVE_MAKER = 120`
  - `JOIN = 48`
  - `CROSS_1T = 1`
- run recommendation match rate:
  - about `28.4%`
- direct demotion count observed inside `live_execution_policy`:
  - `JOIN -> PASSIVE_MAKER = 30`
- candidate live trade journal:
  - `CLOSED = 120`
  - total realized pnl quote about `-759.98`

Operational interpretation:

- current weakest production surface is execution conversion, not obviously raw alpha direction
- current candidate live should be debugged from execution policy, rollout, and breaker evidence first

## 8. Server Install Scripts

Main installers:

- `scripts/install_server_runtime_services.ps1`
  - paper units
- `scripts/install_server_live_runtime_service.ps1`
  - live units
- `scripts/install_server_ws_public_service.ps1`
  - public WS daemon unit
- `scripts/install_server_dashboard_service.ps1`
  - dashboard
- `scripts/install_server_daily_split_challenger_services.ps1`
  - spawn/promote timers
- `scripts/install_server_rank_shadow_service.ps1`
  - rank-shadow timer
- `scripts/install_server_storage_retention_service.ps1`
  - retention timer

Execution-policy artifact refresh:

- `scripts/refresh_live_execution_policy.ps1`

## 9. Daily Operations

### 9.1 Basic Checks

Use these on OCI:

```bash
cd /home/ubuntu/MyApps/Autobot
git status --short
git rev-parse HEAD
systemctl status autobot-paper-v4.service --no-pager
systemctl status autobot-paper-v4-replay.service --no-pager
systemctl status autobot-live-alpha.service --no-pager
systemctl status autobot-live-alpha-candidate.service --no-pager
systemctl status autobot-ws-public.service --no-pager
systemctl status autobot-dashboard.service --no-pager
systemctl list-timers --all | grep autobot
```

### 9.2 Logs

```bash
journalctl -u autobot-paper-v4.service -n 200 --no-pager
journalctl -u autobot-paper-v4-replay.service -n 200 --no-pager
journalctl -u autobot-live-alpha.service -n 200 --no-pager
journalctl -u autobot-live-alpha-candidate.service -n 200 --no-pager
journalctl -u autobot-live-execution-policy.service -n 200 --no-pager
journalctl -u autobot-v4-challenger-spawn.service -n 200 --no-pager
journalctl -u autobot-v4-challenger-promote.service -n 200 --no-pager
journalctl -u autobot-v4-rank-shadow.service -n 200 --no-pager
journalctl -u autobot-dashboard.service -n 200 --no-pager
```

### 9.3 Rollout, Breaker, And Execution Checks

```bash
python -m autobot.cli live rollout status
python -m autobot.cli live breaker status
python scripts/report_live_execution_override_audit.py --db-path data/state/live_candidate/live_state.db --registry-root models/registry --model-family train_v4_crypto_cs --output-dir logs/live_execution_override_audit
/snap/powershell/current/opt/powershell/pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/refresh_live_execution_policy.ps1 -ProjectRoot /home/ubuntu/MyApps/Autobot -PythonExe /home/ubuntu/MyApps/Autobot/.venv/bin/python
```

Typical rollout actions:

```bash
python -m autobot.cli live rollout arm --mode canary --target-unit autobot-live-alpha-candidate.service --arm-token <TOKEN>
python -m autobot.cli live rollout test-order --market KRW-BTC --side bid --ord-type limit --price 5000 --volume 1
python -m autobot.cli live rollout disarm --arm-token <TOKEN> --note "parking main live"
```

### 9.4 Deploy Pattern

1. make change locally
2. run tests locally
3. commit and push
4. SSH to OCI
5. `git pull origin main`
6. rerun installer scripts if systemd unit content changed
7. restart affected services
8. verify rollout, breaker, state DB, execution-policy artifact, and dashboard

## 10. Recovery Playbooks

### 10.1 Main Live Is Inactive

Check in this order:

1. `systemctl status autobot-live-alpha.service --no-pager`
2. `python -m autobot.cli live rollout status`
3. `python -m autobot.cli live breaker status`
4. inspect scoped rollout artifacts under `logs/live_rollout/`
5. verify whether the unit is intentionally parked or unexpectedly halted

If the unit is parked by rollout:

- arm the correct rollout contract
- run `live rollout test-order`
- confirm `start_allowed=true`
- restart the live unit

### 10.2 Candidate Pointer Or Candidate Live Fails To Start

Check:

- `models/registry/train_v4_crypto_cs/latest_candidate.json`
- candidate run directory existence
- required run artifacts:
  - `runtime_recommendations.json`
  - `promotion_decision.json`
  - `trainer_research_evidence.json`
  - `lane_governance.json`
- `journalctl -u autobot-live-alpha-candidate.service -n 200 --no-pager`

If the pointer is bad, repoint or retrain before restarting candidate paper/live units.

### 10.3 Challenger Loop Gets Stuck

Inspect:

- `logs/model_v4_challenger/current_state.json`
- `logs/model_v4_challenger/latest.json`
- `logs/model_v4_challenger/latest_promote_cutover.json`
- spawn/promote service logs
- `logs/model_v4_rank_shadow_cycle/latest.json`
- `logs/model_v4_rank_shadow_cycle/latest_governance_action.json`

Current note:

- the latest observed `rank_shadow` cycle on OCI failed and fell back to the default cls lane governance action

### 10.4 WS Public Looks Healthy But Trading Logic Still Feels Stale

Do not trust only `updated_at_ms`.

Also inspect:

- actual trade/orderbook receive timestamps
- latest raw part timestamps under `data/raw_ws/upbit/public`
- `ws_validate_report.json`
- `aggregate_report.json`

Current freshness logic is metadata-heavy, so manual validation may still be required.

### 10.5 DB Path Confusion

If dashboard/live summaries do not match expectations:

- inspect unit environment for `AUTOBOT_LIVE_STATE_DB_PATH`
- verify whether the unit is using legacy or canonical DB path
- verify dashboard is reading the same path

### 10.6 Execution Policy Or Maker Bias Looks Wrong

Check in this order:

1. `logs/live_execution_policy/combined_live_execution_policy.json`
2. `python scripts/report_live_execution_override_audit.py --db-path data/state/live_candidate/live_state.db --registry-root models/registry --model-family train_v4_crypto_cs --output-dir logs/live_execution_override_audit`
3. `python -m autobot.cli live rollout status`
4. `python -m autobot.cli live breaker status`
5. `journalctl -u autobot-live-alpha-candidate.service -n 200 --no-pager`

Interpretation order:

- if rollout blocks new intents, fix rollout/breaker first
- if run-level recommendation is `JOIN` but actual submit mix remains maker-heavy, inspect `live_execution_policy` before blaming `micro_order_policy`
- if recent positive-edge entries are still missed, refresh the shared execution-policy artifact before retuning alpha

## 11. What To Edit By Concern

If changing data collection:

- `autobot/data/collect/*`
- `autobot/data/micro/*`
- `scripts/daily_micro_pipeline*.ps1`

If changing feature contracts:

- `autobot/features/pipeline_v4.py`
- `autobot/features/feature_set_v4.py`
- `autobot/models/dataset_loader.py`

If changing training / registry / governance:

- `autobot/models/train_v4_crypto_cs.py`
- `autobot/models/train_v4_*`
- `autobot/models/registry.py`
- `scripts/candidate_acceptance.ps1`

If changing paper runtime:

- `autobot/paper/engine.py`
- `autobot/paper/live_features_v3.py`
- `autobot/paper/live_features_v4.py`
- `autobot/models/live_execution_policy.py`
- `autobot/live/execution_policy_refresh.py`
- `scripts/refresh_live_execution_policy.ps1`

If changing live runtime / continuity:

- `autobot/live/model_alpha_runtime.py`
- `autobot/live/model_alpha_runtime_execute.py`
- `autobot/live/daemon.py`
- `autobot/live/reconcile.py`
- `autobot/live/state_store.py`

If changing risk behavior:

- `autobot/risk/live_risk_manager.py`
- `autobot/live/model_risk_plan.py`
- `autobot/live/breakers.py`

If changing rollout / promotion:

- `autobot/live/rollout.py`
- `scripts/daily_champion_challenger_v4_for_server.ps1`
- `scripts/install_server_live_runtime_service.ps1`
- `scripts/install_server_daily_split_challenger_services.ps1`

If changing dashboard:

- `autobot/dashboard_server.py`
- `autobot/dashboard_assets/*`

If changing execution diagnostics / runtime evidence:

- `autobot/ops/live_execution_override_audit.py`
- `scripts/report_live_execution_override_audit.py`

## 12. Current Known Sharp Edges

These are current known issues, not operator mistakes.

- dashboard is network-exposed by default unless constrained externally
- live/paper runtime micro overlay is not the same as offline `micro_v1`
- ws-public freshness logic still leans too much on metadata update times
- v4 pointer mutation still happens before all post-train artifacts are fully durable
- current champion may be a manual promote and not a trainer-evidence-passing champion
- current live execution contract still shows maker bias in meaningful parts of the state space
- `logs/live_rollout/latest.json` can drift from `live_rollout_status` checkpoint truth
- candidate live recent evidence still shows a meaningful missed-entry loop and negative realized pnl
- split-policy history probes can disturb family pointers if not isolated
- `shadow` mode can still emit real protective exits when live risk is enabled
- startup / candidate / challenger state still depends on multiple filesystem artifacts rather than one transactional state machine

## 13. Recommended Reading Order

For a new maintainer, the shortest correct reading order is:

1. `docs/README.md`
2. this file
3. `docs/CONFIG_SCHEMA.md`
4. `docs/FOUNDATIONAL_FAILURE_MODES_2026-03-23.md`
5. `docs/TRAINING_PIPELINE_RESEARCH_COMPARE_2026-03-23.md`
6. `docs/RUNTIME_EXECUTION_FINDINGS_AND_ACTION_PLAN_2026-03-23.md`
7. `autobot/cli.py`
8. `autobot/models/predictor.py`
9. `autobot/models/train_v4_crypto_cs.py`
10. `scripts/candidate_acceptance.ps1`
11. `scripts/daily_champion_challenger_v4_for_server.ps1`
12. `autobot/paper/engine.py`
13. `autobot/live/daemon.py`
14. `autobot/live/model_alpha_runtime.py`
15. `autobot/live/model_alpha_runtime_execute.py`
16. `autobot/live/reconcile.py`
17. `autobot/live/state_store.py`
18. `autobot/dashboard_server.py`

## 14. Final Rule

When docs and code disagree:

- current code wins
- current OCI runtime state beats historical ticket text
- this runbook should be updated to match reality
- older ticket and report docs should be treated as methodology history, not operational truth
