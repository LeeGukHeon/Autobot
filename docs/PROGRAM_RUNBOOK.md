# AUTOBOT PROGRAM RUNBOOK

- Version: 2026-03-18
- Scope: current `autobot/` codebase, server automation scripts, and observed OCI runtime state on 2026-03-18
- Purpose: this is the single reference for architecture, lifecycle, operations, recovery, and current known sharp edges

## 1. What This Program Is

Autobot is a single-repo Upbit KRW spot trading platform that keeps one shared architecture across:

- market data collection
- microstructure aggregation
- feature generation
- model training and registry management
- certification / candidate acceptance
- paper runtime
- live runtime
- dashboard / operations

Current production lane is the v4 family:

- trainer: `train_v4_crypto_cs`
- primary strategy runtime: `ModelAlphaStrategyV1`
- main aliases:
  - `champion_v4`
  - `latest_v4`
  - `latest_candidate_v4`

This is not a notebook-only research repo. It already behaves like a production-style beta trading platform with:

- persistent live state
- restart-safe reconcile / resume
- candidate vs champion separation
- paper and live runtime handoff
- rollout / breaker / dashboard concepts

## 2. Source Of Truth

Main SSOT is root package `autobot/`.

Important top-level directories:

- `autobot/`
  - real application package
- `config/`
  - runtime and training defaults
- `scripts/`
  - server installers, daily loops, acceptance wrappers, local helpers
- `docs/`
  - ADRs, tickets, reports, this runbook
- `models/registry/`
  - trained model families, runs, pointers
- `data/`
  - parquet datasets, raw WS/ticks, paper outputs, state DBs
- `logs/`
  - acceptance, challenger, rollout, reports, calibration
- `tests/`
  - regression coverage for contracts and critical paths

Important note:

- `python/autobot/` is not the development SSOT.
- root `autobot/` is the real package.

## 3. Repo Map By Responsibility

Core package areas:

- `autobot/data`
  - candles, ticks, ws-public raw collection
  - micro merge / validate / stats
- `autobot/features`
  - v1/v2/v3/v4 feature pipelines
- `autobot/models`
  - dataset loading, v4 trainer, registry, runtime recommendations, governance
- `autobot/backtest`
  - candle-based simulation
- `autobot/paper`
  - public-data paper runtime and simulated exchange
- `autobot/live`
  - reconcile, daemon, rollout, runtime, state store, journal, breakers
- `autobot/risk`
  - live risk manager
- `autobot/strategy`
  - strategy policy, micro gate/order policy, runtime overlays
- `autobot/upbit`
  - REST and WS clients, auth, rate limit handling
- `autobot/dashboard_server.py`
  - operational read-only dashboard backend

## 4. End-To-End Lifecycle

### 4.1 Data Lifecycle

The data plane has four layers.

1. Candles

- planned by `autobot/data/collect/plan_candles.py`
- collected by `autobot/data/collect/candles_collector.py`
- validated by `autobot/data/collect/validate_candles_api.py`
- stored under `data/parquet/candles_*`

2. Raw ticks

- planned by `autobot/data/collect/plan_ticks.py`
- collected by `autobot/data/collect/ticks_collector.py`
- validated by `autobot/data/collect/validate_ticks.py`
- stored under `data/raw_ticks/upbit/trades`

3. Raw public WS

- planned by `autobot/data/collect/plan_ws_public.py`
- collected by `autobot/data/collect/ws_public_collector.py`
- validated by `autobot/data/collect/validate_ws_public.py`
- raw data stored under `data/raw_ws/upbit/public`
- health and reports stored under `data/raw_ws/upbit/_meta`

4. `micro_v1`

- merged by `autobot/data/micro/merge_micro_v1.py`
- validated by `autobot/data/micro/validate_micro_v1.py`
- stored under `data/parquet/micro_v1`

Operationally, the server’s “daily micro” step is not optional background noise. It is the upstream dependency for:

- v3/v4 feature builds
- candidate acceptance
- paper runtime micro overlays
- live runtime freshness / gate decisions

### 4.2 Feature Lifecycle

Feature families:

- legacy: v1
- micro-aware: v2
- multi-TF + micro mandatory: v3
- current primary lane: v4

Current primary feature contract is v4:

- pipeline: `autobot/features/pipeline_v4.py`
- feature set definition: `autobot/features/feature_set_v4.py`

Feature builds write:

- partitioned parquet feature dataset
- `_meta/manifest.parquet`
- `_meta/feature_spec.json`
- `_meta/label_spec.json`
- `_meta/build_report.json`

Training and runtime depend on those artifacts, not on implicit column guessing alone.

### 4.3 Training Lifecycle

Main entry:

- `python -m autobot.cli model train --trainer v4_crypto_cs ...`

Primary implementation:

- `autobot/models/train_v4_crypto_cs.py`

High-level flow:

1. load feature dataset through `autobot/models/dataset_loader.py`
2. build train/valid/test and walk-forward splits
3. run booster sweep and primary fit
4. compute thresholds and leaderboard row
5. save core run into registry
6. compute walk-forward, CPCV-lite, factor-block, runtime recommendations, execution acceptance, promotion evidence, lane governance
7. persist runtime/governance artifacts
8. append experiment ledger summary
9. emit train report into `logs/`

Important registry artifacts consumed later by paper/live/dashboard:

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

### 4.4 Registry And Pointers

Registry root:

- `models/registry/<model_family>/<run_id>/`

Family pointers:

- `latest.json`
- `latest_candidate.json`
- `champion.json`

Important behavior:

- `save_run()` in `autobot/models/registry.py` always advances `latest`
- v4 only advances `latest_candidate` automatically for `run_scope=scheduled_daily`
- promotion is pointer mutation, not code mutation

Live/paper do not load “the trainer”. They load a concrete registry run resolved from:

- alias
- pointer
- or pinned `run_id`

### 4.5 Acceptance / Champion-Challenger Lifecycle

Main acceptance wrapper family:

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
4. optionally run paper soak or bootstrap-only path
5. install challenger paper unit pinned to candidate run
6. next day promotion window decides whether yesterday’s challenger becomes champion
7. if promoted, restart configured champion target units

### 4.6 Paper Lifecycle

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

Primary OCI paper units:

- `autobot-paper-v4.service`
- `autobot-paper-v4-challenger.service`

Observed extra installed unit on OCI as of 2026-03-18:

- `autobot-paper-alpha.service`
  - legacy / extra installed unit
  - not the primary v4 lane

### 4.7 Live Lifecycle

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

- load predictor from pinned runtime contract
- build live feature provider
- run `ModelAlphaStrategyV1`
- generate intents
- run admissibility / trade gate / micro gate / risk control
- submit orders or shadow intents
- supervise open orders
- backfill journal / closed orders / risk plans
- restore state after restart

Primary live units:

- `autobot-live-alpha.service`
- `autobot-live-alpha-candidate.service`

Current rollout commands exist in CLI and are operationally important:

- `python -m autobot.cli live rollout status`
- `python -m autobot.cli live rollout arm`
- `python -m autobot.cli live rollout disarm`
- `python -m autobot.cli live rollout test-order`
- `python -m autobot.cli live breaker status`
- `python -m autobot.cli live breaker arm`
- `python -m autobot.cli live breaker clear`

### 4.8 Dashboard Lifecycle

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
- Upbit public/private API for account- and ticker-derived summaries

This last point matters operationally:

- the dashboard is not a pure local-file reader
- it has credential and rate-limit implications

## 5. State And Artifact SSOT

Important live DB tables in `autobot/live/state_store.py`:

- `positions`
- `orders`
- `intents`
- `risk_plans`
- `trade_journal`
- `checkpoints`
- `breaker_state`
- `breaker_events`
- `order_lineage`
- `run_locks`

Important log / artifact roots:

- `logs/model_v4_acceptance`
- `logs/model_v4_challenger`
- `logs/model_v4_rank_shadow_cycle`
- `logs/live_rollout`
- `logs/operational_overlay`

Important rollout artifacts:

- `logs/live_rollout/latest.json`
- scoped live rollout files such as `latest.autobot_live_alpha.service.json`
- test-order artifacts
- arm / disarm archives

Important current DB-path caveat:

- code, dashboard, and server units still support both:
  - legacy main DB: `data/state/live_state.db`
  - canonical-style main DB: `data/state/live/live_state.db`
- candidate DB currently uses:
  - `data/state/live_candidate/live_state.db`

## 6. OCI Runtime Topology

Canonical server repo root:

- `/home/ubuntu/MyApps/Autobot`

Operator access pattern:

- use the local helper `Desktop/connect_oci.bat`
- use `git push` locally, then `git pull origin main` on OCI
- if unit content changes, rerun the installer script; `git pull` alone is not enough

Systemd is the real scheduler on OCI.

Observed on 2026-03-18:

- cron for `ubuntu` was empty
- local and server Git SHA both matched `159de4b7019bef95fd59ebe3e6eb9d10d1e41da2`

### 6.1 Continuous Services

- `autobot-paper-v4.service`
- `autobot-paper-v4-challenger.service`
- `autobot-live-alpha.service`
- `autobot-live-alpha-candidate.service`
- `autobot-ws-public.service`
- `autobot-dashboard.service`
- `autobot-storage-retention.service`
  - timer-driven oneshot service

### 6.2 Timers

- `autobot-v4-challenger-promote.timer`
  - `23:50`
- `autobot-v4-challenger-spawn.timer`
  - `00:10`
- `autobot-v4-rank-shadow.timer`
  - `04:40`
- `autobot-storage-retention.timer`
  - `06:30`

### 6.3 Observed OCI State On 2026-03-18

Observed from live OCI inspection on 2026-03-18:

- `autobot-paper-v4.service`: active
- `autobot-paper-v4-challenger.service`: active
- `autobot-live-alpha.service`: inactive
- `autobot-live-alpha-candidate.service`: active
- `autobot-ws-public.service`: active
- `autobot-dashboard.service`: active

Observed rollout snapshot on OCI:

- global latest rollout artifact targeted `autobot-live-alpha-candidate.service`
- main live scoped rollout artifact was explicitly disarmed with note:
  - `main live parked pending promote evidence`
- candidate live rollout was armed in `canary` mode but had breaker-active state

Operational interpretation:

- main live being inactive on OCI is currently consistent with rollout parking
- candidate live being active does not mean order emission is allowed
- rollout status must be checked before assuming a live unit can trade

## 7. Server Install Scripts

Main installers in repo:

- `scripts/install_server_runtime_services.ps1`
  - paper units
- `scripts/install_server_live_runtime_service.ps1`
  - live units
- `scripts/install_server_dashboard_service.ps1`
  - dashboard
- `scripts/install_server_daily_split_challenger_services.ps1`
  - spawn/promote split timers
- `scripts/install_server_rank_shadow_service.ps1`
  - rank-shadow timer
- `scripts/install_server_storage_retention_service.ps1`
  - retention timer

Important gap:

- there is currently no in-repo OCI installer for `autobot-ws-public.service`
- the unit exists on server, but its installation contract is not represented in this repo

## 8. Daily Operations

### 8.1 Basic Checks

Use these on OCI:

```bash
cd /home/ubuntu/MyApps/Autobot
git status --short
systemctl status autobot-paper-v4.service --no-pager
systemctl status autobot-paper-v4-challenger.service --no-pager
systemctl status autobot-live-alpha.service --no-pager
systemctl status autobot-live-alpha-candidate.service --no-pager
systemctl status autobot-ws-public.service --no-pager
systemctl status autobot-dashboard.service --no-pager
systemctl list-timers --all | grep autobot
```

### 8.2 Logs

```bash
journalctl -u autobot-paper-v4.service -n 200 --no-pager
journalctl -u autobot-paper-v4-challenger.service -n 200 --no-pager
journalctl -u autobot-live-alpha.service -n 200 --no-pager
journalctl -u autobot-live-alpha-candidate.service -n 200 --no-pager
journalctl -u autobot-v4-challenger-spawn.service -n 200 --no-pager
journalctl -u autobot-v4-challenger-promote.service -n 200 --no-pager
journalctl -u autobot-v4-rank-shadow.service -n 200 --no-pager
journalctl -u autobot-dashboard.service -n 200 --no-pager
```

### 8.3 Rollout And Breaker Checks

```bash
python -m autobot.cli live rollout status
python -m autobot.cli live breaker status
```

Typical rollout actions:

```bash
python -m autobot.cli live rollout arm --mode canary --target-unit autobot-live-alpha-candidate.service --arm-token <TOKEN>
python -m autobot.cli live rollout test-order --market KRW-BTC --side bid --ord-type limit --price 5000 --volume 1
python -m autobot.cli live rollout disarm --arm-token <TOKEN> --note "parking main live"
```

### 8.4 Deploy Pattern

1. make change locally
2. run tests locally
3. commit and push
4. SSH to OCI
5. `git pull origin main`
6. rerun installer script if systemd unit content changed
7. restart affected services
8. verify rollout/breaker/state DB/dashboard

## 9. Recovery Playbooks

### 9.1 Main Live Is Inactive

Check in this order:

1. `systemctl status autobot-live-alpha.service --no-pager`
2. `python -m autobot.cli live rollout status`
3. `python -m autobot.cli live breaker status`
4. inspect scoped rollout artifact under `logs/live_rollout/`
5. verify whether the unit is intentionally parked or unexpectedly halted

If the unit is parked by rollout:

- arm the correct rollout contract
- run `live rollout test-order`
- confirm `start_allowed=true`
- restart the live unit

### 9.2 Candidate Pointer Or Candidate Live Fails To Start

Check:

- `models/registry/train_v4_crypto_cs/latest_candidate.json`
- candidate run directory existence
- required artifacts in that run:
  - `runtime_recommendations.json`
  - `promotion_decision.json`
  - `trainer_research_evidence.json`
  - `lane_governance.json`
- `journalctl -u autobot-live-alpha-candidate.service -n 200 --no-pager`

If the pointer is bad, repoint or retrain before restarting paper/live candidate units.

### 9.3 Challenger Loop Gets Stuck

Inspect:

- `logs/model_v4_challenger/current_state.json`
- `logs/model_v4_challenger/latest.json`
- `logs/model_v4_challenger/latest_promote_cutover.json`
- spawn/promote service logs

Current challenger orchestration depends on these artifacts. If they are stale or contradictory, fix state before rerunning timers.

### 9.4 WS Public Looks Healthy But Trading Logic Still Feels Stale

Do not trust only `updated_at_ms`.

Also inspect:

- actual trade/orderbook receive timestamps
- latest raw part timestamps under `data/raw_ws/upbit/public`
- `ws_validate_report.json`
- `aggregate_report.json`

Current freshness logic is metadata-heavy, so manual validation may be required.

### 9.5 DB Path Confusion

If dashboard/live summaries do not match expectations:

- inspect unit environment for `AUTOBOT_LIVE_STATE_DB_PATH`
- verify whether the unit is using legacy or canonical DB path
- verify dashboard is reading the same path

## 10. What To Edit By Concern

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

## 11. Current Known Sharp Edges

These are current known issues, not operator mistakes.

- dashboard is network-exposed by default unless the operator constrains it externally
- `autobot-ws-public.service` install contract is missing from repo
- live/paper runtime micro overlay is not the same as offline `micro_v1`
- ws-public freshness breaker currently relies too much on metadata update times
- v4 pointer mutation happens before all post-train artifacts are fully durable
- split-policy history probes can disturb family pointers if not isolated
- `shadow` mode can still emit real protective exits when live risk is enabled
- startup / candidate / challenger state depends on several filesystem artifacts, not one transactional state machine

## 12. Recommended Reading Order

For a new maintainer, the shortest correct reading order is:

1. this file
2. `README.md`
3. `autobot/cli.py`
4. `autobot/models/predictor.py`
5. `autobot/models/train_v4_crypto_cs.py`
6. `scripts/candidate_acceptance.ps1`
7. `scripts/daily_champion_challenger_v4_for_server.ps1`
8. `autobot/paper/engine.py`
9. `autobot/live/daemon.py`
10. `autobot/live/model_alpha_runtime.py`
11. `autobot/live/reconcile.py`
12. `autobot/live/state_store.py`
13. `autobot/dashboard_server.py`

## 13. Final Rule

When docs and code disagree:

- current code wins
- current OCI systemd state beats historical ticket text
- this runbook should be updated to match reality
- older ticket and report docs should be treated as methodology history, not operational truth
