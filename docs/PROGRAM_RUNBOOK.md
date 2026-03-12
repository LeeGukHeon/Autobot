# AUTOBOT PROGRAM RUNBOOK

- Version: 2026-03-12
- Scope: current codebase in `autobot/`
- Supersedes:
  - `docs/RUNBOOK_LIVE.md`
  - `docs/RUNBOOK_CONTROL_CENTER.md`

## 1. Purpose

This document is the single operational and architectural overview for the current program.

It is not a historical design log. Historical intent remains in:

- `docs/ADR/`
- `docs/TICKETS/`
- `docs/reports/`

Supporting reference docs remain in:

- `docs/API_NOTES.md`
- `docs/CONFIG_SCHEMA.md`
- `docs/EXIT_STATE_CONTRACT.md`

This file explains what the program is now, how the pieces connect, and which contracts are authoritative.

## 2. Current System Summary

The current system is a single-package Upbit trading platform with one shared architecture across:

- offline data collection
- feature generation
- model training and registry
- backtest
- paper runtime
- live runtime
- dashboard / operations

Current source-of-truth package:

- `autobot/`

Current production model lane:

- model family: `train_v4_crypto_cs`
- champion pointer: `champion_v4`
- candidate pointer: `latest_candidate_v4`
- runtime strategy: `model_alpha_v1`
- current learned runtime contract:
  - shared selection policy
  - learned execution profile
  - hold/risk exit family comparison
  - trade-level conditional tail-risk action policy

## 3. What The Program Is And Is Not

The program is:

- a cross-sectional KRW crypto trading system for Upbit
- a unified research-to-runtime stack
- a restart-safe live system with persistent state, journal, risk plans, and breakers
- a governed champion/candidate loop with paper and canary paths

The program is not:

- a multi-exchange platform
- a futures/leveraged system
- a nanosecond/HFT engine
- a single-script research notebook stack

## 4. Package SSOT

Top-level package roles:

- `autobot/backtest`
  - candle-based simulation engine
  - shared strategy adapter and execution simulation
- `autobot/common`
  - shared validation, calibration, evidence, and utility helpers
- `autobot/execution`
  - intent contract and order supervisor logic
- `autobot/features`
  - feature-set construction, especially `feature_set_v4`
- `autobot/live`
  - state DB, reconcile, daemon, rollout, runtime, journal, breakers
- `autobot/models`
  - trainer, registry, predictor, runtime recommendation contracts
- `autobot/paper`
  - live-paper runtime and simulated exchange
- `autobot/risk`
  - live risk manager and config models
- `autobot/strategy`
  - strategy contracts, candidate generation, gates, model-alpha runtime logic
- `autobot/upbit`
  - REST/WS clients and exchange contract handling

Important note:

- `python/autobot/` is not the development SSOT.
- Root `autobot/` is the real package.

## 5. End-To-End System Flow

### 5.1 Data Plane

The program maintains two main market data planes:

- candle/history plane
  - stored under `data/parquet`
  - used by training, backtest, and live/paper feature providers
- public WS / micro plane
  - stored under `data/raw_ws/upbit/public`
  - health and metadata stored under `data/raw_ws/upbit/_meta`
  - used by paper/live runtime freshness checks and micro overlays

Main CLI entry families:

- `python -m autobot.cli data ...`
- `python -m autobot.cli collect ...`
- `python -m autobot.cli micro ...`
- `python -m autobot.cli features ...`

Important command groups in `autobot/cli.py`:

- `data ingest|sniff|validate|inventory`
- `collect plan-candles|candles`
- `collect plan-ticks|ticks`
- `collect plan-ws-public|ws-public`
- `features build|stats`

### 5.2 Feature Plane

Current active strategy lane is based on v4 features.

Key pieces:

- feature contract source: `autobot/features/feature_set_v4.py`
- feature loading and projection: `autobot/models/dataset_loader.py`
- live feature providers:
  - `autobot/paper/live_features_v3.py`
  - `autobot/paper/live_features_v4.py`

Important runtime rule:

- the model feature frame is not just `predictor.feature_columns`
- it must also include runtime auxiliary columns required by learned strategy contracts
- current example:
  - `selection_score`
  - `rv_12`
  - `rv_36`
  - `atr_pct_14`

This wiring is currently resolved through:

- `resolve_model_alpha_runtime_row_columns()` in `autobot/strategy/model_alpha_v1.py`

### 5.3 Training Plane

Main trainer entry:

- `python -m autobot.cli model train --trainer v4_crypto_cs ...`

Current trainer implementation:

- `autobot/models/train_v4_crypto_cs.py`

The trainer produces:

- model bundle
- `train_config.yaml`
- `thresholds.json`
- `selection_recommendations.json`
- `selection_policy.json`
- `selection_calibration.json`
- `runtime_recommendations.json`
- walk-forward / evidence / acceptance artifacts

The most important runtime artifact is:

- `runtime_recommendations.json`

This artifact is the bridge from training to:

- backtest
- paper
- live
- dashboard

### 5.4 Model Registry And Pointers

Registry is under:

- `models/registry/<model_family>/<run_id>/`

Runtime pointer aliases are resolved through registry JSON pointers.

Current important aliases:

- `champion_v4` -> `champion` of `train_v4_crypto_cs`
- `latest_v4` -> `latest` of `train_v4_crypto_cs`
- `latest_candidate_v4` -> `latest_candidate` of `train_v4_crypto_cs`

Relevant code:

- `autobot/models/predictor.py`
- `autobot/live/model_handoff.py`

Promotion is pointer mutation, not direct code mutation.

### 5.5 Backtest

Current backtest engine:

- `autobot/backtest/engine.py`

Backtest characteristics:

- candle-driven simulation
- shared `StrategyOrderIntent` / `StrategyFillEvent` interface
- uses registry-backed predictor and same strategy contracts
- uses simulated exchange and market rules
- supports model-alpha strategy path

Backtest is not a toy path.
It uses the same learned runtime recommendations that paper/live consume.

### 5.6 Paper Runtime

Current paper engine:

- `autobot/paper/engine.py`

Paper characteristics:

- real public market data + simulated execution
- top-N universe scan
- shared predictor loading
- shared `ModelAlphaStrategyV1`
- same exit/action contracts as live
- rolling evidence generation

Current server paper units are installed around:

- `autobot-paper-v4.service`
- `autobot-paper-v4-challenger.service`

### 5.7 Live Runtime

Live runtime is split into two layers.

#### Layer A: live control / sync daemon

Key file:

- `autobot/live/daemon.py`

Responsibilities:

- startup reconcile
- runtime model handoff and pointer health
- ws-public freshness health
- rollout gate
- private WS or polling sync
- breaker evaluation
- persistent runtime checkpoints

#### Layer B: model-alpha strategy runtime

Key file:

- `autobot/live/model_alpha_runtime.py`

Responsibilities:

- load predictor from registry
- build live feature provider
- run `ModelAlphaStrategyV1`
- translate decisions into intents/orders
- manage risk-plan bootstrap and live projection
- sync positions/orders/trade journal after fills/restarts

Current main live units:

- `autobot-live-alpha.service`
- `autobot-live-alpha-candidate.service`

### 5.8 Dashboard

Dashboard server:

- `python -m autobot.dashboard_server`

Files:

- backend: `autobot/dashboard_server.py`
- frontend: `autobot/dashboard_assets/dashboard.js`

Dashboard reads:

- systemd unit state
- model acceptance logs
- rank shadow logs
- paper summaries
- live state DBs
- registry artifacts
- ws-public health metadata

The dashboard is operationally important because it is the read-only synthesis layer across training, paper, live, and rollout.

## 6. Current Strategy Stack

### 6.1 Base Runtime Strategy

Current strategy runtime:

- `ModelAlphaStrategyV1`
- file: `autobot/strategy/model_alpha_v1.py`

This file currently handles:

- selection policy resolution
- learned runtime overrides
- entry sizing
- trade-action application
- exit recommendation interpretation
- fill-to-position state transitions

It is the main runtime policy brain for the v4 lane.

### 6.2 Selection

Selection is no longer just a fixed heuristic threshold.

Current structure:

- model scores
- optional selection calibration
- learned selection recommendations
- normalized selection policy

Artifacts involved:

- `selection_recommendations.json`
- `selection_policy.json`
- `selection_calibration.json`

### 6.3 Trade Action

Current trade-action policy is learned at trade level from OOS replay.

Key file:

- `autobot/models/trade_action_policy.py`

Current model id:

- `conditional_action_linear_quantile_tail_v2`

Current trade-action contract includes:

- state features:
  - `selection_score`
  - `rv_12`
  - `rv_36`
  - `atr_pct_14`
- expected edge
- downside deviation
- expected ES
- expected CTM
- expected action value
- recommended action: `hold` or `risk`
- recommended notional multiplier

Important current principle:

- insufficient state support does not silently degrade to a hidden heuristic
- it returns explicit insufficient-evidence style decisions

### 6.4 Exit Recommendation

Current exit contract is not a single fixed mode.

It uses:

- hold family
- risk family
- family compare
- chosen family
- chosen rule id

Important current fields:

- `chosen_family`
- `chosen_rule_id`
- `family_compare_status`

Current design intent:

- `hold` is a legitimate stopping-rule family
- `risk` is a legitimate stopping-rule family
- the system compares them on a common runtime recommendation contract
- evidence-poor comparisons can abstain rather than quietly defaulting

### 6.5 Execution Overlay And Admissibility

The program separates:

- learned strategy intent
- execution/admissibility constraints

This is important.

The following are exchange/execution layer concerns, not methodology layer concerns:

- tick size
- min total
- fee reserve
- dust remainder
- balance availability

These are handled through the live admissibility path, not by strategy heuristics pretending to be model logic.

## 7. Runtime Contract Artifacts

Current runtime contract files loaded by `ModelPredictor`:

- `train_config.yaml`
- `thresholds.json`
- `selection_recommendations.json`
- `selection_policy.json`
- `selection_calibration.json`
- `runtime_recommendations.json`

Current contract consumer:

- `autobot/models/predictor.py`

Current strategy/runtime normalization path:

- `resolve_runtime_model_alpha_settings()` in `autobot/strategy/model_alpha_v1.py`

This means:

- training output is not just offline reporting
- it directly mutates runtime behavior through normalized contracts

## 8. Live State DB

Current live state DB is the operational SSOT for runtime continuity.

Core tables from `autobot/live/state_store.py`:

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

Operational meanings:

- `positions`
  - current held inventory and attached policy JSONs
- `orders`
  - local/exchange-tracked order state
- `intents`
  - pre-order decisions and admission context
- `risk_plans`
  - active managed exit state machine
- `trade_journal`
  - trade-level audit and realized result view
- `checkpoints`
  - runtime contracts, health, rollout, and resume metadata
- `breaker_state`
  - active risk/control halts
- `order_lineage`
  - replace/cancel ancestry

## 9. Current Exit State Representation

The same exit decision currently appears in several projections:

- `strategy.meta.model_exit_plan`
- `positions.tp_json/sl_json/trailing_json`
- `risk_plans`
- `trade_journal.entry_meta.strategy.meta.model_exit_plan`
- dashboard summaries derived from those records

This works, but it is one of the remaining structural pressure points in the codebase.

The live runtime now actively backfills missing projections to keep them aligned, but the representation is still duplicated rather than perfectly canonical.

Current canonical-contract design for the next hardening slices is documented in:

- `docs/EXIT_STATE_CONTRACT.md`

## 10. Runtime Modes And Pointers

Current server-level mental model:

- champion paper:
  - runs `champion_v4`
- challenger paper:
  - runs `latest_candidate_v4`
- main live:
  - typically pinned to champion
- candidate live canary:
  - pinned to candidate

Important point:

- live runtime pins a concrete run id after startup
- dashboard and health checkpoints compare that pinned run id against pointer state
- if pointer divergence or ws-public staleness appears, breakers can halt new intents

## 11. Rollout Model

Current rollout semantics use:

- `shadow`
- `canary`
- `live`

Relevant files:

- `autobot/live/rollout.py`
- `autobot/live/daemon.py`

Current rollout behavior is not just UI metadata.
It is part of runtime gating for:

- start allowance
- order emission allowance
- target live unit
- test-order freshness

## 12. Server Service Map

Current dashboard-monitored units:

- `autobot-paper-v4.service`
- `autobot-paper-v4-challenger.service`
- `autobot-ws-public.service`
- `autobot-live-alpha.service`
- `autobot-live-alpha-candidate.service`
- `autobot-v4-challenger-spawn.service`
- `autobot-v4-challenger-promote.service`
- `autobot-v4-rank-shadow.service`
- `autobot-v4-challenger-spawn.timer`
- `autobot-v4-challenger-promote.timer`
- `autobot-v4-rank-shadow.timer`
- `autobot-dashboard.service`

Install scripts live under:

- `scripts/install_server_runtime_services.ps1`
- `scripts/install_server_live_runtime_service.ps1`
- `scripts/install_server_dashboard_service.ps1`
- `scripts/install_server_rank_shadow_service.ps1`
- `scripts/install_server_daily_acceptance_service.ps1`
- `scripts/install_server_daily_parallel_acceptance_service.ps1`

## 13. Server Access And Core Ops

Current Oracle server access facts used by operations:

- SSH host:
  - `ubuntu@168.107.44.206`
- Windows SSH executable absolute path:
  - `C:\Windows\System32\OpenSSH\ssh.exe`
- SSH key absolute path:
  - `C:\Users\Administrator\Desktop\OCI_SSH_KEY\ssh-key-2026-03-05.key`
- local helper reference:
  - `C:\Users\Administrator\Desktop\connect_oci.bat`
- server repo root:
  - `/home/ubuntu/MyApps/Autobot`

Hard rule for this environment:

- use the SSH key by absolute path
- do not assume `ssh` is on PATH

Canonical Windows SSH pattern:

```powershell
& "C:\Windows\System32\OpenSSH\ssh.exe" -i "C:\Users\Administrator\Desktop\OCI_SSH_KEY\ssh-key-2026-03-05.key" ubuntu@168.107.44.206
```

Canonical repo update pattern:

```bash
cd /home/ubuntu/MyApps/Autobot
git pull origin main
```

Common service checks:

```bash
systemctl status autobot-paper-v4.service --no-pager
systemctl status autobot-live-alpha.service --no-pager
systemctl status autobot-live-alpha-candidate.service --no-pager
systemctl status autobot-dashboard.service --no-pager
systemctl status autobot-ws-public.service --no-pager
```

Common restarts:

```bash
sudo systemctl restart autobot-paper-v4.service
sudo systemctl restart autobot-live-alpha.service
sudo systemctl restart autobot-live-alpha-candidate.service
sudo systemctl restart autobot-dashboard.service
```

Common logs:

```bash
journalctl -u autobot-paper-v4.service -n 200 --no-pager
journalctl -u autobot-live-alpha.service -n 200 --no-pager
journalctl -u autobot-live-alpha-candidate.service -n 200 --no-pager
journalctl -u autobot-dashboard.service -n 200 --no-pager
```

Useful live candidate DB path:

- `/home/ubuntu/MyApps/Autobot/data/state/live_candidate/live_state.db`

Useful main live DB path:

- `/home/ubuntu/MyApps/Autobot/data/state/live/live_state.db`

Operational intent:

- code changes are made locally
- local changes are committed and pushed to GitHub
- server changes are applied by `git pull origin main`
- services are restarted only if the changed code path requires runtime reload

## 14. Daily Training / Candidate / Promotion Loop

Current training automation is script-driven rather than hidden in a black box.

Key scripts:

- `scripts/daily_champion_challenger_v4_for_server.ps1`
- `scripts/v4_governed_candidate_acceptance.ps1`
- `scripts/v4_promotable_candidate_acceptance.ps1`
- `scripts/v4_scout_candidate_acceptance.ps1`
- `scripts/v4_rank_shadow_candidate_acceptance.ps1`

CLI shortcut:

- `python -m autobot.cli model daily-v4 --mode spawn_only ...`

Important current rule in code:

- `daily-v4` only supports `spawn_only`
- direct runtime mutation is intentionally blocked from that CLI path

This is a deliberate governance guard.

## 15. Local Helper Scripts

The old standalone runbooks are removed, but the helper scripts still exist.

Current interpretation:

- `scripts/AutobotCenter.cmd`
- `scripts/autobot_center.ps1`

These are convenience launchers for local/manual operation.
They are not the authoritative operational contract.

For server truth, prefer:

- systemd unit install scripts under `scripts/install_server_*.ps1`
- CLI entry points in `autobot/cli.py`
- this runbook

## 16. What To Edit For Each Kind Of Change

If changing training artifacts:

- `autobot/models/train_v4_crypto_cs.py`
- `autobot/models/runtime_recommendations.py`
- `autobot/models/runtime_recommendation_contract.py`

If changing runtime strategy behavior:

- `autobot/strategy/model_alpha_v1.py`
- `autobot/models/trade_action_policy.py`

If changing backtest/paper/live feature projection:

- `autobot/models/dataset_loader.py`
- `autobot/paper/live_features_v3.py`
- `autobot/paper/live_features_v4.py`

If changing live continuity or reconcile:

- `autobot/live/daemon.py`
- `autobot/live/model_alpha_runtime.py`
- `autobot/live/reconcile.py`
- `autobot/live/state_store.py`

If changing exit-plan persistence or journal visibility:

- `autobot/live/model_risk_plan.py`
- `autobot/live/trade_journal.py`
- `autobot/dashboard_server.py`
- `autobot/dashboard_assets/dashboard.js`

If changing server unit installation:

- `scripts/install_server_*.ps1`

## 17. Current Strengths

The current codebase is strong in these areas:

- shared architecture across backtest, paper, and live
- registry-driven runtime contracts
- explicit candidate/champion pointer model
- restart-safe live state and reconcile path
- trade journal and dashboard observability
- learned trade-action and learned exit-family comparison already wired into runtime
- reasonable regression coverage on critical paths

## 18. Current Weak Spots

The current codebase is still weak or expensive in these areas:

- very large central files
  - `autobot/cli.py`
  - `autobot/models/train_v4_crypto_cs.py`
  - `autobot/live/model_alpha_runtime.py`
  - `autobot/dashboard_server.py`
  - `autobot/strategy/model_alpha_v1.py`
- duplicated exit-state projections
- duplicated dashboard rendering paths
- some runtime contracts still rely on normalization/backfill glue instead of a single canonical representation

These are software hardening issues more than methodology issues.

## 19. Operational Interpretation

The program should currently be thought of as:

- a production-style beta trading platform
- not a throwaway research project
- but also not yet a fully hardened institutional platform

The architecture is already coherent.
The remaining work is mostly:

- contract unification
- modularization
- end-to-end hardening
- more rigorous ops smoke coverage

## 20. Authoritative Reading Order

For someone new to the codebase, the shortest correct reading order is:

1. this file
2. `README.md`
3. `docs/ADR/0004-package-ssot.md`
4. `autobot/cli.py`
5. `autobot/models/predictor.py`
6. `autobot/models/train_v4_crypto_cs.py`
7. `autobot/strategy/model_alpha_v1.py`
8. `autobot/models/trade_action_policy.py`
9. `autobot/paper/engine.py`
10. `autobot/live/daemon.py`
11. `autobot/live/model_alpha_runtime.py`
12. `autobot/live/state_store.py`
13. `autobot/dashboard_server.py`

## 21. Final Rule

When there is a conflict between an old note and current code:

- current code wins
- this runbook should be updated to match the code
- old ticket/report text should be treated as historical context, not current truth
