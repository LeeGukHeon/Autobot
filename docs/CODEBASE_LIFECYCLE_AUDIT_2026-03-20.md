# Codebase Lifecycle Audit 2026-03-20

- Date: 2026-03-20
- Scope: collection -> features -> training -> acceptance -> backtest -> paper -> candidate live -> main live -> risk -> recovery
- Goal: verify the currently implemented lifecycle, identify stale or superseded logic, and rank cleanup candidates

## 1. Active Production Lifecycle

Current production-oriented path is:

1. data collection / micro aggregation
2. feature build
3. `00:10` candidate train + acceptance
4. challenger paper spawn
5. `23:40` execution contract refresh
6. `23:50` previous challenger promotion check
7. candidate live canary runtime

Observed server timers:

- `23:40` `autobot-live-execution-policy.timer`
- `23:50` `autobot-v4-challenger-promote.timer`
- `00:10` `autobot-v4-challenger-spawn.timer`
- `04:40` `autobot-v4-rank-shadow.timer`

Current service roles:

- `autobot-paper-v4.service`: champion paper
- `autobot-paper-v4-challenger.service`: candidate/challenger paper
- `autobot-live-alpha-candidate.service`: candidate live canary
- `autobot-live-alpha.service`: parked/inactive main live

## 2. Lifecycle Map By Layer

### 2.1 Data / feature layer

Primary active feature lane is `v4`.

Important current paths:

- raw public WS / ticks / candles under `autobot/data`
- `micro_v1` merge/validate under `autobot/data/micro`
- v4 feature build under `autobot/features/pipeline_v4.py`

Status:

- active and required
- not a cleanup target

### 2.2 Training / acceptance layer

Primary trainer:

- `autobot/models/train_v4_crypto_cs.py`

Current orchestration:

- `scripts/candidate_acceptance.ps1`
- `scripts/daily_champion_challenger_v4_for_server.ps1`
- `scripts/install_server_daily_split_challenger_services.ps1`

Execution contract relation:

- execution acceptance/backtest now reads the shared execution contract artifact
- `00:10` spawn path now refreshes and gates on execution contract freshness

### 2.3 Backtest / paper layer

Backtest:

- `autobot/backtest/engine.py`
- `autobot/backtest/exchange.py`
- `autobot/backtest/fill_model.py`

Paper:

- `autobot/paper/engine.py`
- `autobot/paper/sim_exchange.py`
- `autobot/paper/fill_model.py`

Current state after parity work:

- paper/backtest now read the shared execution contract artifact
- paper no longer immediately fills passive-maker orders on submit
- paper/backtest use the same action selector family as live

### 2.4 Live layer

Primary runtime:

- `autobot/live/model_alpha_runtime.py`
- `autobot/live/model_alpha_runtime_execute.py`
- `autobot/live/reconcile.py`
- `autobot/live/state_store.py`

Current state:

- live loads execution contract from DB checkpoint
- candidate live still runs under canary constraints
- breaker / rollout / reconcile remain critical lifecycle gates

### 2.5 Risk / recovery layer

Primary components:

- `autobot/risk/live_risk_manager.py`
- `autobot/common/dynamic_exit_overlay.py`
- `autobot/live/breakers.py`

Current state:

- live exit management is still distinct from paper/backtest
- this is intentional for now, but it remains a parity hotspot

## 3. Ranked Cleanup Candidates

The list below is ranked by cleanup usefulness and safety.

### A. Safe To Clean Up Now

1. Duplicate `_normalize_run_scope()` in `autobot/models/search_budget.py`

- Evidence:
  - two definitions existed in the same module
  - the later definition overwrote the earlier one
  - the earlier definition was dead
- Status:
  - cleaned in this pass

### B. Needs Migration First

1. `live_execution_policy_model` checkpoint/file naming

- Current reality:
  - the payload is no longer just a fill policy
  - it now carries a broader `execution_contract`
- Why not delete now:
  - live runtime, scripts, and tests still reference the old checkpoint/service naming
- Suggested migration:
  - keep current name for compatibility
  - later rename to `live_execution_contract_model`
  - preserve a compatibility reader for one migration window

2. `model` field inside the execution refresh payload

- Current reality:
  - payload now includes both:
    - legacy `model`
    - new `execution_contract`
- Why not delete now:
  - old readers/tests may still expect `payload.model`
- Suggested migration:
  - keep until all readers consume `execution_contract`

3. `autobot-daily-v4-accept.*` timer/service path

- Evidence:
  - still has installer and tests
  - explicitly disabled by split challenger install
  - documented as no longer primary
- Why not delete now:
  - still useful as manual fallback / utility wrapper
- Suggested migration:
  - keep until operators no longer use delayed acceptance mode

4. `autobot-daily-micro.*` naming surface

- Evidence:
  - installer rewires it to the current v4 challenger loop
  - the unit name is legacy, but the behavior is current
- Why not delete now:
  - external operational contracts may still depend on the unit names
- Suggested migration:
  - rename only with an explicit service migration plan

5. `python/autobot/` compatibility mirror

- Evidence:
  - ADR 0004 marks root `autobot/` as SSOT
  - mirror contains only deprecation guidance
- Why not delete now:
  - tooling or external docs may still point at it
- Suggested migration:
  - keep until every external tool path has been checked

6. v3 runtime / preset / trainer surfaces

- Evidence:
  - CLI still exposes `v3_mtf_micro`
  - `live_v3` preset still exists
  - tests and docs still rely on v3 as fallback/baseline
- Why not delete now:
  - still used as benchmark and fallback
- Suggested migration:
  - retire only after formal v3 decommission decision

7. `candidate_v4` alias surface

- Evidence:
  - alias still maps to `latest_candidate_v4`
  - CLI, live daemon fallback, scripts, and docs still reference it
- Why not delete now:
  - this is compatibility glue, not dead code
- Suggested migration:
  - keep until alias cleanup is explicitly scheduled

### C. Keep Intentionally

1. Separate champion / challenger / candidate live roles

- This is not legacy duplication.
- These roles are separate lifecycle states and should remain explicit.

2. Acceptance fixed-breadth compare contract

- This still serves a distinct purpose:
  - apples-to-apples candidate vs champion comparison
- It should not be removed just because paper/live execution parity improved.

3. Rank-shadow lane

- This is an intentional non-promotable analysis lane.
- Not a cleanup target.

## 4. Risky Areas That Still Need Audit Before Cleanup

These are not immediate removal targets, but they are likely simplification candidates later.

1. Search budget runtime profile knobs

- Existing review evidence already suggests parts of this policy surface are effectively dead or collapsed to `compact`.
- Keep for now, but simplify only after reading all callers and tests together.

2. Installer proliferation

- There are multiple installers for:
  - runtime
  - live runtime
  - daily acceptance
  - split challenger services
  - parallel acceptance
- Many are still deliberate because they support different install shapes.
- Cleanup requires an explicit support-matrix decision first.

3. Old docs/tickets referencing no-longer-primary paths

- Historical ADRs/tickets should not be deleted casually.
- They are stale operational guidance candidates, not source cleanup candidates.

## 5. Immediate Follow-Up Sequence

Recommended next passes:

1. full lifecycle read-through of data/feature lineage
2. full lifecycle read-through of live reconcile / recovery / continuity
3. prune more safe-now dead code with tests
4. separate "compatibility aliases" from "real runtime surfaces" in docs and scripts

## 6. Current Audit Conclusion

The codebase is not in a state where large-scale deletion is safe yet.

Current reality:

- there are several legacy names and compatibility aliases
- there are several superseded operational entrypoints
- but most of them still serve either:
  - manual fallback
  - installer compatibility
  - documented migration history
  - baseline benchmarking

Therefore the right cleanup policy is:

- delete only dead duplicate internals now
- keep compatibility surfaces until their consumers are retired
- treat timer/service name changes as migrations, not cleanups
