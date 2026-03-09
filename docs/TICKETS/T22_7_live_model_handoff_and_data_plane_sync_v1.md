# T22.7 - Live Model Handoff And Data Plane Sync v1

## Goal
- Make the live runtime consume the same promoted `champion_v4` model pointer as the paper champion lane.
- Make live runtime cut over on the same daily promote event that updates `champion_v4`.
- Keep live runtime, daily training, and the always-on `ws-public` data plane synchronized so they do not drift into separate timelines.

## Why This Ticket Exists
- The current daily promote loop already has the right direction:
  - promote writes `champion_v4`
  - runtime target units can be restarted via `PromotionTargetUnits`
- That is not sufficient for live operations.
- Live also needs:
  - explicit pinned model-run continuity
  - restart-safe model handoff metadata
  - exact linkage to the same public-data collection plane used by the daily pipeline
  - divergence detection when live runtime is still on an older run after a promote

## Exact Invariants
### 1. One promotion pointer
- `champion_v4` is the single source of truth for both:
  - champion paper runtime
  - live runtime
- No separate live-only model pointer may silently diverge from `champion_v4`.

### 2. One promote cutover event
- When the daily promote loop accepts a challenger:
  - `champion_v4` is updated once
  - the same promote event restarts all promotion target units
  - paper and live must observe the same promoted model run id

### 3. One public-data plane
- `autobot-ws-public.service` remains the always-on public market-data plane.
- Daily training at `00:10` uses the datasets derived from this same data plane.
- Live runtime health must record the latest `ws-public` checkpoint and treat excessive staleness as a breaker condition.

### 4. Restart-safe pinned runtime
- Live runtime startup must:
  - resolve `champion_v4`
  - record the resolved model `run_id`
  - persist that pinned run id into live state/checkpoints
- After restart, the daemon must know:
  - previous pinned run id
  - current `champion_v4` run id
  - whether a promote happened while it was down

### 5. No silent drift
- If live runtime is still executing on a different pinned run id than current `champion_v4`, it must not continue silently.
- This must surface as an explicit health/divergence state:
  - `MODEL_POINTER_DIVERGENCE`

## Scope
### 1. Live runtime contract
- Add one explicit live runtime unit contract:
  - model ref source: `champion_v4`
  - pinned resolved run id
  - live rollout mode
  - startup reconcile mode
  - required ws-public freshness budget

### 2. Promote-to-live target hook
- Extend the promote path so `PromotionTargetUnits` can include the future live runtime unit.
- Persist a promote-cutover artifact with:
  - `previous_champion_run_id`
  - `new_champion_run_id`
  - `promoted_at_ts_ms`
  - `batch_date`
  - `target_units`

### 3. Live daemon startup and health
- Startup must:
  - reconcile exchange state
  - resolve and pin current champion run id
  - store the resolved run id in live checkpoints
  - store the last observed ws-public checkpoint metadata
- Health output must include:
  - `live_runtime_model_run_id`
  - `champion_pointer_run_id`
  - `ws_public_last_checkpoint_ts_ms`
  - `ws_public_staleness_sec`
  - divergence status

### 4. Restart continuity across model handoff
- If a restart happens after a promote:
  - local position/risk continuity is restored first
  - current live state is reconciled
  - the new pinned model run id is applied only after reconcile
- Existing position TP/SL/trailing continuity must not be discarded by the model handoff.

### 5. Data-plane synchronization
- The daily pipeline report and live state must both reference:
  - batch date
  - latest ws-public validate/stats checkpoint
  - latest micro aggregate run id if available
- Live health must be able to prove it is attached to the same data epoch family as the daily loop.

## Non-Heuristic Rules
- The live runtime does not "guess" whether it should switch models.
- It switches only when `champion_v4` changes and the promote hook restarts the live unit.
- The live runtime does not "guess" whether market data is fresh enough.
- It uses explicit ws-public checkpoint timestamps and configured thresholds.
- The live runtime does not overwrite local risk continuity on startup.
- It performs `reconcile -> resume risk state -> bind current pinned model`.

## File Targets
### Add
- `docs/ADR/0012-live-model-handoff-and-data-plane-sync.md`
- `tests/test_live_model_handoff.py`

### Modify
- `scripts/daily_champion_challenger_v4_for_server.ps1`
- `scripts/install_server_runtime_services.ps1`
- `autobot/live/daemon.py`
- `autobot/live/state_store.py`
- `autobot/cli.py`

## Definition of Done
- A promote event can restart both paper and live target units from the same `champion_v4` update.
- Live startup writes a pinned model run id and does not run with an unresolved model contract.
- Live health/reporting exposes model-pointer divergence and ws-public staleness explicitly.
- Restart after promote preserves position/risk continuity and resumes on the correct model run id.

## References
- Existing champion promotion loop:
  - [daily_champion_challenger_v4_for_server.ps1](/d:/MyApps/Autobot/scripts/daily_champion_challenger_v4_for_server.ps1)
- Existing runtime installer and pinned runtime env:
  - [install_server_runtime_services.ps1](/d:/MyApps/Autobot/scripts/install_server_runtime_services.ps1)
- Existing live state and daemon:
  - [state_store.py](/d:/MyApps/Autobot/autobot/live/state_store.py)
  - [daemon.py](/d:/MyApps/Autobot/autobot/live/daemon.py)
