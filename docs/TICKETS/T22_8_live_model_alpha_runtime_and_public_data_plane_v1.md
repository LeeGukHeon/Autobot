# T22.8 - Live ModelAlpha Runtime And Public Data Plane v1

## Goal
- Close the gap between today's `live sync daemon` and an actual model-driven live trading runtime.
- Reuse the same `champion_v4` model family, `feature_set=v4`, and `model_alpha_v1` strategy contract already used by paper.
- Ensure live decisions consume the same public data plane as paper and daily training, rather than running as an isolated reconcile-only loop.

## Why This Ticket Exists
- `T22.1` through `T22.7` make live admissible, restart-safe, synchronized, and rollout-gated.
- But current `live run` still behaves as a restart-safe sync daemon:
  - reconcile
  - order / asset synchronization
  - breaker enforcement
  - runtime model handoff checks
- It does not yet run the actual `model_alpha_v1` selection loop on live features.
- It also does not yet consume shared public market data as the source of entry and exit timing.

## Scope
### 1. Shared public data plane
- Add one explicit live market data adapter that uses the same public data plane used by:
  - `autobot-ws-public.service`
  - paper live feature providers
  - daily feature build inputs
- No separate live-only market data contract.

### 2. Live feature materialization
- Reuse the existing v4 live feature providers used by paper.
- Build the same decision rows for live that paper uses for `model_alpha_v1`.
- Do not invent a second live-only feature schema.

### 3. Live strategy loop
- Add a live strategy runtime that:
  - resolves the pinned `champion_v4` concrete run id
  - instantiates `ModelAlphaStrategyV1`
  - drives `strategy.on_ts(...)` on live decision intervals
  - forwards fills back to `strategy.on_fill(...)`
- The live runtime must preserve the same selection / position / exit semantics as paper.

### 4. Intent bridge
- Convert `StrategyOrderIntent` into exact live `OrderIntent`.
- Every outbound intent must pass:
  - admissibility
  - small-account envelope
  - breaker state
  - rollout gate
- `shadow` mode:
  - build and log hypothetical intents only
  - emit no real orders
- `canary` / `live` mode:
  - submit real orders only when rollout gate says `order_emission_allowed=true`

### 5. Entry-to-risk continuity
- When a live entry fills:
  - attach or update the risk plan immediately
  - preserve TP / SL / trailing continuity across restart
- Reuse `LiveRiskManager` and `risk_loop.py`.

### 6. Promote and restart continuity
- Promote restarts must preserve this sequence:
  1. reconcile
  2. resume risk plans
  3. bind new pinned champion run id
  4. resume live strategy loop
- If the pinned run id diverges from `champion_v4`, halt new intents until restart/rebind completes.

## Non-Heuristic Rules
- No live-only feature schema separate from paper.
- No live-only strategy semantics separate from paper.
- No order emission in `shadow`.
- No order emission in `canary` / `live` without admissibility + rollout pass.
- No bypass of small-account invariants.
- Public data plane contract must be explicit and health-checked.

## Definition of Done
- `live run --rollout-mode shadow` produces hypothetical strategy intents and logs them without real submission.
- `live run --rollout-mode canary` can place real orders only when:
  - armed
  - tested
  - breaker clear
  - small-account safe
- Promote / restart causes live to rebind to the new champion run id and continue safely.
- The same model, feature contract, and strategy logic are shared by paper and live.

