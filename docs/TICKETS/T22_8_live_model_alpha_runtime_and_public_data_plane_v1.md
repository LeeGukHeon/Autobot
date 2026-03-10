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

## Observed Canary Findings
### 2026-03-10 candidate canary
- Candidate live canary was pinned directly to run id `20260310T011523Z-s42-fc53106c`.
- A real entry order was accepted for `KRW-KITE` at `442.0` KRW with requested quantity about `13.56787669`.
- Exchange account state later showed:
  - `KRW` balance reduced to about `8761.89521164`
  - `KITE` balance `13.56787669`
  - `avg_buy_price = 442`
- This confirms that the candidate canary produced at least one real fill.

### Gap exposed by that fill
- Local candidate live state DB still had:
  - `positions = 0`
  - `risk_plans = 0`
  - accepted entry intent and submitted order records only
- Reconcile then observed:
  - `exchange_positions = 3`
  - `ignored_dust_positions = 2`
  - `unknown_positions = 1`
- The runtime armed `FULL_KILL_SWITCH` with reason `UNKNOWN_POSITIONS_DETECTED` and exited.

### Implication
- Entry signal generation, canary order submission, and exchange execution are now proven to work.
- The remaining live-runtime gap is not entry generation.
- The remaining gap is `entry fill -> local position import -> risk plan attach -> restart-safe continuity`.
- Until that convergence is closed, live canary can buy successfully but still self-halt after the exchange-side position appears without a matching local position/risk plan.

### Required follow-up
- Import exchange-confirmed entry fills into local position state deterministically.
- Attach the default/model-derived risk plan immediately after first confirmed fill.
- Ensure restart/reconcile can convert a bot-owned filled entry into:
  - one local position
  - one active risk plan
  - no `UNKNOWN_POSITIONS_DETECTED` breaker
