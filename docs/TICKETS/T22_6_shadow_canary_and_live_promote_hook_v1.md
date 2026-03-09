# T22.6 - Shadow, Canary, And Promote-To-Live Hook v1

## Goal
- Define a safe live rollout path from today's paper champion-challenger stack.
- Do not jump directly from paper promote to unrestricted live execution.
- Keep the actual model-handoff and data-plane synchronization contract out of this ticket.
- That exact runtime synchronization belongs to `T22.7`.

## Scope
### 1. Shadow live mode
- Run:
  - startup reconcile
  - private WS / executor WS
  - live risk state machine
- But emit no real orders.
- Compare hypothetical live intents against admissibility and small-account reports.

### 2. Test-order gate
- Before real canary mode:
  - require successful `Test Order` validation for current endpoint/market/account route.

### 3. Tiny-notional canary
- Manual armed only
- one market slot
- tiny notional cap
- no automatic promote from paper
- kill-switch armed by default

### 4. Promote-to-live hook contract
- Define one explicit live target hook:
  - paper champion id
  - model registry run id
  - live runtime model ref
  - live rollout mode
  - operator arm token
- No silent promote-to-live.
- Exact pinned run-id handoff, ws-public continuity, and model-pointer divergence handling are deferred to `T22.7`.

## Non-Heuristic Rules
- Canary activation must require:
  - reconcile clean
  - breaker clear
  - admissibility healthy
  - test-order success
- Promote-to-live remains manual-armed until canary evidence is sufficient.

## File Targets
### Add
- `docs/ADR/0011-paper-to-live-promotion-contract.md`
- `tests/test_live_rollout.py`

### Modify
- `scripts/daily_champion_challenger_v4_for_server.ps1`
- `autobot/cli.py`
- `autobot/live/daemon.py`
- `autobot/live/state_store.py`

## Definition of Done
- Shadow mode can run continuously with no order emission.
- Canary mode can be armed and disarmed safely.
- Promote-to-live hook is explicit, auditable, and reversible.

## References
- Upbit Test Order
  - https://global-docs.upbit.com/reference/order-test
- Upbit My Order and Trade / My Asset
  - https://global-docs.upbit.com/reference/websocket-myorder
  - https://global-docs.upbit.com/reference/websocket-myasset
- Existing paper promotion baseline:
  - [daily_champion_challenger_v4_for_server.ps1](/d:/MyApps/Autobot/scripts/daily_champion_challenger_v4_for_server.ps1)
