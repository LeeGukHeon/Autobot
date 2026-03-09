# T22.4 - Live Breakers, Kill Switch, And Divergence Handling v1

## Goal
- Add exact operational breakers for live safety.
- Halt on contract violations, not on ad-hoc intuition.

## Scope
### 1. Divergence detector
- Detect and classify:
  - unknown external open orders
  - unknown positions
  - local open order not found on exchange
  - stale private WS / executor stream
  - repeated cancel rejects
  - repeated replace rejects
  - repeated 429 / auth / nonce failures
  - identifier collision

### 2. Breaker actions
- `WARN`
- `HALT_NEW_INTENTS`
- `HALT_AND_CANCEL_BOT_ORDERS`
- `FULL_KILL_SWITCH`

### 3. Kill-switch operator interface
- Add exact CLI:
  - `live kill-switch status`
  - `live kill-switch arm`
  - `live kill-switch clear`

### 4. Persistent breaker ledger
- Persist breaker history and state transitions.
- Export compact artifact:
  - `live_breaker_report.json`

## Non-Heuristic Rules
- Breakers are triggered by explicit contract failures, not discretionary PnL drawdown rules.
- Live alpha may continue to evaluate signals internally, but no new intents leave the bot while halted.

## File Targets
### Add
- `autobot/live/breakers.py`
- `tests/test_live_breakers.py`

### Modify
- `autobot/live/daemon.py`
- `autobot/live/reconcile.py`
- `autobot/live/state_store.py`
- `autobot/cli.py`

## Definition of Done
- Contract divergence can halt live order emission deterministically.
- Operators can inspect and clear breaker state explicitly.
- Breaker state survives restart.

## References
- Upbit REST best practice
  - https://global-docs.upbit.com/docs/rest-api-best-practice
- Upbit WebSocket best practice
  - https://global-docs.upbit.com/docs/websocket-best-practice
