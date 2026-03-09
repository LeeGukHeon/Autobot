# T22.2 - Live Order State Machine And Idempotent Executor v1

## Goal
- Define one exact persisted state machine from intent to terminal order state.
- Make repeated submit/replace/cancel paths idempotent and restart-safe.

## Why This Is The Core Live Ticket
- The live stack will fail operationally if the bot cannot answer these exactly:
  - was an order accepted?
  - which identifier owns it?
  - is this order still open?
  - did replace succeed or create a new UUID?
  - which exit order is linked to which risk plan?

## Scope
### 1. Exact state graph
- Define exact local order states:
  - `INTENT_NEW`
  - `SUBMITTING`
  - `OPEN`
  - `PARTIAL`
  - `REPLACING`
  - `CANCELING`
  - `DONE`
  - `CANCELLED`
  - `REJECTED`
  - `UNKNOWN_EXCHANGE_STATE`
- Define legal transitions only.

### 2. Identifier semantics
- Every live order must have deterministic identifier structure:
  - bot id
  - strategy/run id
  - plan or intent id
  - replace sequence
- Duplicate submit with same live intent must map to one idempotent outcome.

### 3. Replace / cancel-and-new semantics
- Support exact `cancel_and_new` path where appropriate.
- Persist:
  - previous UUID
  - previous identifier
  - new UUID
  - new identifier
  - replace sequence
- Preserve exchange truth even when replace acknowledgement is delayed.

### 4. Executor/live-daemon contract hardening
- Extend event contract so Python can distinguish:
  - accepted but not open yet
  - open
  - partial
  - done
  - cancel
  - replace accepted
  - replace rejected
  - cancel rejected
  - unknown raw state

## Non-Heuristic Rules
- Unknown exchange states must be preserved as raw values and mapped to `UNKNOWN_EXCHANGE_STATE`.
- No inferred terminal state unless confirmed by:
  - private WS event
  - `Get Order`
  - `List Open Orders` + terminal detail fetch
- Replace sequence is monotone and persisted.
- Small-account safety is mandatory here too:
  - partial fills and replace chains must not lose track of low-notional remainder
  - cancel-reject and remain-only semantics must not strand dust exits

## File Targets
### Add
- `docs/ADR/0010-live-order-state-machine.md`
- `tests/test_live_order_state_machine.py`

### Modify
- `autobot/execution/grpc_gateway.py`
- `autobot/execution/order_supervisor.py`
- `autobot/live/state_store.py`
- `autobot/live/daemon.py`
- `autobot/live/reconcile.py`
- `autobot/risk/live_risk_manager.py`
- `cpp/src/executor/*`

## Definition of Done
- Every live order has exactly one persisted lineage from initial intent to terminal state.
- Restart cannot create duplicate exits from stale local assumptions.
- Replace/cancel handling survives late or reordered exchange events.

## References
- Upbit Get Order
  - https://global-docs.upbit.com/reference/get-order
- Upbit List Open Orders
  - https://global-docs.upbit.com/reference/list-open-orders
- Upbit Cancel and New Order
  - https://global-docs.upbit.com/reference/cancel-and-new-order
- Upbit My Order and Trade
  - https://global-docs.upbit.com/reference/websocket-myorder
