# T22.3 - Restart-Safe Position And Risk Resumption v1

## Goal
- Make a full daemon restart deterministic:
  - positions restored
  - open orders relinked
  - TP/SL/trailing continuity preserved
  - risk manager resumes without duplicate orders

## Why This Is Mandatory
- The current code already has strong primitives:
  - persistent positions/orders/intents/risk plans
  - reconcile startup
  - risk plan persistence
- The missing step is exact resumption semantics for live operation.

## Scope
### 1. Startup resume sequence
- Enforce exact order:
  1. acquire run lock
  2. bootstrap exchange snapshot
  3. reconcile positions/orders
  4. relink active risk plans to matching positions and open exit orders
  5. verify watermark / TP / SL / trailing state
  6. only then allow new live intents

### 2. Risk-plan relinking
- Add relink rules for restart:
  - if exit order still open:
    - relink `current_exit_order_uuid`
    - state remains `EXITING`
  - if exit order terminal and position closed:
    - state becomes `CLOSED`
  - if position remains open but exit order missing:
    - state becomes `ACTIVE` or `TRIGGERED` by explicit rule
- Persist `resumed_from_restart=true` evidence.

### 3. Trailing continuity
- Preserve:
  - high watermark
  - armed timestamp
  - last evaluation timestamp
- On restart, do not reset trailing just because process restarted.

### 4. Resume audit artifact
- Write:
  - `live_resume_report.json`
- Include:
  - positions imported
  - orders relinked
  - risk plans resumed
  - plans halted for operator review

## Non-Heuristic Rules
- Exchange balances and open orders remain exchange source of truth.
- TP/SL/trailing parameters remain bot-local source of truth.
- No risk-plan reset based only on elapsed time or process restart.

## File Targets
### Add
- `tests/test_live_resume.py`

### Modify
- `autobot/live/reconcile.py`
- `autobot/live/daemon.py`
- `autobot/live/state_store.py`
- `autobot/risk/live_risk_manager.py`

## Definition of Done
- Restart with an open position and active risk plan resumes deterministically.
- Restart with an in-flight exit order does not duplicate the exit.
- Resume artifact is sufficient for operator audit.

## References
- Existing local baseline:
  - [T06_live_state_reconcile_v1.md](/d:/MyApps/Autobot/docs/TICKETS/T06_live_state_reconcile_v1.md)
  - [T12_live_risk_manager_v1.md](/d:/MyApps/Autobot/docs/TICKETS/T12_live_risk_manager_v1.md)
- Upbit Get Order / List Open Orders / My Asset
  - https://global-docs.upbit.com/reference/get-order
  - https://global-docs.upbit.com/reference/list-open-orders
  - https://global-docs.upbit.com/reference/websocket-myasset
