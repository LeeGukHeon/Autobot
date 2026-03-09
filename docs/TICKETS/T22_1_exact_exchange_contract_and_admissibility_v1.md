# T22.1 - Exact Exchange Contract And Admissibility v1

## Goal
- Build one exact live-order admissibility layer for Upbit.
- Remove any path where live orders can be generated from internal intent alone.

## Why This Comes First
- For live trading, the minimum viable safety layer is not alpha quality.
- It is exact exchange compatibility.
- Small accounts are especially sensitive to:
  - min-total violations
  - tick-size violations
  - fee reserve mistakes
  - dust remainders

## Scope
### 1. Exchange metadata contract
- Add one exact `LiveOrderAdmissibilitySnapshot` built from:
  - `GET /v1/orders/chance`
  - `GET /v1/orderbook/instruments`
  - account balances
  - configured fee schedule
- Persist compact snapshots with timestamp and market.

### 2. Pre-trade admissibility engine
- Add one pure function:
  - input:
    - market
    - side
    - requested notional/qty/price
    - current balances
    - chance snapshot
    - orderbook instruments snapshot
    - fee schedule
  - output:
    - `pass/fail`
    - adjusted price
    - adjusted volume
    - adjusted notional
    - exact reject codes
- Reject codes must be explicit:
  - `PRICE_NOT_TICK_ALIGNED`
  - `BELOW_MIN_TOTAL`
  - `INSUFFICIENT_FREE_BALANCE`
  - `FEE_RESERVE_INSUFFICIENT`
  - `DUST_REMAINDER`
  - `EXPECTED_EDGE_NOT_POSITIVE_AFTER_COST`

### 3. Test-order validation hook
- Add exact optional validation path using `POST /v1/orders/test`.
- For canary live, require:
  - admissibility pass
  - test-order pass

### 4. Logging / evidence
- Write compact artifact:
  - `live_admissibility_report.json`
- Store:
  - exchange snapshot hashes
  - adjusted order fields
  - reject codes
  - remaining balance estimates

## Non-Heuristic Rules
- Tick-size must come only from `/v1/orderbook/instruments`, not static local tables.
- Min-total must come from `/v1/orders/chance`, not hard-coded market assumptions.
- Fee reserve must be explicit in the admissibility result.
- Dust handling must be deterministic:
  - either admissible after exact rounding
  - or rejected with `DUST_REMAINDER`

## File Targets
### Add
- `autobot/live/admissibility.py`
- `tests/test_live_admissibility.py`

### Modify
- `autobot/upbit/private.py`
- `autobot/cli.py`
- `autobot/execution/grpc_gateway.py`
- `autobot/live/daemon.py`

## Definition of Done
- No live order creation path bypasses admissibility.
- Small account orders fail fast with exact reason codes instead of exchange-side surprises.
- Test-order validation works without creating real orders.

## References
- Upbit Create Order
  - https://global-docs.upbit.com/reference/new-order
- Upbit Get Available Order Info
  - https://global-docs.upbit.com/reference/available-order-information
- Upbit Test Order
  - https://global-docs.upbit.com/reference/order-test
- Upbit List Orderbook Instruments
  - https://global-docs.upbit.com/reference/list-orderbook-instruments
