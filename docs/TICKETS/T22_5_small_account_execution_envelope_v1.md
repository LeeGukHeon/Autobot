# T22.5 - Small-Account Execution Envelope v1

## Goal
- Make live trading safe and economically sensible for small accounts.
- Prevent the bot from trading amounts where fees, spread, tick-size rounding, or dust dominate the edge.
- Treat small-account safety as an always-on invariant, not an optional runtime mode.

## Why This Still Needs A Ticket
- Small-account failure is usually not model failure.
- It is usually:
  - min-total failure
  - tick rounding loss
  - fee reserve failure
  - dust remainder
  - turnover too high for the expected edge
- This ticket exists to define the invariant and its audit/report surface.
- The actual controls must be embedded into `T22.1` through `T22.7`, not deferred.

## Scope
### 1. Exact sizing envelope
- Add exact sizing pipeline:
  - target notional from strategy
  - fee reserve deduction
  - tick-size rounding
  - min-total check
  - dust remainder check
  - resulting admissible volume

### 2. Cost-aware expected-edge filter
- For every candidate live order, compute:
  - expected gross edge
  - fee estimate
  - spread/tick proxy
  - replace risk budget
  - resulting expected net edge
- Reject if `expected_net_edge <= 0`.

### 3. Single-slot canary mode
- One position max
- one live order per market at a time
- stricter replace limits
- stricter churn limits

### 4. Small-account report
- Write:
  - `live_small_account_report.json`
- Include:
  - rejected-for-cost count
  - rejected-for-min-total count
  - dust abort count

## Non-Heuristic Rules
- Min-total and tick-size come from exchange metadata.
- Fee reserve is explicit.
- Net-edge filter uses explicit cost components and a formula written in the report.
- No "small account mode" magic constants without artifact output.

## File Targets
### Add
- `autobot/live/small_account.py`
- `tests/test_live_small_account.py`

### Modify
- `autobot/live/admissibility.py`
- `autobot/execution/order_supervisor.py`
- `autobot/cli.py`

## Definition of Done
- Small-account live orders either pass exact admissibility with positive expected net edge or fail with exact reason code.
- Canary mode is operationally safe with very low turnover and one-slot exposure.
- Restart/reconcile/order-replace flows remain valid even for low-notional positions and dust-sensitive balances.

## References
- Upbit Get Available Order Info
  - https://global-docs.upbit.com/reference/available-order-information
- Upbit List Orderbook Instruments
  - https://global-docs.upbit.com/reference/list-orderbook-instruments
- Ciliberti et al., 2025
  - https://www.tandfonline.com/doi/abs/10.1080/1351847X.2025.2558117
- Almgren, Chriss, 2000
  - https://docslib.org/doc/1384720/optimal-execution-of-portfolio-transactions
