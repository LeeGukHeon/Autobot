# T22: Live Ops Readiness And Continuity v1

- Date: 2026-03-09
- Target runtime:
  - Upbit live trading
  - small-account compatible
  - restart-safe
  - paper-to-live promotion capable
- Current baseline in repo:
  - `T06`: state store + exchange reconciliation
  - `T10`: executor private WS
  - `T12`: persistent TP/SL/trailing live risk manager

## Goal
- Move the project from `paper-first automated system with live primitives` to `restart-safe, small-account-safe, operationally auditable live trading stack`.
- Keep the live rollout grounded in:
  - exact exchange contract semantics
  - persisted state transitions
  - deterministic restart/reconcile behavior
  - cost-aware order admissibility
- Avoid "heuristic glue" as much as possible.

## Most Suitable Direction
The right direction is not "add smarter execution first".

The right direction is:

1. treat exchange account/open-order/private-WS events as exchange source of truth
2. treat TP/SL/trailing/order-intent metadata as bot-local source of truth
3. define one exact persisted order/position/risk state machine
4. make every startup a deterministic reconcile-and-resume sequence
5. make every outgoing order pass an exact admissibility contract built from exchange metadata
6. only after that, add canary live rollout and live promotion hooks
7. only after rollout and handoff are exact, connect the actual live strategy/runtime loop

This direction is the best fit for the current codebase because the repo already has:

- persistent SQLite live state: [state_store.py](/d:/MyApps/Autobot/autobot/live/state_store.py)
- startup/manual reconcile: [reconcile.py](/d:/MyApps/Autobot/autobot/live/reconcile.py)
- live daemon with REST / private WS / executor WS modes: [daemon.py](/d:/MyApps/Autobot/autobot/live/daemon.py)
- persistent risk plans with TP/SL/trailing: [live_risk_manager.py](/d:/MyApps/Autobot/autobot/risk/live_risk_manager.py)
- executor gateway contract: [grpc_gateway.py](/d:/MyApps/Autobot/autobot/execution/grpc_gateway.py)

What is still missing is not "another model lane".

What is missing is:

- exact live order admissibility
- exact order lifecycle convergence
- exact restart continuity
- exact divergence breakers and operator controls

## Why This Direction Is Non-Heuristic
The live stack should be driven by explicit contracts, not informal rules:

- admissibility comes from `/v1/orders/chance`, `/v1/orderbook/instruments`, balances, fees, and test-order validation
- order state comes from exact REST/WS order fields and executor acknowledgements
- continuity comes from persisted local state plus exchange snapshot reconcile
- kill-switches come from contract violations:
  - unknown external orders
  - unknown positions
  - stale private WS
  - repeated replace/cancel rejects
  - identifier collisions
  - state divergence between exchange and local DB

## Live Ops Checklist
### A. Exchange Contract
- exact region endpoint selection and auth
- `Create Order`, `Get Order`, `List Open Orders`, `Cancel Order`, `Cancel and New Order` contracts encoded exactly
- `Get Available Order Info` integrated before every live order
- `List Orderbook Instruments` integrated for tick-size rounding
- `Test Order` integrated for pre-live and canary validation

### B. Order Admissibility
- every live intent must pass:
  - tick-size valid
  - min-total valid
  - available balance valid
  - fee reserve valid
  - dust remainder valid
  - expected edge after fee/slippage still positive
- no live order path may bypass admissibility

### C. State Continuity
- startup reconcile mandatory before first live action
- local DB must persist:
  - open positions
  - open orders
  - inferred intents
  - active risk plans
  - current exit order linkage
  - last reconcile checkpoint
  - last executor/private-WS checkpoint
- restart must restore TP/SL/trailing continuity without duplicate exits

### D. Order Lifecycle Convergence
- order submission must be idempotent by identifier
- terminal state resolution must be exact
- replace path must use one exact state transition graph
- timeout/replace/cancel must not create duplicate exit orders
- unknown state values from exchange must be preserved, not collapsed

### E. Small-Account Safety
- support accounts where one bad rounding or fee reserve error can invalidate the whole order
- exact min-total and dust handling
- exact fee reserve and free balance accounting
- single-slot and low-turnover safe mode
- `remain_only` or equivalent exact remainder semantics where supported

### F. Divergence Breakers
- halt on unknown external open orders unless explicit policy says otherwise
- halt on unknown positions unless explicit import policy says otherwise
- halt on stale private WS / executor stream beyond configured threshold
- halt on repeated replace rejects or repeated cancel failures
- halt on reconcile mismatch between local open orders and exchange snapshot

### G. Operator Controls
- read-only live status
- dry-run reconcile
- apply reconcile
- kill switch
- safe unlock / run-lock recovery
- export-state for audits

### H. Rollout
- read-only exchange sync
- private WS / executor WS shadowing
- test-order validation
- tiny-notional canary live
- capped live canary with one slot
- only then live promotion hook

## Ticket Order
1. `T22.1` Exact Exchange Contract And Admissibility v1
2. `T22.2` Live Order State Machine And Idempotent Executor v1
3. `T22.3` Restart-Safe Position And Risk Resumption v1
4. `T22.4` Live Breakers, Kill Switch, And Divergence Handling v1
5. `T22.7` Live Model Handoff And Data Plane Sync v1
6. `T22.6` Shadow, Canary, And Promote-To-Live Hook v1
7. `T22.8` Live ModelAlpha Runtime And Public Data Plane v1

## Cross-Cutting Invariant
- `T22.5` is not an optional late-stage mode.
- `T22.5` defines small-account safety invariants that must hold across all live tickets:
  - exact `min_total`
  - fee reserve
  - dust remainder
  - low-notional exit continuity
  - no live path that becomes invalid only because the account is small

## Recommended Rollout Policy
- Do not auto-promote paper champion directly into real trading yet.
- First complete:
  - `T22.1` through `T22.4`
  - `T22.7`
  - while satisfying `T22.5` small-account invariants throughout
- Then allow only:
  - read-only live sync
  - test-order validation
  - tiny-notional canary
- Keep live promotion manual-armed until at least one stable canary week is completed.

## Research And Official References
### Upbit official docs
- Create Order:
  - https://global-docs.upbit.com/reference/new-order
- Get Available Order Info:
  - https://global-docs.upbit.com/reference/available-order-information
- Test Order:
  - https://global-docs.upbit.com/reference/order-test
- Get Order:
  - https://global-docs.upbit.com/reference/get-order
- List Open Orders:
  - https://global-docs.upbit.com/reference/list-open-orders
- Cancel and New Order:
  - https://global-docs.upbit.com/reference/cancel-and-new-order
- List Orderbook Instruments:
  - https://global-docs.upbit.com/reference/list-orderbook-instruments
- My Order and Trade:
  - https://global-docs.upbit.com/reference/websocket-myorder
- My Asset:
  - https://global-docs.upbit.com/reference/websocket-myasset
- REST API Best Practice:
  - https://global-docs.upbit.com/docs/rest-api-best-practice
- WebSocket Best Practice:
  - https://global-docs.upbit.com/docs/websocket-best-practice

### Practical execution / microstructure references
- Almgren, Chriss, "Optimal Execution of Portfolio Transactions", 2000
  - foundational execution cost/risk framework
  - https://docslib.org/doc/1384720/optimal-execution-of-portfolio-transactions
- Huang, Lehalle, Rosenbaum, "Simulating and Analyzing Order Book Data: The Queue-Reactive Model", 2015
  - practical order-book state model used widely in execution research
  - https://docslib.org/doc/10000650/simulating-and-analyzing-order-book-data-the-queue-reactive-model
- Lokin, Yu, "Fill Probabilities in a Limit Order Book with State-Dependent Stochastic Order Flows", 2024
  - practical fill-probability modeling for passive execution
  - https://arxiv.org/abs/2403.02572
- "A deep learning approach to estimating fill probabilities in a limit order book", 2022
  - practical passive-order execution probability modeling
  - https://www.tandfonline.com/doi/full/10.1080/14697688.2022.2124189
- Ciliberti et al., "The risk of falling short: implementation shortfall variance in portfolio construction", 2025
  - useful for cost-risk-aware small-account sizing and turnover control
  - https://www.tandfonline.com/doi/abs/10.1080/1351847X.2025.2558117
- Genet, "Deep Learning for VWAP Execution in Crypto Markets: Beyond the Volume Curve", 2025
  - crypto-specific execution research; useful later, not first rollout blocker
  - https://arxiv.org/abs/2502.13722

## Notes
- The first live milestone should not try to solve smart execution and continuity at the same time.
- The continuity layer must be exact first.
- Smart execution belongs after exact admissibility, state convergence, and restart safety are closed.
