# Live Parity Execution Simulator Design 2026-03-24

Status: in progress  
Operational authority: implementation guide for simulator-parity work  
Scope: certification/backtest/paper execution semantics, immediate-taker order handling, partial-fill lifecycle, and live-parity simulator architecture

## Current Implementation Status

Implemented in the current branch:

1. structural simulator dispatch split
   - `best/ioc/fok` no longer have to pass through the deferred no-touch resting-order path
2. paper/backtest now preserve final order class
   - engines no longer coerce `best -> limit` before simulation
3. first-pass immediate taker handling
   - `best` orders now use a dedicated immediate submit path
   - immediate fill price can use current spread proxy when a micro snapshot is available
   - paper/live micro snapshots now carry explicit top-of-book bid/ask prices and top1 notionals when WS orderbook is available
   - paper/live micro snapshots now also carry explicit bid/ask ladder levels from the latest WS orderbook event
   - row/offline micro snapshots now preserve side-specific depth and optional top-of-book fields when those columns are available in feature rows
   - offline micro snapshots now overlay historical raw orderbook ladder/top-of-book data when matching raw WS files are available
   - when an offline raw orderbook overlay is used, `snapshot_ts_ms` is promoted to the actual raw orderbook timestamp so depletion keys align to real historical book snapshots rather than bar timestamps
   - simulator uses that ladder first to compute immediate-taker VWAP and finite executable size before falling back to top-of-book/depth/trade proxies
   - same-snapshot immediate takers now deplete remaining ladder size, so sequential orders at the same snapshot do not unrealistically reuse the full depth
   - simulator prefers explicit top-of-book prices over spread-derived price proxy when those fields are present
   - side-specific executable depth now uses ask-depth for buy takers and bid-depth for sell takers when available
   - when book depth is unavailable, immediate takers now fall back to a conservative directional trade-liquidity proxy using recent trade notional, coverage, and imbalance
   - `limit IOC/FOK` now use immediate-taker semantics while `GTC/post_only` keep resting-limit semantics
4. first-pass partial-fill reserve handling
   - partial bid/ask fills preserve residual locked quote/base
5. first-pass accounting parity
   - entry fee now enters bid cost basis
   - flat round-trip realized PnL reconciles to equity in simulator tests
6. terminal fill reporting tightened
   - engine success accounting now waits for terminal `FILLED` state
7. first-pass partial-fill reporting parity
   - paper/backtest execution updates now preserve final order snapshots for any fill, including partial-cancelled `IOC`
   - `orders_filled` now counts unique orders with any fill instead of raw fill events
   - `orders_completed` and `orders_partially_filled` are reported separately
   - `ORDER_PARTIAL` events are emitted when an order has fill but is not terminal `FILLED`
8. explicit fill-timing split
   - paper/backtest summaries now expose:
     - `avg/p50/p90_time_to_first_fill_ms`
     - `avg/p50/p90_time_to_complete_fill_ms`
   - legacy `avg/p50/p90_time_to_fill_ms` fields are preserved and now represent completion timing for compatibility
9. multi-step partial/replace reporting regression coverage
   - paper/backtest now have explicit regression coverage for:
     - partial -> complete
     - partial-cancelled IOC
     - partial -> replace -> partial -> complete
10. first-pass `FOK` rejection regression coverage
   - simulator regressions now pin that insufficient executable depth:
     - cancels `BEST_FOK` / `LIMIT_FOK_*`
     - produces no partial fill side effect
     - preserves flat portfolio / unlocked reserve state
11. same-snapshot ladder depletion regression coverage
   - simulator regressions now pin that:
     - one IOC order can consume historical ladder depth
     - a following IOC on the same snapshot only sees the remainder
     - a following FOK can reject cleanly against depleted remaining depth
12. first-pass inter-snapshot ladder carry
   - simulator now carries remaining per-price ladder deficit into the next historical snapshot for the same market/side
   - later snapshots can restore available size via replenishment above the carried deficit
   - regressions now pin:
     - inter-snapshot depletion carry
     - inter-snapshot replenishment
13. first-pass resting-order queue evolution in backtest
   - resting backtest orders can now initialize queue-ahead volume from historical same-side visible size at the order price
   - advancing historical snapshots shrink that queue-ahead volume when visible size at the same level contracts
   - same-level visible size expansion is treated as behind-us arrival pressure, not ahead-of-us queue growth
   - historical trade-at-level ticks now reduce queue-ahead directly when raw trade data is available
   - historical orderbook event sequences now update queue state event-by-event within the snapshot window, instead of relying only on the final visible-size snapshot
   - once queue-ahead clears and price-touch/through occurs, the resting order can fill
   - regressions now pin queue-blocked then queue-cleared fill behavior across snapshots

Not yet implemented:

1. richer executable-liquidity proxy for immediate takers
   - current implementation now uses explicit live WS ladder levels, can overlay matching historical raw orderbook ladder snapshots in offline mode when raw files exist, depletes same-snapshot ladder depth, and carries per-price deficit into later snapshots, but still does not replay true queue-position evolution through historical L1/L2 state over time
2. fully realistic `FOK` partial rejection semantics under finite executable depth
   - current implementation enforces no-partial rejection under insufficient proxy depth, same-snapshot depletion, and first-pass inter-snapshot carry, but does not yet replay full queue-priority/arrival dynamics over time
3. fuller resting-order arrival-priority dynamics
   - current implementation now uses explicit historical orderbook event sequences plus trade-at-level consumption as a first-pass queue-ahead model, and treats same-level expansions as behind-us arrivals, but does not yet replay full queue-priority aging and participant arrivals/cancellations with participant-level identity
4. certification rerun to verify that blanket `IOC_FOK_NO_TOUCH` behavior is materially reduced

## Goal

Make certification, backtest, and paper execute the same decision stack as live:

1. strategy baseline execution recommendation
2. micro-order-policy override
3. learned execution-policy action selection
4. final submit semantics
5. fill / cancel / replace / partial / reserve lifecycle

The target is not "approximately similar charts."

The target is:

- the same action space
- the same final order meaning
- the same order-state transitions
- the same fee/slippage accounting model
- the same safety semantics around reserve, cancel, and replace

This is the minimum required for acceptance, certification, and runtime parity to be trustworthy.

## Why This Work Is Needed

Recent certification runs showed the current mismatch clearly:

- strategy baseline selected `PASSIVE_MAKER`
- execution policy upgraded many entries to `BEST_IOC`
- certification/backtest then canceled all of them as `IOC_FOK_NO_TOUCH`
- result: hundreds of submitted intents, zero fills

This is not mainly a selection-model bug.

It is a simulator semantics mismatch:

- live treats `best/ioc/fok` as immediate liquidity-taking orders
- current certification/backtest treats them as limit-like paths that are easy to cancel before any realistic immediate fill can occur

## Current Implemented Decision Stack

The current stack is already structurally layered, and that structure is correct:

1. [autobot/strategy/model_alpha_v1.py](/d:/MyApps/Autobot/autobot/strategy/model_alpha_v1.py)
   - `_resolve_runtime_execution_profile(...)`
   - chooses a baseline execution stage from runtime recommendations
2. `micro_order_policy`
   - can modify that baseline profile
3. [autobot/models/live_execution_policy.py](/d:/MyApps/Autobot/autobot/models/live_execution_policy.py)
   - `select_live_execution_action(...)`
   - chooses the final action under state-conditioned utility
4. environment submit path
   - live: [autobot/live/model_alpha_runtime_execute.py](/d:/MyApps/Autobot/autobot/live/model_alpha_runtime_execute.py)
   - paper: [autobot/paper/engine.py](/d:/MyApps/Autobot/autobot/paper/engine.py)
   - backtest: [autobot/backtest/engine.py](/d:/MyApps/Autobot/autobot/backtest/engine.py)

The main parity failure begins after final action selection.

## External References

### Venue semantics

Upbit official references:

- Order:
  - https://global-docs.upbit.com/reference/order
- Order section / order type options:
  - https://global-docs.upbit.com/v1.2.1/reference/order_section
- Cancel and New Order:
  - https://global-docs.upbit.com/v1.2.1/reference/cancel-and-new
- Get Order:
  - https://global-docs.upbit.com/reference/get-order

Key implications:

- `best` is not a resting limit order
- `best` requires `ioc` or `fok`
- `ioc/fok` are immediate execution semantics
- buy and sell payload meaning differs for `best`

Any simulator that:

- rewrites `best -> limit`
- or prevents same-step immediate execution before applying `ioc/fok`

is not venue-parity.

### Fill probability / execution realism

- Arroyo et al., *Deep Attentive Survival Analysis in Limit Order Books*
  - https://arxiv.org/abs/2306.05479
- state-dependent fill probability work
  - https://arxiv.org/abs/2403.02572
- Fabre and Ragel, *Tackling the Problem of State Dependent Execution Probability*
  - https://ssrn.com/abstract=4509063
- Zhang et al., *Towards Generalizable Reinforcement Learning for Trade Execution*
  - https://arxiv.org/abs/2307.11685
- ABIDES high-fidelity exchange simulation
  - https://arxiv.org/abs/1904.12066

Common message:

- passive/resting orders need queue or survival style modeling
- immediate takers need executable-liquidity semantics
- a single generic limit-order simulator is not enough

## Diagnosis Of Current Code

This section records the original gap analysis that motivated the work below.
Some items here have since been implemented; when that is the case, the note
below marks the diagnosis as historical and points to the remaining delta.

### 1. `best` is still degraded in paper/backtest

Status: historical diagnosis, now structurally addressed.

Current behavior:

- paper/backtest keeps `selected_ord_type_runtime`
- but simulates `best` as `limit`

Relevant files:

- [autobot/paper/engine.py](/d:/MyApps/Autobot/autobot/paper/engine.py)
- [autobot/backtest/engine.py](/d:/MyApps/Autobot/autobot/backtest/engine.py)

This loses the main venue distinction:

- immediate taker vs resting limit

### 2. Backtest deferred path forces no-touch behavior before `ioc/fok`

Status: historical diagnosis, now structurally addressed for immediate orders.

Relevant file:

- [autobot/backtest/exchange.py](/d:/MyApps/Autobot/autobot/backtest/exchange.py)

Current behavior:

- `submit_limit_order_deferred(...)` intentionally injects a non-touch price so no same-bar immediate fill can occur
- this is acceptable for resting `gtc/post_only`
- it is structurally wrong for immediate orders

### 3. `sim_exchange` mixes immediate and resting semantics

Status: partially resolved; generic dispatch exists, but executable-liquidity realism is still first-pass only.

Relevant file:

- [autobot/paper/sim_exchange.py](/d:/MyApps/Autobot/autobot/paper/sim_exchange.py)

Current issues:

- one submit path handles both resting and immediate orders
- `ioc/fok` immediately cancels if no immediate fill occurs
- `best` semantics are not modeled explicitly
- `PARTIAL` state exists but lifecycle consistency is incomplete

### 4. Reporting semantics are only partially aligned

Status: largely resolved for first-fill/complete parity; multi-step certification validation is still pending.

Current issues:

- `orders_filled` is still close to order-success count under all-or-nothing behavior
- if partial fills become reachable, `orders_filled`, time-to-fill, and reserve accounting must all be revisited together

## Design Principles

## 1. Two execution classes

### A. Immediate taker class

Examples:

- `BEST_IOC`
- `BEST_FOK`
- `LIMIT_IOC_JOIN`
- `LIMIT_FOK_JOIN`

Properties:

- evaluated at submit time
- does not enter the resting order book unless partially filled and the venue semantics explicitly allow that
- outcome is:
  - fully filled
  - partially filled then canceled (`IOC`)
  - or fully canceled (`IOC/FOK`)

### B. Resting order class

Examples:

- `LIMIT_GTC_PASSIVE_MAKER`
- `LIMIT_POST_ONLY`
- `LIMIT_GTC_JOIN`

Properties:

- enters open-order state
- fills later from ticker/bar/book replay
- participates in replace/cancel lifecycle

## 2. One explicit lifecycle

All environments should share the same state machine:

- `OPEN`
- `PARTIAL`
- `FILLED`
- `CANCELED`
- `FAILED`

And the same invariants:

- reserve lock on submit
- partial fill decrements only filled reserve
- cancel releases remaining reserve only
- replace cancels old remainder then submits new remainder
- realized/unrealized PnL reconciles to equity

## 3. Preserve order meaning through engines

The engine should preserve:

- `ord_type`
- `time_in_force`
- `price_mode`
- final selected action metadata

Do not silently change the order class before the simulator sees it.

## Target Architecture

### Phase 1. Structural split

Files:

- [autobot/paper/sim_exchange.py](/d:/MyApps/Autobot/autobot/paper/sim_exchange.py)
- [autobot/backtest/exchange.py](/d:/MyApps/Autobot/autobot/backtest/exchange.py)
- [autobot/paper/engine.py](/d:/MyApps/Autobot/autobot/paper/engine.py)
- [autobot/backtest/engine.py](/d:/MyApps/Autobot/autobot/backtest/engine.py)

Changes:

1. add generic simulator dispatch for:
   - immediate takers
   - resting orders
2. preserve runtime `ord_type`
3. route:
   - `ioc/fok` and `best` to immediate-taker path
   - `gtc/post_only` resting orders to deferred path

Expected result:

- certification no longer sends `BEST_IOC` through a resting-limit same-bar no-touch path

### Phase 2. Immediate-taker implementation

Use the current best available executable proxy for each environment:

- live:
  - real venue
- paper:
  - latest trade/ticker plus current runtime micro snapshot context
- backtest:
  - current bar reference / current replay step context

Longer term:

- move toward explicit best-bid/best-ask replay where data exists
- then separate:
  - executable price
  - executable size
  - immediate cleanup cost

Important:

- this phase should improve semantics without pretending to be a full L2 replay when no book depth is available

### Phase 3. Partial-fill correctness

Files:

- [autobot/paper/sim_exchange.py](/d:/MyApps/Autobot/autobot/paper/sim_exchange.py)
- [autobot/paper/engine.py](/d:/MyApps/Autobot/autobot/paper/engine.py)
- [autobot/backtest/engine.py](/d:/MyApps/Autobot/autobot/backtest/engine.py)

Changes:

1. reserve decrement by filled amount only
2. keep partially filled orders pending correctly
3. do not treat first fill as terminal success unless state is actually `FILLED`
4. align reporting of fill counts vs completion
5. emit explicit partial-fill order-state snapshots for reporting paths

### Phase 4. Accounting parity

Files:

- [autobot/paper/sim_exchange.py](/d:/MyApps/Autobot/autobot/paper/sim_exchange.py)
- reporting call sites

Changes:

1. entry fee into basis
2. exit fee and slippage into realized PnL
3. unrealized PnL reconciles to basis and equity

## Concrete First Slice To Implement Now

Implement now:

1. backtest gateway should stop routing `ioc/fok` through deferred no-touch path
2. paper/backtest engines should preserve final order class instead of coercing `best -> limit` before simulation
3. simulator should expose explicit dispatch points for:
   - immediate taker submit
   - resting submit

This is not the full high-fidelity endpoint.
It is the structural foundation required before deeper fill realism can be added safely.

## Files To Modify First

1. [autobot/backtest/engine.py](/d:/MyApps/Autobot/autobot/backtest/engine.py)
2. [autobot/backtest/exchange.py](/d:/MyApps/Autobot/autobot/backtest/exchange.py)
3. [autobot/paper/engine.py](/d:/MyApps/Autobot/autobot/paper/engine.py)
4. [autobot/paper/sim_exchange.py](/d:/MyApps/Autobot/autobot/paper/sim_exchange.py)
5. tests:
   - [tests/test_backtest_exchange.py](/d:/MyApps/Autobot/tests/test_backtest_exchange.py)
   - [tests/test_paper_sim_exchange.py](/d:/MyApps/Autobot/tests/test_paper_sim_exchange.py)
   - [tests/test_backtest_model_alpha_integration.py](/d:/MyApps/Autobot/tests/test_backtest_model_alpha_integration.py)
   - [tests/test_paper_engine_model_alpha_integration.py](/d:/MyApps/Autobot/tests/test_paper_engine_model_alpha_integration.py)

## Acceptance Criteria

When the simulator parity work is complete, we should expect:

1. `BEST_IOC` and `LIMIT_IOC_JOIN` no longer blanket-cancel in certification only because of same-bar no-touch forcing
2. `orders_submitted > 0` can produce realistic nonzero fills under aggressive actions
3. paper/backtest/live divergence becomes attributable to market model assumptions, not order-class coercion
4. partial-fill, replace, and cancel behave consistently across paper/backtest/live
5. acceptance failures return to reflecting model/execution quality rather than simulator artifacts

## Next Context Prompt

Continue from `LIVE_PARITY_EXECUTION_SIMULATOR_DESIGN_2026-03-24.md`.

Implement in this order:

1. upgrade immediate-taker executable-liquidity proxy beyond latest-trade / bar-reference heuristics
2. tighten finite-depth `FOK` full-fill rejection semantics with explicit regression coverage
3. extend partial-fill parity through replace/cancel multi-step flows
4. rerun certification and compare blanket `IOC_FOK_NO_TOUCH` frequency before vs after simulator changes
