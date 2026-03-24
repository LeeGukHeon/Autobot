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
4. first-pass partial-fill reserve handling
   - partial bid/ask fills preserve residual locked quote/base
5. first-pass accounting parity
   - entry fee now enters bid cost basis
   - flat round-trip realized PnL reconciles to equity in simulator tests
6. terminal fill reporting tightened
   - engine success accounting now waits for terminal `FILLED` state

Not yet implemented:

1. richer executable-liquidity proxy for immediate takers
   - current implementation still uses latest-trade / bar reference proxy, not replayed L1/L2 executable depth
2. fully realistic `FOK` partial rejection semantics under finite executable depth
3. end-to-end partial-fill replace/cancel reporting parity
4. explicit `time_to_first_fill` vs `time_to_complete` reporting split
5. certification rerun to verify that blanket `IOC_FOK_NO_TOUCH` behavior is materially reduced

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

### 1. `best` is still degraded in paper/backtest

Current behavior:

- paper/backtest keeps `selected_ord_type_runtime`
- but simulates `best` as `limit`

Relevant files:

- [autobot/paper/engine.py](/d:/MyApps/Autobot/autobot/paper/engine.py)
- [autobot/backtest/engine.py](/d:/MyApps/Autobot/autobot/backtest/engine.py)

This loses the main venue distinction:

- immediate taker vs resting limit

### 2. Backtest deferred path forces no-touch behavior before `ioc/fok`

Relevant file:

- [autobot/backtest/exchange.py](/d:/MyApps/Autobot/autobot/backtest/exchange.py)

Current behavior:

- `submit_limit_order_deferred(...)` intentionally injects a non-touch price so no same-bar immediate fill can occur
- this is acceptable for resting `gtc/post_only`
- it is structurally wrong for immediate orders

### 3. `sim_exchange` mixes immediate and resting semantics

Relevant file:

- [autobot/paper/sim_exchange.py](/d:/MyApps/Autobot/autobot/paper/sim_exchange.py)

Current issues:

- one submit path handles both resting and immediate orders
- `ioc/fok` immediately cancels if no immediate fill occurs
- `best` semantics are not modeled explicitly
- `PARTIAL` state exists but lifecycle consistency is incomplete

### 4. Reporting semantics are only partially aligned

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

1. structural immediate-taker vs resting-order dispatch
2. preserve final order class through paper/backtest engines
3. remove deferred no-touch handling for `ioc/fok` in certification/backtest
4. then move to partial-fill lifecycle and fee-basis parity
