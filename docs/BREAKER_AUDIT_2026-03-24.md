# Breaker Audit 2026-03-24

Status: current analysis
Operational authority: yes
Scope: live breakers, rollout gating, reconcile halts, risk-manager exit halts, counter-based breaker logic

## Executive Summary

The breaker system is not uniformly wrong, but it mixes several different responsibilities:

- true safety breakers
- control-plane rollout gating
- reconcile inconsistency detection
- exit workflow failure handling
- counter-based transient API error handling

That mixing creates both logic bugs and real-trading mismatches.

The highest-severity findings are:

1. `RISK_EXIT_STUCK_MAX_REPLACES` is modeled as a breaker that halts new intents, but the real problem is an unfinished exit workflow that should keep escalating until flat.
2. cancel/replace reject counters are global and can be reset by unrelated success events, which can both hide real failures and create false triggers.
3. `LOCAL_POSITION_MISSING_ON_EXCHANGE` escalates too hard for a single snapshot and can stop startup even though the condition may be recoverable by repeated reconcile/manual-close inference.
4. rollout reasons are persisted into the breaker plane, which creates confusing layered behavior and makes `LIVE_BREAKER_ACTIVE` look like a breaker problem when it is often a derived control-plane status.
5. `SUPERVISOR_REPLACE_PERSIST_FAILED` is armed via `arm_breaker(...)` but has no explicit action mapping, so it silently degrades to `WARN`.

## Breaker Inventory

Primary mapping is in:

- [breakers.py](/d:/MyApps/Autobot/autobot/live/breakers.py)

### Reconcile / local-exchange state

- `UNKNOWN_OPEN_ORDERS_DETECTED` -> `HALT_AND_CANCEL_BOT_ORDERS`
- `UNKNOWN_POSITIONS_DETECTED` -> `FULL_KILL_SWITCH`
- `LOCAL_POSITION_MISSING_ON_EXCHANGE` -> `HALT_AND_CANCEL_BOT_ORDERS`
- `LOCAL_OPEN_ORDER_NOT_FOUND_ON_EXCHANGE` -> `WARN`

### Data plane / runtime health

- `STALE_PRIVATE_WS_STREAM` -> `HALT_NEW_INTENTS`
- `STALE_EXECUTOR_STREAM` -> `HALT_NEW_INTENTS`
- `WS_PUBLIC_STALE` -> `HALT_NEW_INTENTS`
- `LIVE_RUNTIME_LOOP_FAILED` -> `HALT_NEW_INTENTS`
- `MODEL_POINTER_DIVERGENCE` -> `HALT_NEW_INTENTS`
- `MODEL_POINTER_UNRESOLVED` -> `HALT_NEW_INTENTS`

### Rollout / control plane

- `LIVE_ROLLOUT_NOT_ARMED` -> `HALT_NEW_INTENTS`
- `LIVE_ROLLOUT_UNIT_MISMATCH` -> `HALT_NEW_INTENTS`
- `LIVE_ROLLOUT_MODE_MISMATCH` -> `HALT_NEW_INTENTS`
- `LIVE_TEST_ORDER_REQUIRED` -> `HALT_NEW_INTENTS`
- `LIVE_TEST_ORDER_STALE` -> `HALT_NEW_INTENTS`
- `LIVE_BREAKER_ACTIVE` -> `HALT_NEW_INTENTS`
- `LIVE_CANARY_REQUIRES_SINGLE_SLOT` -> `HALT_NEW_INTENTS`

### Counter-driven API / executor failures

- `REPEATED_CANCEL_REJECTS` -> `HALT_NEW_INTENTS`
- `REPEATED_REPLACE_REJECTS` -> `HALT_NEW_INTENTS`
- `REPEATED_RATE_LIMIT_ERRORS` -> `HALT_NEW_INTENTS`
- `REPEATED_AUTH_ERRORS` -> `FULL_KILL_SWITCH`
- `REPEATED_NONCE_ERRORS` -> `FULL_KILL_SWITCH`

### Risk control evidence

- `RISK_CONTROL_ONLINE_BREACH_STREAK` -> `HALT_NEW_INTENTS`
- `RISK_CONTROL_MARTINGALE_EVIDENCE` -> `HALT_NEW_INTENTS`
- `RISK_CONTROL_MARTINGALE_CRITICAL_EVIDENCE` -> `HALT_AND_CANCEL_BOT_ORDERS`

### Exit / persistence / identifier issues

- `EXECUTOR_REPLACE_PERSIST_FAILED` -> `HALT_NEW_INTENTS`
- `RISK_EXIT_STUCK_MAX_REPLACES` -> `HALT_NEW_INTENTS`
- `RISK_EXIT_REPLACE_PERSIST_FAILED` -> `HALT_NEW_INTENTS`
- `IDENTIFIER_COLLISION` -> `FULL_KILL_SWITCH`
- `MANUAL_KILL_SWITCH` -> `FULL_KILL_SWITCH`

### Unmapped but armed

- `SUPERVISOR_REPLACE_PERSIST_FAILED`

This reason is armed in:

- [model_alpha_runtime_supervisor.py](/d:/MyApps/Autobot/autobot/live/model_alpha_runtime_supervisor.py)

But it is not present in `REASON_ACTION_MAP`, so it falls back to `WARN`.

## Detailed Findings

### 1. `RISK_EXIT_STUCK_MAX_REPLACES` is logically wrong for real trading

Severity: critical

Source:

- [live_risk_manager.py](/d:/MyApps/Autobot/autobot/risk/live_risk_manager.py)
- [breakers.py](/d:/MyApps/Autobot/autobot/live/breakers.py)

Current behavior:

- plan is in `EXITING`
- timeout expires
- if `replace_attempt < replace_max`, replace exit order
- else arm `RISK_EXIT_STUCK_MAX_REPLACES`

Why this is wrong:

- an entry can be abandoned
- an exit cannot be abandoned while the position still exists
- the current implementation effectively treats “could not finish exit” as a breaker condition instead of an unfinished exit state machine

Correct real-trading behavior should be:

- block new entry intents while unresolved exits exist
- continue exit attempts
- escalate exit aggressiveness
  - passive -> join -> IOC/best -> stronger flatten path
- optionally back off between attempts, but never give up before flat

Recommended fix:

1. remove `RISK_EXIT_STUCK_MAX_REPLACES` from the hard breaker path
2. convert it into an `emergency_flatten` escalation state
3. keep `HALT_NEW_INTENTS` semantics for entry only, not for exit continuation

### 2. Cancel/replace reject counters are globally scoped and can be reset by unrelated events

Severity: critical

Sources:

- [breakers.py](/d:/MyApps/Autobot/autobot/live/breakers.py)
- [daemon.py](/d:/MyApps/Autobot/autobot/live/daemon.py)
- [live_risk_manager.py](/d:/MyApps/Autobot/autobot/risk/live_risk_manager.py)

Current behavior:

- `cancel_reject` and `replace_reject` counters are stored in global checkpoints:
  - `breaker_counter:cancel_reject`
  - `breaker_counter:replace_reject`
- multiple producers write them:
  - private WS
  - executor events
  - risk manager
- resets happen on broad success states like any `cancel/cancelled/done`

Why this is wrong:

- failures from one market/plan/order can be cleared by a success on another
- different subsystems share the same counter namespace
- this can create both:
  - false negatives: real stuck exit hidden by unrelated done event
  - false positives: mixed errors from unrelated workflows aggregate into one breaker

Recommended fix:

1. split counters into scopes:
   - global for auth/rate/nonce
   - per market or per plan for cancel/replace reject
2. only reset per-scope counters on matching-scope success

### 3. `LOCAL_POSITION_MISSING_ON_EXCHANGE` is too strong on first observation

Severity: high

Sources:

- [reconcile.py](/d:/MyApps/Autobot/autobot/live/reconcile.py)
- [breakers.py](/d:/MyApps/Autobot/autobot/live/breakers.py)
- [model_alpha_runtime.py](/d:/MyApps/Autobot/autobot/live/model_alpha_runtime.py)

Current behavior:

- reconcile sees local managed position absent on exchange
- if no close evidence exists, it retains local position
- `evaluate_cycle_contracts(...)` arms `LOCAL_POSITION_MISSING_ON_EXCHANGE`

This is understandable, but too eager for a single snapshot.

Why it is risky:

- exchange snapshots can lag
- manual closes may need repeated inference before being matched
- startup can stop early on this reason because it maps to `HALT_AND_CANCEL_BOT_ORDERS`

Recent fixes improved this by adding:

- repeated missing observation tracking
- manual-close inference from closed orders

But the breaker/action semantics are still severe for what is often a recoverable reconcile mismatch.

Recommended fix:

1. treat first observation as warning or soft hold
2. escalate only after repeated observations without successful manual-close/bot-close inference
3. distinguish:
   - `managed_missing_first_seen`
   - `managed_missing_persistent`

### 4. Rollout gate reasons are mixed into the breaker plane

Severity: high

Sources:

- [rollout.py](/d:/MyApps/Autobot/autobot/live/rollout.py)
- [daemon.py](/d:/MyApps/Autobot/autobot/live/daemon.py)
- [breakers.py](/d:/MyApps/Autobot/autobot/live/breakers.py)

Current behavior:

- rollout gate computes reasons such as:
  - `LIVE_ROLLOUT_NOT_ARMED`
  - `LIVE_TEST_ORDER_STALE`
  - `LIVE_CANARY_REQUIRES_SINGLE_SLOT`
- daemon clears previous rollout reasons
- then re-arms them through the breaker system

Why this is problematic:

- rollout state is a control-plane admission decision, not necessarily a runtime safety breaker
- this layering makes operator-facing status harder to reason about
- `LIVE_BREAKER_ACTIVE` appears as a rollout reason even though it is derived from existing breaker state

It works, but it conflates:

- “runtime should not start”
- “new orders should not emit”
- “a true safety breaker is active”

Recommended fix:

1. keep rollout reasons in rollout status
2. only convert a subset into persistent breakers if absolutely necessary
3. treat `LIVE_BREAKER_ACTIVE` as derived UI state, not a breaker reason

### 5. `SUPERVISOR_REPLACE_PERSIST_FAILED` is armed but not mapped

Severity: medium

Source:

- [model_alpha_runtime_supervisor.py](/d:/MyApps/Autobot/autobot/live/model_alpha_runtime_supervisor.py)

Current behavior:

- calls `arm_breaker(reason_codes=["SUPERVISOR_REPLACE_PERSIST_FAILED"], ...)`
- but `REASON_ACTION_MAP` has no entry
- so it silently falls back to `ACTION_WARN`

Why this is bad:

- the name suggests a breaker-grade persistence failure
- actual behavior is only warning-level
- this is inconsistent and easy to misunderstand

Recommended fix:

Pick one explicitly:

- either map it intentionally in `REASON_ACTION_MAP`
- or rename it to a warning-style reason and stop using `arm_breaker(...)`

### 6. Stream death handling is brittle

Severity: medium

Sources:

- [daemon.py](/d:/MyApps/Autobot/autobot/live/daemon.py)
- [model_alpha_runtime.py](/d:/MyApps/Autobot/autobot/live/model_alpha_runtime.py)

Current behavior:

- if private WS task is done and queue is empty, `STALE_PRIVATE_WS_STREAM` is armed immediately
- similar behavior exists for executor stream
- there is no bounded reconnect retry before breaker escalation in these paths

Why this is risky:

- transient WS disconnects are common
- immediate breaker escalation can halt new entry flow too aggressively

Recommended fix:

1. add bounded reconnect attempts before breaker escalation
2. reserve breaker escalation for repeated or time-bounded unhealed stream failures

### 7. Auth/rate/nonce counters are reset too aggressively by sync success

Severity: medium

Source:

- [daemon.py](/d:/MyApps/Autobot/autobot/live/daemon.py)

Current behavior:

- `rate_limit_error`, `auth_error`, `nonce_error` counters are reset after sync-cycle success

Why this can be wrong:

- sync success does not necessarily mean order-submit path is healthy
- a successful account/orderbook sync can clear a problem that still exists in active order submission

Recommended fix:

1. separate sync-path counters from submit-path counters
2. only reset submit-path counters on submit-path success

### 8. `new_intents_allowed` and `protective_orders_allowed` are the right abstraction, but not fully used

Severity: medium

Sources:

- [breakers.py](/d:/MyApps/Autobot/autobot/live/breakers.py)
- [model_alpha_runtime.py](/d:/MyApps/Autobot/autobot/live/model_alpha_runtime.py)
- [live_risk_manager.py](/d:/MyApps/Autobot/autobot/risk/live_risk_manager.py)

Good:

- the code already distinguishes:
  - `new_intents_allowed`
  - `protective_orders_allowed`

Problem:

- exit-stuck handling still escalates through a breaker path instead of using this separation properly
- the architecture is ready for “block entries, continue exits”, but the state machine does not fully implement it

### 9. Some breaker reasons are logically fine

These are not primary problems:

- `IDENTIFIER_COLLISION` -> `FULL_KILL_SWITCH`
- `REPEATED_AUTH_ERRORS` -> `FULL_KILL_SWITCH`
- `REPEATED_NONCE_ERRORS` -> `FULL_KILL_SWITCH`
- `UNKNOWN_POSITIONS_DETECTED` -> strong halt if explicit policy says halt

These are harsh, but they are at least logically coherent with real-trading risk containment.

## Recommended Fix Order

1. Redesign exit stuck handling
   - `RISK_EXIT_STUCK_MAX_REPLACES` -> emergency flatten escalation
2. Scope cancel/replace counters per market or per plan
3. Soften first-observation `LOCAL_POSITION_MISSING_ON_EXCHANGE`
4. Separate rollout control-plane state from persistent breaker state
5. Resolve unmapped `SUPERVISOR_REPLACE_PERSIST_FAILED`
6. Add reconnect retry budgets for stream-staleness breakers

## Suggested Next Context Prompt

Continue from `BREAKER_AUDIT_2026-03-24.md`.

First implement:

1. replace `RISK_EXIT_STUCK_MAX_REPLACES` hard halt with “block new entry intents but continue emergency exit escalation”
2. scope `cancel_reject` and `replace_reject` counters by market/plan instead of one global counter
