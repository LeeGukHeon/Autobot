# T23_8 Breaker-Integrated Online Risk Halt v1

## Goal

`execution_risk_control_online_state`의 halt state를
단순 intent skip에서 끝내지 않고,
live breaker reason으로도 반영한다.

## Change

new reason code:

- `RISK_CONTROL_ONLINE_BREACH_STREAK`

mapped action:

- `HALT_NEW_INTENTS`

## Runtime behavior

when online state has

- `halt_triggered = true`

runtime does:

1. persist online state checkpoint
2. arm live breaker with `RISK_CONTROL_ONLINE_BREACH_STREAK`
3. skip current intent with the same reason code

when online state has

- `clear_halt = true`

runtime does:

1. clear only `RISK_CONTROL_ONLINE_BREACH_STREAK`
2. keep any unrelated breaker reasons intact

## Why

soft halt only affects code paths that attempt a new intent.
breaker integration makes the halt visible to:

- rollout diagnostics
- dashboard breaker state
- operator inspection
- any code path that already respects `HALT_NEW_INTENTS`

## Deliberate limitation

이번 v1은 breaker integration까지만 한다.

아직은

- e-process / martingale evidence
- automatic hard escalation to `HALT_AND_CANCEL_BOT_ORDERS`
- breaker persistence segmented by subgroup

까지는 가지 않는다.
