# T23-13 Martingale Critical Escalation v1

## Goal

Promote very strong online martingale drift evidence from `HALT_NEW_INTENTS` to
`HALT_AND_CANCEL_BOT_ORDERS` without relying on ad hoc symbol rules.

## Contract

- Keep the existing martingale halt threshold as the first-stage stop signal.
- Add a second threshold, `martingale_escalation_threshold`, that is greater than
  or equal to the halt threshold.
- When the running martingale evidence crosses the first threshold, emit
  `RISK_CONTROL_MARTINGALE_EVIDENCE` and halt new intents.
- When the evidence crosses the escalation threshold, emit
  `RISK_CONTROL_MARTINGALE_CRITICAL_EVIDENCE` and escalate the breaker action to
  `HALT_AND_CANCEL_BOT_ORDERS`.

## Runtime Behavior

- `resolve_execution_risk_control_martingale_state(...)` now emits:
  - `martingale_critical_triggered`
  - `martingale_escalation_threshold`
  - `martingale_halt_action`
  - `martingale_clear_reason_codes`
- Live runtime merges streak-based halt state and martingale halt state into a
  single online checkpoint.
- Breaker clear uses explicit `clear_reason_codes` so recovery clears the reason
  that was actually armed, including the critical martingale reason.

## Observability

- Runtime recommendation summaries now expose martingale halt, clear, and
  escalation thresholds plus normal and critical reason codes.
- Dashboard/runtime state can show whether the current contract is capable of
  escalating from `HALT_NEW_INTENTS` to `HALT_AND_CANCEL_BOT_ORDERS`.

## Validation

- Unit test for critical martingale escalation.
- Live runtime regression that confirms critical martingale evidence arms the
  live breaker with `HALT_AND_CANCEL_BOT_ORDERS`.
