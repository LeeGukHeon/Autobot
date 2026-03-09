# ADR 0011: Paper To Live Promotion Contract

## Status
- Accepted

## Context
- `champion_v4` remains the single model pointer for paper champion and future live runtime.
- The daily promote loop may restart additional runtime units through `PromotionTargetUnits`.
- Live rollout must not silently move from paper-only evidence into real order emission.

## Decision
- Live runtime rollout is explicit and operator-armed.
- `shadow` mode is the default runtime mode and never emits live orders.
- `canary` and `live` modes require:
  - an armed rollout contract
  - a matching target unit
  - a recent successful Upbit `order-test`
  - a clear breaker state
  - single-slot canary readiness for `canary`
- Promote will restart configured live target units only when the rollout contract is armed and promotable.
- If the live target is configured but rollout is not armed, promote records a skip instead of silently restarting live.

## Consequences
- Promote remains auditable and reversible.
- Paper champion can continue to promote independently from live rollout.
- Live runtime can share the same `champion_v4` pointer without creating a silent promote-to-live path.
