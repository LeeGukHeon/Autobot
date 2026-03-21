# V4 Legacy Dependency Findings 2026-03-22

## Current state

The active `v4` contract no longer includes `ctrend_v1`.

Verified by contract inventory:

- total active features: `112`
- features that truly require pre-`2026-03-04` history: `0`
- features buildable from `2026-03-04` onward with bounded in-window warmup only: `112`
- features still tied to legacy `v3` code paths/contracts: `82`

Reference artifacts:

- [V4_FEATURE_DEPENDENCY_INVENTORY_2026-03-22.json](/d:/MyApps/Autobot/docs/V4_FEATURE_DEPENDENCY_INVENTORY_2026-03-22.json)
- [V4_FEATURE_DEPENDENCY_INVENTORY_2026-03-22.md](/d:/MyApps/Autobot/docs/V4_FEATURE_DEPENDENCY_INVENTORY_2026-03-22.md)

## What changed

The old active contract contained `140` features, including `28` `ctrend_v1` fields that forced a pre-`2026-03-04` history dependency.

That blocker is now removed from the active contract.

So if the question is:

`can the active v4 feature contract now be served from the post-2026-03-04 data regime only?`

the answer is:

`yes, but with bounded in-window warmup for rolling and high-tf terms`

## What still remains

Even after removing the old-history blocker, the active contract is still not fully `legacy-free` in code terms.

The remaining `82` legacy-tied features are in these blocks:

- `v3_base_core`
- `v3_one_m_core`
- `v3_high_tf_core`
- `v3_micro_core`
- `v4_trend_volume`
- `v4_interactions`

This means:

- the runtime no longer needs old pre-3/4 history
- but the implementation still depends on `v3` feature code/contracts for a large part of the base row

## Practical meaning

There are now two separate migration goals:

1. `old-history-free runtime`
2. `v3-code-free runtime`

Goal 1 is now achieved at the active contract level.

Goal 2 is **not** achieved yet.

## Next clean step

The next migration target should be:

- replace `compute_base_features_v3`
- replace `feature_columns_v3_contract`
- define a native `v4` live-base contract for:
  - base candle transforms
  - 1m aggregate transforms
  - high-tf transforms
  - micro core projection

Only after that will the active runtime be both:

- free of pre-`2026-03-04` data dependence
- free of `v3` code-contract dependence
