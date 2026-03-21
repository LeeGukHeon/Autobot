# V4 Legacy Dependency Findings 2026-03-22

## Current state

The active `v4` contract is now:

- free of `ctrend_v1`
- free of pre-`2026-03-04` hard history blockers
- free of direct `v3` code-contract dependency in the active `v4` path

Verified by contract inventory:

- total active features: `112`
- features that truly require pre-`2026-03-04` history: `0`
- features buildable from `2026-03-04` onward with bounded in-window warmup only: `112`
- features still tied to legacy `v3` code paths/contracts: `0`

Reference artifacts:

- [V4_FEATURE_DEPENDENCY_INVENTORY_2026-03-22.json](/d:/MyApps/Autobot/docs/V4_FEATURE_DEPENDENCY_INVENTORY_2026-03-22.json)
- [V4_FEATURE_DEPENDENCY_INVENTORY_2026-03-22.md](/d:/MyApps/Autobot/docs/V4_FEATURE_DEPENDENCY_INVENTORY_2026-03-22.md)

## What was removed

The old active contract contained:

- `140` features
- including `28` `ctrend_v1` fields

Those `ctrend_v1` fields were the only true pre-`2026-03-04` history blocker.

They are now out of the active contract.

## What was migrated

The remaining live-base path is now served by native `v4` modules instead of directly depending on:

- `feature_blocks_v3`
- `feature_set_v3`
- `LiveFeatureProviderV3`

The active `v4` path now uses:

- `feature_blocks_v4_live_base`
- `feature_set_v4_live_base`
- `LiveFeatureProviderV4Native`
- `LiveFeatureProviderV4` as the default runtime wrapper on top of the native implementation

## What still remains

There is still bounded warmup inside the post-`2026-03-04` window.

Examples:

- `240m` high-tf trend/regime terms can need roughly `36h`
- `logret_36` / `vol_36` need about `37` base `5m` bars
- order-flow persistence terms need up to `12` bars

That is expected warmup, not legacy dependence.

## Practical meaning

For the original goal:

`use only the properly collected post-2026-03-04 WS + micro + current candles regime`

the active `v4` contract now matches that intent.

## Next work

The next meaningful task is no longer dependency removal.

It is:

1. retrain on the new `112`-feature active contract
2. compare selection / fallback / canary behavior against the older `140`-feature runs
