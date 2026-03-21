# V4 Legacy Dependency Findings 2026-03-22

## Verified scope

- Candidate run `20260320T180256Z-s42-8b956b2f`
- Manual `ev_opt` run `20260321T141459Z-s42-3e50674f`

Both server artifacts use the full current `feature_columns_v4()` contract exactly.

- feature count: `140`
- contract mismatch: `0`

## What this means

The phrase "remove the v3 dependency" actually splits into two different problems:

1. `LiveFeatureProviderV3` runtime-class dependency
2. pre-`2026-03-04` historical data dependency

These are not the same thing.

## Hard result

From the full 140-feature inventory:

- `28` features truly require substantial pre-`2026-03-04` history
- `112` features can be built from `2026-03-04` onward with bounded in-window warmup only
- `82` features still depend on legacy `v3` code paths/contracts even when they do **not** require old history

The exhaustive machine-readable inventory is:

- [V4_FEATURE_DEPENDENCY_INVENTORY_2026-03-22.json](/d:/MyApps/Autobot/docs/V4_FEATURE_DEPENDENCY_INVENTORY_2026-03-22.json)
- [V4_FEATURE_DEPENDENCY_INVENTORY_2026-03-22.md](/d:/MyApps/Autobot/docs/V4_FEATURE_DEPENDENCY_INVENTORY_2026-03-22.md)

## The real blocker

The only block that hard-requires old history is:

- `v4_ctrend_v1`

That block contributes exactly `28` features and currently depends on:

- daily history built from 5m candles
- `candles_api_v1` for recent rows
- `candles_v1` fallback warmup
- `240` day lookback policy
- `200` day MA / volume-MA style indicators

So if the real requirement is:

`the runtime should depend only on the properly collected post-2026-03-04 WS + micro + current candles regime`

then `ctrend_v1` is the first thing that must be removed or replaced.

## What is not the blocker

These blocks do **not** require pre-`2026-03-04` history:

- `v3_base_core`
- `v3_one_m_core`
- `v3_high_tf_core`
- `v3_micro_core`
- `v4_spillover_breadth`
- `v4_periodicity`
- `v4_trend_volume`
- `v4_order_flow_panel_v1`
- `v4_interactions`

They still need warmup, but that warmup is bounded inside the post-`2026-03-04` window.

The worst bounded warmups are:

- `v3_high_tf_core`: about `36h` because of `240m` slow-trend features
- `v3_base_core`: about `37` x `5m` bars for `logret_36` / `vol_36`
- `v4_order_flow_panel_v1`: up to `12` bars

## Practical migration order

If we want the implementation to match the original intent exactly, the order should be:

1. Remove or replace the `28` `ctrend_v1` features from the active v4 contract.
2. Retrain a new v4 model on the reduced contract.
3. Keep the new `LiveFeatureProviderV4Native`, but stop relying on `compute_base_features_v3` / `feature_columns_v3_contract` by introducing a native v4 live-base contract.
4. Re-run paper/canary checks after the retrain because feature-serving parity will have changed.

## Bottom line

If the goal is truly "no dependency on pre-`2026-03-04` legacy history", then:

- removing the `LiveFeatureProviderV3` class dependency is **not enough**
- the first real cut must be `v4_ctrend_v1`
