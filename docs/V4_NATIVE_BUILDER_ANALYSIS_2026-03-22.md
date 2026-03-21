# V4 Native Builder Analysis 2026-03-22

## Goal

Replace the current `LIVE_V4 -> LIVE_V3 base provider -> V4 enrichments` stack with a native `LIVE_V4` builder that does not depend on `LiveFeatureProviderV3` for runtime row construction.

This document captures what `live_features_v3.py` currently does so we can split:

- shared online runtime responsibilities
- v3-only feature responsibilities
- v4-only enrichment responsibilities

## Current Runtime Topology

### Main runtime call sites

- `autobot/paper/engine.py`
  - constructs `LiveFeatureProviderV3` or `LiveFeatureProviderV4`
  - uses `build_frame(ts_ms, markets)` on every decision cycle
  - calls `ingest_ticker(...)` on live ticker updates
- `autobot/live/model_alpha_runtime.py`
  - delegates live provider construction to `model_alpha_runtime_bootstrap.py`
  - also uses `LiveFeatureProviderV3` / `LiveFeatureProviderV4`
- `autobot/paper/live_features_v4.py`
  - currently composes `LiveFeatureProviderV3` as `_base_provider`

### Current v4 layering

`LiveFeatureProviderV4` currently:

1. uses `LiveFeatureProviderV3` to produce a base runtime row
2. adds v4-only enrichments:
   - spillover breadth
   - periodicity
   - trend volume
   - order-flow panel
   - interactions
   - ctrend daily joins

So the existing `v4` runtime builder is only partially native.

## What `LiveFeatureProviderV3` Actually Does

File:

- `autobot/paper/live_features_v3.py`

### 1. Online market state management

Core state objects:

- `_ActiveMinute`
- `_MinuteCandle`
- `_MarketState`

Responsibilities:

- maintain per-market rolling 1m candle state
- keep currently open minute candle from live ticker flow
- track:
  - last price
  - last closed price
  - last event timestamp
  - previous 24h notional value from ticker

This part is not `v3-specific`.
It is shared online runtime infrastructure.

### 2. Ticker-to-1m candle ingestion

Method:

- `LiveFeatureProviderV3.ingest_ticker`

Responsibilities:

- accept a `TickerEvent`
- infer minute bucket
- estimate incremental base volume from `acc_trade_price_24h`
- update or roll the active 1m candle

This is also shared online runtime infrastructure, not a v3-only concern.

### 3. Bootstrap from historical 1m parquet

Methods:

- `_bootstrap_market`
- `_market_files`
- `_resolve_dataset_path`

Responsibilities:

- load historical 1m candles from parquet
- initialize live rolling state before pure WS data is sufficient
- label bootstrap quality:
  - `OK`
  - `PARTIAL`
  - `MISSING`
  - `EMPTY`

This is shared online runtime infrastructure.

### 4. Synthetic minute completion / continuity handling

Methods:

- `_flush_active_until_ts`
- `_append_synth_minute`
- `_build_one_m_frame`

Responsibilities:

- finalize active minute if the decision timestamp moved forward
- synthesize flat candles when there are time gaps
- create an on-demand rolling 1m frame ending at decision timestamp

This is shared online runtime infrastructure.

### 5. Rollup from 1m to base/high TF candles

Method:

- `_rollup_from_1m`

Responsibilities:

- roll 1m candles into:
  - base tf (`5m`)
  - high tfs (`15m`, `60m`, `240m`)

This is shared online runtime infrastructure.

### 6. Base v3 feature computation

Methods / imported functions:

- `compute_base_features_v3`
- `aggregate_1m_for_base`
- `join_1m_aggregate`
- `compute_high_tf_features`
- `join_high_tf_asof`
- `_attach_runtime_aux_columns`

Responsibilities:

- compute v3 feature contract from rolled base candles
- add 1m aggregate features
- add high-TF asof features
- compute runtime aux columns:
  - `atr_14`
  - `atr_pct_14`

This section is split:

- the multi-timeframe / 1m join mechanics are shared online infrastructure
- the actual choice of base feature contract is version-specific

### 7. Micro feature attachment

Methods:

- `_micro_feature_values`
- `_default_micro_feature_values`

Responsibilities:

- query `MicroSnapshotProvider`
- translate snapshot into prefixed micro columns
- emit default zeros for missing/stale micro

Important current behavior:

- missing micro values are explicitly filled with `0.0`

This is shared online runtime infrastructure, but the missing-value policy is a design choice we likely need to revisit.

### 8. Final row projection and missing handling

Methods:

- `_build_market_row`
- `_rows_to_frame`
- `_resolve_extra_row_value`
- `_to_feature_float`

Responsibilities:

- build final per-market runtime row
- project only requested feature columns
- add runtime aux columns in `extra_columns`
- count missing feature cells
- skip row when missing ratio exceeds threshold

Important current behavior:

- if a feature is missing, it is coerced to `0.0`
- only after missing-ratio exceeds threshold is the row dropped

This is where `rv_12 / rv_36 / atr_pct_14` can become `0.0` instead of `None`.

### 9. Build stats and observability

Methods:

- `status`
- `last_build_stats`

Responsibilities:

- expose provider health
- expose:
  - built rows
  - skipped reasons
  - bootstrap missing counts
  - missing feature ratio

This is shared online runtime infrastructure.

## What Is Actually v3-Specific

Only a subset of the file is conceptually v3-specific:

- `compute_base_features_v3`
- `feature_columns_v3_contract` dependency pattern
- assumptions about which columns are base contract columns

Most of the rest is really an online runtime feature-building engine.

## What A Native V4 Builder Should Own

A real `LiveFeatureProviderV4Native` should own:

1. same online candle state lifecycle
2. same bootstrap / synth / rollup infrastructure
3. same micro snapshot attachment
4. same runtime observability
5. its own base feature contract
6. its own missing policy
7. v4 enrichments directly, without going through a v3 base row first

## Recommended Extraction Boundary

Before writing a full native v4 builder, extract a shared module such as:

- `autobot/paper/live_features_online_core.py`

Suggested contents:

- `_ActiveMinute`
- `_MinuteCandle`
- `_MarketState`
- `_rollup_from_1m`
- `_rows_to_frame`
- `_resolve_dataset_path`
- `_market_files`
- `_estimate_volume_base_from_ticker`
- `_safe_float`
- `_safe_int`
- `_collect_lazy`
- shared online candle bootstrap / synth helpers
- shared micro translation helpers

Then:

- `LiveFeatureProviderV3` uses that core + v3 contract
- `LiveFeatureProviderV4Native` uses that core + v4 contract

## Why Native V4 Is Worth Doing

Current risks from the layered design:

- missing-value behavior is inherited from the v3 builder
- runtime row semantics are harder to reason about
- debugging `rv_12/rv_36` zeros is more confusing because v4 depends on v3 internals
- future v4-only runtime fixes are harder to apply safely

## Known Current Issue To Preserve During Migration

We already observed:

- runtime `state_features.rv_12/rv_36` can become `0.0`
- this likely increases `bin_audit_fallback`

When building `LiveFeatureProviderV4Native`, do not blindly preserve:

- `missing -> 0.0`

for all state features.

That behavior should be reviewed explicitly.

## Recommended Implementation Order

1. extract shared online runtime core from `live_features_v3.py`
2. add `LiveFeatureProviderV4Native` behind a non-default option
3. run shadow paper/replay comparison:
   - old `LIVE_V4`
   - new `LIVE_V4_NATIVE`
4. diff:
   - built row counts
   - missing ratios
   - `rv_12/rv_36/atr_pct_14`
   - candidate counts
   - fallback ratios
5. only then make `LIVE_V4_NATIVE` the default

## Practical Next Step

The next coding step should be:

- introduce `live_features_online_core.py`
- move the pure online-state helpers there
- leave `LiveFeatureProviderV3` behavior unchanged
- add a first `LiveFeatureProviderV4Native` scaffold using the extracted core
