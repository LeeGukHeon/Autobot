# V5 PRIMARY RUNTIME MIGRATION CONTRACT 2026-03-27

## 0. Purpose

This document records the current migration decision after reconciling:

- the training blueprint
- the evaluation / paper / live blueprint
- the risk / live-control blueprint
- the runtime / deployment audit findings

Its job is to make one thing explicit:

- the current production-bound replacement for the old v4 runtime slot is `train_v5_fusion`

This is the practical migrated operating path.

## 1. Finalized Operating Rule

Current primary runtime family:

- `train_v5_fusion`

Current primary runtime refs:

- champion lane: `champion`
- candidate lane: `latest_candidate`

Current primary runtime feature contract:

- `feature_set = v4`
- live / paper provider family is `live_v5`

Meaning:

- the runtime slot has migrated from `train_v4_crypto_cs` to `train_v5_fusion`
- the live/backtest/paper runtime still consumes the shared `features_v4` contract as the panel backbone
- the runtime adapter now builds the missing sequence / LOB / fusion expert inputs online on top of the shared live data plane
- the feature-plane replacement is still not a prerequisite for the primary slot migration, but the runtime now resolves the fused expert contract instead of stopping at the panel-only family

## 2. Expert Family Status

The following families remain valid expert / research families feeding the primary runtime slot:

- `train_v5_panel_ensemble`
- `train_v5_sequence`
- `train_v5_lob`

Current contract:

- they write family-local `latest`
- `train_v5_fusion` is the default production runtime family
- the panel / sequence / LOB families remain explicit expert dependencies for the fused slot rather than silent generic fallbacks

Reason:

- the runtime adapter now resolves the exact panel / sequence / LOB expert inputs required by the fused model on live / paper paths
- the fused family can therefore replace the old panel-only primary slot without breaking the shared `features_v4` operating contract

Current implemented support for those expert families:

- `train_v5_sequence` writes `expert_prediction_table.parquet` and now exposes a predictor-valid tabular bridge on top of the pooled sequence feature surface
- `train_v5_sequence` also writes a runtime-loadable pooled feature dataset plus the required runtime/governance artifact bundle under the run directory so the family no longer points only to the raw tensor cache root
- `train_v5_lob` writes `expert_prediction_table.parquet` and now exposes a predictor-valid tabular bridge on top of the pooled LOB feature surface
- `train_v5_lob` also writes a runtime-loadable pooled feature dataset plus the required runtime/governance artifact bundle under the run directory so the family no longer points only to the tensor cache root
- `train_v5_fusion` auto-resolves the latest panel / sequence / LOB expert prediction tables through the CLI when explicit paths are not passed
- `train_v5_fusion` now writes its own runtime-loadable feature dataset, entry-boundary artifact, and complete runtime/governance artifact bundle so the family is promote-ready under the existing candidate/adoption contract
- `LIVE_V5` paper/live providers now reconstruct the online panel / sequence / LOB expert inputs needed by the fused model instead of treating the fusion family as offline-only

## 3. Required Defaults After Migration

Config / runtime defaults must now point to:

- `model_ref = champion`
- `model_family = train_v5_fusion`

Candidate defaults must now point to:

- `model_ref = latest_candidate`
- `model_family = train_v5_fusion`

Acceptance / deployment / dashboard / topology reporting must not assume:

- `champion_v4`
- `latest_candidate_v4`
- `train_v4_crypto_cs`

unless the caller explicitly requests the legacy lane.

## 4. Operational Invariants

1. The daily default acceptance path is `scripts/v5_governed_candidate_acceptance.ps1`.
2. The dashboard must resolve training / candidate state from the current primary family first and fail closed if that family has no candidate.
3. Runtime topology and preflight defaults must use the v5 primary family.
4. Cross-family comparison remains allowed, but it must be explicit through `ChampionCompareModelFamily`.
5. Legacy v4 aliases remain compatibility shorthands only. They are not the default operating path.

## 5. Required Training Data Before V5 Acceptance

The `v5_fusion` acceptance chain is not allowed to start from a partial mutable working tree.

The required pre-training data plane is:

- `candles_api_v1`
  - role: base market candles for `1m / 5m / 15m / 60m / 240m`
  - consumed by: `features_v4`, `candles_second_v1` planning, sequence/LOB pooled feature context
- `candles_second_v1`
  - role: second-level context for `sequence_v1`
  - consumed by: `train_v5_sequence`, `train_v5_lob`
- `ws_candle_v1`
  - role: websocket 1s / 1m candle context
  - consumed by: `sequence_v1`, `train_v5_sequence`, `train_v5_lob`
- `micro_v1`
  - role: mandatory micro contract for `features_v4` plus sequence tensor context
  - consumed by: `features_v4`, `sequence_v1`
- `lob30_v1`
  - role: 30-level orderbook tensor source
  - consumed by: `sequence_v1`, `train_v5_lob`
- `sequence_v1`
  - role: cached tensor store for sequence/LOB expert training
  - consumed by: `train_v5_sequence`, `train_v5_lob`
- `private_execution_v1`
  - role: execution supervision / tradability labels
  - consumed by: `train_v5_tradability`
- `features_v4`
  - role: final panel/fusion feature contract
  - consumed by: `train_v5_panel_ensemble`, `train_v5_fusion`

The required expert chain is:

- `train_v5_panel_ensemble`
- `train_v5_sequence`
- `train_v5_lob`
- `train_v5_tradability`
- `train_v5_fusion`

The hard invariant is:

- every expert run used by `train_v5_fusion` must point to the same non-empty `data_platform_ready_snapshot_id`

## 6. Collection, Reuse, And Freeze Rule

Normal operation is three stages:

1. Collect or top-up mutable source layers.
2. Reuse already-complete derived layers whenever the source validity signature has not changed.
3. Freeze the accepted working set through `data_platform_ready_snapshot.publish` and train only against that frozen snapshot.

The current reuse/freeze contract is:

- mutable collection layer
  - `candles_api_v1`
  - `candles_second_v1`
  - `ws_candle_v1`
  - `micro_v1`
  - `lob30_v1`
- derived reusable layer
  - `sequence_v1`
  - `private_execution_v1`
  - `features_v4`
- frozen ready snapshot
  - `candles_second_v1`
  - `ws_candle_v1`
  - `lob30_v1`
  - `sequence_v1`
  - `private_execution_v1`
  - `candles_api_v1`
  - `features_v4`

The primary freeze command remains:

- `autobot.ops.data_platform_snapshot publish`

Acceptance and downstream v5 trainers must prefer frozen snapshot artifacts over mutable working-tree artifacts whenever the close contract exists and passes.

## 7. Source-By-Source Reuse Rule

The intended operating rule is not "rerun the whole chain until the close passes".

The intended operating rule is:

- inspect each required source independently
- reuse it when the required window is already complete and not stale
- refresh only the missing or stale source
- publish a new ready snapshot only after every required source is complete

This is the required source-by-source reuse policy.

### 7.1 `candles_api_v1`

Expected policy:

- always reuse existing partitions that already cover the required window
- refresh only missing tail or explicit stale holes
- do not rebuild already-covered historical ranges just because another timeframe is missing

Required completeness check:

- for every required tf (`1m`, `5m`, `15m`, `60m`, `240m`)
- for every selected market
- `max_ts_ms` must cover the required end of the train/certification window

Observed recent failure mode:

- `60m` tail was missing while `1m`, `15m`, and `240m` were current
- the feature plane then failed later through stale high-tf joins

Implementation direction:

- top-up plan must prioritize cheap high-tf tail completion before lower-tf bulk work
- request budget must be large enough to let required high-tf tails finish
- close must fail early if required high-tf candle coverage is incomplete

### 7.2 `candles_second_v1`

Expected policy:

- historical second candles must be reused whenever the required date/market partitions already exist and validate
- refresh only missing dates or stale partitions for the requested window

Current problem:

- even single-day `training_critical` refresh still spends heavy request budget on second-level collection

Implementation direction:

- add date/market completeness checks comparable to the `sequence_v1` reuse contract
- avoid global replay of already-covered historical dates

### 7.3 `ws_candle_v1`

Expected policy:

- runtime-rich websocket candles are mutable online context and should be refreshed only when runtime-rich refresh is requested
- historical training-critical refresh should not treat websocket candle collection as a mandatory replay layer unless a downstream contract explicitly depends on it for the requested window

Implementation direction:

- keep this layer separated from the minimal training-critical path whenever possible

### 7.4 `micro_v1`

Expected policy:

- reuse existing date/market partitions when they already match the required source coverage
- recompute only the requested dates that are missing, stale, or invalid

Current problem:

- current training-critical flow overwrites the whole requested date window even when most dates are already present

Implementation direction:

- add date completeness and source-signature checks so micro aggregation becomes incremental

### 7.5 `lob30_v1`

Expected policy:

- historical training-critical refresh must reuse already-valid `lob30_v1` partitions
- real-time websocket collection should not be the default historical recovery behavior

Current problem:

- `training_critical` still runs real-time `lob30` collection with a fixed wall-clock duration
- this makes historical rebuild tests expensive even when the required date already has usable data

Implementation direction:

- either separate `lob30` from historical training-critical refresh
- or add date-level completeness/staleness checks and skip real-time collection when the requested historical date is already complete

### 7.6 `sequence_v1`

Expected policy:

- reuse at the finest valid granularity first
- anchor-level reuse is the minimum contract
- date-level completeness reuse is the next required layer

Current implemented direction:

- anchor-level ready reuse already existed
- date-level completeness reuse was added so fully complete past dates can skip source-frame loading and rebuild entirely

Still required:

- stronger latest-date reuse
- completeness/staleness checks that reduce rebuild cost for near-current dates instead of only older dates

### 7.7 `private_execution_v1`

Expected policy:

- reuse the current label store whenever the underlying execution state/journal coverage for the requested window has not changed
- rebuild only when new execution evidence changes the requested label window

Implementation direction:

- add a cheap source fingerprint for the requested execution label window

### 7.8 `features_v4`

Expected policy:

- reuse the existing feature dataset when the required train/certification window is fully covered and source signatures are unchanged
- rebuild only the window that is missing or stale

Current problem:

- acceptance can still discover late that the frozen feature dataset stops before the required train window end

Implementation direction:

- `features_v4` coverage must be checked directly during close
- close must refuse to publish a snapshot whose frozen `features_v4` coverage does not reach the required window end

## 8. Multi-Timeframe Requirement

`features_v4` under the primary v5 runtime is a real multi-timeframe contract, not a best-effort enrichment.

Required base / high timeframes:

- base tf: `1m` or `5m`
- required high tf set: `15m`, `60m`, `240m`

Operational rule:

- if any required high-tf coverage does not extend through the required train/certification window, the close contract must fail closed

Recent server investigation confirmed:

- `1m`, `15m`, and `240m` could be current while `60m` remained stale
- that stale `60m` tail alone was enough to truncate `features_v4.effective_end`
- acceptance then failed with `FEATURES_V4_WINDOW_NOT_COVERED_BY_CLOSE` / `INSUFFICIENT_TRAINABLE_V4_ROWS`

This means:

- multi-timeframe coverage must be treated as a first-class precondition before training
- high-tf candles are not optional convenience inputs for the primary v5 path

## 9. Current Cost Hotspots

The expensive stages before training are currently:

- `collect_candles_second`
  - Upbit REST request budget can be exhausted before all targets are refreshed
- `collect_lob30`
  - currently uses a fixed real-time duration and therefore costs wall-clock time even for historical refresh
- `collect_sequence_tensors`
  - the largest repeated cost in `training_critical`

Observed current optimization direction:

- candle top-up planning now prioritizes low-call high-tf tails first so `60m` is not starved behind lower-tf bulk targets
- candle refresh request budget was increased from `120` to `240`
- `sequence_v1` now records date-level completeness so already-complete past dates can be skipped without reloading all source frames

Still required for a fully normal contract:

- explicit high-tf coverage preflight before close
- stronger latest-date tensor reuse
- removal or isolation of real-time `lob30` collection from historical `training_critical` refresh

## 10. Why This Is The Current End-State

The blueprints describe an end-state with:

- `v5_panel`
- `v5_sequence`
- `v5_lob`
- `v5_fusion`

The audited codebase now provides the remaining runtime slice that was previously missing:

- `LIVE_V5` runtime input reconstruction for the fused expert family
- scheduled panel -> sequence -> LOB dependency training before fused acceptance
- promote-ready runtime/governance bundles for the fused slot

This migration contract therefore defines `train_v5_fusion` as the strongest currently deployable end-state of the primary runtime path.

## 11. Implementation Priority

The expected implementation order is not arbitrary.

The highest-value work is the work that:

- fails earlier
- preserves more already-collected data
- reduces repeated wall-clock cost for every nightly run

The current recommended implementation priority is:

1. `candles_api_v1` high-tf completeness
   - reason:
     - a missing `60m` tail can invalidate the whole `features_v4` training window
     - this is a cheap upstream failure point and should be made deterministic first
   - required work:
     - keep high-tf tail prioritization
     - keep sufficient candle request budget
     - add explicit high-tf coverage fail-close before snapshot publish

2. `sequence_v1` latest-date incremental reuse
   - reason:
     - this is the largest repeated cost inside `training_critical`
     - anchor/date reuse directly lowers the cost of every close attempt
   - current state:
     - anchor-level reuse exists
     - date-level reuse for complete past dates exists
   - next work:
     - improve latest-date reuse so near-current dates do not rebuild almost everything

3. `lob30_v1` separation from historical training-critical refresh
   - reason:
     - current training-critical mode still pays a real-time wall-clock collection cost
     - this is disproportionate for historical rebuilds
   - next work:
     - either skip `lob30` in historical training-critical mode when completeness already exists
     - or move `lob30` to a separately-managed mutable layer outside the default historical close path

4. `candles_second_v1` date-level completeness
   - reason:
     - current single-day refresh still uses the full candle request budget
     - this is the next major repeated source-plane cost after sequence tensors
   - next work:
     - add date/market completeness and incremental tail-only refresh

5. `micro_v1` date-level incremental rebuild
   - reason:
     - current aggregate/validate flow can still overwrite more than is necessary
   - next work:
     - add date-level stale/missing checks and rebuild only those dates

6. `features_v4` window coverage preflight
   - reason:
     - acceptance currently discovers some unusable frozen windows too late
   - next work:
     - promote feature-window coverage checks into the close contract itself

7. `private_execution_v1` source-fingerprint reuse
   - reason:
     - less expensive than the layers above, but still should not rebuild when the underlying execution evidence has not changed

The practical summary is:

- first fix the cheapest upstream contracts that can invalidate the whole nightly run
- then reduce the large repeated tensor/source costs
- finally tighten smaller downstream reuse surfaces
