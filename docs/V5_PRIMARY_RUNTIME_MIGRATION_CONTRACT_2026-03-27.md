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

## 5. Why This Is The Current End-State

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
