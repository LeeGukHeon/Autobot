# V5 PRIMARY RUNTIME MIGRATION CONTRACT 2026-03-27

## 0. Purpose

This document records the current migration decision after reconciling:

- the training blueprint
- the evaluation / paper / live blueprint
- the risk / live-control blueprint
- the runtime / deployment audit findings

Its job is to make one thing explicit:

- the current production-bound replacement for the old v4 runtime slot is `train_v5_panel_ensemble`

This is the practical migrated operating path.

## 1. Finalized Operating Rule

Current primary runtime family:

- `train_v5_panel_ensemble`

Current primary runtime refs:

- champion lane: `champion`
- candidate lane: `latest_candidate`

Current primary runtime feature contract:

- `feature_set = v4`
- live / paper provider family remains `live_v4`

Meaning:

- the runtime slot has migrated from `train_v4_crypto_cs` to `train_v5_panel_ensemble`
- the live/backtest/paper runtime still consumes the shared `features_v4` contract
- the model family changed first
- the feature-plane replacement is not a prerequisite for the primary slot migration

## 2. Expert Family Status

The following families remain valid expert / research families, but they are not the primary always-on runtime slot:

- `train_v5_sequence`
- `train_v5_lob`
- `train_v5_fusion`

Current contract:

- they may write family-local `latest`
- they must not be treated as the default production runtime family
- they must not silently become the global primary runtime through generic fallbacks

Reason:

- the audited runtime path is fully ready only for `train_v5_panel_ensemble`
- sequence / LOB / fusion still require additional runtime-input wiring or expert-table orchestration before they can replace the primary slot cleanly

## 3. Required Defaults After Migration

Config / runtime defaults must now point to:

- `model_ref = champion`
- `model_family = train_v5_panel_ensemble`

Candidate defaults must now point to:

- `model_ref = latest_candidate`
- `model_family = train_v5_panel_ensemble`

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

The blueprints describe a longer-term end-state with:

- `v5_panel`
- `v5_sequence`
- `v5_lob`
- `v5_fusion`

But the audited codebase shows that the clean runtime replacement today is:

- `v5_panel_ensemble`

This migration contract therefore defines the strongest currently deployable end-state, not a fictional one.

Future work may promote `v5_fusion` into the primary runtime family once its runtime input adapter and promotion path are fully wired.
