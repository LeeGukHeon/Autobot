# T21.8 Shared Rank Selection Policy v1

- Date: 2026-03-10
- Status: implemented locally
- Scope:
  - shared paper/live selection contract
  - replace raw global probability cutoff dependence with a persisted cross-sectional rank policy
  - keep execution/risk gates environment-specific

## Goal

Make `paper` and `live` use the same learned selection policy artifact so that:

- the same champion model produces the same ranked candidate set
- `live` does not stall because a global `top_1pct` raw threshold is too sparse for a small real-time universe
- selection remains auditable and reproducible from registry artifacts

## Why

Recent crypto cross-sectional literature is closer to:

- cross-sectional portfolio sorting
- rank-based candidate selection
- economic significance after costs

than to a single absolute raw-score cutoff carried directly into runtime.

## Contract

Persist `selection_policy.json` per run with:

- `mode`
- `selection_fraction`
- `min_candidates_per_ts`
- `threshold_key`
- `threshold_value`
- `eligible_ratio`
- `recommended_top_pct`
- `selection_recommendation_source`
- compact objective metadata

Supported runtime modes:

- `raw_threshold`
- `rank_effective_quantile`

`paper` and `live` must interpret this artifact through the same strategy module.

## Runtime Rules

- `auto`
  - use registry `selection_policy` if present
  - otherwise keep existing raw-threshold behavior
- explicit manual `min_prob`
  - forces `raw_threshold`
- `rank_effective_quantile`
  - score all active names
  - rank descending by `model_prob`
  - select `max(floor(N * selection_fraction), min_candidates_per_ts)`

## Non-Goals

- calibration artifact
- live-only fallback heuristics
- ranker lane promotion

Those should be separate follow-up tickets.

## Acceptance

- `train_v4_crypto_cs` writes `selection_policy.json`
- `train_config.yaml` includes `selection_policy`
- `ModelPredictor` loads `selection_policy`
- `ModelAlphaStrategyV1` uses the same policy in paper/live
- tests show:
  - rank policy can select candidates where a raw threshold would yield zero
  - manual `min_prob` still forces raw-threshold behavior

