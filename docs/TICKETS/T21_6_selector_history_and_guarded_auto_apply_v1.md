# T21.6 Selector History And Guarded Auto-Apply v1

## Goal
- Turn `T21.5` from report generation into a bounded automatic-improvement loop without letting a single noisy run prune the feature set.

## Why This Fits The Current Server
- it stores only compact family-level history and one latest policy file
- it reuses the existing `v4` feature universe instead of adding new raw data families
- it delays expensive extra validation until a guarded policy actually changes the feature set

## Scope
In scope:
- persist compact factor-block selection history at the model-family level
- derive a guarded auto-apply policy from recent non-weak runs
- keep full-set training until enough stable history exists
- auto-apply only when:
  - enough eligible runs exist
  - accepted ratio is stable
  - economic edge remains positive
  - coverage/turnover proxy stays bounded
- auto-trigger `CPCV-lite` only when guarded policy actually prunes the active feature set

Out of scope:
- one-run immediate pruning
- large wrapper subset search
- changing runtime contracts
- making `CPCV-lite` mandatory for every daily run

## Exact Implementation Standard
- every training run writes:
  - per-run `factor_block_selection.json`
  - family-level `factor_block_selection_history.jsonl`
  - family-level `latest_factor_block_policy.json`
- guarded policy must emit:
  - records considered
  - eligible run count
  - accepted and rejected blocks
  - selected feature columns
  - reasons why pruning is or is not active
- if history is insufficient, the trainer must keep the full feature set and say so explicitly
- `CPCV-lite` may auto-enable only when the guarded policy is actively pruning features

## Deliverables
- selector history artifact
- guarded policy artifact
- `guarded_auto` resolution mode in the trainer
- conditional `CPCV-lite` activation for policy-applied runs
- tests for:
  - missing-policy fallback
  - stable-policy activation
  - trainer auto-pruning behavior
  - trainer auto-`CPCV-lite` trigger

## Acceptance
- the daily trainer can stay on the full `v4` feature set while history is warming up
- after enough stable evidence, the trainer can automatically consume a guarded subset
- storage impact stays negligible
- extra validation cost appears only on policy-applied runs

## Resource Fit
- CPU: low to medium
- RAM: low
- Disk: low

## Follow-On Path
- later extend guarded policy inputs with:
  - candidate acceptance outcome
  - champion/challenger paper outcome
  - regime-conditioned block stability
