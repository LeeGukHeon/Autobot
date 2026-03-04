# ADR 0010: Champion Promotion Policy for `train_v2_micro`

- Date: 2026-03-04
- Status: Accepted
- Context: T14.2 introduces micro-augmented v2 training where early data windows can have sufficient rows for training but limited date span and unstable coverage patterns.

## Decision
- Register every `trainer=v2_micro` run as `latest` in `models/registry/train_v2_micro/<run_id>/`.
- Promote to `champion` only when all conditions pass:
  - improvement vs v1 baseline on same window:
    - `Precision@Top5% delta >= +0.02` OR
    - `EV_net@Top5% delta >= +0.0002`
  - no per-market collapse (delta thresholds enforced in policy)
  - `distinct_dates >= 30`
  - reproducibility: improvement sustained for `>= 2` consecutive v2 retrains
- If any condition fails, keep status as `candidate` and do not update v2 champion pointer.

## Consequences
- Early v2 runs are prevented from replacing production defaults prematurely.
- Comparison and promotion evidence is persisted per run:
  - `compare_to_v1.json`
  - `promotion_decision.json`
- Current ticket does not switch runtime default model selection because v2 remained `candidate`.
