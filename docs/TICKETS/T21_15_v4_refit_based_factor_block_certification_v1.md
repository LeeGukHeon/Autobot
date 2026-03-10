# T21.15 V4 Refit-Based Factor Block Certification v1

- Date: 2026-03-11
- Status: landed locally

## Goal
- Replace median-ablation factor-block selection with bounded refit/drop-block certification.

## Literature Basis
- `Using Machines to Advance Better Models of the Crypto Return Cross-Section`
  - https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4986862

## Why
- current selector is compact and pragmatic, but median ablation is only a rough proxy
- with correlated blocks and tree interactions, ablation can misstate true incremental value
- guarded auto-pruning should rely on stronger evidence before it changes the active feature set

## Scope
In scope:
- run bounded drop-block refits on selected windows
- certify block usefulness using incremental OOS economic contribution
- keep history-based guarded auto policy, but feed it refit-based evidence

Out of scope:
- exhaustive subset search
- black-box SHAP-only pruning

## Exact Implementation Standard
- every rejected optional block must have stored refit evidence
- protected base blocks remain non-prunable
- if budget is insufficient, keep full set and emit explicit insufficiency reasons

## Acceptance
- pruning decisions are based on stored refit evidence, not one-pass ablation only
- guarded auto no longer activates from median-ablation summaries alone

## 2026-03-11 Slice 1 Implementation
- `train_v4_crypto_cs.py` now runs bounded drop-block refits on each walk-forward window for optional blocks:
  - it reuses the selected window hyperparameters
  - it stores `refit_drop_block` evidence rows alongside the existing diagnostic rows
- `factor_block_selector.py` now treats:
  - `median_ablation` as diagnostic-only
  - `refit_drop_block` as the required evidence mode for optional-block rejection
- selection reports now record, per block:
  - `available_evidence_modes`
  - `evidence_mode_used`
  - `refit_certified`
- guarded auto history now only counts refit-certified evidence for optional blocks
- if refit history is missing, the policy keeps the full set with explicit reasons instead of silently pruning

## Regression Coverage
- `tests/test_factor_block_selector.py`
  - median-ablation-only history no longer activates guarded auto
  - refit-certified history can activate guarded auto pruning
- `tests/test_train_v4_crypto_cs.py`
  - bounded refit rows are tagged with `evidence_mode = refit_drop_block`

## 2026-03-11 Slice 2 Implementation
- bounded refit execution is now non-fatal:
  - per-block refit failures keep the full set instead of aborting the run
  - explicit `reason_codes` are stored for missing support such as:
    - `MISSING_WINDOW_BEST_PARAMS`
    - `SELECTION_BASELINE_UNAVAILABLE`
    - `REFIT_MODEL_FAILED_*`
- walk-forward now stores per-window `factor_block_refit_windows`
- `factor_block_selection.json` now stores `refit_support`:
  - run-level support summary
  - block-level support status and insufficiency reasons
- `train_config.yaml`, `decision_surface.json`, and the experiment ledger now surface the same refit-support contract

## Additional Regression Coverage
- `tests/test_train_v4_crypto_cs.py`
  - non-fatal refit failure is captured as explicit support evidence
  - run artifacts surface `refit_support`
- `tests/test_factor_block_selector.py`
  - report-level insufficiency reasons include refit-support provenance
- `tests/test_experiment_ledger.py`
  - ledger captures refit-support status
