# T21.21 V4 Non-Heuristic Split Policy And Bootstrap Lane v1

- Date: 2026-03-13
- Status: proposed
- Scope:
  - `scripts/candidate_acceptance.ps1`
  - `scripts/v4_promotable_candidate_acceptance.ps1`
  - `scripts/v4_scout_candidate_acceptance.ps1`
  - `scripts/v4_rank_shadow_candidate_acceptance.ps1`
  - `scripts/v4_rank_governed_candidate_acceptance.ps1`
  - `scripts/daily_champion_challenger_v4_for_server.ps1`
  - `autobot/models/train_v4_crypto_cs.py`
  - `autobot/models/research_acceptance.py`
  - `autobot/models/execution_acceptance.py`
  - `autobot/models/search_budget.py`
  - `autobot/models/runtime_recommendations.py`

## Goal
- Replace the current fixed `backtest_lookback_days = 8` promotion split with a data-driven split policy.
- Keep promotion evidence clean:
  - no train / certification overlap in the promotable lane
- Allow latest-inclusive learning when post-floor history is still short:
  - but only inside a non-promotable bootstrap/canary lane

## Why This Ticket Exists
- `T21.12` correctly separated:
  - `train_window`
  - `research_window`
  - `certification_window`
- `T21.14` added a train-window ramp from available micro coverage.
- `T23.2` and the 2026-03-13 follow-up made the quality floor explicit:
  - `train_data_quality_floor_date = 2026-03-04`

That hardening exposed the next real methodology gap:

- the system still uses a fixed certification holdout length
- fixed `8` days is a heuristic, not a data-selected contract
- when usable post-floor history is short, the fixed holdout can consume almost the entire trainable window

Observed current example on the live server:

- `q = 2026-03-04`
- `batch_date = 2026-03-12`
- fixed `backtest_lookback_days = 8`
- current promotable split implies:
  - `train = 2026-03-04 .. 2026-03-04`
  - `certification = 2026-03-05 .. 2026-03-12`
- that one-day train slice produced:
  - `rows_final = 899`
  - `min_rows_for_train = 4000`
- but the full post-floor latest-inclusive window:
  - `2026-03-04 .. 2026-03-12`
  - produced `rows_final = 11628`

So the blocker is no longer hidden coupling or stale wrappers.
The blocker is the fixed split policy itself.

## Literature Basis
- `Backtest Overfitting in the Machine Learning Era`
  - recent finance-oriented comparison of:
    - K-fold
    - purged K-fold
    - walk-forward
    - CPCV
  - emphasizes false discovery control and overfitting-aware validation
  - https://doi.org/10.1016/j.knosys.2024.112477
  - https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4686376
- `VIX constant maturity futures trading strategy: A walk-forward machine learning study`
  - recent live-trading-style empirical paper using explicit walk-forward evaluation
  - keeps OOS separation rather than training on the final evaluation slice
  - https://doi.org/10.1371/journal.pone.0302289
- `Optimal model averaging based on forward-validation`
  - argues that forward-validation should be used for time-ordered model choice
  - supports choosing windowing policy from time-respecting validation rather than a fixed manual constant
  - https://www.sciencedirect.com/science/article/pii/S030440762200094X
- `Machine Learning in Asset Pricing`
  - weak-signal finance setting; regularized methods and disciplined validation matter more than naive complexity
  - https://doi.org/10.3386/w33421
- `Artificial Intelligence in Finance: From Market Prediction to Macroeconomic and Firm-Level Forecasting`
  - recent review emphasizing:
    - leakage controls
    - reproducible evaluation
    - time-aware OOS design
  - https://www.mdpi.com/2673-2688/6/11/295
- `Addressing Concept Shift in Online Time Series Forecasting: Detect-then-Adapt`
  - supports a separate adapt lane rather than silently collapsing promotion evaluation into latest-inclusive fit
  - https://arxiv.org/abs/2403.14949
- `DeepFund: A Live Arena Perspective`
  - backtests are not enough; final validation needs live-like evaluation
  - supports using canary/paper/live as a distinct lane rather than contaminating promote evidence
  - https://arxiv.org/abs/2503.18313

## Problem Statement
The current promotable lane mixes two very different needs:

1. promotion evidence must preserve a true out-of-sample certification slice
2. the model should adapt to the most recent usable history

Those needs conflict when post-floor history is still short.

The wrong response would be:
- silently letting train overlap with certification in the promotable lane

The right response is:
- keep the promotable lane strict
- make the split length itself data-driven
- add a separate latest-inclusive bootstrap lane for non-promotable adaptation

## Terms
- `q`
  - train data quality floor date
  - earliest date admissible for promotion-grade training
- `b`
  - current batch date
- `H`
  - certification holdout length in whole days
- `T_H(b)`
  - promotable train window
  - `T_H(b) = [q, b - H]`
- `C_H(b)`
  - promotable certification / backtest window
  - `C_H(b) = [b - H + 1, b]`
- `B(b)`
  - bootstrap latest-inclusive train window
  - `B(b) = [q, b]`
- `r_min`
  - minimum trainable row requirement for the active feature set
- `U`
  - frozen execution-aware certification utility from the shared objective contract

## Non-Negotiable Methodology Rules
- Do not let the promotable lane train on its own certification window.
- Do not keep `H = 8` as a protected magic constant.
- Do not replace the promotion evidence contract with a latest-inclusive fit just because the post-floor history is still short.
- If the promotable split is not feasible, emit an explicit bootstrap-lane status rather than silently weakening the promotable gate.
- Keep train / certification / bootstrap lane identities explicit in every report.
- If evidence is insufficient, emit:
  - `INSUFFICIENT_EVIDENCE`
  - `INSUFFICIENT_TRAINABLE_ROWS`
  - `BOOTSTRAP_ONLY_POLICY`
  - not a silent fallback

## Policy
### 1. Two Explicit Lanes
The system must expose two split-policy lanes:

- `promotion_strict`
  - no overlap between train and certification
  - candidate may become promotable
- `bootstrap_latest_inclusive`
  - latest-inclusive fit allowed
  - candidate may run in paper/canary/shadow
  - candidate may not promote or replace champion from this lane alone

### 2. Candidate Holdout Set
For each batch date `b`, construct candidate holdout lengths:

- `H in {1, 2, ..., H_max(b)}`

where `H_max(b)` is bounded by:
- post-floor history length
- server budget
- minimum anchor count needed for forward-validation

This ticket does not require a large arbitrary candidate set.
A compact bounded set is acceptable, for example:
- `H in {1, 2, 3, 4, 5, 6, 7, 8}`

But the chosen `H*` must come from data-driven validation, not a hardcoded default winner.

### 3. Historical Anchor Construction
For each candidate `H`, build historical anchor dates `tau` satisfying:

- `tau < b`
- `T_H(tau)` exists
- `C_H(tau)` exists
- `rows(T_H(tau)) >= r_min`

At least two admissible anchors are required for promotable split selection.

If fewer than two admissible anchors exist:
- `promotion_strict` is not selectable
- `bootstrap_latest_inclusive` remains the only admissible lane

### 4. Inner / Outer Evaluation Separation
For each admissible pair `(H, tau)`:

- inner fit/search:
  - use only `T_H(tau)`
  - time-aware inner validation only
  - `CPCV-lite`, purged folds, or a bounded temporal inner split
- outer certification:
  - use only `C_H(tau)`
  - this produces the out-of-sample score for choosing `H`

Hard rule:
- outer certification days may not enter inner fit/search

### 5. Holdout-Length Selection
For each admissible `H`, compute certification utilities across anchors:

- `U_(H,tau)`

Aggregate them into:

- `mu_H`
  - mean utility
- `se_H`
  - serial-correlation-robust standard error
  - HAC / Newey-West style estimate is preferred
- `LCB_H`
  - lower confidence bound
  - `LCB_H = mu_H - z_(1-alpha) * se_H`

Choose:

- `H* = argmax_H LCB_H`

Interpretation:
- choose the holdout length with the strongest conservative certification utility
- not the one that only has the highest noisy mean

### 6. Current-Batch Promotion Split
Once `H*` is chosen for batch `b`:

- promotable train window:
  - `T_(H*)(b) = [q, b - H*]`
- promotable certification window:
  - `C_(H*)(b) = [b - H* + 1, b]`

The candidate may only enter promotion comparison if:
- trainable rows in `T_(H*)(b)` meet `r_min`
- certification is non-empty
- no overlap exists

### 7. Bootstrap Lane
If no admissible `H*` exists, but the latest-inclusive post-floor window is trainable:

- `rows(B(b)) >= r_min`

then the system may run:

- `bootstrap_latest_inclusive`

using:
- train = `B(b) = [q, b]`
- no separate promotable certification holdout for that run

But this lane must be marked:
- `promotion_eligible = false`
- `lane_role = bootstrap`
- `promotion_policy_status = BOOTSTRAP_ONLY_POLICY`

### 8. Promotion Guard
A bootstrap candidate may:
- run in paper
- run in canary/shadow
- generate diagnostics

It may not:
- write a promotable acceptance success
- auto-promote champion
- satisfy the certification-only promotion gate

## Exact Artifact Contract
### 1. Split Policy Artifact
Each daily candidate run must persist:

- `split_policy_decision.json`

Required fields:
- `version`
- `policy_id`
- `batch_date`
- `train_data_quality_floor_date`
- `lane_mode`
  - `promotion_strict`
  - `bootstrap_latest_inclusive`
- `promotion_eligible`
- `candidate_holdout_days`
- `selected_holdout_days`
- `selected_by`
  - `forward_validation_lcb`
  - `bootstrap_fallback`
- `admissible_holdout_days`
- `historical_anchor_count`
- `selection_summary`
  - per-`H`:
    - anchor_count
    - mean_utility
    - standard_error
    - lower_confidence_bound
    - admissible
    - insufficiency_reasons
- `current_batch_windows`
  - `train_window`
  - `certification_window`
  - `bootstrap_window`
- `overlap_status`
- `min_rows_for_train`
- `current_batch_rows_final`
- `reason_codes`

### 2. Acceptance Report Surface
`candidate_acceptance.ps1` reports must expose:

- `split_policy_id`
- `lane_mode`
- `promotion_eligible`
- `train_data_quality_floor_date`
- `selected_holdout_days`
- `historical_anchor_count`
- `split_policy_reason`
- `windows_by_step.train`
- `windows_by_step.certification`
- `windows_by_step.bootstrap`
- `windows_by_step.backtest`
- `bootstrap_only`

### 3. Certification Contract
If `lane_mode = bootstrap_latest_inclusive`:

- `gates.overall_pass` may not imply promotion eligibility
- the acceptance report must explicitly say:
  - `promotion_eligible = false`
  - `bootstrap_only = true`

## Runtime / Orchestration Rules
- `daily_champion_challenger_v4_for_server.ps1` must treat:
  - `BOOTSTRAP_ONLY_POLICY`
  - as a successful non-promotable training day
- challenger bootstrap candidates may be allowed only if a dedicated bootstrap runtime policy explicitly permits them
- default safe behavior:
  - bootstrap candidate trains
  - bootstrap candidate may paper/canary
  - no champion mutation

## Current Recommended Policy On The Live Server
At the current post-floor history length:

- `promotion_strict`
  - should remain closed unless at least one admissible `H` has:
    - valid historical anchors
    - non-overlapping certification
    - `rows(T_H(b)) >= r_min`
- `bootstrap_latest_inclusive`
  - should be open
  - because:
    - `q = 2026-03-04`
    - `B(2026-03-12) = [2026-03-04, 2026-03-12]`
    - current server measurement produced:
      - `rows_final = 11628`
      - `min_rows_for_train = 4000`

## Implementation Plan
### Slice 1. Split Policy Artifact
- add `split_policy_decision.json`
- version the lane mode and holdout-selection result

### Slice 2. Bootstrap Lane Contract
- add `bootstrap_latest_inclusive`
- make it explicitly non-promotable
- keep canary/paper eligibility explicit and separate

### Slice 3. Historical Holdout Selection
- add bounded historical anchor evaluation
- compute per-`H` certification utility summaries
- select `H*` by lower confidence bound

### Slice 4. Acceptance / Orchestration Integration
- acceptance consumes the split policy artifact
- daily orchestrator treats bootstrap-only as non-fatal but non-promotable

### Slice 5. Dashboard / Audit Surface
- dashboard shows:
  - lane mode
  - selected holdout
  - quality floor
  - current train/certification/bootstrap windows
  - promotion eligibility

## Acceptance
- the promotable lane no longer depends on a fixed manual `8`-day holdout as the final rule
- holdout length is chosen by a time-respecting validation procedure
- train and certification remain non-overlapping in the promotable lane
- bootstrap latest-inclusive fitting is possible without contaminating the promotion gate
- reports make it impossible to confuse:
  - promotable strict certification
  - bootstrap/canary adaptation

## Non-Goals
- rewriting the current shared objective contract
- replacing current multiple-testing artifacts
- adding a heavy full-CPCV default to every daily run
- allowing bootstrap-only candidates to promote

## Result
This ticket moves the system from:

- fixed split heuristic

to:

- data-selected promotable split policy
- explicit bootstrap lane
- auditable distinction between:
  - adaptation
  - certification
  - promotion eligibility
