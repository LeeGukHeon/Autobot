# T23_9 Covariate Similarity Weighting v1

## Goal

offline execution risk-control contract를

- time-recency weighting only

에서

- time-recency × covariate similarity weighting

으로 확장한다.

## Method

### Reference regime

reference regime = latest walk-forward window.

reference state is built from the median of:

- `selection_score`
- `rv_12`
- `rv_36`
- `atr_pct_14`

on the latest window rows.

### Distance

each row gets a standardized squared distance to the reference state:

- `(x_j - center_j) / scale_j`

where `scale_j` is the global dispersion of that feature
with a minimum floor.

### Similarity weight

`w_similarity = exp(-0.5 * mean_sq_distance / bandwidth^2)`

total weight becomes:

- `w_total = w_recency * w_similarity`

## Why

recency alone assumes “latest data is relevant because it is recent”.

covariate similarity adds:

- “latest-like state is more relevant than old-state-looking samples”

without requiring an explicit density-ratio model.

## Contract fields

`risk_control.weighting.covariate_similarity`

- `enabled`
- `mode = latest_window_gaussian_state_similarity_v1`
- `bandwidth`
- `reference_window_index`
- `feature_names`
- `center`
- `scale`

## Limitation

이번 v1은 explicit density-ratio estimation이 아니다.
즉

- no classifier-based ratio estimation
- no kernel mean matching
- no importance-weight clipping policy

다.

그래도 latest regime 중심의
state-conditioned reweighting이라는 점에서
recency-only보다 한 단계 더 regime-aware하다.
