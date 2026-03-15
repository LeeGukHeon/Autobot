# T23_10 Density Ratio Weighting v1

## Goal

offline execution risk-control weighting을

- recency
- covariate similarity

위에 더해

- classifier-style density ratio approximation

까지 확장한다.

## Method

이번 v1은 외부 classifier dependency 없이
`latest window` vs `history windows`를 구분하는
diagonal Gaussian density ratio를 사용한다.

### Positive class

- latest walk-forward window rows

### Negative class

- all previous walk-forward window rows

### Features

- `selection_score`
- `rv_12`
- `rv_36`
- `atr_pct_14`

### Ratio

각 feature에 대해 positive / negative class의

- mean
- std

를 계산하고,
diagonal Gaussian log-density ratio를 합산한다.

weight는

- `ratio = exp(log_ratio)`

로 계산한 뒤,
importance-weight clipping을 적용한다.

## Final weight

`w_total = w_recency * w_similarity * w_density_ratio`

즉 이번 단계부터 risk-control contract는
세 가지 weighting layer를 가진다.

## Clipping

v1 clipping은 고정 구간이다.

- `clip_min`
- `clip_max`

기본 목적은 unstable tail amplification을 막는 것이다.

## Contract

`risk_control.weighting.density_ratio`

- `enabled`
- `mode = latest_window_diagonal_gaussian_density_ratio_v1`
- `reference_window_index`
- `feature_names`
- `positive_center`
- `positive_scale`
- `negative_center`
- `negative_scale`
- `clip_min`
- `clip_max`

## Why

covariate similarity는 “latest-like” state에 더 큰 weight를 주지만,
명시적인 positive-vs-history reweighting은 아니다.

density ratio weighting은

- latest regime가 old regime와 얼마나 다른지
- current-like rows가 historical support에서 얼마나 희귀한지

를 더 직접적으로 반영한다.

## Limitation

이번 v1은 strict probabilistic density-ratio estimator가 아니다.

즉 아직:

- logistic density-ratio classifier
- calibration of ratio estimates
- cross-fit ratio estimation
- clipped importance-weight diagnostics

까지는 포함하지 않는다.

그래도 recency/similarity보다 한 단계 더
regime-shift aware한 offline weighting이라는 점이 핵심이다.
