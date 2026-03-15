# T23_11 Classifier Density Ratio Weighting v1

## Goal

`latest_window_diagonal_gaussian_density_ratio_v1`를
`classifier-based density ratio`로 대체한다.

## Method

positive class:

- latest walk-forward window rows

negative class:

- all historical walk-forward window rows

features:

- `selection_score`
- `rv_12`
- `rv_36`
- `atr_pct_14`

model:

- lightweight logistic classifier
- standardized inputs
- gradient descent fit

## Ratio

classifier output is `p(latest | x)`.

density ratio estimate:

- `ratio(x) = p / (1 - p) * (n_negative / n_positive)`

then clipped to:

- `[clip_min, clip_max]`

## Contract

`risk_control.weighting.density_ratio`

- `mode = latest_window_logistic_density_ratio_v1`
- `classifier_status`
- `positive_rows`
- `negative_rows`
- `positive_probability_mean`
- `negative_probability_mean`
- `clip_fraction`
- `model.coef`
- `model.intercept`
- `model.x_mean`
- `model.x_scale`
- `model.prior_ratio`

## Why

diagonal Gaussian ratio is a useful first approximation,
but it assumes axis-aligned Gaussian class conditionals.

the logistic ratio layer gives:

- direct latest-vs-history discrimination
- prior-aware density-ratio estimate
- more interpretable clipping diagnostics

while still staying dependency-light.

## Limitation

이번 v1은

- single fit
- no cross-fitting
- no out-of-fold calibration for the ratio itself

이다.

즉 완전한 modern importance weighting이라기보다
lightweight classifier ratio layer다.
