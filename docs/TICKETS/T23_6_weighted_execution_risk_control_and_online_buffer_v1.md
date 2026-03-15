# T23_6 Weighted Execution Risk Control And Online Buffer v1

## Goal

static `execution_risk_control` contract를

- OOS recency weighting
- live recent closed-trade threshold step-up

까지 확장한다.

## Offline weighting

### Policy

- mode: `window_recency_exponential_v1`
- weight for row `i`: `exp(-ln(2) * lag / half_life_windows)`
- lag = `max_window_index - window_index`

### Why

walk-forward windows는 시간순으로 생성되므로,
더 최근 window를 더 크게 반영하는 것은
non-exchangeable adaptation의 1차 근사다.

이번 v1은 density-ratio 추정까지는 가지 않고,
`window recency`만 반영한다.

### Effective sample size

weighted risk/UCB는

- weighted mean / weighted rate
- `n_eff = (sum w)^2 / sum(w^2)`

를 사용한다.

즉 contract는 이제 raw coverage뿐 아니라
`selected_effective_sample_size`도 남긴다.

## Online adaptive buffer

### Policy

- mode: `recent_closed_trade_hoeffding_stepup_v1`
- recent sample = same `live_runtime_model_run_id` 의 recent closed verified trades
- statistics:
  - recent nonpositive rate
  - recent severe-loss rate
  - one-sided Hoeffding UCB

### Action

breach count = `1{nonpositive_ucb > alpha_nonpositive} + 1{severe_ucb > alpha_severe}`

adaptive threshold는

- base threshold에서
- breach count 만큼
- 더 strict한 threshold result로 step-up

된다.

이번 v1은 hysteresis나 stateful recovery 없이
recent trade window만 보고 stateless하게 계산한다.

## Live runtime behavior

live intent meta에는 다음이 추가된다.

- `risk_control_online`
- `size_ladder`

`risk_control_online` 안에는

- base threshold
- adaptive threshold
- recent trade count
- recent nonpositive/severe UCB
- breach count
- step_up

이 기록된다.

## Deliberate limitation

이번 단계는 아직

- density-ratio weighting
- covariate similarity weighting
- online adaptive recovery / hysteresis
- martingale halt

을 포함하지 않는다.

즉 v1은

- offline: recency-weighted
- live: recent-loss step-up

까지만 구현한다.
