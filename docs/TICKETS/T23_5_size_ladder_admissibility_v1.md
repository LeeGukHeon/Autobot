# T23_5 Size Ladder Admissibility v1

## Goal

`trade_action.recommended_notional_multiplier`를 연속값 그대로 live에 쓰지 않고,
학습 산출물에서 계산된 admissible size ladder 안에서만 허용한다.

## Why

이전 단계들로

- entry yes/no threshold
- subgroup-aware abstention threshold

까지는 들어갔다.

하지만 실제 사고에서는

- 방향 판단이 틀린 것
- 틀린 신호에 size까지 크게 실린 것

을 분리해야 한다.

이번 단계는 두 번째 문제를 다룬다.

## Contract

`runtime_recommendations.json -> risk_control -> size_ladder`

주요 필드:

- `status`
- `method = finite_size_ladder_tail_ucb_v1`
- `ladder_multipliers`
- `global_max_multiplier`
- `group_limits`
- `skip_reason_code = SIZE_LADDER_NO_ADMISSIBLE_MULTIPLIER`

각 group limit는:

- `bucket_index`
- `label`
- `coverage`
- `max_multiplier`
- `status`
- `ladder_results`

## Method

입력은 threshold-selected OOS rows다.

즉 global/subgroup execution threshold를 통과한 rows만 사용한다.

그 다음 사전등록 ladder `M`에 대해,
각 multiplier `m in M`에 대해 다음을 계산한다.

- scaled return = `m * realized_return`
- severe-loss rate = `1{scaled_return <= -tau}`
- severe-loss UCB via Hoeffding

feasible multiplier는

- severe-loss UCB <= `alpha_severe`

를 만족하는 multiplier다.

선택 규칙은 단순하다.

- global max = feasible한 최대 multiplier
- subgroup max = 각 subgroup에서 feasible한 최대 multiplier

live runtime은

- `resolved_multiplier = min(requested_multiplier, subgroup_max or global_max)`

로 clamp 한다.

## Live behavior

live runtime은 submit 전에

- `size_ladder.requested_multiplier`
- `size_ladder.resolved_multiplier`
- `size_ladder.subgroup_bucket`

을 meta에 남긴다.

admissible multiplier가 `<= 0`이면 submit하지 않고
`SIZE_LADDER_NO_ADMISSIBLE_MULTIPLIER`로 skip 한다.

## Deliberate limitation

이번 v1은 size ladder를 severe-loss control만으로 정한다.

즉 아직은

- downside expectation constraint
- CTM / ES joint constraint
- online adaptive size buffer

는 넣지 않았다.

다음 단계에서 추가할 수 있다.
