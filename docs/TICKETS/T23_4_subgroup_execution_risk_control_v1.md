# T23_4 Subgroup Execution Risk Control v1

## Goal

`execution_risk_control_hoeffding_v1`를 전체 평균 risk 통제에서
사전등록 subgroup family까지 동시에 만족하는 threshold selection으로 확장한다.

## Scope

이번 v1 subgroup extension은 다음만 포함한다.

- subgroup family = `trade_action.risk_feature_name` quantile buckets
- threshold feasibility = overall constraint AND subgroup constraint
- live runtime = subgroup diagnostics 기록

이번 단계에 포함하지 않는 것:

- online subgroup recalibration
- multivalid exact guarantees
- density-ratio weighting
- martingale subgroup drift alarm

## Contract

`runtime_recommendations.json -> risk_control`

추가 필드:

- `subgroup_family.feature_name`
- `subgroup_family.bucket_count_requested`
- `subgroup_family.bucket_count_effective`
- `subgroup_family.bounds`
- `subgroup_family.min_coverage`
- `selected_subgroup_results`

각 subgroup result는:

- `bucket_index`
- `label`
- `coverage`
- `nonpositive_rate_ucb`
- `severe_loss_rate_ucb`
- `status`

## Rule

threshold `lambda`는 아래를 만족해야 feasible 하다.

- overall coverage >= `min_coverage`
- overall nonpositive UCB <= `alpha_nonpositive`
- overall severe-loss UCB <= `alpha_severe`
- subgroup coverage >= `subgroup_min_coverage` 인 모든 subgroup에 대해
  - subgroup nonpositive UCB <= `alpha_nonpositive`
  - subgroup severe-loss UCB <= `alpha_severe`

coverage가 작은 subgroup은 `insufficient_coverage`로 기록하되,
현재 v1에서는 threshold infeasible 사유로 쓰지 않는다.

## Live Runtime

live runtime은 여전히 global threshold를 사용해 abstain 한다.
다만 decision payload에 다음 subgroup diagnostics를 남긴다.

- `subgroup_feature_name`
- `subgroup_value`
- `subgroup_bucket`
- `subgroup_label`

즉 이번 단계는

- threshold selection: subgroup-aware
- live enforcement: threshold-based with subgroup trace

구조다.

## Why this is useful

이번 사고처럼

- 후보 고유 과대확신 종목군
- 모델 패밀리 공통 blind spot

이 섞여 있을 때, 단일 평균 risk control은 너무 둔하다.
subgroup-aware threshold를 쓰면 특정 volatility bucket에서 깨지는 threshold를
전체 평균이 괜찮아도 자동으로 탈락시킬 수 있다.
