# T23_3 Non-Heuristic Execution Risk Control v1

## Objective

`model_prob -> direct live execution` 연결을 끊고, 학습 산출물에서 유도된
통계적 execution risk-control contract를 live runtime의 마지막 abstention gate로
사용한다.

이번 v1의 목표는 다음이다.

- 휴리스틱 symbol blocklist 없이 실행 중단 기준을 명시적 계약으로 남긴다.
- 후보 모델의 OOS replay rows만으로 threshold를 산출한다.
- live runtime은 해당 contract를 읽어 `submit` 직전에 abstain 할 수 있어야 한다.
- decision surface / runtime artifact / live intent meta에 같은 contract 흔적이 남아야 한다.

## Method

### 1. Offline trade replay source

입력 데이터는 walk-forward test windows에서 생성된 `_trade_action_oos_rows`다.

각 row는 다음을 가진다.

- calibrated `selection_score`
- state features: `rv_12`, `rv_36`, `atr_pct_14`
- simulated `hold_return`
- simulated `risk_return`

trade-action policy는 이 rows를 바탕으로 `recommended_action`과
`expected_action_value`를 추정한다.

### 2. Risk contract

v1 contract는 `expected_action_value >= lambda` 임계값을 선택한다.

threshold 선택은 다음 제약 아래에서 수행한다.

- `nonpositive_rate_ucb(lambda) <= alpha_nonpositive`
- `severe_loss_rate_ucb(lambda) <= alpha_severe`
- `coverage(lambda) >= min_coverage`

여기서 UCB는 one-sided Hoeffding upper bound다.

- `p_ucb = p_hat + sqrt(log(1/delta)/(2n))`

이는 finite-sample, distribution-free upper confidence control을 제공한다.

### 3. Selection target

feasible threshold 집합 중에서 empirical mean return이 최대인 threshold를 채택한다.

동률이면 다음 우선순위를 사용한다.

- higher coverage
- lower nonpositive UCB
- lower severe-loss UCB
- lower threshold

### 4. Live enforcement

live runtime은 `trade_action.expected_action_value`를 읽고,
해당 값이 learned threshold보다 낮으면 order emission 전에 abstain 한다.

이 abstention은 다음과 분리된다.

- admissibility gate
- canary rollout gate
- breaker gate
- small-account gate

즉 v1은 `statistical execution abstention layer`다.

## Artifacts

`runtime_recommendations.json` 안에 `risk_control` payload를 추가한다.

주요 필드:

- `policy = execution_risk_control_hoeffding_v1`
- `decision_metric_name = expected_action_value`
- `selected_threshold`
- `selected_coverage`
- `selected_nonpositive_rate_ucb`
- `selected_severe_loss_rate_ucb`
- `live_gate.enabled`
- `live_gate.metric_name`
- `live_gate.threshold`
- `live_gate.skip_reason_code`

## Why v1 is intentionally limited

이번 변경은 다음을 아직 포함하지 않는다.

- subgroup multivalid / group-conditional risk control
- non-exchangeable weighting
- online adaptive conformal update
- martingale drift detection
- size ladder admissibility

이유는 v1의 목적이
`train artifact -> runtime contract -> live abstain`
경로를 먼저 닫는 것이기 때문이다.

## Planned next phases

### v2

- pre-registered subgroup family `G` 도입
- subgroup별 UCB diagnostics
- candidate acceptance에 subgroup breach contract 추가

### v3

- non-exchangeable weighting
- online adaptive threshold buffer
- drift martingale halt / retrain trigger

### v4

- size ladder admissibility
- promotion hypothesis test와 risk contract 연동
