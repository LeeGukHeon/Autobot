# T23_7 Stateful Online Hysteresis And Halt v1

## Goal

online execution risk control을

- stateless threshold step-up

에서

- stateful hysteresis / recovery
- breach-streak halt

로 확장한다.

## State machine

checkpoint: `execution_risk_control_online_buffer`

state fields:

- `step_up`
- `breach_streak`
- `recovery_streak`
- `halt_triggered`

inputs:

- recent nonpositive UCB
- recent severe-loss UCB
- base threshold
- threshold ladder

## Transition

### breach_count > 0

- `step_up = max(previous_step_up, breach_count)`
- `breach_streak += 1`
- `recovery_streak = 0`

### breach_count == 0

- keep current `step_up`
- `recovery_streak += 1`
- once `recovery_streak >= recovery_streak_required`
  - `step_up -= 1`
  - `recovery_streak = 0`

## Halt

if

- `breach_streak >= halt_breach_streak`

then

- `halt_triggered = true`
- new intents are skipped with `halt_reason_code`

이번 v1은 breaker state를 직접 영구적으로 arm 하지는 않는다.
대신 risk-control state checkpoint만으로
subsequent intents를 계속 skip 시키는 soft halt다.

## Why

stateless online step-up은

- 한 번 breach가 나도 다음 샘플 하나만 좋아지면 바로 내려갈 수 있다.

이건 실제 운영에서 너무 민감하다.

stateful hysteresis를 넣으면

- 나빠질 때는 빠르게 보수화
- 좋아질 때는 천천히 완화

라는 asymmetric control이 가능하다.

## Deliberate limitation

이번 v1은

- checkpoint state machine
- recent closed verified trades
- soft halt

까지만 한다.

아직 포함하지 않는 것:

- martingale-based drift evidence
- breaker-level hard halt integration
- covariate-shift detector based halt
- adaptive recovery based on utility confidence, not just streak
