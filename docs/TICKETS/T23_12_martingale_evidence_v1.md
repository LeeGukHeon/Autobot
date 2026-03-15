# T23_12 Martingale Evidence v1

## Goal

online risk halt 근거를

- loss-rate streak

에만 두지 않고,

- anytime-valid sequential evidence

까지 확장한다.

## Method

이번 v1은 Bernoulli indicator용 betting e-process를 사용한다.

tests:

- `nonpositive` indicator
- `severe loss` indicator

for each new closed verified trade:

- `X_nonpositive = 1{pnl_pct <= 0}`
- `X_severe = 1{pnl_pct <= -tau}`

update:

- `E_t = E_{t-1} * (1 + lambda * (X_t - alpha))`

with

- nonnegative factor constraint
- configurable `lambda`

## State

checkpoint payload now carries:

- `martingale_nonpositive_e_value`
- `martingale_severe_e_value`
- `martingale_max_e_value`
- `martingale_last_processed_exit_ts_ms`
- `martingale_halt_triggered`
- `martingale_clear_halt`

## Halt semantics

if `martingale_max_e_value >= martingale_halt_threshold`

- martingale halt triggers
- skip reason becomes `RISK_CONTROL_MARTINGALE_EVIDENCE`
- live breaker is armed with the same reason code

if previously halted and

- `martingale_max_e_value <= martingale_clear_threshold`

then martingale halt is cleared.

## Relationship to streak halt

current online halt merges:

- streak-based halt
- martingale-based halt

either can trigger a stop.

martingale halt has priority in the final `halt_reason_code`.

## Limitation

이번 v1은

- simple Bernoulli betting process
- fixed bet fraction
- two binary indicators only

이다.

아직 포함하지 않는 것:

- mixture e-process
- adaptive betting fraction
- multi-hypothesis e-value correction
- subgroup-specific martingale evidence
