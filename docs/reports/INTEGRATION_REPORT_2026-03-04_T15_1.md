# INTEGRATION_REPORT_2026-03-04_T15_1

## 0) Summary
- Ticket: T15.1
- Scope: MicroAdaptive Order Policy v1 (timeout/replace/aggressiveness)
- Verdict: INCONCLUSIVE
- Evidence:
  - ON 총 filled_trades(orders_filled) = `5` (`backtest=3`, `paper=2`)로 판정 기준 `30` 미만
  - ON에서 `replaces_total=0` AND `cancels_total=0` (정책 발동 기회 없음)
  - 정책 자체는 적용됨: ON tier `MID` only, on_missing fallback `MICRO_MISSING_FALLBACK` 총 `5`

## 1) Runs
### Backtest OFF
- run_id: `backtest-20260304-112608-4c4c153dda`
- command:
  - `python -m autobot.cli backtest run --tf 5m --quote KRW --top-n 20 --duration-days 8 --micro-gate off --micro-order-policy off`
- key outputs:
  - `data/backtest/runs/backtest-20260304-112608-4c4c153dda/summary.json`
  - `data/backtest/runs/backtest-20260304-112608-4c4c153dda/micro_order_policy_report.json`

### Backtest ON
- run_id: `backtest-20260304-112635-4c4c153dda`
- command:
  - `python -m autobot.cli backtest run --tf 5m --quote KRW --top-n 20 --duration-days 8 --micro-gate off --micro-order-policy on --micro-order-policy-mode trade_only --micro-order-policy-on-missing static_fallback`
- key outputs:
  - `data/backtest/runs/backtest-20260304-112635-4c4c153dda/summary.json`
  - `data/backtest/runs/backtest-20260304-112635-4c4c153dda/micro_order_policy_report.json`

### Paper OFF
- run_id: `paper-20260304-112702`
- command:
  - `python -m autobot.cli paper run --duration-sec 7200 --quote KRW --top-n 20 --micro-gate off --micro-order-policy off`
- key outputs:
  - `data/paper/runs/paper-20260304-112702/summary.json`
  - `data/paper/runs/paper-20260304-112702/micro_order_policy_report.json`

### Paper ON
- run_id: `paper-20260304-132705`
- command:
  - `python -m autobot.cli paper run --duration-sec 7200 --quote KRW --top-n 20 --micro-gate off --micro-order-policy on --micro-order-policy-mode trade_only --micro-order-policy-on-missing static_fallback`
- key outputs:
  - `data/paper/runs/paper-20260304-132705/summary.json`
  - `data/paper/runs/paper-20260304-132705/micro_order_policy_report.json`

## 2) OFF vs ON Comparison
### 2.1 Backtest (`8d`, `top_n=20`, `5m`)
| Metric | OFF | ON |
|---|---:|---:|
| filled_trades (`orders_filled`) | 3 | 3 |
| candidates_total | 2854 | 2854 |
| orders_submitted | 3 | 3 |
| fill_ratio | 1.0000 | 1.0000 |
| avg_time_to_fill_ms | 302116.0 | 302116.0 |
| p50_time_to_fill_ms | 300517.0 | 300517.0 |
| p90_time_to_fill_ms | 305385.8 | 305385.8 |
| replaces_total | 0 | 0 |
| cancels_total | 0 | 0 |
| aborted_timeout_total | 0 | 0 |
| chase_limit_abort_total | 0 *(not observed)* | 0 *(not observed)* |
| dust_abort_total | 0 | 0 |
| slippage_bps_mean | 0.0 | 0.0 |
| slippage_bps_p50 | 0.0 | 0.0 |
| slippage_bps_p90 | 0.0 | 0.0 |
| realized_pnl_quote | 0.0 | 0.0 |
| unrealized_pnl_quote | 2680.5258 | 2680.5258 |
| EV proxy | N/A | N/A |

### 2.2 Paper (`7200s`, `top_n=20`)
| Metric | OFF | ON |
|---|---:|---:|
| filled_trades (`orders_filled`) | 4 | 2 |
| candidates_total | 21885 | 19629 |
| orders_submitted | 4 | 2 |
| fill_ratio | 1.0000 | 1.0000 |
| avg_time_to_fill_ms | 8511.75 | 0.0 |
| p50_time_to_fill_ms | 7735.5 | 0.0 |
| p90_time_to_fill_ms | 17644.5 | 0.0 |
| replaces_total | 0 | 0 |
| cancels_total | 0 | 0 |
| aborted_timeout_total | 0 | 0 |
| chase_limit_abort_total | 0 *(not observed)* | 0 *(not observed)* |
| dust_abort_total | 0 | 0 |
| slippage_bps_mean | 0.0 | 0.0 |
| slippage_bps_p50 | 0.0 | 0.0 |
| slippage_bps_p90 | 0.0 | 0.0 |
| realized_pnl_quote | 0.0 | 0.0 |
| unrealized_pnl_quote | -484.3839 | -88.7663 |
| EV proxy | N/A | N/A |

## 3) Policy Diagnostics
- tier distribution (ON):
  - backtest: `MID=3 (100%)`
  - paper: `MID=2 (100%)`
  - aggregate: `LOW=0, MID=5, HIGH=0`
- price_mode counts (ON, ORDER_SUBMITTED 기준):
  - `PASSIVE_MAKER=0`
  - `JOIN=5`
  - `CROSS_1T=0`
- replaces/cancels reasons topN (ON):
  - `replace_reasons={}` (none)
  - `replaces_total=0`, `cancels_total=0`
- on_missing fallback count (ON):
  - `MICRO_MISSING_FALLBACK=5` (`backtest=3`, `paper=2`)

## 4) Interpretation
- 왜 보류(INCONCLUSIVE)인지:
  - 판정 규칙상 `filled_trades < 30`이므로 보류
  - 동시에 `replaces_total==0 AND cancels_total==0`으로 정책이 실질 발동되지 않음
- 표본( filled_trades ) 충분성:
  - ON 기준 `backtest=3`, `paper=2`, 총 `5`로 부족
- 정책 발동 여부:
  - 정책 선택 자체는 동작(ON tier/fallback 기록 존재)
  - 그러나 실행 단계에서 replace/cancel 트리거가 발생하지 않아 timeout/replace/aggressiveness 효과를 검증할 표본 부재

## 5) Next Ticket (one)
- 선택: `T13.1c-ops`
- 선택 근거:
  - INCONCLUSIVE + replace/cancel 미발동 패턴이므로 우선 운영 표본(체결/미체결/대기시간) 확장을 통해 정책 발동 기회를 늘리는 것이 우선
