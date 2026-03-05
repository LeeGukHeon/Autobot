# INTEGRATION_REPORT_2026-03-05_T18_1

## 0) Summary
- Ticket: `T18.1`
- Status: `ACTIONED`
- Goal: validate `v3 model score -> real backtest engine(order/fill/risk)` end-to-end
- Strict A/B (`top_n=50`, `top_pct=0.05`, `min_prob=0.58`, `min_cands=10`) remains `INCONCLUSIVE` (`orders_filled=0`).
- Added sparse-aware campaign C1/C3 (design spec): `orders_filled=182` for both OFF/ON, so policy comparison is now measurable.
- Added conservative campaign C2 (design spec): `orders_filled=42` (`filled_trades >= 30` PASS).

## 1) Implemented Wiring
- Added strategy path: `--strategy model_alpha_v1`
- Added model runtime modules:
  - `autobot/strategy/model_alpha_v1.py`
  - `autobot/backtest/strategy_adapter.py`
  - `autobot/models/predictor.py`
  - `autobot/models/dataset_loader.py` (`iter_feature_rows_grouped_by_ts`)
- Backtest engine integration:
  - ts-batch signal timing, next-bar touch fill contract
  - hold/risk exit mode
  - cooldown + max_positions_total
  - artifacts:
    - `summary.json`
    - `trades.csv`
    - `per_market.csv`
    - `selection_stats.json`
    - `debug_mismatch.json`

## 2) Campaign A/B (DoD)
- Command window: `2026-02-24 ~ 2026-03-04`
- Universe: `KRW top_n=50`, `tf=5m`
- Entry guard: `top_pct=0.05`, `min_prob=0.58`, `min_cands_per_ts=10`
- Exit mode: `hold_bars=6`

Run commands (design spec verbatim):
```bash
python -m autobot.cli backtest run --strategy model_alpha_v1 --model-ref latest_v3 --feature-set v3 \
  --tf 5m --quote KRW --top-n 50 --start 2026-02-24 --end 2026-03-04 \
  --entry top_pct --top-pct 0.05 --min-prob 0.58 --min-cands-per-ts 10 \
  --exit-mode hold --hold-bars 6 \
  --micro-order-policy off

python -m autobot.cli backtest run --strategy model_alpha_v1 --model-ref latest_v3 --feature-set v3 \
  --tf 5m --quote KRW --top-n 50 --start 2026-02-24 --end 2026-03-04 \
  --entry top_pct --top-pct 0.05 --min-prob 0.58 --min-cands-per-ts 10 \
  --exit-mode hold --hold-bars 6 \
  --micro-order-policy on --micro-order-policy-mode trade_only --micro-order-policy-on-missing abort
```

Note:
- For `model_alpha_v1 + feature_set=v3`, CLI now auto-resolves base candles from `features_v3` policy.
- So the exact commands above run successfully without extra `--dataset-name`.

### 2.1 Campaign A (Static, micro_order_policy=off)
- run_id: `backtest-20260305-072443-70189935b6`
- exit code: `0`
- summary:
  - `scored_rows=9,166`
  - `selected_rows=0`
  - `orders_filled=0`
  - `realized_pnl_quote=0`
  - `exposure_avg_open_positions=0.0`

### 2.2 Campaign B (Micro Adaptive, on-missing=abort)
- run_id: `backtest-20260305-073046-70189935b6`
- exit code: `0`
- summary:
  - `scored_rows=9,166`
  - `selected_rows=0`
  - `orders_filled=0`
  - `realized_pnl_quote=0`
  - `exposure_avg_open_positions=0.0`

### 2.3 DoD Result
- A/B both exit code 0: `PASS`
- mandatory artifacts generated: `PASS`
- `filled_trades >= 30`: `FAIL` (`0`)
- final: `INCONCLUSIVE`

## 3) Why T18(modelbt) and T18.1 differ
- T18 modelbt:
  - `selected_rows=2,156`
  - `trades_count=2,156`
  - `equity_end=19.6053`
  - `max_drawdown=27.5183`
- T18.1 strict A/B:
  - `selected_rows=0`, `orders_filled=0`

Reason:
- modelbt has no fill risk / timeout / cancel / replace / gate constraints.
- real backtest applies execution constraints and selection guard.
- with sparse per-ts candidates in current v3 effective rows, strict guard blocks entries.

## 4) Selection Rule Impact
- `forced min 1 pick` is removed (as required).
- strict guard effect:
  - `blocked_min_candidates_ts=1,969`
  - `dropped_min_prob_rows=1,131` (`12.34% of scored_rows`)
  - `dropped_top_pct_rows=1,223` (`13.34% of scored_rows`)
  - `skipped_missing_features_rows=115,184`
  - debug reasons: `MIN_CANDIDATES_NOT_MET`, `NO_FEATURE_ROWS_AT_TS`

Interpretation:
- `min_prob/min_cands` successfully prevented overtrading.
- but strict setup produced zero fills in this window.

## 5) Campaign C (Sparse-aware, no code change)
- Command window: `2026-02-24 ~ 2026-03-04`
- Universe: `KRW top_n=20`, `tf=5m`
- Entry guard: `top_pct=0.20`, `min_prob=0.55`, `min_cands_per_ts=3`
- Exit mode: `hold_bars=6`

Run C1 (OFF):
```bash
python -m autobot.cli backtest run --strategy model_alpha_v1 --model-ref latest_v3 --feature-set v3 \
  --tf 5m --quote KRW --top-n 20 --start 2026-02-24 --end 2026-03-04 \
  --entry top_pct --top-pct 0.20 --min-prob 0.55 --min-cands-per-ts 3 \
  --exit-mode hold --hold-bars 6 \
  --micro-order-policy off
```

Run C3 (ON, trade_only/static_fallback):
```bash
python -m autobot.cli backtest run --strategy model_alpha_v1 --model-ref latest_v3 --feature-set v3 \
  --tf 5m --quote KRW --top-n 20 --start 2026-02-24 --end 2026-03-04 \
  --entry top_pct --top-pct 0.20 --min-prob 0.55 --min-cands-per-ts 3 \
  --exit-mode hold --hold-bars 6 \
  --micro-order-policy on --micro-order-policy-mode trade_only --micro-order-policy-on-missing static_fallback
```

Run C2 (more conservative, OFF):
```bash
python -m autobot.cli backtest run --strategy model_alpha_v1 --model-ref latest_v3 --feature-set v3 \
  --tf 5m --quote KRW --top-n 20 --start 2026-02-24 --end 2026-03-04 \
  --entry top_pct --top-pct 0.10 --min-prob 0.52 --min-cands-per-ts 3 \
  --exit-mode hold --hold-bars 6 \
  --micro-order-policy off
```

### 5.1 Campaign C1 (OFF)
- run_id: `backtest-20260305-085120-e00950d291`
- exit code: `0`
- summary:
  - `scored_rows=7,153`
  - `selected_rows=247`
  - `orders_filled=182`
  - `fill_rate=0.9333`
  - `avg_time_to_fill_ms=342,857`
  - `slippage_bps_mean=3.0835`
  - `realized_pnl_quote=4,309.3018`
  - `win_rate=0.6923`

### 5.2 Campaign C3 (ON)
- run_id: `backtest-20260305-085406-e00950d291`
- exit code: `0`
- summary:
  - `scored_rows=7,153`
  - `selected_rows=247`
  - `orders_filled=182`
  - `fill_rate=0.9785`
  - `avg_time_to_fill_ms=303,296`
  - `slippage_bps_mean=18.3466`
  - `realized_pnl_quote=1,344.6986`
  - `win_rate=0.5165`
  - `fallback_reasons={"MICRO_MISSING_FALLBACK": 20}`

### 5.3 C1/C3 Comparison
- `filled_trades >= 30`: `PASS` (both `182`)
- ON effect is measurable:
  - Time-to-fill improved (`342,857 -> 303,296 ms`)
  - Fill rate improved (`93.33% -> 97.85%`)
  - Slippage worsened (`3.08 -> 18.35 bps`)
  - Realized PnL decreased (`4,309.30 -> 1,344.70`)
  - Win rate decreased (`69.23% -> 51.65%`)
- Interpretation:
  - strict A/B failure was a density mismatch, not wiring failure.
  - with sparse-aware gate, real engine policy comparison is now possible.

### 5.4 Campaign C2 (Conservative OFF)
- run_id: `backtest-20260305-085957-e00950d291`
- exit code: `0`
- summary:
  - `scored_rows=7,153`
  - `selected_rows=40`
  - `orders_filled=42`
  - `fill_rate=0.9767`
  - `avg_time_to_fill_ms=321,428`
  - `slippage_bps_mean=0.6337`
  - `realized_pnl_quote=2,241.2110`
  - `win_rate=0.8571`
- interpretation:
  - `filled_trades >= 30`: `PASS` (`42`)
  - compared to C1, C2 is more selective (`182 -> 42 fills`) with lower slippage and higher win rate.
  - C2 can be used as a conservative baseline before enabling micro adaptive policy.

## 6) Artifacts
- Campaign A:
  - `data/backtest/runs/backtest-20260305-072443-70189935b6/summary.json`
  - `data/backtest/runs/backtest-20260305-072443-70189935b6/trades.csv`
  - `data/backtest/runs/backtest-20260305-072443-70189935b6/per_market.csv`
  - `data/backtest/runs/backtest-20260305-072443-70189935b6/selection_stats.json`
  - `data/backtest/runs/backtest-20260305-072443-70189935b6/debug_mismatch.json`
- Campaign B:
  - `data/backtest/runs/backtest-20260305-073046-70189935b6/summary.json`
  - `data/backtest/runs/backtest-20260305-073046-70189935b6/trades.csv`
  - `data/backtest/runs/backtest-20260305-073046-70189935b6/per_market.csv`
  - `data/backtest/runs/backtest-20260305-073046-70189935b6/selection_stats.json`
  - `data/backtest/runs/backtest-20260305-073046-70189935b6/debug_mismatch.json`
- Campaign C1 (OFF):
  - `data/backtest/runs/backtest-20260305-085120-e00950d291/summary.json`
  - `data/backtest/runs/backtest-20260305-085120-e00950d291/trades.csv`
  - `data/backtest/runs/backtest-20260305-085120-e00950d291/per_market.csv`
  - `data/backtest/runs/backtest-20260305-085120-e00950d291/selection_stats.json`
  - `data/backtest/runs/backtest-20260305-085120-e00950d291/debug_mismatch.json`
  - `data/backtest/runs/backtest-20260305-085120-e00950d291/micro_order_policy_report.json`
- Campaign C3 (ON):
  - `data/backtest/runs/backtest-20260305-085406-e00950d291/summary.json`
  - `data/backtest/runs/backtest-20260305-085406-e00950d291/trades.csv`
  - `data/backtest/runs/backtest-20260305-085406-e00950d291/per_market.csv`
  - `data/backtest/runs/backtest-20260305-085406-e00950d291/selection_stats.json`
  - `data/backtest/runs/backtest-20260305-085406-e00950d291/debug_mismatch.json`
  - `data/backtest/runs/backtest-20260305-085406-e00950d291/micro_order_policy_report.json`
- Campaign C2 (OFF, conservative):
  - `data/backtest/runs/backtest-20260305-085957-e00950d291/summary.json`
  - `data/backtest/runs/backtest-20260305-085957-e00950d291/trades.csv`
  - `data/backtest/runs/backtest-20260305-085957-e00950d291/per_market.csv`
  - `data/backtest/runs/backtest-20260305-085957-e00950d291/selection_stats.json`
  - `data/backtest/runs/backtest-20260305-085957-e00950d291/debug_mismatch.json`
  - `data/backtest/runs/backtest-20260305-085957-e00950d291/micro_order_policy_report.json`

## 7) Regression
- No code changes in this step; pytest was not re-run.
