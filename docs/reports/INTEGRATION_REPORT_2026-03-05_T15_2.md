# INTEGRATION REPORT - T15.2 TickBps Guard + Timeout Escalation

작성일: 2026-03-05  
티켓: T15.2

## 1) 구현 요약
- `CROSS_1T` 기본 진입 제거: 초기 주문(`replace_attempt=0`)은 강제로 `JOIN`.
- Timeout 승급 도입: `replace_attempt >= 2`에서만 `CROSS_1T` 후보.
- TickBps Gate 도입: `tick_bps <= cross_tick_bps_max`일 때만 `CROSS_1T` 허용.
- 선택 가드:
  - `cross_min_prob` (기본 `null`)
  - `cross_micro_stale_ms` (기본 `null`)
  - `abort_if_tick_bps_gt` (기본 `null`, 초과 시 주문 abort)
- TickSizeResolver 도입: `upbit_rules | krw_table | auto`.
- backtest/paper 공통으로 timeout/replace 경로에 단계 승급 연결.
- 산출물 확장:
  - `slippage_by_market.csv`
  - `price_mode_by_market.csv`
  - `micro_order_policy_report.json` 확장(`tick_bps_stats`, `cross_block_reasons`, `cross_allowed_count`, `cross_used_count`)

## 2) 변경 파일
- `autobot/strategy/micro_order_policy.py`
- `autobot/backtest/engine.py`
- `autobot/paper/engine.py`
- `autobot/cli.py`
- `config/strategy.yaml`
- `docs/CONFIG_SCHEMA.md`
- `autobot/backtest/__init__.py` (circular import 방지용 lazy export)
- 테스트:
  - `tests/test_micro_order_policy_v1.py`
  - `tests/test_backtest_engine_micro_order_policy_integration.py`
  - `tests/test_paper_engine_micro_order_policy_integration.py`

## 3) 실행 커맨드
```powershell
python -m autobot.cli backtest run --strategy model_alpha_v1 --model-ref latest_v3 --feature-set v3 `
  --tf 5m --quote KRW --top-n 20 --start 2026-02-24 --end 2026-03-04 `
  --entry top_pct --top-pct 0.20 --min-prob 0.55 --min-cands-per-ts 3 `
  --exit-mode hold --hold-bars 6 `
  --micro-order-policy on --micro-order-policy-mode trade_only --micro-order-policy-on-missing static_fallback
```

## 4) Campaign D1 결과
- run_id: `backtest-20260305-111709-e00950d291`
- run_dir: `data/backtest/runs/backtest-20260305-111709-e00950d291`
- 핵심 지표:
  - `orders_filled=176`
  - `fill_rate=0.9215`
  - `avg_time_to_fill_ms=306,818`
  - `slippage_bps_mean=0.1547`
  - `slippage_bps_p90=0.0`
  - `realized_pnl_quote=4,124.4106`
  - `win_rate=0.7159`
- 정책 리포트:
  - `tick_bps_stats.max=76.3359`
  - `cross_allowed_count=0`
  - `cross_used_count=0`
  - `cross_block_reasons={}`
  - `resolver_failed_fallback_used_count=0`
  - `fallback_reasons={"MICRO_MISSING_FALLBACK": 20}`

## 5) C1/C3 대비 비교
- 기준선:
  - C1: `backtest-20260305-085120-e00950d291`
  - C3: `backtest-20260305-085406-e00950d291`
- D1 vs C3:
  - `slippage_bps_p90: 72.36 -> 0.00` (100% 감소)
  - `slippage_bps_mean: 18.35 -> 0.15` (강한 개선)
- D1 vs C1:
  - `realized_pnl_quote: 4309.30 -> 4124.41` (C1 대비 95.71%)
  - `fill_rate: 0.9333 -> 0.9215` (약 -1.19%p)

## 6) DoD 판정
- `orders_filled >= 30`: PASS (`176`)
- 산출물 3종 생성: PASS
  - `slippage_by_market.csv`
  - `price_mode_by_market.csv`
  - 확장된 `micro_order_policy_report.json`
- `(vs C3) slippage_bps_p90 50% 이상 감소`: PASS (100% 감소)
- `(vs C1) realized_pnl_quote >= 80%`: PASS (95.71%)
- `fill_rate 하락 <= 5%p (vs C1)`: PASS (-1.19%p)

## 7) 산출물 경로
- 요약: `data/backtest/runs/backtest-20260305-111709-e00950d291/summary.json`
- 정책 리포트: `data/backtest/runs/backtest-20260305-111709-e00950d291/micro_order_policy_report.json`
- 시장별 슬리피지: `data/backtest/runs/backtest-20260305-111709-e00950d291/slippage_by_market.csv`
- 시장별 price_mode: `data/backtest/runs/backtest-20260305-111709-e00950d291/price_mode_by_market.csv`

## 8) 추가 검증 (Top50 / 30일)
- Top50 run
  - run_id: `backtest-20260305-113305-70189935b6`
  - `slippage_bps_mean=0.1833`, `slippage_bps_p90=0.0`
  - `cross_used_count=0` (`price_mode_by_market` 전 시장 `CROSS_1T_count=0`)
- 30일 run
  - run_id: `backtest-20260305-113944-38cdddb4e9`
  - `slippage_bps_mean=0.1191`, `slippage_bps_p90=0.0`
  - `cross_used_count=0` (`price_mode_by_market` 전 시장 `CROSS_1T_count=0`)
- 판정: tick bps 폭탄 재발 없음

## 9) 기본값 고정
- `config/strategy.yaml`에서 `strategy.micro_order_policy.enabled=true`로 기본 ON 고정

## 10) A/C Validation Addendum (2026-03-05)
- Scope: validate `A(universe quality soft ranking)` + `C(one_m synth penalty power)` with minimum 3-run set.
- Window/conditions: `tf=5m`, `quote=KRW`, `top_n=50`, `start=2026-02-24`, `end=2026-03-04`.
- Config variants:
- `Run0 Baseline`: `A=off`, `C power=1.0`
- `Run1 C-only`: `A=off`, `C power=2.0`
- `Run2 A+C`: `A=on(lookback=3,beta=2,q_floor=0.2,oversample=3)`, `C power=2.0`

### 10.1 Features Build Result (3-run)
| run | dataset | rows_final | rows_dropped_no_micro | rows_dropped_one_m | one_m_synth_ratio_p50 | one_m_synth_ratio_p90 |
|---|---|---:|---:|---:|---:|---:|
| Run0 Baseline | `features_v3_ac_baseline` | 15,936 | 35,227 | 2,763 | 0.2000 | 0.8000 |
| Run1 C-only | `features_v3_ac_conly` | 15,936 | 35,227 | 2,763 | 0.2000 | 0.8000 |
| Run2 A+C | `features_v3_ac` | 15,936 | 35,227 | 2,763 | 0.2000 | 0.8000 |

Interpretation:
- `rows_dropped_no_micro` did not worsen with A-on in this window.
- For this top50 window, A changed ordering but not final top50 set membership.

### 10.2 Universe Quality Guard Checks
- Lookback leakage guard:
- `value_from_ts_ms=1771632000000 (2026-02-21T00:00:00Z)`
- `to_ts_ms_exclusive=1771891200000 (2026-02-24T00:00:00Z)`
- Confirmed window is `[start-lookback, start)`.
- Candidate pool:
- discovered markets `=60`
- candidate_count `=60` (expected `top_n*oversample=150`, capped by discovered size)
- Fallback:
- `fallback.filled_count=0`, `warnings=[]` for all runs.

### 10.3 Selected Markets (A on, top50)
- `KRW-XRP, KRW-BTC, KRW-ENSO, KRW-AZTEC, KRW-ETH, KRW-FLOW, KRW-KITE, KRW-SOL, KRW-0G, KRW-DOGE, KRW-BERA, KRW-AXS, KRW-CYBER, KRW-LA, KRW-IP, KRW-ADA, KRW-XLM, KRW-BCH, KRW-LINK, KRW-ENS, KRW-IN, KRW-ARDR, KRW-BREV, KRW-BIRB, KRW-BOUNTY, KRW-AUCTION, KRW-F, KRW-AAVE, KRW-AKT, KRW-APT, KRW-ATH, KRW-BARD, KRW-CFG, KRW-COW, KRW-EDGE, KRW-EGLD, KRW-ENA, KRW-ESP, KRW-KAITO, KRW-KAVA, KRW-KNC, KRW-MANTRA, KRW-MOODENG, KRW-NEAR, KRW-ONDO, KRW-ORBS, KRW-PENGU, KRW-PEPE, KRW-SAHARA, KRW-SHIB`
- Set-level diffs:
- baseline vs C-only: no set diff
- baseline vs A+C top50: no set diff
- baseline vs A+C top20: set diff exists (`KRW-CYBER`, `KRW-DOGE`, `KRW-ARDR`, `KRW-ENS`, `KRW-IN`, `KRW-LINK`)

### 10.4 A-on Score Effect on High Synth Markets
- `KRW-BOUNTY`: synth `0.4713`, `q=0.5287`, `q^2=0.2795`, score scaled down.
- `KRW-AUCTION`: synth `0.6319`, `q=0.3681`, `q^2=0.1355`, score scaled down.
- `KRW-F`: synth `0.7435`, `q=0.2565`, `q^2=0.0658`, score scaled down.
- In this window, these markets still remained within top50 due value rank and limited candidate depth.

### 10.5 C-on Weight Effect (effective sample impact)
- Sample-weight summary:
- baseline (`power=1.0`): high-synth(`>=0.5`) `sample_weight mean=0.2987`
- C-only/A+C (`power=2.0`): high-synth(`>=0.5`) `sample_weight mean=0.1027`
- Mid-synth(`[0.4,0.6)`) mean: `0.5741 -> 0.3445`
- C is functioning as intended (stronger down-weighting on synthetic-heavy rows).

### 10.6 Backtest Note for This Validation
- A quick C2-style backtest check was executed with `latest_v3` fixed model ref.
- Result was effectively identical across runs.
- This is expected because C changes training sample weights; without re-training per run, model-level impact cannot surface.

### 10.7 Artifacts
- Validation summary:
- `logs/ac_validation_20260305/ac_validation_summary.json`
- Sample-weight summary:
- `logs/ac_validation_20260305/sample_weight_summary.json`
- A+C universe report:
- `data/features/features_v3_ac/_meta/universe_quality_report.json`
- A+C build report:
- `data/features/features_v3_ac/_meta/build_report.json`
