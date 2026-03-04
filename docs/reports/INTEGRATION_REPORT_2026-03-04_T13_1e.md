# INTEGRATION_REPORT_2026-03-04_T13_1e

## 0) Summary
- Ticket: `T13.1e`
- Scope: OHLC(v1-equivalent) + `micro_v1` join -> `features_v2` + `label_v1` + validate/stats
- Result:
  - build: `processed=20 ok=0 warn=20 fail=0`
  - validate: `checked=20 ok=0 warn=20 fail=0`
  - pytest: `129 passed`

## 1) Executed Commands
- Candle top-up plan (for precondition recovery, fixed micro markets):
  - `python -m autobot.cli collect plan-candles --base-dataset candles_api_v1 --out data/collect/_meta/candle_topup_plan_t13_1e_5m.json --lookback-months 1 --tf 5m --quote KRW --market-mode fixed_list --markets KRW-0G,KRW-ADA,KRW-AUCTION,KRW-AXS,KRW-AZTEC,KRW-BERA,KRW-BIRB,KRW-BOUNTY,KRW-BTC,KRW-CYBER,KRW-DOGE,KRW-ENSO,KRW-ETH,KRW-F,KRW-FLOW,KRW-IP,KRW-KITE,KRW-LA,KRW-SOL,KRW-XRP --end 2026-03-04`
- Candle top-up execution (2회):
  - `python -m autobot.cli collect candles --plan data/collect/_meta/candle_topup_plan_t13_1e_5m.json --out-dataset candles_api_v1 --workers 1 --dry-run false --rate-limit-strict true`
- Candle top-up plan (1m, same top20 fixed markets):
  - `python -m autobot.cli collect plan-candles --base-dataset candles_api_v1 --out data/collect/_meta/candle_topup_plan_t13_1e_1m.json --lookback-months 1 --tf 1m --quote KRW --market-mode fixed_list --markets KRW-0G,KRW-ADA,KRW-AUCTION,KRW-AXS,KRW-AZTEC,KRW-BERA,KRW-BIRB,KRW-BOUNTY,KRW-BTC,KRW-CYBER,KRW-DOGE,KRW-ENSO,KRW-ETH,KRW-F,KRW-FLOW,KRW-IP,KRW-KITE,KRW-LA,KRW-SOL,KRW-XRP --max-backfill-days-1m 2 --end 2026-03-04`
- Candle top-up execution (1m, 2회):
  - `python -m autobot.cli collect candles --plan data/collect/_meta/candle_topup_plan_t13_1e_1m.json --out-dataset candles_api_v1 --workers 1 --dry-run false --rate-limit-strict true`
- Build:
  - `python -m autobot.cli features build --feature-set v2 --tf 5m --quote KRW --top-n 20 --start 2026-03-03 --end 2026-03-04 --base-candles auto --micro-dataset micro_v1 --require-micro true --dry-run false`
- Validate:
  - `python -m autobot.cli features validate --feature-set v2 --tf 5m --quote KRW --top-n 20`
- Stats:
  - `python -m autobot.cli features stats --feature-set v2 --tf 5m --quote KRW --top-n 20`
- Test:
  - `python -m pytest -q`

## 2) Run Context
- period: `2026-03-03 ~ 2026-03-04` (UTC)
- tf: `5m`
- universe: `KRW top20` (micro_v1 기간 내 market 기반)
- selected markets:
  - `KRW-BOUNTY, KRW-CYBER, KRW-0G, KRW-ADA, KRW-AUCTION, KRW-AXS, KRW-AZTEC, KRW-BERA, KRW-BIRB, KRW-BTC, KRW-DOGE, KRW-ENSO, KRW-ETH, KRW-F, KRW-FLOW, KRW-IP, KRW-KITE, KRW-LA, KRW-SOL, KRW-XRP`

## 3) Preflight (Mandatory)
- selected base candles root: `data/parquet/candles_api_v1`
- preflight status: `PASS`
- candles top-up status:
  - 5m fixed-plan: 최종 `ok=38, fail=2` (3회차 기준, 일부 target fail 잔존)
  - 1m fixed-plan: 최종 `ok=20, fail=0` (재시도 후 전부 OK)
- coverage check:
  - all 20 markets `overlap_ok=true`
  - all 20 markets `label_horizon_ok=true` (`H=12`)
- label tail guard:
  - `tail_dropped_rows(total)=240`

## 4) Build Output
- output root: `data/features/features_v2`
- rows_total: `92`
- min/max ts: `1772551800000 / 1772553600000`
- status_counts: `OK=0, WARN=20, FAIL=0`
- warning root cause:
  - `LOW_JOIN_MATCH_RATIO` (짧은 micro 수집 구간 대비 candle 기준 비교 구간이 길어 join ratio가 낮음)

## 5) Validate / QA
- schema_ok: `true`
- checked/ok/warn/fail: `20 / 0 / 20 / 0`
- null_ratio_overall: `0.0`
- label_distribution: `pos=72, neg=20, neutral=0, total=92`
- micro join quality:
  - `join_match_ratio=0.030717761557177616`
  - `micro_available_ratio=1.0`
  - trade coverage ms: `p50=258486, p90=298050`
  - book coverage ms: `p50=298350, p90=299700`

## 6) Artifacts
- Dataset:
  - `data/features/features_v2/tf=5m/market=*/date=*/part-000.parquet`
- Meta:
  - `data/features/features_v2/_meta/manifest.parquet`
  - `data/features/features_v2/_meta/feature_spec.json`
  - `data/features/features_v2/_meta/label_spec.json`
  - `data/features/features_v2/_meta/build_report.json`
  - `data/features/features_v2/_meta/validate_report.json`

## 7) Next Ticket Gate (T14.2)
- minimum rows threshold: `5000`
- current rows_total: `92`
- ready_for_t14_2: `false`
- reason:
  - 최소 학습 행 수(`5000`) 미달
  - 현재 `micro` 수집 구간이 짧아 샘플 수가 매우 적음
