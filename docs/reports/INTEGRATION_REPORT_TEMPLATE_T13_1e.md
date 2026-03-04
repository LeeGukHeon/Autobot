# INTEGRATION_REPORT_TEMPLATE_T13_1e

## 0) Summary
- Ticket: `T13.1e`
- Scope: OHLC(v1-equivalent) + `micro_v1` join -> `features_v2` + `label_v1` + QA
- Result:
  - build: `processed=<n> ok=<n> warn=<n> fail=<n>`
  - validate: `checked=<n> ok=<n> warn=<n> fail=<n>`
  - pytest: `<PASS/FAIL>`

## 1) Executed Commands
- Build:
  - `python -m autobot.cli features build --feature-set v2 --tf 5m --quote KRW --top-n 20 --start <YYYY-MM-DD> --end <YYYY-MM-DD> --base-candles auto --micro-dataset micro_v1 --require-micro true --dry-run false`
- Validate:
  - `python -m autobot.cli features validate --feature-set v2 --tf 5m --quote KRW --top-n 20`
- Stats:
  - `python -m autobot.cli features stats --feature-set v2 --tf 5m --quote KRW --top-n 20`
- Test:
  - `python -m pytest -q`

## 2) Preflight (Mandatory)
- selected base candles root: `<...>`
- micro period: `<start> ~ <end>`
- preflight status: `<PASS/FAIL>`
- failures (if any):
  - `<market + reason>`

## 3) Build Output
- markets: discovered `<n>`, selected `<n>`
- rows_total: `<n>`
- min/max ts: `<...>`
- tail_dropped_rows(total): `<n>`
- join_match_ratio(weighted): `<ratio>`

## 4) Validate / QA
- schema_ok: `<true/false>`
- null_ratio_overall: `<ratio>`
- worst_columns_top5: `<...>`
- label_distribution: `<pos/neg/neutral/total>`
- micro_join_quality:
  - join_match_ratio: `<ratio>`
  - micro_available_ratio: `<ratio>`
  - coverage_ms trade/book p50/p90: `<...>`

## 5) Artifacts
- Dataset:
  - `data/features/features_v2/tf=5m/market=*/date=*/part-000.parquet`
- Meta:
  - `data/features/features_v2/_meta/manifest.parquet`
  - `data/features/features_v2/_meta/feature_spec.json`
  - `data/features/features_v2/_meta/label_spec.json`
  - `data/features/features_v2/_meta/build_report.json`
  - `data/features/features_v2/_meta/validate_report.json`

## 6) Next Ticket Gate (T14.2)
- minimum rows threshold: `<n>`
- ready_for_t14_2: `<true/false>`
- reason:
  - `<rows/join/schema status>`
