# DAILY_MICRO_REPORT_2026-03-03

## Summary
- target_date: 2026-03-03
- quote/top_n: KRW / 50
- book_available_ratio: 0.017645
- trade_source_ws_ratio: 0.013606
- micro_available_ratio: 1.000000
- join_match_ratio: NA

## Commands
- python -m autobot.cli collect ticks --mode daily --quote KRW --top-n 50 --days-ago 1 --raw-root data/raw_ticks/upbit/trades --rate-limit-strict true --workers 1 --max-pages-per-target 50 --dry-run false
- python -m autobot.cli micro aggregate --start 2026-03-03 --end 2026-03-03 --quote KRW --top-n 50 --raw-ticks-root data/raw_ticks/upbit/trades --raw-ws-root data/raw_ws/upbit/public --out-root data/parquet/micro_v1
- python -m autobot.cli micro validate --out-root data/parquet/micro_v1
- python -m autobot.cli micro stats --out-root data/parquet/micro_v1

## Artifacts
- ticks_collect_report: data/raw_ticks/upbit/_meta/ticks_collect_report.json
- micro_aggregate_report: data\parquet\micro_v1\_meta\aggregate_report.json
- micro_validate_report: data\parquet\micro_v1\_meta\validate_report.json
- micro_manifest: data\parquet\micro_v1\_meta\manifest.parquet

## Excerpts
- ticks run_id: 20260304T082547Z
- micro aggregate run_id: 20260304T082610Z
- micro rows_written_total: 7191
- micro parts: 380
