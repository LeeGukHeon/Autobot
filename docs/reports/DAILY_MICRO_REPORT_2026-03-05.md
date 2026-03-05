# DAILY_MICRO_REPORT_2026-03-05

## Summary
- batch_date: 2026-03-05
- batch_date_source: DEFAULT_KST_YESTERDAY
- ws_date: 2026-03-05
- ws_date_reason: MATCHED_BATCH_DATE_HAS_WS_PARTS
- target_date(legacy): 2026-03-05
- quote/top_n: KRW / 50
- candles_topup: PASS
- book_available_ratio: 0.640635
- trade_source_ws_ratio: 0.565647
- micro_available_ratio: 1.000000
- join_match_ratio: NA
- tiering_recommendation_status: INSUFFICIENT_SAMPLES
- orderbook_verification_gate(ws_date): FAIL
- t15_policy_gate: FAIL
- t15_data_gate: PASS (reason=OK)
- t15_overall_gate: FAIL
- t15_1_revalidation_gate(legacy): FAIL

## Candles Daily Top-up
- skipped: False
- status: PASS
- plan(selected_markets=50, targets=266, skipped_ranges=32)
- collect(processed=266, ok=136, warn=130, fail=0, calls=289)
- validate(checked=250, warn=0, fail=0)
- coverage_delta.average_delta_pct: 0

## Orderbook Verification (ws_date basis: Folder / Validate / Stats / Health)
- ws_date: 2026-03-05
- ws_date_reason: MATCHED_BATCH_DATE_HAS_WS_PARTS
- folder(orderbook parts > 0): PASS (parts=35, bytes=156579184)
- validate(fail_files=0, parse_ok>=0.99): PASS (exit=0, checked=56, fail_files=0, parse_ok_ratio=1.000000)
- stats(orderbook_rows > 0): FAIL (exit=0, orderbook_rows=0, trade_rows=0)
- stats_raw_root_filter(rows_before=494, rows_after=478, ignored_outside_raw_root=16)
- health(connected=true, orderbook_rx_lag_sec<=180): PASS (connected=True, orderbook_rx_lag_sec=0.31, trade_rx_lag_sec=0.28, health_lag_sec=0.28, subscribed_markets=50)
- overall: FAIL

## T15.1 Revalidation Gates (Policy / Data / Overall)
- t15_policy_gate: FAIL (reason=THRESHOLD_NOT_MET)
  - MICRO_MISSING_FALLBACK ratio < 10 %: PASS (actual=0.000000, fallback_count=0, orders_submitted=2)
  - tier_unique_count >= 2: PASS (actual=2)
  - replace+cancel+timeout >= 1: FAIL (actual=0)
- t15_data_gate: PASS (reason=OK)
  - book_available_ratio >= 0.200: PASS (actual=0.640635)
  - trade_source_ws_ratio >= 0.200: PASS (actual=0.565647)
  - batch_date_ws_parts(orderbook=35, trade=21, has_ws_parts=True)
- note: data_gate can be DEFER during early operations when WS backfill is unavailable
- t15_overall_gate(policy AND data): FAIL
- t15_1_revalidation_gate(legacy=overall): FAIL

## Latest Paper Smoke (10m)
- smoke_run_attempted: True
- smoke_run_exit_code: 0
- smoke_skipped: False
- smoke_run_output_preview: [paper-smoke] run_id=paper-20260306-032723 | [paper-smoke] micro_provider=LIVE_WS | [paper-smoke] orders_submitted=2 | [paper-smoke] micro_missing_fallback_count=0 | [paper-smoke] micro_missing_fallback_ratio=0.000000 | [paper-smoke] tier_unique_count=2 (HIGH,LOW) | [paper-smoke] replace_cancel_timeout_total=0 | [paper-smoke] warmup_elapsed_sec=0.134 | [paper-smoke] warmup_satisfied=True | [paper-
- smoke_report_path: /home/ubuntu/MyApps/Autobot/logs/paper_micro_smoke/latest.json
- smoke_available: True
- smoke_generated_at: 03/06/2026 03:37:24
- smoke_run_id: paper-20260306-032723
- smoke_micro_provider: LIVE_WS
- smoke_provider_subscribed_markets: 30
- smoke_live_ws_connected: True
- smoke_live_ws_subscribed_markets: 50
- smoke_live_ws_micro_snapshot_age_ms: 88
- smoke_live_ws_health_snapshot_available: True
- smoke_live_ws_health_snapshot_path: /home/ubuntu/MyApps/Autobot/data/raw_ws/upbit/_meta/ws_public_health.json

## Tiering Recommendation (Recent Paper LQ Score)
- tiering_run_attempted: True
- tiering_run_exit_code: 0
- tiering_skipped: False
- tiering_report_path: /home/ubuntu/MyApps/Autobot/logs/micro_tiering/latest.json
- status: INSUFFICIENT_SAMPLES
- sample_count: 5
- fallback_count: 0
- recommended_t1: 5.33210898718665
- recommended_t2: 16.2305403960464

## Commands
- python -m autobot.cli collect plan-candles --base-dataset candles_api_v1 --parquet-root data/parquet --out /home/ubuntu/MyApps/Autobot/data/collect/_meta/candle_topup_plan_daily.json --lookback-months 3 --tf 1m,5m,15m,60m,240m --quote KRW --market-mode top_n_by_recent_value_est --top-n 50 --max-backfill-days-1m 3 --end 2026-03-05
- python -m autobot.cli collect candles --plan /home/ubuntu/MyApps/Autobot/data/collect/_meta/candle_topup_plan_daily.json --out-dataset candles_api_v1 --parquet-root data/parquet --workers 1 --dry-run false --rate-limit-strict true
- python -m autobot.cli collect ticks --mode daily --quote KRW --top-n 50 --days-ago 1 --raw-root data/raw_ticks/upbit/trades --rate-limit-strict true --workers 1 --max-pages-per-target 50 --dry-run false
- python -m autobot.cli micro aggregate --start 2026-03-05 --end 2026-03-05 --quote KRW --top-n 50 --raw-ticks-root data/raw_ticks/upbit/trades --raw-ws-root data/raw_ws/upbit/public --out-root data/parquet/micro_v1
- python -m autobot.cli micro validate --out-root data/parquet/micro_v1
- python -m autobot.cli micro stats --out-root data/parquet/micro_v1
- python -m autobot.cli collect ws-public validate --date 2026-03-05 --raw-root data/raw_ws/upbit/public --meta-dir data/raw_ws/upbit/_meta --quarantine-corrupt true --min-age-sec 300
- python -m autobot.cli collect ws-public stats --date 2026-03-05 --raw-root data/raw_ws/upbit/public --meta-dir data/raw_ws/upbit/_meta
- pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/paper_micro_smoke.ps1 -DurationSec 600 -PaperMicroProvider live_ws -WarmupSec 60
- pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/recommend_micro_tiering.ps1 -RecentHours 24 -MinSamples 30

## Artifacts
- candles_plan: /home/ubuntu/MyApps/Autobot/data/collect/_meta/candle_topup_plan_daily.json
- candles_collect_report: /home/ubuntu/MyApps/Autobot/data/collect/_meta/candle_collect_report.json
- candles_validate_report: /home/ubuntu/MyApps/Autobot/data/collect/_meta/candle_validate_report.json
- ticks_collect_report: data/raw_ticks/upbit/_meta/ticks_collect_report.json
- micro_aggregate_report: data/parquet/micro_v1/_meta/aggregate_report.json
- micro_validate_report: data/parquet/micro_v1/_meta/validate_report.json
- micro_manifest: data/parquet/micro_v1/_meta/manifest.parquet
- daily_date_alignment: data/parquet/micro_v1/_meta/daily_date_alignment.json
- ws_validate_report: data/raw_ws/upbit/_meta/ws_validate_report.json
- t15_gate_report: data/parquet/micro_v1/_meta/daily_t15_gate_report.json
- smoke_report: /home/ubuntu/MyApps/Autobot/logs/paper_micro_smoke/latest.json
- tiering_report: /home/ubuntu/MyApps/Autobot/logs/micro_tiering/latest.json

## Excerpts
- candles collect processed_targets: 266
- candles collect fail_targets: 0
- ticks run_id: 20260305T182348Z
- micro aggregate run_id: 20260305T182426Z
- micro rows_written_total: 56739
- micro parts: 1500
