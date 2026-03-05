#!/usr/bin/env bash
set -euo pipefail

cd /home/ubuntu/MyApps/Autobot
source /home/ubuntu/MyApps/Autobot/.venv/bin/activate

# KST 어제 날짜(배치 기준)
BATCH_DATE=$(TZ=Asia/Seoul date -d "yesterday" +%F)

python -m autobot.cli collect plan-candles \
  --base-dataset candles_api_v1 \
  --parquet-root data/parquet \
  --out data/collect/_meta/candle_topup_plan_daily.json \
  --lookback-months 3 \
  --tf 1m,5m,15m,60m,240m \
  --quote KRW \
  --market-mode top_n_by_recent_value_est \
  --top-n 50 \
  --max-backfill-days-1m 3 \
  --end "$BATCH_DATE"

python -m autobot.cli collect candles \
  --plan data/collect/_meta/candle_topup_plan_daily.json \
  --out-dataset candles_api_v1 \
  --parquet-root data/parquet \
  --workers 1 \
  --dry-run false \
  --rate-limit-strict true

python -m autobot.cli collect ticks \
  --mode daily \
  --quote KRW \
  --top-n 50 \
  --days-ago 1 \
  --raw-root data/raw_ticks/upbit/trades \
  --rate-limit-strict true \
  --workers 1 \
  --max-pages-per-target 50 \
  --dry-run false

python -m autobot.cli micro aggregate \
  --start "$BATCH_DATE" --end "$BATCH_DATE" \
  --quote KRW --top-n 50 \
  --raw-ticks-root data/raw_ticks/upbit/trades \
  --raw-ws-root data/raw_ws/upbit/public \
  --out-root data/parquet/micro_v1

python -m autobot.cli micro validate --out-root data/parquet/micro_v1
python -m autobot.cli micro stats --out-root data/parquet/micro_v1

export AUTOBOT_BATCH_DATE="$BATCH_DATE"
python scripts/render_daily_report_full.py
