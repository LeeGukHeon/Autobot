# SOURCE PLANE PRIMARY DATA CONTRACT 2026-04-14

## 0. Purpose

This document records the current contract for the always-on first-tier source data plane.

The goal is to keep the raw source layer stable and continuously collected even while:

- training timers are paused
- derived datasets are being redesigned
- acceptance and runtime orchestration are being refactored

In plain language:

- keep collecting the raw data that future training and live systems will need
- do not block first-tier collection on second-tier feature or tensor pipelines

## 1. Current First-Tier Source Plane

The current first-tier source plane is the set of raw inputs that must continue to arrive on the server.

It consists of:

1. `raw_ws_public`
   - channels:
     - `ticker`
     - `trade`
     - `orderbook`
2. `raw_ws_private`
   - channels:
     - `myOrder`
     - `myAsset`
3. `candles_api_v1`
   - timeframes:
     - `1m`
     - `5m`
     - `15m`
     - `60m`
     - `240m`
4. `raw_ticks`
   - `daily`
   - `backfill`
5. `raw_trade_v1`
   - canonical merged trade raw:
     - `ws trade` primary
     - `rest ticks` repair

These five pieces together are the current source plane.

## 2. What Is Not First-Tier Source Data

The following are not first-tier raw source layers:

- `candles_second_v1`
- `ws_candle_v1`
- `micro_v1`
- `lob30_v1`
- `sequence_v1`
- `private_execution_v1`
- `features_v4`
- `train_snapshot_close`

Those are downstream mutable or derived layers.

## 3. Market Universe Contract

The source plane does not use the live dynamic top-n directly.

Current source plane market contract:

- `source_plane.fixed_collection`
- quote:
  - `KRW`
- size:
  - `30`
- policy:
  - `upbit_krw_top_market_cap_ex_stables_v1`

Current interpretation:

- select KRW markets that are listed on Upbit
- rank by external market-cap order
- exclude stable / value-linked assets from the fixed layer

Current excluded symbols:

- `USDT`
- `USDC`
- `USDS`
- `USDE`
- `USD1`
- `XAUT`

The fixed source plane market contract is shared by:

- `ws-public`
- `candles_api_refresh`
- `raw_ticks daily`
- `raw_ticks backfill`
- `raw_trade_v1`

Meaning:

- these source-plane collectors are intended to keep the same 30-market base
- `private-ws` is the only first-tier source that is not market-universe based

## 4. Current Service Layout On Server

The current server-side source plane uses:

- always-on services
  - `autobot-ws-public.service`
  - `autobot-private-ws-archive.service`
- timed source jobs
  - `autobot-candles-api-refresh.timer`
  - `autobot-raw-ticks-daily.timer`
  - `autobot-raw-ticks-backfill.timer`
  - `autobot-raw-trade-v1.timer`

The source plane installer is:

- `scripts/install_server_source_plane_services.ps1`

This installer is expected to install and enable only the source-plane services above.

It must not install or start:

- `autobot-data-platform-refresh.*`
- `autobot-v5-train-snapshot-close.*`

## 5. Scheduling Contract

Current intended schedule:

1. `raw_ticks backfill`
   - `22:00 KST`
   - purpose:
     - repair recent raw trade history before nightly training preparation
   - days:
     - `1,2`
2. `raw_ticks daily`
   - `00:00 KST`
   - purpose:
     - finalize previous closed day raw ticks
   - days:
     - `1`
3. `raw_trade_v1`
   - rolling timer
   - `OnBootSec=6min`
   - `OnUnitActiveSec=20min`
   - purpose:
     - rebuild canonical merged raw trades for recent closed days
   - window:
     - recent `2` UTC dates
4. `candles_api_refresh`
   - rolling timer
   - `OnBootSec=4min`
   - `OnUnitActiveSec=20min`
   - purpose:
     - keep reference candle source complete for the fixed source-plane market layer

## 6. Date Semantics

The source plane is intentionally split between:

- collection trigger times in `KST`
- raw partition and canonical trade dates in `UTC`

This is aligned to Upbit data semantics because:

- candle API returns UTC candle timestamps
- trade ticks and websocket events are normalized into UTC-based partitions

Current rules:

### 6.1 `candles_api_v1`

- partitioned by tf / market
- event timestamps interpreted from Upbit UTC candle timestamps

### 6.2 `raw_ticks`

- stored under:
  - `data/raw_ticks/upbit/trades/date=YYYY-MM-DD/market=KRW-XXX`
- date partition is interpreted from event timestamp in UTC
- `daily` and `backfill` use UTC date semantics when mapping `days_ago` to target dates

### 6.3 `raw_trade_v1`

- stored under:
  - `data/raw_trade_v1/date=YYYY-MM-DD/market=KRW-XXX`
- date partition is UTC
- current rolling builder uses only closed UTC dates
- current in-progress UTC date must not remain as a partial canonical date after rebuild

## 7. Collection Behavior Contract

The source plane must prefer:

- rate-limit compliance
- completeness
- deterministic reuse

It must not prefer:

- short fixed global request budgets that stop before the fixed source-plane universe is fully covered

### 7.1 `ws-public`

Expected behavior:

- always-on daemon
- collects:
  - `ticker`
  - `trade`
  - `orderbook`
- current stored raw trade source of truth is websocket `trade`
- current orderbook stream is intentionally downsampled for storage efficiency

Important note:

- `orderbook_downsample` is not data loss in the same sense as parse failure
- it is intentional storage thinning

### 7.2 `private-ws`

Expected behavior:

- always-on daemon
- archive account event truth
- low event volume is normal when the system is not trading

### 7.3 `candles_api_v1`

Expected behavior:

- top-up missing tails and missing timeframes for the fixed 30-market layer
- use Upbit rate limits safely
- do not stop early because of a small global request cap

Current normalized rule:

- global `max_requests` cap is removed from normal server service execution
- collection runs until the plan is exhausted, loop guard triggers, or real API failure occurs

### 7.4 `raw_ticks`

Expected behavior:

- `daily` finalizes the previous day
- `backfill` repairs recent days
- uses Upbit rate limits safely
- does not stop early because of a small per-target page cap

Current normalized rule:

- `max_pages_per_target` cap is removed from normal server service execution
- collection runs until the trade ticks feed is exhausted, loop guard triggers, or real API failure occurs

### 7.5 `raw_trade_v1`

Expected behavior:

- canonical merged raw trade layer
- merge rule:
  - `ws` primary
  - `rest` repair
- key:
  - `market + sequential_id`
- must rebuild idempotently for targeted recent dates
- must prune future partial UTC dates
- must prune non-fixed legacy market partitions for rebuilt dates

## 8. Current File-Level Expectations

When the source plane is healthy:

- `raw_ws_public`
  - latest `ticker/trade/orderbook` files exist
  - `ws_public_health.json` reports `connected=true`
  - `subscribed_markets_count=30`
- `raw_ws_private`
  - latest `myOrder/myAsset` files may be sparse
  - sparse events are acceptable when there is no trading
- `candles_api_v1`
  - all fixed 30 markets exist for:
    - `1m`
    - `5m`
    - `15m`
    - `60m`
    - `240m`
- `raw_ticks`
  - recent relevant UTC dates should have fixed 30 markets present
  - extra legacy markets should not remain after current cleanup policy is applied to rebuilt dates
- `raw_trade_v1`
  - recent closed UTC dates should have fixed 30 markets present
  - the current open UTC date should not remain as a partially built canonical date

## 9. Current Known Limitations

The source plane is much cleaner than before, but not every historical partition is perfect.

Known limitations:

- older historical `raw_ticks` dates may still have incomplete fixed-30 coverage if those dates were never fully collected originally
- current source-plane cleanup removes legacy extra markets, but it does not invent missing historical rows
- `private-ws` health can look quiet during non-trading periods and should not automatically be interpreted as a failure

## 10. Practical Summary

The current source plane rule is:

- collect raw public market state continuously
- collect private account state continuously
- keep reference candles complete for the fixed 30-market layer
- keep REST trade history repaired for recent days
- keep a canonical merged raw trade layer rebuilt from closed UTC dates

This is the current raw foundation on which later feature, tensor, label, training, and live scanning layers are expected to depend.
