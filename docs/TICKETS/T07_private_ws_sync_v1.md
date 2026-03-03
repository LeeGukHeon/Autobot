# T07 - Private WS Sync v1 (myOrder/myAsset)

## Goal
- Add event-driven live-state synchronization using Upbit private websocket.
- Keep REST polling as a safety net with lower frequency.

## Scope Implemented
- New private websocket client:
  - `autobot/upbit/ws/private_client.py`
  - endpoint: `upbit.websocket.private_url`
  - auth: `Authorization: Bearer <JWT>`
  - reconnect + keepalive + basic stats
- Private payload/parser support:
  - `build_private_subscribe_payload(...)`
  - `parse_private_event(...)` -> `MyOrderEvent | MyAssetEvent`
- Live handlers:
  - `autobot/live/ws_handlers.py`
  - `myOrder` -> `orders/intents` upsert
  - `myAsset` -> `positions` upsert/delete
- Daemon integration:
  - `run_live_sync_daemon_with_private_ws(...)`
  - `live run` uses WS path when `live.sync.use_private_ws=true`
  - REST polling remains enabled with minimum 60s interval as fallback

## Runtime Notes
- `live run` summary now includes:
  - `ws_events`
  - `ws_last_event_ts_ms`
  - `ws_last_event_latency_ms`
  - `ws_stats` (reconnect_count 등)
- WS event checkpoints:
  - `last_ws_event`

## Validation
- Parser/payload/unit tests added for private WS path.
- Daemon integration test verifies WS event -> DB reflection.
