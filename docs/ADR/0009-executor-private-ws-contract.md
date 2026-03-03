# ADR 0009 - Executor Private WS Contract (myOrder/myAsset)

## Status
Accepted (2026-03-03)

## Context
- REST polling-only order tracking has unnecessary latency and avoidable request overhead in live mode.
- Upbit private streams are event-driven and can remain silent for long periods even when healthy.
- `myAsset` subscription payload does not support `codes`; sending it can produce format errors.

## Decision
- Executor adds private WS client (`wss://api.upbit.com/websocket/v1/private`) with JWT `Authorization: Bearer`.
- Subscription contract:
  - `myOrder`: enabled by default, optional `codes` filter.
  - `myAsset`: enabled by default, `codes` is forbidden by builder guard.
  - payload format is configurable (`DEFAULT`, `JSON_LIST`, `SIMPLE`, `SIMPLE_LIST`).
  - if server returns `WRONG_FORMAT`, subscribe retries once with `DEFAULT` and stops fallback retries.
- Parser contract:
  - normalize `myOrder` and `myAsset` payload variants (`type/ty`, `timestamp/tms`, etc.).
  - unknown order `state` values are passed through as-is.
- Runtime contract:
  - startup calls REST `/v1/accounts` once and emits bootstrap `ASSET` events (`source=rest_bootstrap`).
  - WS `myOrder`/`myAsset` events are emitted as executor events with `source=ws_private`.
  - WS stats expose `last_rx_ts_ms`, `last_tx_ts_ms`, `ping_sent_count`, `pong_rx_count`.
  - fallback polling interval is dynamic:
    - WS connected: slow safety-net polling (default 180s).
    - WS disconnected: degraded polling (default 60s).

## Consequences
### Positive
- Live state updates move from poll-first to event-first.
- Polling pressure is reduced while keeping a fallback path for disconnect windows.
- Python daemon can consume a single executor event stream for ORDER/FILL/ASSET updates.

### Trade-offs
- WinHTTP websocket implementation is Windows-first.
- WS reconnection behavior depends on operator env tuning (`*_PRIVATE_WS_*`, polling interval envs).
- Quiet private streams are expected behavior; health must be inferred from keepalive/reconnect state, not message frequency alone.
