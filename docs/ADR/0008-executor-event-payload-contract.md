# ADR 0008 - Executor Event Payload Contract

## Status
Accepted (2026-03-03)

## Context
- Python live sync consumes executor `payload_json` and translates it into private WS-like updates.
- Unstable payload fields make recovery and incident analysis difficult.

## Decision
- Standardize `payload_json.event_name` for executor events while keeping proto `EventType` unchanged.
- Contract (required fields):

### `ORDER_ACCEPTED` (published as `EventType=ORDER_UPDATE`)
- `intent_id`
- `identifier`
- `uuid` / `upbit_uuid` (empty in order-test mode)
- `market`, `side`, `ord_type`
- `price_str`, `volume_str`
- `mode` (`order_test` or `live`)
- `ts_ms`
- optional `remaining_req: {group, sec}`

### `ORDER_STATE` (published as `EventType=ORDER_UPDATE`)
- `uuid` / `identifier`
- `market`
- `state`
- `executed_volume_str`
- `remaining_volume_str`
- `avg_price_str` (nullable)
- `updated_ts_ms`

### `FILL` (published as `EventType=FILL`)
- `uuid` / `identifier`
- `market`
- `price_str`
- `volume_str` (incremental fill size)
- `fee_str` (nullable)
- `ts_ms`

### `CANCEL_RESULT` (published as `EventType=ORDER_UPDATE`)
- `uuid` / `identifier`
- `ok`
- `reason`
- `state`
- `ts_ms`

### `ERROR` (published as `EventType=ERROR`)
- `where`
- `http_status`
- `upbit_error_name`
- `upbit_error_message`
- `breaker_state`
- `ts_ms`

## Consequences
- Python daemon can route by `event_name` even when `event_type` is coarse.
- Order-test cancel path is explicit local ack (`CANCEL_RESULT`) without remote cancel call.

