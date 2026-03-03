# T10 - C++ Executor Private WS v1 + REST Fallback

## Goal
1. C++ executor subscribes to private WS (`myOrder`, `myAsset`) and emits `ORDER_STATE`, `FILL`, `ASSET` events with low latency.
2. REST polling remains only as a low-frequency safety net.
3. Python live daemon uses executor event stream as single source of truth for live state.

## References (Upbit docs)
- WebSocket connection/message limits must be respected.
- `myOrder`/`myAsset` can be quiet for long periods when no changes occur.
- `myOrder` supports `codes` as optional filter.
- `myAsset` must not include `codes`; including it can return `WRONG_FORMAT`.
- Initial `myAsset` updates can be delayed for several minutes after first subscribe.

## Deliverables
1. `cpp/src/executor/upbit/ws_private_client.{h,cpp}`
- Endpoint: `wss://api.upbit.com/websocket/v1/private`
- Auth header: `Authorization: Bearer <JWT>`
- Keepalive (ping/pong) + reconnect with capped backoff
2. Subscription payload builder
- `myOrder`: full-market subscription by default (`codes` omitted or empty)
- `myAsset`: hard guard to never include `codes`
3. Private WS parser and mapper
- `myOrder` -> `ORDER_STATE` / `FILL`
- `myAsset` -> `ASSET`
- Unknown enum/state values are passed through as raw strings
4. `OrderManager` integration
- WS healthy: event-driven state updates
- WS unhealthy: fallback REST polling every 60-180 seconds
5. Python integration rule
- Live daemon consumes executor events only in live mode
- Existing Python private WS path is disabled by config for this mode

## Mandatory Guardrails
1. Reject any `myAsset` subscribe payload that contains `codes`.
2. Bootstrap asset state from REST `/v1/accounts` snapshot on startup; treat WS asset stream as delta updates.
3. Treat no-message private WS periods as normal; rely on keepalive and reconnect policy.
4. Keep connect/send rates below documented limits during reconnect storms.

## File Plan
### Add
- `cpp/src/executor/upbit/ws_private_client.h`
- `cpp/src/executor/upbit/ws_private_client.cpp`
- `cpp/src/executor/upbit/ws_private_parsers.h`
- `cpp/src/executor/upbit/ws_private_parsers.cpp`
- `cpp/src/executor/tests/ws_private_parsers_tests.cpp`

### Modify
- `cpp/src/executor/order_manager.cpp`
- `cpp/src/executor/order_manager.h`
- `docs/ADR/0009-executor-private-ws-contract.md`

## Tests
1. Unit tests
- `myOrder` subscribe payload shape and field contract
- `myAsset` payload guard (`codes` forbidden)
- parser mapping for key order states: `trade`, `done`, `cancel`, `prevented`
2. Integration (mocked transport)
- Inject WS private events and verify executor event outputs
- Verify fallback polling activates on WS disconnect and returns to WS path after reconnect

## Definition of Done
1. Private WS connection stays healthy for at least 2 minutes with keepalive/reconnect logic (even when no data arrives).
2. `myOrder`/`myAsset` changes are emitted to Python as executor events within a few seconds.
3. When WS fails, fallback REST polling works without request bursts or duplicate terminal state transitions.

## Implementation Status (2026-03-03)
### Completed
1. Added private WS transport and parser files:
- `cpp/src/executor/upbit/ws_private_client.{h,cpp}`
- `cpp/src/executor/upbit/ws_private_parsers.{h,cpp}`
2. Added parser/payload unit tests:
- `cpp/src/executor/tests/ws_private_parsers_tests.cpp`
3. Added account bootstrap + WS integration points:
- `UpbitPrivateClient::Accounts()` (`GET /v1/accounts`)
- `UpbitRestClient::GetAccountsSnapshot()`
- `OrderManager` startup bootstrap emits `ASSET` with `source=rest_bootstrap`
- `OrderManager` WS handlers emit `ORDER_STATE`, `FILL`, `ASSET` with `source=ws_private`
4. Added WS-aware fallback polling controls in `OrderManager`:
- WS connected polling interval (default 180s)
- WS degraded polling interval (default 60s)
- rest-only polling interval (default 1500ms)
5. Added CI-friendly test registration:
- `autobot_executor_unit_tests`
- `autobot_executor_ws_private_parsers_tests`

### Guardrails Implemented
1. `myAsset` subscribe object builder rejects non-empty `codes`.
2. Private WS subscribe payload format is configurable (`DEFAULT|JSON_LIST|SIMPLE|SIMPLE_LIST`).
3. On server `WRONG_FORMAT`, executor retries subscribe once with `DEFAULT` and does not loop fallback.
4. Private WS runtime stats expose `last_rx_ts_ms`, `last_tx_ts_ms`, `ping_sent_count`, `pong_rx_count`.
5. WS keepalive + reconnect loop with rate-limit guards:
- connect limiter (`connect_rps`)
- send limiter (`message_rps`, `message_rpm`)
6. WS parser preserves unknown order states without collapsing values.

### Remaining Work (T10.1 Candidate)
1. Add transport-level integration test with mocked websocket frames for reconnect and fallback timing assertions.
2. Add explicit operator CLI switch (`--force-unlock` style parity) for WS emergency disable at process start.
3. Add runbook section with production-safe env presets for reconnect/polling values.
