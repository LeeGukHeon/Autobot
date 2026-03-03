# RUNBOOK - Executor Order-Test to Live Validation

## 1) Preconditions
- Live mode requires:
  - `AUTOBOT_EXECUTOR_MODE=live`
  - `AUTOBOT_LIVE_ENABLE=YES`
  - `UPBIT_ACCESS_KEY`, `UPBIT_SECRET_KEY`
- Optional guard rails:
  - `AUTOBOT_LIVE_ALLOWED_MARKETS=KRW-BTC,KRW-ETH,...`
  - `AUTOBOT_LIVE_MIN_NOTIONAL_KRW=<number>`

## 2) Order-Test Stage (No Real Order Creation)
1. Start executor in order-test mode (`--mode order_test`).
2. Submit test intents (`autobot exec submit-test ...`).
3. Validate stream payload:
   - `event_name=ORDER_ACCEPTED`
   - `mode=order_test`
4. Validate cancel behavior:
   - cancel request returns `event_name=CANCEL_RESULT` local ack.
   - no dependency on order-test UUID for remote cancel/query.

## 3) Live Stage (Controlled Exposure)
1. Switch executor to live mode with live gate enabled.
2. Submit a limit order with near-unfillable price.
3. Verify sequence:
   - `ORDER_ACCEPTED`
   - `ORDER_STATE` (`wait`/`watch`)
   - `CANCEL_RESULT`
   - `ORDER_STATE` (`cancel`)

## 4) Ambiguous Outcome Recovery Drill
1. Submit a live order intent.
2. During submit, induce timeout/network interruption.
3. Confirm executor behavior:
   - no duplicate POST with same identifier
   - lookup recovery via `GET /v1/order?identifier=...`
   - final result is either recovered accepted order or explicit failure requiring new identifier.

## 5) Prohibited Operations
- Do not retry create-order POST with the same `identifier`.
- Do not send POST as form-urlencoded; JSON only.
- Do not use order-test response UUID/identifier for real query/cancel.
- Do not continue rapid group requests after 429; breaker cooldown must elapse.

## 6) Private WS Operations (T10)
- Enable private WS in live mode:
  - `AUTOBOT_EXECUTOR_PRIVATE_WS_ENABLED=true`
  - `AUTOBOT_UPBIT_PRIVATE_WS_URL=wss://api.upbit.com/websocket/v1/private`
- Optional stream controls:
  - `AUTOBOT_EXECUTOR_PRIVATE_WS_ENABLE_MYORDER=true`
  - `AUTOBOT_EXECUTOR_PRIVATE_WS_ENABLE_MYASSET=true`
  - `AUTOBOT_EXECUTOR_PRIVATE_WS_ORDER_CODES=KRW-BTC,KRW-ETH` (myOrder filter only)
  - `AUTOBOT_EXECUTOR_PRIVATE_WS_FORMAT=DEFAULT|JSON_LIST|SIMPLE|SIMPLE_LIST`
  - `AUTOBOT_EXECUTOR_PRIVATE_WS_FORMAT_FALLBACK_ONCE=true` (on `WRONG_FORMAT`, retry once with `DEFAULT`)
- Keepalive/reconnect tuning:
  - `AUTOBOT_EXECUTOR_PRIVATE_WS_KEEPALIVE_MODE=message|frame|off` (default: `message`)
  - `AUTOBOT_EXECUTOR_PRIVATE_WS_PING_ON_CONNECT=true` (default: `true`)
  - `AUTOBOT_EXECUTOR_PRIVATE_WS_PING_INTERVAL_SEC=60`
  - `AUTOBOT_EXECUTOR_PRIVATE_WS_PONG_GRACE_SEC=20`
  - `AUTOBOT_EXECUTOR_PRIVATE_WS_FORCE_RECONNECT_ON_STALE=true`
  - `AUTOBOT_EXECUTOR_PRIVATE_WS_STALE_RX_THRESHOLD_SEC=110`
  - `AUTOBOT_EXECUTOR_PRIVATE_WS_UP_STATUS_LOG=false`
  - `AUTOBOT_EXECUTOR_PRIVATE_WS_IDLE_TIMEOUT_SEC=125`
  - `AUTOBOT_EXECUTOR_PRIVATE_WS_RECONNECT_ENABLED=true`
  - `AUTOBOT_EXECUTOR_PRIVATE_WS_RECONNECT_BASE_MS=1000`
  - `AUTOBOT_EXECUTOR_PRIVATE_WS_RECONNECT_MAX_MS=15000`
- Polling fallback tuning:
  - `AUTOBOT_EXECUTOR_POLL_INTERVAL_WS_CONNECTED_SEC=180`
  - `AUTOBOT_EXECUTOR_POLL_INTERVAL_WS_DEGRADED_SEC=60`
  - `AUTOBOT_EXECUTOR_POLL_INTERVAL_REST_ONLY_MS=1500`
- Guardrail:
  - never include `codes` in `myAsset` subscribe payload.
  - keepalive scheduler sends only ping (`"PING"` text or ping frame); it never re-sends subscribe payload.
  - quiet-account soak check (`>=130s`): confirm `ping_sent_count > 0` and message mode receives `{"status":"UP"}` (`pong_rx_count` increments).

## 7) Replace/Timeout Operations (T11)
- Timeout policy env:
  - `AUTOBOT_EXECUTOR_ORDER_TIMEOUT_SEC=0|N` (`0` disables timeout path)
  - `AUTOBOT_EXECUTOR_ORDER_TIMEOUT_REPLACE_ENABLED=false|true`
- Runtime contract:
  - timeout emits `event_name=ORDER_TIMEOUT`
  - replace path emits `event_name=ORDER_REPLACED`
  - default timeout action is cancel; replace-enabled mode requires external price decision and explicit `ReplaceOrder` RPC call.
