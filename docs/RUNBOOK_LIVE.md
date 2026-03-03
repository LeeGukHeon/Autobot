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
3. Validate submit pass criteria (B1):
   - `event_name=ORDER_ACCEPTED`
   - `mode=order_test`
   - Submit RPC result is accepted (`reason=accepted_in_order_test_mode`).
4. Validate cancel behavior (B1 continuation):
   - cancel request returns local ack (`event_name=CANCEL_RESULT`).
   - order-test cancel pass criterion is local ack only (`reason=cancelled_local_ack_order_test_mode`).
   - no dependency on order-test UUID/identifier for remote cancel/query.
   - order-test response UUID/identifier is not reusable for remote query/cancel expectations.

## 3) Live Stage (Controlled Exposure)
1. Switch executor to live mode with live gate enabled.
2. Build submit parameters from live market constraints:
   - `python scripts/build_live_submit_params.py --market KRW-BTC --side bid`
   - Use script output so `price * volume >= market.<side>.min_total` from `GET /v1/orders/chance`.
   - Use script output price to align tick size (`GET /v1/orderbook/instruments`).
3. Submit a limit order with near-unfillable price.
4. Verify sequence (C3):
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
- Do not send `time_in_force=gtc`; omit `time_in_force` for default limit behavior.
- Do not continue rapid group requests after 429; breaker cooldown must elapse.
- Legacy gRPC compatibility:
  - incoming `tif=GTC` and replace `new_time_in_force=gtc` are mapped to omit before Upbit request build.
  - optional debug log: set `AUTOBOT_EXECUTOR_DEBUG_TIF_COMPAT=true`.

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
  - replace pass criteria (C4): `ORDER_REPLACED` event, or accepted replace followed by `ORDER_STATE` convergence through WS/GET fallback.
  - default timeout action is cancel; replace-enabled mode requires external price decision and explicit `ReplaceOrder` RPC call.
- `new_time_in_force` for replace:
  - `limit`: omit or one of `ioc|fok|post_only`
  - `best`: required `ioc|fok` (do not use `post_only`)
  - never send `gtc`
- T11.1 replace-chain persistence checks (`AUTOBOT_EXECUTOR_STATE_PATH` JSON):
  - new replace record keeps chain fields:
    - `prev_identifier`, `prev_upbit_uuid`
    - `root_identifier`, `root_upbit_uuid`
    - `replace_attempt`
    - `chain_status` (`REPLACE_PENDING` -> `REPLACE_CONFIRMED*`)
    - `last_replace_ts_ms`
  - predecessor record transitions to:
    - `status=REPLACED`
    - `chain_status=REPLACED_BY_SUCCESSOR`

## 8) C6 Fault Injection Drill (T11.2)
- Local-only drill target:
  - `autobot_executor_fault_drills_tests`
  - run: `ctest -C Debug -R autobot_executor_fault_drills_tests --output-on-failure`
- Covered drills:
  - D1: `POST timeout -> GET(identifier) recover`
  - D2: `POST 5xx -> GET(identifier) recover`
  - D3: `429 group breaker cooldown`
  - D4: `418 global breaker cooldown`
  - D5: `WS drop -> degraded polling -> reconnect restore`
- Rules:
  - no intentional real-exchange `429/418` induction.
  - no live fault injection against exchange endpoints.

## 9) Live Risk Manager (T12)
- Persistence:
  - SQLite `risk_plans` table in live state DB.
  - restart-safe plan lifecycle (`ACTIVE/TRIGGERED/EXITING/CLOSED`).
- Trigger engine:
  - TP/SL/Trailing evaluation from ticker `trade_price`.
  - trailing watermark updates on new highs.
- Execution contract:
  - limit-only exit submit.
  - timeout replace via executor `ReplaceOrder` until `replace_max`.
