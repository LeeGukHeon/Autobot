# T11.2 Fault Injection Drill v1

## Goal
- Close C6 with deterministic local drills.
- Validate executor recovery/breaker behavior without hammering real exchange endpoints.

## Scope
- HTTP fault injection through `UpbitHttpClient::ITransport`.
- WS drop/reconnect fallback behavior through `OrderManager` + injected private-WS client.
- Recovery policy verification:
  - POST unknown result must not be blindly retried.
  - GET-by-identifier confirmation path must converge.
- Breaker verification:
  - `429` group breaker cooldown.
  - `418` global breaker cooldown.

## Implementation
- Added `cpp/src/executor/tests/fault_injection_transport.h/.cpp`.
  - Rule key: `(method, endpoint, nth_call, probability)`.
  - Actions: network error injection or custom HTTP response.
- Refactored `upbit/http_client`:
  - transport interface `UpbitHttpClient::ITransport`.
  - default transport delegates to existing WinHTTP code path.
- Added `autobot_executor_fault_drills_tests` target:
  - `cpp/src/executor/tests/fault_drills_tests.cpp`.
- Added `OrderManager` private-WS factory injection path for deterministic WS fault drills.

## Drill Cases
- D1: `POST /v1/orders` timeout -> `GET /v1/order?identifier=...` recovery.
- D2: `POST /v1/orders` 5xx -> `GET /v1/order?identifier=...` recovery.
- D3: `429` -> group breaker cooldown applies to same group requests.
- D4: `418` -> global breaker cooldown applies across groups.
- D5: WS drop -> degraded polling interval -> reconnect -> normal polling interval restore.

## How To Run
- Build target: `autobot_executor_fault_drills_tests`.
- Run via CTest:
  - `ctest -R autobot_executor_fault_drills_tests --output-on-failure`

## DoD Mapping
- Local-only drills, deterministic rules.
- No intentional real-exchange `429/418` generation.
- C6 is evaluated by local drill pass/fail.
