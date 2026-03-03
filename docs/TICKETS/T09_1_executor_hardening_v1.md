# T09.1 - C++ Executor Hardening v1

## Scope
- SSOT: `cpp/src/executor/*`
- Minimal Python integration update: `autobot/live/daemon.py`

## Implemented
- Request signing hardening:
  - Added `upbit/request_builder.*` (`OrderedParams` single-source builder).
  - GET/DELETE hash source uses unencoded ordered query.
  - POST hash source is derived from ordered body params; body is JSON with fixed content type.
- Number formatting hardening:
  - Added `upbit/number_string.*` for locale-stable, non-scientific, trimmed numeric strings.
- Identifier/recovery policy:
  - Added `upbit/recovery_policy.*`.
  - POST create-order requests are single-attempt (`allow_retry=false`).
  - Ambiguous create outcomes route to `GET /v1/order?identifier=...`.
  - Existing identifier reuse is rejected (`identifier_reuse_forbidden_new_identifier_required`).
- State persistence hardening:
  - Added `state/executor_state_store.*`.
  - Lock directory + atomic temp write/replace + `.bak` fallback + schema version.
  - Stores identifier status/meta (`NEW/POST_SENT/CONFIRMED/FAILED/UNKNOWN/CANCELED`).
- Rate-limit hardening:
  - Added missing-header conservative throttle path (`ObserveMissingRemainingReq`).
  - 429 applies group breaker with exponential cooldown growth.
  - 418 applies global breaker metadata.
- Endpoint guards:
  - `GET/DELETE /v1/order`: if both uuid/identifier are given, uuid is sent.
  - `DELETE /v1/orders/uuids`: mutual exclusion (`uuids[]` vs `identifiers[]`), required non-empty, max 20.
  - Order-test cancel remains local ack in `UpbitRestClient`.
- Event contract stabilization:
  - `order_manager` now emits `event_name` contract fields:
    - `ORDER_ACCEPTED`, `ORDER_STATE`, `FILL`, `CANCEL_RESULT`, `ERROR`.
  - Python daemon accepts order events by payload `event_name` as well as legacy `event_type`.

## Tests
- Added C++ unit executable:
  - `cpp/src/executor/tests/executor_unit_tests.cpp`
  - validates:
    - query-hash string contract for ordered array params
    - POST body/hash-source coupling
    - recovery policy behavior for timeout/429
    - order-test cancel guard behavior
    - `orders/uuids` constraint validation
- Added Python test:
  - `tests/test_live_daemon.py::test_apply_executor_event_supports_payload_event_name_contract`

## Build/Run Validation
- `cmake --build cpp/build_vcpkg --config Release --target autobot_executor`
- `cmake --build cpp/build_vcpkg --config Release --target autobot_executor_unit_tests`
- `cpp/build_vcpkg/src/executor/Release/autobot_executor_unit_tests.exe`

