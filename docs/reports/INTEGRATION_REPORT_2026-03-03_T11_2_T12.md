# Integration Report (2026-03-03)

## Scope
- T11.2: C6 local fault-injection drill
- T12: live risk manager v1 (TP/SL/Trailing + risk plan persistence/recovery)

## Implemented
- C++:
  - `UpbitHttpClient::ITransport` injection path
  - fault transport test harness + D1~D5 drills
  - `OrderManager` private-WS factory injection for deterministic WS drop drill
- Python:
  - `risk_plans` persistence in `LiveStateStore`
  - `LiveRiskManager` + `risk_loop`
  - reconcile `attach_default_risk` now writes persistent default risk plans

## Test Execution (No Skip)
- Python:
  - command: `python -m pytest -q`
  - result: `90 passed in 5.37s`
- C++:
  - build: `cmake --build cpp/build_vcpkg --config Debug --target autobot_executor_unit_tests autobot_executor_ws_private_parsers_tests autobot_executor_ws_keepalive_scheduler_tests autobot_executor_fault_drills_tests`
  - run: `ctest -C Debug --output-on-failure` (workspace: `cpp/build_vcpkg`)
  - result: `4/4 passed`
    - `autobot_executor_unit_tests`
    - `autobot_executor_ws_private_parsers_tests`
    - `autobot_executor_ws_keepalive_scheduler_tests`
    - `autobot_executor_fault_drills_tests`

## C6 Drill Result
- Status: `PASS`
- Verified:
  - D1 `POST timeout -> GET(identifier) recover`
  - D2 `POST 5xx -> GET(identifier) recover`
  - D3 `429 group breaker` cooldown behavior
  - D4 `418 global breaker` cooldown behavior
  - D5 `WS drop -> degraded polling -> reconnect restore`

## Notes
- Fault injection is local/mock transport only.
- No intentional real-exchange rate-limit/banning induction was used.
