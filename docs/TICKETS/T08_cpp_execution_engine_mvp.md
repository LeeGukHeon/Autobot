# T08 - C++ Execution Engine MVP (gRPC)

## Goal
- Put C++ into the real live execution path with minimal disruption.
- Keep Python as strategy/risk/state/ops owner.
- Route order intent and cancel commands through a dedicated C++ executor process.

## Scope Implemented
- `proto/autobot.proto` added:
  - `OrderIntent`, `SubmitResult`, `CancelRequest`, `Event`, `Health*`
  - `ExecutionService` RPCs:
    - `SubmitIntent`
    - `Cancel`
    - `StreamEvents`
    - `GetSnapshot`
    - `Health`
- Python gRPC gateway:
  - `autobot/execution/grpc_gateway.py`
  - methods:
    - `ping()`
    - `submit_intent(...)`
    - `submit_test(...)`
    - `cancel(...)`
    - `stream_events()`
    - `get_snapshot()`
- Live daemon integration:
  - `run_live_sync_daemon_with_executor_events(...)`
  - executor events are normalized and applied via existing `apply_private_ws_event(...)` handlers.
  - `live.sync.use_executor_ws` config path added.
- CLI additions:
  - `autobot exec ping`
  - `autobot exec submit-test --market ... --side ... --price ... --volume ...`
- C++ executor skeleton:
  - `cpp/src/executor/*`
  - gRPC server + idempotency cache + event stream + mock Upbit REST adapter
  - build gate: `AUTOBOT_BUILD_EXECUTOR=ON`

## Runtime Notes
- Python private WS and executor WS should not be enabled together:
  - `live.sync.use_private_ws=true`
  - `live.sync.use_executor_ws=true`
  - above combo is rejected at runtime.
- Current C++ MVP uses `order_test_mode=true` by default.
- C++ REST adapter currently mocks network calls; method contracts are ready for live Upbit REST binding.

## Build Notes
- Python stub generation:
  - `scripts/gen_proto_python.ps1`
- C++ build (example):
  - `cmake -S cpp -B cpp/build -DAUTOBOT_BUILD_EXECUTOR=ON`
  - `cmake --build cpp/build --config Release`

## Validation
- Added daemon test for executor event path:
  - `tests/test_live_daemon.py::test_live_daemon_executor_events_updates_state`
