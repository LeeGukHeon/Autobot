# T06 - Live StateStore + Exchange Reconciliation v1

## Goal
- Add a restart-safe live state layer before real-order execution (T07).
- Treat Upbit account/open-order snapshot as exchange SoT, and TP/SL/trailing/order intent metadata as bot-local SoT.
- Prevent duplicate order actions after restart with deterministic reconciliation.

## Scope Implemented
- New package: `autobot/live/`
  - `state_store.py`: SQLite StateStore (WAL) with minimal schema
  - `reconcile.py`: startup/manual reconciliation logic
  - `identifier.py`: bot order identifier helpers
- Upbit private REST extension:
  - `GET /v1/orders/open`
  - `GET /v1/order`
- CLI expansion:
  - `python -m autobot.cli live status`
  - `python -m autobot.cli live reconcile --dry-run`
  - `python -m autobot.cli live export-state`

## StateStore
- DB path default: `data/state/live_state.db`
- Tables:
  - `bot_meta`
  - `positions`
  - `orders`
  - `intents`
  - `checkpoints`
  - `run_locks`
- Write policy:
  - upsert/mark methods commit immediately
  - reconcile checkpoint stored as `last_reconcile`

## Reconciliation Rules
- Bot order matching:
  - 1) `identifier` starts with `<prefix>-<bot_id>-`
  - 2) or exchange `uuid` already exists in local open orders
- Unknown open-order policy:
  - `halt` | `ignore` | `cancel` (cancel is planned-action only in T06)
- Unknown position policy:
  - `halt` | `import_as_unmanaged` | `attach_default_risk`
- Local-only open orders:
  - lookup `/v1/order`, else mark closed (`cancel`) conservatively

## Config (base.yaml)
- Added `live.*` defaults:
  - `live.enabled`
  - `live.bot_id`
  - `live.state.db_path`
  - `live.state.run_lock`
  - `live.startup.unknown_open_orders_policy`
  - `live.startup.unknown_positions_policy`
  - `live.sync.poll_interval_sec`
  - `live.sync.use_private_ws`
  - `live.orders.identifier_prefix`

## Notes
- T06 does not place/cancel real orders yet.
- Private WS (`myOrder/myAsset`) and live execution are deferred to T07.
