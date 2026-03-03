# T06.1 - Live Ops Completion v1

## Goal
- Complete operational gaps on top of T06 base:
  - apply-path cancel execution with safety guards
  - polling daemon for continuous sync
  - intent wiring from exchange-discovered bot orders
  - default-risk attachment for imported unknown positions
  - roadmap alignment for T06/T07/T08 boundaries

## Scope Implemented
- CLI:
  - `python -m autobot.cli live reconcile --apply`
  - `python -m autobot.cli live reconcile --apply --allow-cancel-external`
  - `python -m autobot.cli live run --duration-sec 120`
- Reconcile:
  - emits cancel actions for policy `cancel`
  - blocks external auto-cancel unless config + CLI double opt-in
  - writes inferred intents (`INFERRED_FROM_EXCHANGE`) for bot orders without intent linkage
  - supports `attach_default_risk` DB write for `tp/sl/trailing`
- Daemon:
  - periodic polling loop (`accounts` + `orders/open`)
  - cycle checkpoint (`last_sync`)
  - startup reconcile and halt propagation
- Upbit private REST:
  - `DELETE /v1/order` (`cancel_order`)

## Config Additions
- `live.startup.allow_cancel_external_orders: false`
- `live.default_risk.sl_pct: 2.0`
- `live.default_risk.tp_pct: 3.0`
- `live.default_risk.trailing_enabled: false`

## Safety Rules
- Dry-run is default for `live reconcile`.
- `--apply` is required to execute cancel actions.
- External cancel requires:
  - config: `allow_cancel_external_orders: true`
  - CLI: `--allow-cancel-external`

## Notes
- Private WS (`myOrder/myAsset`) remains in T07 scope.
- Real order manager/state machine execution remains in T08 scope.
