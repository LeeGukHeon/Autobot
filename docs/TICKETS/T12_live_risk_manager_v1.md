# T12 Live Risk Manager v1

## Goal
- Add restart-safe live risk automation for TP/SL/Trailing.
- Keep execution path `limit-only` with replace-based convergence.

## Scope
- New persistent `risk_plans` table in `LiveStateStore` (SQLite).
- Python risk engine:
  - `autobot/risk/models.py`
  - `autobot/risk/live_risk_manager.py`
  - `autobot/live/risk_loop.py`
- Reconcile integration:
  - `unknown_positions_policy=attach_default_risk` now creates persistent default risk plan rows.
- Replace policy:
  - exit timeout -> `ReplaceOrder` (remain_only) until `replace_max`.

## Runtime Contract
- Trigger source: ticker last trade price (`trade_price`).
- Trigger rules:
  - TP: `last_price >= tp_price`.
  - SL: `last_price <= sl_price`.
  - Trailing: watermark update on new highs, trigger on drawdown `trail_pct`.
- State transitions:
  - `ACTIVE/TRIGGERED -> EXITING -> CLOSED`.

## Persistence
- `risk_plans` stores plan parameters, trailing watermark, current exit order linkage, replace counters, and timestamps.
- `LiveStateStore.export_state()` includes `risk_plans` for restart audits.

## Tests
- `tests/test_live_risk_manager.py`
  - TP trigger -> exit submit.
  - trailing watermark update + trigger.
  - timeout replace -> done state close recovery.
  - ticker/executor risk-loop glue behavior.
- `tests/test_live_reconcile.py`
  - default-risk reconcile path now validates risk plan persistence.
