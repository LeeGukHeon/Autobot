# Exit State Contract

- Version: 2026-03-12
- Scope: current `model_alpha_v1` exit-state contract in `autobot/`
- Purpose: define the canonical exit-state document and its derived projections for `T23.2` Slice 2

## 1. Canonical Source

Current canonical runtime exit-state document is:

- `strategy.meta.model_exit_plan`

Current authoritative write path is:

- `autobot/strategy/model_alpha_v1.py`
  - `build_model_alpha_exit_plan_payload()`
- persisted first in:
  - `intents.meta_json.strategy.meta.model_exit_plan`

Operational rule:

- `positions`
- `risk_plans`
- `trade_journal.entry_meta.strategy.meta.model_exit_plan`

are projections or audit copies of the canonical plan, not the canonical writer.

## 2. Canonical Fields

The normalized canonical document now carries both:

- compatibility aliases used by current code
- explicit unit-safe canonical aliases for future migration

Canonical fields:

- `source`
- `version`
- `mode`
- `hold_bars`
- `bar_interval_ms`
- `timeout_delta_ms`
- `tp_ratio`
- `sl_ratio`
- `trailing_ratio`
- `expected_exit_fee_ratio`
- `expected_exit_slippage_bps`

Compatibility aliases retained in the same payload:

- `interval_ms`
- `tp_pct`
- `sl_pct`
- `trailing_pct`
- `expected_exit_fee_rate`

Important compatibility note:

- in `model_exit_plan`, legacy `*_pct` fields are still ratio values
  - example: `tp_pct=0.02` means `2%`
- in DB projections such as `positions.tp_json` and `risk_plans.tp.tp_pct`, `tp_pct/sl_pct` remain percent-points
  - example: `tp_pct=2.0` means `2%`
- current trailing projection is a second legacy exception
  - `positions.trailing_json.trail_pct` and `risk_plans.trailing.trail_pct` are still ratio values
  - example: `trail_pct=0.015` means `1.5%`

This mismatch is why future readers should prefer the canonical `*_ratio` fields when reading `model_exit_plan`.

## 3. Unit Naming Rules

Use these names for new fields:

- `_ratio`
  - unit interval ratio such as `0.02 == 2%`
- `_pct_points`
  - human display / operational percent-points such as `2.0 == 2%`
- `_bps`
  - basis points such as `25 == 0.25%`
- `_ts_ms`
  - absolute timestamp in milliseconds
- `_delta_ms`
  - duration in milliseconds

Current exception kept only for compatibility:

- `model_exit_plan.tp_pct`
- `model_exit_plan.sl_pct`
- `model_exit_plan.trailing_pct`
- `positions.trailing_json.trail_pct`
- `risk_plans.trailing.trail_pct`

These are legacy names and semantically ratios, not percent-points.

## 4. Projection Map

### Canonical write

- source:
  - `strategy.meta.model_exit_plan`
- written by:
  - `ModelAlphaStrategyV1`
- persisted in:
  - `intents.meta_json`

### In-memory strategy state

- source:
  - fill meta `model_exit_plan`
- reader:
  - `ModelAlphaStrategyV1.on_fill()`
- purpose:
  - restart-safe local exit behavior after entry fill

### Position projection

- target:
  - `positions.tp_json`
  - `positions.sl_json`
  - `positions.trailing_json`
- writer:
  - `build_position_record_from_model_exit_plan()`
- purpose:
  - compact operational projection for reconcile / resume
- unit shape:
  - `tp_pct/sl_pct`: percent-points
  - `trail_pct`: ratio

### Risk-plan projection

- target:
  - `risk_plans`
- writer:
  - `build_risk_plan_record_from_model_exit_plan()`
- purpose:
  - executable runtime state machine for managed exits
- unit shape:
  - `tp_pct/sl_pct`: percent-points
  - `trail_pct`: ratio
  - `timeout_ts_ms`: absolute timestamp

### Trade-journal audit copy

- target:
  - `trade_journal.entry_meta.strategy.meta.model_exit_plan`
- writer:
  - `activate_trade_journal_for_position()`
  - `_build_entry_meta_summary()`
- purpose:
  - audit / observability snapshot

### Dashboard summaries

- target:
  - `dashboard snapshot recent_intents`
  - `dashboard snapshot recent_trades`
  - `dashboard snapshot active_risk_plans`
- reader:
  - `autobot/dashboard_server.py`
- rule:
  - dashboard is read-only and must not synthesize canonical exit policy

## 5. Current Code Points

Normalized canonical alias helper:

- `autobot/common/model_exit_contract.py`

Current projection builders:

- `autobot/strategy/model_alpha_v1.py`
- `autobot/live/model_risk_plan.py`
- `autobot/live/trade_journal.py`

## 6. T23.2 Migration Direction

This slice does not change DB schema or service behavior.

It only makes the contract explicit:

- canonical runtime document: `strategy.meta.model_exit_plan`
- other locations: derived projections or audit copies
- compatibility aliases stay readable during migration
