# Risk Control Safety Layer Redesign

- Status: redesign note
- Operational authority: no
- Current risk/runtime truth:
  - `docs/RUNTIME_EXECUTION_FINDINGS_AND_ACTION_PLAN_2026-03-23.md`

## Goal

Reposition `risk_control` from a second alpha selector into a safety executor.

The system should keep:

- learned size control
- online post-trade adaptation
- martingale / e-process halt logic
- telemetry for governance and audit

The system should stop using `risk_control` as:

- a static pre-trade score threshold that owns final entry permission


## Problem

The previous contract allowed `risk_control.live_gate` to veto intents in
strategy / backtest / paper / live.

That created a double-ownership path:

1. `selection` chose candidate rows
2. `trade_action` chose `hold` vs `risk` and notional
3. `risk_control` vetoed entry again using another learned threshold

In practice this made `risk_control` behave like a second alpha model.
The observed failure mode was:

- selection events were present
- selected rows were present
- final intents were zero
- skip reason concentrated in `RISK_CONTROL_BELOW_THRESHOLD`


## Design Principles

1. Alpha ownership stays in `selection + trade_action`.
2. Safety ownership stays in `risk_control + execution + rollout + breaker`.
3. A safety layer may shrink or stop risk, but it should not become the main
   source of sparsity for normal entry flow.
4. Hard vetoes should be explicit and interpretable.


## Target Architecture

### 1. Selection

`selection_policy` still decides which rows are worth considering.

### 2. Trade Action

`trade_action` still decides:

- `hold` vs `risk`
- runtime exit template
- requested notional multiplier

### 3. Risk Control

`risk_control` becomes a safety executor with three responsibilities:

- `size_ladder`
- `online_adaptation`
- `martingale` / halt telemetry

`risk_control.live_gate` remains in the contract for telemetry and backward
compatibility, but is disabled by design for newly trained models.

### 4. Explicit Live Safety

Hard safety remains in explicit layers that already exist:

- trade gate
- micro order policy
- live admissibility
- canary slot limits
- rollout contract
- breakers


## Contract Changes

Newly trained `runtime_recommendations["risk_control"]` should advertise:

- `operating_mode = "safety_executor_only_v1"`
- `live_gate.enabled = false`
- `live_gate.mode = "safety_executor_only_v1"`
- `live_gate.disabled_reason_code = "RISK_CONTROL_STATIC_GATE_DISABLED_BY_DESIGN"`

Important: `selected_threshold` and related diagnostics are retained for
analysis, but they are no longer treated as normal pre-trade ownership.


## Governance Changes

Promotion / governance should require:

- risk control present
- contract ready
- size ladder ready
- online adaptation enabled
- martingale enabled
- density-ratio weighting active

Promotion / governance should not require:

- `live_gate.enabled == true`

This lets the model keep a valid safety executor contract while removing static
entry veto ownership.


## Runtime Effects

For newly trained models:

- strategy / paper / backtest no longer lose intents because of the static
  `RISK_CONTROL_BELOW_THRESHOLD` veto
- size ladder still clamps requested multipliers
- online adaptation and martingale halt still work in live

For legacy models:

- behavior is unchanged unless the model is retrained and republishes
  `runtime_recommendations.json`


## Implementation Scope

This redesign changes:

- `autobot/models/execution_risk_control.py`
- `autobot/models/train_v4_governance.py`
- `autobot/models/train_v4_artifacts.py`
- `autobot/strategy/model_alpha_runtime_contract.py`

This redesign intentionally does not change:

- `trade_action` ownership
- trade gate
- micro order policy
- live admissibility
- canary / rollout / breaker contracts


## Follow-Up Work

The current redesign removes static veto ownership, but a stricter follow-up can
move more safety into explicit white-box constraints:

- expected net edge after cost
- expected ES budget
- tail probability budget
- regime / liquidity no-trade bands

That follow-up is optional and should be treated as a phase-2 refactor.

