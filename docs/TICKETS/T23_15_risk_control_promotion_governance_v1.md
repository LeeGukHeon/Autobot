# T23-15 Risk-Control Promotion Governance v1

## Goal

Make execution risk-control readiness part of formal trainer promotion evidence,
instead of treating it as live-only runtime safety.

## Governance Rule

For non-duplicate candidates, trainer evidence now requires:

- `risk_control.status == ready`
- `risk_control.contract_status != invalid`
- `risk_control.live_gate.enabled == true`
- `risk_control.size_ladder.status == ready`
- `risk_control.online_adaptation.enabled == true`
- `risk_control.online_adaptation.martingale_enabled == true`
- `risk_control.weighting.density_ratio.mode != ""`

If any of these fail, trainer research evidence fails and downstream
certification can observe the failure through `trainer_research_evidence.json`.

## Artifacts

- `promotion_decision.json` now includes `risk_control_acceptance`
- `promotion.checks` now includes `risk_control_*` fields
- `trainer_research_evidence.json` now includes:
  - `risk_control_pass`
  - `risk_control`
  - `checks.risk_control_*`

## Downstream Effect

`scripts/candidate_acceptance.ps1` already consumes trainer research prior
through the top-level `pass` field. This change therefore makes risk-control
readiness part of the promotable evidence chain without changing the script
contract.
