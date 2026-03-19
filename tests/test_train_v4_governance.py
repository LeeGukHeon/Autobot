from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from autobot.models.train_v4_governance import (
    build_trainer_research_evidence_from_promotion_v4,
    manual_promotion_decision_v4,
)


def test_manual_promotion_decision_v4_includes_risk_control_governance_pass(tmp_path: Path) -> None:
    family_dir = tmp_path / "registry" / "train_v4_crypto_cs"
    family_dir.mkdir(parents=True, exist_ok=True)
    (family_dir / "champion.json").write_text('{"run_id":"champion-1"}\n', encoding="utf-8")
    options = SimpleNamespace(
        registry_root=tmp_path / "registry",
        model_family="train_v4_crypto_cs",
        execution_acceptance_enabled=True,
    )
    promotion = manual_promotion_decision_v4(
        options=options,
        run_id="candidate-1",
        walk_forward={
            "summary": {"windows_run": 4},
            "compare_to_champion": {"policy": "balanced_pareto_offline", "comparable": True, "decision": "candidate_edge"},
        },
        execution_acceptance={
            "status": "compared",
            "compare_to_champion": {"policy": "paired_sortino_lpm_execution_v1", "comparable": True, "decision": "candidate_edge"},
        },
        runtime_recommendations={
            "risk_control": {
                "policy": "execution_risk_control_hoeffding_v1",
                "status": "ready",
                "contract_status": "ok",
                "operating_mode": "safety_executor_only_v1",
                "live_gate": {"enabled": False, "mode": "safety_executor_only_v1"},
                "size_ladder": {"status": "ready"},
                "weighting": {"density_ratio": {"mode": "latest_window_logistic_density_ratio_crossfit_v1", "classifier_status": "ready_crossfit"}},
                "online_adaptation": {"enabled": True, "martingale_enabled": True},
                "selected_threshold": 2.0,
                "selected_coverage": 32,
            }
        },
    )

    assert promotion["risk_control_acceptance"]["pass"] is True
    assert promotion["checks"]["risk_control_governance_pass"] is True
    assert "RISK_CONTROL_GOVERNANCE_PASS" in promotion["reasons"]


def test_build_trainer_research_evidence_from_promotion_v4_fails_when_risk_control_governance_fails() -> None:
    evidence = build_trainer_research_evidence_from_promotion_v4(
        promotion={
            "checks": {
                "existing_champion_present": True,
                "walk_forward_present": True,
                "walk_forward_windows_run": 4,
                "balanced_pareto_comparable": True,
                "balanced_pareto_candidate_edge": True,
                "execution_acceptance_enabled": True,
                "execution_acceptance_present": True,
                "execution_balanced_pareto_comparable": True,
                "execution_balanced_pareto_candidate_edge": True,
                "risk_control_required": True,
                "risk_control_present": True,
                "risk_control_ready": True,
                "risk_control_operating_mode": "safety_executor_only_v1",
                "risk_control_live_gate_enabled": False,
                "risk_control_size_ladder_ready": False,
                "risk_control_online_adaptation_enabled": True,
                "risk_control_martingale_enabled": True,
                "risk_control_density_ratio_active": True,
                "risk_control_governance_pass": False,
            },
            "research_acceptance": {
                "compare_to_champion": {"decision": "candidate_edge"},
                "walk_forward_summary": {"windows_run": 4},
            },
            "execution_acceptance": {
                "status": "compared",
                "compare_to_champion": {"decision": "candidate_edge"},
            },
            "risk_control_acceptance": {
                "policy": "execution_risk_control_hoeffding_v1",
                "status": "ready",
                "contract_status": "ok",
                "operating_mode": "safety_executor_only_v1",
                "live_gate_enabled": False,
                "size_ladder_status": "missing",
                "online_adaptation_enabled": True,
                "martingale_enabled": True,
                "density_ratio_mode": "latest_window_logistic_density_ratio_crossfit_v1",
                "density_ratio_classifier_status": "ready_crossfit",
                "reasons": ["RISK_CONTROL_LIVE_GATE_DISABLED_BY_DESIGN", "RISK_CONTROL_SIZE_LADDER_NOT_READY", "RISK_CONTROL_GOVERNANCE_FAIL"],
            },
        }
    )

    assert evidence["pass"] is False
    assert evidence["risk_control_pass"] is False
    assert evidence["checks"]["risk_control_governance_pass"] is False
    assert "RISK_CONTROL_SIZE_LADDER_NOT_READY" in evidence["reasons"]
    assert evidence["risk_control"]["density_ratio_classifier_status"] == "ready_crossfit"
