from __future__ import annotations

import numpy as np

from autobot.models.entry_boundary import build_risk_calibrated_entry_boundary, evaluate_entry_boundary


def test_build_and_evaluate_entry_boundary_contract() -> None:
    contract = build_risk_calibrated_entry_boundary(
        final_rank_score=np.asarray([0.7, 0.6, 0.2, 0.1], dtype=np.float64),
        final_expected_return=np.asarray([0.10, 0.06, -0.02, -0.05], dtype=np.float64),
        final_expected_es=np.asarray([0.02, 0.03, 0.06, 0.08], dtype=np.float64),
        final_tradability=np.asarray([0.8, 0.7, 0.2, 0.1], dtype=np.float64),
        final_uncertainty=np.asarray([0.03, 0.04, 0.08, 0.10], dtype=np.float64),
        final_alpha_lcb=np.asarray([0.05, 0.01, -0.16, -0.23], dtype=np.float64),
        realized_return=np.asarray([0.08, 0.02, -0.09, -0.12], dtype=np.float64),
        severe_loss_bps=0.05,
        target_max_severe_loss_rate=0.5,
    )

    allowed = evaluate_entry_boundary(
        row={
            "final_rank_score": 0.7,
            "final_expected_return": 0.10,
            "final_expected_es": 0.02,
            "final_tradability": 0.8,
            "final_uncertainty": 0.03,
            "final_alpha_lcb": 0.05,
        },
        contract=contract,
    )
    blocked = evaluate_entry_boundary(
        row={
            "final_rank_score": 0.1,
            "final_expected_return": -0.05,
            "final_expected_es": 0.08,
            "final_tradability": 0.1,
            "final_uncertainty": 0.10,
            "final_alpha_lcb": -0.23,
        },
        contract=contract,
    )

    assert contract["policy"] == "risk_calibrated_entry_boundary_v1"
    assert allowed["enabled"] is True
    assert allowed["allowed"] is True
    assert blocked["allowed"] is False
    assert "ENTRY_BOUNDARY_ALPHA_LCB_NOT_POSITIVE" in blocked["reason_codes"]
