from __future__ import annotations

import json
from pathlib import Path

from autobot.models.offpolicy_evaluation import build_execution_dr_ope_report, write_execution_dr_ope_report


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def test_build_execution_dr_ope_report_estimates_logged_and_greedy_policies(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_jsonl(
        run_dir / "opportunity_log.jsonl",
        [
            {
                "opportunity_id": "opp-1",
                "chosen_action": "PASSIVE_MAKER",
                "chosen_action_propensity": 1.0,
                "expected_edge_bps": 20.0,
                "realized_outcome_json": {"realized_pnl_bps": 10.0},
            },
            {
                "opportunity_id": "opp-2",
                "chosen_action": "NO_TRADE",
                "chosen_action_propensity": 1.0,
                "expected_edge_bps": 5.0,
                "realized_outcome_json": {"realized_pnl_bps": 0.0},
            },
        ],
    )
    _write_jsonl(
        run_dir / "counterfactual_action_log.jsonl",
        [
            {
                "opportunity_id": "opp-1",
                "action_propensity": 1.0,
                "action_payload": {"action_code": "PASSIVE_MAKER", "predicted_utility_bps": 8.0},
            },
            {
                "opportunity_id": "opp-1",
                "action_propensity": 0.0,
                "action_payload": {"action_code": "JOIN", "predicted_utility_bps": 15.0},
            },
            {
                "opportunity_id": "opp-1",
                "action_propensity": 0.0,
                "action_payload": {"action_code": "NO_TRADE", "predicted_utility_bps": 0.0},
            },
            {
                "opportunity_id": "opp-2",
                "action_propensity": 0.0,
                "action_payload": {"action_code": "PASSIVE_MAKER", "predicted_utility_bps": -5.0},
            },
            {
                "opportunity_id": "opp-2",
                "action_propensity": 1.0,
                "action_payload": {"action_code": "NO_TRADE", "predicted_utility_bps": 0.0},
            },
        ],
    )

    report = build_execution_dr_ope_report(run_dir=run_dir, execution_contract={})

    assert report["policy"] == "execution_dr_ope_v1"
    assert report["sample_count"] == 2
    assert report["policy_reports"]["logged_policy"]["dr_estimate_bps"] == 5.0
    assert report["policy_reports"]["greedy_predicted_utility_policy"]["dr_estimate_bps"] == 7.5
    assert report["policy_reports"]["greedy_predicted_utility_policy"]["support_rate"] == 0.5


def test_build_execution_dr_ope_report_uses_execution_twin_fallback_when_predicted_utility_missing(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_jsonl(
        run_dir / "opportunity_log.jsonl",
        [
            {
                "opportunity_id": "opp-1",
                "chosen_action": "JOIN",
                "chosen_action_propensity": 1.0,
                "expected_edge_bps": 30.0,
                "realized_outcome_json": {"realized_pnl_bps": 12.0},
            }
        ],
    )
    _write_jsonl(
        run_dir / "counterfactual_action_log.jsonl",
        [
            {"opportunity_id": "opp-1", "action_propensity": 1.0, "action_payload": {"action_code": "JOIN"}},
            {"opportunity_id": "opp-1", "action_propensity": 0.0, "action_payload": {"action_code": "NO_TRADE"}},
        ],
    )
    execution_contract = {
        "policy": "live_execution_contract_v2",
        "execution_twin": {
            "policy": "personalized_execution_twin_v1",
            "price_mode_stats": {
                "JOIN": {
                    "full_fill_probability": 0.5,
                    "expected_shortfall_bps": 3.0,
                }
            },
        },
    }

    report = build_execution_dr_ope_report(run_dir=run_dir, execution_contract=execution_contract)

    assert report["policy_reports"]["logged_policy"]["dm_estimate_bps"] == 12.0
    assert report["policy_reports"]["logged_policy"]["dr_estimate_bps"] == 12.0


def test_write_execution_dr_ope_report_writes_default_path(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_jsonl(run_dir / "opportunity_log.jsonl", [])
    _write_jsonl(run_dir / "counterfactual_action_log.jsonl", [])
    path = write_execution_dr_ope_report(run_dir=run_dir, execution_contract={})
    assert path == run_dir / "execution_ope_report.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["policy"] == "execution_dr_ope_v1"
