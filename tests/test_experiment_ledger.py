from __future__ import annotations

import json
from pathlib import Path

from autobot.models.experiment_ledger import (
    append_experiment_ledger_record,
    build_experiment_ledger_record,
    build_recent_experiment_ledger_summary,
    load_experiment_ledger,
    load_latest_experiment_ledger_summary,
    write_latest_experiment_ledger_summary,
)


def test_experiment_ledger_record_and_summary_capture_duplicate_history(tmp_path: Path) -> None:
    run_dir = tmp_path / "registry" / "train_v4_crypto_cs" / "run-001"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "model.bin").write_bytes(b"abc")

    record = build_experiment_ledger_record(
        run_id="run-001",
        task="cls",
        status="candidate",
        duration_sec=321.5,
        run_dir=run_dir,
        search_budget_decision={
            "status": "throttled",
            "lane_class_requested": "promotion_eligible",
            "lane_class_effective": "scout",
            "budget_contract_id": "v4_promotion_eligible_budget_v1",
            "promotion_eligible_contract": {
                "requested": True,
                "satisfied": False,
            },
            "applied": {
                "booster_sweep_trials": 5,
                "runtime_recommendation_profile": "compact",
                "cpcv_lite_auto_enabled": True,
            },
            "markers": ["SOFT_WALL_TIME_PRESSURE"],
        },
        walk_forward={
            "summary": {"windows_run": 4},
            "compare_to_champion": {"comparable": True, "decision": "indeterminate"},
            "spa_like_window_test": {"comparable": True, "decision": "indeterminate"},
            "white_reality_check": {"comparable": False},
            "hansen_spa": {"comparable": False},
        },
        cpcv_lite={
            "enabled": True,
            "trigger": "guarded_policy",
            "summary": {"status": "partial", "folds_run": 2, "comparable_fold_count": 1},
        },
        factor_block_selection={
            "selection_mode": "guarded_auto",
            "summary": {"status": "trusted", "accepted_block_count": 6, "rejected_block_count": 2},
            "sample_support": {"weak_sample": False},
            "refit_support": {
                "summary": {
                    "status": "partial",
                    "optional_blocks_with_rows": 1,
                }
            },
        },
        factor_block_policy={
            "apply_pruned_feature_set": True,
            "summary": {"status": "stable"},
        },
        factor_block_selection_context={
            "applied": True,
            "resolution_source": "guarded_policy",
        },
        execution_acceptance={
            "status": "compared",
            "compare_to_champion": {"comparable": True, "decision": "candidate_edge"},
        },
        runtime_recommendations={"status": "ready"},
        promotion={"status": "candidate", "promotion_mode": "manual_gate", "reasons": ["MANUAL_PROMOTION_REQUIRED"]},
        duplicate_candidate=True,
        economic_objective_profile={
            "profile_id": "v4_shared_economic_objective_v1",
            "objective_family": "economic_return_first",
            "offline_compare": {"policy": "balanced_pareto_offline"},
            "execution_compare": {"policy": "balanced_pareto_execution"},
        },
    )

    assert record["duplicate_candidate"] is True
    assert record["factor_block_policy"]["apply_pruned_feature_set"] is True
    assert record["cpcv_lite"]["status"] == "partial"
    assert record["search_budget"]["lane_class_requested"] == "promotion_eligible"
    assert record["search_budget"]["lane_class_effective"] == "scout"
    assert record["search_budget"]["budget_contract_id"] == "v4_promotion_eligible_budget_v1"
    assert record["search_budget"]["promotion_eligible_satisfied"] is False
    assert record["economic_objective"]["profile_id"] == "v4_shared_economic_objective_v1"
    assert record["factor_block_selection"]["refit_support_status"] == "partial"
    assert record["factor_block_selection"]["optional_blocks_with_refit_rows"] == 1

    history = [record, dict(record, run_id="run-002"), dict(record, run_id="run-003", duplicate_candidate=False)]
    summary = build_recent_experiment_ledger_summary(history_records=history)

    assert summary["records_considered"] == 3
    assert summary["duplicate_candidate_rate"] == 0.666667
    assert summary["duplicate_candidate_streak"] == 0
    assert summary["guarded_pruning_applied_rate"] == 1.0
    assert summary["cpcv_partial_or_better_rate"] == 1.0


def test_experiment_ledger_history_and_latest_summary_round_trip(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    model_family = "train_v4_crypto_cs"

    path = append_experiment_ledger_record(
        registry_root=registry_root,
        model_family=model_family,
        record={"run_id": "run-a", "duplicate_candidate": False, "duration_sec": 100.0},
    )
    assert path is not None
    append_experiment_ledger_record(
        registry_root=registry_root,
        model_family=model_family,
        record={"run_id": "run-a", "duplicate_candidate": True, "duration_sec": 110.0},
    )
    append_experiment_ledger_record(
        registry_root=registry_root,
        model_family=model_family,
        record={"run_id": "run-b", "duplicate_candidate": True, "duration_sec": 120.0},
    )

    records = load_experiment_ledger(registry_root=registry_root, model_family=model_family)
    assert [item["run_id"] for item in records] == ["run-a", "run-b"]
    assert records[0]["duplicate_candidate"] is True

    summary = build_recent_experiment_ledger_summary(history_records=records)
    summary_path = write_latest_experiment_ledger_summary(
        registry_root=registry_root,
        model_family=model_family,
        run_id="run-b",
        summary=summary,
    )
    assert summary_path is not None

    latest = load_latest_experiment_ledger_summary(registry_root=registry_root, model_family=model_family)
    assert latest["updated_by_run_id"] == "run-b"
    assert latest["duplicate_candidate_streak"] == 2


def test_experiment_ledger_scope_isolated_files(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    model_family = "train_v4_crypto_cs"

    append_experiment_ledger_record(
        registry_root=registry_root,
        model_family=model_family,
        run_scope="manual_daily",
        record={"run_id": "manual-run-1", "run_scope": "manual_daily", "duplicate_candidate": False},
    )
    append_experiment_ledger_record(
        registry_root=registry_root,
        model_family=model_family,
        run_scope="scheduled_daily",
        record={"run_id": "scheduled-run-1", "run_scope": "scheduled_daily", "duplicate_candidate": True},
    )

    manual_records = load_experiment_ledger(registry_root=registry_root, model_family=model_family, run_scope="manual_daily")
    scheduled_records = load_experiment_ledger(registry_root=registry_root, model_family=model_family, run_scope="scheduled_daily")

    assert [item["run_id"] for item in manual_records] == ["manual-run-1"]
    assert [item["run_id"] for item in scheduled_records] == ["scheduled-run-1"]
