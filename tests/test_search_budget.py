from __future__ import annotations

import json
from pathlib import Path

from autobot.models.search_budget import V4SearchBudgetPolicy, resolve_v4_search_budget


def test_resolve_v4_search_budget_defaults_to_compact_profile_for_scheduled_daily(tmp_path: Path) -> None:
    logs_root = tmp_path / "logs"
    logs_root.mkdir(parents=True, exist_ok=True)

    decision = resolve_v4_search_budget(
        project_root=tmp_path,
        logs_root=logs_root,
        registry_root=tmp_path / "registry",
        model_family="train_v4_crypto_cs",
        run_scope="scheduled_daily",
        requested_booster_sweep_trials=10,
        factor_block_selection_context={},
        cpcv_requested=False,
        policy=V4SearchBudgetPolicy(
            soft_disk_used_gb=10_000.0,
            hard_disk_used_gb=20_000.0,
        ),
    )

    assert decision["status"] == "default"
    assert decision["lane_class_requested"] == "promotion_eligible"
    assert decision["lane_class_effective"] == "promotion_eligible"
    assert decision["budget_contract_id"] == "v4_promotion_eligible_budget_v1"
    assert decision["promotion_eligible_contract"]["requested"] is True
    assert decision["promotion_eligible_contract"]["satisfied"] is True
    assert decision["promotion_eligible_contract"]["required_runtime_recommendation_profile"] == "compact"
    assert decision["applied"]["booster_sweep_trials"] == 10
    assert decision["applied"]["runtime_recommendation_profile"] == "compact"
    assert decision["markers"] == []
    assert decision["resource_state"]["project_used_gb"] >= 0.0


def test_resolve_v4_search_budget_defaults_to_compact_profile_for_split_history(tmp_path: Path) -> None:
    logs_root = tmp_path / "logs"
    logs_root.mkdir(parents=True, exist_ok=True)

    decision = resolve_v4_search_budget(
        project_root=tmp_path,
        logs_root=logs_root,
        registry_root=tmp_path / "registry",
        model_family="train_v4_crypto_cs",
        run_scope="scheduled_split_policy_history",
        requested_booster_sweep_trials=10,
        factor_block_selection_context={},
        cpcv_requested=False,
        policy=V4SearchBudgetPolicy(
            soft_disk_used_gb=10_000.0,
            hard_disk_used_gb=20_000.0,
        ),
    )

    assert decision["status"] == "default"
    assert decision["lane_class_requested"] == "promotion_eligible"
    assert decision["lane_class_effective"] == "promotion_eligible"
    assert decision["budget_contract_id"] == "v4_promotion_eligible_budget_v1"
    assert decision["promotion_eligible_contract"]["requested"] is True
    assert decision["promotion_eligible_contract"]["satisfied"] is True
    assert decision["promotion_eligible_contract"]["required_runtime_recommendation_profile"] == "compact"
    assert decision["applied"]["booster_sweep_trials"] == 10
    assert decision["applied"]["runtime_recommendation_profile"] == "compact"
    assert decision["markers"] == []


def test_resolve_v4_search_budget_reduces_trials_under_pressure(tmp_path: Path) -> None:
    logs_root = tmp_path / "logs"
    logs_root.mkdir(parents=True, exist_ok=True)
    (logs_root / "train_v4_report.json").write_text(
        json.dumps({"duration_sec": 8_000}),
        encoding="utf-8",
    )

    decision = resolve_v4_search_budget(
        project_root=tmp_path,
        logs_root=logs_root,
        registry_root=tmp_path / "registry",
        model_family="train_v4_crypto_cs",
        run_scope="scheduled_daily",
        requested_booster_sweep_trials=12,
        factor_block_selection_context={},
        cpcv_requested=False,
        policy=V4SearchBudgetPolicy(
            soft_disk_used_gb=0.0,
            hard_disk_used_gb=10_000.0,
            soft_wall_time_sec=7_200,
            hard_wall_time_sec=10_800,
            soft_booster_trial_cap=8,
            hard_booster_trial_cap=5,
        ),
    )

    assert decision["status"] == "throttled"
    assert decision["lane_class_requested"] == "promotion_eligible"
    assert decision["lane_class_effective"] == "scout"
    assert decision["budget_contract_id"] == "v4_promotion_eligible_budget_v1"
    assert decision["promotion_eligible_contract"]["requested"] is True
    assert decision["promotion_eligible_contract"]["satisfied"] is False
    assert decision["applied"]["booster_sweep_trials"] == 8
    assert decision["applied"]["runtime_recommendation_profile"] == "compact"
    assert "SOFT_DISK_BUDGET_PRESSURE" in decision["markers"]
    assert "SOFT_WALL_TIME_PRESSURE" in decision["markers"]
    assert "PROJECT_USED_GB_AT_OR_ABOVE_SOFT_THRESHOLD" in decision["reasons"]


def test_resolve_v4_search_budget_keeps_compact_profile_under_hard_pressure(tmp_path: Path) -> None:
    logs_root = tmp_path / "logs"
    logs_root.mkdir(parents=True, exist_ok=True)
    (logs_root / "train_v4_report.manual_daily.json").write_text(
        json.dumps({"duration_sec": 20_000}),
        encoding="utf-8",
    )

    decision = resolve_v4_search_budget(
        project_root=tmp_path,
        logs_root=logs_root,
        registry_root=tmp_path / "registry",
        model_family="train_v4_crypto_cs",
        run_scope="manual_daily",
        requested_booster_sweep_trials=12,
        factor_block_selection_context={},
        cpcv_requested=False,
        policy=V4SearchBudgetPolicy(
            soft_disk_used_gb=0.0,
            hard_disk_used_gb=0.0,
            soft_wall_time_sec=7_200,
            hard_wall_time_sec=10_800,
            soft_booster_trial_cap=8,
            hard_booster_trial_cap=5,
            soft_runtime_profile="compact",
            hard_runtime_profile="tiny",
        ),
    )

    assert decision["status"] == "throttled"
    assert decision["applied"]["booster_sweep_trials"] == 5
    assert decision["applied"]["runtime_recommendation_profile"] == "compact"
    assert "HARD_DISK_BUDGET_PRESSURE" in decision["markers"]
    assert "HARD_WALL_TIME_PRESSURE" in decision["markers"]


def test_resolve_v4_search_budget_enables_cpcv_auto_for_guarded_policy(tmp_path: Path) -> None:
    logs_root = tmp_path / "logs"
    logs_root.mkdir(parents=True, exist_ok=True)

    decision = resolve_v4_search_budget(
        project_root=tmp_path,
        logs_root=logs_root,
        registry_root=tmp_path / "registry",
        model_family="train_v4_crypto_cs",
        run_scope="scheduled_daily",
        requested_booster_sweep_trials=6,
        factor_block_selection_context={
            "applied": True,
            "resolution_source": "guarded_policy",
            "resolved_run_id": "selector-run",
        },
        cpcv_requested=False,
        policy=V4SearchBudgetPolicy(
            soft_disk_used_gb=10_000.0,
            hard_disk_used_gb=20_000.0,
        ),
    )

    assert decision["lane_class_requested"] == "promotion_eligible"
    assert decision["lane_class_effective"] == "scout"
    assert decision["promotion_eligible_contract"]["requested"] is True
    assert decision["promotion_eligible_contract"]["satisfied"] is False
    assert decision["applied"]["cpcv_lite_auto_enabled"] is True
    assert decision["applied"]["runtime_recommendation_profile"] == "compact"
    assert "GUARDED_POLICY_ACTIVE" in decision["markers"]
    assert "CPCV_LOAD_SHED_ACTIVE" in decision["markers"]


def test_resolve_v4_search_budget_uses_recent_experiment_ledger_summary(tmp_path: Path) -> None:
    logs_root = tmp_path / "logs"
    logs_root.mkdir(parents=True, exist_ok=True)
    summary_path = tmp_path / "registry" / "train_v4_crypto_cs" / "latest_experiment_ledger_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(
            {
                "records_considered": 4,
                "duplicate_candidate_rate": 0.5,
                "duplicate_candidate_streak": 2,
                "mean_duration_sec": 7_400,
            }
        ),
        encoding="utf-8",
    )

    decision = resolve_v4_search_budget(
        project_root=tmp_path,
        logs_root=logs_root,
        registry_root=tmp_path / "registry",
        model_family="train_v4_crypto_cs",
        run_scope="scheduled_daily",
        requested_booster_sweep_trials=12,
        factor_block_selection_context={},
        cpcv_requested=False,
        policy=V4SearchBudgetPolicy(
            soft_disk_used_gb=10_000.0,
            hard_disk_used_gb=20_000.0,
            soft_wall_time_sec=7_200,
            hard_wall_time_sec=10_800,
            soft_booster_trial_cap=8,
            hard_booster_trial_cap=5,
        ),
    )

    assert decision["applied"]["booster_sweep_trials"] == 8
    assert decision["applied"]["runtime_recommendation_profile"] == "compact"
    assert "LEDGER_SOFT_WALL_TIME_PRESSURE" in decision["markers"]
    assert "LEDGER_DUPLICATE_STREAK_PRESSURE" in decision["markers"]
    assert decision["experiment_ledger_context"]["available"] is True


def test_resolve_v4_search_budget_uses_scope_specific_train_report(tmp_path: Path) -> None:
    logs_root = tmp_path / "logs"
    logs_root.mkdir(parents=True, exist_ok=True)
    (logs_root / "train_v4_report.manual_daily.json").write_text(
        json.dumps({"duration_sec": 8_100}),
        encoding="utf-8",
    )
    (logs_root / "train_v4_report.json").write_text(
        json.dumps({"duration_sec": 10.0}),
        encoding="utf-8",
    )

    decision = resolve_v4_search_budget(
        project_root=tmp_path,
        logs_root=logs_root,
        registry_root=tmp_path / "registry",
        model_family="train_v4_crypto_cs",
        run_scope="manual_daily",
        requested_booster_sweep_trials=12,
        factor_block_selection_context={},
        cpcv_requested=False,
        policy=V4SearchBudgetPolicy(
            soft_disk_used_gb=10_000.0,
            hard_disk_used_gb=20_000.0,
            soft_wall_time_sec=7_200,
            hard_wall_time_sec=10_800,
            soft_booster_trial_cap=8,
            hard_booster_trial_cap=5,
        ),
    )

    assert decision["resource_state"]["previous_train_duration_sec"] == 8100.0
    assert decision["lane_class_requested"] == "scout"
    assert decision["lane_class_effective"] == "scout"
    assert decision["budget_contract_id"] == "v4_scout_budget_v1"
    assert decision["promotion_eligible_contract"]["requested"] is False
    assert decision["promotion_eligible_contract"]["satisfied"] is False
    assert decision["applied"]["runtime_recommendation_profile"] == "compact"
    assert "SOFT_WALL_TIME_PRESSURE" in decision["markers"]
