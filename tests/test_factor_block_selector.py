from __future__ import annotations

import json
from pathlib import Path

from autobot.models.factor_block_selector import (
    append_factor_block_selection_history,
    build_guarded_factor_block_policy,
    build_factor_block_selection_report,
    load_factor_block_selection_history,
    normalize_factor_block_selection_mode,
    resolve_selected_feature_columns_from_latest,
    v4_factor_block_registry,
    write_latest_guarded_factor_block_policy,
    write_latest_factor_block_selection_pointer,
)


def test_v4_factor_block_registry_is_stable_for_known_columns() -> None:
    registry = v4_factor_block_registry(
        feature_columns=(
            "logret_1",
            "one_m_count",
            "tf15m_ret_1",
            "m_trade_events",
            "btc_ret_1",
            "hour_sin",
            "price_trend_short",
            "oflow_v1_signed_volume_imbalance_1",
            "ctrend_v1_rsi_14",
            "mom_x_illiq",
        )
    )

    block_ids = [item.block_id for item in registry]
    assert block_ids == [
        "v3_base_core",
        "v3_one_m_core",
        "v3_high_tf_core",
        "v3_micro_core",
        "v4_spillover_breadth",
        "v4_periodicity",
        "v4_trend_volume",
        "v4_order_flow_panel_v1",
        "v4_ctrend_v1",
        "v4_interactions",
    ]


def test_resolve_selected_feature_columns_from_latest_falls_back_or_applies(tmp_path: Path) -> None:
    feature_columns = ("logret_1", "volume_z", "btc_ret_1", "hour_sin")
    selected, context = resolve_selected_feature_columns_from_latest(
        registry_root=tmp_path / "registry",
        model_family="train_v4_crypto_cs",
        mode="guarded_auto",
        all_feature_columns=feature_columns,
    )
    assert selected == feature_columns
    assert "MISSING_LATEST_FACTOR_BLOCK_POLICY" in context["reasons"]

    family_dir = tmp_path / "registry" / "train_v4_crypto_cs"
    family_dir.mkdir(parents=True, exist_ok=True)
    (family_dir / "latest_factor_block_policy.json").write_text(
        json.dumps(
            {
                "updated_by_run_id": "run-1",
                "apply_pruned_feature_set": True,
                "accepted_blocks": ["v3_base_core", "v4_spillover_breadth"],
                "selected_feature_columns": ["logret_1", "volume_z", "btc_ret_1"],
                "summary": {"status": "stable"},
            }
        ),
        encoding="utf-8",
    )
    selected, context = resolve_selected_feature_columns_from_latest(
        registry_root=tmp_path / "registry",
        model_family="train_v4_crypto_cs",
        mode="guarded_auto",
        all_feature_columns=feature_columns,
    )
    assert selected == ("logret_1", "volume_z", "btc_ret_1")
    assert context["applied"] is True
    assert context["resolved_run_id"] == "run-1"
    assert context["resolution_source"] == "guarded_policy"


def test_build_factor_block_selection_report_and_pointer_are_deterministic(tmp_path: Path) -> None:
    registry = v4_factor_block_registry(
        feature_columns=("logret_1", "one_m_count", "btc_ret_1", "hour_sin")
    )
    rows = [
        {"window_index": 0, "block_id": "v4_spillover_breadth", "delta_ev_net_top5": 0.002, "delta_precision_top5": 0.01, "coverage_cost_proxy": 0.05, "turnover_cost_proxy": 0.10},
        {"window_index": 1, "block_id": "v4_spillover_breadth", "delta_ev_net_top5": 0.001, "delta_precision_top5": 0.00, "coverage_cost_proxy": 0.04, "turnover_cost_proxy": 0.20},
        {"window_index": 0, "block_id": "v4_periodicity", "delta_ev_net_top5": -0.001, "delta_precision_top5": -0.01, "coverage_cost_proxy": 0.02, "turnover_cost_proxy": 0.05},
        {"window_index": 1, "block_id": "v4_periodicity", "delta_ev_net_top5": -0.001, "delta_precision_top5": -0.02, "coverage_cost_proxy": 0.02, "turnover_cost_proxy": 0.05},
    ]

    report = build_factor_block_selection_report(
        block_registry=registry,
        window_rows=rows,
        selection_mode="report_only",
        feature_columns=("logret_1", "one_m_count", "btc_ret_1", "hour_sin"),
        run_id="run-1",
    )

    assert normalize_factor_block_selection_mode("report_only") == "report_only"
    assert normalize_factor_block_selection_mode("guarded_auto") == "guarded_auto"
    assert report["summary"]["status"] == "report_only"
    assert "v3_base_core" in report["accepted_blocks"]
    assert "v3_one_m_core" in report["accepted_blocks"]
    assert "v4_spillover_breadth" in report["accepted_blocks"]
    assert "v4_periodicity" in report["rejected_blocks"]

    pointer_path = write_latest_factor_block_selection_pointer(
        registry_root=tmp_path / "registry",
        model_family="train_v4_crypto_cs",
        run_id="run-1",
        report=report,
    )
    assert pointer_path is not None
    payload = json.loads(pointer_path.read_text(encoding="utf-8"))
    assert payload["run_id"] == "run-1"
    assert "v4_spillover_breadth" in payload["accepted_blocks"]


def test_guarded_auto_policy_history_can_activate_pruned_feature_set(tmp_path: Path) -> None:
    registry = v4_factor_block_registry(
        feature_columns=("logret_1", "one_m_count", "btc_ret_1", "hour_sin")
    )
    registry_root = tmp_path / "registry"
    model_family = "train_v4_crypto_cs"

    runs = (
        ("run-1", 0.003, -0.001),
        ("run-2", 0.002, -0.001),
        ("run-3", 0.002, -0.002),
        ("run-4", 0.001, -0.001),
    )
    for run_id, spillover_delta, periodicity_delta in runs:
        report = build_factor_block_selection_report(
            block_registry=registry,
            window_rows=[
                {
                    "window_index": 0,
                    "block_id": "v4_spillover_breadth",
                    "delta_ev_net_top5": spillover_delta,
                    "delta_precision_top5": 0.01,
                    "coverage_cost_proxy": 0.04,
                    "turnover_cost_proxy": 0.10,
                },
                {
                    "window_index": 1,
                    "block_id": "v4_spillover_breadth",
                    "delta_ev_net_top5": spillover_delta,
                    "delta_precision_top5": 0.01,
                    "coverage_cost_proxy": 0.04,
                    "turnover_cost_proxy": 0.10,
                },
                {
                    "window_index": 0,
                    "block_id": "v4_periodicity",
                    "delta_ev_net_top5": periodicity_delta,
                    "delta_precision_top5": -0.01,
                    "coverage_cost_proxy": 0.02,
                    "turnover_cost_proxy": 0.05,
                },
                {
                    "window_index": 1,
                    "block_id": "v4_periodicity",
                    "delta_ev_net_top5": periodicity_delta,
                    "delta_precision_top5": -0.01,
                    "coverage_cost_proxy": 0.02,
                    "turnover_cost_proxy": 0.05,
                },
            ],
            selection_mode="report_only",
            feature_columns=("logret_1", "one_m_count", "btc_ret_1", "hour_sin"),
            run_id=run_id,
        )
        assert append_factor_block_selection_history(
            registry_root=registry_root,
            model_family=model_family,
            report=report,
        ) is not None

    history = load_factor_block_selection_history(registry_root=registry_root, model_family=model_family)
    assert len(history) == 4
    policy = build_guarded_factor_block_policy(block_registry=registry, history_records=history)
    assert policy["summary"]["status"] == "stable"
    assert policy["apply_pruned_feature_set"] is True
    assert "v4_spillover_breadth" in policy["accepted_blocks"]
    assert "v4_periodicity" in policy["rejected_blocks"]

    policy_path = write_latest_guarded_factor_block_policy(
        registry_root=registry_root,
        model_family=model_family,
        run_id="run-4",
        policy=policy,
    )
    assert policy_path is not None

    selected, context = resolve_selected_feature_columns_from_latest(
        registry_root=registry_root,
        model_family=model_family,
        mode="guarded_auto",
        all_feature_columns=("logret_1", "one_m_count", "btc_ret_1", "hour_sin"),
    )
    assert selected == ("logret_1", "one_m_count", "btc_ret_1")
    assert context["applied"] is True
    assert context["policy_status"] == "stable"


def test_factor_block_selector_scope_isolated_policy_files(tmp_path: Path) -> None:
    family_dir = tmp_path / "registry" / "train_v4_crypto_cs"
    family_dir.mkdir(parents=True, exist_ok=True)
    (family_dir / "latest_factor_block_policy.manual_daily.json").write_text(
        json.dumps(
            {
                "updated_by_run_id": "manual-run",
                "apply_pruned_feature_set": True,
                "accepted_blocks": ["v3_base_core"],
                "selected_feature_columns": ["logret_1"],
                "summary": {"status": "stable"},
            }
        ),
        encoding="utf-8",
    )

    selected_manual, context_manual = resolve_selected_feature_columns_from_latest(
        registry_root=tmp_path / "registry",
        model_family="train_v4_crypto_cs",
        mode="guarded_auto",
        run_scope="manual_daily",
        all_feature_columns=("logret_1", "btc_ret_1"),
    )
    selected_scheduled, context_scheduled = resolve_selected_feature_columns_from_latest(
        registry_root=tmp_path / "registry",
        model_family="train_v4_crypto_cs",
        mode="guarded_auto",
        run_scope="scheduled_daily",
        all_feature_columns=("logret_1", "btc_ret_1"),
    )

    assert selected_manual == ("logret_1",)
    assert context_manual["applied"] is True
    assert selected_scheduled == ("logret_1", "btc_ret_1")
    assert "MISSING_LATEST_FACTOR_BLOCK_POLICY" in context_scheduled["reasons"]
