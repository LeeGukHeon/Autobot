from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from autobot.models.registry import load_json
from autobot.models.train_v5_fusion import (
    TrainV5FusionOptions,
    resume_v5_fusion_tail,
    train_and_register_v5_fusion,
)
from autobot.models.v5_expert_runtime_export import parse_operating_date_to_ts_ms


def _write_expert_run(
    *,
    root: Path,
    family: str,
    run_id: str,
    trainer: str,
    snapshot_id: str,
    rows: list[dict[str, object]],
) -> Path:
    run_dir = root / family / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    table_path = run_dir / "expert_prediction_table.parquet"
    pl.DataFrame(rows).write_parquet(table_path)
    (run_dir / "train_config.yaml").write_text(
        json.dumps(
            {
                "trainer": trainer,
                "model_family": family,
                "data_platform_ready_snapshot_id": snapshot_id,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "runtime_recommendations.json").write_text(
        json.dumps(
            (
                {
                    "status": f"{trainer}_runtime_ready",
                    "source_family": family,
                    "data_platform_ready_snapshot_id": snapshot_id,
                    "exit": {
                        "version": 1,
                        "recommended_exit_mode": "risk",
                        "recommended_exit_mode_source": "test_panel_runtime",
                        "recommended_exit_mode_reason_code": "TEST_PANEL_RUNTIME",
                    },
                    "execution": {
                        "recommended_price_mode": "JOIN",
                        "recommended_timeout_bars": 2,
                        "recommended_replace_max": 1,
                        "recommendation_source": "test_panel_runtime",
                    },
                    "risk_control": {
                        "status": "not_required",
                        "contract_status": "not_required",
                        "operating_mode": "test_panel_runtime",
                    },
                    "trade_action": {"status": "ready", "policy": "test_trade_action"},
                }
                if trainer == "v5_panel_ensemble"
                else {
                "status": f"{trainer}_runtime_ready",
                "source_family": family,
                "data_platform_ready_snapshot_id": snapshot_id,
                }
            ),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return table_path


def _base_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for idx in range(18):
        ts_ms = 1_774_569_600_000 + (idx * 60_000)
        split = "train" if idx < 10 else ("valid" if idx < 14 else "test")
        y_reg = 0.01 * idx
        y_cls = 1 if y_reg > 0.05 else 0
        rows.append(
            {
                "market": "KRW-BTC",
                "ts_ms": ts_ms,
                "split": split,
                "y_cls": y_cls,
                "y_reg": y_reg,
            }
        )
    return rows


def _runtime_rows_for(date_prefix: str) -> list[dict[str, object]]:
    start_ts_ms = int(parse_operating_date_to_ts_ms(date_prefix) or 0)
    end_ts_ms = int(parse_operating_date_to_ts_ms(date_prefix, end_of_day=True) or 0)
    return [
        {
            "market": "KRW-BTC",
            "ts_ms": start_ts_ms,
            "split": "runtime",
            "y_cls": 1,
            "y_reg": 0.1,
        },
        {
            "market": "KRW-BTC",
            "ts_ms": end_ts_ms,
            "split": "runtime",
            "y_cls": 1,
            "y_reg": 0.12,
        },
    ]


def _tradability_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    payload: list[dict[str, object]] = []
    for row in rows:
        y_reg = float(row["y_reg"])
        payload.append(
            {
                **row,
                "tradability_prob": 0.72 if y_reg >= 0.0 else 0.48,
                "fill_within_deadline_prob": 0.76,
                "expected_shortfall_bps": abs(y_reg) * 10.0 + 0.5,
                "adverse_tolerance_prob": 0.81,
                "tradability_uncertainty": 0.04,
            }
        )
    return payload


def _write_runtime_export(*, table_path: Path, rows: list[dict[str, object]]) -> Path:
    table_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(table_path)
    return table_path


def _write_runtime_export_metadata(path: Path, *, start: str, end: str, coverage_dates: list[str], anchor_export_path: str = "") -> None:
    metadata_path = path.parent / "metadata.json"
    metadata_path.write_text(
        json.dumps(
            {
                "start": start,
                "end": end,
                "coverage_dates": coverage_dates,
                "coverage_start_date": coverage_dates[0] if coverage_dates else "",
                "coverage_end_date": coverage_dates[-1] if coverage_dates else "",
                "coverage_start_ts_ms": int(parse_operating_date_to_ts_ms(coverage_dates[0]) or 0) if coverage_dates else 0,
                "coverage_end_ts_ms": int(parse_operating_date_to_ts_ms(coverage_dates[-1], end_of_day=True) or 0) if coverage_dates else 0,
                "window_timezone": "Asia/Seoul",
                "selected_markets": ["KRW-BTC"],
                "requested_selected_markets": ["KRW-BTC"],
                "selected_markets_source": "acceptance_common_runtime_universe",
                "fallback_reason": "",
                "anchor_alignment_complete": bool(anchor_export_path),
                "anchor_export_path": anchor_export_path,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def test_train_v5_fusion_writes_core_contract_artifacts(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    snapshot_id = "snapshot-fusion-001"
    base_rows = _base_rows()
    panel_rows = []
    sequence_rows = []
    lob_rows = []
    for row in base_rows:
        y_reg = float(row["y_reg"])
        panel_rows.append(
            {
                **row,
                "final_rank_score": 0.2 + (y_reg * 2.0),
                "final_expected_return": y_reg * 0.9,
                "final_expected_es": abs(y_reg) * 0.3,
                "final_tradability": 0.8,
                "final_uncertainty": 0.05,
                "final_alpha_lcb": (y_reg * 0.9) - (abs(y_reg) * 0.3) - 0.05,
            }
        )
        sequence_rows.append(
            {
                **row,
                "support_level": "strict_full" if row["split"] != "train" else "reduced_context",
                "directional_probability_primary": 0.3 + (0.01 * (int(row["ts_ms"]) % 7)),
                "sequence_uncertainty_primary": 0.04,
                "return_quantile_h3_q10": y_reg * 0.5,
                "return_quantile_h3_q50": y_reg * 0.8,
                "return_quantile_h3_q90": y_reg * 1.1,
                "regime_embedding_0": 0.1,
            }
        )
        lob_rows.append(
            {
                **row,
                "support_level": "strict_full" if row["split"] != "train" else "reduced_context",
                "micro_alpha_1s": y_reg * 0.2,
                "micro_alpha_5s": y_reg * 0.4,
                "micro_alpha_30s": y_reg * 0.6,
                "micro_alpha_60s": y_reg * 0.7,
                "micro_uncertainty": 0.03,
                "adverse_excursion_30s": abs(y_reg) * 0.2,
            }
        )
    panel_path = _write_expert_run(
        root=registry_root,
        family="train_v5_panel_ensemble",
        run_id="panel-run-001",
        trainer="v5_panel_ensemble",
        snapshot_id=snapshot_id,
        rows=panel_rows,
    )
    sequence_path = _write_expert_run(
        root=registry_root,
        family="train_v5_sequence",
        run_id="sequence-run-001",
        trainer="v5_sequence",
        snapshot_id=snapshot_id,
        rows=sequence_rows,
    )
    lob_path = _write_expert_run(
        root=registry_root,
        family="train_v5_lob",
        run_id="lob-run-001",
        trainer="v5_lob",
        snapshot_id=snapshot_id,
        rows=lob_rows,
    )
    tradability_path = _write_expert_run(
        root=registry_root,
        family="train_v5_tradability",
        run_id="tradability-run-001",
        trainer="v5_tradability",
        snapshot_id=snapshot_id,
        rows=_tradability_rows(base_rows),
    )

    options = TrainV5FusionOptions(
        panel_input_path=panel_path,
        sequence_input_path=sequence_path,
        lob_input_path=lob_path,
        tradability_input_path=tradability_path,
        registry_root=registry_root,
        logs_root=tmp_path / "logs",
        model_family="train_v5_fusion",
        quote="KRW",
        start="2026-03-27",
        end="2026-03-27",
        seed=7,
        stacker_family="linear",
    )
    result = train_and_register_v5_fusion(options)

    assert result.run_dir.exists()
    assert result.fusion_model_contract_path.exists()
    assert result.predictor_contract_path.exists()
    assert result.entry_boundary_contract_path.exists()
    assert result.walk_forward_report_path.exists()
    assert result.promotion_path.exists()
    assert (result.run_dir / "fusion_input_contract.json").exists()
    assert load_json(result.run_dir / "train_config.yaml")["trainer"] == "v5_fusion"
    fusion_contract = load_json(result.fusion_model_contract_path)
    assert fusion_contract["policy"] == "v5_fusion_v1"
    assert fusion_contract["input_experts"]["panel"]["run_id"] == "panel-run-001"
    assert fusion_contract["input_experts"]["tradability"]["trainer"] == "v5_tradability"
    input_contract = load_json(result.run_dir / "fusion_input_contract.json")
    assert input_contract["snapshot_id"] == snapshot_id
    assert input_contract["label_anchor"] == "panel"
    assert input_contract["label_contract_source"] == "train_v5_panel_ensemble"
    assert input_contract["target_alignment_policy"] == "panel_anchor_only"
    assert input_contract["auxiliary_experts"] == ["sequence", "lob", "tradability"]
    assert input_contract["panel_label_columns"]["y_cls"] == "y_cls"
    assert input_contract["runtime_coverage_policy"] == "auxiliary_experts_full_window_required"
    assert input_contract["runtime_coverage_summary"] == {}
    assert input_contract["inputs"]["sequence"]["support_level_counts"]["strict_full"] > 0
    assert "sequence_support_score" in input_contract["feature_contract"]["feature_columns"]
    assert "sequence_support_level" not in input_contract["feature_contract"]["feature_columns"]
    assert load_json(result.predictor_contract_path)["final_rank_score_field"] == "final_rank_score"
    runtime_recommendations = load_json(result.run_dir / "runtime_recommendations.json")
    assert runtime_recommendations["decision_contract_version"] == "v5_post_model_contract_v1"
    assert runtime_recommendations["contract_owner_family"] == "train_v5_fusion"
    assert runtime_recommendations["contract_seed_family"] == "train_v5_panel_ensemble"
    assert "sequence_backbone_name" in runtime_recommendations
    assert "lob_backbone_name" in runtime_recommendations
    assert "tradability_source_run_id" in runtime_recommendations
    assert runtime_recommendations["exit"]["recommended_exit_mode"] == "risk"
    assert runtime_recommendations["exit"]["runtime_source_mode"] == "fusion_owned_panel_seeded"
    assert runtime_recommendations["execution"]["recommended_price_mode"] == "JOIN"
    assert runtime_recommendations["risk_control"]["operating_mode"] == "test_panel_runtime"
    report = load_json(result.train_report_path)
    assert report["data_platform_ready_snapshot_id"] == snapshot_id
    assert report["resumed"] is False
    assert float(report["tail_duration_sec"]) >= 0.0
    artifact_status = load_json(result.run_dir / "artifact_status.json")
    assert artifact_status["tail_context_written"] is True
    assert artifact_status["runtime_recommendations_complete"] is True
    assert artifact_status["governance_artifacts_complete"] is True
    assert (tmp_path / "registry" / "train_v5_fusion" / "latest.json").exists()
    assert not (tmp_path / "registry" / "latest.json").exists()


def test_train_v5_fusion_supports_regime_moe_stacker(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    snapshot_id = "snapshot-fusion-regime-moe-001"
    base_rows = _base_rows()
    panel_rows = []
    sequence_rows = []
    lob_rows = []
    for idx, row in enumerate(base_rows):
        y_reg = float(row["y_reg"])
        panel_rows.append(
            {
                **row,
                "final_rank_score": 0.2 + (y_reg * 2.0),
                "final_expected_return": y_reg * 0.9,
                "final_expected_es": abs(y_reg) * 0.3,
                "final_tradability": 0.8,
                "final_uncertainty": 0.05,
                "final_alpha_lcb": (y_reg * 0.9) - (abs(y_reg) * 0.3) - 0.05,
            }
        )
        regime_value = -1.0 if idx < 9 else 1.0
        sequence_rows.append(
            {
                **row,
                "support_level": "strict_full",
                "directional_probability_primary": 0.45 + (0.02 * (idx % 3)),
                "sequence_uncertainty_primary": 0.04,
                "return_quantile_h3_q10": y_reg * 0.5,
                "return_quantile_h3_q50": y_reg * 0.8,
                "return_quantile_h3_q90": y_reg * 1.1,
                "regime_embedding_0": regime_value,
                "regime_embedding_1": regime_value * 0.5,
            }
        )
        lob_rows.append(
            {
                **row,
                "support_level": "strict_full",
                "micro_alpha_1s": y_reg * 0.2,
                "micro_alpha_5s": y_reg * 0.4,
                "micro_alpha_30s": y_reg * 0.6,
                "micro_alpha_60s": y_reg * 0.7,
                "micro_uncertainty": 0.03,
                "adverse_excursion_30s": abs(y_reg) * 0.2,
            }
        )
    panel_path = _write_expert_run(
        root=registry_root,
        family="train_v5_panel_ensemble",
        run_id="panel-run-regime-moe-001",
        trainer="v5_panel_ensemble",
        snapshot_id=snapshot_id,
        rows=panel_rows,
    )
    sequence_path = _write_expert_run(
        root=registry_root,
        family="train_v5_sequence",
        run_id="sequence-run-regime-moe-001",
        trainer="v5_sequence",
        snapshot_id=snapshot_id,
        rows=sequence_rows,
    )
    lob_path = _write_expert_run(
        root=registry_root,
        family="train_v5_lob",
        run_id="lob-run-regime-moe-001",
        trainer="v5_lob",
        snapshot_id=snapshot_id,
        rows=lob_rows,
    )
    tradability_path = _write_expert_run(
        root=registry_root,
        family="train_v5_tradability",
        run_id="tradability-run-regime-moe-001",
        trainer="v5_tradability",
        snapshot_id=snapshot_id,
        rows=_tradability_rows(base_rows),
    )
    options = TrainV5FusionOptions(
        panel_input_path=panel_path,
        sequence_input_path=sequence_path,
        lob_input_path=lob_path,
        tradability_input_path=tradability_path,
        registry_root=registry_root,
        logs_root=tmp_path / "logs",
        model_family="train_v5_fusion",
        quote="KRW",
        start="2026-03-27",
        end="2026-03-27",
        seed=7,
        stacker_family="regime_moe",
    )

    result = train_and_register_v5_fusion(options)

    fusion_contract = load_json(result.fusion_model_contract_path)
    runtime_recommendations = load_json(result.run_dir / "runtime_recommendations.json")
    assert fusion_contract["stacker_family"] == "regime_moe"
    assert fusion_contract["gating_policy"] == "sequence_regime_embedding_nearest_centroid_v1"
    assert fusion_contract["regime_cluster_count"] >= 1
    assert runtime_recommendations["fusion_stacker_family"] == "regime_moe"
    assert runtime_recommendations["fusion_gating_policy"] == "sequence_regime_embedding_nearest_centroid_v1"


def test_train_v5_fusion_fails_on_snapshot_mismatch(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    base_rows = _base_rows()
    panel_path = _write_expert_run(
        root=registry_root,
        family="train_v5_panel_ensemble",
        run_id="panel-run-001",
        trainer="v5_panel_ensemble",
        snapshot_id="snapshot-a",
        rows=[{**row, "final_rank_score": 0.5, "final_expected_return": 0.1, "final_expected_es": 0.02, "final_tradability": 0.8, "final_uncertainty": 0.05, "final_alpha_lcb": 0.03} for row in base_rows],
    )
    sequence_path = _write_expert_run(
        root=registry_root,
        family="train_v5_sequence",
        run_id="sequence-run-001",
        trainer="v5_sequence",
        snapshot_id="snapshot-b",
        rows=[{**row, "support_level": "strict_full", "directional_probability_primary": 0.5, "sequence_uncertainty_primary": 0.04} for row in base_rows],
    )
    lob_path = _write_expert_run(
        root=registry_root,
        family="train_v5_lob",
        run_id="lob-run-001",
        trainer="v5_lob",
        snapshot_id="snapshot-a",
        rows=[{**row, "support_level": "strict_full", "micro_alpha_1s": 0.1, "micro_alpha_5s": 0.1, "micro_alpha_30s": 0.1, "micro_uncertainty": 0.03} for row in base_rows],
    )
    tradability_path = _write_expert_run(
        root=registry_root,
        family="train_v5_tradability",
        run_id="tradability-run-001",
        trainer="v5_tradability",
        snapshot_id="snapshot-a",
        rows=_tradability_rows(base_rows),
    )
    options = TrainV5FusionOptions(
        panel_input_path=panel_path,
        sequence_input_path=sequence_path,
        lob_input_path=lob_path,
        tradability_input_path=tradability_path,
        registry_root=registry_root,
        logs_root=tmp_path / "logs",
        model_family="train_v5_fusion",
        quote="KRW",
        start="2026-03-27",
        end="2026-03-27",
        seed=7,
    )
    with pytest.raises(ValueError, match="same non-empty data_platform_ready_snapshot_id"):
        train_and_register_v5_fusion(options)


def test_train_v5_fusion_fails_when_panel_runtime_contract_lacks_top_level_docs(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    snapshot_id = "snapshot-fusion-missing-001"
    base_rows = _base_rows()
    panel_path = _write_expert_run(
        root=registry_root,
        family="train_v5_panel_ensemble",
        run_id="panel-run-missing-001",
        trainer="v5_panel_ensemble",
        snapshot_id=snapshot_id,
        rows=[{**row, "final_rank_score": 0.5, "final_expected_return": 0.1, "final_expected_es": 0.02, "final_tradability": 0.8, "final_uncertainty": 0.05, "final_alpha_lcb": 0.03} for row in base_rows],
    )
    sequence_path = _write_expert_run(
        root=registry_root,
        family="train_v5_sequence",
        run_id="sequence-run-missing-001",
        trainer="v5_sequence",
        snapshot_id=snapshot_id,
        rows=[{**row, "support_level": "strict_full", "directional_probability_primary": 0.5, "sequence_uncertainty_primary": 0.04} for row in base_rows],
    )
    lob_path = _write_expert_run(
        root=registry_root,
        family="train_v5_lob",
        run_id="lob-run-missing-001",
        trainer="v5_lob",
        snapshot_id=snapshot_id,
        rows=[{**row, "support_level": "strict_full", "micro_alpha_1s": 0.1, "micro_alpha_5s": 0.1, "micro_alpha_30s": 0.1, "micro_uncertainty": 0.03} for row in base_rows],
    )
    tradability_path = _write_expert_run(
        root=registry_root,
        family="train_v5_tradability",
        run_id="tradability-run-missing-001",
        trainer="v5_tradability",
        snapshot_id=snapshot_id,
        rows=_tradability_rows(base_rows),
    )
    (panel_path.parent / "runtime_recommendations.json").write_text(
        json.dumps(
            {
                "status": "v5_panel_ensemble_runtime_ready",
                "source_family": "train_v5_panel_ensemble",
                "data_platform_ready_snapshot_id": snapshot_id,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    options = TrainV5FusionOptions(
        panel_input_path=panel_path,
        sequence_input_path=sequence_path,
        lob_input_path=lob_path,
        tradability_input_path=tradability_path,
        registry_root=registry_root,
        logs_root=tmp_path / "logs",
        model_family="train_v5_fusion",
        quote="KRW",
        start="2026-03-27",
        end="2026-03-27",
        seed=7,
    )
    with pytest.raises(ValueError, match="FUSION_RUNTIME_RECOMMENDATION_TOP_LEVEL_MISSING"):
        train_and_register_v5_fusion(options)


def test_train_v5_fusion_backfills_missing_trade_action_doc(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    snapshot_id = "snapshot-fusion-trade-action-backfill-001"
    base_rows = _base_rows()
    panel_path = _write_expert_run(
        root=registry_root,
        family="train_v5_panel_ensemble",
        run_id="panel-run-trade-action-backfill-001",
        trainer="v5_panel_ensemble",
        snapshot_id=snapshot_id,
        rows=[{**row, "final_rank_score": 0.5, "final_expected_return": 0.1, "final_expected_es": 0.02, "final_tradability": 0.8, "final_uncertainty": 0.05, "final_alpha_lcb": 0.03} for row in base_rows],
    )
    sequence_path = _write_expert_run(
        root=registry_root,
        family="train_v5_sequence",
        run_id="sequence-run-trade-action-backfill-001",
        trainer="v5_sequence",
        snapshot_id=snapshot_id,
        rows=[{**row, "support_level": "strict_full", "directional_probability_primary": 0.5, "sequence_uncertainty_primary": 0.04} for row in base_rows],
    )
    lob_path = _write_expert_run(
        root=registry_root,
        family="train_v5_lob",
        run_id="lob-run-trade-action-backfill-001",
        trainer="v5_lob",
        snapshot_id=snapshot_id,
        rows=[{**row, "support_level": "strict_full", "micro_alpha_1s": 0.1, "micro_alpha_5s": 0.1, "micro_alpha_30s": 0.1, "micro_uncertainty": 0.03} for row in base_rows],
    )
    tradability_path = _write_expert_run(
        root=registry_root,
        family="train_v5_tradability",
        run_id="tradability-run-trade-action-backfill-001",
        trainer="v5_tradability",
        snapshot_id=snapshot_id,
        rows=_tradability_rows(base_rows),
    )
    (panel_path.parent / "runtime_recommendations.json").write_text(
        json.dumps(
            {
                "status": "v5_panel_ensemble_runtime_ready",
                "source_family": "train_v5_panel_ensemble",
                "data_platform_ready_snapshot_id": snapshot_id,
                "exit": {
                    "recommended_exit_mode": "risk",
                    "recommended_exit_mode_source": "test_panel_runtime",
                    "recommended_exit_mode_reason_code": "TEST_PANEL_RUNTIME",
                },
                "execution": {
                    "recommended_price_mode": "JOIN",
                    "recommended_timeout_bars": 2,
                    "recommended_replace_max": 1,
                },
                "risk_control": {
                    "policy": "test_risk_control",
                    "operating_mode": "test_panel_runtime",
                },
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    result = train_and_register_v5_fusion(
        TrainV5FusionOptions(
            panel_input_path=panel_path,
            sequence_input_path=sequence_path,
            lob_input_path=lob_path,
            tradability_input_path=tradability_path,
            registry_root=registry_root,
            logs_root=tmp_path / "logs",
            model_family="train_v5_fusion",
            quote="KRW",
            start="2026-03-27",
            end="2026-03-27",
            seed=7,
        )
    )

    runtime_recommendations = load_json(result.run_dir / "runtime_recommendations.json")
    assert runtime_recommendations["trade_action"]["policy"] == "fusion_advisory_trade_action_backfill_v1"


def test_train_v5_fusion_uses_panel_as_canonical_label_anchor(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    snapshot_id = "snapshot-fusion-anchor-001"
    base_rows = _base_rows()
    panel_path = _write_expert_run(
        root=registry_root,
        family="train_v5_panel_ensemble",
        run_id="panel-run-anchor-001",
        trainer="v5_panel_ensemble",
        snapshot_id=snapshot_id,
        rows=[
            {
                **row,
                "final_rank_score": 0.2 + (float(row["y_reg"]) * 2.0),
                "final_expected_return": float(row["y_reg"]) * 0.9,
                "final_expected_es": abs(float(row["y_reg"])) * 0.3,
                "final_tradability": 0.8,
                "final_uncertainty": 0.05,
                "final_alpha_lcb": (float(row["y_reg"]) * 0.9) - (abs(float(row["y_reg"])) * 0.3) - 0.05,
            }
            for row in base_rows
        ],
    )
    sequence_rows = []
    lob_rows = []
    for row in base_rows:
        flipped_cls = 0 if int(row["y_cls"]) == 1 else 1
        shifted_reg = float(row["y_reg"]) + 0.5
        sequence_rows.append(
            {
                **row,
                "y_cls": flipped_cls,
                "y_reg": shifted_reg,
                "support_level": "strict_full",
                "directional_probability_primary": 0.55,
                "sequence_uncertainty_primary": 0.04,
            }
        )
        lob_rows.append(
            {
                **row,
                "y_cls": flipped_cls,
                "y_reg": shifted_reg,
                "support_level": "strict_full",
                "micro_alpha_1s": 0.1,
                "micro_alpha_5s": 0.12,
                "micro_alpha_30s": 0.15,
                "micro_uncertainty": 0.03,
            }
        )
    sequence_path = _write_expert_run(
        root=registry_root,
        family="train_v5_sequence",
        run_id="sequence-run-anchor-001",
        trainer="v5_sequence",
        snapshot_id=snapshot_id,
        rows=sequence_rows,
    )
    lob_path = _write_expert_run(
        root=registry_root,
        family="train_v5_lob",
        run_id="lob-run-anchor-001",
        trainer="v5_lob",
        snapshot_id=snapshot_id,
        rows=lob_rows,
    )
    tradability_path = _write_expert_run(
        root=registry_root,
        family="train_v5_tradability",
        run_id="tradability-run-anchor-001",
        trainer="v5_tradability",
        snapshot_id=snapshot_id,
        rows=_tradability_rows(base_rows),
    )

    options = TrainV5FusionOptions(
        panel_input_path=panel_path,
        sequence_input_path=sequence_path,
        lob_input_path=lob_path,
        tradability_input_path=tradability_path,
        registry_root=registry_root,
        logs_root=tmp_path / "logs",
        model_family="train_v5_fusion",
        quote="KRW",
        start="2026-03-27",
        end="2026-03-27",
        seed=7,
        stacker_family="linear",
    )

    result = train_and_register_v5_fusion(options)
    input_contract = load_json(result.run_dir / "fusion_input_contract.json")
    assert result.run_dir.exists()
    assert input_contract["label_anchor"] == "panel"
    assert input_contract["target_alignment_policy"] == "panel_anchor_only"


def test_resume_v5_fusion_tail_reuses_existing_artifacts(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    snapshot_id = "snapshot-fusion-002"
    base_rows = _base_rows()
    panel_path = _write_expert_run(
        root=registry_root,
        family="train_v5_panel_ensemble",
        run_id="panel-run-002",
        trainer="v5_panel_ensemble",
        snapshot_id=snapshot_id,
        rows=[{**row, "final_rank_score": 0.4, "final_expected_return": 0.1, "final_expected_es": 0.02, "final_tradability": 0.8, "final_uncertainty": 0.05, "final_alpha_lcb": 0.03} for row in base_rows],
    )
    sequence_path = _write_expert_run(
        root=registry_root,
        family="train_v5_sequence",
        run_id="sequence-run-002",
        trainer="v5_sequence",
        snapshot_id=snapshot_id,
        rows=[{**row, "support_level": "strict_full", "directional_probability_primary": 0.5, "sequence_uncertainty_primary": 0.04} for row in base_rows],
    )
    lob_path = _write_expert_run(
        root=registry_root,
        family="train_v5_lob",
        run_id="lob-run-002",
        trainer="v5_lob",
        snapshot_id=snapshot_id,
        rows=[{**row, "support_level": "strict_full", "micro_alpha_1s": 0.1, "micro_alpha_5s": 0.1, "micro_alpha_30s": 0.1, "micro_uncertainty": 0.03} for row in base_rows],
    )
    tradability_path = _write_expert_run(
        root=registry_root,
        family="train_v5_tradability",
        run_id="tradability-run-002",
        trainer="v5_tradability",
        snapshot_id=snapshot_id,
        rows=_tradability_rows(base_rows),
    )
    options = TrainV5FusionOptions(
        panel_input_path=panel_path,
        sequence_input_path=sequence_path,
        lob_input_path=lob_path,
        tradability_input_path=tradability_path,
        registry_root=registry_root,
        logs_root=tmp_path / "logs",
        model_family="train_v5_fusion",
        quote="KRW",
        start="2026-03-27",
        end="2026-03-27",
        seed=7,
    )
    result = train_and_register_v5_fusion(options)
    before = load_json(result.train_report_path)
    resumed = resume_v5_fusion_tail(run_dir=result.run_dir)
    after = load_json(resumed.train_report_path)
    assert resumed.run_id == result.run_id
    assert after["resumed"] is True
    assert after["data_platform_ready_snapshot_id"] == before["data_platform_ready_snapshot_id"]
    assert (result.run_dir / "entry_boundary_contract.json").exists()


def test_train_v5_fusion_uses_runtime_input_bundle_for_runtime_dataset(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    snapshot_id = "snapshot-fusion-runtime-001"
    base_rows = _base_rows()
    panel_path = _write_expert_run(
        root=registry_root,
        family="train_v5_panel_ensemble",
        run_id="panel-run-runtime-001",
        trainer="v5_panel_ensemble",
        snapshot_id=snapshot_id,
        rows=[{**row, "final_rank_score": 0.4, "final_expected_return": 0.1, "final_expected_es": 0.02, "final_tradability": 0.8, "final_uncertainty": 0.05, "final_alpha_lcb": 0.03} for row in base_rows],
    )
    sequence_path = _write_expert_run(
        root=registry_root,
        family="train_v5_sequence",
        run_id="sequence-run-runtime-001",
        trainer="v5_sequence",
        snapshot_id=snapshot_id,
        rows=[{**row, "support_level": "strict_full", "directional_probability_primary": 0.5, "sequence_uncertainty_primary": 0.04} for row in base_rows],
    )
    lob_path = _write_expert_run(
        root=registry_root,
        family="train_v5_lob",
        run_id="lob-run-runtime-001",
        trainer="v5_lob",
        snapshot_id=snapshot_id,
        rows=[{**row, "support_level": "strict_full", "micro_alpha_1s": 0.1, "micro_alpha_5s": 0.1, "micro_alpha_30s": 0.1, "micro_uncertainty": 0.03} for row in base_rows],
    )
    tradability_path = _write_expert_run(
        root=registry_root,
        family="train_v5_tradability",
        run_id="tradability-run-runtime-001",
        trainer="v5_tradability",
        snapshot_id=snapshot_id,
        rows=_tradability_rows(base_rows),
    )
    panel_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_panel_ensemble" / "panel-run-runtime-001" / "_runtime_exports" / "2026-03-28__2026-03-28" / "expert_prediction_table.parquet",
        rows=[{**row, "final_rank_score": 0.41, "final_expected_return": 0.11, "final_expected_es": 0.02, "final_tradability": 0.8, "final_uncertainty": 0.05, "final_alpha_lcb": 0.04} for row in _runtime_rows_for("2026-03-28")],
    )
    sequence_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_sequence" / "sequence-run-runtime-001" / "_runtime_exports" / "2026-03-28__2026-03-28" / "expert_prediction_table.parquet",
        rows=[{**row, "support_level": "strict_full", "directional_probability_primary": 0.51, "sequence_uncertainty_primary": 0.04} for row in _runtime_rows_for("2026-03-28")],
    )
    lob_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_lob" / "lob-run-runtime-001" / "_runtime_exports" / "2026-03-28__2026-03-28" / "expert_prediction_table.parquet",
        rows=[{**row, "support_level": "strict_full", "micro_alpha_1s": 0.11, "micro_alpha_5s": 0.11, "micro_alpha_30s": 0.11, "micro_uncertainty": 0.03} for row in _runtime_rows_for("2026-03-28")],
    )
    tradability_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_tradability" / "tradability-run-runtime-001" / "_runtime_exports" / "2026-03-28__2026-03-28" / "expert_prediction_table.parquet",
        rows=_tradability_rows(_runtime_rows_for("2026-03-28")),
    )
    options = TrainV5FusionOptions(
        panel_input_path=panel_path,
        sequence_input_path=sequence_path,
        lob_input_path=lob_path,
        tradability_input_path=tradability_path,
        panel_runtime_input_path=panel_runtime,
        sequence_runtime_input_path=sequence_runtime,
        lob_runtime_input_path=lob_runtime,
        tradability_runtime_input_path=tradability_runtime,
        registry_root=registry_root,
        logs_root=tmp_path / "logs",
        model_family="train_v5_fusion",
        quote="KRW",
        start="2026-03-27",
        end="2026-03-27",
        runtime_start="2026-03-28",
        runtime_end="2026-03-28",
        seed=7,
    )
    result = train_and_register_v5_fusion(options)
    runtime_contract = load_json(result.run_dir / "fusion_runtime_input_contract.json")
    tail_context = load_json(result.run_dir / "expert_tail_context.json")
    assert runtime_contract["runtime_window"]["start"] == "2026-03-28"
    assert runtime_contract["runtime_window"]["end"] == "2026-03-28"
    assert runtime_contract["common_runtime_markets"] == ["KRW-BTC"]
    assert runtime_contract["common_runtime_universe_id"].startswith("common_runtime_universe_")
    assert runtime_contract["runtime_rows_after_date_filter"] == 2
    assert runtime_contract["runtime_coverage_policy"] == "auxiliary_experts_full_window_required"
    assert runtime_contract["runtime_coverage_summary"]["common_runtime_market_count"] == 1
    assert runtime_contract["runtime_coverage_summary"]["common_runtime_markets"] == ["KRW-BTC"]
    assert runtime_contract["runtime_coverage_summary"]["experts"]["sequence"]["missing_rows"] == 0
    assert runtime_contract["runtime_coverage_summary"]["experts"]["lob"]["missing_rows"] == 0
    assert runtime_contract["runtime_coverage_summary"]["experts"]["tradability"]["missing_rows"] == 0
    assert tail_context["panel_runtime_input_path"] == str(panel_runtime)
    assert tail_context["sequence_runtime_input_path"] == str(sequence_runtime)
    assert tail_context["lob_runtime_input_path"] == str(lob_runtime)
    assert tail_context["tradability_runtime_input_path"] == str(tradability_runtime)
    assert tail_context["runtime_window_id"] == "2026-03-28__2026-03-28"
    feature_manifest = load_json(result.train_report_path)
    assert feature_manifest["runtime_dataset_root"].endswith("runtime_feature_dataset")


def test_train_v5_fusion_fails_on_runtime_input_window_gap(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    snapshot_id = "snapshot-fusion-runtime-002"
    base_rows = _base_rows()
    panel_path = _write_expert_run(
        root=registry_root,
        family="train_v5_panel_ensemble",
        run_id="panel-run-runtime-002",
        trainer="v5_panel_ensemble",
        snapshot_id=snapshot_id,
        rows=[{**row, "final_rank_score": 0.4, "final_expected_return": 0.1, "final_expected_es": 0.02, "final_tradability": 0.8, "final_uncertainty": 0.05, "final_alpha_lcb": 0.03} for row in base_rows],
    )
    sequence_path = _write_expert_run(
        root=registry_root,
        family="train_v5_sequence",
        run_id="sequence-run-runtime-002",
        trainer="v5_sequence",
        snapshot_id=snapshot_id,
        rows=[{**row, "support_level": "strict_full", "directional_probability_primary": 0.5, "sequence_uncertainty_primary": 0.04} for row in base_rows],
    )
    lob_path = _write_expert_run(
        root=registry_root,
        family="train_v5_lob",
        run_id="lob-run-runtime-002",
        trainer="v5_lob",
        snapshot_id=snapshot_id,
        rows=[{**row, "support_level": "strict_full", "micro_alpha_1s": 0.1, "micro_alpha_5s": 0.1, "micro_alpha_30s": 0.1, "micro_uncertainty": 0.03} for row in base_rows],
    )
    tradability_path = _write_expert_run(
        root=registry_root,
        family="train_v5_tradability",
        run_id="tradability-run-runtime-002",
        trainer="v5_tradability",
        snapshot_id=snapshot_id,
        rows=_tradability_rows(base_rows),
    )
    partial_runtime_rows = _runtime_rows_for("2026-03-29")
    panel_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_panel_ensemble" / "panel-run-runtime-002" / "_runtime_exports" / "2026-03-28__2026-03-28" / "expert_prediction_table.parquet",
        rows=[{**row, "final_rank_score": 0.41, "final_expected_return": 0.11, "final_expected_es": 0.02, "final_tradability": 0.8, "final_uncertainty": 0.05, "final_alpha_lcb": 0.04} for row in partial_runtime_rows],
    )
    sequence_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_sequence" / "sequence-run-runtime-002" / "_runtime_exports" / "2026-03-28__2026-03-28" / "expert_prediction_table.parquet",
        rows=[{**row, "support_level": "strict_full", "directional_probability_primary": 0.51, "sequence_uncertainty_primary": 0.04} for row in partial_runtime_rows],
    )
    lob_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_lob" / "lob-run-runtime-002" / "_runtime_exports" / "2026-03-28__2026-03-28" / "expert_prediction_table.parquet",
        rows=[{**row, "support_level": "strict_full", "micro_alpha_1s": 0.11, "micro_alpha_5s": 0.11, "micro_alpha_30s": 0.11, "micro_uncertainty": 0.03} for row in partial_runtime_rows],
    )
    tradability_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_tradability" / "tradability-run-runtime-002" / "_runtime_exports" / "2026-03-28__2026-03-28" / "expert_prediction_table.parquet",
        rows=_tradability_rows(partial_runtime_rows),
    )
    options = TrainV5FusionOptions(
        panel_input_path=panel_path,
        sequence_input_path=sequence_path,
        lob_input_path=lob_path,
        tradability_input_path=tradability_path,
        panel_runtime_input_path=panel_runtime,
        sequence_runtime_input_path=sequence_runtime,
        lob_runtime_input_path=lob_runtime,
        tradability_runtime_input_path=tradability_runtime,
        registry_root=registry_root,
        logs_root=tmp_path / "logs",
        model_family="train_v5_fusion",
        quote="KRW",
        start="2026-03-27",
        end="2026-03-27",
        runtime_start="2026-03-28",
        runtime_end="2026-03-28",
        seed=7,
    )
    with pytest.raises(ValueError, match="(PANEL_RUNTIME_WINDOW_GAP|FUSION_RUNTIME_INPUT_WINDOW_EMPTY)"):
        train_and_register_v5_fusion(options)


def test_train_v5_fusion_fails_on_common_runtime_universe_empty(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    snapshot_id = "snapshot-fusion-runtime-universe-empty"
    base_rows = _base_rows()
    panel_path = _write_expert_run(
        root=registry_root,
        family="train_v5_panel_ensemble",
        run_id="panel-run-runtime-universe-empty",
        trainer="v5_panel_ensemble",
        snapshot_id=snapshot_id,
        rows=[{**row, "final_rank_score": 0.4, "final_expected_return": 0.1, "final_expected_es": 0.02, "final_tradability": 0.8, "final_uncertainty": 0.05, "final_alpha_lcb": 0.03} for row in base_rows],
    )
    sequence_path = _write_expert_run(
        root=registry_root,
        family="train_v5_sequence",
        run_id="sequence-run-runtime-universe-empty",
        trainer="v5_sequence",
        snapshot_id=snapshot_id,
        rows=[{**row, "support_level": "strict_full", "directional_probability_primary": 0.5, "sequence_uncertainty_primary": 0.04} for row in base_rows],
    )
    lob_path = _write_expert_run(
        root=registry_root,
        family="train_v5_lob",
        run_id="lob-run-runtime-universe-empty",
        trainer="v5_lob",
        snapshot_id=snapshot_id,
        rows=[{**row, "support_level": "strict_full", "micro_alpha_1s": 0.1, "micro_alpha_5s": 0.1, "micro_alpha_30s": 0.1, "micro_uncertainty": 0.03} for row in base_rows],
    )
    tradability_path = _write_expert_run(
        root=registry_root,
        family="train_v5_tradability",
        run_id="tradability-run-runtime-universe-empty",
        trainer="v5_tradability",
        snapshot_id=snapshot_id,
        rows=_tradability_rows(base_rows),
    )
    panel_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_panel_ensemble" / "panel-run-runtime-universe-empty" / "_runtime_exports" / "2026-03-28__2026-03-28" / "expert_prediction_table.parquet",
        rows=[{**row, "market": "KRW-BTC", "final_rank_score": 0.41, "final_expected_return": 0.11, "final_expected_es": 0.02, "final_tradability": 0.8, "final_uncertainty": 0.05, "final_alpha_lcb": 0.04} for row in _runtime_rows_for("2026-03-28")],
    )
    sequence_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_sequence" / "sequence-run-runtime-universe-empty" / "_runtime_exports" / "2026-03-28__2026-03-28" / "expert_prediction_table.parquet",
        rows=[{**row, "market": "KRW-ETH", "support_level": "strict_full", "directional_probability_primary": 0.51, "sequence_uncertainty_primary": 0.04} for row in _runtime_rows_for("2026-03-28")],
    )
    lob_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_lob" / "lob-run-runtime-universe-empty" / "_runtime_exports" / "2026-03-28__2026-03-28" / "expert_prediction_table.parquet",
        rows=[{**row, "market": "KRW-XRP", "support_level": "strict_full", "micro_alpha_1s": 0.11, "micro_alpha_5s": 0.11, "micro_alpha_30s": 0.11, "micro_uncertainty": 0.03} for row in _runtime_rows_for("2026-03-28")],
    )
    tradability_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_tradability" / "tradability-run-runtime-universe-empty" / "_runtime_exports" / "2026-03-28__2026-03-28" / "expert_prediction_table.parquet",
        rows=_tradability_rows([{**row, "market": "KRW-XRP"} for row in _runtime_rows_for("2026-03-28")]),
    )
    options = TrainV5FusionOptions(
        panel_input_path=panel_path,
        sequence_input_path=sequence_path,
        lob_input_path=lob_path,
        tradability_input_path=tradability_path,
        panel_runtime_input_path=panel_runtime,
        sequence_runtime_input_path=sequence_runtime,
        lob_runtime_input_path=lob_runtime,
        tradability_runtime_input_path=tradability_runtime,
        registry_root=registry_root,
        logs_root=tmp_path / "logs",
        model_family="train_v5_fusion",
        quote="KRW",
        start="2026-03-27",
        end="2026-03-27",
        runtime_start="2026-03-28",
        runtime_end="2026-03-28",
        seed=7,
    )
    with pytest.raises(ValueError, match="COMMON_RUNTIME_UNIVERSE_EMPTY"):
        train_and_register_v5_fusion(options)


def test_train_v5_fusion_fails_on_runtime_sequence_coverage_gap(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    snapshot_id = "snapshot-fusion-runtime-seq-gap"
    base_rows = _base_rows()
    panel_path = _write_expert_run(
        root=registry_root,
        family="train_v5_panel_ensemble",
        run_id="panel-run-runtime-seq-gap",
        trainer="v5_panel_ensemble",
        snapshot_id=snapshot_id,
        rows=[{**row, "final_rank_score": 0.4, "final_expected_return": 0.1, "final_expected_es": 0.02, "final_tradability": 0.8, "final_uncertainty": 0.05, "final_alpha_lcb": 0.03} for row in base_rows],
    )
    sequence_path = _write_expert_run(
        root=registry_root,
        family="train_v5_sequence",
        run_id="sequence-run-runtime-seq-gap",
        trainer="v5_sequence",
        snapshot_id=snapshot_id,
        rows=[{**row, "support_level": "strict_full", "directional_probability_primary": 0.5, "sequence_uncertainty_primary": 0.04} for row in base_rows],
    )
    lob_path = _write_expert_run(
        root=registry_root,
        family="train_v5_lob",
        run_id="lob-run-runtime-seq-gap",
        trainer="v5_lob",
        snapshot_id=snapshot_id,
        rows=[{**row, "support_level": "strict_full", "micro_alpha_1s": 0.1, "micro_alpha_5s": 0.1, "micro_alpha_30s": 0.1, "micro_uncertainty": 0.03} for row in base_rows],
    )
    tradability_path = _write_expert_run(
        root=registry_root,
        family="train_v5_tradability",
        run_id="tradability-run-runtime-seq-gap",
        trainer="v5_tradability",
        snapshot_id=snapshot_id,
        rows=_tradability_rows(base_rows),
    )
    runtime_rows = _runtime_rows_for("2026-03-28")
    panel_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_panel_ensemble" / "panel-run-runtime-seq-gap" / "_runtime_exports" / "2026-03-28__2026-03-28" / "expert_prediction_table.parquet",
        rows=[{**row, "final_rank_score": 0.41, "final_expected_return": 0.11, "final_expected_es": 0.02, "final_tradability": 0.8, "final_uncertainty": 0.05, "final_alpha_lcb": 0.04} for row in runtime_rows],
    )
    sequence_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_sequence" / "sequence-run-runtime-seq-gap" / "_runtime_exports" / "2026-03-28__2026-03-28" / "expert_prediction_table.parquet",
        rows=[{**row, "support_level": "strict_full", "directional_probability_primary": 0.51, "sequence_uncertainty_primary": 0.04} for row in _runtime_rows_for("2026-03-29")],
    )
    lob_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_lob" / "lob-run-runtime-seq-gap" / "_runtime_exports" / "2026-03-28__2026-03-28" / "expert_prediction_table.parquet",
        rows=[{**row, "support_level": "strict_full", "micro_alpha_1s": 0.11, "micro_alpha_5s": 0.11, "micro_alpha_30s": 0.11, "micro_uncertainty": 0.03} for row in runtime_rows],
    )
    tradability_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_tradability" / "tradability-run-runtime-seq-gap" / "_runtime_exports" / "2026-03-28__2026-03-28" / "expert_prediction_table.parquet",
        rows=_tradability_rows(runtime_rows),
    )
    options = TrainV5FusionOptions(
        panel_input_path=panel_path,
        sequence_input_path=sequence_path,
        lob_input_path=lob_path,
        tradability_input_path=tradability_path,
        panel_runtime_input_path=panel_runtime,
        sequence_runtime_input_path=sequence_runtime,
        lob_runtime_input_path=lob_runtime,
        tradability_runtime_input_path=tradability_runtime,
        registry_root=registry_root,
        logs_root=tmp_path / "logs",
        model_family="train_v5_fusion",
        quote="KRW",
        start="2026-03-27",
        end="2026-03-27",
        runtime_start="2026-03-28",
        runtime_end="2026-03-28",
        seed=7,
    )
    with pytest.raises(ValueError, match="(SEQUENCE_RUNTIME_WINDOW_GAP|FUSION_RUNTIME_SEQUENCE_COVERAGE_GAP)"):
        train_and_register_v5_fusion(options)


def test_train_v5_fusion_allows_single_trailing_runtime_day_gap(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    snapshot_id = "snapshot-fusion-runtime-trailing-gap"
    base_rows = _base_rows()
    panel_path = _write_expert_run(
        root=registry_root,
        family="train_v5_panel_ensemble",
        run_id="panel-run-runtime-trailing-gap",
        trainer="v5_panel_ensemble",
        snapshot_id=snapshot_id,
        rows=[{**row, "final_rank_score": 0.4, "final_expected_return": 0.1, "final_expected_es": 0.02, "final_tradability": 0.8, "final_uncertainty": 0.05, "final_alpha_lcb": 0.03} for row in base_rows],
    )
    sequence_path = _write_expert_run(
        root=registry_root,
        family="train_v5_sequence",
        run_id="sequence-run-runtime-trailing-gap",
        trainer="v5_sequence",
        snapshot_id=snapshot_id,
        rows=[{**row, "support_level": "strict_full", "directional_probability_primary": 0.5, "sequence_uncertainty_primary": 0.04} for row in base_rows],
    )
    lob_path = _write_expert_run(
        root=registry_root,
        family="train_v5_lob",
        run_id="lob-run-runtime-trailing-gap",
        trainer="v5_lob",
        snapshot_id=snapshot_id,
        rows=[{**row, "support_level": "strict_full", "micro_alpha_1s": 0.1, "micro_alpha_5s": 0.1, "micro_alpha_30s": 0.1, "micro_uncertainty": 0.03} for row in base_rows],
    )
    tradability_path = _write_expert_run(
        root=registry_root,
        family="train_v5_tradability",
        run_id="tradability-run-runtime-trailing-gap",
        trainer="v5_tradability",
        snapshot_id=snapshot_id,
        rows=_tradability_rows(base_rows),
    )
    runtime_rows = _runtime_rows_for("2026-03-28")
    panel_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_panel_ensemble" / "panel-run-runtime-trailing-gap" / "_runtime_exports" / "2026-03-28__2026-03-29" / "expert_prediction_table.parquet",
        rows=[{**row, "final_rank_score": 0.41, "final_expected_return": 0.11, "final_expected_es": 0.02, "final_tradability": 0.8, "final_uncertainty": 0.05, "final_alpha_lcb": 0.04} for row in runtime_rows],
    )
    _write_runtime_export_metadata(panel_runtime, start="2026-03-28", end="2026-03-29", coverage_dates=["2026-03-28"])
    sequence_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_sequence" / "sequence-run-runtime-trailing-gap" / "_runtime_exports" / "2026-03-28__2026-03-29" / "expert_prediction_table.parquet",
        rows=[{**row, "support_level": "strict_full", "directional_probability_primary": 0.51, "sequence_uncertainty_primary": 0.04} for row in runtime_rows],
    )
    _write_runtime_export_metadata(sequence_runtime, start="2026-03-28", end="2026-03-29", coverage_dates=["2026-03-28"], anchor_export_path=str(panel_runtime.resolve()))
    lob_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_lob" / "lob-run-runtime-trailing-gap" / "_runtime_exports" / "2026-03-28__2026-03-29" / "expert_prediction_table.parquet",
        rows=[{**row, "support_level": "strict_full", "micro_alpha_1s": 0.11, "micro_alpha_5s": 0.11, "micro_alpha_30s": 0.11, "micro_uncertainty": 0.03} for row in runtime_rows],
    )
    _write_runtime_export_metadata(lob_runtime, start="2026-03-28", end="2026-03-29", coverage_dates=["2026-03-28"], anchor_export_path=str(panel_runtime.resolve()))
    tradability_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_tradability" / "tradability-run-runtime-trailing-gap" / "_runtime_exports" / "2026-03-28__2026-03-29" / "expert_prediction_table.parquet",
        rows=_tradability_rows(runtime_rows),
    )
    _write_runtime_export_metadata(tradability_runtime, start="2026-03-28", end="2026-03-29", coverage_dates=["2026-03-28"], anchor_export_path=str(panel_runtime.resolve()))
    options = TrainV5FusionOptions(
        panel_input_path=panel_path,
        sequence_input_path=sequence_path,
        lob_input_path=lob_path,
        tradability_input_path=tradability_path,
        panel_runtime_input_path=panel_runtime,
        sequence_runtime_input_path=sequence_runtime,
        lob_runtime_input_path=lob_runtime,
        tradability_runtime_input_path=tradability_runtime,
        registry_root=registry_root,
        logs_root=tmp_path / "logs",
        model_family="train_v5_fusion",
        quote="KRW",
        start="2026-03-27",
        end="2026-03-27",
        runtime_start="2026-03-28",
        runtime_end="2026-03-29",
        seed=7,
    )

    result = train_and_register_v5_fusion(options)
    assert result.run_dir.exists()


def test_train_v5_fusion_fails_on_runtime_lob_coverage_gap(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    snapshot_id = "snapshot-fusion-runtime-lob-gap"
    base_rows = _base_rows()
    panel_path = _write_expert_run(
        root=registry_root,
        family="train_v5_panel_ensemble",
        run_id="panel-run-runtime-lob-gap",
        trainer="v5_panel_ensemble",
        snapshot_id=snapshot_id,
        rows=[{**row, "final_rank_score": 0.4, "final_expected_return": 0.1, "final_expected_es": 0.02, "final_tradability": 0.8, "final_uncertainty": 0.05, "final_alpha_lcb": 0.03} for row in base_rows],
    )
    sequence_path = _write_expert_run(
        root=registry_root,
        family="train_v5_sequence",
        run_id="sequence-run-runtime-lob-gap",
        trainer="v5_sequence",
        snapshot_id=snapshot_id,
        rows=[{**row, "support_level": "strict_full", "directional_probability_primary": 0.5, "sequence_uncertainty_primary": 0.04} for row in base_rows],
    )
    lob_path = _write_expert_run(
        root=registry_root,
        family="train_v5_lob",
        run_id="lob-run-runtime-lob-gap",
        trainer="v5_lob",
        snapshot_id=snapshot_id,
        rows=[{**row, "support_level": "strict_full", "micro_alpha_1s": 0.1, "micro_alpha_5s": 0.1, "micro_alpha_30s": 0.1, "micro_uncertainty": 0.03} for row in base_rows],
    )
    tradability_path = _write_expert_run(
        root=registry_root,
        family="train_v5_tradability",
        run_id="tradability-run-runtime-lob-gap",
        trainer="v5_tradability",
        snapshot_id=snapshot_id,
        rows=_tradability_rows(base_rows),
    )
    runtime_rows = _runtime_rows_for("2026-03-28")
    panel_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_panel_ensemble" / "panel-run-runtime-lob-gap" / "_runtime_exports" / "2026-03-28__2026-03-28" / "expert_prediction_table.parquet",
        rows=[{**row, "final_rank_score": 0.41, "final_expected_return": 0.11, "final_expected_es": 0.02, "final_tradability": 0.8, "final_uncertainty": 0.05, "final_alpha_lcb": 0.04} for row in runtime_rows],
    )
    sequence_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_sequence" / "sequence-run-runtime-lob-gap" / "_runtime_exports" / "2026-03-28__2026-03-28" / "expert_prediction_table.parquet",
        rows=[{**row, "support_level": "strict_full", "directional_probability_primary": 0.51, "sequence_uncertainty_primary": 0.04} for row in runtime_rows],
    )
    lob_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_lob" / "lob-run-runtime-lob-gap" / "_runtime_exports" / "2026-03-28__2026-03-28" / "expert_prediction_table.parquet",
        rows=[{**row, "support_level": "strict_full", "micro_alpha_1s": 0.11, "micro_alpha_5s": 0.11, "micro_alpha_30s": 0.11, "micro_uncertainty": 0.03} for row in _runtime_rows_for("2026-03-29")],
    )
    tradability_runtime = _write_runtime_export(
        table_path=registry_root / "train_v5_tradability" / "tradability-run-runtime-lob-gap" / "_runtime_exports" / "2026-03-28__2026-03-28" / "expert_prediction_table.parquet",
        rows=_tradability_rows(runtime_rows),
    )
    options = TrainV5FusionOptions(
        panel_input_path=panel_path,
        sequence_input_path=sequence_path,
        lob_input_path=lob_path,
        tradability_input_path=tradability_path,
        panel_runtime_input_path=panel_runtime,
        sequence_runtime_input_path=sequence_runtime,
        lob_runtime_input_path=lob_runtime,
        tradability_runtime_input_path=tradability_runtime,
        registry_root=registry_root,
        logs_root=tmp_path / "logs",
        model_family="train_v5_fusion",
        quote="KRW",
        start="2026-03-27",
        end="2026-03-27",
        runtime_start="2026-03-28",
        runtime_end="2026-03-28",
        seed=7,
    )
    with pytest.raises(ValueError, match="(LOB_RUNTIME_WINDOW_GAP|FUSION_RUNTIME_LOB_COVERAGE_GAP)"):
        train_and_register_v5_fusion(options)
