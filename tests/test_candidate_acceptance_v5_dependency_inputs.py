from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
ACCEPTANCE_SCRIPT = REPO_ROOT / "scripts" / "candidate_acceptance.ps1"


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required for this test")


def _seed_train_snapshot_close_contract(
    project_root: Path,
    *,
    batch_date: str,
    snapshot_id: str,
) -> None:
    _write_json(project_root / "data" / "_meta" / "data_platform_ready_snapshot.json", {"snapshot_id": snapshot_id})
    _write_json(
        project_root / "data" / "collect" / "_meta" / "train_snapshot_close_latest.json",
        {
            "policy": "v5_train_snapshot_close_v1",
            "batch_date": batch_date,
            "snapshot_id": snapshot_id,
            "snapshot_root": str(project_root / "data" / "snapshots" / "data_platform" / snapshot_id),
            "published_at_utc": "2026-03-08T00:05:00Z",
            "generated_at_utc": "2026-03-08T00:05:00Z",
            "training_critical_start_date": "2026-03-04",
            "training_critical_end_date": batch_date,
            "deadline_met": True,
            "overall_pass": True,
            "failure_reasons": [],
            "micro_root": str(project_root / "data" / "parquet" / "micro_v1"),
            "micro_date_coverage_counts": {},
            "source_freshness": {
                "candles_api_refresh": {"pass": True},
                "raw_ticks_daily": {"pass": True, "batch_date": batch_date, "batch_covered": True},
            },
        },
    )


def _make_fake_python_exe(tmp_path: Path) -> Path:
    driver_path = tmp_path / "fake_python_driver.py"
    driver_path.write_text(
        textwrap.dedent(
            """
            from datetime import datetime, timezone
            import json
            import sys
            from pathlib import Path

            ROOT = Path.cwd()
            SNAPSHOT_ID = "snapshot-dependency-001"
            PANEL_RUN_ID = "panel-run-001"
            SEQ_RUN_ID = "sequence-run-001"
            LOB_RUN_ID = "lob-run-001"
            TRAD_RUN_ID = "tradability-run-001"
            FUSION_RUN_ID = "fusion-run-001"

            def arg_value(name: str, default: str = "") -> str:
                if name not in sys.argv:
                    return default
                index = sys.argv.index(name)
                if index + 1 >= len(sys.argv):
                    return default
                return sys.argv[index + 1]

            def write_json(path: Path, payload: object) -> None:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(payload), encoding="utf-8")

            def append_log(payload: object) -> None:
                log_path = ROOT / "logs" / "fake_python_invocations.jsonl"
                log_path.parent.mkdir(parents=True, exist_ok=True)
                with log_path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(payload) + "\\n")

            def date_to_ts_ms(value: str, end_of_day: bool = False) -> int:
                parsed = datetime.fromisoformat(value)
                if end_of_day:
                    parsed = parsed.replace(hour=23, minute=59, second=59, microsecond=999000)
                parsed = parsed.replace(tzinfo=timezone.utc)
                return int(parsed.timestamp() * 1000)

            def expert_run(family: str, trainer: str, run_id: str) -> Path:
                run_dir = ROOT / "models" / "registry" / family / run_id
                run_dir.mkdir(parents=True, exist_ok=True)
                dependency_expert_only = "--dependency-expert-only" in sys.argv
                sequence_variant_name = "patchtst_v1__none"
                lob_variant_name = "deeplob_v1"
                fusion_variant_name = "linear"
                write_json(run_dir / "train_config.yaml", {
                    "trainer": trainer,
                    "model_family": family,
                    "data_platform_ready_snapshot_id": SNAPSHOT_ID,
                    "feature_set": arg_value("--feature-set"),
                    "label_set": arg_value("--label-set"),
                    "task": arg_value("--task"),
                    "run_scope": arg_value("--run-scope"),
                    "tf": arg_value("--tf"),
                    "quote": arg_value("--quote"),
                    "top_n": int(arg_value("--top-n", "0") or 0),
                    "start": arg_value("--start"),
                    "end": arg_value("--end"),
                    "execution_acceptance_eval_start": arg_value("--execution-eval-start"),
                    "execution_acceptance_eval_end": arg_value("--execution-eval-end"),
                    "seed": int(arg_value("--seed", "0") or 0),
                    "dependency_expert_only": dependency_expert_only,
                    "sequence_variant_name": sequence_variant_name if trainer in {"v5_sequence", "v5_fusion"} else "",
                    "lob_variant_name": lob_variant_name if trainer in {"v5_lob", "v5_fusion"} else "",
                    "fusion_variant_name": fusion_variant_name if trainer == "v5_fusion" else "",
                })
                write_json(run_dir / "artifact_status.json", {
                    "run_id": run_id,
                    "status": "candidate",
                    "core_saved": True,
                    "support_artifacts_written": True,
                    "expert_prediction_table_complete": True,
                })
                write_json(run_dir / "promotion_decision.json", {"status": "candidate"})
                write_json(run_dir / "trainer_research_evidence.json", {"available": True})
                write_json(run_dir / "search_budget_decision.json", {"status": "default"})
                write_json(run_dir / "economic_objective_profile.json", {"profile_id": "test"})
                write_json(run_dir / "lane_governance.json", {"lane_id": "cls_primary"})
                write_json(run_dir / "decision_surface.json", {"status": "ok"})
                if trainer == "v5_sequence":
                    write_json(
                        run_dir / "runtime_recommendations.json",
                        {
                            "source_family": family,
                            "sequence_variant_name": sequence_variant_name,
                            "sequence_backbone_name": "patchtst_v1",
                            "sequence_pretrain_method": "none",
                            "sequence_pretrain_status": "disabled",
                            "sequence_pretrain_objective": "none",
                            "sequence_pretrain_ready": False,
                            "sequence_pretrain_best_epoch": 0,
                            "sequence_pretrain_encoder_present": False,
                        },
                    )
                if trainer == "v5_sequence":
                    write_json(
                        run_dir / "sequence_pretrain_contract.json",
                        {
                            "policy": "sequence_pretrain_contract_v1",
                            "backbone_family": "patchtst_v1",
                            "pretrain_method": "none",
                            "pretrain_impl_method": "none",
                            "status": "disabled",
                            "pretrain_ready": False,
                            "encoder_artifact_path": "",
                        },
                    )
                    write_json(
                        run_dir / "sequence_pretrain_report.json",
                        {
                            "policy": "sequence_pretrain_report_v1",
                            "objective_name": "none",
                            "status": "disabled",
                            "best_epoch": 0,
                            "encoder_dim": 16,
                            "final_component_values": {},
                            "encoder_norm_summary": {"module_mean_l2_norms": {}, "global_mean_l2_norm": 0.0},
                        },
                    )
                    write_json(
                        run_dir / "domain_weighting_report.json",
                        {
                            "policy": "v5_domain_weighting_v1",
                            "domain_weighting_enabled": True,
                            "domain_details": {"source_kind": "regime_inverse_frequency_v1"},
                            "effective_sample_weight_summary": {"mean": 1.0},
                        },
                    )
                if trainer == "v5_lob":
                    write_json(
                        run_dir / "runtime_recommendations.json",
                        {
                            "source_family": family,
                            "lob_variant_name": lob_variant_name,
                            "lob_backbone_name": "deeplob_v1",
                        },
                    )
                if trainer == "v5_lob":
                    write_json(
                        run_dir / "lob_backbone_contract.json",
                        {
                            "policy": "lob_backbone_contract_v1",
                            "backbone_family": "deeplob_v1",
                            "uncertainty_head": "softplus_scalar",
                        },
                    )
                    write_json(
                        run_dir / "lob_target_contract.json",
                        {
                            "policy": "lob_target_contract_v1",
                            "primary_horizon_seconds": 30,
                            "auxiliary_targets": ["micro_alpha_60s", "five_min_alpha", "adverse_excursion_30s"],
                        },
                    )
                    write_json(
                        run_dir / "domain_weighting_report.json",
                        {
                            "policy": "v5_domain_weighting_v1",
                            "domain_weighting_enabled": True,
                            "domain_details": {"source_kind": "regime_inverse_frequency_v1"},
                            "effective_sample_weight_summary": {"mean": 1.0},
                        },
                    )
                if trainer == "v5_tradability":
                    write_json(run_dir / "runtime_recommendations.json", {"source_family": family})
                if trainer == "v5_tradability":
                    write_json(
                        run_dir / "tradability_model_contract.json",
                        {
                            "policy": "v5_tradability_v1",
                            "input_experts": {
                                "panel": {"run_id": "panel-run-001"},
                                "sequence": {"run_id": "sequence-run-001"},
                                "lob": {"run_id": "lob-run-001"},
                            },
                        },
                    )
                    write_json(
                        run_dir / "domain_weighting_report.json",
                        {
                            "policy": "v5_domain_weighting_v1",
                            "domain_weighting_enabled": True,
                            "domain_details": {"source_kind": "regime_inverse_frequency_v1"},
                            "effective_sample_weight_summary": {"mean": 1.0},
                        },
                    )
                if trainer == "v5_fusion":
                    write_json(
                        run_dir / "runtime_recommendations.json",
                        {
                            "source_family": family,
                            "sequence_variant_name": sequence_variant_name,
                            "lob_variant_name": lob_variant_name,
                            "fusion_variant_name": fusion_variant_name,
                            "sequence_backbone_name": "patchtst_v1",
                            "sequence_pretrain_method": "none",
                            "sequence_pretrain_ready": False,
                            "sequence_pretrain_status": "disabled",
                            "sequence_pretrain_objective": "none",
                            "sequence_pretrain_best_epoch": 0,
                            "sequence_pretrain_encoder_present": False,
                            "sequence_pretrain_contract_path": str(ROOT / "models" / "registry" / "train_v5_sequence" / SEQ_RUN_ID / "sequence_pretrain_contract.json"),
                            "sequence_pretrain_report_path": str(ROOT / "models" / "registry" / "train_v5_sequence" / SEQ_RUN_ID / "sequence_pretrain_report.json"),
                            "lob_backbone_name": "deeplob_v1",
                            "tradability_source_run_id": TRAD_RUN_ID,
                            "fusion_stacker_family": "linear",
                            "fusion_gating_policy": "single_expert_v1",
                            "domain_weighting_policy": "v5_domain_weighting_v1",
                            "domain_weighting_source_kind": "regime_inverse_frequency_v1",
                        },
                    )
                    write_json(run_dir / "fusion_model_contract.json", {"policy": "v5_fusion_v1", "input_experts": {"tradability": {"run_id": TRAD_RUN_ID}}})
                    write_json(run_dir / "domain_weighting_report.json", {"policy": "v5_domain_weighting_v1", "effective_sample_weight_summary": {"mean": 1.0}, "domain_details": {"source_kind": "regime_inverse_frequency_v1"}, "domain_weighting_enabled": True})
                table = run_dir / "expert_prediction_table.parquet"
                table.write_bytes(b"PAR1")
                return run_dir

            def available_markets_for(trainer: str) -> list[str]:
                if trainer == "v5_panel_ensemble":
                    return ["KRW-BTC", "KRW-ETH"]
                if trainer == "v5_sequence":
                    return ["KRW-ETH", "KRW-XRP"]
                if trainer == "v5_lob":
                    return ["KRW-ETH", "KRW-BTC"]
                return []

            def export_run(family: str, trainer: str, run_id: str, start: str, end: str, explicit_markets: list[str], resolve_only: bool, anchor_export_path: str) -> dict:
                export_root = ROOT / "models" / "registry" / family / run_id / "_runtime_exports" / f"{start}__{end}"
                export_root.mkdir(parents=True, exist_ok=True)
                export_path = export_root / "expert_prediction_table.parquet"
                metadata_path = export_root / "metadata.json"
                reused = export_path.exists() and metadata_path.exists()
                selected_markets = explicit_markets or available_markets_for(trainer)
                metadata = {
                    "run_id": run_id,
                    "trainer": trainer,
                    "model_family": family,
                    "data_platform_ready_snapshot_id": SNAPSHOT_ID,
                    "start": start,
                    "end": end,
                    "coverage_start_ts_ms": date_to_ts_ms(start),
                    "coverage_end_ts_ms": date_to_ts_ms(end, end_of_day=True),
                    "coverage_start_date": start,
                    "coverage_end_date": end,
                    "coverage_dates": [start, end] if start == end else [],
                    "window_timezone": "Asia/Seoul",
                    "rows": 12,
                    "requested_selected_markets": explicit_markets or [],
                    "selected_markets": selected_markets,
                    "selected_markets_source": "acceptance_common_runtime_universe" if explicit_markets else "window_available_markets_fallback",
                    "fallback_reason": "",
                    "anchor_alignment_complete": bool(anchor_export_path) if trainer != "v5_panel_ensemble" else False,
                    "anchor_export_path": anchor_export_path or "",
                }
                if start != end:
                    metadata["coverage_dates"] = []
                    cursor = start
                    while cursor <= end:
                        metadata["coverage_dates"].append(cursor)
                        y,m,d = map(int, cursor.split("-"))
                        import datetime
                        cursor = (datetime.date(y,m,d) + datetime.timedelta(days=1)).isoformat()
                if resolve_only:
                    return {
                        **metadata,
                        "export_path": "",
                        "metadata_path": "",
                        "reused": False,
                        "source_mode": "resolve_markets_only",
                    }
                export_path.write_bytes(b"PAR1")
                write_json(metadata_path, metadata)
                return {
                    **metadata,
                    "export_path": str(export_path),
                    "metadata_path": str(metadata_path),
                    "reused": reused,
                    "source_mode": "existing_export" if reused else "fresh_export",
                }

            args = sys.argv[1:]
            command_key = tuple(args[:4])

            if command_key == ("-m", "autobot.cli", "features", "build"):
                append_log({"command": "features build", "label_set": arg_value("--label-set")})
                print("features_build_ok")
                sys.exit(0)

            if tuple(args[:3]) == ("-m", "autobot.ops.data_contract_registry", "--project-root"):
                report_path = ROOT / "data" / "_meta" / "data_contract_registry.json"
                write_json(report_path, {"summary": {"contract_count": 1}, "entries": [{"contract_id": "feature_dataset:features_v4"}]})
                print(str(report_path))
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "features", "validate"):
                report_path = ROOT / "data" / "features" / "features_v4" / "_meta" / "validate_report.json"
                write_json(report_path, {"checked_files": 1, "ok_files": 1, "warn_files": 0, "fail_files": 0, "schema_ok": True, "leakage_smoke": "PASS"})
                print(str(report_path))
                sys.exit(0)

            if tuple(args[:2]) == ("-m", "autobot.ops.live_feature_parity_report"):
                report_path = ROOT / "data" / "features" / "features_v4" / "_meta" / "live_feature_parity_report.json"
                write_json(report_path, {"sampled_pairs": 1, "compared_pairs": 1, "passing_pairs": 1, "acceptable": True, "status": "PASS"})
                print(str(report_path))
                sys.exit(0)

            if tuple(args[:2]) == ("-m", "autobot.ops.feature_dataset_certification"):
                report_path = ROOT / "data" / "features" / "features_v4" / "_meta" / "feature_dataset_certification.json"
                write_json(report_path, {"policy": "feature_dataset_certification_v1", "status": "PASS", "pass": True, "reasons": []})
                print(f"[ops][feature-dataset-certification] path={report_path}")
                sys.exit(0)

            if tuple(args[:2]) == ("-m", "autobot.ops.private_execution_label_store"):
                build_path = ROOT / "data" / "parquet" / "private_execution_v1" / "_meta" / "build_report.json"
                write_json(build_path, {"rows_written_total": 12, "status": "PASS"})
                write_json(build_path.parent / "validate_report.json", {"status": "PASS", "pass": True, "reasons": []})
                print(str(build_path))
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "model", "train"):
                trainer = arg_value("--trainer")
                family = arg_value("--model-family")
                append_log({
                    "command": "model train",
                    "trainer": trainer,
                    "family": family,
                    "args": args,
                })
                if trainer == "v5_panel_ensemble":
                    run_dir = expert_run(family, trainer, PANEL_RUN_ID)
                    print(json.dumps({"run_dir": str(run_dir), "run_id": PANEL_RUN_ID}))
                    sys.exit(0)
                if trainer == "v5_sequence":
                    run_dir = expert_run(family, trainer, SEQ_RUN_ID)
                    print(json.dumps({"run_dir": str(run_dir), "run_id": SEQ_RUN_ID}))
                    sys.exit(0)
                if trainer == "v5_lob":
                    run_dir = expert_run(family, trainer, LOB_RUN_ID)
                    print(json.dumps({"run_dir": str(run_dir), "run_id": LOB_RUN_ID}))
                    sys.exit(0)
                if trainer == "v5_tradability":
                    run_dir = expert_run(family, trainer, TRAD_RUN_ID)
                    print(json.dumps({"run_dir": str(run_dir), "run_id": TRAD_RUN_ID}))
                    sys.exit(0)
                if trainer == "v5_fusion":
                    panel_input = arg_value("--fusion-panel-input")
                    sequence_input = arg_value("--fusion-sequence-input")
                    lob_input = arg_value("--fusion-lob-input")
                    tradability_input = arg_value("--fusion-tradability-input")
                    panel_runtime_input = arg_value("--fusion-panel-runtime-input")
                    sequence_runtime_input = arg_value("--fusion-sequence-runtime-input")
                    lob_runtime_input = arg_value("--fusion-lob-runtime-input")
                    tradability_runtime_input = arg_value("--fusion-tradability-runtime-input")
                    runtime_start = arg_value("--fusion-runtime-start")
                    runtime_end = arg_value("--fusion-runtime-end")
                    expected_panel = str(ROOT / "models" / "registry" / "train_v5_panel_ensemble" / PANEL_RUN_ID / "expert_prediction_table.parquet")
                    expected_sequence = str(ROOT / "models" / "registry" / "train_v5_sequence" / SEQ_RUN_ID / "expert_prediction_table.parquet")
                    expected_lob = str(ROOT / "models" / "registry" / "train_v5_lob" / LOB_RUN_ID / "expert_prediction_table.parquet")
                    expected_tradability = str(ROOT / "models" / "registry" / "train_v5_tradability" / TRAD_RUN_ID / "expert_prediction_table.parquet")
                    expected_panel_runtime = str(ROOT / "models" / "registry" / "train_v5_panel_ensemble" / PANEL_RUN_ID / "_runtime_exports" / f"{runtime_start}__{runtime_end}" / "expert_prediction_table.parquet")
                    expected_sequence_runtime = str(ROOT / "models" / "registry" / "train_v5_sequence" / SEQ_RUN_ID / "_runtime_exports" / f"{runtime_start}__{runtime_end}" / "expert_prediction_table.parquet")
                    expected_lob_runtime = str(ROOT / "models" / "registry" / "train_v5_lob" / LOB_RUN_ID / "_runtime_exports" / f"{runtime_start}__{runtime_end}" / "expert_prediction_table.parquet")
                    expected_tradability_runtime = str(ROOT / "models" / "registry" / "train_v5_tradability" / TRAD_RUN_ID / "_runtime_exports" / f"{runtime_start}__{runtime_end}" / "expert_prediction_table.parquet")
                    if panel_input != expected_panel or sequence_input != expected_sequence or lob_input != expected_lob or tradability_input != expected_tradability or panel_runtime_input != expected_panel_runtime or sequence_runtime_input != expected_sequence_runtime or lob_runtime_input != expected_lob_runtime or tradability_runtime_input != expected_tradability_runtime:
                        print("fusion input mismatch", file=sys.stderr)
                        print(json.dumps({
                            "panel_input": panel_input,
                            "sequence_input": sequence_input,
                            "lob_input": lob_input,
                            "tradability_input": tradability_input,
                            "panel_runtime_input": panel_runtime_input,
                            "sequence_runtime_input": sequence_runtime_input,
                            "lob_runtime_input": lob_runtime_input,
                            "tradability_runtime_input": tradability_runtime_input,
                            "expected_panel": expected_panel,
                            "expected_sequence": expected_sequence,
                            "expected_lob": expected_lob,
                            "expected_tradability": expected_tradability,
                            "expected_panel_runtime": expected_panel_runtime,
                            "expected_sequence_runtime": expected_sequence_runtime,
                            "expected_lob_runtime": expected_lob_runtime,
                            "expected_tradability_runtime": expected_tradability_runtime,
                        }), file=sys.stderr)
                        sys.exit(2)
                    run_dir = expert_run(family, trainer, FUSION_RUN_ID)
                    write_json(run_dir / "fusion_runtime_input_contract.json", {
                        "snapshot_id": SNAPSHOT_ID,
                        "runtime_window": {
                            "start": runtime_start,
                            "end": runtime_end,
                            "start_ts_ms": 1774656000000,
                            "end_ts_ms": 1774742399999,
                        },
                        "coverage_start_ts_ms": 1774656000000,
                        "coverage_end_ts_ms": 1774742399999,
                        "runtime_rows_after_date_filter": 12,
                        "runtime_dataset_root": str(run_dir / "runtime_feature_dataset"),
                    })
                    print(json.dumps({"run_dir": str(run_dir), "run_id": FUSION_RUN_ID}))
                    sys.exit(0)

            if command_key == ("-m", "autobot.cli", "model", "train-variant-matrix"):
                trainer = arg_value("--trainer")
                family = arg_value("--model-family")
                append_log({
                    "command": "model train-variant-matrix",
                    "trainer": trainer,
                    "family": family,
                    "args": args,
                })
                if trainer == "v5_sequence":
                    run_dir = expert_run(family, trainer, SEQ_RUN_ID)
                    write_json(
                        run_dir / "sequence_variant_report.json",
                        {
                            "policy": "v5_sequence_variant_report_v1",
                            "chosen_variant_name": "patchtst_v1__none",
                            "evaluated_variant_count": 4,
                            "chosen_reason_code": "BASELINE_RETAINED_NO_CLEAR_EDGE",
                            "baseline_kept_reason_code": "NO_CLEAR_EDGE",
                        },
                    )
                    print(json.dumps({"run_dir": str(run_dir), "run_id": SEQ_RUN_ID, "chosen_variant_name": "patchtst_v1__none", "variant_report_path": str(run_dir / "sequence_variant_report.json"), "evaluated_variant_count": 4, "source_mode": "fresh_train", "chosen_reason_code": "BASELINE_RETAINED_NO_CLEAR_EDGE", "baseline_kept_reason_code": "NO_CLEAR_EDGE"}))
                    sys.exit(0)
                if trainer == "v5_lob":
                    run_dir = expert_run(family, trainer, LOB_RUN_ID)
                    write_json(
                        run_dir / "lob_variant_report.json",
                        {
                            "policy": "v5_lob_variant_report_v1",
                            "chosen_variant_name": "deeplob_v1",
                            "evaluated_variant_count": 3,
                            "chosen_reason_code": "BASELINE_RETAINED_NO_CLEAR_EDGE",
                            "baseline_kept_reason_code": "NO_CLEAR_EDGE",
                        },
                    )
                    print(json.dumps({"run_dir": str(run_dir), "run_id": LOB_RUN_ID, "chosen_variant_name": "deeplob_v1", "variant_report_path": str(run_dir / "lob_variant_report.json"), "evaluated_variant_count": 3, "source_mode": "fresh_train", "chosen_reason_code": "BASELINE_RETAINED_NO_CLEAR_EDGE", "baseline_kept_reason_code": "NO_CLEAR_EDGE"}))
                    sys.exit(0)
                if trainer == "v5_fusion":
                    run_dir = expert_run(family, trainer, FUSION_RUN_ID)
                    write_json(
                        run_dir / "fusion_variant_report.json",
                        {
                            "policy": "v5_fusion_variant_report_v1",
                            "chosen_variant_name": "linear",
                            "selected_sequence_variant_name": "patchtst_v1__none",
                            "selected_lob_variant_name": "deeplob_v1",
                            "selected_fusion_stacker": "linear",
                            "evaluated_variant_count": 3,
                            "chosen_reason_code": "BASELINE_RETAINED_NO_CLEAR_EDGE",
                            "baseline_kept_reason_code": "NO_CLEAR_EDGE",
                        },
                    )
                    print(json.dumps({"run_dir": str(run_dir), "run_id": FUSION_RUN_ID, "chosen_variant_name": "linear", "variant_report_path": str(run_dir / "fusion_variant_report.json"), "evaluated_variant_count": 3, "source_mode": "fresh_train", "chosen_reason_code": "BASELINE_RETAINED_NO_CLEAR_EDGE", "baseline_kept_reason_code": "NO_CLEAR_EDGE", "input_provenance": {"sequence_variant_name": "patchtst_v1__none", "lob_variant_name": "deeplob_v1"}}))
                    sys.exit(0)

            if command_key == ("-m", "autobot.cli", "model", "export-expert-table"):
                trainer = arg_value("--trainer")
                run_dir = Path(arg_value("--run-dir"))
                start = arg_value("--start")
                end = arg_value("--end")
                explicit_markets = [item.strip() for item in arg_value("--markets").split(",") if item.strip()]
                anchor_export_path = arg_value("--anchor-export-path")
                resolve_only = "--resolve-markets-only" in sys.argv
                family = run_dir.parent.name
                run_id = run_dir.name
                append_log({
                    "command": "model export-expert-table",
                    "trainer": trainer,
                    "run_id": run_id,
                    "start": start,
                    "end": end,
                    "markets": explicit_markets,
                    "anchor_export_path": anchor_export_path,
                    "resolve_markets_only": resolve_only,
                })
                print(json.dumps(export_run(family, trainer, run_id, start, end, explicit_markets, resolve_only, anchor_export_path)))
                if trainer in {"v5_sequence", "v5_lob"} and not resolve_only:
                    print("UserWarning: runtime export alignment note")
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "model", "inspect-runtime-dataset"):
                dataset_root = Path(arg_value("--dataset-root"))
                contract_path = dataset_root.parent / "fusion_runtime_input_contract.json"
                contract = json.loads(contract_path.read_text(encoding="utf-8")) if contract_path.exists() else {}
                runtime_window = contract.get("runtime_window", {})
                runtime_start = runtime_window.get("start", "2026-03-07")
                runtime_end = runtime_window.get("end", runtime_start)
                append_log({
                    "command": "model inspect-runtime-dataset",
                    "dataset_root": str(dataset_root),
                })
                print(json.dumps({
                    "dataset_root": str(dataset_root),
                    "manifest_path": str(dataset_root / "_meta" / "manifest.parquet"),
                    "data_file_count": 1,
                    "rows": int(contract.get("runtime_rows_after_date_filter", 12) or 0),
                    "min_ts_ms": date_to_ts_ms(runtime_start),
                    "max_ts_ms": date_to_ts_ms(runtime_end, end_of_day=True),
                    "markets": ["KRW-BTC"],
                    "exists": True,
                    "manifest_exists": True,
                }))
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "backtest", "alpha"):
                model_ref = arg_value("--model-ref")
                runs_dir = ROOT / "data" / "backtest" / "runs"
                run_dir = runs_dir / ("candidate" if model_ref == FUSION_RUN_ID else "champion")
                run_dir.mkdir(parents=True, exist_ok=True)
                payload = {
                    "orders_filled": 64,
                    "realized_pnl_quote": 250.0 if model_ref == FUSION_RUN_ID else 100.0,
                    "fill_rate": 0.82,
                    "max_drawdown_pct": 0.05 if model_ref == FUSION_RUN_ID else 0.08,
                    "slippage_bps_mean": 1.0 if model_ref == FUSION_RUN_ID else 1.4,
                }
                write_json(run_dir / "summary.json", payload)
                print(json.dumps({"run_dir": str(run_dir), "model_ref": model_ref}))
                sys.exit(0)

            if tuple(args[:2]) == ("-m", "autobot.models.stat_validation"):
                print(json.dumps({"comparable": True, "deflated_sharpe_ratio_est": 0.75, "probabilistic_sharpe_ratio": 0.90}))
                sys.exit(0)

            if tuple(args[:2]) == ("-m", "autobot.common.operational_overlay_calibration"):
                output_path = arg_value("--output-path")
                if output_path:
                    write_json(Path(output_path), {"report_count": 0, "sufficient_reports": False, "applied_fields": []})
                print("{}")
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "model", "promote"):
                print("promote_ok")
                sys.exit(0)

            print("unexpected fake python invocation: " + " ".join(args), file=sys.stderr)
            sys.exit(1)
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    if os.name == "nt":
        wrapper_path = tmp_path / "fake_python.cmd"
        wrapper_path.write_text(
            f'@echo off\r\n"{sys.executable}" "%~dp0fake_python_driver.py" %*\r\n',
            encoding="utf-8",
        )
    else:
        wrapper_path = tmp_path / "fake_python"
        wrapper_path.write_text(
            "#!/bin/sh\n"
            f'"{sys.executable}" "$(dirname "$0")/fake_python_driver.py" "$@"\n',
            encoding="utf-8",
        )
        wrapper_path.chmod(0o755)
    return wrapper_path


def test_candidate_acceptance_passes_dependency_expert_tables_to_fusion(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v5_fusion" / "champion.json",
        {"run_id": "champion-run-000"},
    )
    _seed_train_snapshot_close_contract(project_root, batch_date="2026-03-08", snapshot_id="snapshot-dependency-001")
    python_exe = _make_fake_python_exe(tmp_path)
    wrapper_script = tmp_path / "run_acceptance.ps1"
    wrapper_script.write_text(
        (
            "& "
            + json.dumps(str(ACCEPTANCE_SCRIPT))
            + " -ProjectRoot "
            + json.dumps(str(project_root))
            + " -PythonExe "
            + json.dumps(str(python_exe))
            + " -OutDir "
            + json.dumps("logs/test_acceptance_v5_dependency")
            + " -BatchDate "
            + json.dumps("2026-03-08")
            + " -TrainLookbackDays 2 -BacktestLookbackDays 2 -SkipDailyPipeline -SkipPaperSoak -SkipPromote "
            + "-ModelFamily train_v5_fusion -Trainer v5_fusion -DependencyTrainers @(\"v5_panel_ensemble\",\"v5_sequence\",\"v5_lob\",\"v5_tradability\")\n"
        ),
        encoding="utf-8",
    )
    command = [
        _powershell_exe(),
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(wrapper_script),
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stdout + "\n" + result.stderr

    invocations = [
        json.loads(line)
        for line in (project_root / "logs" / "fake_python_invocations.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    train_calls = [row for row in invocations if row.get("command") == "model train"]
    assert [row["trainer"] for row in train_calls] == [
        "v5_panel_ensemble",
        "v5_sequence",
        "v5_lob",
        "v5_tradability",
        "v5_fusion",
    ]
    assert [row for row in invocations if row.get("command") == "features build"] == []
    assert "--dependency-expert-only" not in train_calls[0]["args"]
    assert "--dependency-expert-only" not in train_calls[1]["args"]
    assert "--dependency-expert-only" not in train_calls[2]["args"]
    export_calls = [row for row in invocations if row.get("command") == "model export-expert-table"]
    assert [row["trainer"] for row in export_calls] == [
        "v5_panel_ensemble",
        "v5_sequence",
        "v5_lob",
        "v5_panel_ensemble",
        "v5_sequence",
        "v5_lob",
        "v5_tradability",
    ]
    resolve_calls = [row for row in export_calls if row.get("resolve_markets_only")]
    materialize_calls = [row for row in export_calls if not row.get("resolve_markets_only")]
    assert len(resolve_calls) == 3
    assert len(materialize_calls) == 4
    assert all(row["markets"] == [] for row in resolve_calls)
    assert all(row["markets"] == ["KRW-ETH"] for row in materialize_calls)
    fusion_call = train_calls[-1]
    args = fusion_call["args"]
    assert "--fusion-panel-input" in args
    assert "--fusion-sequence-input" in args
    assert "--fusion-lob-input" in args
    assert "--fusion-tradability-input" in args
    assert "--fusion-panel-runtime-input" in args
    assert "--fusion-sequence-runtime-input" in args
    assert "--fusion-lob-runtime-input" in args
    assert "--fusion-tradability-runtime-input" in args

    report = json.loads(
        (project_root / "logs" / "test_acceptance_v5_dependency" / "latest.json").read_text(encoding="utf-8-sig")
    )
    assert report["candidate"]["fusion_run_id"] == "fusion-run-001"
    assert report["candidate"]["dependency_trainer_run_ids"] == [
        "panel-run-001",
        "sequence-run-001",
        "lob-run-001",
        "tradability-run-001",
    ]
    assert report["candidate"]["snapshot_chain_consistent"] is True
    assert report["steps"]["dependency_trainers"]["trained_count"] == 4
    assert report["steps"]["dependency_trainers"]["reused_count"] == 0
    assert report["steps"]["dependency_runtime_universe"]["common_markets"] == ["KRW-ETH"]
    assert report["steps"]["common_runtime_universe"]["common_markets"] == ["KRW-ETH"]
    assert report["steps"]["dependency_runtime_export_contract"]["pass"] is True
    assert report["steps"]["dependency_runtime_exports"]["count"] == 4
    export_results = report["steps"]["dependency_runtime_exports"]["results"]
    assert export_results[0]["requested_selected_markets"] == ["KRW-ETH"]
    assert export_results[0]["selected_markets"] == ["KRW-ETH"]
    assert export_results[0]["selected_markets_source"] == "acceptance_common_runtime_universe"
    assert export_results[0]["fallback_reason"] == ""
    common_universe_path = Path(report["steps"]["common_runtime_universe"]["artifact_path"])
    assert common_universe_path.exists()
    common_universe_payload = json.loads(common_universe_path.read_text(encoding="utf-8-sig"))
    assert common_universe_payload["common_runtime_universe_id"] == report["steps"]["common_runtime_universe"]["common_runtime_universe_id"]
    inputs = report["steps"]["train"]["fusion_dependency_inputs"]
    runtime_inputs = report["steps"]["train"]["fusion_dependency_runtime_inputs"]
    assert inputs["fusion_panel_input"].replace("\\", "/").endswith("/train_v5_panel_ensemble/panel-run-001/expert_prediction_table.parquet")
    assert inputs["fusion_sequence_input"].replace("\\", "/").endswith("/train_v5_sequence/sequence-run-001/expert_prediction_table.parquet")
    assert inputs["fusion_lob_input"].replace("\\", "/").endswith("/train_v5_lob/lob-run-001/expert_prediction_table.parquet")
    assert inputs["fusion_tradability_input"].replace("\\", "/").endswith("/train_v5_tradability/tradability-run-001/expert_prediction_table.parquet")
    assert runtime_inputs["fusion_panel_runtime_input"].replace("\\", "/").endswith("/train_v5_panel_ensemble/panel-run-001/_runtime_exports/2026-03-07__2026-03-08/expert_prediction_table.parquet")
    assert runtime_inputs["fusion_sequence_runtime_input"].replace("\\", "/").endswith("/train_v5_sequence/sequence-run-001/_runtime_exports/2026-03-07__2026-03-08/expert_prediction_table.parquet")
    assert runtime_inputs["fusion_lob_runtime_input"].replace("\\", "/").endswith("/train_v5_lob/lob-run-001/_runtime_exports/2026-03-07__2026-03-08/expert_prediction_table.parquet")
    assert runtime_inputs["fusion_tradability_runtime_input"].replace("\\", "/").endswith("/train_v5_tradability/tradability-run-001/_runtime_exports/2026-03-07__2026-03-08/expert_prediction_table.parquet")


def test_candidate_acceptance_variant_matrix_selection_routes_chosen_variants(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v5_fusion" / "champion.json",
        {"run_id": "champion-run-000"},
    )
    _seed_train_snapshot_close_contract(project_root, batch_date="2026-03-08", snapshot_id="snapshot-dependency-001")
    python_exe = _make_fake_python_exe(tmp_path)
    wrapper_script = tmp_path / "run_acceptance_matrix.ps1"
    wrapper_script.write_text(
        (
            "& "
            + json.dumps(str(ACCEPTANCE_SCRIPT))
            + " -ProjectRoot "
            + json.dumps(str(project_root))
            + " -PythonExe "
            + json.dumps(str(python_exe))
            + " -OutDir "
            + json.dumps("logs/test_acceptance_v5_variant_matrix")
            + " -BatchDate "
            + json.dumps("2026-03-08")
            + " -TrainLookbackDays 2 -BacktestLookbackDays 2 -SkipDailyPipeline -SkipPaperSoak -SkipPromote -EnableVariantMatrixSelection "
            + "-ModelFamily train_v5_fusion -Trainer v5_fusion -DependencyTrainers @(\"v5_panel_ensemble\",\"v5_sequence\",\"v5_lob\",\"v5_tradability\")\n"
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(wrapper_script),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr

    invocations = [
        json.loads(line)
        for line in (project_root / "logs" / "fake_python_invocations.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [row["trainer"] for row in invocations if row.get("command") == "model train-variant-matrix"] == [
        "v5_sequence",
        "v5_lob",
        "v5_fusion",
    ]
    tradability_calls = [row for row in invocations if row.get("command") == "model train" and row.get("trainer") == "v5_tradability"]
    assert len(tradability_calls) == 1
    tradability_args = tradability_calls[0]["args"]
    assert "--tradability-sequence-input" in tradability_args
    assert "--tradability-lob-input" in tradability_args

    report = json.loads(
        (project_root / "logs" / "test_acceptance_v5_variant_matrix" / "latest.json").read_text(encoding="utf-8-sig")
    )
    assert report["steps"]["sequence_variant_selection"]["pass"] is True
    assert report["steps"]["lob_variant_selection"]["pass"] is True
    assert report["steps"]["fusion_variant_selection"]["pass"] is True
    assert report["steps"]["sequence_variant_selection"]["chosen_variant_name"] == "patchtst_v1__none"
    assert report["steps"]["lob_variant_selection"]["chosen_variant_name"] == "deeplob_v1"
    assert report["steps"]["fusion_variant_selection"]["chosen_variant_name"] == "linear"
    assert report["candidate"]["sequence_variant_name"] == "patchtst_v1__none"
    assert report["candidate"]["lob_variant_name"] == "deeplob_v1"
    assert report["candidate"]["fusion_variant_name"] == "linear"
