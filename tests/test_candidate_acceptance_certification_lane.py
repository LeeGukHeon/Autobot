from datetime import datetime, timedelta
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
    normalized = str(path).replace("\\", "/")
    if normalized.endswith("/models/registry/train_v4_crypto_cs/champion.json"):
        mirror = path.parents[1] / "train_v5_fusion" / "champion.json"
        mirror.parent.mkdir(parents=True, exist_ok=True)
        mirror.write_text(json.dumps(payload), encoding="utf-8")


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required for this test")


def _make_fake_python_exe(
    tmp_path: Path,
    *,
    write_decision_surface: bool,
    write_feature_validate_report: bool = True,
    feature_validate_fail_files: int = 0,
    feature_validate_leakage_smoke: str = "PASS",
    write_live_feature_parity_report: bool = True,
    live_feature_parity_acceptable: bool = True,
    write_trainer_research_evidence: bool = True,
    write_latest_candidate_pointer: bool = True,
    emit_train_run_dir: bool = True,
    emit_cli_prefixed_train_run_dir: bool = False,
    budget_lane_class_requested: str = "promotion_eligible",
    budget_lane_class_effective: str = "promotion_eligible",
    budget_contract_id: str = "v4_promotion_eligible_budget_v1",
    budget_promotion_eligible_satisfied: bool = True,
    candidate_orders_submitted: int = 64,
    candidate_orders_filled: int = 64,
    candidate_candidates_aborted_by_policy: int = 0,
    profile_candidate_min_orders_filled: int = 30,
    feature_rows_by_window: dict[str, int] | None = None,
    feature_min_rows_for_train: int = 4000,
    history_anchor_backtest_by_window: dict[str, dict[str, float | int]] | None = None,
    candidate_execution_structure: dict[str, object] | None = None,
    champion_execution_structure: dict[str, object] | None = None,
) -> Path:
    driver_path = tmp_path / "fake_python_driver.py"
    driver_path.write_text(
        textwrap.dedent(
            f"""
            import json
            import sys
            from datetime import datetime, timezone
            from pathlib import Path

            ROOT = Path.cwd()
            CANDIDATE_RUN_ID = "candidate-run-001"
            CHAMPION_RUN_ID = "champion-run-000"
            WRITE_DECISION_SURFACE = {str(write_decision_surface)}
            WRITE_FEATURE_VALIDATE_REPORT = {str(write_feature_validate_report)}
            FEATURE_VALIDATE_FAIL_FILES = {int(feature_validate_fail_files)}
            FEATURE_VALIDATE_LEAKAGE_SMOKE = {feature_validate_leakage_smoke!r}
            WRITE_LIVE_FEATURE_PARITY_REPORT = {str(write_live_feature_parity_report)}
            LIVE_FEATURE_PARITY_ACCEPTABLE = {str(live_feature_parity_acceptable)}
            WRITE_TRAINER_RESEARCH_EVIDENCE = {str(write_trainer_research_evidence)}
            WRITE_LATEST_CANDIDATE_POINTER = {str(write_latest_candidate_pointer)}
            EMIT_TRAIN_RUN_DIR = {str(emit_train_run_dir)}
            EMIT_CLI_PREFIXED_TRAIN_RUN_DIR = {str(emit_cli_prefixed_train_run_dir)}
            BUDGET_LANE_CLASS_REQUESTED = {budget_lane_class_requested!r}
            BUDGET_LANE_CLASS_EFFECTIVE = {budget_lane_class_effective!r}
            BUDGET_CONTRACT_ID = {budget_contract_id!r}
            BUDGET_PROMOTION_ELIGIBLE_SATISFIED = {str(budget_promotion_eligible_satisfied)}
            CANDIDATE_ORDERS_SUBMITTED = {int(candidate_orders_submitted)}
            CANDIDATE_ORDERS_FILLED = {int(candidate_orders_filled)}
            CANDIDATE_CANDIDATES_ABORTED_BY_POLICY = {int(candidate_candidates_aborted_by_policy)}
            PROFILE_CANDIDATE_MIN_ORDERS_FILLED = {int(profile_candidate_min_orders_filled)}
            FEATURE_ROWS_BY_WINDOW = {json.dumps(feature_rows_by_window or {})}
            FEATURE_MIN_ROWS_FOR_TRAIN = {int(feature_min_rows_for_train)}
            HISTORY_ANCHOR_BACKTEST_BY_WINDOW = {json.dumps(history_anchor_backtest_by_window or {})}
            CANDIDATE_EXECUTION_STRUCTURE = {json.dumps(candidate_execution_structure or {})}
            CHAMPION_EXECUTION_STRUCTURE = {json.dumps(champion_execution_structure or {})}


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

            def date_to_ts_ms(text: str, *, end_of_day: bool = False) -> int:
                parsed = datetime.fromisoformat(text)
                if end_of_day:
                    parsed = parsed.replace(hour=23, minute=59, second=59, microsecond=999000)
                parsed = parsed.replace(tzinfo=timezone.utc)
                return int(parsed.timestamp() * 1000)


            def append_log(payload: object) -> None:
                log_path = ROOT / "logs" / "fake_python_invocations.jsonl"
                log_path.parent.mkdir(parents=True, exist_ok=True)
                with log_path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(payload) + "\\n")


            args = sys.argv[1:]
            command_key = tuple(args[:4])

            if command_key == ("-m", "autobot.cli", "model", "train"):
                family = arg_value("--model-family", "train_v4_crypto_cs")
                task = arg_value("--task", "cls").strip().lower() or "cls"
                run_scope = arg_value("--run-scope", "scheduled_daily")
                start_value = arg_value("--start")
                end_value = arg_value("--end")
                execution_eval_start = arg_value("--execution-eval-start")
                execution_eval_end = arg_value("--execution-eval-end")
                is_split_policy_history = run_scope == "scheduled_split_policy_history"
                candidate_run_id = (
                    f"history-run-{{start_value.replace('-', '')}}-{{end_value.replace('-', '')}}"
                    if is_split_policy_history
                    else CANDIDATE_RUN_ID
                )
                registry_dir = ROOT / "models" / "registry" / family
                candidate_dir = registry_dir / candidate_run_id
                append_log(
                    {{
                        "command": "model train",
                        "start": start_value,
                        "end": end_value,
                        **({{"run_scope": run_scope}} if is_split_policy_history else {{}}),
                    }}
                )
                if execution_eval_start or execution_eval_end:
                    append_log(
                        {{
                            "command": "model train eval window",
                            "start": execution_eval_start,
                            "end": execution_eval_end,
                            **({{"run_scope": run_scope}} if is_split_policy_history else {{}}),
                        }}
                    )
                if WRITE_LATEST_CANDIDATE_POINTER and (not is_split_policy_history):
                    write_json(registry_dir / "latest_candidate.json", {{"run_id": CANDIDATE_RUN_ID}})
                if is_split_policy_history:
                    candidate_dir.mkdir(parents=True, exist_ok=True)
                    print(json.dumps({{"run_dir": str(candidate_dir), "run_id": candidate_run_id}}))
                    sys.exit(0)
                write_json(
                    candidate_dir / "promotion_decision.json",
                    {{
                        "status": "candidate",
                        "checks": {{
                            "existing_champion_present": True,
                            "walk_forward_present": True,
                            "walk_forward_windows_run": 4,
                            "balanced_pareto_comparable": True,
                            "balanced_pareto_candidate_edge": True,
                            "spa_like_present": True,
                            "spa_like_comparable": True,
                            "spa_like_candidate_edge": True,
                            "white_rc_present": True,
                            "white_rc_comparable": True,
                            "white_rc_candidate_edge": True,
                            "hansen_spa_present": True,
                            "hansen_spa_comparable": True,
                            "hansen_spa_candidate_edge": True,
                            "execution_acceptance_enabled": True,
                            "execution_acceptance_present": True,
                            "execution_balanced_pareto_comparable": True,
                            "execution_balanced_pareto_candidate_edge": True,
                        }},
                        "research_acceptance": {{
                            "policy": "balanced_pareto_offline",
                            "walk_forward_summary": {{"windows_run": 4}},
                            "compare_to_champion": {{
                                "policy": "balanced_pareto_offline",
                                "decision": "candidate_edge",
                                "comparable": True,
                            }},
                            "spa_like_window_test": {{
                                "policy": "spa_like",
                                "decision": "candidate_edge",
                                "comparable": True,
                            }},
                            "white_reality_check": {{
                                "policy": "white_rc",
                                "decision": "candidate_edge",
                                "candidate_edge": True,
                                "comparable": True,
                            }},
                            "hansen_spa": {{
                                "policy": "hansen_spa",
                                "decision": "candidate_edge",
                                "candidate_edge": True,
                                "comparable": True,
                            }},
                        }},
                        "execution_acceptance": {{
                            "status": "compared",
                            "compare_to_champion": {{
                                "policy": "balanced_pareto_execution",
                                "decision": "candidate_edge",
                                "comparable": True,
                            }},
                        }},
                    }},
                )
                if WRITE_TRAINER_RESEARCH_EVIDENCE:
                    write_json(
                        candidate_dir / "trainer_research_evidence.json",
                        {{
                            "policy": "v4_trainer_research_evidence_v1",
                            "source": "train_v4_crypto_cs",
                            "available": True,
                            "pass": True,
                            "offline_pass": True,
                            "execution_pass": True,
                            "reasons": ["TRAINER_EVIDENCE_PASS"],
                            "checks": {{
                                "existing_champion_present": True,
                                "walk_forward_present": True,
                                "walk_forward_windows_run": 4,
                                "offline_comparable": True,
                                "offline_candidate_edge": True,
                                "spa_like_present": True,
                                "spa_like_comparable": True,
                                "spa_like_candidate_edge": True,
                                "white_rc_present": True,
                                "white_rc_comparable": True,
                                "white_rc_candidate_edge": True,
                                "hansen_spa_present": True,
                                "hansen_spa_comparable": True,
                                "hansen_spa_candidate_edge": True,
                                "execution_acceptance_enabled": True,
                                "execution_acceptance_present": True,
                                "execution_comparable": True,
                                "execution_candidate_edge": True,
                            }},
                            "offline": {{
                                "policy": "balanced_pareto_offline",
                                "decision": "candidate_edge",
                                "comparable": True,
                            }},
                            "spa_like": {{
                                "policy": "spa_like",
                                "decision": "candidate_edge",
                                "comparable": True,
                            }},
                            "white_rc": {{
                                "policy": "white_rc",
                                "decision": "candidate_edge",
                                "comparable": True,
                            }},
                            "hansen_spa": {{
                                "policy": "hansen_spa",
                                "decision": "candidate_edge",
                                "comparable": True,
                            }},
                            "execution": {{
                                "status": "compared",
                                "policy": "balanced_pareto_execution",
                                "decision": "candidate_edge",
                                "comparable": True,
                            }},
                            "support_lane": {{
                                "policy": "v4_certification_support_lane_v1",
                                "source": "train_v4_crypto_cs",
                                "support_only": True,
                                "summary": {{
                                    "status": "supported",
                                    "windows_run": 4,
                                    "multiple_testing_supported": True,
                                    "cpcv_lite_status": "partial",
                                    "reasons": ["WHITE_RC_PASS", "HANSEN_SPA_HOLD", "BUDGET_CUT"],
                                }},
                                "multiple_testing_panel_diagnostics": {{
                                    "comparable": True,
                                    "common_panel_key_count": 3,
                                    "reasons": [],
                                }},
                                "spa_like": {{
                                    "policy": "spa_like_window_ev_net",
                                    "decision": "candidate_edge",
                                    "comparable": True,
                                    "status": "supported",
                                    "reasons": ["SPA_LIKE_PASS"],
                                }},
                                "white_rc": {{
                                    "policy": "white_reality_check",
                                    "decision": "candidate_edge",
                                    "comparable": True,
                                    "status": "supported",
                                    "reasons": ["WHITE_RC_PASS"],
                                    "panel_diagnostics": {{"common_panel_key_count": 3, "reasons": []}},
                                }},
                                "hansen_spa": {{
                                    "policy": "hansen_spa",
                                    "decision": "candidate_edge",
                                    "comparable": True,
                                    "status": "supported",
                                    "reasons": ["HANSEN_SPA_PASS"],
                                    "panel_diagnostics": {{"common_panel_key_count": 3, "reasons": []}},
                                }},
                                "cpcv_lite": {{
                                    "enabled": True,
                                    "trigger": "guarded_policy",
                                    "status": "partial",
                                    "support_status": "partial",
                                    "summary": {{"status": "partial", "reasons": ["BUDGET_CUT"]}},
                                    "insufficiency_reasons": ["BUDGET_CUT"],
                                    "pbo": {{"comparable": True}},
                                    "dsr": {{"comparable": True}},
                                }},
                            }},
                        }},
                    )
                write_json(
                    candidate_dir / "search_budget_decision.json",
                    {{
                        "policy": "v4_daily_search_budget_v1",
                        "status": "default",
                        "lane_class_requested": BUDGET_LANE_CLASS_REQUESTED,
                        "lane_class_effective": BUDGET_LANE_CLASS_EFFECTIVE,
                        "budget_contract_id": BUDGET_CONTRACT_ID,
                        "promotion_eligible_contract": {{
                            "requested": BUDGET_LANE_CLASS_REQUESTED == "promotion_eligible",
                            "satisfied": BUDGET_PROMOTION_ELIGIBLE_SATISFIED,
                            "contract_id": "v4_promotion_eligible_budget_v1",
                            "min_booster_sweep_trials": 10,
                            "required_runtime_recommendation_profile": "full",
                            "require_cpcv_lite_auto_disabled": True,
                        }},
                        "applied": {{
                            "booster_sweep_trials": 10,
                            "runtime_recommendation_profile": "full",
                            "cpcv_lite_auto_enabled": False,
                        }},
                        "markers": [],
                        "reasons": [],
                    }},
                )
                write_json(
                    candidate_dir / "economic_objective_profile.json",
                    {{
                        "version": 2,
                        "policy": "v4_shared_economic_objective_contract",
                        "profile_id": "v4_shared_economic_objective_v3",
                        "promotion_compare": {{
                            "policy": "balanced_pareto_calmar_gate",
                            "pareto_higher_is_better": ["realized_pnl_quote", "fill_rate"],
                            "pareto_lower_is_better": ["max_drawdown_pct", "slippage_bps_mean"],
                            "utility_metric": "calmar_like",
                            "threshold_defaults": {{
                                "candidate_min_orders_filled": PROFILE_CANDIDATE_MIN_ORDERS_FILLED,
                                "candidate_min_realized_pnl_quote": 0.0,
                                "candidate_min_deflated_sharpe_ratio": 0.2,
                                "candidate_min_pnl_delta_vs_champion": 0.0,
                                "champion_min_drawdown_improvement_pct": 0.1,
                            }},
                            "policy_variants": {{
                                "balanced_pareto": {{
                                    "allow_stability_override": True,
                                    "champion_pnl_tolerance_pct": 0.05,
                                    "champion_max_fill_rate_degradation": 0.02,
                                    "champion_max_slippage_deterioration_bps": 2.5,
                                    "champion_min_utility_edge_pct": 0.0,
                                    "use_pareto": True,
                                    "use_utility_tie_break": True,
                                    "backtest_compare_required": True,
                                    "paper_final_gate": False,
                                }},
                                "strict": {{
                                    "allow_stability_override": False,
                                    "champion_pnl_tolerance_pct": 0.0,
                                    "champion_max_fill_rate_degradation": 0.0,
                                    "champion_max_slippage_deterioration_bps": 0.0,
                                    "champion_min_utility_edge_pct": 0.0,
                                    "use_pareto": False,
                                    "use_utility_tie_break": False,
                                    "backtest_compare_required": True,
                                    "paper_final_gate": False,
                                }},
                                "conservative_pareto": {{
                                    "allow_stability_override": True,
                                    "champion_pnl_tolerance_pct": 0.02,
                                    "champion_max_fill_rate_degradation": 0.01,
                                    "champion_max_slippage_deterioration_bps": 1.0,
                                    "champion_min_utility_edge_pct": 0.05,
                                    "use_pareto": True,
                                    "use_utility_tie_break": True,
                                    "backtest_compare_required": True,
                                    "paper_final_gate": False,
                                }},
                                "paper_final_balanced": {{
                                    "allow_stability_override": True,
                                    "champion_pnl_tolerance_pct": 0.05,
                                    "champion_max_fill_rate_degradation": 0.02,
                                    "champion_max_slippage_deterioration_bps": 2.5,
                                    "champion_min_utility_edge_pct": 0.0,
                                    "use_pareto": True,
                                    "use_utility_tie_break": True,
                                    "backtest_compare_required": False,
                                    "paper_final_gate": True,
                                }},
                            }},
                        }},
                    }},
                )
                if task == "rank":
                    lane_id = "rank_shadow"
                    lane_role = "shadow"
                    shadow_only = True
                    promotion_allowed = False
                    live_replacement_allowed = False
                    governance_reasons = ["RANK_LANE_SHADOW_EVALUATION_ONLY", "EXPLICIT_GOVERNANCE_DECISION_REQUIRED"]
                elif task == "cls":
                    lane_id = "cls_primary"
                    lane_role = "primary"
                    shadow_only = False
                    promotion_allowed = True
                    live_replacement_allowed = True
                    governance_reasons = ["PRIMARY_LANE_ELIGIBLE"]
                else:
                    lane_id = task + "_research"
                    lane_role = "research"
                    shadow_only = False
                    promotion_allowed = False
                    live_replacement_allowed = False
                    governance_reasons = ["NON_PRIMARY_LANE_REQUIRES_EXPLICIT_GOVERNANCE"]
                write_json(
                    candidate_dir / "lane_governance.json",
                    {{
                        "version": 1,
                        "policy": "v4_lane_governance_v1",
                        "lane_id": lane_id,
                        "task": task,
                        "run_scope": run_scope,
                        "lane_role": lane_role,
                        "shadow_only": shadow_only,
                        "production_lane_id": "cls_primary",
                        "production_task": "cls",
                        "promotion_allowed": promotion_allowed,
                        "live_replacement_allowed": live_replacement_allowed,
                        "governance_reasons": governance_reasons,
                    }},
                )
                if WRITE_DECISION_SURFACE:
                    write_json(
                        candidate_dir / "decision_surface.json",
                        {{
                            "trainer_entrypoint": {{
                                "dataset_window": {{
                                    "start": arg_value("--start"),
                                    "end": arg_value("--end"),
                                }}
                            }}
                        }},
                    )
                if family == "train_v5_fusion":
                    runtime_start = execution_eval_start or start_value
                    runtime_end = execution_eval_end or end_value
                    write_json(
                        candidate_dir / "fusion_runtime_input_contract.json",
                        {{
                            "snapshot_id": "snapshot-test-001",
                            "runtime_window": {{
                                "start": runtime_start,
                                "end": runtime_end,
                                "start_ts_ms": date_to_ts_ms(runtime_start),
                                "end_ts_ms": date_to_ts_ms(runtime_end, end_of_day=True),
                            }},
                            "coverage_start_ts_ms": date_to_ts_ms(runtime_start),
                            "coverage_end_ts_ms": date_to_ts_ms(runtime_end, end_of_day=True),
                            "runtime_rows_after_date_filter": 12,
                            "runtime_dataset_root": str(candidate_dir / "runtime_feature_dataset"),
                        }},
                    )
                if EMIT_TRAIN_RUN_DIR and EMIT_CLI_PREFIXED_TRAIN_RUN_DIR:
                    print(f"[model][train][v4_crypto_cs] run_dir={{candidate_dir}}")
                elif EMIT_TRAIN_RUN_DIR:
                    print(json.dumps({{"run_dir": str(candidate_dir), "run_id": CANDIDATE_RUN_ID}}))
                else:
                    print("train_ok")
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "features", "build"):
                start_value = arg_value("--start")
                end_value = arg_value("--end")
                append_log(
                    {{
                        "command": "features build",
                        "start": start_value,
                        "end": end_value,
                    }}
                )
                key = f"{{start_value}}|{{end_value}}"
                rows_final = FEATURE_ROWS_BY_WINDOW.get(key)
                if rows_final is not None:
                    report_path = ROOT / "data" / "features" / "features_v4" / "_meta" / "build_report.json"
                    write_json(
                        report_path,
                        {{
                            "dataset_name": "features_v4",
                            "requested_start": start_value,
                            "requested_end": end_value,
                            "effective_start": start_value if int(rows_final) > 0 else "",
                            "effective_end": end_value if int(rows_final) > 0 else "",
                            "rows_final": int(rows_final),
                            "min_rows_for_train": FEATURE_MIN_ROWS_FOR_TRAIN,
                            "status": "PASS" if int(rows_final) >= FEATURE_MIN_ROWS_FOR_TRAIN else "FAIL",
                            "error_message": (
                                ""
                                if int(rows_final) >= FEATURE_MIN_ROWS_FOR_TRAIN
                                else f"NEED_MORE_MICRO_DAYS_OR_LOOSEN_UNIVERSE: rows_final={{int(rows_final)}} < min_rows_for_train={{FEATURE_MIN_ROWS_FOR_TRAIN}}"
                            ),
                        }},
                    )
                    print(f"[features][build][v4] report={{report_path}}")
                    if int(rows_final) < FEATURE_MIN_ROWS_FOR_TRAIN:
                        print(
                            f"[features][error] NEED_MORE_MICRO_DAYS_OR_LOOSEN_UNIVERSE: rows_final={{int(rows_final)}} < min_rows_for_train={{FEATURE_MIN_ROWS_FOR_TRAIN}}",
                            file=sys.stderr,
                        )
                        sys.exit(2)
                print("features_ok")
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "features", "validate"):
                start_value = arg_value("--start")
                end_value = arg_value("--end")
                append_log(
                    {{
                        "command": "features validate",
                        "start": start_value,
                        "end": end_value,
                    }}
                )
                report_path = ROOT / "data" / "features" / "features_v4" / "_meta" / "validate_report.json"
                if WRITE_FEATURE_VALIDATE_REPORT:
                    write_json(
                        report_path,
                        {{
                            "dataset_name": "features_v4",
                            "tf": "5m",
                            "quote": "KRW",
                            "checked_files": 1,
                            "ok_files": 1 if FEATURE_VALIDATE_FAIL_FILES == 0 and FEATURE_VALIDATE_LEAKAGE_SMOKE == "PASS" else 0,
                            "warn_files": 0,
                            "fail_files": FEATURE_VALIDATE_FAIL_FILES,
                            "schema_ok": FEATURE_VALIDATE_FAIL_FILES == 0,
                            "null_ratio_overall": 0.0,
                            "leakage_smoke": FEATURE_VALIDATE_LEAKAGE_SMOKE,
                            "staleness_fail_rows": 0,
                            "dropped_rows_no_micro": 0,
                            "details": [],
                        }},
                    )
                    print(f"[features][validate][v4] report={{report_path}}")
                if (not WRITE_FEATURE_VALIDATE_REPORT) or FEATURE_VALIDATE_FAIL_FILES > 0 or FEATURE_VALIDATE_LEAKAGE_SMOKE != "PASS":
                    print("[features][validate][v4] fail", file=sys.stderr)
                    sys.exit(2)
                print("features_validate_ok")
                sys.exit(0)

            if tuple(args[:3]) == ("-m", "autobot.ops.data_contract_registry", "--project-root"):
                append_log(
                    {{
                        "command": "data contract registry",
                        "project_root": arg_value("--project-root"),
                    }}
                )
                report_path = ROOT / "data" / "_meta" / "data_contract_registry.json"
                write_json(
                    report_path,
                    {{
                        "version": 1,
                        "entries": [
                            {{"contract_id": "raw_ws_dataset:upbit_public"}},
                            {{"contract_id": "raw_ticks_dataset:upbit_trades"}},
                            {{"contract_id": "parquet_dataset:candles_v1"}},
                            {{"contract_id": "micro_dataset:micro_v1"}},
                            {{"contract_id": "feature_dataset:features_v4"}},
                        ],
                        "summary": {{"contract_count": 5}},
                    }},
                )
                print(f"[ops][data-contract-registry] path={{report_path}}")
                sys.exit(0)

            if tuple(args[:3]) == ("-m", "autobot.ops.live_feature_parity_report", "--project-root"):
                report_path = ROOT / "data" / "features" / "features_v4" / "_meta" / "live_feature_parity_report.json"
                append_log(
                    {{
                        "command": "live feature parity",
                        "project_root": arg_value("--project-root"),
                    }}
                )
                if WRITE_LIVE_FEATURE_PARITY_REPORT:
                    write_json(
                        report_path,
                        {{
                            "artifact_version": 1,
                            "policy": "live_feature_parity_report_v1",
                            "sampled_pairs": 1,
                            "compared_pairs": 1,
                            "passing_pairs": 1 if LIVE_FEATURE_PARITY_ACCEPTABLE else 0,
                            "acceptable": LIVE_FEATURE_PARITY_ACCEPTABLE,
                            "status": "PASS" if LIVE_FEATURE_PARITY_ACCEPTABLE else "FAIL",
                            "details": [],
                        }},
                    )
                    print(f"[ops][live-feature-parity] path={{report_path}}")
                if (not WRITE_LIVE_FEATURE_PARITY_REPORT) or (not LIVE_FEATURE_PARITY_ACCEPTABLE):
                    print("[ops][live-feature-parity] fail", file=sys.stderr)
                    sys.exit(2)
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "model", "inspect-runtime-dataset"):
                dataset_root = Path(arg_value("--dataset-root"))
                contract_path = dataset_root.parent / "fusion_runtime_input_contract.json"
                contract = json.loads(contract_path.read_text(encoding="utf-8")) if contract_path.exists() else {{}}
                runtime_window = contract.get("runtime_window", {{}})
                runtime_start = runtime_window.get("start", "2026-03-06")
                runtime_end = runtime_window.get("end", runtime_start)
                append_log(
                    {{
                        "command": "model inspect-runtime-dataset",
                        "dataset_root": str(dataset_root),
                    }}
                )
                print(
                    json.dumps(
                        {{
                            "dataset_root": str(dataset_root),
                            "manifest_path": str(dataset_root / "_meta" / "manifest.parquet"),
                            "data_file_count": 1,
                            "rows": int(contract.get("runtime_rows_after_date_filter", 12) or 0),
                            "min_ts_ms": date_to_ts_ms(runtime_start),
                            "max_ts_ms": date_to_ts_ms(runtime_end, end_of_day=True),
                            "markets": ["KRW-BTC"],
                            "exists": True,
                            "manifest_exists": True,
                        }}
                    )
                )
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "backtest", "alpha"):
                model_ref = arg_value("--model-ref")
                start_value = arg_value("--start")
                end_value = arg_value("--end")
                preset = arg_value("--preset")
                append_log(
                    {{
                        "command": "backtest alpha",
                        "preset": preset,
                        "model_ref": model_ref,
                        "start": start_value,
                        "end": end_value,
                    }}
                )
                runs_dir = ROOT / "data" / "backtest" / "runs"
                run_dir = runs_dir / ("candidate" if model_ref == CANDIDATE_RUN_ID else "champion" if model_ref == CHAMPION_RUN_ID else model_ref)
                run_dir.mkdir(parents=True, exist_ok=True)
                if model_ref == CANDIDATE_RUN_ID:
                    payload = {{
                        "orders_submitted": CANDIDATE_ORDERS_SUBMITTED,
                        "orders_filled": CANDIDATE_ORDERS_FILLED,
                        "realized_pnl_quote": 250.0,
                        "fill_rate": 0.82,
                        "max_drawdown_pct": 0.05,
                        "slippage_bps_mean": 1.0,
                        "candidates_aborted_by_policy": CANDIDATE_CANDIDATES_ABORTED_BY_POLICY,
                        "execution_structure": CANDIDATE_EXECUTION_STRUCTURE,
                    }}
                else:
                    payload = HISTORY_ANCHOR_BACKTEST_BY_WINDOW.get(f"{{start_value}}|{{end_value}}")
                    if payload is None:
                        payload = {{
                            "orders_submitted": 64,
                            "orders_filled": 64,
                            "realized_pnl_quote": 100.0,
                            "fill_rate": 0.80,
                            "max_drawdown_pct": 0.08,
                            "slippage_bps_mean": 1.4,
                            "candidates_aborted_by_policy": 0,
                            "execution_structure": CHAMPION_EXECUTION_STRUCTURE,
                        }}
                write_json(run_dir / "summary.json", payload)
                print(json.dumps({{"run_dir": str(run_dir), "model_ref": model_ref}}))
                sys.exit(0)

            if tuple(args[:2]) == ("-m", "autobot.models.stat_validation"):
                print(
                    json.dumps(
                        {{
                            "comparable": True,
                            "deflated_sharpe_ratio_est": 0.75,
                            "probabilistic_sharpe_ratio": 0.90,
                        }}
                    )
                )
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


def _make_fake_daily_pipeline_script(
    tmp_path: Path,
    *,
    populate_batch_micro_date: bool = False,
    tf: str = "5m",
    market: str = "KRW-BTC",
) -> Path:
    script_path = tmp_path / "fake_daily_pipeline.ps1"
    script_body = textwrap.dedent(
        """
            param(
                [string]$PythonExe = "",
                [string]$ProjectRoot = "",
                [string]$Date = "",
                [string]$SmokeReportJson = "logs/paper_micro_smoke/latest.json",
                [switch]$SkipCandles,
                [switch]$SkipTicks,
                [switch]$SkipAggregate,
                [switch]$SkipValidate,
                [switch]$SkipSmoke,
                [switch]$SkipTieringRecommend
            )

            $ErrorActionPreference = "Stop"
            $logPath = Join-Path $ProjectRoot "logs/fake_daily_pipeline_invocations.jsonl"
            New-Item -ItemType Directory -Force -Path (Split-Path -Parent $logPath) | Out-Null
            if (__POPULATE_BATCH_MICRO_DATE__) {
                $partDir = Join-Path $ProjectRoot "data/parquet/micro_v1/tf=__TF__/market=__MARKET__/date=$Date"
                New-Item -ItemType Directory -Force -Path $partDir | Out-Null
                Set-Content -Path (Join-Path $partDir "part.parquet") -Value "placeholder" -Encoding UTF8
            }
            $entry = [ordered]@{
                date = $Date
                smoke_report_json = $SmokeReportJson
                skip_candles = [bool]$SkipCandles
                skip_ticks = [bool]$SkipTicks
                skip_aggregate = [bool]$SkipAggregate
                skip_validate = [bool]$SkipValidate
                skip_smoke = [bool]$SkipSmoke
                skip_tiering_recommend = [bool]$SkipTieringRecommend
            }
            ($entry | ConvertTo-Json -Compress) | Add-Content -Path $logPath -Encoding UTF8
            Write-Host "[daily-micro] report=ok"
            """
    )
    script_body = (
        script_body.replace("__POPULATE_BATCH_MICRO_DATE__", "$true" if populate_batch_micro_date else "$false")
        .replace("__TF__", tf)
        .replace("__MARKET__", market)
        .strip()
        + "\n"
    )
    script_path.write_text(script_body, encoding="utf-8")
    return script_path


def _write_micro_dates(project_root: Path, *, tf: str, market: str, dates: list[str]) -> None:
    for date_value in dates:
        part_dir = project_root / "data" / "parquet" / "micro_v1" / f"tf={tf}" / f"market={market}" / f"date={date_value}"
        part_dir.mkdir(parents=True, exist_ok=True)
        (part_dir / "part.parquet").write_text("placeholder", encoding="utf-8")
    close_report_path = project_root / "data" / "collect" / "_meta" / "train_snapshot_close_latest.json"
    if not close_report_path.exists():
        _seed_train_snapshot_close_contract(project_root, batch_date=sorted(dates)[-1], tf=tf)


def _seed_train_snapshot_close_contract(
    project_root: Path,
    *,
    batch_date: str,
    snapshot_id: str = "snapshot-test-001",
    tf: str = "5m",
) -> None:
    counts: dict[str, int] = {}
    tf_root = project_root / "data" / "parquet" / "micro_v1" / f"tf={tf}"
    if tf_root.exists():
        for market_dir in tf_root.glob("market=*"):
            for date_dir in market_dir.glob("date=*"):
                date_value = date_dir.name.removeprefix("date=")
                counts[date_value] = counts.get(date_value, 0) + 1
    coverage_start = min(counts.keys()) if counts else (datetime.fromisoformat(batch_date) - timedelta(days=7)).date().isoformat()

    _write_json(
        project_root / "data" / "_meta" / "data_platform_ready_snapshot.json",
        {"snapshot_id": snapshot_id},
    )
    _write_json(
        project_root / "data" / "collect" / "_meta" / "train_snapshot_close_latest.json",
        {
            "policy": "v5_train_snapshot_close_v1",
            "batch_date": batch_date,
            "snapshot_id": snapshot_id,
            "snapshot_root": str(project_root / "data" / "snapshots" / "data_platform" / snapshot_id),
            "published_at_utc": "2026-03-07T00:05:00Z",
            "generated_at_utc": "2026-03-07T00:05:00Z",
            "training_critical_start_date": coverage_start,
            "training_critical_end_date": batch_date,
            "coverage_window": {"start": coverage_start, "end": batch_date},
            "deadline_met": True,
            "overall_pass": True,
            "failure_reasons": [],
            "micro_root": str(project_root / "data" / "parquet" / "micro_v1"),
            "micro_date_coverage_counts": counts,
            "source_freshness": {
                "candles_api_refresh": {"pass": True},
                "raw_ticks_daily": {"pass": True, "batch_date": batch_date, "batch_covered": True},
            },
        },
    )


def _write_split_policy_selector_history(
    project_root: Path,
    *,
    task: str,
    records: list[dict[str, object]],
) -> Path:
    path = project_root / "models" / "registry" / "train_v4_crypto_cs" / f"split_policy_selector_history.{task}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "\n".join(json.dumps(record) for record in records)
    path.write_text((payload + "\n") if payload else "", encoding="utf-8")
    mirror = project_root / "models" / "registry" / "train_v5_fusion" / f"split_policy_selector_history.{task}.jsonl"
    mirror.parent.mkdir(parents=True, exist_ok=True)
    mirror.write_text((payload + "\n") if payload else "", encoding="utf-8")
    return path


def _run_acceptance(
    project_root: Path,
    python_exe: Path,
    daily_pipeline_script: Path,
    *,
    extra_args: list[str] | None = None,
) -> subprocess.CompletedProcess[str]:
    _seed_train_snapshot_close_contract(project_root, batch_date="2026-03-07")
    return subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(ACCEPTANCE_SCRIPT),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            str(python_exe),
            "-DailyPipelineScript",
            str(daily_pipeline_script),
            "-OutDir",
            "logs/test_acceptance",
            "-BatchDate",
            "2026-03-07",
            "-TrainLookbackDays",
            "3",
            "-BacktestLookbackDays",
            "2",
            "-SkipPaperSoak",
            "-SkipPromote",
            "-SkipReportRefresh",
            "-TrainerEvidenceMode",
            "required",
            *(extra_args or []),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def test_candidate_acceptance_ramps_train_window_from_available_micro_history(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v5_fusion" / "champion.json",
        {"run_id": "champion-run-000"},
    )
    _write_micro_dates(
        project_root,
        tf="5m",
        market="KRW-BTC",
        dates=["2026-03-04", "2026-03-05", "2026-03-06", "2026-03-07"],
    )

    python_exe = _make_fake_python_exe(tmp_path, write_decision_surface=True)
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = _run_acceptance(project_root, python_exe, daily_pipeline_script)

    assert result.returncode == 0, result.stdout + "\n" + result.stderr

    invocations = [
        json.loads(line)
        for line in (project_root / "logs" / "fake_python_invocations.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))

    assert [entry for entry in invocations if entry["command"] == "features build"] == [
        {"command": "features build", "start": "2026-03-04", "end": "2026-03-05"}
    ]
    assert [entry for entry in invocations if entry["command"] == "model train"] == [
        {"command": "model train", "start": "2026-03-04", "end": "2026-03-05"}
    ]
    assert report["config"]["train_lookback_days_requested"] == 3
    assert report["config"]["train_lookback_days_effective"] == 2
    assert report["config"]["train_window_ramp_active"] is True
    assert report["config"]["train_window_ramp_reason"] == "RAMP_ACTIVE"
    assert report["windows_by_step"]["train"]["start"] == "2026-03-04"
    assert report["windows_by_step"]["train"]["end"] == "2026-03-05"


def test_candidate_acceptance_fails_when_features_v4_validate_report_is_missing(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )
    _write_micro_dates(
        project_root,
        tf="5m",
        market="KRW-BTC",
        dates=["2026-03-04", "2026-03-05", "2026-03-06", "2026-03-07"],
    )

    python_exe = _make_fake_python_exe(
        tmp_path,
        write_decision_surface=True,
        write_feature_validate_report=False,
    )
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = _run_acceptance(project_root, python_exe, daily_pipeline_script)

    assert result.returncode == 2, result.stdout + "\n" + result.stderr
    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))
    assert report["reasons"] == ["FEATURES_VALIDATE_MISSING_OR_FAILED"]

    invocations = [
        json.loads(line)
        for line in (project_root / "logs" / "fake_python_invocations.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [entry for entry in invocations if entry["command"] == "features validate"] == [
        {"command": "features validate", "start": "2026-03-04", "end": "2026-03-05"}
    ]
    assert [entry for entry in invocations if entry["command"] == "model train"] == []


def test_candidate_acceptance_fails_when_live_feature_parity_report_is_missing_or_failed(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )
    _write_micro_dates(
        project_root,
        tf="5m",
        market="KRW-BTC",
        dates=["2026-03-04", "2026-03-05", "2026-03-06", "2026-03-07"],
    )

    python_exe = _make_fake_python_exe(
        tmp_path,
        write_decision_surface=True,
        live_feature_parity_acceptable=False,
    )
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = _run_acceptance(project_root, python_exe, daily_pipeline_script)

    assert result.returncode == 2, result.stdout + "\n" + result.stderr
    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))
    assert report["reasons"] == ["FEATURE_PARITY_MISSING_OR_FAILED"]
    assert report["gates"]["overall_pass"] is False
    assert report["steps"]["train"]["attempted"] is False


def test_candidate_acceptance_generates_data_contract_registry_before_training(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )
    _write_micro_dates(
        project_root,
        tf="5m",
        market="KRW-BTC",
        dates=["2026-03-04", "2026-03-05", "2026-03-06", "2026-03-07"],
    )

    python_exe = _make_fake_python_exe(tmp_path, write_decision_surface=True)
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = _run_acceptance(project_root, python_exe, daily_pipeline_script)

    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    invocations = [
        json.loads(line)
        for line in (project_root / "logs" / "fake_python_invocations.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [entry for entry in invocations if entry["command"] == "data contract registry"] == [
        {"command": "data contract registry", "project_root": str(project_root)}
    ]

    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))
    assert report["steps"]["data_contract_registry"]["contract_count"] == 5
    assert Path(report["steps"]["data_contract_registry"]["registry_path"]).exists()


def test_candidate_acceptance_uses_train_snapshot_close_coverage_for_window_ramp(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )
    _write_micro_dates(
        project_root,
        tf="5m",
        market="KRW-BTC",
        dates=["2026-03-03", "2026-03-04", "2026-03-05", "2026-03-06", "2026-03-07"],
    )

    python_exe = _make_fake_python_exe(tmp_path, write_decision_surface=True)
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(ACCEPTANCE_SCRIPT),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            str(python_exe),
            "-DailyPipelineScript",
            str(daily_pipeline_script),
            "-OutDir",
            "logs/test_acceptance",
            "-BatchDate",
            "2026-03-07",
            "-TrainLookbackDays",
            "3",
            "-BacktestLookbackDays",
            "2",
            "-SkipPaperSoak",
            "-SkipPromote",
            "-SkipReportRefresh",
            "-TrainerEvidenceMode",
            "required",
        ],
        cwd=REPO_ROOT,
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
    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))

    assert [entry for entry in invocations if entry["command"] == "features build"] == [
        {"command": "features build", "start": "2026-03-03", "end": "2026-03-05"}
    ]
    assert [entry for entry in invocations if entry["command"] == "model train"] == [
        {"command": "model train", "start": "2026-03-03", "end": "2026-03-05"}
    ]
    assert report["config"]["train_lookback_days_effective"] == 3
    assert report["config"]["train_window_ramp_reason"] == "TARGET_REACHED"
    assert report["config"]["train_window_ramp_available_contiguous_micro_days"] == 5
    assert report["steps"]["window_ramp_recomputed_after_pipeline"]["attempted"] is False
    assert report["steps"]["window_ramp_recomputed_after_pipeline"]["reason"] == "REPLACED_BY_TRAIN_SNAPSHOT_CLOSE"
    assert report["windows_by_step"]["train"]["start"] == "2026-03-03"
    assert report["windows_by_step"]["train"]["end"] == "2026-03-05"


def test_candidate_acceptance_selects_first_train_window_that_meets_trainable_row_budget(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )
    _write_micro_dates(
        project_root,
        tf="5m",
        market="KRW-BTC",
        dates=["2026-03-01", "2026-03-02", "2026-03-03", "2026-03-04", "2026-03-05", "2026-03-06", "2026-03-07"],
    )

    python_exe = _make_fake_python_exe(
        tmp_path,
        write_decision_surface=True,
        feature_rows_by_window={
            "2026-03-01|2026-03-05": 1200,
            "2026-03-02|2026-03-05": 2500,
            "2026-03-03|2026-03-05": 4300,
        },
    )
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(ACCEPTANCE_SCRIPT),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            str(python_exe),
            "-DailyPipelineScript",
            str(daily_pipeline_script),
            "-OutDir",
            "logs/test_acceptance",
            "-BatchDate",
            "2026-03-07",
            "-TrainLookbackDays",
            "5",
            "-BacktestLookbackDays",
            "2",
            "-SkipPaperSoak",
            "-SkipPromote",
            "-SkipReportRefresh",
            "-TrainerEvidenceMode",
            "required",
        ],
        cwd=REPO_ROOT,
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
    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))

    assert [entry for entry in invocations if entry["command"] == "features build"] == [
        {"command": "features build", "start": "2026-03-01", "end": "2026-03-05"},
        {"command": "features build", "start": "2026-03-02", "end": "2026-03-05"},
        {"command": "features build", "start": "2026-03-03", "end": "2026-03-05"},
    ]
    assert [entry for entry in invocations if entry["command"] == "model train"] == [
        {"command": "model train", "start": "2026-03-03", "end": "2026-03-05"}
    ]
    assert report["windows_by_step"]["train"]["start"] == "2026-03-03"
    assert report["windows_by_step"]["train"]["end"] == "2026-03-05"
    assert report["steps"]["features_build"]["rows_final"] == 4300
    assert report["steps"]["features_build"]["attempts"][0]["rows_final"] == 1200
    assert report["steps"]["features_build"]["attempts"][1]["rows_final"] == 2500
    assert report["steps"]["features_build"]["attempts"][2]["rows_final"] == 4300


def test_candidate_acceptance_fails_explicitly_when_no_train_window_meets_min_rows(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )
    _write_micro_dates(
        project_root,
        tf="5m",
        market="KRW-BTC",
        dates=["2026-03-01", "2026-03-02", "2026-03-03", "2026-03-04", "2026-03-05", "2026-03-06", "2026-03-07"],
    )

    python_exe = _make_fake_python_exe(
        tmp_path,
        write_decision_surface=True,
        feature_rows_by_window={
            "2026-03-01|2026-03-05": 1200,
            "2026-03-02|2026-03-05": 1100,
            "2026-03-03|2026-03-05": 900,
            "2026-03-04|2026-03-05": 600,
            "2026-03-05|2026-03-05": 200,
        },
    )
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(ACCEPTANCE_SCRIPT),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            str(python_exe),
            "-DailyPipelineScript",
            str(daily_pipeline_script),
            "-OutDir",
            "logs/test_acceptance",
            "-BatchDate",
            "2026-03-07",
            "-TrainLookbackDays",
            "5",
            "-BacktestLookbackDays",
            "2",
            "-SkipPaperSoak",
            "-SkipPromote",
            "-SkipReportRefresh",
            "-TrainerEvidenceMode",
            "required",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2, result.stdout + "\n" + result.stderr

    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))

    assert report["reasons"] == ["INSUFFICIENT_TRAINABLE_V4_ROWS"]
    assert report["steps"]["train"]["reason"] == "INSUFFICIENT_TRAINABLE_V4_ROWS"
    assert report["steps"]["train"]["best_attempt"]["rows_final"] == 1200
    assert report["steps"]["features_build"]["resolution_status"] == "INSUFFICIENT_TRAINABLE_ROWS"
    assert len(report["steps"]["features_build"]["attempts"]) == 5


def test_candidate_acceptance_falls_back_to_bootstrap_latest_inclusive_lane_when_strict_split_is_untrainable(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )
    _write_micro_dates(
        project_root,
        tf="5m",
        market="KRW-BTC",
        dates=[
            "2026-03-04",
            "2026-03-05",
            "2026-03-06",
            "2026-03-07",
            "2026-03-08",
            "2026-03-09",
            "2026-03-10",
            "2026-03-11",
            "2026-03-12",
        ],
    )

    python_exe = _make_fake_python_exe(
        tmp_path,
        write_decision_surface=True,
        feature_rows_by_window={
            "2026-03-04|2026-03-04": 899,
            "2026-03-04|2026-03-12": 11628,
        },
    )
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(ACCEPTANCE_SCRIPT),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            str(python_exe),
            "-DailyPipelineScript",
            str(daily_pipeline_script),
            "-OutDir",
            "logs/test_acceptance",
            "-BatchDate",
            "2026-03-12",
            "-TrainLookbackDays",
            "30",
            "-BacktestLookbackDays",
            "8",
            "-TrainDataQualityFloorDate",
            "2026-03-04",
            "-SkipPaperSoak",
            "-SkipPromote",
            "-SkipReportRefresh",
            "-TrainerEvidenceMode",
            "required",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2, result.stdout + "\n" + result.stderr

    invocations = [
        json.loads(line)
        for line in (project_root / "logs" / "fake_python_invocations.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))
    split_policy_path = Path(report["candidate"]["split_policy_artifact_path"])
    split_policy = json.loads(split_policy_path.read_text(encoding="utf-8-sig"))

    assert [entry for entry in invocations if entry["command"] == "features build"] == [
        {"command": "features build", "start": "2026-03-04", "end": "2026-03-04"},
        {"command": "features build", "start": "2026-03-04", "end": "2026-03-12"},
    ]
    assert [entry for entry in invocations if entry["command"] == "model train"] == [
        {"command": "model train", "start": "2026-03-04", "end": "2026-03-12"}
    ]
    assert report["reasons"] == ["BOOTSTRAP_ONLY_POLICY"]
    assert report["candidate"]["lane_mode"] == "bootstrap_latest_inclusive"
    assert report["candidate"]["promotion_eligible"] is False
    assert report["split_policy"]["lane_mode"] == "bootstrap_latest_inclusive"
    assert report["split_policy"]["promotion_eligible"] is False
    assert report["split_policy"]["selected_holdout_days"] == 0
    assert report["steps"]["features_build"]["resolution_status"] == "BOOTSTRAP_ONLY_POLICY"
    assert report["steps"]["runtime_dataset_coverage_preflight"]["attempted"] is False
    assert report["steps"]["runtime_dataset_coverage_preflight"]["reason"] == "BOOTSTRAP_OR_NO_CERTIFICATION_WINDOW"
    assert report["steps"]["features_build"]["strict_best_attempt"]["rows_final"] == 899
    assert report["steps"]["features_build"]["bootstrap_attempt"]["rows_final"] == 11628
    assert report["windows_by_step"]["train"]["start"] == "2026-03-04"
    assert report["windows_by_step"]["train"]["end"] == "2026-03-12"
    assert split_policy["lane_mode"] == "bootstrap_latest_inclusive"
    assert split_policy["promotion_eligible"] is False
    assert split_policy["current_batch_windows"]["bootstrap"]["start"] == "2026-03-04"
    assert split_policy["current_batch_windows"]["bootstrap"]["end"] == "2026-03-12"


def test_candidate_acceptance_selects_holdout_by_forward_validation_lcb_when_history_exists(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )
    _write_micro_dates(
        project_root,
        tf="5m",
        market="KRW-BTC",
        dates=["2026-03-01", "2026-03-02", "2026-03-03", "2026-03-04", "2026-03-05", "2026-03-06", "2026-03-07"],
    )
    _write_split_policy_selector_history(
        project_root,
        task="cls",
        records=[
            {"task": "cls", "holdout_days": 1, "anchor_date": "2026-03-05", "status": "EVALUATED", "utility_score": 10.0},
            {"task": "cls", "holdout_days": 1, "anchor_date": "2026-03-06", "status": "EVALUATED", "utility_score": 9.0},
            {"task": "cls", "holdout_days": 2, "anchor_date": "2026-03-05", "status": "EVALUATED", "utility_score": 16.0},
            {"task": "cls", "holdout_days": 2, "anchor_date": "2026-03-06", "status": "EVALUATED", "utility_score": 15.0},
        ],
    )

    python_exe = _make_fake_python_exe(
        tmp_path,
        write_decision_surface=True,
        feature_rows_by_window={
            "2026-03-01|2026-03-06": 5200,
            "2026-03-01|2026-03-05": 5100,
        },
    )
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(ACCEPTANCE_SCRIPT),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            str(python_exe),
            "-DailyPipelineScript",
            str(daily_pipeline_script),
            "-OutDir",
            "logs/test_acceptance",
            "-BatchDate",
            "2026-03-07",
            "-TrainDataQualityFloorDate",
            "2026-03-01",
            "-BacktestLookbackDays",
            "2",
            "-SplitPolicyHistoricalSelectorEnabled",
            "-SplitPolicyCandidateHoldoutDays",
            "1,2",
            "-SplitPolicyMinHistoricalAnchors",
            "2",
            "-SplitPolicyMaxNewAnchorEvaluationsPerRun",
            "0",
            "-SkipPaperSoak",
            "-SkipPromote",
            "-SkipReportRefresh",
            "-TrainerEvidenceMode",
            "required",
        ],
        cwd=REPO_ROOT,
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
    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))

    assert [entry for entry in invocations if entry["command"] == "features build"] == [
        {"command": "features build", "start": "2026-03-01", "end": "2026-03-07"},
        {"command": "features build", "start": "2026-03-01", "end": "2026-03-06"},
        {"command": "features build", "start": "2026-03-01", "end": "2026-03-05"},
        {"command": "features build", "start": "2026-03-01", "end": "2026-03-05"},
    ]
    assert [entry for entry in invocations if entry["command"] == "model train"] == [
        {"command": "model train", "start": "2026-03-01", "end": "2026-03-05"}
    ]
    assert report["split_policy"]["lane_mode"] == "promotion_strict"
    assert report["split_policy"]["selected_by"] == "forward_validation_lcb"
    assert report["split_policy"]["selected_holdout_days"] == 2
    assert report["split_policy"]["historical_anchor_count"] == 2
    assert report["steps"]["split_policy_selector"]["selected_holdout_days"] == 2
    assert report["windows_by_step"]["train"]["start"] == "2026-03-01"
    assert report["windows_by_step"]["train"]["end"] == "2026-03-05"
    assert report["config"]["train_lookback_days_effective"] == 5
    assert report["steps"]["backtest_candidate"]["start"] == "2026-03-06"
    assert report["steps"]["backtest_candidate"]["end"] == "2026-03-07"


def test_candidate_acceptance_backfills_selector_history_and_selects_holdout_when_budget_allows(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )
    _write_micro_dates(
        project_root,
        tf="5m",
        market="KRW-BTC",
        dates=["2026-03-01", "2026-03-02", "2026-03-03", "2026-03-04", "2026-03-05", "2026-03-06"],
    )

    python_exe = _make_fake_python_exe(
        tmp_path,
        write_decision_surface=True,
        feature_rows_by_window={
            "2026-03-01|2026-03-01": 4500,
            "2026-03-01|2026-03-05": 5200,
        },
        history_anchor_backtest_by_window={
            "2026-03-02|2026-03-02": {
                "orders_filled": 12,
                "realized_pnl_quote": 120.0,
                "fill_rate": 0.71,
                "max_drawdown_pct": 0.10,
                "slippage_bps_mean": 1.1,
            }
        },
    )
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(ACCEPTANCE_SCRIPT),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            str(python_exe),
            "-DailyPipelineScript",
            str(daily_pipeline_script),
            "-OutDir",
            "logs/test_acceptance",
            "-BatchDate",
            "2026-03-06",
            "-TrainDataQualityFloorDate",
            "2026-03-01",
            "-BacktestLookbackDays",
            "1",
            "-SplitPolicyHistoricalSelectorEnabled",
            "-SplitPolicyCandidateHoldoutDays",
            "1",
            "-SplitPolicyMinHistoricalAnchors",
            "1",
            "-SplitPolicyMaxNewAnchorEvaluationsPerRun",
            "1",
            "-SkipPaperSoak",
            "-SkipPromote",
            "-SkipReportRefresh",
            "-TrainerEvidenceMode",
            "required",
        ],
        cwd=REPO_ROOT,
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
    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))
    history_path = project_root / "models" / "registry" / "train_v5_fusion" / "split_policy_selector_history.cls.jsonl"
    history_records = [
        json.loads(line)
        for line in history_path.read_text(encoding="utf-8-sig").splitlines()
        if line.strip()
    ]

    assert [entry for entry in invocations if entry["command"] == "model train"] == [
        {"command": "model train", "start": "2026-03-01", "end": "2026-03-01", "run_scope": "scheduled_split_policy_history"},
        {"command": "model train", "start": "2026-03-01", "end": "2026-03-05"},
    ]
    assert report["split_policy"]["lane_mode"] == "promotion_strict"
    assert report["split_policy"]["selected_holdout_days"] == 1
    assert report["steps"]["split_policy_selector"]["new_evaluation_count"] == 1
    assert len(history_records) == 1
    assert history_records[0]["holdout_days"] == 1
    assert history_records[0]["anchor_date"] == "2026-03-02"
    assert history_records[0]["status"] == "EVALUATED"
    assert history_records[0]["utility_score"] == 1200.0


def test_candidate_acceptance_ignores_selector_history_before_quality_floor(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )
    _write_micro_dates(
        project_root,
        tf="5m",
        market="KRW-BTC",
        dates=["2026-03-04", "2026-03-05", "2026-03-06", "2026-03-07"],
    )
    _write_split_policy_selector_history(
        project_root,
        task="cls",
        records=[
            {"task": "cls", "holdout_days": 2, "anchor_date": "2026-03-05", "status": "EVALUATED", "utility_score": 999.0},
            {"task": "cls", "holdout_days": 2, "anchor_date": "2026-03-06", "status": "EVALUATED", "utility_score": 10.0},
        ],
    )

    python_exe = _make_fake_python_exe(
        tmp_path,
        write_decision_surface=True,
        feature_rows_by_window={
            "2026-03-04|2026-03-05": 5200,
        },
    )
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(ACCEPTANCE_SCRIPT),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            str(python_exe),
            "-DailyPipelineScript",
            str(daily_pipeline_script),
            "-OutDir",
            "logs/test_acceptance",
            "-BatchDate",
            "2026-03-07",
            "-TrainDataQualityFloorDate",
            "2026-03-04",
            "-BacktestLookbackDays",
            "2",
            "-SplitPolicyHistoricalSelectorEnabled",
            "-SplitPolicyCandidateHoldoutDays",
            "2",
            "-SplitPolicyMinHistoricalAnchors",
            "1",
            "-SplitPolicyMaxNewAnchorEvaluationsPerRun",
            "0",
            "-SkipPaperSoak",
            "-SkipPromote",
            "-SkipReportRefresh",
            "-TrainerEvidenceMode",
            "required",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + "\n" + result.stderr

    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))

    assert report["split_policy"]["selected_holdout_days"] == 2
    assert report["split_policy"]["historical_anchor_count"] == 1
    assert report["windows_by_step"]["train"]["start"] == "2026-03-04"
    assert report["windows_by_step"]["train"]["end"] == "2026-03-05"
    assert report["config"]["train_lookback_days_effective"] == 2


def test_candidate_acceptance_applies_train_data_quality_floor_date(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )
    _write_micro_dates(
        project_root,
        tf="5m",
        market="KRW-BTC",
        dates=["2026-03-01", "2026-03-02", "2026-03-03", "2026-03-04", "2026-03-05", "2026-03-06", "2026-03-07"],
    )

    python_exe = _make_fake_python_exe(tmp_path, write_decision_surface=True)
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(ACCEPTANCE_SCRIPT),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            str(python_exe),
            "-DailyPipelineScript",
            str(daily_pipeline_script),
            "-OutDir",
            "logs/test_acceptance",
            "-BatchDate",
            "2026-03-07",
            "-TrainLookbackDays",
            "5",
            "-BacktestLookbackDays",
            "1",
            "-TrainDataQualityFloorDate",
            "2026-03-04",
            "-SkipPaperSoak",
            "-SkipPromote",
            "-SkipReportRefresh",
            "-TrainerEvidenceMode",
            "required",
        ],
        cwd=REPO_ROOT,
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
    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))

    assert [entry for entry in invocations if entry["command"] == "features build"] == [
        {"command": "features build", "start": "2026-03-04", "end": "2026-03-06"}
    ]
    assert [entry for entry in invocations if entry["command"] == "model train"] == [
        {"command": "model train", "start": "2026-03-04", "end": "2026-03-06"}
    ]
    assert report["config"]["train_data_quality_floor_date"] == "2026-03-04"
    assert report["config"]["train_data_quality_floor_applied"] is True
    assert report["config"]["train_start_floor_date"] == "2026-03-04"
    assert report["config"]["train_start_floor_applied"] is True
    assert report["config"]["train_lookback_days_effective"] == 3
    assert report["config"]["train_window_ramp_reason"] == "TRAIN_DATA_QUALITY_FLOOR_ACTIVE"
    assert report["windows_by_step"]["train"]["start"] == "2026-03-04"
    assert report["windows_by_step"]["train"]["end"] == "2026-03-06"


def test_candidate_acceptance_writes_certification_artifact_and_separates_windows(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )

    python_exe = _make_fake_python_exe(tmp_path, write_decision_surface=True)
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = _run_acceptance(project_root, python_exe, daily_pipeline_script)

    assert result.returncode == 0, result.stdout + "\n" + result.stderr

    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))
    invocations = [
        json.loads(line)
        for line in (project_root / "logs" / "fake_python_invocations.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    certification_path = Path(report["candidate"]["certification_artifact_path"])
    certification = json.loads(certification_path.read_text(encoding="utf-8-sig"))

    assert report["steps"]["train"]["start"] == "2026-03-03"
    assert report["steps"]["train"]["end"] == "2026-03-05"
    assert report["candidate"]["search_budget_decision_path"].endswith("search_budget_decision.json")
    assert report["candidate"]["economic_objective_profile_path"].endswith("economic_objective_profile.json")
    assert report["candidate"]["economic_objective_profile_id"] == "v4_shared_economic_objective_v3"
    assert report["steps"]["backtest_candidate"]["start"] == "2026-03-06"
    assert report["steps"]["backtest_candidate"]["end"] == "2026-03-07"
    assert report["steps"]["train"]["trainer_evidence"]["source"] == "certification_artifact"
    assert report["gates"]["backtest"]["trainer_evidence_gate_pass"] is True
    assert report["gates"]["backtest"]["budget_contract_gate_pass"] is True
    assert report["gates"]["backtest"]["budget_lane_class_effective"] == "promotion_eligible"
    assert report["gates"]["backtest"]["certification_window_valid"] is True
    assert report["gates"]["backtest"]["decision_basis"] == "PARETO_DOMINANCE"

    assert [entry for entry in invocations if entry["command"] == "features build"] == [
        {"command": "features build", "start": "2026-03-03", "end": "2026-03-05"}
    ]
    assert [entry for entry in invocations if entry["command"] == "model train"] == [
        {"command": "model train", "start": "2026-03-03", "end": "2026-03-05"}
    ]

    assert certification["provenance"]["trainer_evidence_source"] == "certification_artifact"
    assert certification["provenance"]["research_evidence_source"] == "certification_lane_backtest"
    assert certification["provenance"]["trainer_research_prior_source"] == "trainer_research_evidence_artifact"
    assert certification["provenance"]["economic_objective_profile_present"] is True
    assert certification["provenance"]["economic_objective_profile_id"] == "v4_shared_economic_objective_v3"
    assert certification["windows"]["train_window"]["start"] == "2026-03-03"
    assert certification["windows"]["train_window"]["end"] == "2026-03-05"
    assert certification["windows"]["research_window"]["start"] == "2026-03-03"
    assert certification["windows"]["research_window"]["end"] == "2026-03-05"
    assert certification["windows"]["certification_window"]["start"] == "2026-03-06"
    assert certification["windows"]["certification_window"]["end"] == "2026-03-07"
    assert certification["valid_window_contract"] is True
    assert certification["certification"]["evaluated"] is True
    assert certification["certification"]["gate"]["pass"] is True
    assert certification["research_evidence"]["source"] == "certification_lane_backtest"
    assert certification["research_evidence"]["policy"] == "candidate_acceptance_certification_research_evidence_v1"


def test_candidate_acceptance_passes_certification_window_into_trainer_internal_execution_eval(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )
    _write_micro_dates(
        project_root,
        tf="5m",
        market="KRW-BTC",
        dates=["2026-03-03", "2026-03-04", "2026-03-05", "2026-03-06", "2026-03-07"],
    )

    python_exe = _make_fake_python_exe(tmp_path, write_decision_surface=True)
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(ACCEPTANCE_SCRIPT),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            str(python_exe),
            "-DailyPipelineScript",
            str(daily_pipeline_script),
            "-OutDir",
            "logs/test_acceptance",
            "-BatchDate",
            "2026-03-07",
            "-TrainLookbackDays",
            "3",
            "-BacktestLookbackDays",
            "2",
            "-SkipPaperSoak",
            "-SkipPromote",
            "-SkipReportRefresh",
            "-TrainerEvidenceMode",
            "required",
        ],
        cwd=REPO_ROOT,
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

    assert [entry for entry in invocations if entry["command"] == "model train eval window"] == [
        {"command": "model train eval window", "start": "2026-03-06", "end": "2026-03-07"}
    ]


def test_candidate_acceptance_resolves_fresh_run_from_train_stdout_when_candidate_pointer_is_not_updated(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )

    python_exe = _make_fake_python_exe(
        tmp_path,
        write_decision_surface=True,
        write_latest_candidate_pointer=False,
    )
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = _run_acceptance(project_root, python_exe, daily_pipeline_script)

    assert result.returncode == 0, result.stdout + "\n" + result.stderr

    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))
    certification_path = Path(report["candidate"]["certification_artifact_path"])
    certification = json.loads(certification_path.read_text(encoding="utf-8-sig"))
    artifact_status = json.loads((Path(report["candidate"]["run_dir"]) / "artifact_status.json").read_text(encoding="utf-8-sig"))

    assert report["candidate"]["run_id"] == "candidate-run-001"
    assert Path(report["candidate"]["run_dir"]).name == "candidate-run-001"
    assert report["gates"]["backtest"]["pass"] is True
    assert certification["candidate_run_id"] == "candidate-run-001"
    assert artifact_status["acceptance_completed"] is True
    assert artifact_status["candidate_adoptable"] is True
    assert artifact_status["candidate_adopted"] is True
    assert artifact_status["promoted"] is False


def test_candidate_acceptance_resolves_fresh_run_from_cli_prefixed_train_stdout(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )

    python_exe = _make_fake_python_exe(
        tmp_path,
        write_decision_surface=True,
        write_latest_candidate_pointer=False,
        emit_cli_prefixed_train_run_dir=True,
    )
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = _run_acceptance(project_root, python_exe, daily_pipeline_script)

    assert result.returncode == 0, result.stdout + "\n" + result.stderr

    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))
    assert report["candidate"]["run_id"] == "candidate-run-001"
    assert Path(report["candidate"]["run_dir"]).name == "candidate-run-001"


def test_candidate_acceptance_does_not_fall_back_to_latest_when_train_stdout_has_no_run_dir(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "latest.json",
        {"run_id": "stale-latest-run"},
    )

    python_exe = _make_fake_python_exe(
        tmp_path,
        write_decision_surface=True,
        emit_train_run_dir=False,
    )
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = _run_acceptance(project_root, python_exe, daily_pipeline_script)

    assert result.returncode == 2, result.stdout + "\n" + result.stderr

    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))

    assert report["reasons"] == ["TRAIN_OR_CANDIDATE_POINTER_FAILED"]
    assert report["steps"]["train"]["candidate_run_id"] == ""
    assert report["candidate"] == {}


def test_candidate_acceptance_required_trainer_evidence_fails_without_decision_surface(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )

    python_exe = _make_fake_python_exe(tmp_path, write_decision_surface=False)
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = _run_acceptance(project_root, python_exe, daily_pipeline_script)

    assert result.returncode == 2, result.stdout + "\n" + result.stderr

    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))
    certification_path = Path(report["candidate"]["certification_artifact_path"])
    certification = json.loads(certification_path.read_text(encoding="utf-8-sig"))

    assert report["gates"]["backtest"]["pass"] is False
    assert report["gates"]["backtest"]["decision_basis"] == "TRAINER_EVIDENCE_REQUIRED_FAIL"
    assert report["gates"]["backtest"]["trainer_evidence_gate_pass"] is False
    assert "MISSING_DECISION_SURFACE" in report["gates"]["backtest"]["trainer_evidence_reasons"]
    assert report["reasons"] == ["BACKTEST_ACCEPTANCE_FAILED", "TRAINER_EVIDENCE_REQUIRED_FAILED"]

    assert certification["valid_window_contract"] is False
    assert "MISSING_DECISION_SURFACE" in certification["reasons"]


def test_candidate_acceptance_rejects_scout_only_budget_evidence(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )

    python_exe = _make_fake_python_exe(
        tmp_path,
        write_decision_surface=True,
        budget_lane_class_requested="promotion_eligible",
        budget_lane_class_effective="scout",
        budget_contract_id="v4_promotion_eligible_budget_v1",
        budget_promotion_eligible_satisfied=False,
    )
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = _run_acceptance(project_root, python_exe, daily_pipeline_script)

    assert result.returncode == 2, result.stdout + "\n" + result.stderr

    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))
    certification_path = Path(report["candidate"]["certification_artifact_path"])
    certification = json.loads(certification_path.read_text(encoding="utf-8-sig"))

    assert report["gates"]["backtest"]["pass"] is False
    assert report["gates"]["backtest"]["trainer_evidence_gate_pass"] is True
    assert report["gates"]["backtest"]["budget_contract_gate_pass"] is False
    assert report["gates"]["backtest"]["budget_lane_class_requested"] == "promotion_eligible"
    assert report["gates"]["backtest"]["budget_lane_class_effective"] == "scout"
    assert report["gates"]["backtest"]["budget_promotion_eligible_satisfied"] is False
    assert report["gates"]["backtest"]["decision_basis"] == "SCOUT_ONLY_BUDGET_EVIDENCE"
    assert report["gates"]["backtest"]["budget_contract_reasons"] == ["SCOUT_ONLY_BUDGET_EVIDENCE"]
    assert report["gates"]["backtest"]["economic_objective_profile_id"] == "v4_shared_economic_objective_v3"
    assert report["reasons"] == ["BACKTEST_ACCEPTANCE_FAILED", "SCOUT_ONLY_BUDGET_EVIDENCE"]

    assert certification["valid_window_contract"] is True
    assert certification["certification"]["gate"]["budget_contract_gate_pass"] is False
    assert certification["certification"]["gate"]["decision_basis"] == "SCOUT_ONLY_BUDGET_EVIDENCE"
    assert certification["certification"]["gate"]["pass"] is False


def test_candidate_acceptance_certification_evidence_does_not_require_trainer_research_prior(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )

    python_exe = _make_fake_python_exe(
        tmp_path,
        write_decision_surface=True,
        write_trainer_research_evidence=False,
    )
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = _run_acceptance(project_root, python_exe, daily_pipeline_script)

    assert result.returncode == 0, result.stdout + "\n" + result.stderr

    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))
    certification_path = Path(report["candidate"]["certification_artifact_path"])
    certification = json.loads(certification_path.read_text(encoding="utf-8-sig"))

    assert report["steps"]["train"]["trainer_evidence"]["source"] == "certification_artifact"
    assert report["gates"]["backtest"]["trainer_evidence_gate_pass"] is True
    assert certification["provenance"]["trainer_research_prior_present"] is False
    assert certification["research_evidence"]["trainer_research_prior"]["present"] is False
    assert certification["research_evidence"]["pass"] is True
    assert certification["research_evidence"]["support_lane"]["summary"]["status"] == "missing_prior"


def test_candidate_acceptance_uses_profile_governed_backtest_thresholds(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )

    python_exe = _make_fake_python_exe(
        tmp_path,
        write_decision_surface=True,
        candidate_orders_filled=24,
        profile_candidate_min_orders_filled=70,
    )
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = _run_acceptance(project_root, python_exe, daily_pipeline_script)

    assert result.returncode == 2, result.stdout + "\n" + result.stderr

    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))

    assert report["config"]["backtest_min_orders_filled"] == 70
    assert report["config"]["promotion_policy_contract_source"] == "economic_objective_profile"
    assert report["gates"]["backtest"]["promotion_policy_contract_profile_id"] == "v4_shared_economic_objective_v3"
    assert report["gates"]["backtest"]["candidate_min_orders_threshold"] == 70
    assert report["gates"]["backtest"]["candidate_min_orders_pass"] is False
    assert report["gates"]["backtest"]["pass"] is False
    assert report["reasons"][0] == "BACKTEST_ACCEPTANCE_FAILED"


def test_candidate_acceptance_surfaces_execution_policy_veto_failure(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )

    python_exe = _make_fake_python_exe(
        tmp_path,
        write_decision_surface=True,
        candidate_orders_submitted=0,
        candidate_orders_filled=0,
        candidate_candidates_aborted_by_policy=17,
    )
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = _run_acceptance(
        project_root,
        python_exe,
        daily_pipeline_script,
    )

    assert result.returncode == 2, result.stdout + "\n" + result.stderr

    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))
    certification_path = Path(report["candidate"]["certification_artifact_path"])
    certification = json.loads(certification_path.read_text(encoding="utf-8-sig"))

    assert report["gates"]["backtest"]["pass"] is False
    assert report["gates"]["backtest"]["candidate_orders_submitted"] == 0
    assert report["gates"]["backtest"]["candidate_candidates_aborted_by_policy"] == 17
    assert report["gates"]["backtest"]["candidate_execution_policy_veto_failure"] is True
    assert report["gates"]["backtest"]["decision_basis"] == "EXECUTION_POLICY_VETO_FAILURE"
    assert "BACKTEST_ACCEPTANCE_FAILED" in report["reasons"]
    assert "EXECUTION_POLICY_VETO_FAILURE" in report["reasons"]
    assert "RUNTIME_PARITY_BACKTEST_FAILED" in report["reasons"]
    assert "RUNTIME_PARITY_EXECUTION_POLICY_VETO_FAILURE" in report["reasons"]
    assert certification["certification"]["gate"]["candidate_execution_policy_veto_failure"] is True
    assert certification["certification"]["gate"]["decision_basis"] == "EXECUTION_POLICY_VETO_FAILURE"


def test_candidate_acceptance_runs_runtime_parity_backtests_and_reports_gate(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )
    _write_micro_dates(
        project_root,
        tf="5m",
        market="KRW-BTC",
        dates=["2026-03-04", "2026-03-05", "2026-03-06", "2026-03-07"],
    )

    python_exe = _make_fake_python_exe(tmp_path, write_decision_surface=True)
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = _run_acceptance(project_root, python_exe, daily_pipeline_script)

    assert result.returncode == 0, result.stdout + "\n" + result.stderr

    invocations = [
        json.loads(line)
        for line in (project_root / "logs" / "fake_python_invocations.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    backtests = [item for item in invocations if item["command"] == "backtest alpha"]
    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))

    assert len(backtests) == 4
    assert [item["preset"] for item in backtests].count("acceptance") == 2
    assert [item["preset"] for item in backtests].count("runtime_parity") == 2
    assert report["steps"]["backtest_candidate"]["preset"] == "acceptance"
    assert report["steps"]["backtest_champion"]["preset"] == "acceptance"
    assert report["gates"]["runtime_parity"]["evaluated"] is True
    assert report["gates"]["runtime_parity"]["pass"] is True
    assert report["steps"]["backtest_runtime_parity_candidate"]["preset"] == "runtime_parity"
    assert report["steps"]["backtest_runtime_parity_champion"]["preset"] == "runtime_parity"


def test_candidate_acceptance_rejects_bad_backtest_execution_structure(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )

    python_exe = _make_fake_python_exe(
        tmp_path,
        write_decision_surface=True,
        candidate_execution_structure={
            "closed_trade_count": 5,
            "payoff_ratio": 0.4,
            "market_loss_concentration": 0.95,
        },
    )
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = _run_acceptance(project_root, python_exe, daily_pipeline_script)

    assert result.returncode == 2, result.stdout + "\n" + result.stderr

    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))

    assert report["gates"]["backtest"]["candidate_execution_structure_evaluated"] is True
    assert report["gates"]["backtest"]["candidate_payoff_ratio_pass"] is False
    assert report["gates"]["backtest"]["candidate_market_loss_concentration_pass"] is False
    assert "PAYOFF_RATIO_TOO_LOW" in report["reasons"]
    assert "LOSS_CONCENTRATION_TOO_HIGH" in report["reasons"]


def test_candidate_acceptance_reports_rank_shadow_lane_governance(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )

    python_exe = _make_fake_python_exe(tmp_path, write_decision_surface=True)
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = _run_acceptance(
        project_root,
        python_exe,
        daily_pipeline_script,
        extra_args=["-Task", "rank", "-RunScope", "manual_daily_rank_shadow_scout"],
    )

    assert result.returncode == 0, result.stdout + "\n" + result.stderr

    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))
    certification_path = Path(report["candidate"]["certification_artifact_path"])
    certification = json.loads(certification_path.read_text(encoding="utf-8-sig"))

    assert report["config"]["task"] == "rank"
    assert report["config"]["lane_id"] == "rank_shadow"
    assert report["config"]["lane_shadow_only"] is True
    assert report["candidate"]["lane_id"] == "rank_shadow"
    assert report["candidate"]["lane_shadow_only"] is True
    assert report["gates"]["backtest"]["lane_shadow_only"] is True
    assert "SHADOW_LANE_ONLY" in report["notes"]
    assert certification["provenance"]["lane_id"] == "rank_shadow"
    assert certification["provenance"]["lane_shadow_only"] is True
    assert certification["lane_governance"]["shadow_only"] is True


def test_candidate_acceptance_cli_override_can_relax_profile_thresholds(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )

    python_exe = _make_fake_python_exe(
        tmp_path,
        write_decision_surface=True,
        candidate_orders_filled=24,
        profile_candidate_min_orders_filled=70,
    )
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)
    result = _run_acceptance(
        project_root,
        python_exe,
        daily_pipeline_script,
        extra_args=["-BacktestMinOrdersFilled", "10"],
    )

    assert result.returncode == 0, result.stdout + "\n" + result.stderr

    report = json.loads((project_root / "logs" / "test_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))

    assert report["config"]["backtest_min_orders_filled"] == 10
    assert report["config"]["promotion_policy_cli_override_keys"] == ["backtest_min_orders_filled"]
    assert report["gates"]["backtest"]["promotion_policy_cli_override_keys"] == ["backtest_min_orders_filled"]
    assert report["gates"]["backtest"]["candidate_min_orders_threshold"] == 10
    assert report["gates"]["backtest"]["candidate_min_orders_pass"] is True
    assert report["gates"]["backtest"]["pass"] is True
