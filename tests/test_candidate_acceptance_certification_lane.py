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


def _make_fake_python_exe(
    tmp_path: Path,
    *,
    write_decision_surface: bool,
    write_trainer_research_evidence: bool = True,
    write_latest_candidate_pointer: bool = True,
    budget_lane_class_requested: str = "promotion_eligible",
    budget_lane_class_effective: str = "promotion_eligible",
    budget_contract_id: str = "v4_promotion_eligible_budget_v1",
    budget_promotion_eligible_satisfied: bool = True,
    candidate_orders_filled: int = 64,
    profile_candidate_min_orders_filled: int = 30,
) -> Path:
    driver_path = tmp_path / "fake_python_driver.py"
    driver_path.write_text(
        textwrap.dedent(
            f"""
            import json
            import sys
            from pathlib import Path

            ROOT = Path.cwd()
            CANDIDATE_RUN_ID = "candidate-run-001"
            CHAMPION_RUN_ID = "champion-run-000"
            WRITE_DECISION_SURFACE = {str(write_decision_surface)}
            WRITE_TRAINER_RESEARCH_EVIDENCE = {str(write_trainer_research_evidence)}
            WRITE_LATEST_CANDIDATE_POINTER = {str(write_latest_candidate_pointer)}
            BUDGET_LANE_CLASS_REQUESTED = {budget_lane_class_requested!r}
            BUDGET_LANE_CLASS_EFFECTIVE = {budget_lane_class_effective!r}
            BUDGET_CONTRACT_ID = {budget_contract_id!r}
            BUDGET_PROMOTION_ELIGIBLE_SATISFIED = {str(budget_promotion_eligible_satisfied)}
            CANDIDATE_ORDERS_FILLED = {int(candidate_orders_filled)}
            PROFILE_CANDIDATE_MIN_ORDERS_FILLED = {int(profile_candidate_min_orders_filled)}


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


            args = sys.argv[1:]
            command_key = tuple(args[:4])

            if command_key == ("-m", "autobot.cli", "model", "train"):
                family = arg_value("--model-family", "train_v4_crypto_cs")
                task = arg_value("--task", "cls").strip().lower() or "cls"
                run_scope = arg_value("--run-scope", "scheduled_daily")
                registry_dir = ROOT / "models" / "registry" / family
                candidate_dir = registry_dir / CANDIDATE_RUN_ID
                append_log(
                    {{
                        "command": "model train",
                        "start": arg_value("--start"),
                        "end": arg_value("--end"),
                    }}
                )
                if WRITE_LATEST_CANDIDATE_POINTER:
                    write_json(registry_dir / "latest_candidate.json", {{"run_id": CANDIDATE_RUN_ID}})
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
                        "version": 1,
                        "policy": "v4_shared_economic_objective_contract",
                        "profile_id": "v4_shared_economic_objective_v1",
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
                print(json.dumps({{"run_dir": str(candidate_dir), "run_id": CANDIDATE_RUN_ID}}))
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "features", "build"):
                append_log(
                    {{
                        "command": "features build",
                        "start": arg_value("--start"),
                        "end": arg_value("--end"),
                    }}
                )
                print("features_ok")
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "backtest", "alpha"):
                model_ref = arg_value("--model-ref")
                append_log(
                    {{
                        "command": "backtest alpha",
                        "model_ref": model_ref,
                        "start": arg_value("--start"),
                        "end": arg_value("--end"),
                    }}
                )
                runs_dir = ROOT / "data" / "backtest" / "runs"
                run_dir = runs_dir / ("candidate" if model_ref == CANDIDATE_RUN_ID else "champion")
                run_dir.mkdir(parents=True, exist_ok=True)
                if model_ref == CANDIDATE_RUN_ID:
                    payload = {{
                        "orders_filled": CANDIDATE_ORDERS_FILLED,
                        "realized_pnl_quote": 250.0,
                        "fill_rate": 0.82,
                        "max_drawdown_pct": 0.05,
                        "slippage_bps_mean": 1.0,
                    }}
                else:
                    payload = {{
                        "orders_filled": 64,
                        "realized_pnl_quote": 100.0,
                        "fill_rate": 0.80,
                        "max_drawdown_pct": 0.08,
                        "slippage_bps_mean": 1.4,
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


def _make_fake_daily_pipeline_script(tmp_path: Path) -> Path:
    script_path = tmp_path / "fake_daily_pipeline.ps1"
    script_path.write_text(
        textwrap.dedent(
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
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    return script_path


def _run_acceptance(
    project_root: Path,
    python_exe: Path,
    daily_pipeline_script: Path,
    *,
    extra_args: list[str] | None = None,
) -> subprocess.CompletedProcess[str]:
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
    assert report["candidate"]["economic_objective_profile_id"] == "v4_shared_economic_objective_v1"
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
    assert certification["provenance"]["economic_objective_profile_id"] == "v4_shared_economic_objective_v1"
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
    assert certification["research_evidence"]["trainer_research_prior"]["present"] is True
    assert certification["research_evidence"]["trainer_research_prior"]["pass"] is True
    assert certification["research_evidence"]["support_lane"]["summary"]["status"] == "supported"
    assert certification["research_evidence"]["support_lane"]["cpcv_lite"]["status"] == "partial"


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

    assert report["candidate"]["run_id"] == "candidate-run-001"
    assert Path(report["candidate"]["run_dir"]).name == "candidate-run-001"
    assert report["gates"]["backtest"]["pass"] is True
    assert certification["candidate_run_id"] == "candidate-run-001"


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
    assert report["gates"]["backtest"]["economic_objective_profile_id"] == "v4_shared_economic_objective_v1"
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
    assert report["gates"]["backtest"]["promotion_policy_contract_profile_id"] == "v4_shared_economic_objective_v1"
    assert report["gates"]["backtest"]["candidate_min_orders_threshold"] == 70
    assert report["gates"]["backtest"]["candidate_min_orders_pass"] is False
    assert report["gates"]["backtest"]["pass"] is False
    assert report["reasons"][0] == "BACKTEST_ACCEPTANCE_FAILED"


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
