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


def _make_fake_python_exe(tmp_path: Path, *, write_decision_surface: bool) -> Path:
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
                registry_dir = ROOT / "models" / "registry" / family
                candidate_dir = registry_dir / CANDIDATE_RUN_ID
                append_log(
                    {{
                        "command": "model train",
                        "start": arg_value("--start"),
                        "end": arg_value("--end"),
                    }}
                )
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
                print("train_ok")
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
                        "orders_filled": 64,
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


def _run_acceptance(project_root: Path, python_exe: Path, daily_pipeline_script: Path) -> subprocess.CompletedProcess[str]:
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
    assert report["steps"]["backtest_candidate"]["start"] == "2026-03-06"
    assert report["steps"]["backtest_candidate"]["end"] == "2026-03-07"
    assert report["steps"]["train"]["trainer_evidence"]["source"] == "certification_artifact"
    assert report["gates"]["backtest"]["trainer_evidence_gate_pass"] is True
    assert report["gates"]["backtest"]["certification_window_valid"] is True
    assert report["gates"]["backtest"]["decision_basis"] == "PARETO_DOMINANCE"

    assert [entry for entry in invocations if entry["command"] == "features build"] == [
        {"command": "features build", "start": "2026-03-03", "end": "2026-03-05"}
    ]
    assert [entry for entry in invocations if entry["command"] == "model train"] == [
        {"command": "model train", "start": "2026-03-03", "end": "2026-03-05"}
    ]

    assert certification["provenance"]["trainer_evidence_source"] == "certification_artifact"
    assert certification["provenance"]["research_evidence_source"] == "promotion_decision"
    assert certification["windows"]["train_window"]["start"] == "2026-03-03"
    assert certification["windows"]["train_window"]["end"] == "2026-03-05"
    assert certification["windows"]["research_window"]["start"] == "2026-03-03"
    assert certification["windows"]["research_window"]["end"] == "2026-03-05"
    assert certification["windows"]["certification_window"]["start"] == "2026-03-06"
    assert certification["windows"]["certification_window"]["end"] == "2026-03-07"
    assert certification["valid_window_contract"] is True
    assert certification["certification"]["evaluated"] is True
    assert certification["certification"]["gate"]["pass"] is True


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
