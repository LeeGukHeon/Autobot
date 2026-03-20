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


def _make_fake_python_exe(tmp_path: Path, *, allow_train: bool = True) -> Path:
    driver_path = tmp_path / "fake_python_driver.py"
    driver_path.write_text(
        textwrap.dedent(
            """
            import json
            import sys
            from pathlib import Path

            ROOT = Path.cwd()
            CANDIDATE_RUN_ID = "candidate-run-001"
            CHAMPION_RUN_ID = "champion-run-000"
            ALLOW_TRAIN = __ALLOW_TRAIN__


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


            args = sys.argv[1:]
            command_key = tuple(args[:4])

            if command_key == ("-m", "autobot.cli", "model", "train"):
                if not ALLOW_TRAIN:
                    print("unexpected train invocation", file=sys.stderr)
                    sys.exit(1)
                family = arg_value("--model-family", "train_v4_crypto_cs")
                registry_dir = ROOT / "models" / "registry" / family
                candidate_dir = registry_dir / CANDIDATE_RUN_ID
                write_json(registry_dir / "latest_candidate.json", {"run_id": CANDIDATE_RUN_ID})
                write_json(candidate_dir / "promotion_decision.json", {"status": "PASS"})
                print("train_ok")
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "backtest", "alpha"):
                model_ref = arg_value("--model-ref")
                runs_dir = ROOT / "data" / "backtest" / "runs"
                runs_dir.mkdir(parents=True, exist_ok=True)
                run_index = len([item for item in runs_dir.iterdir() if item.is_dir()]) + 1
                run_dir = runs_dir / f"run_{run_index:03d}_actual"
                run_dir.mkdir(parents=True, exist_ok=True)
                if model_ref == CANDIDATE_RUN_ID:
                    summary = {
                        "orders_filled": 64,
                        "realized_pnl_quote": 250.0,
                        "fill_rate": 0.82,
                        "max_drawdown_pct": 0.05,
                        "slippage_bps_mean": 1.0,
                    }
                elif model_ref == CHAMPION_RUN_ID:
                    summary = {
                        "orders_filled": 64,
                        "realized_pnl_quote": 100.0,
                        "fill_rate": 0.80,
                        "max_drawdown_pct": 0.08,
                        "slippage_bps_mean": 1.4,
                    }
                else:
                    summary = {
                        "orders_filled": 0,
                        "realized_pnl_quote": -100.0,
                        "fill_rate": 0.0,
                        "max_drawdown_pct": 1.0,
                        "slippage_bps_mean": 20.0,
                    }
                write_json(run_dir / "summary.json", summary)
                shadow_dir = runs_dir / f"run_{run_index:03d}_shadow"
                shadow_dir.mkdir(parents=True, exist_ok=True)
                write_json(
                    shadow_dir / "summary.json",
                    {
                        "orders_filled": 1,
                        "realized_pnl_quote": -999.0,
                        "fill_rate": 0.01,
                        "max_drawdown_pct": 0.99,
                        "slippage_bps_mean": 50.0,
                    },
                )
                print(json.dumps({"run_dir": str(run_dir), "model_ref": model_ref}))
                sys.exit(0)

            if tuple(args[:2]) == ("-m", "autobot.models.stat_validation"):
                print(
                    json.dumps(
                        {
                            "comparable": True,
                            "deflated_sharpe_ratio_est": 0.75,
                            "probabilistic_sharpe_ratio": 0.90,
                        }
                    )
                )
                sys.exit(0)

            if tuple(args[:2]) == ("-m", "autobot.common.operational_overlay_calibration"):
                output_path = arg_value("--output-path")
                if output_path:
                    write_json(
                        Path(output_path),
                        {"report_count": 1, "sufficient_reports": False, "applied_fields": []},
                    )
                print("{}")
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "model", "promote"):
                print("promote_ok")
                sys.exit(0)

            print("unexpected fake python invocation: " + " ".join(args), file=sys.stderr)
            sys.exit(1)
            """
        ).replace("__ALLOW_TRAIN__", "True" if allow_train else "False").strip()
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


def _write_existing_candidate_run(project_root: Path) -> None:
    run_dir = project_root / "models" / "registry" / "train_v4_crypto_cs" / "candidate-run-001"
    _write_json(project_root / "models" / "registry" / "train_v4_crypto_cs" / "latest_candidate.json", {"run_id": "candidate-run-001"})
    _write_json(run_dir / "promotion_decision.json", {"status": "candidate", "promote": False, "reasons": []})
    _write_json(
        run_dir / "trainer_research_evidence.json",
        {
            "available": True,
            "pass": True,
            "offline_pass": True,
            "execution_pass": True,
            "reasons": [],
            "checks": {},
            "offline": {},
            "execution": {},
        },
    )
    _write_json(
        run_dir / "search_budget_decision.json",
        {
            "status": "default",
            "lane_class_requested": "promotion_eligible",
            "lane_class_effective": "promotion_eligible",
            "budget_contract_id": "v4_promotion_eligible_budget_v1",
            "promotion_eligible_contract": {"satisfied": True},
        },
    )
    _write_json(run_dir / "economic_objective_profile.json", {"profile_id": "test_profile"})
    _write_json(
        run_dir / "lane_governance.json",
        {
            "policy": "v4_lane_governance_v1",
            "lane_id": "cls_primary",
            "task": "cls",
            "run_scope": "scheduled_daily",
            "lane_role": "primary",
            "shadow_only": False,
            "promotion_allowed": True,
            "governance_reasons": ["PRIMARY_LANE_ELIGIBLE"],
        },
    )
    _write_json(run_dir / "decision_surface.json", {"policy": {}})


def _make_fake_paper_smoke_script(tmp_path: Path) -> Path:
    script_path = tmp_path / "fake_paper_smoke.ps1"
    script_path.write_text(
        textwrap.dedent(
            """
            param(
                [string]$ProjectRoot = "",
                [string]$PythonExe = "",
                [int]$DurationSec = 0,
                [string]$Quote = "",
                [int]$TopN = 0,
                [double]$MaxFallbackRatio = 0.0,
                [int]$MinOrdersSubmitted = 0,
                [int]$MinTierCount = 0,
                [int]$MinPolicyEvents = 0,
                [string]$PaperMicroProvider = "",
                [int]$WarmupSec = 0,
                [int]$WarmupMinTradeEventsPerMarket = 0,
                [string]$Strategy = "",
                [string]$Tf = "",
                [string]$ModelRef = "",
                [string]$ModelFamily = "",
                [string]$FeatureSet = "",
                [double]$TopPct = -1.0,
                [double]$MinProb = -1.0,
                [int]$MinCandsPerTs = -1,
                [string]$ExitMode = "",
                [int]$HoldBars = -1,
                [string]$PaperFeatureProvider = "",
                [string]$OutDir = ""
            )

            $ErrorActionPreference = "Stop"
            if ($ModelRef -ne "candidate-run-001") {
                throw "unexpected paper ModelRef: $ModelRef"
            }

            New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
            $runReportPath = Join-Path $OutDir "paper_micro_smoke_20260308-000000.json"
            $latestReportPath = Join-Path $OutDir "latest.json"
            $payload = [ordered]@{
                generated_at = "2026-03-08T00:00:00Z"
                run_id = "paper-run-001"
                orders_submitted = 5
                orders_filled = 3
                fill_rate = 0.60
                realized_pnl_quote = 50.0
                max_drawdown_pct = 0.03
                slippage_bps_mean = 1.1
                micro_quality_score_mean = 0.80
                runtime_risk_multiplier_mean = 1.0
                rolling_active_windows = 2
                rolling_nonnegative_active_window_ratio = 1.0
                rolling_max_fill_concentration_ratio = 0.20
                replace_cancel_timeout_total = 0
                micro_missing_fallback_ratio = 0.0
                gates = [ordered]@{
                    smoke_connectivity_pass = $true
                    t15_gate_pass = $true
                }
            }
            $json = $payload | ConvertTo-Json -Depth 8
            $json | Set-Content -Path $runReportPath -Encoding UTF8
            $json | Set-Content -Path $latestReportPath -Encoding UTF8
            Write-Host ("[paper-smoke] report={0}" -f $runReportPath)
            Write-Host ("[paper-smoke] latest={0}" -f $latestReportPath)
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    return script_path


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required for this test")


def test_candidate_acceptance_pins_concrete_model_refs_for_backtest_and_paper(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )

    python_exe = _make_fake_python_exe(tmp_path)
    paper_smoke_script = _make_fake_paper_smoke_script(tmp_path)
    powershell_exe = _powershell_exe()

    command = [
        powershell_exe,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(ACCEPTANCE_SCRIPT),
        "-ProjectRoot",
        str(project_root),
        "-PythonExe",
        str(python_exe),
        "-PaperSmokeScript",
        str(paper_smoke_script),
        "-OutDir",
        "logs/test_acceptance",
        "-BatchDate",
        "2026-03-07",
        "-TrainLookbackDays",
        "2",
        "-BacktestLookbackDays",
        "2",
        "-SkipDailyPipeline",
        "-SkipReportRefresh",
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stdout + "\n" + result.stderr

    report_path = project_root / "logs" / "test_acceptance" / "latest.json"
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))

    candidate = report["candidate"]
    backtest_candidate = report["steps"]["backtest_candidate"]
    backtest_champion = report["steps"]["backtest_champion"]
    paper_candidate = report["steps"]["paper_candidate"]

    assert candidate["run_id"] == "candidate-run-001"
    assert candidate["candidate_model_ref_requested"] == "latest_candidate_v4"
    assert candidate["candidate_run_id_used_for_backtest"] == "candidate-run-001"
    assert candidate["candidate_run_id_used_for_paper"] == "candidate-run-001"
    assert candidate["champion_model_ref_requested"] == "champion_v4"
    assert candidate["champion_run_id_used_for_backtest"] == "champion-run-000"

    assert backtest_candidate["model_ref_requested"] == "latest_candidate_v4"
    assert backtest_candidate["model_ref_used"] == "candidate-run-001"
    assert "--model-ref candidate-run-001" in backtest_candidate["command"]
    assert backtest_candidate["run_dir"].endswith("run_001_actual")

    assert backtest_champion["model_ref_requested"] == "champion_v4"
    assert backtest_champion["model_ref_used"] == "champion-run-000"
    assert "--model-ref champion-run-000" in backtest_champion["command"]
    assert backtest_champion["run_dir"].endswith("run_003_actual")

    assert paper_candidate["model_ref_requested"] == "latest_candidate_v4"
    assert paper_candidate["model_ref_used"] == "candidate-run-001"
    assert "-ModelRef candidate-run-001" in paper_candidate["command"]
    assert paper_candidate["smoke_report_source"] == "run_report"

    assert report["gates"]["backtest"]["pass"] is True
    assert report["gates"]["paper"]["pass"] is True
    assert report["gates"]["overall_pass"] is True


def test_candidate_acceptance_can_reuse_existing_candidate_without_retraining(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )
    _write_existing_candidate_run(project_root)

    python_exe = _make_fake_python_exe(tmp_path, allow_train=False)
    paper_smoke_script = _make_fake_paper_smoke_script(tmp_path)
    powershell_exe = _powershell_exe()

    command = [
        powershell_exe,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(ACCEPTANCE_SCRIPT),
        "-ProjectRoot",
        str(project_root),
        "-PythonExe",
        str(python_exe),
        "-PaperSmokeScript",
        str(paper_smoke_script),
        "-OutDir",
        "logs/test_acceptance",
        "-BatchDate",
        "2026-03-07",
        "-TrainLookbackDays",
        "2",
        "-BacktestLookbackDays",
        "2",
        "-CandidateModelRef",
        "candidate-run-001",
        "-SkipTrain",
        "-SkipDailyPipeline",
        "-SkipReportRefresh",
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stdout + "\n" + result.stderr

    report_path = project_root / "logs" / "test_acceptance" / "latest.json"
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))

    train_step = report["steps"]["train"]
    candidate = report["candidate"]

    assert train_step["attempted"] is False
    assert train_step["reused_existing_candidate"] is True
    assert train_step["reason"] == "REUSED_EXISTING_CANDIDATE"
    assert train_step["candidate_run_id"] == "candidate-run-001"
    assert candidate["run_id"] == "candidate-run-001"
    assert candidate["candidate_model_ref_requested"] == "candidate-run-001"
    assert candidate["candidate_run_id_used_for_backtest"] == "candidate-run-001"
    assert candidate["candidate_run_id_used_for_paper"] == "candidate-run-001"
    assert report["gates"]["backtest"]["pass"] is True
    assert report["gates"]["paper"]["pass"] is True
    assert report["gates"]["overall_pass"] is True
