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


def _make_fake_python_exe(tmp_path: Path) -> Path:
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

            if command_key == ("-m", "autobot.cli", "features", "build"):
                append_log(
                    {
                        "command": "features build",
                        "feature_set": arg_value("--feature-set"),
                        "label_set": arg_value("--label-set"),
                        "start": arg_value("--start"),
                        "end": arg_value("--end"),
                    }
                )
                print("features_build_ok")
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "features", "validate"):
                report_path = ROOT / "data" / "features" / "features_v4" / "_meta" / "validate_report.json"
                write_json(
                    report_path,
                    {
                        "checked_files": 1,
                        "ok_files": 1,
                        "warn_files": 0,
                        "fail_files": 0,
                        "schema_ok": True,
                        "leakage_smoke": "PASS",
                    },
                )
                append_log(
                    {
                        "command": "features validate",
                        "feature_set": arg_value("--feature-set"),
                        "start": arg_value("--start"),
                        "end": arg_value("--end"),
                    }
                )
                print(f"[features][validate][v4] report={report_path}")
                sys.exit(0)

            if tuple(args[:3]) == ("-m", "autobot.ops.data_contract_registry", "--project-root"):
                report_path = ROOT / "data" / "_meta" / "data_contract_registry.json"
                write_json(
                    report_path,
                    {
                        "version": 1,
                        "entries": [{"contract_id": "feature_dataset:features_v4"}],
                        "summary": {"contract_count": 1},
                    },
                )
                append_log(
                    {
                        "command": "data contract registry",
                        "project_root": arg_value("--project-root"),
                    }
                )
                print(f"[ops][data-contract-registry] path={report_path}")
                sys.exit(0)

            if tuple(args[:3]) == ("-m", "autobot.ops.live_feature_parity_report", "--project-root"):
                report_path = ROOT / "data" / "features" / "features_v4" / "_meta" / "live_feature_parity_report.json"
                write_json(
                    report_path,
                    {
                        "artifact_version": 1,
                        "policy": "live_feature_parity_report_v1",
                        "sampled_pairs": 1,
                        "compared_pairs": 1,
                        "passing_pairs": 1,
                        "acceptable": True,
                        "status": "PASS",
                        "details": [],
                    },
                )
                append_log(
                    {
                        "command": "live feature parity",
                        "project_root": arg_value("--project-root"),
                    }
                )
                print(f"[ops][live-feature-parity] path={report_path}")
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "model", "train"):
                family = arg_value("--model-family", "train_v4_crypto_cs")
                registry_dir = ROOT / "models" / "registry" / family
                candidate_dir = registry_dir / CANDIDATE_RUN_ID
                append_log(
                    {
                        "command": "model train",
                        "feature_set": arg_value("--feature-set"),
                        "label_set": arg_value("--label-set"),
                        "start": arg_value("--start"),
                        "end": arg_value("--end"),
                    }
                )
                write_json(registry_dir / "latest.json", {"run_id": CANDIDATE_RUN_ID})
                write_json(candidate_dir / "promotion_decision.json", {"status": "PASS"})
                print(json.dumps({"run_dir": str(candidate_dir), "run_id": CANDIDATE_RUN_ID}))
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "backtest", "alpha"):
                model_ref = arg_value("--model-ref")
                runs_dir = ROOT / "data" / "backtest" / "runs"
                run_dir = runs_dir / ("candidate" if model_ref == CANDIDATE_RUN_ID else "champion")
                run_dir.mkdir(parents=True, exist_ok=True)
                payload = {
                    "orders_filled": 64,
                    "realized_pnl_quote": 250.0 if model_ref == CANDIDATE_RUN_ID else 100.0,
                    "fill_rate": 0.82,
                    "max_drawdown_pct": 0.05 if model_ref == CANDIDATE_RUN_ID else 0.08,
                    "slippage_bps_mean": 1.0 if model_ref == CANDIDATE_RUN_ID else 1.4,
                }
                write_json(run_dir / "summary.json", payload)
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
                        {"report_count": 0, "sufficient_reports": False, "applied_fields": []},
                    )
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
            $reportPath = Join-Path $ProjectRoot "logs/fake_daily_pipeline_report.txt"
            Set-Content -Path $reportPath -Value "ok" -Encoding UTF8
            Write-Host ("[daily-micro] report={0}" -f $reportPath)
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    return script_path


def test_candidate_acceptance_uses_nonempty_smoke_report_path_for_refresh_when_paper_is_skipped(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )

    python_exe = _make_fake_python_exe(tmp_path)
    daily_pipeline_script = _make_fake_daily_pipeline_script(tmp_path)

    completed = subprocess.run(
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
            "logs/model_acceptance_test",
            "-SkipPaperSoak",
            "-SkipPromote",
            "-TrainerEvidenceMode",
            "ignore",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "[candidate-accept] overall_pass=True" in completed.stdout

    invocations_path = project_root / "logs" / "fake_daily_pipeline_invocations.jsonl"
    entries = [
        json.loads(line)
        for line in invocations_path.read_text(encoding="utf-8-sig").splitlines()
        if line.strip()
    ]
    assert len(entries) == 2
    assert entries[0]["smoke_report_json"] == "logs/paper_micro_smoke/latest.json"

    expected_refresh_path = (
        project_root / "logs" / "model_acceptance_test" / "paper_smoke" / "latest.json"
    )
    assert entries[1]["smoke_report_json"] == str(expected_refresh_path)
    assert entries[1]["smoke_report_json"] != str(project_root)

    python_invocations_path = project_root / "logs" / "fake_python_invocations.jsonl"
    python_invocations = [
        json.loads(line)
        for line in python_invocations_path.read_text(encoding="utf-8-sig").splitlines()
        if line.strip()
    ]
    commands = [entry["command"] for entry in python_invocations]
    assert "features build" in commands
    assert "model train" in commands
    assert commands.index("features build") < commands.index("model train")

    latest_report = json.loads(
        (project_root / "logs" / "model_acceptance_test" / "latest.json").read_text(encoding="utf-8-sig")
    )
    assert latest_report["candidate_run_id"] == "candidate-run-001"
    assert latest_report["candidate_run_dir"].endswith("candidate-run-001")
    assert latest_report["overall_pass"] is True
    assert latest_report["backtest_pass"] is True
    assert latest_report["paper_pass"] is None
    assert latest_report["completed_at"]
    assert latest_report["steps"]["features_build"]["attempted"] is True
    assert latest_report["steps"]["features_build"]["feature_set"] == "v4"
    assert latest_report["steps"]["features_build"]["label_set"] == "v2"
    assert latest_report["reasons"] == []
    assert latest_report["notes"] == ["PAPER_SOAK_SKIPPED"]
    assert json.loads(
        (project_root / "models" / "registry" / "train_v4_crypto_cs" / "latest_candidate.json").read_text(encoding="utf-8-sig")
    )["run_id"] == "candidate-run-001"
