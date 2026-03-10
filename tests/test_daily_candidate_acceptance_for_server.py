import json
import shutil
import subprocess
import textwrap
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "daily_candidate_acceptance_for_server.ps1"


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required for this test")


def _make_fake_acceptance_script(tmp_path: Path, *, payload: dict, exit_code: int) -> Path:
    script_path = tmp_path / f"fake_acceptance_{exit_code}.ps1"
    payload_json = json.dumps(payload)
    script_path.write_text(
        textwrap.dedent(
            f"""
            param(
                [string]$ProjectRoot = "",
                [string]$PythonExe = "",
                [string]$BatchDate = "",
                [switch]$SkipDailyPipeline,
                [switch]$SkipReportRefresh,
                [switch]$DryRun
            )

            $reportPath = Join-Path $ProjectRoot "logs/fake_acceptance/report.json"
            New-Item -ItemType Directory -Force -Path (Split-Path -Parent $reportPath) | Out-Null
            @'
            {payload_json}
            '@ | Set-Content -Path $reportPath -Encoding UTF8
            Write-Host ("[fake-accept] report={{0}}" -f $reportPath)
            exit {exit_code}
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    return script_path


def test_daily_candidate_acceptance_treats_scout_only_budget_rejection_as_success(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        payload={
            "gates": {
                "backtest": {
                    "budget_contract_reasons": ["SCOUT_ONLY_BUDGET_EVIDENCE"],
                }
            },
            "reasons": ["BACKTEST_ACCEPTANCE_FAILED", "SCOUT_ONLY_BUDGET_EVIDENCE"],
        },
        exit_code=2,
    )

    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(SCRIPT_PATH),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            "python",
            "-AcceptanceScript",
            str(acceptance_script),
            "-BatchDate",
            "2026-03-08",
            "-SkipDailyPipeline",
            "-SkipReportRefresh",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    assert "scout_nonfatal_reason=SCOUT_ONLY_BUDGET_EVIDENCE" in completed.stdout


def test_daily_candidate_acceptance_preserves_fatal_acceptance_failure(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        payload={
            "steps": {
                "exception": {"message": "boom"},
            },
            "reasons": ["UNHANDLED_EXCEPTION"],
        },
        exit_code=2,
    )

    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(SCRIPT_PATH),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            "python",
            "-AcceptanceScript",
            str(acceptance_script),
            "-BatchDate",
            "2026-03-08",
            "-SkipDailyPipeline",
            "-SkipReportRefresh",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 2
