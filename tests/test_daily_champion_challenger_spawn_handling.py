import json
import shutil
import subprocess
import textwrap
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
DAILY_CC_SCRIPT = REPO_ROOT / "scripts" / "daily_champion_challenger_v4_for_server.ps1"


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required for this test")


def _make_fake_acceptance_script(tmp_path: Path, payload: dict, exit_code: int) -> Path:
    script_path = tmp_path / f"fake_acceptance_{exit_code}.ps1"
    payload_json = json.dumps(payload)
    script_path.write_text(
        textwrap.dedent(
            f"""
            param(
                [string]$ProjectRoot = "",
                [string]$PythonExe = "",
                [string]$BatchDate = "",
                [switch]$SkipPaperSoak,
                [switch]$SkipPromote,
                [switch]$SkipDailyPipeline,
                [switch]$SkipReportRefresh,
                [switch]$DryRun
            )

            $ErrorActionPreference = "Stop"
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


def _run_spawn_only(project_root: Path, acceptance_script: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(DAILY_CC_SCRIPT),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            "python",
            "-AcceptanceScript",
            str(acceptance_script),
            "-Mode",
            "spawn_only",
            "-DryRun",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )


def test_spawn_only_treats_candidate_rejection_as_successful_no_challenger_day(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "steps": {
                "train": {"candidate_run_id": "candidate-run-001"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["BACKTEST_ACCEPTANCE_FAILED"],
        },
        exit_code=2,
    )

    completed = _run_spawn_only(project_root, acceptance_script)

    assert completed.returncode == 0
    assert "[daily-cc] mode=spawn_only" in completed.stdout
    assert "[daily-cc] challenger_candidate_run_id=candidate-run-001" in completed.stdout


def test_spawn_only_still_fails_when_acceptance_reports_runtime_exception(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "steps": {
                "exception": {"message": "boom"},
                "train": {"candidate_run_id": "candidate-run-001"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["UNHANDLED_EXCEPTION"],
        },
        exit_code=2,
    )

    completed = _run_spawn_only(project_root, acceptance_script)

    assert completed.returncode != 0
    assert "candidate acceptance failed unexpectedly" in completed.stderr or "candidate acceptance failed unexpectedly" in completed.stdout
