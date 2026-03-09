import json
import os
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


def _make_fake_sudo(tmp_path: Path) -> Path:
    if os.name == "nt":
        wrapper_path = tmp_path / "sudo.cmd"
        wrapper_path.write_text(
            "@echo off\r\n%*\r\n",
            encoding="utf-8",
        )
    else:
        wrapper_path = tmp_path / "sudo"
        wrapper_path.write_text(
            "#!/bin/sh\n\"$@\"\n",
            encoding="utf-8",
        )
        wrapper_path.chmod(0o755)
    return wrapper_path


def _make_fake_systemctl(tmp_path: Path) -> Path:
    if os.name == "nt":
        wrapper_path = tmp_path / "systemctl.cmd"
        wrapper_path.write_text(
            "@echo off\r\n"
            "if \"%1\"==\"is-active\" exit /b 1\r\n"
            "exit /b 0\r\n",
            encoding="utf-8",
        )
    else:
        wrapper_path = tmp_path / "systemctl"
        wrapper_path.write_text(
            "#!/bin/sh\n"
            "if [ \"$1\" = \"is-active\" ]; then\n"
            "  exit 1\n"
            "fi\n"
            "exit 0\n",
            encoding="utf-8",
        )
        wrapper_path.chmod(0o755)
    return wrapper_path


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


def _run_spawn_only(
    project_root: Path,
    acceptance_script: Path,
    *,
    dry_run: bool = True,
) -> subprocess.CompletedProcess[str]:
    sudo_dir = acceptance_script.parent
    _make_fake_sudo(sudo_dir)
    _make_fake_systemctl(sudo_dir)
    args = [
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
    ]
    if dry_run:
        args.append("-DryRun")
    return subprocess.run(
        args,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "PATH": str(sudo_dir) + os.pathsep + os.environ.get("PATH", "")},
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

    completed = _run_spawn_only(project_root, acceptance_script, dry_run=False)

    assert completed.returncode == 0
    assert "[daily-cc] mode=spawn_only" in completed.stdout
    assert "[daily-cc] challenger_candidate_run_id=candidate-run-001" in completed.stdout


def test_spawn_only_uses_trainer_evidence_failure_as_root_start_reason(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "steps": {
                "train": {"candidate_run_id": "candidate-run-evidence"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["BACKTEST_ACCEPTANCE_FAILED", "TRAINER_EVIDENCE_REQUIRED_FAILED"],
            "notes": ["PAPER_SOAK_SKIPPED"],
        },
        exit_code=2,
    )

    completed = _run_spawn_only(project_root, acceptance_script, dry_run=False)

    assert completed.returncode == 0
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))
    start_step = latest["steps"]["start_challenger"]
    assert start_step["reason"] == "TRAINER_EVIDENCE_REQUIRED_FAILED"
    assert start_step["acceptance_notes"] == ["PAPER_SOAK_SKIPPED"]


def test_spawn_only_treats_duplicate_candidate_as_successful_no_challenger_day(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "steps": {
                "train": {"candidate_run_id": "candidate-run-dup"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["DUPLICATE_CANDIDATE"],
        },
        exit_code=2,
    )

    completed = _run_spawn_only(project_root, acceptance_script)

    assert completed.returncode == 0
    assert "[daily-cc] mode=spawn_only" in completed.stdout
    assert "[daily-cc] challenger_candidate_run_id=candidate-run-dup" in completed.stdout


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
