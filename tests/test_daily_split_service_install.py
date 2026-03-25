import shutil
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
INSTALL_SCRIPT = REPO_ROOT / "scripts" / "install_server_daily_split_challenger_services.ps1"


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required for this test")


def test_split_installer_generates_promote_and_spawn_units() -> None:
    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(INSTALL_SCRIPT),
            "-ProjectRoot",
            str(REPO_ROOT),
            "-PythonExe",
            "python",
            "-DryRun",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )

    stdout = completed.stdout
    assert "[daily-split-install][dry-run] promote_service=autobot-v4-challenger-promote.service" in stdout
    assert "[daily-split-install][dry-run] spawn_service=autobot-v4-challenger-spawn.service" in stdout
    assert "OnCalendar=*-*-* 00:10:00" in stdout
    assert "OnCalendar=*-*-* 00:20:00" in stdout
    assert "promote_only" in stdout
    assert "spawn_only" in stdout
    assert "ExecStart=/bin/bash -lc " in stdout
    assert "disable_legacy_service=autobot-paper-v4-replay.service" in stdout
    assert "disable_legacy_service=autobot-live-alpha-replay-shadow.service" in stdout


def test_split_installer_can_pass_candidate_target_units_to_spawn_service() -> None:
    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(INSTALL_SCRIPT),
            "-ProjectRoot",
            str(REPO_ROOT),
            "-PythonExe",
            "python",
            "-CandidateTargetUnits",
            "autobot-live-alpha-candidate.service",
            "-DryRun",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )

    stdout = completed.stdout
    assert "autobot-live-alpha-candidate.service" in stdout
    assert "-CandidateTargetUnits" in stdout
