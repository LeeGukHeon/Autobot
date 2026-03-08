import shutil
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
INSTALL_SCRIPT = REPO_ROOT / "scripts" / "install_server_daily_parallel_acceptance_service.ps1"


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required for this test")


def test_daily_parallel_installer_uses_bash_wrapper_and_service_user() -> None:
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
    assert "[daily-parallel-install][dry-run] service_user=ubuntu" in stdout
    assert "[Service]" in stdout
    assert "TimeoutStartSec=0" in stdout
    assert "User=ubuntu" in stdout
    assert "ExecStart=/bin/bash -lc " in stdout
