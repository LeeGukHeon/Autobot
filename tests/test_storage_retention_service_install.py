import shutil
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
INSTALL_SCRIPT = REPO_ROOT / "scripts" / "install_server_storage_retention_service.ps1"


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required for this test")


def test_storage_retention_installer_generates_units() -> None:
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
    assert "[storage-retention-install][dry-run] service=autobot-storage-retention.service" in stdout
    assert "[storage-retention-install][dry-run] timer=autobot-storage-retention.timer" in stdout
    assert "OnCalendar=*-*-* 06:30:00" in stdout
    assert "autobot.ops.storage_retention" in stdout
    assert "--execution-backtest-retention-days" in stdout
    assert "ExecStart=/bin/bash -lc " in stdout
