import shutil
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
INSTALL_SCRIPT = REPO_ROOT / "scripts" / "install_server_ws_public_service.ps1"


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required for this test")


def test_ws_public_installer_generates_runtime_unit() -> None:
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
    assert "[ws-public-install][dry-run] unit=autobot-ws-public.service" in stdout
    assert "collect" in stdout
    assert "ws-public" in stdout
    assert "daemon" in stdout
    assert "--raw-root" in stdout
    assert "--meta-dir" in stdout
    assert "--quote" in stdout
    assert "--top-n" in stdout
    assert "Restart=always" in stdout
    assert "ExecStart=/bin/bash -lc " in stdout
