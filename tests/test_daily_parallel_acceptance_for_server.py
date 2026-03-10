import json
import shutil
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "daily_parallel_acceptance_for_server.ps1"


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required for this test")


def test_daily_parallel_acceptance_defaults_v4_lane_to_scout_wrapper(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    scripts_dir = project_root / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    (scripts_dir / "v3_candidate_acceptance.ps1").write_text("# noop\n", encoding="utf-8")
    (scripts_dir / "v4_scout_candidate_acceptance.ps1").write_text("# noop\n", encoding="utf-8")
    (scripts_dir / "daily_micro_pipeline_for_server.ps1").write_text("# noop\n", encoding="utf-8")

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
            "-BatchDate",
            "2026-03-08",
            "-SkipDailyPipeline",
            "-SkipFeaturesBuild",
            "-SkipV3",
            "-DryRun",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    latest = json.loads((project_root / "logs" / "daily_parallel_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))
    assert latest["overall_pass"] is True
    assert "v4_scout_candidate_acceptance.ps1" in latest["lanes"]["v4"]["command"]
