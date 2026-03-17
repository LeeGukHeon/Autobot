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


def test_daily_parallel_acceptance_marks_stalled_lane_as_hung_process(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    scripts_dir = project_root / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    (scripts_dir / "daily_micro_pipeline_for_server.ps1").write_text("# noop\n", encoding="utf-8")
    (scripts_dir / "v3_candidate_acceptance.ps1").write_text("# noop\n", encoding="utf-8")
    (scripts_dir / "v4_scout_candidate_acceptance.ps1").write_text(
        """
param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$BatchDate = "",
    [switch]$SkipDailyPipeline,
    [switch]$SkipReportRefresh
)
Write-Host "[v4-accept] started"
Start-Sleep -Seconds 10
""".strip()
        + "\n",
        encoding="utf-8",
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
            "-BatchDate",
            "2026-03-08",
            "-SkipDailyPipeline",
            "-SkipFeaturesBuild",
            "-SkipV3",
            "-LanePollIntervalSec",
            "1",
            "-LaneStallTimeoutSec",
            "2",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 2, completed.stdout + "\n" + completed.stderr
    latest = json.loads((project_root / "logs" / "daily_parallel_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))
    assert latest["overall_pass"] is False
    assert latest["lanes"]["v4"]["watchdog_stalled"] is True
    assert latest["lanes"]["v4"]["watchdog_reason_codes"] == ["HUNG_PROCESS"]
    assert "HUNG_PROCESS" in latest["lanes"]["v4"]["latest_reasons"]


def test_daily_parallel_acceptance_allows_long_running_lane_with_progress_heartbeat(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    scripts_dir = project_root / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    (scripts_dir / "daily_micro_pipeline_for_server.ps1").write_text("# noop\n", encoding="utf-8")
    (scripts_dir / "v3_candidate_acceptance.ps1").write_text("# noop\n", encoding="utf-8")
    (scripts_dir / "v4_scout_candidate_acceptance.ps1").write_text(
        """
param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$BatchDate = "",
    [switch]$SkipDailyPipeline,
    [switch]$SkipReportRefresh
)
$outDir = Join-Path $ProjectRoot "logs/model_v4_acceptance"
New-Item -ItemType Directory -Path $outDir -Force | Out-Null
$latestPath = Join-Path $outDir "latest.json"
for ($i = 0; $i -lt 3; $i++) {
    @{ heartbeat = $i } | ConvertTo-Json | Set-Content -Path $latestPath -Encoding UTF8
    Write-Host ("[v4-accept] heartbeat={0}" -f $i)
    Start-Sleep -Seconds 1
}
$runReportPath = Join-Path $outDir "run-v4-report.json"
@{
    gates = @{
        overall_pass = $true
    }
    reasons = @()
} | ConvertTo-Json -Depth 4 | Set-Content -Path $runReportPath -Encoding UTF8
Copy-Item -Path $runReportPath -Destination $latestPath -Force
Write-Host ("[v4-accept] report={0}" -f $runReportPath)
""".strip()
        + "\n",
        encoding="utf-8",
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
            "-BatchDate",
            "2026-03-08",
            "-SkipDailyPipeline",
            "-SkipFeaturesBuild",
            "-SkipV3",
            "-LanePollIntervalSec",
            "1",
            "-LaneStallTimeoutSec",
            "2",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    latest = json.loads((project_root / "logs" / "daily_parallel_acceptance" / "latest.json").read_text(encoding="utf-8-sig"))
    assert latest["overall_pass"] is True
    assert latest["lanes"]["v4"]["watchdog_stalled"] is False
    assert latest["lanes"]["v4"]["effective_report_source"] == "run_report"
