import json
import shutil
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required for this test")


def _seed_fake_acceptance_script(path: Path, marker: str) -> None:
    path.write_text(
        "param([string]$ProjectRoot = '', [string]$PythonExe = '', [string]$BatchDate = '')\n"
        f"Write-Host '[fake-wrapper] {marker}'\n"
        "exit 0\n",
        encoding="utf-8",
    )


def test_governed_candidate_acceptance_defaults_to_cls_primary_without_governance_action(tmp_path: Path) -> None:
    scripts_dir = tmp_path / "scripts"
    logs_dir = tmp_path / "logs" / "model_v4_rank_shadow_cycle"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    governed_script = scripts_dir / "v4_governed_candidate_acceptance.ps1"
    governed_script.write_text(
        (REPO_ROOT / "scripts" / "v4_governed_candidate_acceptance.ps1").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    _seed_fake_acceptance_script(scripts_dir / "v4_promotable_candidate_acceptance.ps1", "cls-primary")
    _seed_fake_acceptance_script(scripts_dir / "v4_rank_governed_candidate_acceptance.ps1", "rank-governed")

    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(governed_script),
            "-ProjectRoot",
            str(tmp_path),
            "-PythonExe",
            "python",
            "-BatchDate",
            "2026-03-08",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    assert "selected_acceptance_script" in completed.stdout
    assert "[fake-wrapper] cls-primary" in completed.stdout


def test_governed_candidate_acceptance_selects_rank_governed_when_action_requests_it(tmp_path: Path) -> None:
    scripts_dir = tmp_path / "scripts"
    logs_dir = tmp_path / "logs" / "model_v4_rank_shadow_cycle"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    governed_script = scripts_dir / "v4_governed_candidate_acceptance.ps1"
    governed_script.write_text(
        (REPO_ROOT / "scripts" / "v4_governed_candidate_acceptance.ps1").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    _seed_fake_acceptance_script(scripts_dir / "v4_promotable_candidate_acceptance.ps1", "cls-primary")
    _seed_fake_acceptance_script(scripts_dir / "v4_rank_governed_candidate_acceptance.ps1", "rank-governed")
    (logs_dir / "latest_governance_action.json").write_text(
        json.dumps(
            {
                "selected_lane_id": "rank_governed_primary",
                "selected_acceptance_script": "v4_rank_governed_candidate_acceptance.ps1",
            }
        ),
        encoding="utf-8",
    )

    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(governed_script),
            "-ProjectRoot",
            str(tmp_path),
            "-PythonExe",
            "python",
            "-BatchDate",
            "2026-03-08",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    assert "[fake-wrapper] rank-governed" in completed.stdout


def test_governed_candidate_acceptance_ignores_stale_shadow_governance_when_disabled(tmp_path: Path, monkeypatch) -> None:
    scripts_dir = tmp_path / "scripts"
    logs_dir = tmp_path / "logs" / "model_v4_rank_shadow_cycle"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    governed_script = scripts_dir / "v4_governed_candidate_acceptance.ps1"
    governed_script.write_text(
        (REPO_ROOT / "scripts" / "v4_governed_candidate_acceptance.ps1").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    _seed_fake_acceptance_script(scripts_dir / "v4_promotable_candidate_acceptance.ps1", "cls-primary")
    _seed_fake_acceptance_script(scripts_dir / "v4_rank_governed_candidate_acceptance.ps1", "rank-governed")
    (logs_dir / "latest_governance_action.json").write_text(
        json.dumps(
            {
                "selected_lane_id": "rank_governed_primary",
                "selected_acceptance_script": "v4_rank_governed_candidate_acceptance.ps1",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("AUTOBOT_RANK_SHADOW_GOVERNANCE", "false")

    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(governed_script),
            "-ProjectRoot",
            str(tmp_path),
            "-PythonExe",
            "python",
            "-BatchDate",
            "2026-03-08",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    assert "rank_shadow_governance_enabled=false" in completed.stdout
    assert "[fake-wrapper] cls-primary" in completed.stdout
