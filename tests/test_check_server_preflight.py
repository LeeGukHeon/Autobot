from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "check_server_preflight.ps1"


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required")


def _git_exe() -> str:
    resolved = shutil.which("git")
    if not resolved:
        pytest.skip("git executable is required")
    return resolved


def test_server_preflight_passes_for_clean_candidate_state(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    family_dir = project_root / "models" / "registry" / "train_v4_crypto_cs"
    family_dir.mkdir(parents=True, exist_ok=True)
    _write_json(family_dir / "champion.json", {"run_id": "run-001"})
    (family_dir / "run-001").mkdir(parents=True, exist_ok=True)

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
            "-RequiredPointers",
            "champion",
            "-CheckCandidateStateConsistency",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    report = json.loads((project_root / "logs" / "ops" / "server_preflight" / "latest.json").read_text(encoding="utf-8-sig"))
    assert report["summary"]["status"] == "healthy"


def test_server_preflight_fails_on_dirty_worktree_and_candidate_state_mismatch(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    family_dir = project_root / "models" / "registry" / "train_v4_crypto_cs"
    family_dir.mkdir(parents=True, exist_ok=True)
    _write_json(family_dir / "champion.json", {"run_id": "run-001"})
    _write_json(family_dir / "latest_candidate.json", {"run_id": "run-001"})
    (family_dir / "run-001").mkdir(parents=True, exist_ok=True)

    git_exe = _git_exe()
    subprocess.run([git_exe, "init"], cwd=project_root, check=True, capture_output=True, text=True)
    subprocess.run(
        [git_exe, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "--allow-empty", "-m", "init"],
        cwd=project_root,
        check=True,
        capture_output=True,
        text=True,
    )
    (project_root / "untracked.txt").write_text("dirty\n", encoding="utf-8")

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
            "-RequiredPointers",
            "champion",
            "-CheckCandidateStateConsistency",
            "-FailOnDirtyWorktree",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 2, completed.stdout + "\n" + completed.stderr
    report = json.loads((project_root / "logs" / "ops" / "server_preflight" / "latest.json").read_text(encoding="utf-8-sig"))
    assert report["summary"]["status"] == "violation"
    assert "DIRTY_WORKTREE" in report["summary"]["violation_codes"]
    assert "LATEST_CANDIDATE_WITHOUT_CURRENT_STATE" in report["summary"]["violation_codes"]
