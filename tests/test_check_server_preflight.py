from __future__ import annotations

import json
import os
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


def _make_fake_systemctl(tmp_path: Path) -> Path:
    if os.name == "nt":
        wrapper_path = tmp_path / "systemctl.cmd"
        wrapper_path.write_text(
            "@echo off\r\n"
            "set first=%~1\r\n"
            "if /I \"%first%\"==\"list-units\" goto list_units\r\n"
            "if /I \"%first%\"==\"list-unit-files\" goto list_unit_files\r\n"
            "exit /b 0\r\n"
            ":list_units\r\n"
            "echo autobot-paper-v4.service loaded active running Fake Unit\r\n"
            "echo autobot-paper-v4-challenger.service loaded inactive dead Fake Unit\r\n"
            "echo autobot-v4-challenger-spawn.service loaded inactive dead Fake Unit\r\n"
            "echo autobot-v4-challenger-promote.service loaded inactive dead Fake Unit\r\n"
            "exit /b 0\r\n"
            ":list_unit_files\r\n"
            "echo autobot-paper-v4.service enabled enabled\r\n"
            "echo autobot-paper-v4-challenger.service disabled enabled\r\n"
            "echo autobot-v4-challenger-spawn.timer enabled enabled\r\n"
            "echo autobot-v4-challenger-promote.timer enabled enabled\r\n"
            "echo autobot-paper-v4-replay.service disabled enabled\r\n"
            "echo autobot-live-alpha-replay-shadow.service disabled enabled\r\n"
            "exit /b 0\r\n",
            encoding="utf-8",
        )
    else:
        wrapper_path = tmp_path / "systemctl"
        wrapper_path.write_text(
            "#!/bin/sh\n"
            "if [ \"$1\" = \"list-units\" ]; then\n"
            "  echo \"autobot-paper-v4.service loaded active running Fake Unit\"\n"
            "  echo \"autobot-paper-v4-challenger.service loaded inactive dead Fake Unit\"\n"
            "  echo \"autobot-v4-challenger-spawn.service loaded inactive dead Fake Unit\"\n"
            "  echo \"autobot-v4-challenger-promote.service loaded inactive dead Fake Unit\"\n"
            "  exit 0\n"
            "fi\n"
            "if [ \"$1\" = \"list-unit-files\" ]; then\n"
            "  echo \"autobot-paper-v4.service enabled enabled\"\n"
            "  echo \"autobot-paper-v4-challenger.service disabled enabled\"\n"
            "  echo \"autobot-v4-challenger-spawn.timer enabled enabled\"\n"
            "  echo \"autobot-v4-challenger-promote.timer enabled enabled\"\n"
            "  echo \"autobot-paper-v4-replay.service disabled enabled\"\n"
            "  echo \"autobot-live-alpha-replay-shadow.service disabled enabled\"\n"
            "  exit 0\n"
            "fi\n"
            "exit 0\n",
            encoding="utf-8",
        )
        wrapper_path.chmod(0o755)
    return wrapper_path


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
    assert report["runtime_topology_report"]["exit_code"] == 0
    assert report["pointer_consistency_report"]["exit_code"] == 0
    assert (project_root / "logs" / "runtime_topology" / "latest.json").exists()
    assert (project_root / "logs" / "ops" / "pointer_consistency" / "latest.json").exists()


def test_server_preflight_can_require_champion_pointer_from_external_family(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    baseline_family_dir = project_root / "models" / "registry" / "train_v4_crypto_cs"
    baseline_family_dir.mkdir(parents=True, exist_ok=True)
    _write_json(baseline_family_dir / "champion.json", {"run_id": "v4-champion-001"})
    (baseline_family_dir / "v4-champion-001").mkdir(parents=True, exist_ok=True)
    candidate_family_dir = project_root / "models" / "registry" / "train_v5_panel_ensemble"
    candidate_family_dir.mkdir(parents=True, exist_ok=True)

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
            "-ModelFamily",
            "train_v5_panel_ensemble",
            "-ChampionPointerFamily",
            "train_v4_crypto_cs",
            "-RequiredPointers",
            "champion",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    report = json.loads((project_root / "logs" / "ops" / "server_preflight" / "latest.json").read_text(encoding="utf-8-sig"))
    assert report["model_family"] == "train_v5_panel_ensemble"
    assert report["champion_pointer_family"] == "train_v4_crypto_cs"
    required_pointer_checks = [item for item in report["checks"] if item["code"] == "REQUIRED_POINTER_RESOLVED"]
    assert any(item["evidence"]["model_family"] == "train_v4_crypto_cs" for item in required_pointer_checks)


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
    assert report["required_pointers"] == ["champion"]
    assert report["check_candidate_state_consistency"] is True


def test_server_preflight_checks_expected_unit_states_and_state_db_paths(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    family_dir = project_root / "models" / "registry" / "train_v4_crypto_cs"
    family_dir.mkdir(parents=True, exist_ok=True)
    _write_json(family_dir / "champion.json", {"run_id": "run-001"})
    (family_dir / "run-001").mkdir(parents=True, exist_ok=True)
    state_db_path = project_root / "data" / "state" / "live_candidate" / "live_state.db"
    state_db_path.parent.mkdir(parents=True, exist_ok=True)
    state_db_path.write_text("", encoding="utf-8")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir(parents=True, exist_ok=True)
    _make_fake_systemctl(fake_bin)

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
            "-ExpectedUnitStates",
            "autobot-paper-v4.service=enabled,autobot-paper-v4-challenger.service=disabled,autobot-v4-challenger-spawn.timer=enabled",
            "-RequiredStateDbPaths",
            "data/state/live_candidate/live_state.db",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "PATH": str(fake_bin) + os.pathsep + os.environ.get("PATH", "")},
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    report = json.loads((project_root / "logs" / "ops" / "server_preflight" / "latest.json").read_text(encoding="utf-8-sig"))
    assert report["summary"]["status"] == "healthy"
    assert report["expected_unit_states"] == [
        "autobot-paper-v4.service=enabled",
        "autobot-paper-v4-challenger.service=disabled",
        "autobot-v4-challenger-spawn.timer=enabled",
    ]
    assert report["required_state_db_paths"] == ["data/state/live_candidate/live_state.db"]
    assert "UNIT_FILE_STATE_EXPECTATION_OK" not in report["summary"]["violation_codes"]
    assert "STATE_DB_PATH_MISSING" not in report["summary"]["violation_codes"]


def test_server_preflight_fails_on_pointer_report_only_violation(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    family_dir = project_root / "models" / "registry" / "train_v4_crypto_cs"
    family_dir.mkdir(parents=True, exist_ok=True)
    _write_json(family_dir / "champion.json", {"run_id": "run-001"})
    _write_json(family_dir / "latest.json", {"run_id": "run-002"})
    _write_json(family_dir / "latest_candidate.json", {"run_id": "run-003"})
    _write_json(project_root / "models" / "registry" / "latest.json", {"run_id": "run-002", "model_family": "train_v4_crypto_cs"})
    _write_json(project_root / "models" / "registry" / "latest_candidate.json", {"run_id": "run-003", "model_family": "train_v4_crypto_cs"})
    (family_dir / "run-001").mkdir(parents=True, exist_ok=True)
    (family_dir / "run-002").mkdir(parents=True, exist_ok=True)
    (family_dir / "run-003").mkdir(parents=True, exist_ok=True)
    _write_json(
        project_root / "logs" / "model_v4_challenger" / "current_state.json",
        {
            "candidate_run_id": "run-003",
            "champion_run_id_at_start": "",
            "lane_mode": "",
        },
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
            "-RequiredPointers",
            "champion",
            "-CheckCandidateStateConsistency",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 2, completed.stdout + "\n" + completed.stderr
    report = json.loads((project_root / "logs" / "ops" / "server_preflight" / "latest.json").read_text(encoding="utf-8-sig"))
    assert report["summary"]["status"] == "violation"
    assert "POINTER_CONSISTENCY_VIOLATION" in report["summary"]["violation_codes"]
