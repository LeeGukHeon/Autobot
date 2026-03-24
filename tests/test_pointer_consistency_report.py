from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

import autobot.ops.pointer_consistency_report as pointer_module
from autobot.ops.pointer_consistency_report import build_pointer_consistency_report, write_pointer_consistency_report


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "check_pointer_consistency.ps1"


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required for this test")


def test_build_pointer_consistency_report_flags_invalid_server_like_state(tmp_path: Path) -> None:
    project_root = tmp_path
    family_dir = project_root / "models" / "registry" / "train_v4_crypto_cs"
    family_dir.mkdir(parents=True, exist_ok=True)
    _write_json(family_dir / "champion.json", {"run_id": "run-1"})
    _write_json(family_dir / "latest.json", {"run_id": "run-2"})
    _write_json(family_dir / "latest_candidate.json", {"run_id": "run-1"})
    _write_json(project_root / "models" / "registry" / "latest.json", {"run_id": "run-2", "model_family": "train_v4_crypto_cs"})
    _write_json(project_root / "models" / "registry" / "latest_candidate.json", {"run_id": "run-1", "model_family": "train_v4_crypto_cs"})
    (family_dir / "run-1").mkdir(parents=True, exist_ok=True)
    (family_dir / "run-2").mkdir(parents=True, exist_ok=True)
    _write_json(
        project_root / "logs" / "model_v4_challenger" / "latest.json",
        {"mode": "spawn_only", "exception": {"message": "candidate acceptance failed unexpectedly"}},
    )

    original_systemd = pointer_module._systemd_topology_snapshot
    pointer_module._systemd_topology_snapshot = lambda: {
        "available": True,
        "services": [
            {"unit": "autobot-live-alpha-candidate.service", "active": "active", "sub": "running", "load": "loaded", "description": "Candidate live"},
            {"unit": "autobot-paper-v4.service", "active": "active", "sub": "running", "load": "loaded", "description": "Champion paper"},
        ],
        "timers": [],
        "unit_files": [],
        "errors": {},
    }
    try:
        report = build_pointer_consistency_report(project_root=project_root, ts_ms=10_000)
    finally:
        pointer_module._systemd_topology_snapshot = original_systemd

    assert report["summary"]["status"] == "violation"
    assert "LATEST_CANDIDATE_WITHOUT_CURRENT_STATE" in report["summary"]["violation_codes"]
    assert "CHAMPION_EQUALS_LATEST_CANDIDATE_NO_TRANSITION_STATE" in report["summary"]["violation_codes"]
    assert report["runtime_units"]["candidate_live"]["is_active_like"] is True


def test_build_pointer_consistency_report_is_healthy_for_aligned_candidate_state(tmp_path: Path) -> None:
    project_root = tmp_path
    family_dir = project_root / "models" / "registry" / "train_v4_crypto_cs"
    family_dir.mkdir(parents=True, exist_ok=True)
    _write_json(family_dir / "champion.json", {"run_id": "run-1"})
    _write_json(family_dir / "latest.json", {"run_id": "run-2"})
    _write_json(family_dir / "latest_candidate.json", {"run_id": "run-3"})
    _write_json(project_root / "models" / "registry" / "latest.json", {"run_id": "run-2", "model_family": "train_v4_crypto_cs"})
    _write_json(project_root / "models" / "registry" / "latest_candidate.json", {"run_id": "run-3", "model_family": "train_v4_crypto_cs"})
    (family_dir / "run-1").mkdir(parents=True, exist_ok=True)
    (family_dir / "run-2").mkdir(parents=True, exist_ok=True)
    (family_dir / "run-3").mkdir(parents=True, exist_ok=True)
    _write_json(
        project_root / "logs" / "model_v4_challenger" / "current_state.json",
        {
            "candidate_run_id": "run-3",
            "champion_run_id_at_start": "run-1",
            "started_at_utc": "2026-03-25T00:00:00Z",
            "lane_mode": "promotion_strict",
            "promotion_eligible": True,
        },
    )
    _write_json(project_root / "logs" / "model_v4_challenger" / "latest.json", {"mode": "spawn_only"})

    original_systemd = pointer_module._systemd_topology_snapshot
    pointer_module._systemd_topology_snapshot = lambda: {
        "available": True,
        "services": [
            {"unit": "autobot-live-alpha-candidate.service", "active": "active", "sub": "running", "load": "loaded", "description": "Candidate live"},
            {"unit": "autobot-paper-v4-challenger.service", "active": "inactive", "sub": "dead", "load": "loaded", "description": "Challenger paper"},
            {"unit": "autobot-paper-v4.service", "active": "active", "sub": "running", "load": "loaded", "description": "Champion paper"},
        ],
        "timers": [],
        "unit_files": [],
        "errors": {},
    }
    try:
        report = build_pointer_consistency_report(project_root=project_root, ts_ms=10_000)
    finally:
        pointer_module._systemd_topology_snapshot = original_systemd

    assert report["summary"]["status"] == "healthy"
    assert report["summary"]["violation_count"] == 0
    assert report["summary"]["warning_count"] == 0


def test_write_pointer_consistency_report_uses_default_output_path(tmp_path: Path) -> None:
    project_root = tmp_path
    family_dir = project_root / "models" / "registry" / "train_v4_crypto_cs"
    family_dir.mkdir(parents=True, exist_ok=True)
    _write_json(family_dir / "champion.json", {"run_id": "run-1"})
    (family_dir / "run-1").mkdir(parents=True, exist_ok=True)

    output_path = write_pointer_consistency_report(project_root=project_root, ts_ms=10_000)

    assert output_path == project_root / "logs" / "ops" / "pointer_consistency" / "latest.json"
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["version"] == 1


def test_check_pointer_consistency_script_returns_nonzero_on_violations(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    family_dir = project_root / "models" / "registry" / "train_v4_crypto_cs"
    family_dir.mkdir(parents=True, exist_ok=True)
    _write_json(family_dir / "champion.json", {"run_id": "run-1"})
    _write_json(family_dir / "latest.json", {"run_id": "run-2"})
    _write_json(family_dir / "latest_candidate.json", {"run_id": "run-1"})
    _write_json(project_root / "models" / "registry" / "latest.json", {"run_id": "run-2", "model_family": "train_v4_crypto_cs"})
    _write_json(project_root / "models" / "registry" / "latest_candidate.json", {"run_id": "run-1", "model_family": "train_v4_crypto_cs"})
    (family_dir / "run-1").mkdir(parents=True, exist_ok=True)
    (family_dir / "run-2").mkdir(parents=True, exist_ok=True)

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
            sys.executable,
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={**os.environ},
        check=False,
    )

    assert completed.returncode == 2, completed.stdout + "\n" + completed.stderr
    report_path = project_root / "logs" / "ops" / "pointer_consistency" / "latest.json"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["summary"]["violation_count"] >= 1
