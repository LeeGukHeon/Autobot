from __future__ import annotations

import json
import os
import shutil
import subprocess
import textwrap
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
ADOPT_SCRIPT = REPO_ROOT / "scripts" / "adopt_v4_candidate_for_server.ps1"


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required for this test")


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _make_fake_sudo(tmp_path: Path) -> Path:
    if os.name == "nt":
        wrapper_path = tmp_path / "sudo.cmd"
        wrapper_path.write_text(
            "@echo off\r\n%*\r\n",
            encoding="utf-8",
        )
    else:
        wrapper_path = tmp_path / "sudo"
        wrapper_path.write_text(
            "#!/bin/sh\n\"$@\"\n",
            encoding="utf-8",
        )
        wrapper_path.chmod(0o755)
    return wrapper_path


def _make_fake_systemctl(tmp_path: Path) -> Path:
    if os.name == "nt":
        wrapper_path = tmp_path / "systemctl.cmd"
        wrapper_path.write_text(
            "@echo off\r\n"
            "if not \"%FAKE_SYSTEMCTL_LOG%\"==\"\" echo %*>>\"%FAKE_SYSTEMCTL_LOG%\"\r\n"
            "if \"%1\"==\"is-active\" goto is_active\r\n"
            "exit /b 0\r\n"
            ":is_active\r\n"
            "set \"TARGET=%2\"\r\n"
            "if \"%2\"==\"--quiet\" set \"TARGET=%3\"\r\n"
            "echo ,%FAKE_ACTIVE_UNITS%, | findstr /I /C:\",%TARGET%,\" >nul\r\n"
            "if not errorlevel 1 exit /b 0\r\n"
            "exit /b 1\r\n",
            encoding="utf-8",
        )
    else:
        wrapper_path = tmp_path / "systemctl"
        wrapper_path.write_text(
            "#!/bin/sh\n"
            "if [ -n \"$FAKE_SYSTEMCTL_LOG\" ]; then\n"
            "  echo \"$@\" >> \"$FAKE_SYSTEMCTL_LOG\"\n"
            "fi\n"
            "if [ \"$1\" = \"is-active\" ]; then\n"
            "  target=\"$2\"\n"
            "  if [ \"$2\" = \"--quiet\" ]; then\n"
            "    target=\"$3\"\n"
            "  fi\n"
            "  case \",${FAKE_ACTIVE_UNITS},\" in\n"
            "    *,$target,*) exit 0 ;;\n"
            "  esac\n"
            "  exit 1\n"
            "fi\n"
            "exit 0\n",
            encoding="utf-8",
        )
        wrapper_path.chmod(0o755)
    return wrapper_path


def _make_fake_runtime_install_script(tmp_path: Path) -> Path:
    script_path = tmp_path / "fake_runtime_install.ps1"
    script_path.write_text(
        textwrap.dedent(
            """
            param(
                [string]$ProjectRoot = "",
                [string]$PythonExe = "",
                [string]$PaperUnitName = "",
                [string]$PaperPreset = "",
                [string]$PaperRuntimeRole = "",
                [string]$PaperModelRefPinned = "",
                [string]$PaperCliArgs = ""
            )

            $logPath = Join-Path $ProjectRoot "logs/fake_runtime_install/report.json"
            New-Item -ItemType Directory -Force -Path (Split-Path -Parent $logPath) | Out-Null
            @{
                paper_unit_name = $PaperUnitName
                paper_preset = $PaperPreset
                paper_runtime_role = $PaperRuntimeRole
                paper_model_ref_pinned = $PaperModelRefPinned
                paper_cli_args = $PaperCliArgs
            } | ConvertTo-Json -Depth 4 | Set-Content -Path $logPath -Encoding UTF8
            Write-Host "[fake-runtime-install] ok"
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    return script_path


def _run_adopt(
    project_root: Path,
    *,
    runtime_install_script: Path,
    candidate_run_id: str,
    candidate_target_units: list[str] | None = None,
    active_units: list[str] | None = None,
    extra_args: list[str] | None = None,
) -> subprocess.CompletedProcess[str]:
    tool_dir = runtime_install_script.parent
    _make_fake_sudo(tool_dir)
    _make_fake_systemctl(tool_dir)
    command = [
        _powershell_exe(),
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(ADOPT_SCRIPT),
        "-ProjectRoot",
        str(project_root),
        "-PythonExe",
        "python",
        "-RuntimeInstallScript",
        str(runtime_install_script),
        "-BatchDate",
        "2026-03-24",
        "-CandidateRunId",
        candidate_run_id,
    ]
    if candidate_target_units:
        command.extend(["-CandidateTargetUnits", ",".join(candidate_target_units)])
    if extra_args:
        command.extend(extra_args)
    return subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": str(tool_dir) + os.pathsep + os.environ.get("PATH", ""),
            "FAKE_ACTIVE_UNITS": ",".join(active_units or []),
            "FAKE_SYSTEMCTL_LOG": str(tool_dir / "systemctl.log"),
        },
        check=False,
    )


def test_adopt_v4_candidate_for_server_updates_pointers_state_and_artifact_status(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    runtime_install_script = _make_fake_runtime_install_script(tmp_path)
    run_dir = project_root / "models" / "registry" / "train_v4_crypto_cs" / "run-001"
    _write_json(
        run_dir / "artifact_status.json",
        {
            "run_id": "run-001",
            "status": "acceptance_completed",
            "core_saved": True,
            "support_artifacts_written": True,
            "execution_acceptance_complete": True,
            "runtime_recommendations_complete": True,
            "governance_artifacts_complete": True,
            "acceptance_completed": True,
            "candidate_adoptable": True,
            "candidate_adopted": False,
            "promoted": False,
            "updated_at_utc": "2026-03-24T00:00:00Z",
        },
    )
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-001"},
    )

    completed = _run_adopt(
        project_root,
        runtime_install_script=runtime_install_script,
        candidate_run_id="run-001",
        candidate_target_units=["autobot-live-alpha-candidate.service"],
        active_units=[],
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr

    family_pointer = json.loads(
        (project_root / "models" / "registry" / "train_v4_crypto_cs" / "latest_candidate.json").read_text(
            encoding="utf-8-sig"
        )
    )
    global_pointer = json.loads(
        (project_root / "models" / "registry" / "latest_candidate.json").read_text(encoding="utf-8-sig")
    )
    state = json.loads(
        (project_root / "logs" / "model_v4_challenger" / "current_state.json").read_text(encoding="utf-8-sig")
    )
    report = json.loads(
        (project_root / "logs" / "model_v4_challenger" / "latest_candidate_adoption.json").read_text(
            encoding="utf-8-sig"
        )
    )
    runtime_install = json.loads(
        (project_root / "logs" / "fake_runtime_install" / "report.json").read_text(encoding="utf-8-sig")
    )
    artifact_status = json.loads((run_dir / "artifact_status.json").read_text(encoding="utf-8-sig"))
    systemctl_log = (tmp_path / "systemctl.log").read_text(encoding="utf-8")

    assert family_pointer["run_id"] == "run-001"
    assert global_pointer["run_id"] == "run-001"
    assert global_pointer["model_family"] == "train_v4_crypto_cs"
    assert state["candidate_run_id"] == "run-001"
    assert state["champion_run_id_at_start"] == "champion-run-001"
    assert report["steps"]["restart_candidate_targets"]["restarted_units"] == [
        "autobot-live-alpha-candidate.service"
    ]
    assert report["steps"]["restart_candidate_targets"]["started_from_inactive_units"] == [
        "autobot-live-alpha-candidate.service"
    ]
    assert runtime_install["paper_unit_name"] == "autobot-paper-v4-paired.service"
    assert runtime_install["paper_preset"] == "paired_v4"
    assert runtime_install["paper_runtime_role"] == "paired"
    assert artifact_status["status"] == "candidate_adopted"
    assert artifact_status["candidate_adopted"] is True
    assert "restart autobot-live-alpha-candidate.service" in systemctl_log


def test_adopt_v4_candidate_for_server_fails_closed_when_artifact_status_is_incomplete(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    runtime_install_script = _make_fake_runtime_install_script(tmp_path)
    run_dir = project_root / "models" / "registry" / "train_v4_crypto_cs" / "run-002"
    _write_json(
        run_dir / "artifact_status.json",
        {
            "run_id": "run-002",
            "status": "trainer_artifacts_complete",
            "core_saved": True,
            "support_artifacts_written": True,
            "execution_acceptance_complete": True,
            "runtime_recommendations_complete": True,
            "governance_artifacts_complete": True,
            "acceptance_completed": False,
            "candidate_adoptable": False,
            "candidate_adopted": False,
            "promoted": False,
            "updated_at_utc": "2026-03-24T00:00:00Z",
        },
    )

    completed = _run_adopt(
        project_root,
        runtime_install_script=runtime_install_script,
        candidate_run_id="run-002",
        candidate_target_units=["autobot-live-alpha-candidate.service"],
    )

    assert completed.returncode == 2, completed.stdout + "\n" + completed.stderr

    latest_report = json.loads(
        (project_root / "logs" / "model_v4_challenger" / "latest_candidate_adoption.json").read_text(
            encoding="utf-8-sig"
        )
    )
    assert "acceptance_completed" in latest_report["exception"]["message"]
    assert not (project_root / "models" / "registry" / "train_v4_crypto_cs" / "latest_candidate.json").exists()
    assert not (project_root / "models" / "registry" / "latest_candidate.json").exists()
    assert not (project_root / "logs" / "model_v4_challenger" / "current_state.json").exists()
