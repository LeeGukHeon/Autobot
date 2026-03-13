import json
import os
import shutil
import subprocess
import textwrap
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
DAILY_CC_SCRIPT = REPO_ROOT / "scripts" / "daily_champion_challenger_v4_for_server.ps1"


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required for this test")


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
            "if [ \"$1\" = \"is-active\" ]; then\n"
            "  exit 1\n"
            "fi\n"
            "exit 0\n",
            encoding="utf-8",
        )
        wrapper_path.chmod(0o755)
    return wrapper_path


def _make_fake_acceptance_script(
    tmp_path: Path,
    payload: dict,
    exit_code: int,
    *,
    emit_daily_micro_report: bool = False,
) -> Path:
    script_path = tmp_path / f"fake_acceptance_{exit_code}.ps1"
    payload_json = json.dumps(payload)
    prelude = ""
    if emit_daily_micro_report:
        prelude = textwrap.indent(
            textwrap.dedent(
                """
                $dailyReportPath = Join-Path $ProjectRoot "docs/reports/DAILY_MICRO_REPORT_2026-03-08.md"
                New-Item -ItemType Directory -Force -Path (Split-Path -Parent $dailyReportPath) | Out-Null
                "# fake daily report" | Set-Content -Path $dailyReportPath -Encoding UTF8
                Write-Host ("[daily-micro] report={0}" -f $dailyReportPath)
                """
            ).strip(),
            "            ",
        )
    script_path.write_text(
        textwrap.dedent(
            f"""
            param(
                [string]$ProjectRoot = "",
                [string]$PythonExe = "",
                [string]$BatchDate = "",
                [switch]$SkipPaperSoak,
                [switch]$SkipPromote,
                [switch]$SkipDailyPipeline,
                [switch]$SkipReportRefresh,
                [switch]$DryRun
            )

            $ErrorActionPreference = "Stop"
            $reportPath = Join-Path $ProjectRoot "logs/fake_acceptance/report.json"
            New-Item -ItemType Directory -Force -Path (Split-Path -Parent $reportPath) | Out-Null
            {prelude}
            @'
            {payload_json}
            '@ | Set-Content -Path $reportPath -Encoding UTF8
            Write-Host ("[fake-accept] report={{0}}" -f $reportPath)
            exit {exit_code}
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    return script_path


def _make_fake_runtime_install_script(tmp_path: Path) -> Path:
    script_path = tmp_path / "fake_runtime_install.ps1"
    script_path.write_text(
        textwrap.dedent(
            """
            param(
                [string]$ProjectRoot = "",
                [string]$PythonExe = "",
                [string]$PaperUnitName = "",
                [string]$PaperModelRefPinned = "",
                [string]$PaperCliArgs = ""
            )

            $logPath = Join-Path $ProjectRoot "logs/fake_runtime_install/report.json"
            New-Item -ItemType Directory -Force -Path (Split-Path -Parent $logPath) | Out-Null
            @{
                paper_unit_name = $PaperUnitName
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


def _run_spawn_only(
    project_root: Path,
    acceptance_script: Path,
    *,
    dry_run: bool = True,
    extra_args: list[str] | None = None,
    active_units: list[str] | None = None,
) -> subprocess.CompletedProcess[str]:
    sudo_dir = acceptance_script.parent
    _make_fake_sudo(sudo_dir)
    _make_fake_systemctl(sudo_dir)
    args = [
        _powershell_exe(),
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(DAILY_CC_SCRIPT),
        "-ProjectRoot",
        str(project_root),
        "-PythonExe",
        "python",
        "-AcceptanceScript",
        str(acceptance_script),
        "-Mode",
        "spawn_only",
    ]
    if extra_args:
        args.extend(extra_args)
    if dry_run:
        args.append("-DryRun")
    return subprocess.run(
        args,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": str(sudo_dir) + os.pathsep + os.environ.get("PATH", ""),
            "FAKE_ACTIVE_UNITS": ",".join(active_units or []),
        },
    )


def test_spawn_only_treats_candidate_rejection_as_successful_no_challenger_day(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "steps": {
                "train": {"candidate_run_id": "candidate-run-001"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["BACKTEST_ACCEPTANCE_FAILED"],
        },
        exit_code=2,
    )

    completed = _run_spawn_only(project_root, acceptance_script, dry_run=False)

    assert completed.returncode == 0
    assert "[daily-cc] mode=spawn_only" in completed.stdout
    assert "[daily-cc] challenger_candidate_run_id=candidate-run-001" in completed.stdout


def test_spawn_only_uses_trainer_evidence_failure_as_root_start_reason(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "steps": {
                "train": {"candidate_run_id": "candidate-run-evidence"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["BACKTEST_ACCEPTANCE_FAILED", "TRAINER_EVIDENCE_REQUIRED_FAILED"],
            "notes": ["PAPER_SOAK_SKIPPED"],
        },
        exit_code=2,
    )

    completed = _run_spawn_only(project_root, acceptance_script, dry_run=False)

    assert completed.returncode == 0
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))
    start_step = latest["steps"]["start_challenger"]
    assert start_step["reason"] == "TRAINER_EVIDENCE_REQUIRED_FAILED"
    assert start_step["acceptance_notes"] == ["PAPER_SOAK_SKIPPED"]


def test_spawn_only_treats_duplicate_candidate_as_successful_no_challenger_day(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "steps": {
                "train": {"candidate_run_id": "candidate-run-dup"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["DUPLICATE_CANDIDATE"],
        },
        exit_code=2,
    )

    completed = _run_spawn_only(project_root, acceptance_script)

    assert completed.returncode == 0
    assert "[daily-cc] mode=spawn_only" in completed.stdout
    assert "[daily-cc] challenger_candidate_run_id=candidate-run-dup" in completed.stdout


def test_spawn_only_prefers_final_json_report_over_daily_markdown_report(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "steps": {
                "train": {"candidate_run_id": "candidate-run-002"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["BACKTEST_ACCEPTANCE_FAILED"],
        },
        exit_code=2,
        emit_daily_micro_report=True,
    )

    completed = _run_spawn_only(project_root, acceptance_script, dry_run=False)

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    assert "[daily-cc] challenger_candidate_run_id=candidate-run-002" in completed.stdout


def test_spawn_only_still_fails_when_acceptance_reports_runtime_exception(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "steps": {
                "exception": {"message": "boom"},
                "train": {"candidate_run_id": "candidate-run-001"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["UNHANDLED_EXCEPTION"],
        },
        exit_code=2,
    )

    completed = _run_spawn_only(project_root, acceptance_script)

    assert completed.returncode != 0
    assert "candidate acceptance failed unexpectedly" in completed.stderr or "candidate acceptance failed unexpectedly" in completed.stdout


def test_spawn_only_accepts_comma_joined_promotion_target_units_from_installer(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "steps": {
                "train": {"candidate_run_id": "candidate-run-array"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["DUPLICATE_CANDIDATE"],
        },
        exit_code=2,
    )

    completed = _run_spawn_only(
        project_root,
        acceptance_script,
        dry_run=False,
        extra_args=[
            "-PromotionTargetUnits",
            "autobot-live-alpha.service,autobot-live-alpha-candidate.service",
        ],
    )

    assert completed.returncode == 0
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))
    assert latest["promotion_target_units"] == [
        "autobot-live-alpha.service",
        "autobot-live-alpha-candidate.service",
    ]


def test_spawn_only_starts_bootstrap_candidate_as_non_promotable_challenger(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "candidate": {
                "lane_mode": "bootstrap_latest_inclusive",
                "promotion_eligible": False,
                "split_policy_artifact_path": str(project_root / "models" / "registry" / "train_v4_crypto_cs" / "candidate-run-bootstrap" / "split_policy_decision.json"),
            },
            "split_policy": {
                "policy_id": "v4_split_policy_bootstrap_transition_v1",
                "lane_mode": "bootstrap_latest_inclusive",
                "promotion_eligible": False,
            },
            "steps": {
                "train": {"candidate_run_id": "candidate-run-bootstrap"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["BOOTSTRAP_ONLY_POLICY"],
        },
        exit_code=2,
    )
    runtime_install_script = _make_fake_runtime_install_script(tmp_path)

    completed = _run_spawn_only(
        project_root,
        acceptance_script,
        dry_run=False,
        extra_args=[
            "-RuntimeInstallScript",
            str(runtime_install_script),
        ],
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))
    state = json.loads((project_root / "logs" / "model_v4_challenger" / "current_state.json").read_text(encoding="utf-8-sig"))

    assert latest["steps"]["train_candidate"]["bootstrap_only"] is True
    assert latest["steps"]["start_challenger"]["candidate_run_id"] == "candidate-run-bootstrap"
    assert latest["steps"]["start_challenger"]["bootstrap_only"] is True
    assert state["candidate_run_id"] == "candidate-run-bootstrap"
    assert state["lane_mode"] == "bootstrap_latest_inclusive"
    assert state["promotion_eligible"] is False
    assert state["bootstrap_only"] is True


def test_spawn_only_restarts_configured_candidate_targets_when_active(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    runtime_install_script = _make_fake_runtime_install_script(tmp_path)

    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        {
            "candidate": {
                "lane_mode": "bootstrap_latest_inclusive",
                "promotion_eligible": False,
            },
            "split_policy": {
                "lane_mode": "bootstrap_latest_inclusive",
                "promotion_eligible": False,
            },
            "steps": {
                "train": {"candidate_run_id": "candidate-run-live-canary"},
            },
            "gates": {
                "backtest": {"pass": False},
                "overall_pass": False,
            },
            "reasons": ["BOOTSTRAP_ONLY_POLICY"],
        },
        exit_code=2,
    )

    completed = _run_spawn_only(
        project_root,
        acceptance_script,
        dry_run=False,
        extra_args=[
            "-RuntimeInstallScript",
            str(runtime_install_script),
            "-CandidateTargetUnits",
            "autobot-live-alpha-candidate.service",
        ],
        active_units=["autobot-live-alpha-candidate.service"],
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))

    restart_step = latest["steps"]["restart_candidate_targets"]
    assert restart_step["attempted"] is True
    assert restart_step["candidate_run_id"] == "candidate-run-live-canary"
    assert restart_step["restarted_units"] == ["autobot-live-alpha-candidate.service"]
    assert restart_step["skipped_units"] == []


def test_promote_only_skips_previous_bootstrap_candidate(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    sudo_dir = tmp_path
    _make_fake_sudo(sudo_dir)
    _make_fake_systemctl(sudo_dir)
    state_path = project_root / "logs" / "model_v4_challenger" / "current_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {
                "batch_date": "2026-03-08",
                "candidate_run_id": "candidate-run-bootstrap",
                "champion_run_id_at_start": "champion-run-000",
                "started_ts_ms": 1,
                "lane_mode": "bootstrap_latest_inclusive",
                "promotion_eligible": False,
                "bootstrap_only": True,
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
            str(DAILY_CC_SCRIPT),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            "python",
            "-Mode",
            "promote_only",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "PATH": str(sudo_dir) + os.pathsep + os.environ.get("PATH", "")},
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    latest = json.loads((project_root / "logs" / "model_v4_challenger" / "latest.json").read_text(encoding="utf-8-sig"))

    assert latest["steps"]["promote_previous_challenger"]["reason"] == "BOOTSTRAP_ONLY_POLICY"
    assert latest["steps"]["promote_previous_challenger"]["promotion_eligible"] is False
    assert not state_path.exists()
