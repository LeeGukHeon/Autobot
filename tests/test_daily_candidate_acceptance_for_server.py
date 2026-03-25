import json
import shutil
import subprocess
import textwrap
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "daily_candidate_acceptance_for_server.ps1"


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required for this test")


def _make_fake_acceptance_script(
    tmp_path: Path,
    *,
    payload: dict,
    exit_code: int,
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
                [switch]$SkipDailyPipeline,
                [switch]$SkipReportRefresh,
                [switch]$DryRun
            )

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


def _seed_preflight_minimum(project_root: Path) -> None:
    family_dir = project_root / "models" / "registry" / "train_v4_crypto_cs"
    family_dir.mkdir(parents=True, exist_ok=True)
    (family_dir / "champion-run-000").mkdir(parents=True, exist_ok=True)
    (family_dir / "champion.json").write_text(json.dumps({"run_id": "champion-run-000"}), encoding="utf-8")


def test_daily_candidate_acceptance_treats_scout_only_budget_rejection_as_success(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _seed_preflight_minimum(project_root)
    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        payload={
            "gates": {
                "backtest": {
                    "budget_contract_reasons": ["SCOUT_ONLY_BUDGET_EVIDENCE"],
                }
            },
            "reasons": ["BACKTEST_ACCEPTANCE_FAILED", "SCOUT_ONLY_BUDGET_EVIDENCE"],
        },
        exit_code=2,
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
            "-AcceptanceScript",
            str(acceptance_script),
            "-BatchDate",
            "2026-03-08",
            "-SkipDailyPipeline",
            "-SkipReportRefresh",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    assert "scout_nonfatal_reason=SCOUT_ONLY_BUDGET_EVIDENCE" in completed.stdout


def test_daily_candidate_acceptance_preserves_fatal_acceptance_failure(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _seed_preflight_minimum(project_root)
    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        payload={
            "steps": {
                "exception": {"message": "boom"},
            },
            "reasons": ["UNHANDLED_EXCEPTION"],
        },
        exit_code=2,
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
            "-AcceptanceScript",
            str(acceptance_script),
            "-BatchDate",
            "2026-03-08",
            "-SkipDailyPipeline",
            "-SkipReportRefresh",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 2


def test_daily_candidate_acceptance_treats_bootstrap_only_policy_as_success(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _seed_preflight_minimum(project_root)
    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        payload={
            "candidate": {
                "lane_mode": "bootstrap_latest_inclusive",
                "promotion_eligible": False,
            },
            "split_policy": {
                "lane_mode": "bootstrap_latest_inclusive",
                "promotion_eligible": False,
            },
            "reasons": ["BOOTSTRAP_ONLY_POLICY"],
        },
        exit_code=2,
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
            "-AcceptanceScript",
            str(acceptance_script),
            "-BatchDate",
            "2026-03-08",
            "-SkipDailyPipeline",
            "-SkipReportRefresh",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    assert "bootstrap_nonfatal_reason=BOOTSTRAP_ONLY_POLICY" in completed.stdout


def test_daily_candidate_acceptance_prefers_final_json_report_over_daily_markdown_report(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _seed_preflight_minimum(project_root)
    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        payload={
            "gates": {
                "backtest": {
                    "budget_contract_reasons": ["SCOUT_ONLY_BUDGET_EVIDENCE"],
                }
            },
            "reasons": ["BACKTEST_ACCEPTANCE_FAILED", "SCOUT_ONLY_BUDGET_EVIDENCE"],
        },
        exit_code=2,
        emit_daily_micro_report=True,
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
            "-AcceptanceScript",
            str(acceptance_script),
            "-BatchDate",
            "2026-03-08",
            "-SkipDailyPipeline",
            "-SkipReportRefresh",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    assert "scout_nonfatal_reason=SCOUT_ONLY_BUDGET_EVIDENCE" in completed.stdout


def test_daily_candidate_acceptance_fails_fast_on_server_preflight_violation(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _seed_preflight_minimum(project_root)
    family_dir = project_root / "models" / "registry" / "train_v4_crypto_cs"
    (family_dir / "latest_candidate.json").write_text(json.dumps({"run_id": "candidate-run-stale"}), encoding="utf-8")
    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        payload={"reasons": []},
        exit_code=0,
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
            "-AcceptanceScript",
            str(acceptance_script),
            "-BatchDate",
            "2026-03-08",
            "-SkipDailyPipeline",
            "-SkipReportRefresh",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 2
    assert "preflight_failed" in completed.stdout
    assert not (project_root / "logs" / "fake_acceptance" / "report.json").exists()
    preflight_report = json.loads((project_root / "logs" / "ops" / "server_preflight" / "latest.json").read_text(encoding="utf-8-sig"))
    assert preflight_report["required_pointers"] == ["champion"]
    assert preflight_report["check_candidate_state_consistency"] is True
