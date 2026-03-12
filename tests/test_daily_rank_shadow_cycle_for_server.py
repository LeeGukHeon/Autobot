import json
import shutil
import subprocess
import textwrap
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "daily_rank_shadow_cycle_for_server.ps1"


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required for this test")


def _make_fake_acceptance_script(tmp_path: Path, *, payload: dict, exit_code: int) -> Path:
    script_path = tmp_path / f"fake_rank_shadow_acceptance_{exit_code}.ps1"
    payload_json = json.dumps(payload)
    script_path.write_text(
        textwrap.dedent(
            f"""
            param(
                [string]$ProjectRoot = "",
                [string]$PythonExe = "",
                [string]$BatchDate = "",
                [switch]$SkipDailyPipeline,
                [switch]$SkipReportRefresh,
                [switch]$SkipPaperSoak,
                [switch]$DryRun
            )

            $reportPath = Join-Path $ProjectRoot "logs/fake_rank_shadow_acceptance/report.json"
            New-Item -ItemType Directory -Force -Path (Split-Path -Parent $reportPath) | Out-Null
            @'
            {payload_json}
            '@ | Set-Content -Path $reportPath -Encoding UTF8
            Write-Host ("[fake-rank-shadow] report={{0}}" -f $reportPath)
            exit {exit_code}
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    return script_path


def test_rank_shadow_cycle_marks_shadow_pass_ready_for_manual_governance(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        payload={
            "candidate": {
                "run_id": "rank-run-001",
                "run_dir": str(project_root / "models" / "registry" / "train_v4_crypto_cs" / "rank-run-001"),
                "lane_id": "rank_shadow",
                "lane_role": "shadow",
                "lane_shadow_only": True,
                "lane_promotion_allowed": False,
            },
            "config": {
                "task": "rank",
            },
            "gates": {
                "overall_pass": True,
                "backtest": {
                    "pass": True,
                    "decision_basis": "PARETO_DOMINANCE",
                },
            },
            "reasons": [],
            "notes": ["SHADOW_LANE_ONLY"],
        },
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

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    latest = json.loads((project_root / "logs" / "model_v4_rank_shadow_cycle" / "latest.json").read_text(encoding="utf-8-sig"))
    governed = json.loads((project_root / "logs" / "model_v4_rank_shadow_cycle" / "latest_governed_candidate.json").read_text(encoding="utf-8-sig"))
    governance = json.loads((project_root / "logs" / "model_v4_rank_shadow_cycle" / "latest_governance_action.json").read_text(encoding="utf-8-sig"))

    assert latest["status"] == "shadow_pass"
    assert latest["next_action"] == "use_rank_governed_lane"
    assert latest["candidate_run_id"] == "rank-run-001"
    assert latest["lane_id"] == "rank_shadow"
    assert governed["status"] == "shadow_pass"
    assert governance["selected_lane_id"] == "rank_governed_primary"
    assert governance["selected_acceptance_script"] == "v4_rank_governed_candidate_acceptance.ps1"


def test_rank_shadow_cycle_preserves_fatal_acceptance_failure_and_writes_cycle_report(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    governed_candidate_path = project_root / "logs" / "model_v4_rank_shadow_cycle" / "latest_governed_candidate.json"
    governed_candidate_path.parent.mkdir(parents=True, exist_ok=True)
    governed_candidate_path.write_text(json.dumps({"status": "shadow_pass"}), encoding="utf-8")
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
    latest = json.loads((project_root / "logs" / "model_v4_rank_shadow_cycle" / "latest.json").read_text(encoding="utf-8-sig"))
    governance = json.loads((project_root / "logs" / "model_v4_rank_shadow_cycle" / "latest_governance_action.json").read_text(encoding="utf-8-sig"))

    assert latest["status"] == "fatal_error"
    assert latest["next_action"] == "use_cls_primary_lane"
    assert governance["selected_lane_id"] == "cls_primary"
    assert governance["selected_acceptance_script"] == "v4_promotable_candidate_acceptance.ps1"
    assert latest["governance_action"]["selected_lane_id"] == "cls_primary"
    assert not governed_candidate_path.exists()


def test_rank_shadow_cycle_accepts_comma_joined_array_args_from_installer(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    acceptance_script = _make_fake_acceptance_script(
        tmp_path,
        payload={
            "candidate": {
                "run_id": "rank-run-array-001",
                "run_dir": str(project_root / "models" / "registry" / "train_v4_crypto_cs" / "rank-run-array-001"),
                "lane_id": "rank_shadow",
                "lane_role": "shadow",
                "lane_shadow_only": True,
                "lane_promotion_allowed": False,
            },
            "config": {
                "task": "rank",
            },
            "gates": {
                "overall_pass": True,
                "backtest": {
                    "pass": True,
                    "decision_basis": "PARETO_DOMINANCE",
                },
            },
            "reasons": [],
            "notes": ["SHADOW_LANE_ONLY"],
        },
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
            "-BlockOnActiveUnits",
            "autobot-v4-challenger-spawn.service,autobot-v4-challenger-promote.service",
            "-AcceptanceArgs",
            "-SkipPaperSoak",
            "-SkipDailyPipeline",
            "-SkipReportRefresh",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    latest = json.loads((project_root / "logs" / "model_v4_rank_shadow_cycle" / "latest.json").read_text(encoding="utf-8-sig"))

    assert latest["batch_date"] == "2026-03-08"
    assert latest["status"] == "shadow_pass"
    assert latest["candidate_run_id"] == "rank-run-array-001"
