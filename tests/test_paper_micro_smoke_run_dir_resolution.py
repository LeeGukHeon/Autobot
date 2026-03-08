import json
import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
PAPER_SMOKE_SCRIPT = REPO_ROOT / "scripts" / "paper_micro_smoke.ps1"


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required for this test")


def _make_fake_python_exe(tmp_path: Path) -> Path:
    driver_path = tmp_path / "fake_python_driver.py"
    driver_path.write_text(
        textwrap.dedent(
            """
            import json
            import sys
            from pathlib import Path

            ROOT = Path.cwd()


            def write_json(path: Path, payload: object) -> None:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(payload), encoding="utf-8")


            args = sys.argv[1:]
            if tuple(args[:4]) != ("-m", "autobot.cli", "paper", "run"):
                print("unexpected fake python invocation: " + " ".join(args), file=sys.stderr)
                sys.exit(1)

            runs_dir = ROOT / "data" / "paper" / "runs"
            actual_dir = runs_dir / "paper-actual-001"
            shadow_dir = runs_dir / "paper-shadow-001"
            actual_dir.mkdir(parents=True, exist_ok=True)
            shadow_dir.mkdir(parents=True, exist_ok=True)

            write_json(
                actual_dir / "summary.json",
                {
                    "orders_submitted": 5,
                    "orders_filled": 3,
                    "fill_rate": 0.60,
                    "realized_pnl_quote": 42.0,
                    "max_drawdown_pct": 0.03,
                    "slippage_bps_mean": 1.2,
                    "micro_quality_score_mean": 0.75,
                    "runtime_risk_multiplier_mean": 1.0,
                    "rolling_active_windows": 2,
                    "rolling_windows_total": 2,
                    "rolling_nonnegative_active_window_ratio": 1.0,
                    "rolling_max_fill_concentration_ratio": 0.20,
                    "rolling_max_window_drawdown_pct": 0.03,
                },
            )
            write_json(
                actual_dir / "micro_order_policy_report.json",
                {
                    "fallback_reasons": {},
                    "tiers": {"t1": 3},
                    "replaces_total": 0,
                    "cancels_total": 0,
                    "aborted_timeout_total": 0,
                },
            )
            write_json(
                shadow_dir / "summary.json",
                {
                    "orders_submitted": 0,
                    "orders_filled": 0,
                    "fill_rate": 0.0,
                    "realized_pnl_quote": -999.0,
                    "max_drawdown_pct": 0.99,
                    "slippage_bps_mean": 50.0,
                    "micro_quality_score_mean": 0.0,
                    "runtime_risk_multiplier_mean": 1.0,
                    "rolling_active_windows": 0,
                    "rolling_windows_total": 1,
                    "rolling_nonnegative_active_window_ratio": 0.0,
                    "rolling_max_fill_concentration_ratio": 1.0,
                    "rolling_max_window_drawdown_pct": 0.99,
                },
            )
            write_json(
                shadow_dir / "micro_order_policy_report.json",
                {
                    "fallback_reasons": {"MICRO_MISSING_FALLBACK": 99},
                    "tiers": {},
                    "replaces_total": 0,
                    "cancels_total": 0,
                    "aborted_timeout_total": 0,
                },
            )

            print(json.dumps({"run_dir": str(actual_dir)}))
            sys.exit(0)
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    if os.name == "nt":
        wrapper_path = tmp_path / "fake_python.cmd"
        wrapper_path.write_text(
            f'@echo off\r\n"{sys.executable}" "%~dp0fake_python_driver.py" %*\r\n',
            encoding="utf-8",
        )
    else:
        wrapper_path = tmp_path / "fake_python"
        wrapper_path.write_text(
            "#!/bin/sh\n"
            f'"{sys.executable}" "$(dirname "$0")/fake_python_driver.py" "$@"\n',
            encoding="utf-8",
        )
        wrapper_path.chmod(0o755)
    return wrapper_path


def test_paper_micro_smoke_uses_reported_run_dir_instead_of_latest_directory(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    python_exe = _make_fake_python_exe(tmp_path)
    powershell_exe = _powershell_exe()

    command = [
        powershell_exe,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(PAPER_SMOKE_SCRIPT),
        "-ProjectRoot",
        str(project_root),
        "-PythonExe",
        str(python_exe),
        "-DurationSec",
        "1",
        "-MinOrdersSubmitted",
        "1",
        "-MinTierCount",
        "1",
        "-MinPolicyEvents",
        "0",
        "-OutDir",
        "logs/test_paper_smoke",
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stdout + "\n" + result.stderr

    report_path = project_root / "logs" / "test_paper_smoke" / "latest.json"
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))

    assert report["run_id"] == "paper-actual-001"
    assert report["run_dir"].endswith("paper-actual-001")
    assert report["orders_submitted"] == 5
    assert report["orders_filled"] == 3
    assert report["realized_pnl_quote"] == 42.0
    assert report["gates"]["smoke_connectivity_pass"] is True
    assert report["gates"]["t15_gate_pass"] is True
