import json
import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
ACCEPTANCE_SCRIPT = REPO_ROOT / "scripts" / "candidate_acceptance.ps1"


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


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
            CANDIDATE_RUN_ID = "candidate-run-001"


            def arg_value(name: str, default: str = "") -> str:
                if name not in sys.argv:
                    return default
                index = sys.argv.index(name)
                if index + 1 >= len(sys.argv):
                    return default
                return sys.argv[index + 1]


            def write_json(path: Path, payload: object) -> None:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(payload), encoding="utf-8")


            args = sys.argv[1:]
            command_key = tuple(args[:4])
            if command_key == ("-m", "autobot.cli", "model", "train"):
                family = arg_value("--model-family", "train_v4_crypto_cs")
                registry_dir = ROOT / "models" / "registry" / family
                candidate_dir = registry_dir / CANDIDATE_RUN_ID
                candidate_dir.mkdir(parents=True, exist_ok=True)
                write_json(registry_dir / "latest.json", {"run_id": CANDIDATE_RUN_ID})
                write_json(candidate_dir / "promotion_decision.json", {"status": "candidate"})
                (candidate_dir / "model.bin").write_bytes(b"same-model")
                write_json(candidate_dir / "thresholds.json", {"top_5pct": 0.75})
                print("train_ok")
                sys.exit(0)

            print("unexpected fake python invocation: " + " ".join(args), file=sys.stderr)
            sys.exit(97)
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


def test_candidate_acceptance_short_circuits_duplicate_candidate_before_backtest(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    registry_dir = project_root / "models" / "registry" / "train_v4_crypto_cs"
    champion_dir = registry_dir / "champion-run-000"
    champion_dir.mkdir(parents=True, exist_ok=True)
    _write_json(registry_dir / "champion.json", {"run_id": "champion-run-000"})
    _write_json(registry_dir / "latest_candidate.json", {"run_id": "candidate-prev-000"})
    _write_json(project_root / "models" / "registry" / "latest_candidate.json", {"run_id": "candidate-prev-000", "model_family": "train_v4_crypto_cs"})
    (champion_dir / "model.bin").write_bytes(b"same-model")
    _write_json(champion_dir / "thresholds.json", {"top_5pct": 0.75})

    python_exe = _make_fake_python_exe(tmp_path)
    command = [
        _powershell_exe(),
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(ACCEPTANCE_SCRIPT),
        "-ProjectRoot",
        str(project_root),
        "-PythonExe",
        str(python_exe),
        "-OutDir",
        "logs/test_acceptance",
        "-BatchDate",
        "2026-03-08",
        "-TrainLookbackDays",
        "2",
        "-BacktestLookbackDays",
        "2",
        "-SkipDailyPipeline",
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)

    assert result.returncode == 2, result.stdout + "\n" + result.stderr

    report_path = project_root / "logs" / "test_acceptance" / "latest.json"
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))

    assert report["reasons"] == ["DUPLICATE_CANDIDATE"]
    assert report["gates"]["overall_pass"] is False
    assert report["gates"]["backtest"]["pass"] is False
    assert report["gates"]["backtest"]["decision_basis"] == "DUPLICATE_CANDIDATE"
    assert report["candidate"]["duplicate_candidate"] is True
    assert report["steps"]["features_build"]["attempted"] is False
    assert report["steps"]["features_build"]["reason"] == "SKIPPED_BY_FLAG"
    assert report["steps"]["backtest_candidate"]["attempted"] is False
    assert report["steps"]["backtest_candidate"]["reason"] == "DUPLICATE_CANDIDATE"
    assert report["steps"]["paper_candidate"]["attempted"] is False
    assert report["steps"]["promote"]["reason"] == "DUPLICATE_CANDIDATE"
    assert json.loads((registry_dir / "latest_candidate.json").read_text(encoding="utf-8"))["run_id"] == "candidate-prev-000"
