from __future__ import annotations

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


def _seed_train_snapshot_close_contract(
    project_root: Path,
    *,
    batch_date: str,
    snapshot_id: str,
) -> None:
    _write_json(
        project_root / "data" / "collect" / "_meta" / "train_snapshot_close_latest.json",
        {
            "policy": "v5_train_snapshot_close_v1",
            "batch_date": batch_date,
            "snapshot_id": snapshot_id,
            "snapshot_root": str(project_root / "data" / "snapshots" / "data_platform" / snapshot_id),
            "published_at_utc": "2026-03-08T00:05:00Z",
            "generated_at_utc": "2026-03-08T00:05:00Z",
            "training_critical_start_date": "2026-03-04",
            "training_critical_end_date": batch_date,
            "deadline_met": True,
            "overall_pass": True,
            "failure_reasons": [],
            "micro_root": str(project_root / "data" / "parquet" / "micro_v1"),
            "micro_date_coverage_counts": {},
            "source_freshness": {
                "candles_api_refresh": {"pass": True},
                "raw_ticks_daily": {"pass": True, "batch_date": batch_date, "batch_covered": True},
            },
        },
    )


def _make_fake_python_exe(tmp_path: Path) -> Path:
    driver_path = tmp_path / "fake_python_driver.py"
    driver_path.write_text(
        textwrap.dedent(
            """
            import json
            import sys
            from pathlib import Path

            ROOT = Path.cwd()
            CANDIDATE_RUN_ID = "candidate-run-coverage-001"

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
                family = arg_value("--model-family", "train_v5_fusion")
                registry_dir = ROOT / "models" / "registry" / family
                candidate_dir = registry_dir / CANDIDATE_RUN_ID
                write_json(registry_dir / "latest.json", {"run_id": CANDIDATE_RUN_ID})
                write_json(candidate_dir / "promotion_decision.json", {"status": "candidate"})
                write_json(candidate_dir / "trainer_research_evidence.json", {"available": True, "pass": True})
                write_json(candidate_dir / "search_budget_decision.json", {"status": "default"})
                write_json(candidate_dir / "economic_objective_profile.json", {"profile_id": "coverage-test"})
                write_json(candidate_dir / "lane_governance.json", {"lane_id": "cls_primary"})
                write_json(candidate_dir / "decision_surface.json", {"status": "ok"})
                write_json(candidate_dir / "train_config.yaml", {"data_platform_ready_snapshot_id": "snapshot-coverage-001"})
                print(json.dumps({"run_dir": str(candidate_dir), "run_id": CANDIDATE_RUN_ID}))
                sys.exit(0)

            if tuple(args[:2]) == ("-m", "autobot.ops.data_contract_registry"):
                report_path = ROOT / "data" / "_meta" / "data_contract_registry.json"
                write_json(report_path, {"summary": {"contract_count": 1}})
                print(str(report_path))
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "features", "validate"):
                report_path = ROOT / "data" / "features" / "features_v4" / "_meta" / "validate_report.json"
                write_json(report_path, {"checked_files": 1, "ok_files": 1, "warn_files": 0, "fail_files": 0, "schema_ok": True, "leakage_smoke": "PASS"})
                print(str(report_path))
                sys.exit(0)

            if tuple(args[:2]) == ("-m", "autobot.ops.live_feature_parity_report"):
                report_path = ROOT / "data" / "features" / "features_v4" / "_meta" / "live_feature_parity_report.json"
                write_json(report_path, {"sampled_pairs": 1, "compared_pairs": 1, "passing_pairs": 1, "acceptable": True, "status": "PASS"})
                print(str(report_path))
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "backtest", "alpha"):
                raise RuntimeError("backtest should not run when runtime coverage preflight fails")

            if tuple(args[:2]) == ("-m", "autobot.models.stat_validation"):
                raise RuntimeError("stat validation should not run when runtime coverage preflight fails")

            print("unexpected fake python invocation: " + " ".join(args), file=sys.stderr)
            sys.exit(1)
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


def test_candidate_acceptance_fails_early_on_runtime_dataset_coverage_gap(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(project_root / "models" / "registry" / "train_v5_fusion" / "champion.json", {"run_id": "champion-run-000"})
    _write_json(project_root / "data" / "_meta" / "data_platform_ready_snapshot.json", {"snapshot_id": "snapshot-coverage-001"})
    _seed_train_snapshot_close_contract(project_root, batch_date="2026-03-08", snapshot_id="snapshot-coverage-001")
    python_exe = _make_fake_python_exe(tmp_path)
    wrapper_script = tmp_path / "run_acceptance.ps1"
    wrapper_script.write_text(
        (
            "& "
            + json.dumps(str(ACCEPTANCE_SCRIPT))
            + " -ProjectRoot "
            + json.dumps(str(project_root))
            + " -PythonExe "
            + json.dumps(str(python_exe))
            + " -OutDir "
            + json.dumps("logs/test_acceptance_runtime_coverage")
            + " -BatchDate "
            + json.dumps("2026-03-08")
            + " -TrainLookbackDays 2 -BacktestLookbackDays 2 -SkipDailyPipeline -SkipPaperSoak -SkipPromote "
            + "-ModelFamily train_v5_fusion -Trainer v5_fusion\n"
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        [_powershell_exe(), "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(wrapper_script)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode in (0, 2), result.stdout + "\n" + result.stderr
    report = json.loads(
        (project_root / "logs" / "test_acceptance_runtime_coverage" / "latest.json").read_text(encoding="utf-8-sig")
    )
    assert report["reasons"] == ["CANDIDATE_RUNTIME_DATASET_CERTIFICATION_WINDOW_EMPTY"]
    assert report["steps"]["runtime_dataset_coverage_preflight"]["pass"] is False
    assert report["steps"]["train_snapshot_close_preflight"]["training_critical_start_date"] == "2026-03-04"
    assert report["steps"]["train_snapshot_close_preflight"]["training_critical_end_date"] == "2026-03-08"
