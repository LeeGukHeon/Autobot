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


def _make_fake_python_exe(tmp_path: Path) -> Path:
    driver_path = tmp_path / "fake_python_driver.py"
    driver_path.write_text(
        textwrap.dedent(
            """
            import json
            import sys
            from datetime import datetime, timezone
            from pathlib import Path

            ROOT = Path.cwd()
            CANDIDATE_RUN_ID = "candidate-run-001"
            CHAMPION_RUN_ID = "champion-run-000"

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

            def date_to_ts_ms(text: str, *, end_of_day: bool = False) -> int:
                parsed = datetime.fromisoformat(text)
                if end_of_day:
                    parsed = parsed.replace(hour=23, minute=59, second=59, microsecond=999000)
                parsed = parsed.replace(tzinfo=timezone.utc)
                return int(parsed.timestamp() * 1000)

            def append_log(payload: object) -> None:
                log_path = ROOT / "logs" / "fake_python_invocations.jsonl"
                log_path.parent.mkdir(parents=True, exist_ok=True)
                with log_path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(payload) + "\\n")

            args = sys.argv[1:]
            command_key = tuple(args[:4])

            if command_key == ("-m", "autobot.cli", "model", "train"):
                family = arg_value("--model-family", "train_v5_fusion")
                registry_dir = ROOT / "models" / "registry" / family
                candidate_dir = registry_dir / CANDIDATE_RUN_ID
                execution_eval_start = arg_value("--execution-eval-start")
                execution_eval_end = arg_value("--execution-eval-end")
                append_log({"command": "model train"})
                write_json(registry_dir / "latest.json", {"run_id": CANDIDATE_RUN_ID})
                write_json(candidate_dir / "promotion_decision.json", {"status": "candidate", "checks": {"execution_acceptance_enabled": True, "execution_acceptance_present": True}})
                write_json(candidate_dir / "trainer_research_evidence.json", {"available": True, "pass": True, "offline_pass": True, "execution_pass": True, "reasons": ["TRAINER_EVIDENCE_PASS"]})
                write_json(candidate_dir / "search_budget_decision.json", {"status": "default"})
                write_json(candidate_dir / "economic_objective_profile.json", {"profile_id": "cache-test"})
                write_json(candidate_dir / "lane_governance.json", {"lane_id": "cls_primary"})
                write_json(candidate_dir / "decision_surface.json", {"status": "ok"})
                write_json(candidate_dir / "train_config.yaml", {"data_platform_ready_snapshot_id": "snapshot-cache-001"})
                runtime_start = execution_eval_start or arg_value("--start")
                runtime_end = execution_eval_end or arg_value("--end")
                write_json(
                    candidate_dir / "fusion_runtime_input_contract.json",
                    {
                        "snapshot_id": "snapshot-cache-001",
                        "runtime_window": {
                            "start": runtime_start,
                            "end": runtime_end,
                            "start_ts_ms": date_to_ts_ms(runtime_start),
                            "end_ts_ms": date_to_ts_ms(runtime_end, end_of_day=True),
                        },
                        "coverage_start_ts_ms": date_to_ts_ms(runtime_start),
                        "coverage_end_ts_ms": date_to_ts_ms(runtime_end, end_of_day=True),
                        "runtime_rows_after_date_filter": 12,
                        "runtime_dataset_root": str(candidate_dir / "runtime_feature_dataset"),
                    },
                )
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
                model_ref = arg_value("--model-ref")
                preset = arg_value("--preset")
                append_log({"command": "backtest alpha", "model_ref": model_ref, "preset": preset})
                runs_dir = ROOT / "data" / "backtest" / "runs"
                runs_dir.mkdir(parents=True, exist_ok=True)
                run_index = len([item for item in runs_dir.iterdir() if item.is_dir()]) + 1
                run_dir = runs_dir / f"run_{run_index:03d}"
                run_dir.mkdir(parents=True, exist_ok=True)
                payload = {
                    "orders_submitted": 64,
                    "orders_filled": 64,
                    "realized_pnl_quote": 250.0 if model_ref == CANDIDATE_RUN_ID else 100.0,
                    "fill_rate": 0.82 if model_ref == CANDIDATE_RUN_ID else 0.80,
                    "max_drawdown_pct": 0.05 if model_ref == CANDIDATE_RUN_ID else 0.08,
                    "slippage_bps_mean": 1.0 if model_ref == CANDIDATE_RUN_ID else 1.4,
                    "candidates_aborted_by_policy": 0,
                    "execution_structure": {"closed_trade_count": 5, "payoff_ratio": 1.0, "market_loss_concentration": 0.20},
                }
                write_json(run_dir / "summary.json", payload)
                print(json.dumps({"run_dir": str(run_dir), "model_ref": model_ref}))
                sys.exit(0)

            if tuple(args[:2]) == ("-m", "autobot.models.stat_validation"):
                append_log({"command": "stat_validation", "run_dir": arg_value("--run-dir")})
                print(json.dumps({"comparable": True, "deflated_sharpe_ratio_est": 0.75, "probabilistic_sharpe_ratio": 0.90}))
                sys.exit(0)

            if tuple(args[:2]) == ("-m", "autobot.common.operational_overlay_calibration"):
                output_path = arg_value("--output-path")
                if output_path:
                    write_json(Path(output_path), {"report_count": 0, "sufficient_reports": False, "applied_fields": []})
                print("{}")
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "model", "promote"):
                print("promote_ok")
                sys.exit(0)

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


def test_candidate_acceptance_reuses_backtest_and_stat_validation_cache(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(project_root / "data" / "_meta" / "data_platform_ready_snapshot.json", {"snapshot_id": "snapshot-cache-001"})
    _write_json(project_root / "models" / "registry" / "train_v5_fusion" / "champion.json", {"run_id": "champion-run-000"})
    python_exe = _make_fake_python_exe(tmp_path)
    wrapper_script = tmp_path / "run_acceptance_once.ps1"
    wrapper_script.write_text(
        (
            "& "
            + json.dumps(str(ACCEPTANCE_SCRIPT))
            + " -ProjectRoot "
            + json.dumps(str(project_root))
            + " -PythonExe "
            + json.dumps(str(python_exe))
            + " -OutDir "
            + json.dumps("logs/test_acceptance_backtest_cache")
            + " -BatchDate "
            + json.dumps("2026-03-08")
            + " -TrainLookbackDays 2 -BacktestLookbackDays 2 -SkipDailyPipeline -SkipPaperSoak -SkipPromote "
            + "-ModelFamily train_v5_fusion -Trainer v5_fusion\n"
        ),
        encoding="utf-8",
    )
    command = [
        _powershell_exe(),
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(wrapper_script),
    ]

    first = subprocess.run(command, capture_output=True, text=True, check=False)
    assert first.returncode == 0, first.stdout + "\n" + first.stderr
    second = subprocess.run(command, capture_output=True, text=True, check=False)
    assert second.returncode == 0, second.stdout + "\n" + second.stderr

    invocations = [
        json.loads(line)
        for line in (project_root / "logs" / "fake_python_invocations.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    backtests = [item for item in invocations if item["command"] == "backtest alpha"]
    stat_validations = [item for item in invocations if item["command"] == "stat_validation"]
    assert len(backtests) == 4
    assert len(stat_validations) == 4

    report = json.loads(
        (project_root / "logs" / "test_acceptance_backtest_cache" / "latest.json").read_text(encoding="utf-8-sig")
    )
    assert report["backtest_cache_hits"] == 2
    assert report["backtest_cache_misses"] == 0
    assert report["runtime_parity_cache_hits"] == 2
    assert report["runtime_parity_cache_misses"] == 0
    assert report["steps"]["backtest_candidate"]["reused"] is True
    assert report["steps"]["backtest_champion"]["reused"] is True
    assert report["steps"]["backtest_runtime_parity_candidate"]["reused"] is True
    assert report["steps"]["backtest_runtime_parity_champion"]["reused"] is True
    cache_root = project_root / "models" / "registry" / "train_v5_fusion" / "_acceptance_backtest_cache"
    assert cache_root.exists()
    assert any((cache_root / entry).is_dir() for entry in os.listdir(cache_root))
