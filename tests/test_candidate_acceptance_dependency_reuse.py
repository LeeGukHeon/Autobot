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


def _seed_train_snapshot_close_contract(project_root: Path) -> None:
    _write_json(
        project_root / "data" / "collect" / "_meta" / "train_snapshot_close_latest.json",
        {
            "policy": "v5_train_snapshot_close_v1",
            "batch_date": "2026-03-08",
            "snapshot_id": "snapshot-dependency-002",
            "snapshot_root": str(project_root / "data" / "snapshots" / "data_platform" / "snapshot-dependency-002"),
            "published_at_utc": "2026-03-08T00:05:00Z",
            "generated_at_utc": "2026-03-08T00:05:00Z",
            "training_critical_start_date": "2026-03-04",
            "training_critical_end_date": "2026-03-08",
            "coverage_window": {"start": "2026-03-04", "end": "2026-03-08"},
            "deadline_met": True,
            "overall_pass": True,
            "failure_reasons": [],
            "micro_root": str(project_root / "data" / "parquet" / "micro_v1"),
            "micro_date_coverage_counts": {},
            "source_freshness": {
                "candles_api_refresh": {"pass": True},
                "raw_ticks_daily": {"pass": True, "batch_date": "2026-03-08", "batch_covered": True},
            },
        },
    )


def _make_fake_python_exe(tmp_path: Path) -> Path:
    driver_path = tmp_path / "fake_python_driver.py"
    driver_path.write_text(
        textwrap.dedent(
            """
            from datetime import datetime, timezone
            import json
            import sys
            from pathlib import Path

            ROOT = Path.cwd()
            SNAPSHOT_ID = "snapshot-dependency-002"
            PANEL_RUN_ID = "panel-run-002"
            SEQ_RUN_ID = "sequence-run-002"
            LOB_RUN_ID = "lob-run-002"
            FUSION_RUN_ID = "fusion-run-002"

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

            def append_log(payload: object) -> None:
                log_path = ROOT / "logs" / "fake_python_invocations.jsonl"
                log_path.parent.mkdir(parents=True, exist_ok=True)
                with log_path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(payload) + "\\n")

            def date_to_ts_ms(value: str, end_of_day: bool = False) -> int:
                parsed = datetime.fromisoformat(value)
                if end_of_day:
                    parsed = parsed.replace(hour=23, minute=59, second=59, microsecond=999000)
                parsed = parsed.replace(tzinfo=timezone.utc)
                return int(parsed.timestamp() * 1000)

            def train_config_doc(family: str, trainer: str) -> dict:
                return {
                    "trainer": trainer,
                    "model_family": family,
                    "data_platform_ready_snapshot_id": SNAPSHOT_ID,
                    "feature_set": arg_value("--feature-set"),
                    "label_set": arg_value("--label-set"),
                    "task": arg_value("--task"),
                    "run_scope": arg_value("--run-scope"),
                    "tf": arg_value("--tf"),
                    "quote": arg_value("--quote"),
                    "top_n": int(arg_value("--top-n", "0") or 0),
                    "start": arg_value("--start"),
                    "end": arg_value("--end"),
                    "execution_acceptance_eval_start": arg_value("--execution-eval-start"),
                    "execution_acceptance_eval_end": arg_value("--execution-eval-end"),
                    "seed": int(arg_value("--seed", "0") or 0),
                    "dependency_expert_only": ("--dependency-expert-only" in sys.argv),
                }

            def expert_run(family: str, trainer: str, run_id: str) -> Path:
                run_dir = ROOT / "models" / "registry" / family / run_id
                run_dir.mkdir(parents=True, exist_ok=True)
                write_json(run_dir / "train_config.yaml", train_config_doc(family, trainer))
                write_json(run_dir / "artifact_status.json", {
                    "run_id": run_id,
                    "status": "candidate",
                    "core_saved": True,
                    "support_artifacts_written": True,
                    "tail_context_written": True,
                    "execution_acceptance_complete": True,
                    "runtime_recommendations_complete": True,
                    "governance_artifacts_complete": True,
                    "promotion_complete": True,
                    "decision_surface_complete": True,
                    "expert_prediction_table_complete": True,
                })
                write_json(run_dir / "promotion_decision.json", {"status": "candidate", "promotion_mode": "dependency"})
                write_json(run_dir / "trainer_research_evidence.json", {"available": True, "pass": True})
                write_json(run_dir / "search_budget_decision.json", {"status": "default"})
                write_json(run_dir / "economic_objective_profile.json", {"profile_id": "test"})
                write_json(run_dir / "lane_governance.json", {"lane_id": "cls_primary"})
                write_json(run_dir / "decision_surface.json", {"status": "ok"})
                write_json(run_dir / "runtime_recommendations.json", {"status": "ready"})
                write_json(run_dir / "execution_acceptance_report.json", {"status": "trainer_runtime_contract_ready"})
                table = run_dir / "expert_prediction_table.parquet"
                table.write_bytes(b"PAR1")
                return run_dir

            def export_run(family: str, trainer: str, run_id: str, start: str, end: str, explicit_markets: list[str], resolve_only: bool, anchor_export_path: str) -> dict:
                export_root = ROOT / "models" / "registry" / family / run_id / "_runtime_exports" / f"{start}__{end}"
                export_root.mkdir(parents=True, exist_ok=True)
                export_path = export_root / "expert_prediction_table.parquet"
                metadata_path = export_root / "metadata.json"
                reused = export_path.exists() and metadata_path.exists()
                metadata = {
                    "run_id": run_id,
                    "trainer": trainer,
                    "model_family": family,
                    "data_platform_ready_snapshot_id": SNAPSHOT_ID,
                    "start": start,
                    "end": end,
                    "coverage_start_ts_ms": date_to_ts_ms(start),
                    "coverage_end_ts_ms": date_to_ts_ms(end, end_of_day=True),
                    "coverage_start_date": start,
                    "coverage_end_date": end,
                    "coverage_dates": [start, end] if start == end else [],
                    "window_timezone": "Asia/Seoul",
                    "rows": 12,
                    "requested_selected_markets": explicit_markets,
                    "selected_markets": explicit_markets or ["KRW-BTC", "KRW-ETH"],
                    "selected_markets_source": "acceptance_common_runtime_universe" if explicit_markets else "window_available_markets_fallback",
                    "fallback_reason": "",
                    "anchor_alignment_complete": bool(anchor_export_path) if trainer != "v5_panel_ensemble" else False,
                    "anchor_export_path": anchor_export_path or "",
                }
                if start != end:
                    metadata["coverage_dates"] = []
                    cursor = start
                    while cursor <= end:
                        metadata["coverage_dates"].append(cursor)
                        y,m,d = map(int, cursor.split("-"))
                        import datetime
                        cursor = (datetime.date(y,m,d) + datetime.timedelta(days=1)).isoformat()
                if resolve_only:
                    return {
                        **metadata,
                        "export_path": "",
                        "metadata_path": "",
                        "reused": False,
                        "source_mode": "resolve_markets_only",
                    }
                export_path.write_bytes(b"PAR1")
                write_json(metadata_path, metadata)
                return {
                    **metadata,
                    "export_path": str(export_path),
                    "metadata_path": str(metadata_path),
                    "reused": reused,
                    "source_mode": "existing_export" if reused else "fresh_export",
                }

            args = sys.argv[1:]
            command_key = tuple(args[:4])

            if command_key == ("-m", "autobot.cli", "features", "build"):
                print("features_build_ok")
                sys.exit(0)

            if tuple(args[:2]) == ("-m", "autobot.ops.data_contract_registry"):
                report_path = ROOT / "data" / "_meta" / "data_contract_registry.json"
                write_json(report_path, {"summary": {"contract_count": 1}, "entries": [{"contract_id": "feature_dataset:features_v4"}]})
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

            if tuple(args[:2]) == ("-m", "autobot.ops.feature_dataset_certification"):
                report_path = ROOT / "data" / "features" / "features_v4" / "_meta" / "feature_dataset_certification.json"
                write_json(report_path, {"policy": "feature_dataset_certification_v1", "status": "PASS", "pass": True, "reasons": []})
                print(f"[ops][feature-dataset-certification] path={report_path}")
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "model", "train"):
                trainer = arg_value("--trainer")
                family = arg_value("--model-family")
                append_log({
                    "command": "model train",
                    "trainer": trainer,
                    "family": family,
                    "args": args,
                })
                if trainer == "v5_panel_ensemble":
                    run_dir = expert_run(family, trainer, PANEL_RUN_ID)
                    print(json.dumps({"run_dir": str(run_dir), "run_id": PANEL_RUN_ID}))
                    sys.exit(0)
                if trainer == "v5_sequence":
                    run_dir = expert_run(family, trainer, SEQ_RUN_ID)
                    print(json.dumps({"run_dir": str(run_dir), "run_id": SEQ_RUN_ID}))
                    sys.exit(0)
                if trainer == "v5_lob":
                    run_dir = expert_run(family, trainer, LOB_RUN_ID)
                    print(json.dumps({"run_dir": str(run_dir), "run_id": LOB_RUN_ID}))
                    sys.exit(0)
                if trainer == "v5_fusion":
                    runtime_start = arg_value("--fusion-runtime-start")
                    runtime_end = arg_value("--fusion-runtime-end")
                    run_dir = expert_run(family, trainer, FUSION_RUN_ID)
                    write_json(run_dir / "fusion_runtime_input_contract.json", {
                        "snapshot_id": SNAPSHOT_ID,
                        "runtime_window": {
                            "start": runtime_start,
                            "end": runtime_end,
                            "start_ts_ms": 1774656000000,
                            "end_ts_ms": 1774742399999,
                        },
                        "coverage_start_ts_ms": 1774656000000,
                        "coverage_end_ts_ms": 1774742399999,
                        "runtime_rows_after_date_filter": 12,
                        "runtime_dataset_root": str(run_dir / "runtime_feature_dataset"),
                    })
                    print(json.dumps({"run_dir": str(run_dir), "run_id": FUSION_RUN_ID}))
                    sys.exit(0)

            if command_key == ("-m", "autobot.cli", "model", "export-expert-table"):
                trainer = arg_value("--trainer")
                run_dir = Path(arg_value("--run-dir"))
                start = arg_value("--start")
                end = arg_value("--end")
                explicit_markets = [item.strip() for item in arg_value("--markets").split(",") if item.strip()]
                anchor_export_path = arg_value("--anchor-export-path")
                resolve_only = "--resolve-markets-only" in sys.argv
                family = run_dir.parent.name
                run_id = run_dir.name
                append_log({
                    "command": "model export-expert-table",
                    "trainer": trainer,
                    "run_id": run_id,
                    "start": start,
                    "end": end,
                    "anchor_export_path": anchor_export_path,
                    "markets": explicit_markets,
                    "resolve_markets_only": resolve_only,
                })
                print(json.dumps(export_run(family, trainer, run_id, start, end, explicit_markets, resolve_only, anchor_export_path)))
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "model", "inspect-runtime-dataset"):
                dataset_root = Path(arg_value("--dataset-root"))
                contract_path = dataset_root.parent / "fusion_runtime_input_contract.json"
                contract = json.loads(contract_path.read_text(encoding="utf-8")) if contract_path.exists() else {}
                runtime_window = contract.get("runtime_window", {})
                runtime_start = runtime_window.get("start", "2026-03-07")
                runtime_end = runtime_window.get("end", runtime_start)
                append_log({
                    "command": "model inspect-runtime-dataset",
                    "dataset_root": str(dataset_root),
                })
                print(json.dumps({
                    "dataset_root": str(dataset_root),
                    "manifest_path": str(dataset_root / "_meta" / "manifest.parquet"),
                    "data_file_count": 1,
                    "rows": int(contract.get("runtime_rows_after_date_filter", 12) or 0),
                    "min_ts_ms": date_to_ts_ms(runtime_start),
                    "max_ts_ms": date_to_ts_ms(runtime_end, end_of_day=True),
                    "markets": ["KRW-BTC"],
                    "exists": True,
                    "manifest_exists": True,
                }))
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "backtest", "alpha"):
                model_ref = arg_value("--model-ref")
                runs_dir = ROOT / "data" / "backtest" / "runs"
                run_dir = runs_dir / ("candidate" if model_ref == FUSION_RUN_ID else "champion")
                run_dir.mkdir(parents=True, exist_ok=True)
                payload = {
                    "orders_filled": 64,
                    "realized_pnl_quote": 250.0 if model_ref == FUSION_RUN_ID else 100.0,
                    "fill_rate": 0.82,
                    "max_drawdown_pct": 0.05 if model_ref == FUSION_RUN_ID else 0.08,
                    "slippage_bps_mean": 1.0 if model_ref == FUSION_RUN_ID else 1.4,
                }
                write_json(run_dir / "summary.json", payload)
                print(json.dumps({"run_dir": str(run_dir), "model_ref": model_ref}))
                sys.exit(0)

            if tuple(args[:2]) == ("-m", "autobot.models.stat_validation"):
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


def test_candidate_acceptance_reuses_matching_dependency_runs(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_json(
        project_root / "data" / "_meta" / "data_platform_ready_snapshot.json",
        {"snapshot_id": "snapshot-dependency-002"},
    )
    _seed_train_snapshot_close_contract(project_root)
    _write_json(
        project_root / "models" / "registry" / "train_v5_fusion" / "champion.json",
        {"run_id": "champion-run-000"},
    )
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
            + json.dumps("logs/test_acceptance_v5_dependency_reuse")
            + " -BatchDate "
            + json.dumps("2026-03-08")
            + " -TrainLookbackDays 2 -BacktestLookbackDays 2 -SkipDailyPipeline -SkipPaperSoak -SkipPromote "
            + "-ModelFamily train_v5_fusion -Trainer v5_fusion -DependencyTrainers @(\"v5_panel_ensemble\",\"v5_sequence\",\"v5_lob\")\n"
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
    train_calls = [row for row in invocations if row.get("command") == "model train"]
    export_calls = [row for row in invocations if row.get("command") == "model export-expert-table"]
    trainer_names = [row["trainer"] for row in train_calls]
    assert trainer_names.count("v5_panel_ensemble") == 1
    assert trainer_names.count("v5_sequence") == 1
    assert trainer_names.count("v5_lob") == 1
    assert trainer_names.count("v5_fusion") == 2
    assert len(export_calls) == 12

    report = json.loads(
        (project_root / "logs" / "test_acceptance_v5_dependency_reuse" / "latest.json").read_text(encoding="utf-8-sig")
    )
    assert report["steps"]["dependency_trainers"]["trained_count"] == 0
    assert report["steps"]["dependency_trainers"]["reused_count"] == 3
    results = report["steps"]["dependency_trainers"]["results"]
    assert [item["trainer"] for item in results] == [
        "v5_panel_ensemble",
        "v5_sequence",
        "v5_lob",
    ]
    assert all(item["reused"] is True for item in results)
    assert all(item["source_mode"] == "existing_run" for item in results)
    assert all(item["required_artifacts_complete"] is True for item in results)
    assert results[0]["tail_mode"] == "dependency_expert_only"
    assert results[1]["tail_mode"] == "expert_tail"
    assert results[2]["tail_mode"] == "expert_tail"
    runtime_exports = report["steps"]["dependency_runtime_exports"]["results"]
    assert all(item["reused"] is True for item in runtime_exports)
