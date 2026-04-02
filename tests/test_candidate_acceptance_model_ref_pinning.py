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
    normalized = str(path).replace("\\", "/")
    if normalized.endswith("/models/registry/train_v4_crypto_cs/champion.json"):
        mirror = path.parents[1] / "train_v5_fusion" / "champion.json"
        mirror.parent.mkdir(parents=True, exist_ok=True)
        mirror.write_text(json.dumps(payload), encoding="utf-8")


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


            args = sys.argv[1:]
            command_key = tuple(args[:4])

            if command_key == ("-m", "autobot.cli", "model", "train"):
                family = arg_value("--model-family", "train_v4_crypto_cs")
                registry_dir = ROOT / "models" / "registry" / family
                candidate_dir = registry_dir / CANDIDATE_RUN_ID
                execution_eval_start = arg_value("--execution-eval-start")
                execution_eval_end = arg_value("--execution-eval-end")
                write_json(registry_dir / "latest.json", {"run_id": CANDIDATE_RUN_ID})
                write_json(candidate_dir / "promotion_decision.json", {"status": "PASS"})
                if family == "train_v5_fusion":
                    runtime_start = execution_eval_start or arg_value("--start")
                    runtime_end = execution_eval_end or arg_value("--end")
                    write_json(
                        candidate_dir / "fusion_runtime_input_contract.json",
                        {
                            "snapshot_id": "snapshot-test-001",
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
                registry_path = ROOT / "data" / "_meta" / "data_contract_registry.json"
                write_json(
                    registry_path,
                    {
                        "registry_path_default": str(registry_path),
                        "summary": {"contract_count": 3},
                    },
                )
                print(str(registry_path))
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "features", "validate"):
                report_path = ROOT / "data" / "features" / "features_v4" / "_meta" / "validate_report.json"
                write_json(
                    report_path,
                    {
                        "checked_files": 4,
                        "ok_files": 4,
                        "warn_files": 0,
                        "fail_files": 0,
                        "schema_ok": True,
                        "leakage_smoke": "PASS",
                    },
                )
                print(str(report_path))
                sys.exit(0)

            if tuple(args[:2]) == ("-m", "autobot.ops.live_feature_parity_report"):
                report_path = ROOT / "data" / "features" / "features_v4" / "_meta" / "live_feature_parity_report.json"
                write_json(
                    report_path,
                    {
                        "sampled_pairs": 4,
                        "compared_pairs": 4,
                        "passing_pairs": 4,
                        "acceptable": True,
                        "status": "PASS",
                    },
                )
                print(str(report_path))
                sys.exit(0)

            if tuple(args[:2]) == ("-m", "autobot.ops.feature_dataset_certification"):
                report_path = ROOT / "data" / "features" / "features_v4" / "_meta" / "feature_dataset_certification.json"
                write_json(
                    report_path,
                    {
                        "policy": "feature_dataset_certification_v1",
                        "status": "PASS",
                        "pass": True,
                        "reasons": [],
                    },
                )
                print(f"[ops][feature-dataset-certification] path={report_path}")
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "model", "inspect-runtime-dataset"):
                dataset_root = Path(arg_value("--dataset-root"))
                contract_path = dataset_root.parent / "fusion_runtime_input_contract.json"
                contract = json.loads(contract_path.read_text(encoding="utf-8")) if contract_path.exists() else {}
                runtime_window = contract.get("runtime_window", {})
                runtime_start = runtime_window.get("start", "2026-03-07")
                runtime_end = runtime_window.get("end", runtime_start)
                print(
                    json.dumps(
                        {
                            "dataset_root": str(dataset_root),
                            "manifest_path": str(dataset_root / "_meta" / "manifest.parquet"),
                            "data_file_count": 1,
                            "rows": int(contract.get("runtime_rows_after_date_filter", 12) or 0),
                            "min_ts_ms": date_to_ts_ms(runtime_start),
                            "max_ts_ms": date_to_ts_ms(runtime_end, end_of_day=True),
                            "markets": ["KRW-BTC"],
                            "exists": True,
                            "manifest_exists": True,
                        }
                    )
                )
                sys.exit(0)

            if command_key == ("-m", "autobot.cli", "backtest", "alpha"):
                model_ref = arg_value("--model-ref")
                runs_dir = ROOT / "data" / "backtest" / "runs"
                runs_dir.mkdir(parents=True, exist_ok=True)
                run_index = len([item for item in runs_dir.iterdir() if item.is_dir()]) + 1
                run_dir = runs_dir / f"run_{run_index:03d}_actual"
                run_dir.mkdir(parents=True, exist_ok=True)
                if model_ref == CANDIDATE_RUN_ID:
                    summary = {
                        "orders_filled": 64,
                        "realized_pnl_quote": 250.0,
                        "fill_rate": 0.82,
                        "max_drawdown_pct": 0.05,
                        "slippage_bps_mean": 1.0,
                    }
                elif model_ref == CHAMPION_RUN_ID:
                    summary = {
                        "orders_filled": 64,
                        "realized_pnl_quote": 100.0,
                        "fill_rate": 0.80,
                        "max_drawdown_pct": 0.08,
                        "slippage_bps_mean": 1.4,
                    }
                else:
                    summary = {
                        "orders_filled": 0,
                        "realized_pnl_quote": -100.0,
                        "fill_rate": 0.0,
                        "max_drawdown_pct": 1.0,
                        "slippage_bps_mean": 20.0,
                    }
                write_json(run_dir / "summary.json", summary)
                shadow_dir = runs_dir / f"run_{run_index:03d}_shadow"
                shadow_dir.mkdir(parents=True, exist_ok=True)
                write_json(
                    shadow_dir / "summary.json",
                    {
                        "orders_filled": 1,
                        "realized_pnl_quote": -999.0,
                        "fill_rate": 0.01,
                        "max_drawdown_pct": 0.99,
                        "slippage_bps_mean": 50.0,
                    },
                )
                print(json.dumps({"run_dir": str(run_dir), "model_ref": model_ref}))
                sys.exit(0)

            if tuple(args[:2]) == ("-m", "autobot.models.stat_validation"):
                print(
                    json.dumps(
                        {
                            "comparable": True,
                            "deflated_sharpe_ratio_est": 0.75,
                            "probabilistic_sharpe_ratio": 0.90,
                        }
                    )
                )
                sys.exit(0)

            if tuple(args[:2]) == ("-m", "autobot.common.operational_overlay_calibration"):
                output_path = arg_value("--output-path")
                if output_path:
                    write_json(
                        Path(output_path),
                        {"report_count": 1, "sufficient_reports": False, "applied_fields": []},
                    )
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


def _make_fake_paper_smoke_script(tmp_path: Path) -> Path:
    script_path = tmp_path / "fake_paper_smoke.ps1"
    script_path.write_text(
        textwrap.dedent(
            """
            param(
                [string]$ProjectRoot = "",
                [string]$PythonExe = "",
                [int]$DurationSec = 0,
                [string]$Quote = "",
                [int]$TopN = 0,
                [double]$MaxFallbackRatio = 0.0,
                [int]$MinOrdersSubmitted = 0,
                [int]$MinTierCount = 0,
                [int]$MinPolicyEvents = 0,
                [string]$PaperMicroProvider = "",
                [int]$WarmupSec = 0,
                [int]$WarmupMinTradeEventsPerMarket = 0,
                [string]$Strategy = "",
                [string]$Tf = "",
                [string]$ModelRef = "",
                [string]$ModelFamily = "",
                [string]$FeatureSet = "",
                [double]$TopPct = -1.0,
                [double]$MinProb = -1.0,
                [int]$MinCandsPerTs = -1,
                [string]$ExitMode = "",
                [int]$HoldBars = -1,
                [string]$PaperFeatureProvider = "",
                [string]$OutDir = ""
            )

            $ErrorActionPreference = "Stop"
            if ($ModelRef -ne "candidate-run-001") {
                throw "unexpected paper ModelRef: $ModelRef"
            }

            New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
            $runReportPath = Join-Path $OutDir "paper_micro_smoke_20260308-000000.json"
            $latestReportPath = Join-Path $OutDir "latest.json"
            $payload = [ordered]@{
                generated_at = "2026-03-08T00:00:00Z"
                run_id = "paper-run-001"
                orders_submitted = 5
                orders_filled = 3
                fill_rate = 0.60
                realized_pnl_quote = 50.0
                max_drawdown_pct = 0.03
                slippage_bps_mean = 1.1
                micro_quality_score_mean = 0.80
                runtime_risk_multiplier_mean = 1.0
                rolling_active_windows = 2
                rolling_nonnegative_active_window_ratio = 1.0
                rolling_max_fill_concentration_ratio = 0.20
                replace_cancel_timeout_total = 0
                micro_missing_fallback_ratio = 0.0
                gates = [ordered]@{
                    smoke_connectivity_pass = $true
                    t15_gate_pass = $true
                }
            }
            $json = $payload | ConvertTo-Json -Depth 8
            $json | Set-Content -Path $runReportPath -Encoding UTF8
            $json | Set-Content -Path $latestReportPath -Encoding UTF8
            Write-Host ("[paper-smoke] report={0}" -f $runReportPath)
            Write-Host ("[paper-smoke] latest={0}" -f $latestReportPath)
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    return script_path


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required for this test")


def _seed_train_snapshot_close_contract(project_root: Path, *, batch_date: str = "2026-03-07") -> None:
    coverage_start = "2026-01-01"
    certification_start = "2026-02-28"
    train_end = "2026-02-27"
    train_start = "2026-01-29"
    _write_json(project_root / "data" / "_meta" / "data_platform_ready_snapshot.json", {"snapshot_id": "snapshot-test-001"})
    _write_json(
        project_root / "data" / "collect" / "_meta" / "train_snapshot_close_latest.json",
        {
            "policy": "v5_train_snapshot_close_v1",
            "batch_date": batch_date,
            "snapshot_id": "snapshot-test-001",
            "snapshot_root": str(project_root / "data" / "snapshots" / "data_platform" / "snapshot-test-001"),
            "published_at_utc": "2026-03-07T00:05:00Z",
            "generated_at_utc": "2026-03-07T00:05:00Z",
            "deadline_met": True,
            "overall_pass": True,
            "failure_reasons": [],
            "coverage_window": {"start": coverage_start, "end": batch_date},
            "train_window": {"start": train_start, "end": train_end},
            "certification_window": {"start": certification_start, "end": batch_date},
            "coverage_window_source": "test_seed",
            "refresh_argument_mode": "explicit_window",
            "features_v4_effective_end": batch_date,
            "micro_root": str(project_root / "data" / "parquet" / "micro_v1"),
            "micro_date_coverage_counts": {},
            "source_freshness": {
                "candles_api_refresh": {"pass": True},
                "raw_ticks_daily": {"pass": True, "batch_date": batch_date, "batch_covered": True},
            },
        },
    )


def test_candidate_acceptance_pins_concrete_model_refs_for_backtest_and_paper(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    _write_json(
        project_root / "models" / "registry" / "train_v5_fusion" / "champion.json",
        {"run_id": "champion-run-000"},
    )
    _seed_train_snapshot_close_contract(project_root)

    python_exe = _make_fake_python_exe(tmp_path)
    paper_smoke_script = _make_fake_paper_smoke_script(tmp_path)
    powershell_exe = _powershell_exe()

    command = [
        powershell_exe,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(ACCEPTANCE_SCRIPT),
        "-ProjectRoot",
        str(project_root),
        "-PythonExe",
        str(python_exe),
        "-PaperSmokeScript",
        str(paper_smoke_script),
        "-OutDir",
        "logs/test_acceptance",
        "-BatchDate",
        "2026-03-07",
        "-TrainLookbackDays",
        "2",
        "-BacktestLookbackDays",
        "2",
        "-SkipDailyPipeline",
        "-SkipReportRefresh",
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stdout + "\n" + result.stderr

    report_path = project_root / "logs" / "test_acceptance" / "latest.json"
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))

    candidate = report["candidate"]
    backtest_candidate = report["steps"]["backtest_candidate"]
    backtest_champion = report["steps"]["backtest_champion"]
    paper_candidate = report["steps"]["paper_candidate"]

    assert candidate["run_id"] == "candidate-run-001"
    assert candidate["candidate_model_ref_requested"] == "latest_candidate"
    assert candidate["candidate_run_id_used_for_backtest"] == "candidate-run-001"
    assert candidate["candidate_run_id_used_for_paper"] == "candidate-run-001"
    assert candidate["champion_model_ref_requested"] == "champion"
    assert candidate["champion_run_id_used_for_backtest"] == "champion-run-000"
    assert candidate["fusion_run_id"] == "candidate-run-001"
    assert candidate["dependency_trainer_run_ids"] == []
    assert candidate["snapshot_chain_consistent"] is True

    assert backtest_candidate["model_ref_requested"] == "latest_candidate"
    assert backtest_candidate["model_ref_used"] == "candidate-run-001"
    assert "--model-ref candidate-run-001" in backtest_candidate["command"]
    assert backtest_candidate["run_dir"].endswith("run_001_actual")

    assert backtest_champion["model_ref_requested"] == "champion"
    assert backtest_champion["model_ref_used"] == "champion-run-000"
    assert "--model-ref champion-run-000" in backtest_champion["command"]
    assert backtest_champion["run_dir"].endswith("run_003_actual")

    assert paper_candidate["model_ref_requested"] == "latest_candidate"
    assert paper_candidate["model_ref_used"] == "candidate-run-001"
    assert "-ModelRef candidate-run-001" in paper_candidate["command"]
    assert paper_candidate["smoke_report_source"] == "run_report"

    assert report["gates"]["backtest"]["pass"] is True
    assert report["gates"]["paper"]["pass"] is True
    assert report["gates"]["overall_pass"] is True
    assert json.loads(
        (project_root / "models" / "registry" / "train_v5_fusion" / "latest_candidate.json").read_text(encoding="utf-8-sig")
    )["run_id"] == "candidate-run-001"
    global_candidate_pointer = json.loads(
        (project_root / "models" / "registry" / "latest_candidate.json").read_text(encoding="utf-8-sig")
    )
    assert global_candidate_pointer["run_id"] == "candidate-run-001"
    assert global_candidate_pointer["model_family"] == "train_v5_fusion"


def test_candidate_acceptance_can_skip_champion_compare_for_new_baseline(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )
    _seed_train_snapshot_close_contract(project_root)

    python_exe = _make_fake_python_exe(tmp_path)
    paper_smoke_script = _make_fake_paper_smoke_script(tmp_path)
    powershell_exe = _powershell_exe()

    command = [
        powershell_exe,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(ACCEPTANCE_SCRIPT),
        "-ProjectRoot",
        str(project_root),
        "-PythonExe",
        str(python_exe),
        "-PaperSmokeScript",
        str(paper_smoke_script),
        "-OutDir",
        "logs/test_acceptance_skip_compare",
        "-BatchDate",
        "2026-03-07",
        "-TrainLookbackDays",
        "2",
        "-BacktestLookbackDays",
        "2",
        "-SkipChampionCompare",
        "-SkipDailyPipeline",
        "-SkipReportRefresh",
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stdout + "\n" + result.stderr

    report_path = project_root / "logs" / "test_acceptance_skip_compare" / "latest.json"
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))

    assert report["gates"]["overall_pass"] is True
    assert report["gates"]["backtest"]["compare_required"] is False
    assert report["gates"]["backtest"]["decision_basis"] in {"SKIPPED_CHAMPION_COMPARE", "SANITY_ONLY_BACKTEST"}
    assert report["steps"]["backtest_champion"]["reason"] == "SKIPPED_BY_FLAG"
    assert report["steps"]["backtest_runtime_parity_champion"]["reason"] == "SKIPPED_BY_FLAG"


def test_candidate_acceptance_can_compare_v5_candidate_against_v4_champion_family(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()

    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json",
        {"run_id": "champion-run-000"},
    )
    _seed_train_snapshot_close_contract(project_root)

    python_exe = _make_fake_python_exe(tmp_path)
    paper_smoke_script = _make_fake_paper_smoke_script(tmp_path)
    powershell_exe = _powershell_exe()

    command = [
        powershell_exe,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(ACCEPTANCE_SCRIPT),
        "-ProjectRoot",
        str(project_root),
        "-PythonExe",
        str(python_exe),
        "-PaperSmokeScript",
        str(paper_smoke_script),
        "-OutDir",
        "logs/test_acceptance_v5",
        "-BatchDate",
        "2026-03-07",
        "-TrainLookbackDays",
        "2",
        "-BacktestLookbackDays",
        "2",
        "-ModelFamily",
        "train_v5_panel_ensemble",
        "-Trainer",
        "v5_panel_ensemble",
        "-LabelSet",
        "v3",
        "-CandidateModelRef",
        "latest_candidate",
        "-ChampionModelRef",
        "champion",
        "-ChampionModelFamily",
        "train_v4_crypto_cs",
        "-SkipDailyPipeline",
        "-SkipReportRefresh",
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stdout + "\n" + result.stderr

    report_path = project_root / "logs" / "test_acceptance_v5" / "latest.json"
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))

    candidate = report["candidate"]
    backtest_candidate = report["steps"]["backtest_candidate"]
    backtest_champion = report["steps"]["backtest_champion"]

    assert candidate["run_dir"].replace("\\", "/").endswith("/train_v5_panel_ensemble/candidate-run-001")
    assert candidate["champion_model_family_used_for_backtest"] == "train_v4_crypto_cs"
    assert backtest_candidate["command"].count("--model-family") == 1
    assert "--model-family train_v5_panel_ensemble" in backtest_candidate["command"]
    assert "--model-family train_v4_crypto_cs" in backtest_champion["command"]
