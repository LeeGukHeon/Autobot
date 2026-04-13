from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required for installer dry-run test")


def _run_script_dry_run(script_name: str, *extra_args: str) -> str:
    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(REPO_ROOT / "scripts" / script_name),
            "-ProjectRoot",
            str(REPO_ROOT),
            "-PythonExe",
            "python",
            *extra_args,
            "-DryRun",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return completed.stdout


def _make_fake_python_exe(tmp_path: Path) -> Path:
    driver_path = tmp_path / "fake_python_driver.py"
    driver_path.write_text(
        textwrap.dedent(
            """
            import json
            import sys
            from pathlib import Path

            log_path = Path(sys.argv[1])
            payload = {
                "argv": sys.argv[2:],
                "argc": len(sys.argv[2:]),
            }
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with log_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload) + "\\n")
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    if sys.platform.startswith("win"):
        wrapper_path = tmp_path / "fake_python.cmd"
        wrapper_path.write_text(
            f'@echo off\r\n"{sys.executable}" "%~dp0fake_python_driver.py" "%~dp0fake_python_invocations.jsonl" %*\r\n',
            encoding="utf-8",
        )
    else:
        wrapper_path = tmp_path / "fake_python"
        wrapper_path.write_text(
            "#!/bin/sh\n"
            f'"{sys.executable}" "$(dirname "$0")/fake_python_driver.py" "$(dirname "$0")/fake_python_invocations.jsonl" "$@"\n',
            encoding="utf-8",
        )
        wrapper_path.chmod(0o755)
    return wrapper_path


def _make_fake_python_exe_with_ws_candle_partial_failure(tmp_path: Path) -> Path:
    driver_path = tmp_path / "fake_python_driver_ws_partial.py"
    driver_path.write_text(
        textwrap.dedent(
            """
            import json
            import sys
            from pathlib import Path

            argv = sys.argv[2:]
            payload = {"argv": argv, "argc": len(argv)}
            log_path = Path(sys.argv[1])
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with log_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload) + "\\n")

            if argv[:4] == ["-m", "autobot.cli", "collect", "ws-candles"]:
                meta_dir = Path.cwd() / "data" / "collect" / "_meta"
                meta_dir.mkdir(parents=True, exist_ok=True)
                (meta_dir / "ws_candle_collect_report.json").write_text(
                    json.dumps(
                        {
                            "run_id": "partial-ws-candle-run",
                            "rows_written": 12,
                            "failures": [{"reason": "MAX_RECONNECT_PER_MIN_REACHED"}],
                        },
                        ensure_ascii=False,
                    ),
                    encoding="utf-8",
                )
                (meta_dir / "ws_candle_validate_report.json").write_text(
                    json.dumps(
                        {
                            "checked_files": 1,
                            "ok_files": 1,
                            "warn_files": 0,
                            "fail_files": 0,
                            "status": "PASS",
                        },
                        ensure_ascii=False,
                    ),
                    encoding="utf-8",
                )
                sys.exit(2)

            sys.exit(0)
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    if sys.platform.startswith("win"):
        wrapper_path = tmp_path / "fake_python_ws_partial.cmd"
        wrapper_path.write_text(
            f'@echo off\r\n"{sys.executable}" "%~dp0fake_python_driver_ws_partial.py" "%~dp0fake_python_invocations_ws_partial.jsonl" %*\r\n',
            encoding="utf-8",
        )
    else:
        wrapper_path = tmp_path / "fake_python_ws_partial"
        wrapper_path.write_text(
            "#!/bin/sh\n"
            f'"{sys.executable}" "$(dirname "$0")/fake_python_driver_ws_partial.py" "$(dirname "$0")/fake_python_invocations_ws_partial.jsonl" "$@"\n',
            encoding="utf-8",
        )
        wrapper_path.chmod(0o755)
    return wrapper_path


def test_t23_2_dashboard_installer_dry_run_keeps_protected_unit_contract() -> None:
    stdout = _run_script_dry_run("install_server_dashboard_service.ps1")

    assert "[dashboard-install][dry-run] unit=autobot-dashboard.service" in stdout
    assert "autobot.dashboard_server" in stdout
    assert "--project-root" in stdout
    assert "--host" in stdout
    assert "--port" in stdout
    assert "ExecStart=/bin/bash -lc " in stdout


def test_t23_2_daily_acceptance_installer_dry_run_keeps_wrapper_and_runtime_units() -> None:
    stdout = _run_script_dry_run("install_server_daily_acceptance_service.ps1")

    assert "[daily-accept-install][dry-run] service=autobot-daily-v4-accept.service" in stdout
    assert "[daily-accept-install][dry-run] timer=autobot-daily-v4-accept.timer" in stdout
    assert "daily_champion_challenger_v5_for_server.ps1" in stdout
    assert "v5_governed_candidate_acceptance.ps1" in stdout
    assert "autobot-paper-v5.service" in stdout


def test_t23_2_rank_shadow_installer_dry_run_keeps_protected_units() -> None:
    stdout = _run_script_dry_run("install_server_rank_shadow_service.ps1")

    assert "[rank-shadow-install][dry-run] service=autobot-v4-rank-shadow.service" in stdout
    assert "[rank-shadow-install][dry-run] timer=autobot-v4-rank-shadow.timer" in stdout
    assert "daily_rank_shadow_cycle_for_server.ps1" in stdout
    assert "v4_rank_shadow_candidate_acceptance.ps1" in stdout
    assert "autobot-v4-challenger-spawn.service" in stdout
    assert "autobot-v4-challenger-promote.service" in stdout
    assert "autobot-v4-challenger-spawn.service,autobot-v4-challenger-promote.service" in stdout
    assert "-AcceptanceArgs" in stdout
    assert "-SkipPaperSoak" in stdout


def test_t23_2_live_execution_policy_installer_dry_run_keeps_timer_contract() -> None:
    stdout = _run_script_dry_run("install_server_live_execution_policy_service.ps1")

    assert "[live-exec-install][dry-run] service=autobot-live-execution-policy.service" in stdout
    assert "[live-exec-install][dry-run] timer=autobot-live-execution-policy.timer" in stdout
    assert "refresh_live_execution_policy.ps1" in stdout
    assert "data/state/live_state.db,data/state/live_canary/live_state.db,data/state/live_candidate/live_state.db" in stdout


def test_t23_2_data_platform_refresh_installer_dry_run_keeps_new_dataset_contracts() -> None:
    stdout = _run_script_dry_run("install_server_data_platform_refresh_service.ps1")

    assert "[data-platform-install][dry-run] service=autobot-data-platform-refresh.service" in stdout
    assert "[data-platform-install][dry-run] timer=autobot-data-platform-refresh.timer" in stdout
    assert "refresh_data_platform_layers.ps1" in stdout
    assert "mode=runtime_rich" in stdout
    assert "skip_publish_ready_snapshot=True" in stdout
    assert "candles_second_v1" in stdout
    assert "ws_candle_v1" in stdout
    assert "lob30_v1" in stdout
    assert "sequence_v1" in stdout
    assert "private_execution_v1" in stdout
    assert "tensor_recent_dates=2" in stdout


def test_t23_2_data_platform_refresh_wrapper_dry_run_emits_all_step_commands() -> None:
    stdout = _run_script_dry_run("refresh_data_platform_layers.ps1")

    assert "[data-platform-refresh] step=plan_candles_second" in stdout
    assert "[data-platform-refresh] step=collect_candles_second" in stdout
    assert "[data-platform-refresh] step=plan_ws_candles" in stdout
    assert "[data-platform-refresh] step=collect_ws_candles" in stdout
    assert "[data-platform-refresh] step=plan_lob30" in stdout
    assert "[data-platform-refresh] step=collect_lob30" in stdout
    assert "[data-platform-refresh] step=aggregate_micro_current_window" in stdout
    assert "[data-platform-refresh] step=validate_micro_current_window" in stdout
    assert "[data-platform-refresh] step=collect_sequence_tensors" in stdout
    assert "[data-platform-refresh] step=collect_sequence_tensors_prev1" in stdout
    assert "[data-platform-refresh] step=refresh_private_execution_label_store" in stdout
    assert "[data-platform-refresh] step=refresh_data_contract_registry" in stdout
    assert "candles_second_v1" in stdout
    assert "ws_candle_v1" in stdout
    assert "lob30_v1" in stdout
    assert "sequence_v1" in stdout
    assert "private_execution_v1" in stdout


def test_t23_2_data_platform_refresh_wrapper_runtime_rich_dry_run_excludes_training_steps() -> None:
    stdout = _run_script_dry_run(
        "refresh_data_platform_layers.ps1",
        "-Mode",
        "runtime_rich",
        "-SkipPublishReadySnapshot",
    )

    assert "mode=runtime_rich" in stdout
    assert "[data-platform-refresh] step=plan_ws_candles" in stdout
    assert "[data-platform-refresh] step=collect_ws_candles" in stdout
    assert "[data-platform-refresh] step=refresh_private_execution_label_store" in stdout
    assert "[data-platform-refresh] step=aggregate_micro_current_window" not in stdout
    assert "[data-platform-refresh] step=collect_sequence_tensors" not in stdout
    assert "[data-platform-refresh] step=publish_data_platform_snapshot" not in stdout


def test_t23_2_v5_lane_migration_dry_run_surfaces_v5_split_contract() -> None:
    stdout = _run_script_dry_run("migrate_v5_candidate_lane_contract.ps1")

    assert "[v5-lane-migrate][dry-run] stop_disable=autobot-v4-challenger-spawn.timer" in stdout
    assert "[daily-split-install][dry-run] promote_service=autobot-v5-challenger-promote.service" in stdout
    assert "[daily-split-install][dry-run] spawn_service=autobot-v5-challenger-spawn.service" in stdout
    assert "model_family=train_v5_fusion" in stdout


def test_t23_2_v5_lane_migration_dry_run_surfaces_state_db_copy_when_legacy_db_exists(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    legacy_db = project_root / "data" / "state" / "live_candidate" / "live_state.db"
    legacy_db.parent.mkdir(parents=True, exist_ok=True)
    legacy_db.write_text("sqlite-placeholder", encoding="utf-8")

    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(REPO_ROOT / "scripts" / "migrate_v5_candidate_lane_contract.ps1"),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            "python",
            "-NoInstall",
            "-DryRun",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )

    stdout = completed.stdout
    assert "[v5-lane-migrate][dry-run] copy_state_db" in stdout
    assert str(legacy_db) in stdout
    assert str(project_root / "data" / "state" / "live_canary" / "live_state.db") in stdout


def test_t23_2_data_platform_refresh_wrapper_training_critical_dry_run_excludes_ws_candles() -> None:
    stdout = _run_script_dry_run(
        "refresh_data_platform_layers.ps1",
        "-Mode",
        "training_critical",
        "-SkipPublishReadySnapshot",
    )

    assert "mode=training_critical" in stdout
    assert "[data-platform-refresh] step=plan_candles_second" in stdout
    assert "[data-platform-refresh] step=aggregate_micro_current_window" in stdout
    assert "[data-platform-refresh] step=collect_sequence_tensors" in stdout
    assert "--skip-existing-ready' 'true" in stdout or '--skip-existing-ready" "true' in stdout or "--skip-existing-ready true" in stdout
    assert "[data-platform-refresh] step=plan_ws_candles" not in stdout
    assert "[data-platform-refresh] step=collect_ws_candles" not in stdout
    assert "[data-platform-refresh] step=publish_data_platform_snapshot" not in stdout


def test_t23_2_data_platform_refresh_wrapper_training_critical_dry_run_writes_range_summary(tmp_path: Path) -> None:
    summary_path = tmp_path / "training_critical_summary.json"
    stdout = _run_script_dry_run(
        "refresh_data_platform_layers.ps1",
        "-Mode",
        "training_critical",
        "-SkipPublishReadySnapshot",
        "-TensorStartDate",
        "2026-03-04",
        "-TensorEndDate",
        "2026-03-08",
        "-MicroStartDate",
        "2026-03-04",
        "-MicroEndDate",
        "2026-03-08",
        "-SummaryPath",
        str(summary_path),
    )

    assert str(summary_path) in stdout
    payload = json.loads(summary_path.read_text(encoding="utf-8-sig"))
    assert payload["mode"] == "training_critical"
    assert payload["start_date"] == "2026-03-04"
    assert payload["end_date"] == "2026-03-08"
    assert payload["window_source"] == "explicit_date_range"
    assert payload["tensor_dates"][0] == "2026-03-08"
    assert payload["tensor_dates"][-1] == "2026-03-04"
    assert payload["micro_dates"][0] == "2026-03-08"
    assert payload["micro_dates"][-1] == "2026-03-04"
    assert payload["refresh_argument_mode"] == "explicit_date_range"
    assert payload["top_n"] == 50
    assert payload["tensor_max_markets_effective"] == 50


def test_t23_2_data_platform_refresh_wrapper_applies_explicit_tensor_markets_to_all_tensor_steps() -> None:
    stdout = _run_script_dry_run(
        "refresh_data_platform_layers.ps1",
        "-Mode",
        "training_critical",
        "-SkipPublishReadySnapshot",
        "-TensorStartDate",
        "2026-03-04",
        "-TensorEndDate",
        "2026-03-06",
        "-TensorMarkets",
        "KRW-BTC,KRW-ETH",
    )

    assert stdout.count("--markets") >= 3
    assert "collect_sequence_tensors_prev1" in stdout
    assert "collect_sequence_tensors_prev2" in stdout


def test_t23_2_data_platform_refresh_wrapper_executes_python_steps_with_full_args(tmp_path: Path) -> None:
    fake_python = _make_fake_python_exe(tmp_path)
    project_root = tmp_path / "project"
    project_root.mkdir()
    summary_path = project_root / "data" / "collect" / "_meta" / "refresh_summary.json"
    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(REPO_ROOT / "scripts" / "refresh_data_platform_layers.ps1"),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            str(fake_python),
            "-Mode",
            "training_critical",
            "-TensorStartDate",
            "2026-03-04",
            "-TensorEndDate",
            "2026-03-05",
            "-MicroStartDate",
            "2026-03-04",
            "-MicroEndDate",
            "2026-03-05",
            "-SummaryPath",
            str(summary_path),
            "-PublishLockFile",
            "",
            "-SkipPublishReadySnapshot",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )

    assert summary_path.exists(), completed.stdout + "\n" + completed.stderr
    log_path = tmp_path / "fake_python_invocations.jsonl"
    lines = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert lines, completed.stdout + "\n" + completed.stderr
    assert lines[0]["argc"] > 0
    assert lines[0]["argv"][:4] == ["-m", "autobot.cli", "collect", "plan-candles"]


def test_t23_2_data_platform_refresh_wrapper_handles_single_day_explicit_range(tmp_path: Path) -> None:
    fake_python = _make_fake_python_exe(tmp_path)
    project_root = tmp_path / "project"
    project_root.mkdir()
    summary_path = project_root / "data" / "collect" / "_meta" / "refresh_summary_single_day.json"

    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(REPO_ROOT / "scripts" / "refresh_data_platform_layers.ps1"),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            str(fake_python),
            "-Mode",
            "training_critical",
            "-TensorStartDate",
            "2026-03-04",
            "-TensorEndDate",
            "2026-03-04",
            "-MicroStartDate",
            "2026-03-04",
            "-MicroEndDate",
            "2026-03-04",
            "-SummaryPath",
            str(summary_path),
            "-PublishLockFile",
            "",
            "-SkipPublishReadySnapshot",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    payload = json.loads(summary_path.read_text(encoding="utf-8-sig"))
    assert payload["tensor_dates"] == ["2026-03-04"]
    assert payload["micro_dates"] == ["2026-03-04"]


def test_t23_2_data_platform_refresh_wrapper_tolerates_partial_ws_candle_collect_when_validate_passes(tmp_path: Path) -> None:
    fake_python = _make_fake_python_exe_with_ws_candle_partial_failure(tmp_path)
    project_root = tmp_path / "project"
    project_root.mkdir()
    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(REPO_ROOT / "scripts" / "refresh_data_platform_layers.ps1"),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            str(fake_python),
            "-Mode",
            "runtime_rich",
            "-PublishLockFile",
            "",
            "-SkipPublishReadySnapshot",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    assert "tolerating partial ws-candle collect" in (completed.stdout + completed.stderr)


def test_t23_2_train_snapshot_close_installer_dry_run_keeps_v5_timer_contract() -> None:
    stdout = _run_script_dry_run("install_server_train_snapshot_close_service.ps1")

    assert "[train-snapshot-close-install][dry-run] service=autobot-v5-train-snapshot-close.service" in stdout
    assert "[train-snapshot-close-install][dry-run] timer=autobot-v5-train-snapshot-close.timer" in stdout
    assert "close_v5_train_ready_snapshot.ps1" in stdout
    assert "OnCalendar=*-*-* 00:05:00" in stdout


def test_t23_2_foundation_ingestion_installer_dry_run_keeps_only_first_tier_collectors() -> None:
    stdout = _run_script_dry_run("install_server_foundation_ingestion_services.ps1")

    assert "[candles-api-install][dry-run] service=autobot-candles-api-refresh.service" in stdout
    assert "[raw-ticks-install][dry-run] service=autobot-raw-ticks-daily.service" in stdout
    assert "[raw-ticks-backfill-install][dry-run] service=autobot-raw-ticks-backfill.service" in stdout
    assert "[raw-trade-v1-install][dry-run] service=autobot-raw-trade-v1.service" in stdout
    assert "autobot-v5-train-snapshot-close.timer" not in stdout
    assert "close_v5_train_ready_snapshot.ps1" not in stdout


def test_t23_2_source_plane_installer_dry_run_keeps_only_source_plane_units() -> None:
    stdout = _run_script_dry_run("install_server_source_plane_services.ps1")

    assert "[ws-public-install][dry-run] unit=autobot-ws-public.service" in stdout
    assert "[private-ws-install][dry-run] unit=autobot-private-ws-archive.service" in stdout
    assert "[candles-api-install][dry-run] service=autobot-candles-api-refresh.service" in stdout
    assert "[raw-ticks-install][dry-run] service=autobot-raw-ticks-daily.service" in stdout
    assert "[raw-ticks-backfill-install][dry-run] service=autobot-raw-ticks-backfill.service" in stdout
    assert "[raw-trade-v1-install][dry-run] service=autobot-raw-trade-v1.service" in stdout
    assert "autobot-data-platform-refresh.timer" not in stdout
    assert "refresh_data_platform_layers.ps1" not in stdout
    assert "autobot-v5-train-snapshot-close.timer" not in stdout


def test_t23_2_raw_trade_v1_installer_dry_run_keeps_source_plane_contract() -> None:
    stdout = _run_script_dry_run("install_server_raw_trade_v1_service.ps1")

    assert "[raw-trade-v1-install][dry-run] service=autobot-raw-trade-v1.service" in stdout
    assert "[raw-trade-v1-install][dry-run] refresh_script=" in stdout
    assert "run_raw_trade_v1_refresh.ps1" in stdout
    assert "OnUnitActiveSec=20min" in stdout


def test_t23_2_raw_trade_v1_refresh_wrapper_dry_run_emits_rolling_window_build() -> None:
    stdout = _run_script_dry_run("run_raw_trade_v1_refresh.ps1")

    assert "[raw-trade-v1-refresh] step=build_raw_trade_v1" in stdout
    assert "autobot.cli" in stdout
    assert "collect" in stdout
    assert "raw-trade-v1" in stdout
    assert "--prefer-source-order" in stdout


def test_t23_2_raw_ticks_backfill_wrapper_dry_run_uses_recent_two_days_window() -> None:
    stdout = _run_script_dry_run("run_raw_ticks_backfill_sweep.ps1")

    assert "--days-ago' '1,2" in stdout or '--days-ago" "1,2' in stdout or "--days-ago 1,2" in stdout
    assert "OnCalendar=*-*-* 22:00:00" in _run_script_dry_run("install_server_raw_ticks_backfill_service.ps1")


def test_t23_2_candles_api_refresh_wrapper_omits_global_request_cap_when_zero() -> None:
    stdout = _run_script_dry_run("run_candles_api_refresh.ps1", "-MaxRequests", "0")

    assert "--max-requests" not in stdout


def test_t23_2_candles_api_refresh_installer_omits_maxrequests_when_zero() -> None:
    stdout = _run_script_dry_run("install_server_candles_api_refresh_service.ps1", "-MaxRequests", "0")

    assert "-MaxRequests" not in stdout


def test_t23_2_train_snapshot_close_wrapper_dry_run_emits_training_close_contract() -> None:
    stdout = _run_script_dry_run("close_v5_train_ready_snapshot.ps1")

    assert "[train-snapshot-close] command=" in stdout
    assert "refresh_data_platform_layers.ps1" in stdout
    assert "-Mode' 'training_critical" in stdout or '-Mode" "training_critical' in stdout or "-Mode training_critical" in stdout
    assert "-TensorStartDate" in stdout
    assert "-TensorEndDate" in stdout
    assert "-MicroStartDate" in stdout
    assert "-MicroEndDate" in stdout
    assert "refresh_current_features_v4_contract_artifacts.ps1" in stdout
    assert "-StartDate" in stdout
    assert "-EndDate" in stdout
    assert "-TopN" in stdout
    assert "-UseTopNUniverse" in stdout
    assert "-SkipParity" not in stdout
    assert "-SkipRegistryRefresh" not in stdout
    assert "autobot.ops.data_platform_snapshot" in stdout
    assert "train_snapshot_close_latest.json" in stdout


def test_t23_2_recover_v5_fusion_train_window_wrapper_dry_run_emits_recovery_chain() -> None:
    stdout = _run_script_dry_run(
        "recover_v5_fusion_train_window.ps1",
        "-BatchDate",
        "2026-03-31",
        "-TrainingCriticalStartDate",
        "2026-03-04",
        "-TrainingCriticalEndDate",
        "2026-03-31",
    )

    assert "close_v5_train_ready_snapshot.ps1" in stdout
    assert "v5_governed_candidate_acceptance.ps1" in stdout
    assert "daily_champion_challenger_v5_for_server.ps1" in stdout
    assert "v5_fusion_train_window_recovery" in stdout


def test_t23_2_feature_contract_refresh_wrapper_dry_run_emits_contract_steps() -> None:
    stdout = _run_script_dry_run(
        "refresh_current_features_v4_contract_artifacts.ps1",
        "-StartDate",
        "2026-03-04",
        "-EndDate",
        "2026-03-18",
        "-Quote",
        "KRW",
        "-TopN",
        "50",
        "-LabelSet",
        "v3",
        "-Markets",
        "KRW-BTC,KRW-ETH",
    )

    assert "[feature-contract-refresh] step=micro_aggregate_contract_window" in stdout
    assert "[feature-contract-refresh] step=micro_validate_contract_window" in stdout
    assert "[feature-contract-refresh] step=features_v4_build_contract_window" in stdout
    assert "[feature-contract-refresh] step=features_v4_validate_contract_window" in stdout
    assert "[feature-contract-refresh] step=features_v4_live_parity_contract_window" in stdout
    assert "[feature-contract-refresh] step=refresh_data_contract_registry" in stdout
    assert "autobot.cli' 'micro' 'aggregate" in stdout or 'autobot.cli" "micro" "aggregate' in stdout or "autobot.cli micro aggregate" in stdout
    assert "autobot.cli' 'features' 'build" in stdout or 'autobot.cli" "features" "build' in stdout or "autobot.cli features build" in stdout
    assert "autobot.ops.live_feature_parity_report" in stdout
    assert "--top-n' '20" in stdout or '--top-n" "20' in stdout or "--top-n 20" in stdout


def test_t23_2_daily_acceptance_installer_serializes_nested_array_args_safely() -> None:
    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            (
                "& { "
                f"& '{REPO_ROOT / 'scripts' / 'install_server_daily_acceptance_service.ps1'}' "
                f"-ProjectRoot '{REPO_ROOT}' "
                "-PythonExe 'python' "
                "-PromotionTargetUnits @('autobot-live-alpha.service','autobot-live-alpha-candidate.service') "
                "-BlockOnActiveUnits @('autobot-v4-challenger-spawn.service','autobot-v4-challenger-promote.service') "
                "-AcceptanceArgs @('-SkipPaperSoak','-SkipPromote') "
                "-DryRun "
                "}"
            ),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    stdout = completed.stdout

    assert "autobot-live-alpha.service,autobot-live-alpha-candidate.service" in stdout
    assert "autobot-v4-challenger-spawn.service,autobot-v4-challenger-promote.service" in stdout
    assert "-SkipPaperSoak,-SkipPromote" in stdout


def test_t23_2_daily_orchestrator_param_surface_keeps_protected_names() -> None:
    source = (REPO_ROOT / "scripts" / "daily_champion_challenger_v4_for_server.ps1").read_text(encoding="utf-8")

    for snippet in (
        '[string]$AcceptanceScript = ""',
        '[string]$RuntimeInstallScript = ""',
        '[string]$FeatureContractRefreshScript = ""',
        '[string]$BatchDate = ""',
        '[string]$ChampionUnitName = "autobot-paper-v4.service"',
        '[string]$ChallengerUnitName = "autobot-paper-v4-challenger.service"',
        '[string[]]$PromotionTargetUnits = @()',
        '[string[]]$CandidateTargetUnits = @()',
        '[string[]]$BlockOnActiveUnits = @()',
        '[string[]]$AcceptanceArgs = @()',
        '[ValidateSet("combined", "promote_only", "spawn_only")]',
        '[string]$Mode = "combined"',
        '[switch]$SkipDailyPipeline',
        '[switch]$SkipFeatureContractRefresh',
        '[switch]$SkipReportRefresh',
        '[switch]$DryRun',
    ):
        assert snippet in source


def test_t23_2_acceptance_scripts_keep_frozen_pointer_aliases_and_runtime_units() -> None:
    protected_scripts = (
        "v4_promotable_candidate_acceptance.ps1",
        "v4_scout_candidate_acceptance.ps1",
        "v4_rank_shadow_candidate_acceptance.ps1",
        "v4_rank_governed_candidate_acceptance.ps1",
    )

    for script_name in protected_scripts:
        source = (REPO_ROOT / "scripts" / script_name).read_text(encoding="utf-8")
        assert '. (Join-Path $PSScriptRoot "v4_acceptance_contract.ps1")' in source
        assert '$knownRuntimeUnits = @("autobot-paper-v4.service", "autobot-live-alpha.service")' in source
        assert '$trainDataQualityFloorDate = Get-V4TrainDataQualityFloorDate' in source
        assert '-CandidateModelRef "latest_candidate_v4"' in source
        assert '-ChampionModelRef "champion_v4"' in source
        assert '-TrainDataQualityFloorDate $trainDataQualityFloorDate' in source
        assert '-KnownRuntimeUnits $knownRuntimeUnits' in source
        assert '-TrainStartFloorDate "2026-03-04"' not in source


def test_t23_2_v4_acceptance_contract_keeps_explicit_data_quality_floor() -> None:
    source = (REPO_ROOT / "scripts" / "v4_acceptance_contract.ps1").read_text(encoding="utf-8")

    assert "Get-V4TrainDataQualityFloorDate" in source
    assert '"2026-03-04"' in source


def test_t23_2_governed_acceptance_script_keeps_promotable_fallback() -> None:
    source = (REPO_ROOT / "scripts" / "v4_governed_candidate_acceptance.ps1").read_text(encoding="utf-8")

    assert 'selected_acceptance_script' in source
    assert 'v4_promotable_candidate_acceptance.ps1' in source
    assert re.search(r'& \$selectedScriptPath @args', source) is not None


def test_t23_2_runtime_installer_accepts_serialized_paper_cli_args() -> None:
    stdout = _run_script_dry_run(
        "install_server_runtime_services.ps1",
        "-PaperCliArgs",
        "--model-ref,run-123",
    )

    assert "--model-ref" in stdout
    assert "run-123" in stdout
    assert "bootstrap_champion=False" in stdout


def test_t23_2_runtime_installer_supports_paired_paper_preset() -> None:
    stdout = _run_script_dry_run(
        "install_server_runtime_services.ps1",
        "-PaperPreset",
        "paired_v4",
        "-PaperUnitName",
        "autobot-paper-v4-paired.service",
    )

    assert "autobot.paper.paired_runtime" in stdout
    assert "run-service" in stdout
    assert "ConditionPathExists=" in stdout
    assert "autobot-paper-v4-paired.service" in stdout


def test_t23_2_runtime_installer_keeps_explicit_bootstrap_switch() -> None:
    source = (REPO_ROOT / "scripts" / "install_server_runtime_services.ps1").read_text(encoding="utf-8")

    assert '[switch]$BootstrapChampion' in source
    assert 'install no longer auto-bootstraps' in source


def test_t23_2_v4_candidate_state_helper_is_shared_by_scripts() -> None:
    helper_snippet = '. (Join-Path $PSScriptRoot "v4_candidate_state_helpers.ps1")'

    for script_name in (
        "candidate_acceptance.ps1",
        "adopt_v4_candidate_for_server.ps1",
        "daily_champion_challenger_v4_for_server.ps1",
    ):
        source = (REPO_ROOT / "scripts" / script_name).read_text(encoding="utf-8")
        assert helper_snippet in source
