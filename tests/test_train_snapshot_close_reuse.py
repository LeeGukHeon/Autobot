from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    raise RuntimeError("PowerShell executable is required")


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_fail_if_called_script(path: Path, marker_name: str) -> None:
    path.write_text(
        "\n".join(
            [
                "param(",
                "    [string]$ProjectRoot = '',",
                "    [string]$PythonExe = ''",
                ")",
                "$markerPath = Join-Path $ProjectRoot 'logs/test_markers/" + marker_name + "'",
                "New-Item -ItemType Directory -Force -Path (Split-Path -Parent $markerPath) | Out-Null",
                "\"called\" | Set-Content -Path $markerPath -Encoding UTF8",
                "Write-Error 'unexpected invocation'",
                "exit 99",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_close_v5_train_snapshot_reuses_existing_matching_summary(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    snapshot_id = "snapshot-reuse-001"
    snapshot_root = project_root / "data" / "snapshots" / "data_platform" / snapshot_id
    snapshot_summary_path = snapshot_root / "_meta" / "summary.json"
    summary_path = project_root / "data" / "collect" / "_meta" / "train_snapshot_close_latest.json"
    pointer_path = project_root / "data" / "_meta" / "data_platform_ready_snapshot.json"

    snapshot_root.mkdir(parents=True, exist_ok=True)
    _write_json(snapshot_summary_path, {"snapshot_id": snapshot_id})
    _write_json(pointer_path, {"snapshot_id": snapshot_id})
    _write_json(
        summary_path,
        {
            "policy": "v5_train_snapshot_close_v1",
            "generated_at_utc": "2026-04-08T20:00:00Z",
            "batch_date": "2026-04-08",
            "tf": "1m",
            "training_critical_start_date": "2026-04-06",
            "training_critical_end_date": "2026-04-08",
            "train_window": {"start": "2026-04-06", "end": "2026-04-07"},
            "certification_window": {"start": "2026-04-08", "end": "2026-04-08"},
            "coverage_window": {"start": "2026-04-06", "end": "2026-04-08"},
            "feature_contract_refresh": {"top_n": 7, "tf": "1m"},
            "snapshot_id": snapshot_id,
            "snapshot_root": str(snapshot_root),
            "snapshot_summary_path": str(snapshot_summary_path),
            "overall_pass": True,
            "failure_reasons": [],
        },
    )

    refresh_script = tmp_path / "fake_refresh.ps1"
    feature_script = tmp_path / "fake_feature_refresh.ps1"
    _write_fail_if_called_script(refresh_script, "refresh_called.txt")
    _write_fail_if_called_script(feature_script, "feature_called.txt")

    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(REPO_ROOT / "scripts" / "close_v5_train_ready_snapshot.ps1"),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            sys.executable,
            "-BatchDate",
            "2026-04-08",
            "-TrainLookbackDays",
            "2",
            "-BacktestLookbackDays",
            "1",
            "-FeatureTopN",
            "7",
            "-DataPlatformRefreshScript",
            str(refresh_script),
            "-FeatureContractRefreshScript",
            str(feature_script),
            "-SkipDeadline",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    assert "reuse_snapshot_id=snapshot-reuse-001" in completed.stdout
    assert not (project_root / "logs" / "test_markers" / "refresh_called.txt").exists()
    assert not (project_root / "logs" / "test_markers" / "feature_called.txt").exists()

    reused_summary = json.loads(summary_path.read_text(encoding="utf-8-sig"))
    assert reused_summary["snapshot_id"] == snapshot_id
    assert reused_summary["reused_existing_summary"] is True
    assert reused_summary["reused_source_generated_at_utc"] == "2026-04-08T20:00:00Z"
    assert reused_summary["reuse_reason"] == "MATCHING_BATCH_DATE_SNAPSHOT_CLOSE"


def test_close_v5_train_snapshot_does_not_reuse_mismatched_tf_summary(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    snapshot_id = "snapshot-reuse-001"
    snapshot_root = project_root / "data" / "snapshots" / "data_platform" / snapshot_id
    snapshot_summary_path = snapshot_root / "_meta" / "summary.json"
    summary_path = project_root / "data" / "collect" / "_meta" / "train_snapshot_close_latest.json"
    pointer_path = project_root / "data" / "_meta" / "data_platform_ready_snapshot.json"

    snapshot_root.mkdir(parents=True, exist_ok=True)
    _write_json(snapshot_summary_path, {"snapshot_id": snapshot_id})
    _write_json(pointer_path, {"snapshot_id": snapshot_id})
    _write_json(
        summary_path,
        {
            "policy": "v5_train_snapshot_close_v1",
            "generated_at_utc": "2026-04-08T20:00:00Z",
            "batch_date": "2026-04-08",
            "tf": "5m",
            "training_critical_start_date": "2026-04-06",
            "training_critical_end_date": "2026-04-08",
            "train_window": {"start": "2026-04-06", "end": "2026-04-07"},
            "certification_window": {"start": "2026-04-08", "end": "2026-04-08"},
            "coverage_window": {"start": "2026-04-06", "end": "2026-04-08"},
            "feature_contract_refresh": {"top_n": 7, "tf": "5m"},
            "snapshot_id": snapshot_id,
            "snapshot_root": str(snapshot_root),
            "snapshot_summary_path": str(snapshot_summary_path),
            "overall_pass": True,
            "failure_reasons": [],
        },
    )

    refresh_script = tmp_path / "refresh_ok.ps1"
    feature_script = tmp_path / "feature_ok.ps1"
    refresh_summary_path = project_root / "data" / "collect" / "_meta" / "train_snapshot_training_critical_refresh_latest.json"
    feature_summary_path = project_root / "data" / "features" / "features_v4" / "_meta" / "nightly_train_snapshot_contract_refresh.json"
    feature_build_report_path = project_root / "data" / "features" / "features_v4" / "_meta" / "build_report.json"
    refresh_script.write_text(
        "\n".join(
            [
                "param(",
                "    [string]$ProjectRoot = '',",
                "    [string]$SummaryPath = '',",
                "    [string]$TensorStartDate = '',",
                "    [string]$TensorEndDate = ''",
                ")",
                "$summary = @{",
                "    generated_at_utc = '2026-04-08T20:05:00Z';",
                "    start_date = $TensorStartDate;",
                "    end_date = $TensorEndDate;",
                "    steps = @();",
                "    top_n = 7",
                "}",
                "$summary | ConvertTo-Json -Depth 6 | Set-Content -Path $SummaryPath -Encoding UTF8",
                "exit 0",
            ]
        ) + "\n",
        encoding="utf-8",
    )
    feature_script.write_text(
        "\n".join(
            [
                "param(",
                "    [string]$ProjectRoot = '',",
                "    [string]$SummaryPath = '',",
                "    [string]$Tf = '',",
                "    [string]$StartDate = '',",
                "    [string]$EndDate = '',",
                "    [int]$TopN = 0",
                ")",
                "$metaRoot = Join-Path $ProjectRoot 'data/features/features_v4/_meta'",
                "New-Item -ItemType Directory -Force -Path $metaRoot | Out-Null",
                "$summary = @{",
                "    tf = $Tf;",
                "    start = $StartDate;",
                "    end = $EndDate;",
                "    top_n = $TopN;",
                "    universe_mode = 'top_n_dynamic'",
                "}",
                "$summary | ConvertTo-Json -Depth 6 | Set-Content -Path $SummaryPath -Encoding UTF8",
                "$build = @{ tf = $Tf; effective_start = $StartDate; effective_end = $EndDate }",
                "$build | ConvertTo-Json -Depth 6 | Set-Content -Path (Join-Path $metaRoot 'build_report.json') -Encoding UTF8",
                "exit 0",
            ]
        ) + "\n",
        encoding="utf-8",
    )
    fake_python = tmp_path / "fake_python.cmd"
    fake_python.write_text(
        "\n".join(
            [
                "@echo off",
                "setlocal EnableDelayedExpansion",
                "if \"%~1\"==\"-m\" if \"%~2\"==\"autobot.ops.data_platform_snapshot\" if \"%~3\"==\"publish\" (",
                f"  echo {{\"snapshot_id\":\"{snapshot_id}\",\"snapshot_root\":\"{str(snapshot_root).replace('\\', '\\\\')}\",\"summary_path\":\"{str(snapshot_summary_path).replace('\\', '\\\\')}\"}}",
                "  exit /b 0",
                ")",
                "exit /b 9",
            ]
        ) + "\n",
        encoding="utf-8",
    )

    candles_summary = project_root / "data" / "collect" / "_meta" / "candles_api_refresh_latest.json"
    ticks_summary = project_root / "data" / "raw_ticks" / "upbit" / "_meta" / "ticks_daily_latest.json"
    _write_json(candles_summary, {"generated_at_utc": "2099-01-01T00:00:00Z", "steps": []})
    _write_json(ticks_summary, {"generated_at_utc": "2099-01-01T00:00:00Z", "batch_date": "2026-04-08", "steps": []})

    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(REPO_ROOT / "scripts" / "close_v5_train_ready_snapshot.ps1"),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            str(fake_python),
            "-BatchDate",
            "2026-04-08",
            "-Tf",
            "1m",
            "-TrainLookbackDays",
            "2",
            "-BacktestLookbackDays",
            "1",
            "-FeatureTopN",
            "7",
            "-DataPlatformRefreshScript",
            str(refresh_script),
            "-FeatureContractRefreshScript",
            str(feature_script),
            "-SkipDeadline",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    assert "reuse_snapshot_id=snapshot-reuse-001" not in completed.stdout
    latest_summary = json.loads(summary_path.read_text(encoding="utf-8-sig"))
    assert latest_summary.get("reused_existing_summary") is not True
    assert latest_summary["tf"] == "1m"
    assert latest_summary["feature_contract_refresh"]["tf"] == "1m"
