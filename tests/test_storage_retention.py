from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock

from autobot.ops.storage_retention import (
    EmergencyRetentionPolicy,
    StorageRetentionPolicy,
    _systemd_unit_active,
    run_storage_retention,
)


def _write_file(path: Path, content: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _set_mtime(path: Path, *, age_days: int) -> None:
    ts = (datetime.now(timezone.utc) - timedelta(days=age_days)).timestamp()
    os.utime(path, (ts, ts))


def _make_run_dir(root: Path, name: str, *, age_days: int) -> Path:
    run_dir = root / name
    _write_file(run_dir / "summary.json", "{}")
    _set_mtime(run_dir / "summary.json", age_days=age_days)
    _set_mtime(run_dir, age_days=age_days)
    return run_dir


def test_run_storage_retention_prunes_old_operational_artifacts(tmp_path: Path) -> None:
    project_root = tmp_path / "repo"
    today = datetime.now(timezone.utc).date()
    old_ws_date = (today - timedelta(days=20)).isoformat()
    keep_ws_date = (today - timedelta(days=1)).isoformat()
    old_ticks_date = (today - timedelta(days=40)).isoformat()
    keep_ticks_date = (today - timedelta(days=2)).isoformat()
    old_micro_date = (today - timedelta(days=120)).isoformat()
    keep_micro_date = (today - timedelta(days=5)).isoformat()

    _write_file(project_root / "data" / "raw_ws" / "upbit" / "public" / "trade" / f"date={old_ws_date}" / "hour=00" / "part.zst")
    _write_file(project_root / "data" / "raw_ws" / "upbit" / "public" / "trade" / f"date={keep_ws_date}" / "hour=00" / "part.zst")
    _write_file(project_root / "data" / "raw_ticks" / "upbit" / "trades" / "market=KRW-BTC" / f"date={old_ticks_date}" / "part.parquet")
    _write_file(project_root / "data" / "raw_ticks" / "upbit" / "trades" / "market=KRW-BTC" / f"date={keep_ticks_date}" / "part.parquet")
    _write_file(project_root / "data" / "parquet" / "micro_v1" / "tf=5m" / "market=KRW-BTC" / f"date={old_micro_date}" / "part.parquet")
    _write_file(project_root / "data" / "parquet" / "micro_v1" / "tf=5m" / "market=KRW-BTC" / f"date={keep_micro_date}" / "part.parquet")

    _make_run_dir(project_root / "data" / "paper" / "runs", "paper-old", age_days=45)
    keep_paper = _make_run_dir(project_root / "data" / "paper" / "runs", "paper-keep", age_days=1)
    _make_run_dir(project_root / "data" / "backtest" / "runs", "backtest-old", age_days=10)
    keep_backtest = _make_run_dir(project_root / "data" / "backtest" / "runs", "backtest-keep", age_days=2)
    _make_run_dir(project_root / "logs" / "train_v4_execution_backtest" / "runs", "backtest-old", age_days=4)
    keep_exec = _make_run_dir(project_root / "logs" / "train_v4_execution_backtest" / "runs", "backtest-keep", age_days=1)

    family_root = project_root / "models" / "registry" / "train_v4_crypto_cs"
    protected_run = _make_run_dir(family_root, "20260201T000000Z-s42-protected", age_days=120)
    removable_run = _make_run_dir(family_root, "20260201T000100Z-s42-old", age_days=120)
    keep_recent_run = _make_run_dir(family_root, "20260308T000100Z-s42-new", age_days=1)
    (family_root / "champion.json").write_text(json.dumps({"run_id": protected_run.name}), encoding="utf-8")
    (family_root / "latest_candidate.json").write_text(json.dumps({"run_id": keep_recent_run.name}), encoding="utf-8")
    (family_root / "latest.json").write_text(json.dumps({"run_id": keep_recent_run.name}), encoding="utf-8")
    state_path = project_root / "logs" / "model_v4_challenger" / "current_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps({"candidate_run_id": keep_recent_run.name}), encoding="utf-8")

    payload = run_storage_retention(
        project_root=project_root,
        policy=StorageRetentionPolicy(
            ws_public_retention_days=14,
            raw_ticks_retention_days=30,
            micro_parquet_retention_days=90,
            candles_api_retention_days=90,
            paper_runs_retention_days=30,
            backtest_runs_retention_days=5,
            execution_backtest_retention_days=2,
            registry_retention_days=30,
            registry_keep_recent_count=1,
        ),
        emergency_policy=EmergencyRetentionPolicy(),
        model_family="train_v4_crypto_cs",
        report_dir=Path("logs/storage_retention"),
        warning_threshold_gb=9999.0,
        force_threshold_gb=10000.0,
        dry_run=False,
        compact_candles_api=False,
    )

    assert not (project_root / "data" / "raw_ws" / "upbit" / "public" / "trade" / f"date={old_ws_date}").exists()
    assert (project_root / "data" / "raw_ws" / "upbit" / "public" / "trade" / f"date={keep_ws_date}").exists()
    assert not (project_root / "data" / "raw_ticks" / "upbit" / "trades" / "market=KRW-BTC" / f"date={old_ticks_date}").exists()
    assert (project_root / "data" / "raw_ticks" / "upbit" / "trades" / "market=KRW-BTC" / f"date={keep_ticks_date}").exists()
    assert not (project_root / "data" / "parquet" / "micro_v1" / "tf=5m" / "market=KRW-BTC" / f"date={old_micro_date}").exists()
    assert (project_root / "data" / "parquet" / "micro_v1" / "tf=5m" / "market=KRW-BTC" / f"date={keep_micro_date}").exists()
    assert not (project_root / "data" / "paper" / "runs" / "paper-old").exists()
    assert keep_paper.exists()
    assert not (project_root / "data" / "backtest" / "runs" / "backtest-old").exists()
    assert keep_backtest.exists()
    assert not (project_root / "logs" / "train_v4_execution_backtest" / "runs" / "backtest-old").exists()
    assert keep_exec.exists()
    assert protected_run.exists()
    assert keep_recent_run.exists()
    assert not removable_run.exists()
    assert payload["status"] == "ok"
    assert (project_root / "logs" / "storage_retention" / "latest.json").exists()


def test_run_storage_retention_dry_run_keeps_files(tmp_path: Path) -> None:
    project_root = tmp_path / "repo"
    old_date = (datetime.now(timezone.utc).date() - timedelta(days=40)).isoformat()
    target = project_root / "data" / "raw_ws" / "upbit" / "public" / "orderbook" / f"date={old_date}" / "hour=00" / "part.zst"
    _write_file(target)

    payload = run_storage_retention(
        project_root=project_root,
        policy=StorageRetentionPolicy(ws_public_retention_days=14),
        emergency_policy=EmergencyRetentionPolicy(),
        model_family="train_v4_crypto_cs",
        report_dir=Path("logs/storage_retention"),
        warning_threshold_gb=9999.0,
        force_threshold_gb=10000.0,
        dry_run=True,
        compact_candles_api=False,
    )

    assert target.exists()
    ws_section = next(section for section in payload["sections"] if section["name"] == "ws_public")
    assert ws_section["removed_count"] == 1
    assert payload["report_path"].endswith(".json")
    assert not (project_root / "logs" / "storage_retention" / "latest.json").exists()


def test_systemd_unit_active_uses_fallback_path(monkeypatch) -> None:
    monkeypatch.setattr("autobot.ops.storage_retention.shutil.which", lambda _name: None)
    monkeypatch.setattr(
        "autobot.ops.storage_retention.Path.exists",
        lambda self: str(self).replace("\\", "/").endswith("/usr/bin/systemctl"),
    )
    run_mock = Mock(return_value=Mock(returncode=0))
    monkeypatch.setattr("autobot.ops.storage_retention.subprocess.run", run_mock)

    assert _systemd_unit_active("autobot-v4-challenger-spawn.service") is True
    run_mock.assert_called_once()
