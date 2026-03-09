from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import subprocess

import pytest

import autobot.live.daemon as daemon_module
from autobot.live.daemon import LiveDaemonSettings, run_live_sync_daemon
from autobot.live.state_store import LiveStateStore


REPO_ROOT = Path(__file__).resolve().parents[1]


class _QuietClient:
    def accounts(self):  # noqa: ANN201
        return []

    def open_orders(self, *, states):  # noqa: ANN201
        _ = states
        return []

    def order(self, *, uuid: str | None = None, identifier: str | None = None):  # noqa: ANN201
        _ = uuid, identifier
        return {}

    def cancel_order(self, *, uuid: str | None = None, identifier: str | None = None):  # noqa: ANN201
        return {"ok": True, "uuid": uuid, "identifier": identifier}


class _PointerFlipClient(_QuietClient):
    def __init__(self, *, pointer_path: Path, next_run_id: str) -> None:
        self._pointer_path = pointer_path
        self._next_run_id = next_run_id
        self._flipped = False

    def accounts(self):  # noqa: ANN201
        if not self._flipped:
            self._pointer_path.write_text(
                json.dumps({"run_id": self._next_run_id}, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            self._flipped = True
        return []


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _ts_ms_to_iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


def _seed_runtime_contract(tmp_path: Path, *, champion_run_id: str, ws_updated_at_ms: int) -> tuple[Path, Path, Path, Path]:
    registry_root = tmp_path / "models" / "registry"
    family_dir = registry_root / "train_v4_crypto_cs"
    (family_dir / champion_run_id).mkdir(parents=True, exist_ok=True)
    _write_json(family_dir / "champion.json", {"run_id": champion_run_id})

    ws_raw_root = tmp_path / "data" / "raw_ws" / "upbit" / "public"
    ws_meta_dir = tmp_path / "data" / "raw_ws" / "upbit" / "_meta"
    _write_json(
        ws_meta_dir / "ws_public_health.json",
        {
            "run_id": "ws-run-1",
            "updated_at_ms": ws_updated_at_ms,
            "connected": True,
            "last_rx_ts_ms": {"trade": ws_updated_at_ms, "orderbook": ws_updated_at_ms},
            "subscribed_markets_count": 20,
        },
    )
    _write_json(
        ws_meta_dir / "ws_collect_report.json",
        {
            "run_id": "ws-collect-1",
            "generated_at": _ts_ms_to_iso(max(ws_updated_at_ms - 50, 0)),
        },
    )
    _write_json(
        ws_meta_dir / "ws_validate_report.json",
        {
            "run_id": "ws-validate-1",
            "generated_at": _ts_ms_to_iso(max(ws_updated_at_ms - 25, 0)),
            "checked_files": 4,
            "ok_files": 4,
            "warn_files": 0,
            "fail_files": 0,
        },
    )
    _write_json(
        ws_meta_dir / "ws_runs_summary.json",
        {
            "runs": [
                {
                    "run_id": "ws-run-1",
                    "parts": 4,
                    "rows_total": 100,
                    "bytes_total": 1000,
                    "min_date": "2026-03-08",
                    "max_date": "2026-03-09",
                }
            ]
        },
    )
    micro_report_path = tmp_path / "data" / "parquet" / "micro_v1" / "_meta" / "aggregate_report.json"
    _write_json(
        micro_report_path,
        {
            "run_id": "micro-run-1",
            "start": "2026-03-08",
            "end": "2026-03-08",
            "rows_written_total": 123,
        },
    )
    return registry_root, family_dir / "champion.json", ws_raw_root, ws_meta_dir


def test_live_startup_binds_runtime_model_and_ws_public_contract(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(daemon_module.time, "time", lambda: 10.0)
    registry_root, _, ws_raw_root, ws_meta_dir = _seed_runtime_contract(
        tmp_path,
        champion_run_id="run-new",
        ws_updated_at_ms=9_900,
    )

    with LiveStateStore(tmp_path / "live_state.db") as store:
        summary = run_live_sync_daemon(
            store=store,
            client=_QuietClient(),
            settings=LiveDaemonSettings(
                bot_id="autobot-001",
                identifier_prefix="AUTOBOT",
                unknown_open_orders_policy="ignore",
                unknown_positions_policy="import_as_unmanaged",
                allow_cancel_external_orders=False,
                poll_interval_sec=1,
                max_cycles=1,
                startup_reconcile=True,
                registry_root=str(registry_root),
                runtime_model_ref_source="champion_v4",
                runtime_model_family="train_v4_crypto_cs",
                ws_public_raw_root=str(ws_raw_root),
                ws_public_meta_dir=str(ws_meta_dir),
                ws_public_stale_threshold_sec=180,
                micro_aggregate_report_path=str(tmp_path / "data" / "parquet" / "micro_v1" / "_meta" / "aggregate_report.json"),
            ),
        )
        runtime_contract = store.runtime_contract()
        ws_contract = store.ws_public_contract()
        runtime_health = store.live_runtime_health()

    assert summary["halted"] is False
    assert summary["live_runtime_model_run_id"] == "run-new"
    assert summary["champion_pointer_run_id"] == "run-new"
    assert summary["model_pointer_divergence"] is False
    assert summary["ws_public_staleness_sec"] == pytest.approx(0.1, rel=0.01)
    assert runtime_contract is not None
    assert runtime_contract["live_runtime_model_run_id"] == "run-new"
    assert runtime_contract["promote_happened_while_down"] is False
    assert ws_contract is not None
    assert ws_contract["ws_public_stale"] is False
    assert ws_contract["micro_aggregate"]["run_id"] == "micro-run-1"
    assert runtime_health is not None
    assert runtime_health["ws_public_run_id"] == "ws-run-1"


def test_live_startup_prefers_fresh_health_checkpoint_even_if_slightly_ahead_of_read_clock(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(daemon_module.time, "time", lambda: 10.0)
    registry_root, _, ws_raw_root, ws_meta_dir = _seed_runtime_contract(
        tmp_path,
        champion_run_id="run-fresh",
        ws_updated_at_ms=10_050,
    )

    with LiveStateStore(tmp_path / "live_state.db") as store:
        summary = run_live_sync_daemon(
            store=store,
            client=_QuietClient(),
            settings=LiveDaemonSettings(
                bot_id="autobot-001",
                identifier_prefix="AUTOBOT",
                unknown_open_orders_policy="ignore",
                unknown_positions_policy="import_as_unmanaged",
                allow_cancel_external_orders=False,
                poll_interval_sec=1,
                max_cycles=1,
                startup_reconcile=False,
                registry_root=str(registry_root),
                runtime_model_ref_source="champion_v4",
                runtime_model_family="train_v4_crypto_cs",
                ws_public_raw_root=str(ws_raw_root),
                ws_public_meta_dir=str(ws_meta_dir),
                ws_public_stale_threshold_sec=180,
                micro_aggregate_report_path=str(tmp_path / "data" / "parquet" / "micro_v1" / "_meta" / "aggregate_report.json"),
            ),
        )
        ws_contract = store.ws_public_contract()

    assert summary["halted"] is False
    assert summary["ws_public_staleness_sec"] == pytest.approx(0.0, abs=1e-9)
    assert ws_contract is not None
    assert ws_contract["ws_public_last_checkpoint_source"] == "health_snapshot.updated_at_ms"
    assert ws_contract["ws_public_last_checkpoint_ts_ms"] == 10_050
    assert ws_contract["ws_public_stale"] is False


def test_live_daemon_halts_on_model_pointer_divergence(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(daemon_module.time, "time", lambda: 10.0)
    registry_root, champion_pointer_path, ws_raw_root, ws_meta_dir = _seed_runtime_contract(
        tmp_path,
        champion_run_id="run-old",
        ws_updated_at_ms=9_900,
    )
    ((registry_root / "train_v4_crypto_cs" / "run-new")).mkdir(parents=True, exist_ok=True)

    with LiveStateStore(tmp_path / "live_state.db") as store:
        summary = run_live_sync_daemon(
            store=store,
            client=_PointerFlipClient(pointer_path=champion_pointer_path, next_run_id="run-new"),
            settings=LiveDaemonSettings(
                bot_id="autobot-001",
                identifier_prefix="AUTOBOT",
                unknown_open_orders_policy="ignore",
                unknown_positions_policy="import_as_unmanaged",
                allow_cancel_external_orders=False,
                poll_interval_sec=1,
                max_cycles=1,
                startup_reconcile=False,
                registry_root=str(registry_root),
                runtime_model_ref_source="champion_v4",
                runtime_model_family="train_v4_crypto_cs",
                ws_public_raw_root=str(ws_raw_root),
                ws_public_meta_dir=str(ws_meta_dir),
                ws_public_stale_threshold_sec=180,
                micro_aggregate_report_path=str(tmp_path / "data" / "parquet" / "micro_v1" / "_meta" / "aggregate_report.json"),
            ),
        )

    assert summary["halted"] is True
    assert "MODEL_POINTER_DIVERGENCE" in summary["halted_reasons"]
    assert summary["model_pointer_divergence"] is True
    assert summary["live_runtime_model_run_id"] == "run-old"
    assert summary["champion_pointer_run_id"] == "run-new"


def test_live_startup_halts_when_ws_public_is_stale(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(daemon_module.time, "time", lambda: 10.0)
    registry_root, _, ws_raw_root, ws_meta_dir = _seed_runtime_contract(
        tmp_path,
        champion_run_id="run-stale",
        ws_updated_at_ms=1_000,
    )

    with LiveStateStore(tmp_path / "live_state.db") as store:
        summary = run_live_sync_daemon(
            store=store,
            client=_QuietClient(),
            settings=LiveDaemonSettings(
                bot_id="autobot-001",
                identifier_prefix="AUTOBOT",
                unknown_open_orders_policy="ignore",
                unknown_positions_policy="import_as_unmanaged",
                allow_cancel_external_orders=False,
                poll_interval_sec=1,
                max_cycles=1,
                startup_reconcile=False,
                registry_root=str(registry_root),
                runtime_model_ref_source="champion_v4",
                runtime_model_family="train_v4_crypto_cs",
                ws_public_raw_root=str(ws_raw_root),
                ws_public_meta_dir=str(ws_meta_dir),
                ws_public_stale_threshold_sec=5,
                micro_aggregate_report_path=str(tmp_path / "data" / "parquet" / "micro_v1" / "_meta" / "aggregate_report.json"),
            ),
        )

    assert summary["halted"] is True
    assert "WS_PUBLIC_STALE" in summary["halted_reasons"]
    assert summary["ws_public_staleness_sec"] == pytest.approx(9.0, rel=0.01)


def test_runtime_installer_dry_run_exposes_model_contract_envs() -> None:
    pwsh = shutil.which("powershell.exe") or shutil.which("pwsh")
    if not pwsh:
        pytest.skip("PowerShell executable is required for installer dry-run test")
    script = REPO_ROOT / "scripts" / "install_server_runtime_services.ps1"
    completed = subprocess.run(
        [
            pwsh,
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script),
            "-ProjectRoot",
            str(REPO_ROOT),
            "-PythonExe",
            "python",
            "-DryRun",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )

    stdout = completed.stdout
    assert "Environment=AUTOBOT_RUNTIME_MODEL_REF_SOURCE=champion_v4" in stdout
    assert "Environment=AUTOBOT_RUNTIME_MODEL_FAMILY=train_v4_crypto_cs" in stdout
