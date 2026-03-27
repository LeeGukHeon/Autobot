from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest

import autobot.live.daemon as daemon_module
from autobot.live.daemon import LiveDaemonSettings, run_live_sync_daemon
from autobot.live.rollout import build_rollout_contract, build_rollout_test_order_record
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


def _make_fake_sudo(tmp_path: Path) -> Path:
    if os.name == "nt":
        wrapper_path = tmp_path / "sudo.cmd"
        wrapper_path.write_text("@echo off\r\n%*\r\n", encoding="utf-8")
    else:
        wrapper_path = tmp_path / "sudo"
        wrapper_path.write_text("#!/bin/sh\n\"$@\"\n", encoding="utf-8")
        wrapper_path.chmod(0o755)
    return wrapper_path


def _make_fake_systemctl(tmp_path: Path) -> Path:
    if os.name == "nt":
        wrapper_path = tmp_path / "systemctl.cmd"
        wrapper_path.write_text(
            "@echo off\r\n"
            "if not \"%FAKE_SYSTEMCTL_LOG%\"==\"\" echo %*>>\"%FAKE_SYSTEMCTL_LOG%\"\r\n"
            "if \"%1\"==\"is-active\" exit /b 1\r\n"
            "exit /b 0\r\n",
            encoding="utf-8",
        )
    else:
        wrapper_path = tmp_path / "systemctl"
        wrapper_path.write_text(
            "#!/bin/sh\n"
            "if [ -n \"$FAKE_SYSTEMCTL_LOG\" ]; then\n"
            "  echo \"$@\" >> \"$FAKE_SYSTEMCTL_LOG\"\n"
            "fi\n"
            "if [ \"$1\" = \"is-active\" ]; then\n"
            "  exit 1\n"
            "fi\n"
            "exit 0\n",
            encoding="utf-8",
        )
        wrapper_path.chmod(0o755)
    return wrapper_path


def _make_fake_install(tmp_path: Path) -> Path:
    if os.name == "nt":
        wrapper_path = tmp_path / "install.cmd"
        wrapper_path.write_text(
            "@echo off\r\n"
            "if not \"%FAKE_INSTALL_LOG%\"==\"\" echo %*>>\"%FAKE_INSTALL_LOG%\"\r\n"
            "exit /b 0\r\n",
            encoding="utf-8",
        )
    else:
        wrapper_path = tmp_path / "install"
        wrapper_path.write_text(
            "#!/bin/sh\n"
            "if [ -n \"$FAKE_INSTALL_LOG\" ]; then\n"
            "  echo \"$@\" >> \"$FAKE_INSTALL_LOG\"\n"
            "fi\n"
            "exit 0\n",
            encoding="utf-8",
        )
        wrapper_path.chmod(0o755)
    return wrapper_path


def _make_fake_python_logger(tmp_path: Path) -> Path:
    if os.name == "nt":
        wrapper_path = tmp_path / "fake_python.cmd"
        wrapper_path.write_text(
            "@echo off\r\n"
            "if not \"%FAKE_PYTHON_LOG%\"==\"\" echo %*>>\"%FAKE_PYTHON_LOG%\"\r\n"
            "echo {}\r\n"
            "exit /b 0\r\n",
            encoding="utf-8",
        )
    else:
        wrapper_path = tmp_path / "fake_python"
        wrapper_path.write_text(
            "#!/bin/sh\n"
            "if [ -n \"$FAKE_PYTHON_LOG\" ]; then\n"
            "  echo \"$@\" >> \"$FAKE_PYTHON_LOG\"\n"
            "fi\n"
            "printf '{}\\n'\n",
            encoding="utf-8",
        )
        wrapper_path.chmod(0o755)
    return wrapper_path


def _ts_ms_to_iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


def _seed_runtime_contract(
    tmp_path: Path,
    *,
    champion_run_id: str,
    ws_updated_at_ms: int,
    ws_last_rx_ts_ms: int | dict[str, int] | None = None,
    collect_generated_at_ms: int | None = None,
    validate_generated_at_ms: int | None = None,
) -> tuple[Path, Path, Path, Path]:
    registry_root = tmp_path / "models" / "registry"
    family_dir = registry_root / "train_v4_crypto_cs"
    (family_dir / champion_run_id).mkdir(parents=True, exist_ok=True)
    _write_json(family_dir / "champion.json", {"run_id": champion_run_id})

    if ws_last_rx_ts_ms is None:
        last_rx_ts_ms = {"trade": ws_updated_at_ms, "orderbook": ws_updated_at_ms}
    elif isinstance(ws_last_rx_ts_ms, dict):
        last_rx_ts_ms = {str(key): int(value) for key, value in ws_last_rx_ts_ms.items()}
    else:
        last_rx_ts_ms = {"trade": int(ws_last_rx_ts_ms), "orderbook": int(ws_last_rx_ts_ms)}

    ws_raw_root = tmp_path / "data" / "raw_ws" / "upbit" / "public"
    ws_meta_dir = tmp_path / "data" / "raw_ws" / "upbit" / "_meta"
    _write_json(
        ws_meta_dir / "ws_public_health.json",
        {
            "run_id": "ws-run-1",
            "updated_at_ms": ws_updated_at_ms,
            "connected": True,
            "last_rx_ts_ms": last_rx_ts_ms,
            "subscribed_markets_count": 20,
        },
    )
    collect_at_ms = collect_generated_at_ms if collect_generated_at_ms is not None else max(ws_updated_at_ms - 50, 0)
    validate_at_ms = validate_generated_at_ms if validate_generated_at_ms is not None else max(ws_updated_at_ms - 25, 0)
    _write_json(
        ws_meta_dir / "ws_collect_report.json",
        {
            "run_id": "ws-collect-1",
            "generated_at": _ts_ms_to_iso(max(int(collect_at_ms), 0)),
        },
    )
    _write_json(
        ws_meta_dir / "ws_validate_report.json",
        {
            "run_id": "ws-validate-1",
            "generated_at": _ts_ms_to_iso(max(int(validate_at_ms), 0)),
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
    assert ws_contract["ws_public_last_checkpoint_source"] == "health_snapshot.last_rx_ts_ms"
    assert ws_contract["ws_public_last_checkpoint_ts_ms"] == 10_050
    assert ws_contract["ws_public_stale"] is False


def test_live_startup_uses_actual_receive_timestamp_even_if_reports_are_rewritten(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(daemon_module.time, "time", lambda: 10.0)
    registry_root, _, ws_raw_root, ws_meta_dir = _seed_runtime_contract(
        tmp_path,
        champion_run_id="run-fresh",
        ws_updated_at_ms=1_000,
        ws_last_rx_ts_ms=9_950,
        collect_generated_at_ms=10_000,
        validate_generated_at_ms=10_000,
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
    assert summary["ws_public_staleness_sec"] == pytest.approx(0.05, rel=0.01)
    assert ws_contract is not None
    assert ws_contract["ws_public_last_checkpoint_source"] == "health_snapshot.last_rx_ts_ms"
    assert ws_contract["ws_public_last_checkpoint_ts_ms"] == 9_950
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


def test_live_daemon_allows_direct_model_ref_without_pointer_monitoring(tmp_path: Path, monkeypatch) -> None:
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
                bot_id="autobot-candidate-001",
                identifier_prefix="AUTOBOT",
                unknown_open_orders_policy="ignore",
                unknown_positions_policy="import_as_unmanaged",
                allow_cancel_external_orders=False,
                poll_interval_sec=1,
                max_cycles=1,
                startup_reconcile=False,
                registry_root=str(registry_root),
                runtime_model_ref_source="run-old",
                runtime_model_family="train_v4_crypto_cs",
                ws_public_raw_root=str(ws_raw_root),
                ws_public_meta_dir=str(ws_meta_dir),
                ws_public_stale_threshold_sec=180,
                micro_aggregate_report_path=str(tmp_path / "data" / "parquet" / "micro_v1" / "_meta" / "aggregate_report.json"),
            ),
        )

    assert summary["halted"] is False
    assert summary["model_pointer_divergence"] is False
    assert summary["live_runtime_model_run_id"] == "run-old"
    assert summary["champion_pointer_run_id"] == "run-new"


def test_live_daemon_canary_tolerates_candidate_pointer_divergence(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(daemon_module.time, "time", lambda: 10.0)
    registry_root, champion_pointer_path, ws_raw_root, ws_meta_dir = _seed_runtime_contract(
        tmp_path,
        champion_run_id="run-old",
        ws_updated_at_ms=9_900,
    )
    family_dir = registry_root / "train_v4_crypto_cs"
    (family_dir / "run-new").mkdir(parents=True, exist_ok=True)
    (family_dir / "latest_candidate.json").write_text(
        json.dumps({"run_id": "run-new"}, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )

    with LiveStateStore(tmp_path / "live_state.db") as store:
        now_ms = 10_000
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="canary",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1000,
            ),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        summary = run_live_sync_daemon(
            store=store,
            client=_QuietClient(),
            settings=LiveDaemonSettings(
                bot_id="autobot-candidate-001",
                identifier_prefix="AUTOBOT",
                unknown_open_orders_policy="ignore",
                unknown_positions_policy="import_as_unmanaged",
                allow_cancel_external_orders=False,
                poll_interval_sec=1,
                max_cycles=1,
                startup_reconcile=False,
                registry_root=str(registry_root),
                runtime_model_ref_source="latest_candidate_v4",
                runtime_model_family="train_v4_crypto_cs",
                ws_public_raw_root=str(ws_raw_root),
                ws_public_meta_dir=str(ws_meta_dir),
                ws_public_stale_threshold_sec=180,
                micro_aggregate_report_path=str(tmp_path / "data" / "parquet" / "micro_v1" / "_meta" / "aggregate_report.json"),
                rollout_mode="canary",
                rollout_target_unit="autobot-live-alpha.service",
                small_account_canary_enabled=True,
                small_account_max_positions=1,
                small_account_max_open_orders_per_market=1,
            ),
        )

    assert summary["halted"] is False
    assert summary["model_pointer_divergence"] is False
    assert summary["live_runtime_model_run_id"] == "run-new"
    assert summary["champion_pointer_run_id"] == "run-old"


def test_live_daemon_candidate_reuses_previous_pinned_run_when_candidate_pointer_is_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(daemon_module.time, "time", lambda: 10.0)
    registry_root, _, ws_raw_root, ws_meta_dir = _seed_runtime_contract(
        tmp_path,
        champion_run_id="run-old",
        ws_updated_at_ms=9_900,
    )
    family_dir = registry_root / "train_v4_crypto_cs"
    (family_dir / "run-prev-candidate").mkdir(parents=True, exist_ok=True)

    with LiveStateStore(tmp_path / "live_state.db") as store:
        now_ms = 10_000
        store.set_runtime_contract(
            payload={
                "live_runtime_model_run_id": "run-prev-candidate",
                "model_family_resolved": "train_v4_crypto_cs",
            },
            ts_ms=9_500,
        )
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="canary",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1000,
            ),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        summary = run_live_sync_daemon(
            store=store,
            client=_QuietClient(),
            settings=LiveDaemonSettings(
                bot_id="autobot-candidate-001",
                identifier_prefix="AUTOBOT",
                unknown_open_orders_policy="ignore",
                unknown_positions_policy="import_as_unmanaged",
                allow_cancel_external_orders=False,
                poll_interval_sec=1,
                max_cycles=1,
                startup_reconcile=False,
                registry_root=str(registry_root),
                runtime_model_ref_source="latest_candidate_v4",
                runtime_model_family="train_v4_crypto_cs",
                ws_public_raw_root=str(ws_raw_root),
                ws_public_meta_dir=str(ws_meta_dir),
                ws_public_stale_threshold_sec=180,
                micro_aggregate_report_path=str(tmp_path / "data" / "parquet" / "micro_v1" / "_meta" / "aggregate_report.json"),
                rollout_mode="canary",
                rollout_target_unit="autobot-live-alpha.service",
                small_account_canary_enabled=True,
                small_account_max_positions=1,
                small_account_max_open_orders_per_market=1,
            ),
        )
        runtime_contract = store.runtime_contract()

    assert summary["halted"] is False
    assert summary["live_runtime_model_run_id"] == "run-prev-candidate"
    assert runtime_contract is not None
    assert runtime_contract["startup_resolution_fallback"] == "previous_pinned_run"
    assert runtime_contract["startup_resolution_warning"] == "LATEST_CANDIDATE_POINTER_UNRESOLVED"
    assert runtime_contract["requested_pointer_name"] == "latest_candidate"


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


def test_runtime_installer_dry_run_accepts_model_family_override() -> None:
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
            "-PaperPreset",
            "paired_v4",
            "-PaperModelFamilyOverride",
            "train_v5_panel_ensemble",
            "-DryRun",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )

    stdout = completed.stdout
    assert "Environment=AUTOBOT_RUNTIME_MODEL_FAMILY=train_v5_panel_ensemble" in stdout
    assert "--model-family" in stdout
    assert "train_v5_panel_ensemble" in stdout


def test_runtime_installer_rejects_challenger_role_without_pinned_candidate_ref() -> None:
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
            "-PaperPreset",
            "live_v4",
            "-PaperRuntimeRole",
            "challenger",
            "-DryRun",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode != 0
    assert "cannot default to champion source" in (completed.stdout + completed.stderr)


def test_runtime_installer_rejects_champion_role_with_candidate_default_source() -> None:
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
            "-PaperPreset",
            "candidate_v4",
            "-PaperRuntimeRole",
            "champion",
            "-DryRun",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode != 0
    assert "cannot default to candidate source" in (completed.stdout + completed.stderr)


def test_runtime_installer_allows_challenger_role_with_pinned_model_ref() -> None:
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
            "-PaperPreset",
            "live_v4",
            "-PaperRuntimeRole",
            "challenger",
            "-PaperModelRefPinned",
            "run-123",
            "-DryRun",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )

    stdout = completed.stdout
    assert "Environment=AUTOBOT_PAPER_RUNTIME_ROLE=challenger" in stdout
    assert "Environment=AUTOBOT_PAPER_MODEL_REF_PINNED=run-123" in stdout


def test_runtime_installer_requires_explicit_bootstrap_when_champion_missing(tmp_path: Path) -> None:
    pwsh = shutil.which("powershell.exe") or shutil.which("pwsh")
    if not pwsh:
        pytest.skip("PowerShell executable is required for installer test")
    project_root = tmp_path / "project"
    (project_root / "models" / "registry" / "train_v4_crypto_cs").mkdir(parents=True, exist_ok=True)
    wrappers_dir = tmp_path / "wrappers"
    wrappers_dir.mkdir()
    _make_fake_sudo(wrappers_dir)
    _make_fake_systemctl(wrappers_dir)
    _make_fake_install(wrappers_dir)
    fake_python = _make_fake_python_logger(wrappers_dir)
    python_log = tmp_path / "python.log"

    completed = subprocess.run(
        [
            pwsh,
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(REPO_ROOT / "scripts" / "install_server_runtime_services.ps1"),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            str(fake_python),
            "-NoEnable",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": str(wrappers_dir) + os.pathsep + os.environ.get("PATH", ""),
            "FAKE_PYTHON_LOG": str(python_log),
        },
        check=False,
    )

    assert completed.returncode != 0
    assert "no longer auto-bootstraps" in (completed.stdout + completed.stderr)
    assert not python_log.exists() or python_log.read_text(encoding="utf-8").strip() == ""


def test_runtime_installer_only_bootstraps_when_explicitly_requested(tmp_path: Path) -> None:
    pwsh = shutil.which("powershell.exe") or shutil.which("pwsh")
    if not pwsh:
        pytest.skip("PowerShell executable is required for installer test")
    project_root = tmp_path / "project"
    (project_root / "models" / "registry" / "train_v4_crypto_cs").mkdir(parents=True, exist_ok=True)
    wrappers_dir = tmp_path / "wrappers"
    wrappers_dir.mkdir()
    _make_fake_sudo(wrappers_dir)
    _make_fake_systemctl(wrappers_dir)
    _make_fake_install(wrappers_dir)
    fake_python = _make_fake_python_logger(wrappers_dir)
    python_log = tmp_path / "python.log"
    systemctl_log = tmp_path / "systemctl.log"
    install_log = tmp_path / "install.log"

    completed = subprocess.run(
        [
            pwsh,
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(REPO_ROOT / "scripts" / "install_server_runtime_services.ps1"),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            str(fake_python),
            "-BootstrapChampion",
            "-NoEnable",
            "-NoStart",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": str(wrappers_dir) + os.pathsep + os.environ.get("PATH", ""),
            "FAKE_PYTHON_LOG": str(python_log),
            "FAKE_SYSTEMCTL_LOG": str(systemctl_log),
            "FAKE_INSTALL_LOG": str(install_log),
        },
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    assert "[paper-install][bootstrap]" in completed.stdout
    python_args = python_log.read_text(encoding="utf-8")
    assert "autobot.cli model promote" in python_args
    assert "latest_candidate_v4" in python_args
    assert "daemon-reload" in systemctl_log.read_text(encoding="utf-8")
    assert install_log.exists()
