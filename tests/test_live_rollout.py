from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
from types import SimpleNamespace

import pytest

import autobot.cli as cli_module
import autobot.live.daemon as daemon_module
from autobot.live.breakers import ACTION_HALT_NEW_INTENTS, arm_breaker
from autobot.live.daemon import LiveDaemonSettings, run_live_sync_daemon
from autobot.live.rollout import (
    build_rollout_contract,
    build_rollout_test_order_record,
    evaluate_live_rollout_gate,
    load_rollout_latest,
    resolve_rollout_gate_inputs,
    rollout_gate_to_payload,
    rollout_latest_artifact_path,
    write_rollout_latest,
)
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


class _AutoRefreshTestOrderClient(_QuietClient):
    def __init__(self) -> None:
        self.order_test_calls: list[dict[str, object]] = []

    def order_test(self, **kwargs):  # noqa: ANN201
        self.order_test_calls.append(dict(kwargs))
        return {"ok": True, "uuid": "auto-test-order-1"}


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_runtime_contract(tmp_path: Path, *, champion_run_id: str, ws_updated_at_ms: int) -> Path:
    registry_root = tmp_path / "models" / "registry"
    family_dir = registry_root / "train_v4_crypto_cs"
    (family_dir / champion_run_id).mkdir(parents=True, exist_ok=True)
    _write_json(family_dir / "champion.json", {"run_id": champion_run_id})
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
        ws_meta_dir / "ws_runs_summary.json",
        {"runs": [{"run_id": "ws-run-1", "rows_total": 10, "bytes_total": 100}]},
    )
    _write_json(
        tmp_path / "data" / "parquet" / "micro_v1" / "_meta" / "aggregate_report.json",
        {"run_id": "micro-run-1", "rows_written_total": 12},
    )
    return registry_root


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required for installer dry-run test")


def test_rollout_shadow_mode_starts_without_order_emission() -> None:
    gate = evaluate_live_rollout_gate(
        mode="shadow",
        target_unit="autobot-live-alpha.service",
        contract={},
        test_order={},
        breaker_active=False,
        require_test_order=True,
        test_order_max_age_sec=86400,
        small_account_single_slot_ready=True,
        ts_ms=10_000,
    )
    assert gate.start_allowed is True
    assert gate.order_emission_allowed is False
    assert gate.reason_codes == ()


def test_rollout_canary_requires_arm_and_test_order() -> None:
    gate = evaluate_live_rollout_gate(
        mode="canary",
        target_unit="autobot-live-alpha.service",
        contract={},
        test_order={},
        breaker_active=False,
        require_test_order=True,
        test_order_max_age_sec=86400,
        small_account_single_slot_ready=True,
        ts_ms=10_000,
    )
    assert gate.start_allowed is False
    assert gate.order_emission_allowed is False
    assert "LIVE_ROLLOUT_NOT_ARMED" in gate.reason_codes
    assert "LIVE_TEST_ORDER_REQUIRED" in gate.reason_codes


def test_rollout_payload_exposes_operational_reason_taxonomy() -> None:
    gate = evaluate_live_rollout_gate(
        mode="canary",
        target_unit="autobot-live-alpha.service",
        contract={},
        test_order={},
        breaker_active=False,
        require_test_order=True,
        test_order_max_age_sec=86400,
        small_account_single_slot_ready=True,
        ts_ms=10_000,
    )

    payload = rollout_gate_to_payload(gate)

    assert payload["taxonomy_version"] == 1
    assert payload["primary_reason_type"] == "OPERATIONAL_POLICY"
    assert payload["reason_types"] == ["OPERATIONAL_POLICY"]
    assert payload["typed_reason_codes"][0]["reason_code"] == "LIVE_ROLLOUT_NOT_ARMED"
    assert payload["typed_reason_codes"][0]["clear_policy"] == "ROLLOUT_RECOVERY"


def test_rollout_canary_passes_with_matching_contract_and_fresh_test_order() -> None:
    contract = build_rollout_contract(
        mode="canary",
        target_unit="autobot-live-alpha.service",
        arm_token="demo-token",
        ts_ms=5_000,
    )
    test_order = build_rollout_test_order_record(
        market="KRW-BTC",
        side="bid",
        ord_type="limit",
        price="5000",
        volume="1",
        ok=True,
        response_payload={"ok": True},
        ts_ms=9_500,
    )
    gate = evaluate_live_rollout_gate(
        mode="canary",
        target_unit="autobot-live-alpha.service",
        contract=contract,
        test_order=test_order,
        breaker_active=False,
        require_test_order=True,
        test_order_max_age_sec=86400,
        small_account_single_slot_ready=True,
        ts_ms=10_000,
    )
    assert gate.start_allowed is True
    assert gate.order_emission_allowed is True
    assert gate.reason_codes == ()


def test_resolve_rollout_gate_inputs_prefers_contract_mode_and_target() -> None:
    contract = build_rollout_contract(
        mode="canary",
        target_unit="autobot-live-alpha.service",
        arm_token="demo-token",
        ts_ms=5_000,
    )
    mode, target_unit = resolve_rollout_gate_inputs(
        default_mode="shadow",
        default_target_unit="shadow-unit.service",
        contract=contract,
    )
    assert mode == "canary"
    assert target_unit == "autobot-live-alpha.service"


def test_resolve_rollout_gate_inputs_falls_back_to_defaults_without_contract() -> None:
    mode, target_unit = resolve_rollout_gate_inputs(
        default_mode="shadow",
        default_target_unit="autobot-live-alpha.service",
        contract=None,
    )
    assert mode == "shadow"
    assert target_unit == "autobot-live-alpha.service"


def test_write_rollout_latest_writes_scoped_and_global_artifacts(tmp_path: Path) -> None:
    contract = build_rollout_contract(
        mode="canary",
        target_unit="autobot-live-alpha.service",
        arm_token="demo-token",
        ts_ms=5_000,
    )
    test_order = build_rollout_test_order_record(
        market="KRW-BTC",
        side="bid",
        ord_type="limit",
        price="5000",
        volume="1",
        ok=True,
        response_payload={"ok": True},
        ts_ms=9_500,
    )
    path = write_rollout_latest(
        project_root=tmp_path,
        event_kind="ARM",
        contract=contract,
        test_order=test_order,
        status={"target_unit": "autobot-live-alpha.service", "mode": "canary"},
        ts_ms=10_000,
    )

    global_doc = json.loads(path.read_text(encoding="utf-8"))
    scoped_path = rollout_latest_artifact_path(tmp_path, target_unit="autobot-live-alpha.service")
    scoped_doc = json.loads(scoped_path.read_text(encoding="utf-8"))

    assert global_doc["target_unit"] == "autobot-live-alpha.service"
    assert scoped_doc["target_unit"] == "autobot-live-alpha.service"
    assert scoped_path.name.startswith("latest.autobot_live_alpha_service")


def test_load_rollout_latest_prefers_scoped_target_artifact(tmp_path: Path) -> None:
    root = tmp_path / "logs" / "live_rollout"
    root.mkdir(parents=True, exist_ok=True)
    (root / "latest.json").write_text(json.dumps({"target_unit": "autobot-live-alpha-candidate.service"}), encoding="utf-8")
    (root / "latest.autobot_live_alpha_service.json").write_text(
        json.dumps({"target_unit": "autobot-live-alpha.service", "status": {"mode": "canary"}}),
        encoding="utf-8",
    )

    loaded = load_rollout_latest(tmp_path, target_unit="autobot-live-alpha.service")

    assert loaded["target_unit"] == "autobot-live-alpha.service"


def test_live_daemon_keeps_running_when_canary_rollout_is_not_armed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(daemon_module.time, "time", lambda: 10.0)
    registry_root = _seed_runtime_contract(tmp_path, champion_run_id="run-live", ws_updated_at_ms=9_900)
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
                ws_public_raw_root=str(tmp_path / "data" / "raw_ws" / "upbit" / "public"),
                ws_public_meta_dir=str(tmp_path / "data" / "raw_ws" / "upbit" / "_meta"),
                ws_public_stale_threshold_sec=180,
                micro_aggregate_report_path=str(tmp_path / "data" / "parquet" / "micro_v1" / "_meta" / "aggregate_report.json"),
                rollout_mode="canary",
                rollout_target_unit="autobot-live-alpha.service",
            ),
        )
    assert summary["halted"] is False
    assert "LIVE_ROLLOUT_NOT_ARMED" in summary["rollout_reason_codes"]
    assert summary["rollout_start_allowed"] is False
    assert summary["rollout_order_emission_allowed"] is False


def test_live_daemon_clears_recovered_stale_breaker_before_canary_gate(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(daemon_module.time, "time", lambda: 10.0)
    registry_root = _seed_runtime_contract(tmp_path, champion_run_id="run-live", ws_updated_at_ms=9_950)
    now_ms = 10_000
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="canary",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1_000,
            ),
            ts_ms=now_ms - 1_000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="5000",
                volume="1",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms - 100,
            ),
            ts_ms=now_ms - 100,
        )
        arm_breaker(
            store,
            reason_codes=["WS_PUBLIC_STALE", "LIVE_BREAKER_ACTIVE", "LIVE_PUBLIC_WS_STREAM_FAILED", "LIVE_RUNTIME_LOOP_FAILED"],
            source="test",
            ts_ms=now_ms - 2_000,
            action=ACTION_HALT_NEW_INTENTS,
        )
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
                ws_public_raw_root=str(tmp_path / "data" / "raw_ws" / "upbit" / "public"),
                ws_public_meta_dir=str(tmp_path / "data" / "raw_ws" / "upbit" / "_meta"),
                ws_public_stale_threshold_sec=180,
                micro_aggregate_report_path=str(tmp_path / "data" / "parquet" / "micro_v1" / "_meta" / "aggregate_report.json"),
                rollout_mode="canary",
                rollout_target_unit="autobot-live-alpha.service",
                small_account_canary_enabled=True,
                small_account_max_positions=1,
                small_account_max_open_orders_per_market=1,
            ),
        )
        breaker = store.breaker_state(breaker_key="live")

    assert summary["halted"] is False
    assert summary["rollout_start_allowed"] is True
    assert summary["rollout_order_emission_allowed"] is True
    assert summary["ws_public_staleness_sec"] == pytest.approx(0.05, rel=0.01)
    assert breaker is not None
    assert breaker["active"] is False
    assert breaker["reason_codes"] == []


def test_live_daemon_auto_refreshes_stale_rollout_test_order(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(daemon_module.time, "time", lambda: 10.0)
    registry_root = _seed_runtime_contract(tmp_path, champion_run_id="run-live", ws_updated_at_ms=9_950)
    now_ms = 10_000
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="canary",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 2_000,
            ),
            ts_ms=now_ms - 2_000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="5000",
                volume="1",
                ok=True,
                response_payload={"ok": True},
                ts_ms=0,
            ),
            ts_ms=0,
        )
        client = _AutoRefreshTestOrderClient()
        summary = run_live_sync_daemon(
            store=store,
            client=client,
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
                ws_public_raw_root=str(tmp_path / "data" / "raw_ws" / "upbit" / "public"),
                ws_public_meta_dir=str(tmp_path / "data" / "raw_ws" / "upbit" / "_meta"),
                ws_public_stale_threshold_sec=180,
                micro_aggregate_report_path=str(tmp_path / "data" / "parquet" / "micro_v1" / "_meta" / "aggregate_report.json"),
                rollout_mode="canary",
                rollout_target_unit="autobot-live-alpha.service",
                rollout_test_order_max_age_sec=5,
                small_account_canary_enabled=True,
                small_account_max_positions=1,
                small_account_max_open_orders_per_market=1,
            ),
        )
        refreshed_test_order = store.live_test_order() or {}
        rollout_status = store.live_rollout_status() or {}
        auto_refresh_checkpoint = store.get_checkpoint(name=daemon_module.LIVE_ROLLOUT_AUTO_TEST_ORDER_REFRESH_CHECKPOINT)

    assert summary["halted"] is False
    assert summary["rollout_order_emission_allowed"] is True
    assert len(client.order_test_calls) == 1
    assert client.order_test_calls[0]["market"] == "KRW-BTC"
    assert refreshed_test_order["ts_ms"] == now_ms
    assert rollout_status["order_emission_allowed"] is True
    assert auto_refresh_checkpoint is not None
    assert auto_refresh_checkpoint["payload"]["ok"] is True


def test_live_installer_dry_run_exposes_rollout_mode() -> None:
    script = REPO_ROOT / "scripts" / "install_server_live_runtime_service.ps1"
    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script),
            "-ProjectRoot",
            str(REPO_ROOT),
            "-PythonExe",
            "python",
            "-RolloutMode",
            "shadow",
            "-SyncMode",
            "private_ws",
            "-DryRun",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    stdout = completed.stdout
    assert "[live-install][dry-run] unit=autobot-live-alpha.service" in stdout
    assert "autobot.cli" in stdout
    assert "'live'" in stdout or '"live"' in stdout
    assert "'run'" in stdout or '"run"' in stdout
    assert "--rollout-mode" in stdout
    assert "--use-private-ws" in stdout
    assert "Environment=AUTOBOT_LIVE_MODEL_REF_SOURCE=champion" in stdout
    assert "Environment=AUTOBOT_LIVE_MODEL_FAMILY=train_v5_fusion" in stdout
    assert "RestartPreventExitStatus=2" in stdout
    assert "StartLimitIntervalSec=60" in stdout
    assert "StartLimitBurst=4" in stdout


def test_live_installer_dry_run_supports_strategy_runtime() -> None:
    script = REPO_ROOT / "scripts" / "install_server_live_runtime_service.ps1"
    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script),
            "-ProjectRoot",
            str(REPO_ROOT),
            "-PythonExe",
            "python",
            "-RolloutMode",
            "shadow",
            "-SyncMode",
            "poll",
            "-StrategyRuntime",
            "-DryRun",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    stdout = completed.stdout
    assert "--strategy-runtime" in stdout


def test_live_installer_dry_run_supports_strategy_runtime_with_private_ws() -> None:
    script = REPO_ROOT / "scripts" / "install_server_live_runtime_service.ps1"
    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script),
            "-ProjectRoot",
            str(REPO_ROOT),
            "-PythonExe",
            "python",
            "-RolloutMode",
            "shadow",
            "-SyncMode",
            "private_ws",
            "-StrategyRuntime",
            "-DryRun",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    stdout = completed.stdout
    assert "--strategy-runtime" in stdout
    assert "--use-private-ws" in stdout


def test_live_installer_dry_run_supports_candidate_specific_overrides() -> None:
    script = REPO_ROOT / "scripts" / "install_server_live_runtime_service.ps1"
    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script),
            "-ProjectRoot",
            str(REPO_ROOT),
            "-PythonExe",
            "python",
            "-UnitName",
            "autobot-live-alpha-candidate.service",
            "-BotId",
            "autobot-candidate-001",
            "-StateDbPath",
            "data/state/live_state_candidate.db",
            "-ModelRefSource",
            "20260310T011523Z-s42-fc53106c",
            "-ModelFamily",
            "train_v4_crypto_cs",
            "-ModelRegistryRoot",
            "models/registry",
            "-RolloutMode",
            "canary",
            "-RolloutTargetUnit",
            "autobot-live-alpha-candidate.service",
            "-SyncMode",
            "poll",
            "-StrategyRuntime",
            "-DryRun",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    stdout = completed.stdout
    assert "Environment=AUTOBOT_LIVE_BOT_ID=autobot-candidate-001" in stdout
    assert "Environment=AUTOBOT_LIVE_STATE_DB_PATH=data/state/live_state_candidate.db" in stdout
    assert "Environment=AUTOBOT_LIVE_MODEL_REF_SOURCE=20260310T011523Z-s42-fc53106c" in stdout
    assert "Environment=AUTOBOT_LIVE_MODEL_FAMILY=train_v4_crypto_cs" in stdout
    assert "Environment=AUTOBOT_LIVE_TARGET_UNIT=autobot-live-alpha-candidate.service" in stdout
    assert "Environment=AUTOBOT_LIVE_SMALL_ACCOUNT_MAX_POSITIONS=" in stdout


def test_live_installer_candidate_defaults_to_latest_candidate_when_model_source_blank() -> None:
    script = REPO_ROOT / "scripts" / "install_server_live_runtime_service.ps1"
    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script),
            "-ProjectRoot",
            str(REPO_ROOT),
            "-PythonExe",
            "python",
            "-UnitName",
            "autobot-live-alpha-canary.service",
            "-RolloutMode",
            "canary",
            "-RolloutTargetUnit",
            "autobot-live-alpha-canary.service",
            "-SyncMode",
            "poll",
            "-StrategyRuntime",
            "-DryRun",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    stdout = completed.stdout
    assert "Environment=AUTOBOT_LIVE_MODEL_REF_SOURCE=latest_candidate" in stdout
    assert "Environment=AUTOBOT_LIVE_STATE_DB_PATH=data/state/live_canary/live_state.db" in stdout
    assert "Environment=AUTOBOT_LIVE_MODEL_FAMILY=train_v5_fusion" in stdout


def test_live_installer_candidate_dry_run_surfaces_legacy_state_seed_when_target_missing(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    legacy_db = project_root / "data" / "state" / "live_candidate" / "live_state.db"
    legacy_db.parent.mkdir(parents=True, exist_ok=True)
    legacy_db.write_text("sqlite-placeholder", encoding="utf-8")

    script = REPO_ROOT / "scripts" / "install_server_live_runtime_service.ps1"
    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            "python",
            "-UnitName",
            "autobot-live-alpha-canary.service",
            "-RolloutMode",
            "canary",
            "-RolloutTargetUnit",
            "autobot-live-alpha-canary.service",
            "-SyncMode",
            "poll",
            "-StrategyRuntime",
            "-DryRun",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )

    stdout = completed.stdout
    assert "[live-install][dry-run] seed_state_db=" in stdout
    assert str(legacy_db) in stdout
    assert str(project_root / "data" / "state" / "live_canary" / "live_state.db") in stdout


def test_live_installer_candidate_accepts_small_account_position_override() -> None:
    script = REPO_ROOT / "scripts" / "install_server_live_runtime_service.ps1"
    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script),
            "-ProjectRoot",
            str(REPO_ROOT),
            "-PythonExe",
            "python",
            "-UnitName",
            "autobot-live-alpha-candidate.service",
            "-RolloutMode",
            "canary",
            "-RolloutTargetUnit",
            "autobot-live-alpha-candidate.service",
            "-SmallAccountMaxPositions",
            "2",
            "-SyncMode",
            "poll",
            "-StrategyRuntime",
            "-DryRun",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    stdout = completed.stdout
    assert "Environment=AUTOBOT_LIVE_SMALL_ACCOUNT_MAX_POSITIONS=2" in stdout


def test_live_installer_dry_run_emits_strategy_tf_override_when_requested() -> None:
    script = REPO_ROOT / "scripts" / "install_server_live_runtime_service.ps1"
    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script),
            "-ProjectRoot",
            str(REPO_ROOT),
            "-PythonExe",
            "python",
            "-UnitName",
            "autobot-live-alpha-canary.service",
            "-RolloutMode",
            "canary",
            "-RolloutTargetUnit",
            "autobot-live-alpha-canary.service",
            "-StrategyTf",
            "1m",
            "-SyncMode",
            "private_ws",
            "-StrategyRuntime",
            "-DryRun",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    stdout = completed.stdout
    assert "[live-install][dry-run] strategy_tf=1m" in stdout
    assert "Environment=AUTOBOT_LIVE_STRATEGY_TF=1m" in stdout


def test_live_installer_main_defaults_to_server_live_state_path_when_blank() -> None:
    script = REPO_ROOT / "scripts" / "install_server_live_runtime_service.ps1"
    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script),
            "-ProjectRoot",
            str(REPO_ROOT),
            "-PythonExe",
            "python",
            "-UnitName",
            "autobot-live-alpha.service",
            "-RolloutMode",
            "canary",
            "-RolloutTargetUnit",
            "autobot-live-alpha.service",
            "-SyncMode",
            "poll",
            "-StrategyRuntime",
            "-DryRun",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    stdout = completed.stdout
    assert "Environment=AUTOBOT_LIVE_MODEL_REF_SOURCE=champion" in stdout
    assert "Environment=AUTOBOT_LIVE_STATE_DB_PATH=data/state/live_state.db" in stdout
    assert "Environment=AUTOBOT_LIVE_MODEL_FAMILY=train_v5_fusion" in stdout


def test_live_defaults_accept_environment_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    base_config = {
        "live": {
            "bot_id": "autobot-001",
            "state": {"db_path": "data/state/live_state.db"},
            "sync": {"use_private_ws": False, "use_executor_ws": False},
            "model": {"ref": "champion_v4", "family": "train_v4_crypto_cs", "registry_root": "models/registry"},
            "rollout": {"mode": "shadow", "target_unit": "autobot-live-alpha.service"},
            "strategy": {"tf": "5m"},
        },
        "universe": {"quote_currency": "KRW"},
    }
    monkeypatch.setenv("AUTOBOT_LIVE_BOT_ID", "autobot-candidate-001")
    monkeypatch.setenv("AUTOBOT_LIVE_STATE_DB_PATH", "data/state/live_state_candidate.db")
    monkeypatch.setenv("AUTOBOT_LIVE_MODEL_REF_SOURCE", "20260310T011523Z-s42-fc53106c")
    monkeypatch.setenv("AUTOBOT_LIVE_MODEL_FAMILY", "train_v4_crypto_cs")
    monkeypatch.setenv("AUTOBOT_LIVE_MODEL_REGISTRY_ROOT", "models/registry")
    monkeypatch.setenv("AUTOBOT_LIVE_ROLLOUT_MODE", "canary")
    monkeypatch.setenv("AUTOBOT_LIVE_TARGET_UNIT", "autobot-live-alpha-candidate.service")
    monkeypatch.setenv("AUTOBOT_LIVE_SYNC_MODE", "poll")
    monkeypatch.setenv("AUTOBOT_LIVE_STRATEGY_TF", "1m")

    defaults = cli_module._live_defaults(base_config)

    assert defaults["bot_id"] == "autobot-candidate-001"
    assert defaults["state_db_path"] == "data/state/live_state_candidate.db"
    assert defaults["model_ref_source"] == "20260310T011523Z-s42-fc53106c"
    assert defaults["model_family"] == "train_v4_crypto_cs"
    assert defaults["model_registry_root"] == "models/registry"
    assert defaults["rollout_mode"] == "canary"
    assert defaults["rollout_target_unit"] == "autobot-live-alpha-candidate.service"
    assert defaults["sync_use_private_ws"] is False
    assert defaults["sync_use_executor_ws"] is False
    assert defaults["strategy_tf"] == "1m"


def test_build_live_model_alpha_runtime_settings_prefers_live_strategy_tf_override() -> None:
    daemon_settings = SimpleNamespace(use_executor_ws=False)
    live_defaults = {
        "quote_currency": "KRW",
        "strategy_tf": "1m",
        "strategy_decision_interval_sec": 1.0,
        "risk_enabled": False,
        "risk_exit_aggress_bps": 8.0,
        "risk_timeout_sec": 20,
        "risk_replace_max": 2,
        "risk_default_trail_pct": 1.0,
    }
    strategy_defaults = {
        "quote": "KRW",
        "top_n": 20,
        "tf": "5m",
        "universe_refresh_sec": 60.0,
        "universe_hold_sec": 120.0,
        "per_trade_krw": 10000.0,
        "max_positions": 2,
        "min_order_krw": 5000.0,
        "max_consecutive_failures": 3,
        "cooldown_sec_after_fail": 60,
        "micro_gate": {},
        "micro_order_policy": {},
        "paper_live_parquet_root": "data/parquet",
        "paper_live_candles_dataset": "candles_api_v1",
        "paper_live_bootstrap_1m_bars": 2000,
        "paper_live_micro_max_age_ms": 300000,
        "model_alpha": {},
    }

    settings = cli_module._build_live_model_alpha_runtime_settings(
        daemon_settings=daemon_settings,
        live_defaults=live_defaults,
        strategy_defaults=strategy_defaults,
    )

    assert settings.tf == "1m"


def test_resolve_v5_reconcile_interval_ms_uses_one_minute_for_v5_fusion() -> None:
    assert cli_module._resolve_v5_reconcile_interval_ms(
        model_family="train_v5_fusion",
        strategy_tf="1m",
    ) == 60_000


def test_resolve_v5_reconcile_interval_ms_leaves_legacy_family_at_five_minutes() -> None:
    assert cli_module._resolve_v5_reconcile_interval_ms(
        model_family="train_v4_crypto_cs",
        strategy_tf="1m",
    ) == 300_000
