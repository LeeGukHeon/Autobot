from __future__ import annotations

import asyncio
import json
from pathlib import Path
import pytest
import time

import autobot.live.daemon as daemon_module
from autobot.live.breakers import ACTION_FULL_KILL_SWITCH, ACTION_HALT_NEW_INTENTS, arm_breaker, breaker_status
from autobot.live.daemon import (
    LiveDaemonSettings,
    run_live_sync_daemon,
    run_live_sync_daemon_with_executor_events,
    run_live_sync_daemon_with_private_ws,
)
from autobot.live.state_store import LiveStateStore, OrderRecord, PositionRecord
from autobot.upbit.ws import MyAssetEvent, MyOrderEvent


class _FakePrivateClient:
    def __init__(self) -> None:
        self._open_orders_calls = 0
        self.cancel_calls: list[tuple[str | None, str | None]] = []

    def accounts(self):  # noqa: ANN201
        return [
            {
                "currency": "BTC",
                "balance": "0.01000000",
                "locked": "0",
                "avg_buy_price": "100000000",
            }
        ]

    def open_orders(self, *, states):  # noqa: ANN201
        _ = states
        self._open_orders_calls += 1
        if self._open_orders_calls == 1:
            return [
                {
                    "uuid": "bot-1",
                    "identifier": "AUTOBOT-autobot-001-intent-1-123-abc",
                    "market": "KRW-BTC",
                    "side": "bid",
                    "ord_type": "limit",
                    "price": "100000000",
                    "volume": "0.01",
                    "state": "wait",
                }
            ]
        return []

    def order(self, *, uuid: str | None = None, identifier: str | None = None):  # noqa: ANN201
        _ = identifier
        if uuid == "bot-1":
            return {
                "uuid": "bot-1",
                "identifier": "AUTOBOT-autobot-001-intent-1-123-abc",
                "market": "KRW-BTC",
                "state": "cancel",
            }
        return {}

    def cancel_order(self, *, uuid: str | None = None, identifier: str | None = None):  # noqa: ANN201
        self.cancel_calls.append((uuid, identifier))
        return {"ok": True, "uuid": uuid, "identifier": identifier}


class _FakePrivateWsClient:
    def __init__(self) -> None:
        self._events = [
            MyOrderEvent(
                ts_ms=1700000000100,
                uuid="bot-1",
                identifier="AUTOBOT-autobot-001-intent-1-123-abc",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                state="wait",
                price=100000000.0,
                volume=0.01,
                executed_volume=0.0,
            ),
            MyAssetEvent(
                ts_ms=1700000000200,
                currency="BTC",
                balance=0.01,
                locked=0.0,
                avg_buy_price=100000000.0,
            ),
        ]
        self._stats = {
            "reconnect_count": 0,
            "received_events": 2,
            "last_event_ts_ms": 1700000000200,
            "last_event_latency_ms": 1,
        }

    @property
    def stats(self):  # noqa: ANN201
        return dict(self._stats)

    async def stream_private(self, *, channels=("myOrder", "myAsset")):  # noqa: ANN201
        _ = channels
        for event in self._events:
            yield event
        while True:
            await asyncio.sleep(0.05)
            yield MyAssetEvent(
                ts_ms=1700000000200,
                currency="BTC",
                balance=0.01,
                locked=0.0,
                avg_buy_price=100000000.0,
            )


class _FakeExecutorGateway:
    def stream_events(self):  # noqa: ANN201
        yield {
            "event_type": "ORDER_UPDATE",
            "ts_ms": 1700000000100,
            "payload": {
                "uuid": "bot-exec-1",
                "identifier": "AUTOBOT-autobot-001-intent-exec-1-123-abc",
                "market": "KRW-BTC",
                "side": "bid",
                "ord_type": "limit",
                "state": "wait",
                "price": "100000000",
                "volume": "0.01",
                "executed_volume": "0",
            },
        }
        yield {
            "event_type": "ASSET",
            "ts_ms": 1700000000200,
            "payload": {
                "currency": "BTC",
                "balance": "0.01",
                "locked": "0",
                "avg_buy_price": "100000000",
            },
        }
        while True:
            time.sleep(0.05)
            yield {
                "event_type": "HEALTH",
                "ts_ms": 1700000000300,
                "payload": {
                    "message": "ok",
                },
            }


class _CancelRejectPrivateWsClient:
    stats = {"received_events": 1}

    async def stream_private(self, *, channels=("myOrder", "myAsset")):  # noqa: ANN201
        _ = channels
        yield MyOrderEvent(
            ts_ms=1700000000100,
            uuid="bot-1",
            identifier="AUTOBOT-autobot-001-intent-1-123-abc",
            market="KRW-BTC",
            side="bid",
            ord_type="limit",
            state="cancel_reject",
            price=100000000.0,
            volume=0.01,
            executed_volume=0.0,
        )
        while True:
            await asyncio.sleep(10)


class _CancelRejectExecutorGateway:
    def stream_events(self):  # noqa: ANN201
        yield {
            "event_type": "ORDER_UPDATE",
            "ts_ms": 1700000000100,
            "payload": {
                "event_name": "ORDER_STATE",
                "uuid": "bot-1",
                "identifier": "AUTOBOT-autobot-001-intent-1-123-abc",
                "market": "KRW-BTC",
                "side": "bid",
                "ord_type": "limit",
                "state": "cancel_reject",
                "price": "100000000",
                "volume": "0.01",
                "executed_volume": "0",
            },
        }
        while True:
            time.sleep(10)


class _IdentifierCollisionPrivateWsClient:
    stats = {"received_events": 1}

    async def stream_private(self, *, channels=("myOrder", "myAsset")):  # noqa: ANN201
        _ = channels
        yield MyOrderEvent(
            ts_ms=1700000000100,
            uuid="bot-2",
            identifier="AUTOBOT-autobot-001-intent-1-123-abc",
            market="KRW-BTC",
            side="bid",
            ord_type="limit",
            state="wait",
            price=100000000.0,
            volume=0.01,
            executed_volume=0.0,
        )
        while True:
            await asyncio.sleep(10)


class _IdentifierCollisionExecutorGateway:
    def stream_events(self):  # noqa: ANN201
        yield {
            "event_type": "ORDER_UPDATE",
            "ts_ms": 1700000000100,
            "payload": {
                "event_name": "ORDER_STATE",
                "uuid": "bot-2",
                "identifier": "AUTOBOT-autobot-001-intent-1-123-abc",
                "market": "KRW-BTC",
                "side": "bid",
                "ord_type": "limit",
                "state": "wait",
                "price": "100000000",
                "volume": "0.01",
                "executed_volume": "0",
            },
        }
        while True:
            time.sleep(10)


class _FakeUnknownExternalClient(_FakePrivateClient):
    def open_orders(self, *, states):  # noqa: ANN201
        _ = states
        return [
            {
                "uuid": "bot-1",
                "identifier": "AUTOBOT-autobot-001-intent-1-123-abc",
                "market": "KRW-BTC",
                "side": "bid",
                "ord_type": "limit",
                "price": "100000000",
                "volume": "0.01",
                "state": "wait",
            },
            {
                "uuid": "manual-1",
                "identifier": "MANUAL-ORDER-1",
                "market": "KRW-BTC",
                "side": "bid",
                "ord_type": "limit",
                "price": "100000000",
                "volume": "0.01",
                "state": "wait",
            },
        ]


class _FakePositionMissingClient(_FakePrivateClient):
    def accounts(self):  # noqa: ANN201
        return []

    def open_orders(self, *, states):  # noqa: ANN201
        _ = states
        return []


class _FakeRateLimitClient(_FakePrivateClient):
    def accounts(self):  # noqa: ANN201
        from autobot.upbit.exceptions import RateLimitError

        raise RateLimitError("too many requests", status_code=429)


class _FakeMultiPositionClient(_FakePrivateClient):
    def accounts(self):  # noqa: ANN201
        return [
            {
                "currency": "BTC",
                "balance": "0.01000000",
                "locked": "0",
                "avg_buy_price": "100000000",
            },
            {
                "currency": "ETH",
                "balance": "0.02000000",
                "locked": "0",
                "avg_buy_price": "4000000",
            },
        ]

    def open_orders(self, *, states):  # noqa: ANN201
        _ = states
        return []


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _runtime_contract_settings(tmp_path: Path, *, ws_updated_at_ms: int | None = None) -> dict[str, object]:
    registry_root = tmp_path / "models" / "registry"
    family_dir = registry_root / "train_v4_crypto_cs"
    run_id = "run-current"
    (family_dir / run_id).mkdir(parents=True, exist_ok=True)
    _write_json(family_dir / "champion.json", {"run_id": run_id})

    now_ms = int(time.time() * 1000) if ws_updated_at_ms is None else int(ws_updated_at_ms)
    ws_raw_root = tmp_path / "data" / "raw_ws" / "upbit" / "public"
    ws_meta_dir = tmp_path / "data" / "raw_ws" / "upbit" / "_meta"
    _write_json(
        ws_meta_dir / "ws_public_health.json",
        {
            "run_id": "ws-run-current",
            "updated_at_ms": now_ms,
            "connected": True,
            "last_rx_ts_ms": {"trade": now_ms, "orderbook": now_ms},
            "subscribed_markets_count": 10,
        },
    )
    _write_json(
        ws_meta_dir / "ws_collect_report.json",
        {
            "run_id": "ws-collect-current",
            "generated_at": "2026-03-09T00:00:00+00:00",
        },
    )
    _write_json(
        ws_meta_dir / "ws_validate_report.json",
        {
            "run_id": "ws-validate-current",
            "generated_at": "2026-03-09T00:00:00+00:00",
            "checked_files": 1,
            "ok_files": 1,
            "warn_files": 0,
            "fail_files": 0,
        },
    )
    _write_json(
        ws_meta_dir / "ws_runs_summary.json",
        {
            "runs": [
                {
                    "run_id": "ws-run-current",
                    "parts": 1,
                    "rows_total": 10,
                    "bytes_total": 100,
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
            "run_id": "micro-run-current",
            "start": "2026-03-08",
            "end": "2026-03-08",
            "rows_written_total": 10,
        },
    )
    return {
        "registry_root": str(registry_root),
        "runtime_model_ref_source": "champion_v4",
        "runtime_model_family": "train_v4_crypto_cs",
        "ws_public_raw_root": str(ws_raw_root),
        "ws_public_meta_dir": str(ws_meta_dir),
        "ws_public_stale_threshold_sec": 180,
        "micro_aggregate_report_path": str(micro_report_path),
    }


def test_live_daemon_polling_updates_state(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        client = _FakePrivateClient()
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
                max_cycles=2,
                startup_reconcile=True,
                **_runtime_contract_settings(tmp_path),
            ),
        )

        positions = store.list_positions()
        intents = store.list_intents()
        checkpoints = store.export_state()["checkpoints"]

    assert summary["halted"] is False
    assert summary["cycles"] == 2
    assert len(positions) == 1
    assert len(intents) >= 1
    assert any(item["name"] == "last_sync" for item in checkpoints)
    assert summary["resume_report"] is not None
    assert any(item["name"] == "last_resume" for item in checkpoints)


def test_live_daemon_cancel_policy_executes_bot_cancel(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        client = _FakePrivateClient()
        summary = run_live_sync_daemon(
            store=store,
            client=client,
            settings=LiveDaemonSettings(
                bot_id="autobot-001",
                identifier_prefix="AUTOBOT",
                unknown_open_orders_policy="cancel",
                unknown_positions_policy="import_as_unmanaged",
                allow_cancel_external_orders=False,
                allow_cancel_external_cli=False,
                poll_interval_sec=1,
                max_cycles=1,
                startup_reconcile=True,
                **_runtime_contract_settings(tmp_path),
            ),
        )

    assert summary["halted"] is False
    assert ("bot-1", "AUTOBOT-autobot-001-intent-1-123-abc") in client.cancel_calls


def test_live_daemon_private_ws_updates_state(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        client = _FakeMultiPositionClient()
        ws_client = _FakePrivateWsClient()
        summary = asyncio.run(
            run_live_sync_daemon_with_private_ws(
                store=store,
                client=client,
                ws_client=ws_client,
                settings=LiveDaemonSettings(
                    bot_id="autobot-001",
                    identifier_prefix="AUTOBOT",
                    unknown_open_orders_policy="ignore",
                    unknown_positions_policy="import_as_unmanaged",
                    allow_cancel_external_orders=False,
                    poll_interval_sec=1,
                    startup_reconcile=True,
                    duration_sec=1,
                    use_private_ws=True,
                    **_runtime_contract_settings(tmp_path),
                ),
            )
        )
        order = store.order_by_uuid(uuid="bot-1")
        position = store.position_by_market(market="KRW-BTC")
        checkpoints = store.export_state()["checkpoints"]

    assert summary["halted"] is False
    assert summary["ws_events"] >= 1
    assert order is not None
    assert position is not None
    assert any(item["name"] == "last_ws_event" for item in checkpoints)


def test_live_daemon_executor_events_updates_state(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        client = _FakePrivateClient()
        executor_gateway = _FakeExecutorGateway()
        summary = run_live_sync_daemon_with_executor_events(
            store=store,
            client=client,
            executor_gateway=executor_gateway,
            settings=LiveDaemonSettings(
                bot_id="autobot-001",
                identifier_prefix="AUTOBOT",
                unknown_open_orders_policy="ignore",
                unknown_positions_policy="import_as_unmanaged",
                allow_cancel_external_orders=False,
                poll_interval_sec=1,
                startup_reconcile=True,
                duration_sec=1,
                use_executor_ws=True,
                **_runtime_contract_settings(tmp_path),
            ),
        )
        order = store.order_by_uuid(uuid="bot-exec-1")
        position = store.position_by_market(market="KRW-BTC")
        checkpoints = store.export_state()["checkpoints"]

    assert summary["halted"] is False
    assert summary["executor_events"] >= 1
    assert order is not None
    assert position is not None
    assert any(item["name"] == "last_executor_event" for item in checkpoints)


def test_live_daemon_private_ws_halts_immediately_when_event_arms_breaker(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_order(
            OrderRecord(
                uuid="bot-1",
                identifier="AUTOBOT-autobot-001-intent-1-123-abc",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=100000000.0,
                volume_req=0.01,
                volume_filled=0.0,
                state="wait",
                created_ts=1000,
                updated_ts=1000,
                intent_id="intent-1",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="bot-1",
            )
        )
        client = _FakePrivateClient()
        summary = asyncio.run(
            run_live_sync_daemon_with_private_ws(
                store=store,
                client=client,
                ws_client=_CancelRejectPrivateWsClient(),
                settings=LiveDaemonSettings(
                    bot_id="autobot-001",
                    identifier_prefix="AUTOBOT",
                    unknown_open_orders_policy="ignore",
                    unknown_positions_policy="import_as_unmanaged",
                    allow_cancel_external_orders=False,
                    poll_interval_sec=1,
                    startup_reconcile=False,
                    duration_sec=1,
                    use_private_ws=True,
                    breaker_cancel_reject_limit=1,
                    **_runtime_contract_settings(tmp_path),
                ),
            )
        )

    assert summary["halted"] is True
    assert "REPEATED_CANCEL_REJECTS" in summary["halted_reasons"]
    assert client.cancel_calls == []
    assert summary["last_breaker_cancel_summary"] is None


def test_live_daemon_private_ws_cancels_immediately_on_identifier_collision_full_kill(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_order(
            OrderRecord(
                uuid="bot-1",
                identifier="AUTOBOT-autobot-001-intent-1-123-abc",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=100000000.0,
                volume_req=0.01,
                volume_filled=0.0,
                state="wait",
                created_ts=1000,
                updated_ts=1000,
                intent_id="intent-1",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="bot-1",
            )
        )
        client = _FakePrivateClient()
        summary = asyncio.run(
            run_live_sync_daemon_with_private_ws(
                store=store,
                client=client,
                ws_client=_IdentifierCollisionPrivateWsClient(),
                settings=LiveDaemonSettings(
                    bot_id="autobot-001",
                    identifier_prefix="AUTOBOT",
                    unknown_open_orders_policy="ignore",
                    unknown_positions_policy="import_as_unmanaged",
                    allow_cancel_external_orders=False,
                    poll_interval_sec=1,
                    startup_reconcile=False,
                    duration_sec=1,
                    use_private_ws=True,
                    **_runtime_contract_settings(tmp_path),
                ),
            )
        )

    assert summary["halted"] is True
    assert "IDENTIFIER_COLLISION" in summary["halted_reasons"]
    assert ("bot-1", "AUTOBOT-autobot-001-intent-1-123-abc") in client.cancel_calls
    assert summary["last_breaker_cancel_summary"] is not None
    assert summary["last_breaker_cancel_summary"]["executed"] == 1


def test_live_daemon_executor_halts_immediately_when_event_arms_breaker(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_order(
            OrderRecord(
                uuid="bot-1",
                identifier="AUTOBOT-autobot-001-intent-1-123-abc",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=100000000.0,
                volume_req=0.01,
                volume_filled=0.0,
                state="wait",
                created_ts=1000,
                updated_ts=1000,
                intent_id="intent-1",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="bot-1",
            )
        )
        client = _FakePrivateClient()
        summary = run_live_sync_daemon_with_executor_events(
            store=store,
            client=client,
            executor_gateway=_CancelRejectExecutorGateway(),
            settings=LiveDaemonSettings(
                bot_id="autobot-001",
                identifier_prefix="AUTOBOT",
                unknown_open_orders_policy="ignore",
                unknown_positions_policy="import_as_unmanaged",
                allow_cancel_external_orders=False,
                poll_interval_sec=1,
                startup_reconcile=False,
                duration_sec=1,
                use_executor_ws=True,
                breaker_cancel_reject_limit=1,
                **_runtime_contract_settings(tmp_path),
            ),
        )

    assert summary["halted"] is True
    assert "REPEATED_CANCEL_REJECTS" in summary["halted_reasons"]
    assert client.cancel_calls == []
    assert summary["last_breaker_cancel_summary"] is None


def test_live_daemon_executor_cancels_immediately_on_identifier_collision_full_kill(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_order(
            OrderRecord(
                uuid="bot-1",
                identifier="AUTOBOT-autobot-001-intent-1-123-abc",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=100000000.0,
                volume_req=0.01,
                volume_filled=0.0,
                state="wait",
                created_ts=1000,
                updated_ts=1000,
                intent_id="intent-1",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="bot-1",
            )
        )
        client = _FakePrivateClient()
        summary = run_live_sync_daemon_with_executor_events(
            store=store,
            client=client,
            executor_gateway=_IdentifierCollisionExecutorGateway(),
            settings=LiveDaemonSettings(
                bot_id="autobot-001",
                identifier_prefix="AUTOBOT",
                unknown_open_orders_policy="ignore",
                unknown_positions_policy="import_as_unmanaged",
                allow_cancel_external_orders=False,
                poll_interval_sec=1,
                startup_reconcile=False,
                duration_sec=1,
                use_executor_ws=True,
                **_runtime_contract_settings(tmp_path),
            ),
        )

    assert summary["halted"] is True
    assert "IDENTIFIER_COLLISION" in summary["halted_reasons"]
    assert ("bot-1", "AUTOBOT-autobot-001-intent-1-123-abc") in client.cancel_calls
    assert summary["last_breaker_cancel_summary"] is not None
    assert summary["last_breaker_cancel_summary"]["executed"] == 1


def test_apply_executor_event_supports_payload_event_name_contract(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        action = daemon_module._apply_executor_event(
            store=store,
            event={
                "event_type": "HEALTH",
                "ts_ms": 1700000000300,
                "payload": {
                    "event_name": "ORDER_STATE",
                    "uuid": "bot-contract-1",
                    "identifier": "AUTOBOT-autobot-001-intent-contract-1-123-abc",
                    "market": "KRW-BTC",
                    "side": "bid",
                    "ord_type": "limit",
                    "state": "wait",
                    "price": "100000000",
                    "volume": "0.01",
                    "executed_volume": "0",
                },
            },
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            quote_currency="KRW",
        )

        order = store.order_by_uuid(uuid="bot-contract-1")

    assert action["type"] == "ws_order_upsert"
    assert order is not None
    assert order["market"] == "KRW-BTC"
    assert order["local_state"] == "OPEN"


def test_apply_executor_event_supports_timeout_and_replaced_contract(tmp_path: Path) -> None:
    timeout_action = daemon_module._apply_executor_event(
        store=None,  # type: ignore[arg-type]
        event={
            "event_type": "ORDER_UPDATE",
            "ts_ms": 1700000000400,
            "payload": {
                "event_name": "ORDER_TIMEOUT",
                "identifier": "AUTOBOT-timeout-1",
                "uuid": "timeout-uuid-1",
            },
        },
        bot_id="autobot-001",
        identifier_prefix="AUTOBOT",
        quote_currency="KRW",
    )

    with LiveStateStore(tmp_path / "replace_state.db") as store:
        replaced_action = daemon_module._apply_executor_event(
            store=store,
            event={
                "event_type": "ORDER_UPDATE",
                "ts_ms": 1700000000500,
                "payload": {
                    "event_name": "ORDER_REPLACED",
                    "prev_uuid": "prev-1",
                    "prev_identifier": "AUTOBOT-prev",
                    "new_uuid": "new-1",
                    "new_identifier": "AUTOBOT-new",
                },
            },
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            quote_currency="KRW",
        )
        checkpoints = store.export_state()["checkpoints"]
        order_lineage = store.export_state()["order_lineage"]
        replaced_order = store.order_by_uuid(uuid="new-1")

    assert timeout_action["type"] == "executor_order_timeout"
    assert replaced_action["type"] == "executor_order_replaced"
    assert any(item["name"] == "last_replace_chain" for item in checkpoints)
    assert len(order_lineage) == 1
    assert order_lineage[0]["new_uuid"] == "new-1"
    assert replaced_order is not None
    assert replaced_order["local_state"] == "REPLACING"


def test_apply_executor_event_with_breakers_halts_on_replace_persist_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with LiveStateStore(tmp_path / "replace_breaker.db") as store:
        def _raise_persist_error(*args, **kwargs) -> None:  # noqa: ANN002, ANN003
            raise RuntimeError("database is locked")

        monkeypatch.setattr(store, "append_order_lineage", _raise_persist_error)

        action = daemon_module._apply_executor_event_with_breakers(
            store=store,
            event={
                "event_type": "ORDER_UPDATE",
                "ts_ms": 1700000000600,
                "payload": {
                    "event_name": "ORDER_REPLACED",
                    "prev_uuid": "prev-2",
                    "prev_identifier": "AUTOBOT-prev-2",
                    "new_uuid": "new-2",
                    "new_identifier": "AUTOBOT-new-2",
                },
            },
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            quote_currency="KRW",
            settings=LiveDaemonSettings(
                bot_id="autobot-001",
                identifier_prefix="AUTOBOT",
                unknown_open_orders_policy="ignore",
                unknown_positions_policy="import_as_unmanaged",
                allow_cancel_external_orders=False,
                poll_interval_sec=1,
                use_executor_ws=True,
            ),
        )
        report = breaker_status(store)

    assert action["type"] == "executor_breaker_replace_persist_failed"
    assert "EXECUTOR_REPLACE_PERSIST_FAILED" in action["error"]
    assert report["active"] is True
    assert report["action"] == ACTION_HALT_NEW_INTENTS
    assert "EXECUTOR_REPLACE_PERSIST_FAILED" in report["reason_codes"]


def test_private_ws_terminal_event_resets_replace_reject_counter(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        daemon_module._apply_private_ws_event_with_breakers(
            store=store,
            event=MyOrderEvent(
                ts_ms=1000,
                uuid="replace-1",
                identifier="AUTOBOT-autobot-001-intent-replace-1-1000-a",
                market="KRW-BTC",
                side="ask",
                ord_type="limit",
                state="replace_reject",
                price=100000000.0,
                volume=0.01,
                executed_volume=0.0,
            ),
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            quote_currency="KRW",
            settings=LiveDaemonSettings(
                bot_id="autobot-001",
                identifier_prefix="AUTOBOT",
                unknown_open_orders_policy="ignore",
                unknown_positions_policy="import_as_unmanaged",
                allow_cancel_external_orders=False,
                poll_interval_sec=1,
                use_private_ws=True,
            ),
        )
        daemon_module._apply_private_ws_event_with_breakers(
            store=store,
            event=MyOrderEvent(
                ts_ms=2000,
                uuid="replace-1",
                identifier="AUTOBOT-autobot-001-intent-replace-1-1000-a",
                market="KRW-BTC",
                side="ask",
                ord_type="limit",
                state="done",
                price=100000000.0,
                volume=0.01,
                executed_volume=0.01,
            ),
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            quote_currency="KRW",
            settings=LiveDaemonSettings(
                bot_id="autobot-001",
                identifier_prefix="AUTOBOT",
                unknown_open_orders_policy="ignore",
                unknown_positions_policy="import_as_unmanaged",
                allow_cancel_external_orders=False,
                poll_interval_sec=1,
                use_private_ws=True,
            ),
        )
        counter = store.get_checkpoint(name="breaker_counter:replace_reject")

    assert counter is not None
    assert counter["payload"]["count"] == 0


def test_executor_terminal_event_resets_replace_reject_counter(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    settings = LiveDaemonSettings(
        bot_id="autobot-001",
        identifier_prefix="AUTOBOT",
        unknown_open_orders_policy="ignore",
        unknown_positions_policy="import_as_unmanaged",
        allow_cancel_external_orders=False,
        poll_interval_sec=1,
        use_executor_ws=True,
    )
    with LiveStateStore(db_path) as store:
        daemon_module._apply_executor_event_with_breakers(
            store=store,
            event={
                "event_type": "ORDER_UPDATE",
                "ts_ms": 1000,
                "payload": {
                    "event_name": "ORDER_STATE",
                    "uuid": "replace-1",
                    "identifier": "AUTOBOT-autobot-001-intent-replace-1-1000-a",
                    "market": "KRW-BTC",
                    "side": "ask",
                    "ord_type": "limit",
                    "state": "replace_reject",
                },
            },
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            quote_currency="KRW",
            settings=settings,
        )
        daemon_module._apply_executor_event_with_breakers(
            store=store,
            event={
                "event_type": "ORDER_UPDATE",
                "ts_ms": 2000,
                "payload": {
                    "event_name": "ORDER_STATE",
                    "uuid": "replace-1",
                    "identifier": "AUTOBOT-autobot-001-intent-replace-1-1000-a",
                    "market": "KRW-BTC",
                    "side": "ask",
                    "ord_type": "limit",
                    "state": "done",
                },
            },
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            quote_currency="KRW",
            settings=settings,
        )
        counter = store.get_checkpoint(name="breaker_counter:replace_reject")

    assert counter is not None
    assert counter["payload"]["count"] == 0


def test_live_daemon_settings_reject_dual_ws_sources() -> None:
    with pytest.raises(ValueError, match="cannot both be true"):
        LiveDaemonSettings(
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            unknown_open_orders_policy="ignore",
            unknown_positions_policy="import_as_unmanaged",
            allow_cancel_external_orders=False,
            poll_interval_sec=60,
            use_private_ws=True,
            use_executor_ws=True,
        )


def test_live_daemon_halts_and_cancels_bot_orders_on_unknown_external_order(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        client = _FakeUnknownExternalClient()
        summary = run_live_sync_daemon(
            store=store,
            client=client,
            settings=LiveDaemonSettings(
                bot_id="autobot-001",
                identifier_prefix="AUTOBOT",
                unknown_open_orders_policy="halt",
                unknown_positions_policy="import_as_unmanaged",
                allow_cancel_external_orders=False,
                poll_interval_sec=1,
                max_cycles=1,
                startup_reconcile=True,
                **_runtime_contract_settings(tmp_path),
            ),
        )

    assert summary["halted"] is True
    assert "UNKNOWN_OPEN_ORDERS_DETECTED" in summary["halted_reasons"]
    assert summary["breaker_report"]["action"] == "HALT_AND_CANCEL_BOT_ORDERS"
    assert ("bot-1", "AUTOBOT-autobot-001-intent-1-123-abc") in client.cancel_calls
    assert ("manual-1", "MANUAL-ORDER-1") not in client.cancel_calls


def test_live_daemon_classifies_local_position_missing_on_exchange_as_manual_sell(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-BTC",
                base_currency="BTC",
                base_amount=0.01,
                avg_entry_price=100000000.0,
                updated_ts=1000,
                managed=False,
            )
        )
        client = _FakePositionMissingClient()
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
                startup_reconcile=True,
                **_runtime_contract_settings(tmp_path),
            ),
        )

    assert summary["halted"] is False
    actions = summary["last_report"]["actions"]
    assert any(item["type"] == "close_position_as_manual_sell" for item in actions)


def test_live_daemon_halts_on_managed_position_missing_without_close_evidence(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-BTC",
                base_currency="BTC",
                base_amount=0.01,
                avg_entry_price=100000000.0,
                updated_ts=1000,
                managed=True,
            )
        )
        client = _FakePositionMissingClient()
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
                startup_reconcile=True,
                **_runtime_contract_settings(tmp_path),
            ),
        )

    assert summary["halted"] is True
    assert "LOCAL_POSITION_MISSING_ON_EXCHANGE" in summary["halted_reasons"]


def test_live_daemon_single_slot_canary_halts_on_multi_slot_state(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-BTC",
                base_currency="BTC",
                base_amount=0.01,
                avg_entry_price=100000000.0,
                updated_ts=1000,
            )
        )
        store.upsert_position(
            PositionRecord(
                market="KRW-ETH",
                base_currency="ETH",
                base_amount=0.02,
                avg_entry_price=4000000.0,
                updated_ts=1001,
            )
        )
        client = _FakeMultiPositionClient()
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
                startup_reconcile=True,
                small_account_canary_enabled=True,
                small_account_max_positions=1,
                small_account_max_open_orders_per_market=1,
                **_runtime_contract_settings(tmp_path),
            ),
        )

    assert summary["halted"] is True
    assert "SMALL_ACCOUNT_CANARY_MAX_POSITIONS_EXCEEDED" in summary["halted_reasons"]
    assert summary["small_account_report"] is not None
    assert (tmp_path / "live_small_account_report.json").exists()


def test_live_daemon_arms_manual_kill_switch_before_cycles(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        arm_breaker(
            store,
            reason_codes=["MANUAL_KILL_SWITCH"],
            source="test",
            ts_ms=1000,
            action=ACTION_FULL_KILL_SWITCH,
        )
        client = _FakePrivateClient()
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
                startup_reconcile=True,
                **_runtime_contract_settings(tmp_path),
            ),
        )

    assert summary["halted"] is True
    assert summary["halted_reasons"] == ["MANUAL_KILL_SWITCH"]


def test_live_daemon_repeated_rate_limit_arms_breaker(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        client = _FakeRateLimitClient()
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
                max_cycles=3,
                startup_reconcile=True,
                breaker_rate_limit_error_limit=2,
                **_runtime_contract_settings(tmp_path),
            ),
        )

    assert summary["halted"] is True
    assert "REPEATED_RATE_LIMIT_ERRORS" in summary["halted_reasons"]


def test_live_daemon_halts_when_private_ws_stream_ends(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)

    class _FiniteWsClient:
        stats = {}

        async def stream_private(self, *, channels=("myOrder", "myAsset")):  # noqa: ANN201
            _ = channels
            if False:
                yield None
            return

    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        client = _FakePrivateClient()
        summary = asyncio.run(
            run_live_sync_daemon_with_private_ws(
                store=store,
                client=client,
                ws_client=_FiniteWsClient(),
                settings=LiveDaemonSettings(
                    bot_id="autobot-001",
                    identifier_prefix="AUTOBOT",
                    unknown_open_orders_policy="ignore",
                    unknown_positions_policy="import_as_unmanaged",
                    allow_cancel_external_orders=False,
                    poll_interval_sec=1,
                    startup_reconcile=True,
                    duration_sec=1,
                    use_private_ws=True,
                    **_runtime_contract_settings(tmp_path),
                ),
            )
        )

    assert summary["halted"] is True
    assert "STALE_PRIVATE_WS_STREAM" in summary["halted_reasons"]
