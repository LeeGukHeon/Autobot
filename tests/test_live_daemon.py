from __future__ import annotations

import asyncio
from pathlib import Path
import pytest

import autobot.live.daemon as daemon_module
from autobot.live.daemon import (
    LiveDaemonSettings,
    run_live_sync_daemon,
    run_live_sync_daemon_with_executor_events,
    run_live_sync_daemon_with_private_ws,
)
from autobot.live.state_store import LiveStateStore
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
            ),
        )

    assert summary["halted"] is False
    assert ("bot-1", "AUTOBOT-autobot-001-intent-1-123-abc") in client.cancel_calls


def test_live_daemon_private_ws_updates_state(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(daemon_module.time, "sleep", lambda _: None)
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        client = _FakePrivateClient()
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
