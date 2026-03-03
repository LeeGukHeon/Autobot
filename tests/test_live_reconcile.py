from __future__ import annotations

from pathlib import Path

from autobot.live.reconcile import reconcile_exchange_snapshot
from autobot.live.state_store import LiveStateStore, OrderRecord


def test_reconcile_halts_on_unknown_external_open_order(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        report = reconcile_exchange_snapshot(
            store=store,
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            accounts_payload=[],
            open_orders_payload=[
                {
                    "uuid": "ex-1",
                    "identifier": "MANUAL-ORDER-1",
                    "market": "KRW-BTC",
                    "state": "wait",
                }
            ],
            unknown_open_orders_policy="halt",
            unknown_positions_policy="halt",
            dry_run=True,
        )

    assert report["halted"] is True
    assert "UNKNOWN_OPEN_ORDERS_DETECTED" in report["halted_reasons"]


def test_reconcile_imports_unknown_position_as_unmanaged(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        report = reconcile_exchange_snapshot(
            store=store,
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            accounts_payload=[
                {
                    "currency": "BTC",
                    "balance": "0.01000000",
                    "locked": "0",
                    "avg_buy_price": "100000000",
                }
            ],
            open_orders_payload=[],
            unknown_open_orders_policy="ignore",
            unknown_positions_policy="import_as_unmanaged",
            dry_run=False,
        )
        positions = store.list_positions()

    assert report["halted"] is False
    assert len(positions) == 1
    assert positions[0]["market"] == "KRW-BTC"
    assert positions[0]["managed"] is False


def test_reconcile_closes_local_only_open_order(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_order(
            OrderRecord(
                uuid="local-1",
                identifier="AUTOBOT-autobot-001-intent-1-1000-a",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=100000000.0,
                volume_req=0.01,
                volume_filled=0.0,
                state="wait",
                created_ts=1000,
                updated_ts=1000,
            )
        )

        report = reconcile_exchange_snapshot(
            store=store,
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            accounts_payload=[],
            open_orders_payload=[],
            unknown_open_orders_policy="ignore",
            unknown_positions_policy="halt",
            dry_run=False,
        )
        orders = store.list_orders(open_only=False)

    assert report["halted"] is False
    assert orders[0]["uuid"] == "local-1"
    assert orders[0]["state"] == "cancel"


def test_reconcile_cancel_policy_creates_bot_cancel_action(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        report = reconcile_exchange_snapshot(
            store=store,
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            accounts_payload=[],
            open_orders_payload=[
                {
                    "uuid": "bot-1",
                    "identifier": "AUTOBOT-autobot-001-intent-1-123-abc",
                    "market": "KRW-BTC",
                    "side": "bid",
                    "ord_type": "limit",
                    "state": "wait",
                }
            ],
            unknown_open_orders_policy="cancel",
            unknown_positions_policy="halt",
            dry_run=True,
        )

    action_types = {item["type"] for item in report["actions"] if isinstance(item, dict)}
    assert "cancel_bot_open_order" in action_types


def test_reconcile_cancel_external_requires_opt_in(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        report = reconcile_exchange_snapshot(
            store=store,
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            accounts_payload=[],
            open_orders_payload=[
                {
                    "uuid": "manual-1",
                    "identifier": "MANUAL-ORDER-1",
                    "market": "KRW-BTC",
                    "state": "wait",
                }
            ],
            unknown_open_orders_policy="cancel",
            unknown_positions_policy="halt",
            allow_cancel_external_orders=False,
            dry_run=True,
        )

    assert report["halted"] is True
    assert "EXTERNAL_OPEN_ORDERS_CANCEL_BLOCKED" in report["halted_reasons"]


def test_reconcile_attach_default_risk_sets_policy_json(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        reconcile_exchange_snapshot(
            store=store,
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            accounts_payload=[
                {
                    "currency": "ETH",
                    "balance": "0.01000000",
                    "locked": "0",
                    "avg_buy_price": "3000000",
                }
            ],
            open_orders_payload=[],
            unknown_open_orders_policy="ignore",
            unknown_positions_policy="attach_default_risk",
            default_risk_sl_pct=2.5,
            default_risk_tp_pct=4.0,
            default_risk_trailing_enabled=True,
            dry_run=False,
        )
        positions = store.list_positions()

    assert len(positions) == 1
    assert positions[0]["managed"] is True
    assert positions[0]["sl"]["sl_pct"] == 2.5
    assert positions[0]["tp"]["tp_pct"] == 4.0
    assert positions[0]["trailing"]["enabled"] is True


def test_reconcile_infers_intent_from_exchange_bot_order(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        reconcile_exchange_snapshot(
            store=store,
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            accounts_payload=[],
            open_orders_payload=[
                {
                    "uuid": "bot-2",
                    "identifier": "AUTOBOT-autobot-001-intent-2-123-abc",
                    "market": "KRW-BTC",
                    "side": "bid",
                    "ord_type": "limit",
                    "price": "100000000",
                    "volume": "0.01",
                    "state": "wait",
                }
            ],
            unknown_open_orders_policy="ignore",
            unknown_positions_policy="halt",
            dry_run=False,
        )
        order = store.order_by_uuid(uuid="bot-2")
        intents = store.list_intents()

    assert order is not None
    assert str(order["intent_id"]).startswith("inferred-bot-2")
    assert any(str(item["intent_id"]).startswith("inferred-bot-2") for item in intents)
