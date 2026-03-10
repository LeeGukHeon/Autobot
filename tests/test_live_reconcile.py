from __future__ import annotations

import json
from pathlib import Path

from autobot.live.reconcile import reconcile_exchange_snapshot
from autobot.live.state_store import IntentRecord, LiveStateStore, OrderRecord, PositionRecord, RiskPlanRecord


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
        risk_plans = store.list_risk_plans()

    assert len(positions) == 1
    assert positions[0]["managed"] is True
    assert positions[0]["sl"]["sl_pct"] == 2.5
    assert positions[0]["tp"]["tp_pct"] == 4.0
    assert positions[0]["trailing"]["enabled"] is True
    assert len(risk_plans) == 1
    assert risk_plans[0]["market"] == "KRW-ETH"
    assert risk_plans[0]["state"] == "ACTIVE"
    assert risk_plans[0]["tp"]["tp_pct"] == 4.0
    assert risk_plans[0]["sl"]["sl_pct"] == 2.5


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


def test_reconcile_ignores_unknown_dust_position_below_exchange_min_total(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        report = reconcile_exchange_snapshot(
            store=store,
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            accounts_payload=[
                {
                    "currency": "MOODENG",
                    "balance": "0.0081",
                    "locked": "0",
                    "avg_buy_price": "74.1",
                }
            ],
            open_orders_payload=[],
            fetch_market_chance=lambda market: {
                "market": {
                    "bid": {"min_total": "5000"},
                    "ask": {"min_total": "5000"},
                }
            },
            unknown_open_orders_policy="ignore",
            unknown_positions_policy="halt",
            dry_run=True,
        )

    assert report["halted"] is False
    assert report["counts"]["unknown_positions"] == 0
    assert report["counts"]["ignored_dust_positions"] == 1
    assert report["ignored_dust_positions"][0]["market"] == "KRW-MOODENG"
    action_types = {item["type"] for item in report["actions"] if isinstance(item, dict)}
    assert "ignore_unknown_dust_position" in action_types


def test_reconcile_keeps_unknown_position_when_notional_is_above_min_total(tmp_path: Path) -> None:
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
            fetch_market_chance=lambda market: {
                "market": {
                    "bid": {"min_total": "5000"},
                    "ask": {"min_total": "5000"},
                }
            },
            unknown_open_orders_policy="ignore",
            unknown_positions_policy="halt",
            dry_run=True,
        )

    assert report["halted"] is True
    assert "UNKNOWN_POSITIONS_DETECTED" in report["halted_reasons"]
    assert report["counts"]["ignored_dust_positions"] == 0


def test_reconcile_drops_managed_dust_position_and_closes_risk_plan(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-KITE",
                base_currency="KITE",
                base_amount=0.00000001,
                avg_entry_price=443.0,
                updated_ts=1000,
                tp_json=json.dumps({"enabled": False, "source": "model_alpha_v1"}, ensure_ascii=False),
                sl_json=json.dumps({"enabled": False, "source": "model_alpha_v1"}, ensure_ascii=False),
                trailing_json=json.dumps({"enabled": False, "source": "model_alpha_v1"}, ensure_ascii=False),
                managed=True,
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="model-risk-kite-dust",
                market="KRW-KITE",
                side="long",
                entry_price_str="443",
                qty_str="0.00000001",
                tp_enabled=False,
                sl_enabled=False,
                trailing_enabled=False,
                state="ACTIVE",
                last_eval_ts_ms=1000,
                last_action_ts_ms=0,
                replace_attempt=0,
                created_ts=1000,
                updated_ts=1000,
                timeout_ts_ms=1801000,
                plan_source="model_alpha_v1",
                source_intent_id="intent-kite-dust",
            )
        )

        report = reconcile_exchange_snapshot(
            store=store,
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            accounts_payload=[
                {
                    "currency": "KITE",
                    "balance": "0.00000001",
                    "locked": "0",
                    "avg_buy_price": "443",
                }
            ],
            open_orders_payload=[],
            fetch_market_chance=lambda market: {
                "market": {
                    "bid": {"min_total": "5000"},
                    "ask": {"min_total": "5000"},
                }
            },
            unknown_open_orders_policy="ignore",
            unknown_positions_policy="halt",
            dry_run=False,
            ts_ms=5000,
        )
        positions = store.list_positions()
        plans = store.list_risk_plans()

    assert report["halted"] is False
    assert report["counts"]["ignored_dust_positions"] == 1
    assert any(item["type"] == "drop_managed_dust_position" for item in report["actions"])
    assert positions == []
    assert len(plans) == 1
    assert plans[0]["market"] == "KRW-KITE"
    assert plans[0]["state"] == "CLOSED"
    assert plans[0]["plan_source"] == "model_alpha_v1"
    assert plans[0]["source_intent_id"] == "intent-kite-dust"


def test_reconcile_ignores_bot_owned_dust_position_before_managed_import(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        intent_meta = {
            "model_exit_plan": {
                "source": "model_alpha_v1",
                "mode": "hold",
                "hold_bars": 6,
                "timeout_delta_ms": 1800000,
                "tp_pct": 0.02,
                "sl_pct": 0.01,
                "trailing_pct": 0.015,
            },
            "submit_result": {"accepted": True, "order_uuid": "entry-order-dust"},
        }
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-entry-dust",
                ts_ms=1000,
                market="KRW-KITE",
                side="bid",
                price=443.0,
                volume=0.00000001,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                meta_json=json.dumps(intent_meta, ensure_ascii=False, sort_keys=True),
                status="SUBMITTED",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="entry-order-dust",
                identifier="AUTOBOT-autobot-001-intent-entry-dust-1000-a",
                market="KRW-KITE",
                side="bid",
                ord_type="limit",
                price=443.0,
                volume_req=0.00000001,
                volume_filled=0.00000001,
                state="done",
                created_ts=1000,
                updated_ts=1000,
                intent_id="intent-entry-dust",
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="entry-order-dust",
            )
        )

        report = reconcile_exchange_snapshot(
            store=store,
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            accounts_payload=[
                {
                    "currency": "KITE",
                    "balance": "0.00000001",
                    "locked": "0",
                    "avg_buy_price": "443",
                }
            ],
            open_orders_payload=[],
            fetch_market_chance=lambda market: {
                "market": {
                    "bid": {"min_total": "5000"},
                    "ask": {"min_total": "5000"},
                }
            },
            unknown_open_orders_policy="ignore",
            unknown_positions_policy="halt",
            dry_run=False,
            ts_ms=5000,
        )
        positions = store.list_positions()
        plans = store.list_risk_plans()

    assert report["halted"] is False
    assert report["counts"]["unknown_positions"] == 0
    assert report["counts"]["ignored_dust_positions"] == 1
    assert any(item["type"] == "ignore_unknown_dust_position" for item in report["actions"])
    assert not any(item["type"] == "import_managed_position_from_bot_intent" for item in report["actions"])
    assert positions == []
    assert plans == []


def test_reconcile_imports_bot_owned_filled_entry_with_model_risk_plan(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        intent_meta = {
            "runtime": {"live_runtime_model_run_id": "run-1"},
            "model_exit_plan": {
                "source": "model_alpha_v1",
                "mode": "risk",
                "hold_bars": 6,
                "interval_ms": 300000,
                "timeout_delta_ms": 1800000,
                "tp_pct": 0.02,
                "sl_pct": 0.01,
                "trailing_pct": 0.015,
            },
            "submit_result": {"accepted": True, "order_uuid": "entry-order-1"},
        }
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-entry-1",
                ts_ms=1000,
                market="KRW-KITE",
                side="bid",
                price=442.0,
                volume=13.56787669,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                meta_json=json.dumps(intent_meta, ensure_ascii=False, sort_keys=True),
                status="SUBMITTED",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="entry-order-1",
                identifier="AUTOBOT-autobot-001-intent-entry-1-1000-a",
                market="KRW-KITE",
                side="bid",
                ord_type="limit",
                price=442.0,
                volume_req=13.56787669,
                volume_filled=13.56787669,
                state="wait",
                created_ts=1000,
                updated_ts=1000,
                intent_id="intent-entry-1",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="SUBMIT_ACCEPTED",
                event_source="test",
                root_order_uuid="entry-order-1",
            )
        )

        report = reconcile_exchange_snapshot(
            store=store,
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            accounts_payload=[
                {
                    "currency": "KITE",
                    "balance": "13.56787669",
                    "locked": "0",
                    "avg_buy_price": "442",
                }
            ],
            open_orders_payload=[],
            unknown_open_orders_policy="ignore",
            unknown_positions_policy="halt",
            dry_run=False,
            ts_ms=5000,
        )
        positions = store.list_positions()
        plans = store.list_risk_plans()
        order = store.order_by_uuid(uuid="entry-order-1")

    assert report["halted"] is False
    assert any(item["type"] == "import_managed_position_from_bot_intent" for item in report["actions"])
    assert len(positions) == 1
    assert positions[0]["market"] == "KRW-KITE"
    assert positions[0]["managed"] is True
    assert positions[0]["tp"]["tp_pct"] == 2.0
    assert positions[0]["sl"]["sl_pct"] == 1.0
    assert len(plans) == 1
    assert plans[0]["market"] == "KRW-KITE"
    assert plans[0]["plan_source"] == "model_alpha_v1"
    assert plans[0]["source_intent_id"] == "intent-entry-1"
    assert plans[0]["timeout_ts_ms"] == 1801000
    assert plans[0]["tp"]["tp_pct"] == 2.0
    assert plans[0]["sl"]["sl_pct"] == 1.0
    assert plans[0]["trailing"]["trail_pct"] == 0.015
    assert order is not None
    assert order["state"] == "done"


def test_reconcile_imports_managed_position_after_order_detail_sync_preserves_intent(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        intent_meta = {
            "model_exit_plan": {
                "source": "model_alpha_v1",
                "mode": "hold",
                "hold_bars": 12,
                "timeout_delta_ms": 900000,
                "tp_pct": 0.02,
                "sl_pct": 0.01,
                "trailing_pct": 0.015,
            },
            "submit_result": {"accepted": True, "order_uuid": "entry-order-2"},
        }
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-entry-2",
                ts_ms=1000,
                market="KRW-FLOW",
                side="bid",
                price=88.1,
                volume=64.59970922,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                meta_json=json.dumps(intent_meta, ensure_ascii=False, sort_keys=True),
                status="SUBMITTED",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="entry-order-2",
                identifier="AUTOBOT-autobot-001-intent-entry-2-1000-a",
                market="KRW-FLOW",
                side="bid",
                ord_type="limit",
                price=88.1,
                volume_req=64.59970922,
                volume_filled=0.0,
                state="wait",
                created_ts=1000,
                updated_ts=1000,
                intent_id="intent-entry-2",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="SUBMIT_ACCEPTED",
                event_source="test",
                root_order_uuid="entry-order-2",
            )
        )

        report = reconcile_exchange_snapshot(
            store=store,
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            accounts_payload=[
                {
                    "currency": "FLOW",
                    "balance": "64.59970922",
                    "locked": "0",
                    "avg_buy_price": "88.1",
                }
            ],
            open_orders_payload=[],
            fetch_order_detail=lambda uuid, identifier: {
                "uuid": "entry-order-2",
                "identifier": "AUTOBOT-autobot-001-intent-entry-2-1000-a",
                "market": "KRW-FLOW",
                "side": "bid",
                "ord_type": "limit",
                "price": "88.1",
                "volume": "64.59970922",
                "executed_volume": "64.59970922",
                "state": "done",
                "created_at": "2026-03-10T14:00:00+09:00",
            },
            unknown_open_orders_policy="ignore",
            unknown_positions_policy="halt",
            dry_run=False,
            ts_ms=5000,
        )
        positions = store.list_positions()
        plans = store.list_risk_plans()
        order = store.order_by_uuid(uuid="entry-order-2")

    assert report["halted"] is False
    assert any(item["type"] == "sync_local_order_from_detail" for item in report["actions"])
    assert any(item["type"] == "import_managed_position_from_bot_intent" for item in report["actions"])
    assert len(positions) == 1
    assert positions[0]["market"] == "KRW-FLOW"
    assert positions[0]["managed"] is True
    assert len(plans) == 1
    assert plans[0]["market"] == "KRW-FLOW"
    assert plans[0]["plan_source"] == "model_alpha_v1"
    assert plans[0]["source_intent_id"] == "intent-entry-2"
    assert plans[0]["tp"]["tp_pct"] == 2.0
    assert plans[0]["sl"]["sl_pct"] == 1.0
    assert order is not None
    assert order["intent_id"] == "intent-entry-2"
    assert order["state"] == "done"


def test_reconcile_import_preserves_existing_exiting_plan_metadata(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        intent_meta = {
            "model_exit_plan": {
                "source": "model_alpha_v1",
                "mode": "hold",
                "hold_bars": 6,
                "timeout_delta_ms": 1800000,
                "tp_pct": 0.0,
                "sl_pct": 0.0,
                "trailing_pct": 0.0,
            },
            "submit_result": {"accepted": True, "order_uuid": "entry-order-kite"},
        }
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-entry-kite",
                ts_ms=1000,
                market="KRW-KITE",
                side="bid",
                price=441.0,
                volume=12.77,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                meta_json=json.dumps(intent_meta, ensure_ascii=False, sort_keys=True),
                status="SUBMITTED",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="entry-order-kite",
                identifier="AUTOBOT-autobot-001-intent-entry-kite-1000-a",
                market="KRW-KITE",
                side="bid",
                ord_type="limit",
                price=441.0,
                volume_req=12.77,
                volume_filled=12.77,
                state="done",
                created_ts=1000,
                updated_ts=1000,
                intent_id="intent-entry-kite",
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="entry-order-kite",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="exit-order-kite",
                identifier="AUTOBOT-autobot-001-intent-exit-kite-1300-a",
                market="KRW-KITE",
                side="ask",
                ord_type="limit",
                price=443.0,
                volume_req=12.77,
                volume_filled=0.0,
                state="wait",
                created_ts=1300,
                updated_ts=1400,
                intent_id="intent-exit-kite",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="exit-order-kite",
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="model-risk-intent-entry-kite",
                market="KRW-KITE",
                side="long",
                entry_price_str="441",
                qty_str="12.77",
                tp_enabled=False,
                sl_enabled=False,
                trailing_enabled=False,
                state="EXITING",
                last_eval_ts_ms=1200,
                last_action_ts_ms=0,
                current_exit_order_uuid="exit-order-kite",
                current_exit_order_identifier="AUTOBOT-autobot-001-intent-exit-kite-1300-a",
                replace_attempt=0,
                created_ts=1000,
                updated_ts=1500,
                timeout_ts_ms=1801000,
                plan_source="model_alpha_v1",
                source_intent_id="intent-entry-kite",
            )
        )

        report = reconcile_exchange_snapshot(
            store=store,
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            accounts_payload=[
                {
                    "currency": "KITE",
                    "balance": "12.77",
                    "locked": "0",
                    "avg_buy_price": "441",
                }
            ],
            open_orders_payload=[],
            unknown_open_orders_policy="ignore",
            unknown_positions_policy="halt",
            dry_run=False,
            ts_ms=5000,
        )
        plan = store.risk_plan_by_id(plan_id="model-risk-intent-entry-kite")
        order = store.order_by_uuid(uuid="exit-order-kite")

    assert report["halted"] is False
    assert any(item["type"] == "import_managed_position_from_bot_intent" for item in report["actions"])
    assert plan is not None
    assert plan["state"] == "EXITING"
    assert plan["current_exit_order_uuid"] == "exit-order-kite"
    assert plan["last_action_ts_ms"] == 5000
    assert order is not None
    assert order["tp_sl_link"] == "model-risk-intent-entry-kite"


def test_reconcile_closes_local_position_when_bot_exit_is_done_and_exchange_position_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-KITE",
                base_currency="KITE",
                base_amount=13.56787669,
                avg_entry_price=442.0,
                updated_ts=1000,
                tp_json=json.dumps({"enabled": False, "source": "model_alpha_v1"}, ensure_ascii=False),
                sl_json=json.dumps({"enabled": False, "source": "model_alpha_v1"}, ensure_ascii=False),
                trailing_json=json.dumps({"enabled": False, "source": "model_alpha_v1"}, ensure_ascii=False),
                managed=True,
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="model-risk-intent-1",
                market="KRW-KITE",
                side="long",
                entry_price_str="442",
                qty_str="13.56787669",
                tp_enabled=False,
                sl_enabled=False,
                trailing_enabled=False,
                state="ACTIVE",
                last_eval_ts_ms=1000,
                last_action_ts_ms=0,
                replace_attempt=0,
                created_ts=1000,
                updated_ts=1000,
                timeout_ts_ms=1801000,
                plan_source="model_alpha_v1",
                source_intent_id="intent-entry-1",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="exit-order-1",
                identifier="AUTOBOT-autobot-001-exit-order-1",
                market="KRW-KITE",
                side="ask",
                ord_type="limit",
                price=450.0,
                volume_req=13.56787669,
                volume_filled=13.56787669,
                state="done",
                created_ts=1100,
                updated_ts=2000,
                intent_id=None,
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="exit-order-1",
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
            ts_ms=3000,
        )
        positions = store.list_positions()
        plans = store.list_risk_plans()

    assert report["halted"] is False
    assert report["counts"]["local_positions_missing_on_exchange"] == 0
    assert any(item["type"] == "close_managed_position_from_bot_exit" for item in report["actions"])
    assert positions == []
    assert len(plans) == 1
    assert plans[0]["state"] == "CLOSED"
    assert plans[0]["current_exit_order_uuid"] == "exit-order-1"
    assert plans[0]["plan_source"] == "model_alpha_v1"
    assert plans[0]["source_intent_id"] == "intent-entry-1"
