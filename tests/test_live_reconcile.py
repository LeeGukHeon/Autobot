from __future__ import annotations

import json
from pathlib import Path

from autobot.live.reconcile import reconcile_exchange_snapshot
from autobot.live.state_store import IntentRecord, LiveStateStore, OrderRecord, PositionRecord, RiskPlanRecord, TradeJournalRecord


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


def test_reconcile_treats_risk_identifier_as_bot_order(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        report = reconcile_exchange_snapshot(
            store=store,
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            accounts_payload=[],
            open_orders_payload=[
                {
                    "uuid": "risk-1",
                    "identifier": "AUTOBOT-autobot-001-RISK-model-risk-1773391515252",
                    "market": "KRW-BTC",
                    "side": "ask",
                    "ord_type": "limit",
                    "state": "wait",
                }
            ],
            unknown_open_orders_policy="halt",
            unknown_positions_policy="halt",
            dry_run=True,
        )

    assert report["halted"] is False
    assert report["counts"]["external_open_orders"] == 0
    assert report["counts"]["exchange_bot_open_orders"] == 1


def test_reconcile_treats_other_bot_protective_identifier_as_external_order(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        report = reconcile_exchange_snapshot(
            store=store,
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            accounts_payload=[],
            open_orders_payload=[
                {
                    "uuid": "risk-foreign-1",
                    "identifier": "AUTOBOT-autobot-candidate-001-RISK-model-risk-1773391515252",
                    "market": "KRW-BTC",
                    "side": "ask",
                    "ord_type": "limit",
                    "state": "wait",
                }
            ],
            unknown_open_orders_policy="halt",
            unknown_positions_policy="halt",
            dry_run=True,
        )

    assert report["halted"] is True
    assert "UNKNOWN_OPEN_ORDERS_DETECTED" in report["halted_reasons"]
    assert report["counts"]["external_open_orders"] == 1
    assert report["counts"]["exchange_bot_open_orders"] == 0


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


def test_reconcile_attach_strategy_risk_builds_model_plan(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    registry_root = tmp_path / "models" / "registry"
    run_dir = registry_root / "train_v4_crypto_cs" / "run-live"
    run_dir.mkdir(parents=True, exist_ok=True)
    (registry_root / "train_v4_crypto_cs" / "champion.json").write_text(
        json.dumps({"run_id": "run-live"}, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    (run_dir / "runtime_recommendations.json").write_text(
        json.dumps(
            {
                "exit": {
                    "version": 1,
                    "recommended_exit_mode": "hold",
                    "recommended_exit_mode_source": "execution_backtest_grid_search_compare",
                    "recommended_exit_mode_reason_code": "HOLD_EXECUTION_COMPARE_EDGE",
                    "recommended_hold_bars": 6,
                }
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
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
            unknown_positions_policy="attach_strategy_risk",
            registry_root=str(registry_root),
            runtime_model_ref_source="champion_v4",
            runtime_model_family="train_v4_crypto_cs",
            dry_run=False,
            ts_ms=5_000,
        )
        positions = store.list_positions()
        risk_plans = store.list_risk_plans()

    assert len(positions) == 1
    assert positions[0]["managed"] is True
    assert len(risk_plans) == 1
    assert risk_plans[0]["plan_source"] == "model_alpha_v1"
    assert risk_plans[0]["state"] == "ACTIVE"
    assert risk_plans[0]["timeout_ts_ms"] == 5_000 + (6 * 300_000)


def test_reconcile_attach_strategy_risk_reuses_pending_entry_journal_for_filled_bid(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    registry_root = tmp_path / "models" / "registry"
    run_dir = registry_root / "train_v4_crypto_cs" / "run-live"
    run_dir.mkdir(parents=True, exist_ok=True)
    (registry_root / "train_v4_crypto_cs" / "champion.json").write_text(
        json.dumps({"run_id": "run-live"}, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    (run_dir / "runtime_recommendations.json").write_text(
        json.dumps(
            {
                "exit": {
                    "version": 1,
                    "recommended_exit_mode": "hold",
                    "recommended_exit_mode_source": "execution_backtest_grid_search_compare",
                    "recommended_exit_mode_reason_code": "HOLD_EXECUTION_COMPARE_EDGE",
                    "recommended_hold_bars": 6,
                }
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    with LiveStateStore(db_path) as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-avnt-1",
                ts_ms=1_000,
                market="KRW-AVNT",
                side="bid",
                price=251.0,
                volume=22.35025913,
                reason_code="CLOSED_ORDERS_BACKFILL",
                meta_json=json.dumps({"source": "closed_orders_backfill"}, ensure_ascii=False, sort_keys=True),
                status="UPDATED_FROM_CLOSED_ORDERS",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="avnt-order-1",
                identifier="AUTOBOT-autobot-001-intent-avnt-1-1000-abcd",
                market="KRW-AVNT",
                side="bid",
                ord_type="limit",
                price=251.0,
                volume_req=22.35025913,
                volume_filled=22.35025913,
                state="done",
                created_ts=1_100,
                updated_ts=1_100,
                intent_id="intent-avnt-1",
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="CLOSED_ORDERS_BACKFILL",
                event_source="closed_orders_backfill",
                root_order_uuid="avnt-order-1",
                executed_funds=5610.91504163,
                paid_fee=2.805457520815,
            )
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="intent-avnt-1",
                market="KRW-AVNT",
                status="PENDING_ENTRY",
                entry_intent_id="intent-avnt-1",
                entry_order_uuid="avnt-order-1",
                exit_order_uuid=None,
                plan_id=None,
                entry_submitted_ts_ms=1_000,
                entry_filled_ts_ms=None,
                exit_ts_ms=None,
                entry_price=251.0,
                exit_price=None,
                qty=22.35025913,
                entry_notional_quote=5610.91504163,
                exit_notional_quote=None,
                realized_pnl_quote=None,
                realized_pnl_pct=None,
                entry_reason_code="MODEL_ALPHA_ENTRY_V1",
                close_reason_code=None,
                close_mode=None,
                updated_ts=1_000,
            )
        )

        reconcile_exchange_snapshot(
            store=store,
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            accounts_payload=[
                {
                    "currency": "AVNT",
                    "balance": "22.35025913",
                    "locked": "0",
                    "avg_buy_price": "251",
                }
            ],
            open_orders_payload=[],
            unknown_open_orders_policy="ignore",
            unknown_positions_policy="attach_strategy_risk",
            registry_root=str(registry_root),
            runtime_model_ref_source="champion_v4",
            runtime_model_family="train_v4_crypto_cs",
            dry_run=False,
            ts_ms=5_000,
        )
        journals = store.list_trade_journal(market="KRW-AVNT")
        risk_plans = store.list_risk_plans(market="KRW-AVNT")

    assert len(journals) == 1
    assert journals[0]["journal_id"] == "intent-avnt-1"
    assert journals[0]["status"] == "OPEN"
    assert journals[0]["entry_filled_ts_ms"] == 1_100
    assert journals[0]["plan_id"] == risk_plans[0]["plan_id"]
    assert risk_plans[0]["source_intent_id"] == "intent-avnt-1"


def test_reconcile_classifies_missing_position_as_manual_sell(tmp_path: Path) -> None:
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
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-manual-sell",
                market="KRW-BTC",
                status="OPEN",
                entry_intent_id=None,
                entry_order_uuid=None,
                exit_order_uuid=None,
                plan_id=None,
                entry_submitted_ts_ms=1000,
                entry_filled_ts_ms=1000,
                entry_price=100000000.0,
                qty=0.01,
                entry_notional_quote=1000000.0,
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
            unknown_positions_policy="import_as_unmanaged",
            dry_run=False,
            ts_ms=5000,
        )
        positions = store.list_positions()
        journal = store.trade_journal_by_id(journal_id="journal-manual-sell")

    assert report["halted"] is False
    assert any(item["type"] == "close_position_as_manual_sell" for item in report["actions"])
    assert positions == []
    assert journal is not None
    assert journal["status"] == "CLOSED"
    assert journal["close_reason_code"] == "MANUAL_SELL_DETECTED"
    assert journal["close_mode"] == "external_manual_order"


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


def test_reconcile_import_promotes_existing_active_plan_to_exiting_when_exchange_exit_is_open(tmp_path: Path) -> None:
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
            "submit_result": {"accepted": True, "order_uuid": "entry-order-wave"},
        }
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-entry-wave",
                ts_ms=1000,
                market="KRW-WAVE",
                side="bid",
                price=100.0,
                volume=5.0,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                meta_json=json.dumps(intent_meta, ensure_ascii=False, sort_keys=True),
                status="SUBMITTED",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="entry-order-wave",
                identifier="AUTOBOT-autobot-001-intent-entry-wave-1000-a",
                market="KRW-WAVE",
                side="bid",
                ord_type="limit",
                price=100.0,
                volume_req=5.0,
                volume_filled=5.0,
                state="done",
                created_ts=1000,
                updated_ts=1000,
                intent_id="intent-entry-wave",
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="entry-order-wave",
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="model-risk-intent-entry-wave",
                market="KRW-WAVE",
                side="long",
                entry_price_str="100",
                qty_str="5",
                tp_enabled=False,
                sl_enabled=False,
                trailing_enabled=False,
                state="ACTIVE",
                last_eval_ts_ms=1200,
                last_action_ts_ms=0,
                replace_attempt=0,
                created_ts=1000,
                updated_ts=1500,
                timeout_ts_ms=1801000,
                plan_source="model_alpha_v1",
                source_intent_id="intent-entry-wave",
            )
        )

        reconcile_exchange_snapshot(
            store=store,
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            accounts_payload=[
                {
                    "currency": "WAVE",
                    "balance": "5",
                    "locked": "0",
                    "avg_buy_price": "100",
                }
            ],
            open_orders_payload=[
                {
                    "uuid": "exit-order-wave",
                    "identifier": "AUTOBOT-autobot-001-intent-exit-wave-1300-a",
                    "market": "KRW-WAVE",
                    "side": "ask",
                    "ord_type": "limit",
                    "price": "101.5",
                    "volume": "5",
                    "executed_volume": "0",
                    "state": "wait",
                }
            ],
            unknown_open_orders_policy="ignore",
            unknown_positions_policy="halt",
            dry_run=False,
            ts_ms=5000,
        )
        plan = store.risk_plan_by_id(plan_id="model-risk-intent-entry-wave")
        exit_order = store.order_by_uuid(uuid="exit-order-wave")

    assert plan is not None
    assert plan["state"] == "EXITING"
    assert plan["current_exit_order_uuid"] == "exit-order-wave"
    assert plan["current_exit_order_identifier"] == "AUTOBOT-autobot-001-intent-exit-wave-1300-a"
    assert plan["last_action_ts_ms"] == 5000
    assert exit_order is not None
    assert exit_order["tp_sl_link"] == "model-risk-intent-entry-wave"


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


def test_reconcile_closes_exiting_position_when_exchange_position_is_missing_and_no_open_orders(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-KITE",
                base_currency="KITE",
                base_amount=12.77943794,
                avg_entry_price=441.0,
                updated_ts=1000,
                tp_json=json.dumps({"enabled": False, "source": "model_alpha_v1"}, ensure_ascii=False),
                sl_json=json.dumps({"enabled": False, "source": "model_alpha_v1"}, ensure_ascii=False),
                trailing_json=json.dumps({"enabled": False, "source": "model_alpha_v1"}, ensure_ascii=False),
                managed=True,
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="model-risk-intent-kite",
                market="KRW-KITE",
                side="long",
                entry_price_str="441",
                qty_str="12.77943794",
                tp_enabled=False,
                sl_enabled=False,
                trailing_enabled=False,
                state="EXITING",
                last_eval_ts_ms=2000,
                last_action_ts_ms=2500,
                replace_attempt=1,
                created_ts=1000,
                updated_ts=2500,
                current_exit_order_uuid="exit-order-kite",
                current_exit_order_identifier="AUTOBOT-autobot-001-exit-kite",
                timeout_ts_ms=1801000,
                plan_source="model_alpha_v1",
                source_intent_id="intent-entry-kite",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="exit-order-kite",
                identifier="AUTOBOT-autobot-001-exit-kite",
                market="KRW-KITE",
                side="ask",
                ord_type="limit",
                price=443.0,
                volume_req=12.77943794,
                volume_filled=0.0,
                state="cancel",
                created_ts=2100,
                updated_ts=2400,
                intent_id=None,
                tp_sl_link="model-risk-intent-kite",
                local_state="CANCELLED",
                raw_exchange_state="cancel",
                last_event_name="ORDER_REPLACED",
                event_source="test",
                root_order_uuid="exit-order-kite",
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
    close_actions = [item for item in report["actions"] if item["type"] == "close_managed_position_from_bot_exit"]
    assert len(close_actions) == 1
    assert close_actions[0]["close_mode"] == "missing_on_exchange_after_exit_plan"
    assert positions == []
    assert len(plans) == 1
    assert plans[0]["state"] == "CLOSED"
    assert plans[0]["current_exit_order_uuid"] == "exit-order-kite"
    assert plans[0]["plan_source"] == "model_alpha_v1"
