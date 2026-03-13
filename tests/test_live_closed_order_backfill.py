from __future__ import annotations

import json
import pytest

from autobot.live.closed_order_backfill import backfill_recent_bot_closed_orders
from autobot.live.state_store import LiveStateStore, OrderRecord, TradeJournalRecord


class _StubClosedOrdersClient:
    def __init__(self, payload):  # noqa: ANN001
        self.payload = payload
        self.calls = []

    def closed_orders(self, **kwargs):  # noqa: ANN003, ANN201
        self.calls.append(dict(kwargs))
        return list(self.payload)


def test_backfill_recent_bot_closed_orders_upserts_closed_order_and_recomputes(tmp_path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_order(
            OrderRecord(
                uuid="entry-order-1",
                identifier="AUTOBOT-autobot-001-intent-entry-1700000000000-a",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=100.0,
                volume_req=1.0,
                volume_filled=1.0,
                state="done",
                created_ts=1_000,
                updated_ts=1_100,
                intent_id="intent-entry",
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="ORDER_STATE",
                event_source="test",
                replace_seq=0,
                root_order_uuid="entry-order-1",
                executed_funds=100.0,
                paid_fee=0.05,
            )
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-1",
                market="KRW-BTC",
                status="CLOSED",
                entry_intent_id="intent-entry",
                entry_order_uuid="entry-order-1",
                exit_order_uuid="exit-order-1",
                plan_id="plan-1",
                entry_submitted_ts_ms=1_000,
                entry_filled_ts_ms=1_100,
                exit_ts_ms=2_000,
                entry_price=100.0,
                exit_price=None,
                qty=1.0,
                entry_notional_quote=100.0,
                exit_notional_quote=None,
                realized_pnl_quote=None,
                realized_pnl_pct=None,
                entry_reason_code="MODEL_ALPHA_ENTRY_V1",
                close_reason_code="POSITION_CLOSED",
                close_mode="missing_on_exchange_after_exit_plan",
                entry_meta_json=json.dumps(
                    {
                        "admissibility": {"sizing": {"fee_rate": 0.0005}, "snapshot": {"bid_fee": 0.0005, "ask_fee": 0.0005}},
                        "execution": {"requested_price": 100.0},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                updated_ts=2_000,
            )
        )

        client = _StubClosedOrdersClient(
            [
                {
                    "uuid": "exit-order-1",
                    "identifier": "AUTOBOT-autobot-001-intent-exit-1700000000001-a-rid_run-123",
                    "market": "KRW-BTC",
                    "side": "ask",
                    "ord_type": "limit",
                    "state": "done",
                    "price": "103",
                    "volume": "1",
                    "executed_volume": "1",
                    "executed_funds": "103",
                    "paid_fee": "0.0515",
                    "created_at": "2026-03-13T00:10:00Z",
                    "done_at": "2026-03-13T00:11:00Z",
                }
            ]
        )

        report = backfill_recent_bot_closed_orders(
            store=store,
            client=client,
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            now_ts_ms=1_700_000_200_000,
        )
        order = store.order_by_uuid(uuid="exit-order-1")
        journal = store.trade_journal_by_id(journal_id="journal-1")

    assert report["supported"] is True
    assert report["orders_upserted"] == 1
    assert client.calls
    assert order is not None
    assert order["local_state"] == "DONE"
    assert journal is not None
    assert journal["realized_pnl_quote"] == pytest.approx(2.8985)
    assert journal["exit_price"] == 103.0
