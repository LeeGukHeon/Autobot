from __future__ import annotations

import json

from autobot.live.candidate_canary_report import build_candidate_canary_report, render_candidate_canary_markdown
from autobot.live.state_store import LiveStateStore, OrderRecord, PositionRecord, TradeJournalRecord


def test_candidate_canary_report_builds_metrics_and_dedupes_synthetic_rows(tmp_path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-BTC",
                base_currency="BTC",
                base_amount=0.01,
                avg_entry_price=100000000.0,
                updated_ts=5_000,
                managed=True,
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="open-order-1",
                identifier="AUTOBOT-RISK-open-order-1",
                market="KRW-BTC",
                side="ask",
                ord_type="limit",
                price=101000000.0,
                volume_req=0.01,
                volume_filled=0.0,
                state="wait",
                created_ts=5_000,
                updated_ts=5_000,
                intent_id="intent-open-order-1",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="open-order-1",
            )
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-canonical",
                market="KRW-ETH",
                status="CLOSED",
                entry_intent_id="intent-1",
                entry_order_uuid="entry-order-1",
                exit_order_uuid="exit-order-1",
                plan_id="plan-1",
                entry_submitted_ts_ms=1_000,
                entry_filled_ts_ms=1_100,
                exit_ts_ms=1_900,
                entry_price=100.0,
                exit_price=103.0,
                qty=1.0,
                entry_notional_quote=100.05,
                exit_notional_quote=102.9485,
                realized_pnl_quote=2.8985,
                realized_pnl_pct=2.8970514742628906,
                entry_reason_code="MODEL_ALPHA_ENTRY_V1",
                close_reason_code="CLOSED_ORDERS_BACKFILL",
                close_mode="done_ask_order",
                entry_meta_json=json.dumps(
                    {
                        "strategy": {
                            "meta": {
                                "entry_decision": {"reason_codes": ["ENTRY_GATE_BREAKER_ACTIVE"]},
                                "safety_vetoes": {"entry_boundary": {"reason_codes": ["ENTRY_BOUNDARY_ALPHA_LCB_NOT_POSITIVE"]}},
                                "exit_decision": {"decision_reason_code": "CONTINUATION_VALUE_EXIT"},
                                "liquidation_policy": {"tier_name": "normal_protective"},
                            }
                        }
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                exit_meta_json=json.dumps(
                    {"close_verified": True, "close_verification_status": "verified_exit_order"},
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                updated_ts=1_900,
            )
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="trade-KRW-ETH-1900",
                market="KRW-ETH",
                status="CLOSED",
                entry_intent_id=None,
                entry_order_uuid=None,
                exit_order_uuid="exit-order-1",
                plan_id=None,
                entry_submitted_ts_ms=1_000,
                entry_filled_ts_ms=1_150,
                exit_ts_ms=1_900,
                entry_price=100.0,
                exit_price=103.0,
                qty=1.0,
                entry_notional_quote=100.05,
                exit_notional_quote=102.9485,
                realized_pnl_quote=2.8985,
                realized_pnl_pct=2.8970514742628906,
                close_reason_code="CLOSED_ORDERS_BACKFILL",
                close_mode="done_ask_order",
                exit_meta_json=json.dumps(
                    {"close_verified": True, "close_verification_status": "verified_exit_order"},
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                updated_ts=1_900,
            )
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-loss",
                market="KRW-XRP",
                status="CLOSED",
                entry_intent_id="intent-2",
                entry_order_uuid="entry-order-2",
                exit_order_uuid="exit-order-2",
                plan_id="plan-2",
                entry_submitted_ts_ms=2_000,
                entry_filled_ts_ms=2_100,
                exit_ts_ms=2_700,
                entry_price=200.0,
                exit_price=198.0,
                qty=1.0,
                entry_notional_quote=200.1,
                exit_notional_quote=197.901,
                realized_pnl_quote=-2.199,
                realized_pnl_pct=-1.0994502748625685,
                entry_reason_code="MODEL_ALPHA_ENTRY_V1",
                close_reason_code="EXCHANGE_SNAPSHOT",
                close_mode="missing_on_exchange_after_exit_plan",
                exit_meta_json=json.dumps(
                    {"close_verified": True, "close_verification_status": "verified_exit_order"},
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                updated_ts=2_700,
            )
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-cancel",
                market="KRW-DOGE",
                status="CANCELLED_ENTRY",
                entry_intent_id="intent-3",
                entry_order_uuid="entry-order-3",
                exit_ts_ms=3_000,
                entry_price=10.0,
                qty=100.0,
                close_reason_code="ENTRY_ORDER_TIMEOUT",
                close_mode="entry_order_timeout",
                updated_ts=3_000,
            )
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-open",
                market="KRW-BTC",
                status="OPEN",
                entry_intent_id="intent-4",
                entry_order_uuid="entry-order-4",
                entry_submitted_ts_ms=4_000,
                entry_filled_ts_ms=4_100,
                entry_price=101.0,
                qty=1.0,
                updated_ts=4_100,
            )
        )

    report = build_candidate_canary_report(db_path)

    assert report["closed_total"] == 2
    assert report["verified_closed_total"] == 2
    assert report["cancelled_entry_total"] == 1
    assert report["open_total"] == 1
    assert report["positions_count"] == 1
    assert report["open_orders_count"] == 1
    assert report["wins_verified"] == 1
    assert report["losses_verified"] == 1
    assert report["realized_pnl_quote_total_verified"] == 0.6995
    assert report["entry_decision_reasons_top"][0][0] == "ENTRY_GATE_BREAKER_ACTIVE"
    assert report["safety_veto_reasons_top"][0][0] == "ENTRY_BOUNDARY_ALPHA_LCB_NOT_POSITIVE"
    assert report["exit_decision_reasons_top"][0][0] == "CONTINUATION_VALUE_EXIT"
    assert report["liquidation_policy_tiers"]["normal_protective"] == 1
    assert report["latest_closed"][0]["journal_id"] == "journal-loss"
    assert all(item["journal_id"] != "trade-KRW-ETH-1900" for item in report["latest_closed"])


def test_candidate_canary_report_renders_markdown(tmp_path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-md",
                market="KRW-BTC",
                status="CLOSED",
                entry_intent_id="intent-md",
                entry_order_uuid="entry-order-md",
                exit_order_uuid="exit-order-md",
                entry_submitted_ts_ms=1_000,
                entry_filled_ts_ms=1_100,
                exit_ts_ms=1_900,
                entry_price=100.0,
                exit_price=101.0,
                qty=1.0,
                entry_notional_quote=100.0,
                exit_notional_quote=101.0,
                realized_pnl_quote=1.0,
                realized_pnl_pct=1.0,
                close_reason_code="CLOSED_ORDERS_BACKFILL",
                close_mode="done_ask_order",
                exit_meta_json=json.dumps(
                    {"close_verified": True, "close_verification_status": "verified_exit_order"},
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                updated_ts=1_900,
            )
        )

    markdown = render_candidate_canary_markdown(build_candidate_canary_report(db_path))

    assert "# Candidate Canary Trade Report" in markdown
    assert "## Summary" in markdown
    assert "| Market | Closed | Verified | Wins | Losses | Realized PnL |" in markdown
    assert "KRW-BTC" in markdown


def test_candidate_canary_report_reads_opportunity_reason_summary(tmp_path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path):
        pass
    opportunity_log_path = tmp_path / "logs" / "opportunity_log" / "candidate" / "latest.jsonl"
    opportunity_log_path.parent.mkdir(parents=True, exist_ok=True)
    opportunity_log_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "opportunity_id": "entry:1000:KRW-BTC",
                        "run_id": "run-a",
                        "skip_reason_code": "ENTRY_GATE_PORTFOLIO_BUDGET_BLOCKED",
                        "meta": {
                            "entry_decision": {
                                "reason_codes": ["ENTRY_GATE_ALPHA_LCB_NOT_POSITIVE"],
                            },
                            "safety_vetoes": {
                                "portfolio_budget": {
                                    "reason_codes": ["ENTRY_GATE_PORTFOLIO_BUDGET_BLOCKED"],
                                }
                            },
                        },
                    },
                    ensure_ascii=False,
                ),
                json.dumps(
                    {
                        "opportunity_id": "entry:1000:KRW-ETH",
                        "run_id": "run-b",
                        "skip_reason_code": "ENTRY_GATE_BREAKER_ACTIVE",
                        "meta": {
                            "entry_decision": {
                                "reason_codes": ["ENTRY_GATE_BREAKER_ACTIVE"],
                            }
                        },
                    },
                    ensure_ascii=False,
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    report = build_candidate_canary_report(
        db_path,
        opportunity_log_path=opportunity_log_path,
        run_id="run-a",
    )

    assert report["opportunity_rows_total"] == 2
    assert report["opportunity_run_rows_total"] == 1
    assert report["opportunity_entry_decision_reasons_top"][0][0] == "ENTRY_GATE_ALPHA_LCB_NOT_POSITIVE"
    assert report["opportunity_safety_veto_reasons_top"][0][0] == "ENTRY_GATE_PORTFOLIO_BUDGET_BLOCKED"
    assert report["opportunity_skip_reasons_top"][0][0] == "ENTRY_GATE_PORTFOLIO_BUDGET_BLOCKED"
