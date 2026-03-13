from __future__ import annotations

from autobot.live.state_store import LiveStateStore, TradeJournalRecord
from autobot.live.trade_journal_cleanup import cleanup_imported_trade_journal_duplicates


def test_cleanup_imported_trade_journal_duplicates_removes_only_imported_rows(tmp_path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-canonical",
                market="KRW-API3",
                status="CLOSED",
                entry_intent_id="intent-1",
                entry_order_uuid="entry-order-1",
                exit_order_uuid="exit-order-1",
                plan_id="plan-1",
                entry_filled_ts_ms=1_100,
                exit_ts_ms=1_900,
                entry_price=100.0,
                exit_price=101.0,
                qty=1.0,
                entry_notional_quote=100.0,
                exit_notional_quote=101.0,
                realized_pnl_quote=0.9,
                updated_ts=1_900,
            )
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="imported-KRW-API3-1900",
                market="KRW-API3",
                status="CLOSED",
                exit_order_uuid="exit-order-1",
                exit_ts_ms=1_900,
                realized_pnl_quote=50.0,
                updated_ts=1_900,
            )
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="imported-KRW-COMP-2000",
                market="KRW-COMP",
                status="CLOSED",
                exit_order_uuid=None,
                exit_ts_ms=2_000,
                realized_pnl_quote=-0.5,
                updated_ts=2_000,
            )
        )

    dry_run = cleanup_imported_trade_journal_duplicates(db_path, apply_changes=False)
    assert dry_run["duplicate_row_count"] == 1
    assert dry_run["groups"][0]["delete_journal_ids"] == ["imported-KRW-API3-1900"]
    assert dry_run["groups"][0]["keep_journal_ids"] == ["journal-canonical"]
    assert dry_run["groups"][0]["match_kind"] == "exit_order_uuid"

    applied = cleanup_imported_trade_journal_duplicates(db_path, apply_changes=True)
    assert applied["duplicate_row_count"] == 1

    with LiveStateStore(db_path) as store:
        rows = sorted(store.list_trade_journal(), key=lambda item: str(item["journal_id"]))

    assert [row["journal_id"] for row in rows] == ["imported-KRW-COMP-2000", "journal-canonical"]


def test_cleanup_imported_trade_journal_duplicates_keeps_multi_entry_canonical_rows(tmp_path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-a",
                market="KRW-XRP",
                status="CLOSED",
                entry_intent_id="intent-a",
                entry_order_uuid="entry-order-a",
                exit_order_uuid="exit-order-shared",
                plan_id="plan-a",
                entry_filled_ts_ms=1_000,
                exit_ts_ms=2_000,
                realized_pnl_quote=-8.5,
                updated_ts=2_000,
            )
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-b",
                market="KRW-XRP",
                status="CLOSED",
                entry_intent_id="intent-b",
                entry_order_uuid="entry-order-b",
                exit_order_uuid="exit-order-shared",
                plan_id="plan-b",
                entry_filled_ts_ms=1_100,
                exit_ts_ms=2_000,
                realized_pnl_quote=-7.4,
                updated_ts=2_000,
            )
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="imported-KRW-XRP-2000",
                market="KRW-XRP",
                status="CLOSED",
                exit_order_uuid="exit-order-shared",
                exit_ts_ms=2_000,
                realized_pnl_quote=-7.4,
                updated_ts=2_000,
            )
        )

    applied = cleanup_imported_trade_journal_duplicates(db_path, apply_changes=True)
    assert applied["duplicate_row_count"] == 1
    assert applied["groups"][0]["keep_journal_ids"] == ["journal-a", "journal-b"]

    with LiveStateStore(db_path) as store:
        rows = sorted(store.list_trade_journal(statuses=("CLOSED",)), key=lambda item: str(item["journal_id"]))

    assert [row["journal_id"] for row in rows] == ["journal-a", "journal-b"]


def test_cleanup_imported_trade_journal_duplicates_removes_recent_geometry_match_without_exit_uuid(tmp_path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-nom",
                market="KRW-NOM",
                status="CLOSED",
                entry_intent_id="intent-nom",
                entry_order_uuid="entry-order-nom",
                exit_order_uuid="exit-order-nom",
                plan_id="plan-nom",
                entry_filled_ts_ms=1_000,
                exit_ts_ms=2_020,
                entry_price=7.49,
                exit_price=None,
                qty=751.77764922,
                entry_notional_quote=5633.63,
                exit_notional_quote=None,
                realized_pnl_quote=None,
                updated_ts=2_020,
            )
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="imported-KRW-NOM-2000",
                market="KRW-NOM",
                status="CLOSED",
                entry_intent_id=None,
                entry_order_uuid=None,
                exit_order_uuid=None,
                plan_id=None,
                entry_price=7.49,
                exit_price=7.73,
                qty=751.77764922,
                entry_notional_quote=5633.63,
                exit_notional_quote=5811.24,
                realized_pnl_quote=177.61,
                exit_ts_ms=2_000,
                updated_ts=2_000,
            )
        )

    dry_run = cleanup_imported_trade_journal_duplicates(db_path, apply_changes=False)
    assert dry_run["duplicate_row_count"] == 1
    assert dry_run["groups"][0]["match_kind"] == "recent_market_geometry"
    assert dry_run["groups"][0]["delete_journal_ids"] == ["imported-KRW-NOM-2000"]
    assert dry_run["groups"][0]["keep_journal_ids"] == ["journal-nom"]

    cleanup_imported_trade_journal_duplicates(db_path, apply_changes=True)
    with LiveStateStore(db_path) as store:
        rows = store.list_trade_journal(statuses=("CLOSED",), market="KRW-NOM")

    assert [row["journal_id"] for row in rows] == ["journal-nom"]
