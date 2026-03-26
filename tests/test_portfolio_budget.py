from __future__ import annotations

from autobot.live.state_store import LiveStateStore, OrderRecord, PositionRecord, TradeJournalRecord
from autobot.risk.portfolio_budget import resolve_portfolio_risk_budget


def test_resolve_portfolio_risk_budget_clamps_to_gross_remaining_quote(tmp_path) -> None:
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-ETH",
                base_currency="ETH",
                base_amount=0.003,
                avg_entry_price=5_000_000.0,
                updated_ts=1000,
            )
        )
        payload = resolve_portfolio_risk_budget(
            store=store,
            market="KRW-BTC",
            side="bid",
            target_notional_quote=20_000.0,
            base_budget_quote=10_000.0,
            quote_free=50_000.0,
            min_total_krw=5_000.0,
            effective_max_positions=2,
            rollout_mode="live",
            runtime_model_run_id="run-live",
        )

    assert payload["allowed"] is True
    assert payload["resolved_notional_quote"] == 5_000.0
    assert payload["position_budget_fraction"] == 0.5
    assert "PORTFOLIO_GROSS_BUDGET_CLAMP" in payload["risk_reason_codes"]


def test_resolve_portfolio_risk_budget_counts_open_bid_order_exposure_in_gross_budget(tmp_path) -> None:
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_order(
            OrderRecord(
                uuid="order-1",
                identifier="AUTOBOT-1",
                market="KRW-BTC",
                side="bid",
                ord_type="best",
                price=6_000.0,
                volume_req=None,
                volume_filled=0.0,
                state="wait",
                created_ts=1000,
                updated_ts=1000,
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="SUBMIT_ACCEPTED",
                event_source="live_model_alpha_runtime",
                reserved_fee=3.0,
                paid_fee=0.0,
                remaining_fee=3.0,
            )
        )
        payload = resolve_portfolio_risk_budget(
            store=store,
            market="KRW-ETH",
            side="bid",
            target_notional_quote=6_000.0,
            base_budget_quote=10_000.0,
            quote_free=20_000.0,
            min_total_krw=5_000.0,
            effective_max_positions=1,
            rollout_mode="canary",
            runtime_model_run_id="run-live",
        )

    assert payload["current_total_cash_at_risk_quote"] == 6_003.0
    assert payload["cluster_utilization"]["clusters"][0]["open_bid_order_notional_quote"] == 6_003.0
    assert payload["allowed"] is False
    assert "PORTFOLIO_GROSS_BUDGET_CLAMP" in payload["risk_reason_codes"]
    assert "PORTFOLIO_GROSS_BUDGET_EXHAUSTED" in payload["risk_reason_codes"]


def test_resolve_portfolio_risk_budget_applies_recent_loss_streak_haircut(tmp_path) -> None:
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="j2",
                market="KRW-BTC",
                status="CLOSED",
                entry_submitted_ts_ms=1000,
                entry_filled_ts_ms=1001,
                exit_ts_ms=2000,
                entry_price=100.0,
                exit_price=99.0,
                qty=1.0,
                entry_notional_quote=100.0,
                exit_notional_quote=99.0,
                realized_pnl_quote=-1.0,
                realized_pnl_pct=-1.0,
                entry_meta_json='{"runtime":{"live_runtime_model_run_id":"run-live"}}',
                updated_ts=2000,
            )
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="j1",
                market="KRW-ETH",
                status="CLOSED",
                entry_submitted_ts_ms=900,
                entry_filled_ts_ms=901,
                exit_ts_ms=1900,
                entry_price=100.0,
                exit_price=99.0,
                qty=1.0,
                entry_notional_quote=100.0,
                exit_notional_quote=99.0,
                realized_pnl_quote=-1.0,
                realized_pnl_pct=-1.0,
                entry_meta_json='{"runtime":{"live_runtime_model_run_id":"run-live"}}',
                updated_ts=1900,
            )
        )
        payload = resolve_portfolio_risk_budget(
            store=store,
            market="KRW-XRP",
            side="bid",
            target_notional_quote=10_000.0,
            base_budget_quote=10_000.0,
            quote_free=20_000.0,
            min_total_krw=5_000.0,
            effective_max_positions=2,
            rollout_mode="live",
            runtime_model_run_id="run-live",
        )

    assert payload["recent_loss_streak_haircut"] == 0.75
    assert "PORTFOLIO_RECENT_LOSS_STREAK_HAIRCUT" in payload["risk_reason_codes"]


def test_resolve_portfolio_risk_budget_canary_bypasses_soft_haircuts_but_keeps_warning(tmp_path) -> None:
    with LiveStateStore(tmp_path / "live_state.db") as store:
        payload = resolve_portfolio_risk_budget(
            store=store,
            market="KRW-XRP",
            side="bid",
            target_notional_quote=5_600.0,
            base_budget_quote=10_000.0,
            quote_free=20_000.0,
            min_total_krw=5_000.0,
            effective_max_positions=1,
            rollout_mode="canary",
            state_features={"spread_bps": 35.0},
            runtime_model_run_id="run-live",
        )

    assert payload["allowed"] is True
    assert payload["warning_only"] is True
    assert payload["enforcement_mode"] == "warning_only"
    assert "CANARY_PORTFOLIO_BUDGET_NOT_APPLIED" in payload["warning_reason_codes"]
    assert "PORTFOLIO_SPREAD_HAIRCUT" in payload["warning_reason_codes"]
    assert payload["resolved_notional_quote"] == 5_600.0
    assert payload["diagnostic_resolved_notional_quote"] == 4_200.0
    assert payload["soft_budget_clamped"] is True


def test_resolve_portfolio_risk_budget_ignores_pre_reset_loss_streak_history(tmp_path) -> None:
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="j1",
                market="KRW-ETH",
                status="CLOSED",
                entry_submitted_ts_ms=900,
                entry_filled_ts_ms=901,
                exit_ts_ms=1900,
                entry_price=100.0,
                exit_price=99.0,
                qty=1.0,
                entry_notional_quote=100.0,
                exit_notional_quote=99.0,
                realized_pnl_quote=-1.0,
                realized_pnl_pct=-1.0,
                entry_meta_json='{"runtime":{"live_runtime_model_run_id":"run-live"}}',
                updated_ts=1900,
            )
        )
        store.set_checkpoint(
            name="live_suppressor_reset",
            payload={"history_reset_ts_ms": 2000, "run_id": "run-live"},
            ts_ms=2000,
        )
        payload = resolve_portfolio_risk_budget(
            store=store,
            market="KRW-XRP",
            side="bid",
            target_notional_quote=10_000.0,
            base_budget_quote=10_000.0,
            quote_free=20_000.0,
            min_total_krw=5_000.0,
            effective_max_positions=2,
            rollout_mode="live",
            runtime_model_run_id="run-live",
        )

    assert payload["recent_loss_streak_haircut"] == 1.0
    assert "PORTFOLIO_RECENT_LOSS_STREAK_HAIRCUT" not in payload["risk_reason_codes"]
