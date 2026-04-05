from __future__ import annotations

from autobot.backtest.engine import _resolve_candidate_portfolio_budget as resolve_backtest_candidate_portfolio_budget
from autobot.backtest.engine import BacktestRunSettings
from autobot.execution.intent import new_order_intent
from autobot.live.state_store import LiveStateStore, OrderRecord, PositionRecord, TradeJournalRecord
from autobot.paper.engine import _resolve_candidate_portfolio_budget as resolve_paper_candidate_portfolio_budget
from autobot.paper.engine import PaperRunSettings
from autobot.paper.sim_exchange import MarketRules, PaperSimExchange
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


def test_resolve_portfolio_risk_budget_uses_alpha_es_and_tradability_inputs(tmp_path) -> None:
    with LiveStateStore(tmp_path / "live_state.db") as store:
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
            uncertainty=0.25,
            expected_return_bps=8.0,
            expected_es_bps=16.0,
            tradability_prob=0.40,
            alpha_lcb_bps=4.0,
            runtime_model_run_id="run-live",
        )

    assert payload["alpha_strength_haircut"] == 0.75
    assert payload["expected_es_haircut"] == 0.5
    assert payload["tradability_haircut"] == 0.5
    assert payload["confidence_haircut"] == 0.8
    assert payload["resolved_notional_quote"] == 5_000.0
    assert "PORTFOLIO_ALPHA_LCB_HAIRCUT" in payload["risk_reason_codes"]
    assert "PORTFOLIO_EXPECTED_ES_HAIRCUT" in payload["risk_reason_codes"]
    assert "PORTFOLIO_TRADABILITY_HAIRCUT" in payload["risk_reason_codes"]


def test_resolve_portfolio_risk_budget_preserves_min_total_floor_under_soft_haircuts(tmp_path) -> None:
    with LiveStateStore(tmp_path / "live_state.db") as store:
        payload = resolve_portfolio_risk_budget(
            store=store,
            market="KRW-XRP",
            side="bid",
            target_notional_quote=5_000.0,
            base_budget_quote=10_000.0,
            quote_free=20_000.0,
            min_total_krw=5_000.0,
            effective_max_positions=2,
            rollout_mode="live",
            uncertainty=0.25,
            expected_return_bps=8.0,
            expected_es_bps=16.0,
            tradability_prob=0.40,
            alpha_lcb_bps=4.0,
            runtime_model_run_id="run-live",
        )

    assert payload["allowed"] is True
    assert payload["structural_resolved_notional_quote"] == 5_000.0
    assert payload["diagnostic_resolved_notional_quote"] == 2_500.0
    assert payload["resolved_notional_quote"] == 5_000.0
    assert payload["soft_budget_clamped"] is True


def test_resolve_portfolio_risk_budget_canary_enforces_soft_haircuts(tmp_path) -> None:
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

    assert payload["allowed"] is False
    assert payload["warning_only"] is False
    assert payload["enforcement_mode"] == "enforced"
    assert payload["warning_reason_codes"] == []
    assert "PORTFOLIO_SPREAD_HAIRCUT" in payload["risk_reason_codes"]
    assert payload["resolved_notional_quote"] == 4_200.0
    assert payload["diagnostic_resolved_notional_quote"] == 4_200.0
    assert payload["soft_budget_clamped"] is True


def test_resolve_portfolio_risk_budget_applies_platform_quality_haircut(tmp_path) -> None:
    with LiveStateStore(tmp_path / "live_state.db") as store:
        payload = resolve_portfolio_risk_budget(
            store=store,
            market="KRW-BTC",
            side="bid",
            target_notional_quote=10_000.0,
            base_budget_quote=10_000.0,
            quote_free=20_000.0,
            min_total_krw=5_000.0,
            effective_max_positions=2,
            rollout_mode="live",
            runtime_model_run_id="run-live",
            platform_quality_budget={
                "micro_available_ratio": 0.50,
                "micro_validate_parse_ok_ratio": 0.80,
                "micro_validate_short_trade_coverage_ratio": 0.95,
                "micro_validate_short_book_coverage_ratio": 0.60,
                "one_m_synth_ratio_p90": 0.80,
                "rows_dropped_no_micro": 2,
            },
        )

    assert payload["data_quality_haircut"] < 1.0
    assert payload["resolved_notional_quote"] < 10_000.0
    assert "PORTFOLIO_DATA_QUALITY_MICRO_AVAILABILITY_HAIRCUT" in payload["risk_reason_codes"]
    assert "PORTFOLIO_DATA_QUALITY_PARSE_OK_HAIRCUT" in payload["risk_reason_codes"]
    assert "PORTFOLIO_DATA_QUALITY_SHORT_TRADE_COVERAGE_SEVERE" in payload["risk_reason_codes"]
    assert "PORTFOLIO_DATA_QUALITY_SHORT_BOOK_COVERAGE_HIGH" in payload["risk_reason_codes"]
    assert "PORTFOLIO_DATA_QUALITY_SYNTH_P90_SEVERE" in payload["risk_reason_codes"]
    assert "PORTFOLIO_DATA_QUALITY_MICRO_DROPS_PRESENT" in payload["risk_reason_codes"]


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


def test_resolve_portfolio_risk_budget_reads_simulator_positions_and_open_orders() -> None:
    exchange = PaperSimExchange(quote_currency="KRW", starting_cash_quote=60_000.0)
    rules = MarketRules(min_total=5_000.0, tick_size=1.0)
    entry_intent = new_order_intent(
        market="KRW-ETH",
        side="bid",
        ord_type="best",
        time_in_force="ioc",
        price=15_000.0,
        volume=1.0,
        reason_code="MODEL_ALPHA_ENTRY_V1",
        ts_ms=1_000,
    )
    exchange.submit_best_order(
        intent=entry_intent,
        rules=rules,
        latest_trade_price=5_000_000.0,
        ts_ms=1_000,
    )
    open_intent = new_order_intent(
        market="KRW-BTC",
        side="bid",
        price=10_000.0,
        volume=1.0,
        reason_code="MODEL_ALPHA_ENTRY_V1",
        ts_ms=2_000,
    )
    exchange.submit_limit_order(
        intent=open_intent,
        rules=rules,
        latest_trade_price=11_000.0,
        ts_ms=2_000,
    )

    payload = resolve_portfolio_risk_budget(
        store=exchange,
        market="KRW-XRP",
        side="bid",
        target_notional_quote=10_000.0,
        base_budget_quote=10_000.0,
        quote_free=float(exchange.quote_balance().free),
        min_total_krw=5_000.0,
        effective_max_positions=3,
        rollout_mode="paper",
    )

    assert payload["current_total_cash_at_risk_quote"] > 25_000.0
    assert payload["cluster_utilization"]["decision_cluster_current_notional_quote"] == 0.0
    assert payload["allowed"] is False
    assert "PORTFOLIO_GROSS_BUDGET_EXHAUSTED" in payload["risk_reason_codes"]


def test_backtest_and_paper_candidate_budget_helpers_use_operational_max_positions() -> None:
    exchange = PaperSimExchange(quote_currency="KRW", starting_cash_quote=80_000.0)
    rules = MarketRules(min_total=5_000.0, tick_size=1.0)
    entry_intent = new_order_intent(
        market="KRW-ETH",
        side="bid",
        ord_type="best",
        time_in_force="ioc",
        price=15_000.0,
        volume=1.0,
        reason_code="MODEL_ALPHA_ENTRY_V1",
        ts_ms=1_000,
    )
    exchange.submit_best_order(
        intent=entry_intent,
        rules=rules,
        latest_trade_price=5_000_000.0,
        ts_ms=1_000,
    )
    candidate_meta = {
        "operational_overlay": {"max_positions_used": 2},
        "state_features": {"spread_bps": 4.0},
        "final_expected_return": 0.0012,
        "final_expected_es": 0.0004,
        "final_tradability": 0.9,
        "final_alpha_lcb": 0.0008,
        "uncertainty": 0.05,
    }

    backtest_payload = resolve_backtest_candidate_portfolio_budget(
        exchange=exchange,
        market="KRW-BTC",
        side="bid",
        target_notional_quote=10_000.0,
        candidate_meta=candidate_meta,
        run_settings=BacktestRunSettings(per_trade_krw=10_000.0),
        rules=rules,
    )
    paper_payload = resolve_paper_candidate_portfolio_budget(
        exchange=exchange,
        market="KRW-BTC",
        side="bid",
        target_notional_quote=10_000.0,
        candidate_meta=candidate_meta,
        run_settings=PaperRunSettings(per_trade_krw=10_000.0),
        rules=rules,
    )

    assert backtest_payload["allowed"] is False
    assert paper_payload["allowed"] is False
    assert backtest_payload["primary_reason_code"] == "PORTFOLIO_GROSS_BUDGET_EXHAUSTED"
    assert paper_payload["primary_reason_code"] == "PORTFOLIO_GROSS_BUDGET_EXHAUSTED"
    assert backtest_payload["resolved_notional_quote"] == paper_payload["resolved_notional_quote"]
    assert backtest_payload["resolved_notional_quote"] < 10_000.0
