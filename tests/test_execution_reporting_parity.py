from __future__ import annotations

from pathlib import Path

import pytest

from autobot.backtest.engine import BacktestRunEngine, ExecutionUpdate as BacktestExecutionUpdate
from autobot.common.event_store import JsonlEventStore
from autobot.paper.engine import ExecutionUpdate as PaperExecutionUpdate, PaperRunEngine
from autobot.paper.sim_exchange import FillEvent, PaperOrder
from autobot.strategy.trade_gate_v1 import GateSettings, TradeGateV1


def _make_engine(engine_cls: type[PaperRunEngine] | type[BacktestRunEngine]) -> PaperRunEngine | BacktestRunEngine:
    engine = engine_cls.__new__(engine_cls)
    engine._runtime_state = {
        "intent_context": {
            "intent-1": {
                "first_submit_ts_ms": 1_000,
                "initial_ref_price": 100.0,
                "reason_code": "TEST",
                "exec_profile": {"price_mode": "JOIN"},
                "strategy_meta": {},
            }
        },
        "fill_records": [],
        "slippage_bps": [],
        "order_exec_profile_by_order_id": {},
        "order_policy_diag_by_order_id": {},
    }
    engine._runtime_counters = {
        "orders_submitted": 0,
        "orders_filled": 0,
        "orders_canceled": 0,
        "orders_partially_filled": 0,
        "orders_completed": 0,
        "fill_events_total": 0,
        "intents_failed": 0,
        "cancels_total": 0,
        "replaces_total": 0,
        "aborted_timeout_total": 0,
        "dust_abort_total": 0,
        "order_supervisor_reasons": {},
    }
    return engine


def _apply_update(
    *,
    engine: PaperRunEngine | BacktestRunEngine,
    update: PaperExecutionUpdate | BacktestExecutionUpdate,
    run_dir: Path,
) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    trade_gate = TradeGateV1(
        GateSettings(
            per_trade_krw=10_000.0,
            max_positions=2,
            min_order_krw=5_000.0,
            max_consecutive_failures=3,
            cooldown_sec_after_fail=60,
        )
    )
    with JsonlEventStore(run_dir, write_equity=False) as store:
        def append_event(event_type: str, ts_ms: int, payload: dict[str, object] | None = None) -> None:
            record = {"event_type": event_type, "ts_ms": int(ts_ms), "payload": payload or {}}
            events.append(record)
            store.append_event(event_type=event_type, ts_ms=ts_ms, payload=payload)

        kwargs = {
            "update": update,
            "trade_gate": trade_gate,
            "event_store": store,
            "append_event": append_event,
            "ts_ms": 1_500,
            "counters": engine._runtime_counters,
        }
        if isinstance(engine, PaperRunEngine):
            kwargs["strategy_adapter"] = None
        engine._apply_execution_update(**kwargs)
    return events


def _order(
    *,
    order_id: str,
    state: str,
    updated_ts_ms: int,
    volume_filled: float,
    failure_reason: str | None = None,
) -> PaperOrder:
    return PaperOrder(
        order_id=order_id,
        intent_id="intent-1",
        state=state,
        created_ts_ms=1_000,
        updated_ts_ms=updated_ts_ms,
        market="KRW-BTC",
        side="bid",
        ord_type="limit",
        time_in_force="gtc",
        price=100.0,
        volume_req=10.0,
        volume_filled=volume_filled,
        avg_fill_price=101.0,
        fee_paid_quote=0.0,
        maker_or_taker="taker",
        failure_reason=failure_reason,
    )


@pytest.mark.parametrize(
    ("engine_cls", "update_cls"),
    [
        (PaperRunEngine, PaperExecutionUpdate),
        (BacktestRunEngine, BacktestExecutionUpdate),
    ],
)
def test_apply_execution_update_splits_first_fill_and_completion_timing(
    tmp_path: Path,
    engine_cls: type[PaperRunEngine] | type[BacktestRunEngine],
    update_cls: type[PaperExecutionUpdate] | type[BacktestExecutionUpdate],
) -> None:
    engine = _make_engine(engine_cls)

    partial_order = _order(order_id="paper-order-1", state="PARTIAL", updated_ts_ms=1_500, volume_filled=4.0)
    partial_fill = FillEvent(
        order_id="paper-order-1",
        market="KRW-BTC",
        ts_ms=1_500,
        price=101.0,
        volume=4.0,
        fee_quote=0.4,
    )
    partial_events = _apply_update(
        engine=engine,
        update=update_cls(
            orders_with_fill=[partial_order],
            order_states_after_fill={partial_order.order_id: partial_order},
            fills=[partial_fill],
        ),
        run_dir=tmp_path / "partial",
    )

    filled_order = _order(order_id="paper-order-1", state="FILLED", updated_ts_ms=2_500, volume_filled=10.0)
    filled_fill = FillEvent(
        order_id="paper-order-1",
        market="KRW-BTC",
        ts_ms=2_500,
        price=101.0,
        volume=6.0,
        fee_quote=0.6,
    )
    completion_events = _apply_update(
        engine=engine,
        update=update_cls(
            orders_with_fill=[filled_order],
            order_states_after_fill={filled_order.order_id: filled_order},
            orders_filled=[filled_order],
            fills=[filled_fill],
            success_markets=["KRW-BTC"],
        ),
        run_dir=tmp_path / "filled",
    )
    events = partial_events + completion_events

    assert engine._runtime_counters["orders_filled"] == 1
    assert engine._runtime_counters["orders_partially_filled"] == 1
    assert engine._runtime_counters["orders_completed"] == 1
    assert engine._runtime_counters["fill_events_total"] == 2
    assert engine._runtime_state["time_to_first_fill_ms"] == [500.0]
    assert engine._runtime_state["time_to_complete_fill_ms"] == [1500.0]
    assert engine._runtime_state["time_to_fill_ms"] == [1500.0]
    assert any(item["event_type"] == "ORDER_PARTIAL" for item in events)


@pytest.mark.parametrize(
    ("engine_cls", "update_cls"),
    [
        (PaperRunEngine, PaperExecutionUpdate),
        (BacktestRunEngine, BacktestExecutionUpdate),
    ],
)
def test_apply_execution_update_reports_partial_cancelled_order_as_partial_fill(
    tmp_path: Path,
    engine_cls: type[PaperRunEngine] | type[BacktestRunEngine],
    update_cls: type[PaperExecutionUpdate] | type[BacktestExecutionUpdate],
) -> None:
    engine = _make_engine(engine_cls)
    partial_cancelled_order = _order(
        order_id="paper-order-2",
        state="CANCELED",
        updated_ts_ms=1_500,
        volume_filled=6.0,
        failure_reason="IOC_PARTIAL_CANCELLED_REMAINDER",
    )
    partial_fill = FillEvent(
        order_id="paper-order-2",
        market="KRW-BTC",
        ts_ms=1_500,
        price=101.0,
        volume=6.0,
        fee_quote=0.6,
    )

    events = _apply_update(
        engine=engine,
        update=update_cls(
            orders_with_fill=[partial_cancelled_order],
            order_states_after_fill={partial_cancelled_order.order_id: partial_cancelled_order},
            fills=[partial_fill],
        ),
        run_dir=tmp_path / "partial_cancelled",
    )

    assert engine._runtime_counters["orders_filled"] == 1
    assert engine._runtime_counters["orders_partially_filled"] == 1
    assert engine._runtime_counters["orders_completed"] == 0
    assert engine._runtime_counters["fill_events_total"] == 1
    assert engine._runtime_state["time_to_first_fill_ms"] == [500.0]
    assert engine._runtime_state["time_to_complete_fill_ms"] == []
    partial_events = [item for item in events if item["event_type"] == "ORDER_PARTIAL"]
    assert partial_events
    assert partial_events[0]["payload"]["failure_reason"] == "IOC_PARTIAL_CANCELLED_REMAINDER"
