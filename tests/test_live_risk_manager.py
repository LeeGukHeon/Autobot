from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

from autobot.live.risk_loop import apply_executor_event, apply_ticker_event
from autobot.live.state_store import LiveStateStore, RiskPlanRecord
from autobot.risk.live_risk_manager import LiveRiskManager
from autobot.risk.models import RiskManagerConfig


@dataclass
class _SubmitCall:
    identifier: str
    market: str
    side: str
    price: float
    volume: float


@dataclass
class _ReplaceCall:
    prev_order_uuid: str | None
    prev_order_identifier: str | None
    new_identifier: str
    new_price_str: str
    new_volume_str: str


class _FakeExecutorGateway:
    def __init__(self) -> None:
        self.submit_calls: list[_SubmitCall] = []
        self.replace_calls: list[_ReplaceCall] = []
        self._submit_seq = 0
        self._replace_seq = 0

    def submit_intent(self, *, intent, identifier: str, meta_json: str):  # noqa: ANN001, ANN201
        _ = meta_json
        self._submit_seq += 1
        self.submit_calls.append(
            _SubmitCall(
                identifier=identifier,
                market=str(intent.market),
                side=str(intent.side),
                price=float(intent.price),
                volume=float(intent.volume),
            )
        )
        return SimpleNamespace(
            accepted=True,
            reason="accepted",
            upbit_uuid=f"exit-uuid-{self._submit_seq}",
            identifier=identifier,
        )

    def replace_order(
        self,
        *,
        intent_id: str,
        prev_order_uuid: str | None = None,
        prev_order_identifier: str | None = None,
        new_identifier: str,
        new_price_str: str,
        new_volume_str: str,
        new_time_in_force: str | None = None,
    ):  # noqa: ANN201
        _ = intent_id, new_time_in_force
        self._replace_seq += 1
        self.replace_calls.append(
            _ReplaceCall(
                prev_order_uuid=prev_order_uuid,
                prev_order_identifier=prev_order_identifier,
                new_identifier=new_identifier,
                new_price_str=new_price_str,
                new_volume_str=new_volume_str,
            )
        )
        return SimpleNamespace(
            accepted=True,
            reason="replaced",
            cancelled_order_uuid=prev_order_uuid or "prev-uuid",
            new_order_uuid=f"new-exit-uuid-{self._replace_seq}",
            new_identifier=new_identifier,
        )


def test_risk_manager_tp_trigger_submits_exit(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    gateway = _FakeExecutorGateway()
    with LiveStateStore(db_path) as store:
        manager = LiveRiskManager(
            store=store,
            executor_gateway=gateway,
            config=RiskManagerConfig(
                exit_aggress_bps=10.0,
                default_sl_pct=2.0,
                default_tp_pct=3.0,
                default_trailing_enabled=False,
            ),
        )
        plan = manager.attach_default_risk(
            market="KRW-BTC",
            entry_price=100.0,
            qty=1.25,
            ts_ms=1000,
        )
        actions = manager.evaluate_price(market="KRW-BTC", last_price=104.0, ts_ms=2000)
        persisted = store.risk_plan_by_id(plan_id=plan.plan_id)

    assert any(item["type"] == "risk_exit_submitted" for item in actions)
    assert len(gateway.submit_calls) == 1
    assert gateway.submit_calls[0].side == "ask"
    assert gateway.submit_calls[0].market == "KRW-BTC"
    assert persisted is not None
    assert persisted["state"] == "EXITING"
    assert persisted["current_exit_order_uuid"] == "exit-uuid-1"


def test_risk_manager_trailing_watermark_then_trigger(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    gateway = _FakeExecutorGateway()
    with LiveStateStore(db_path) as store:
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="trailing-1",
                market="KRW-ETH",
                side="long",
                entry_price_str="100",
                qty_str="2",
                tp_enabled=False,
                sl_enabled=False,
                trailing_enabled=True,
                trail_pct=0.05,
                state="ACTIVE",
                last_eval_ts_ms=0,
                last_action_ts_ms=0,
                replace_attempt=0,
                created_ts=1000,
                updated_ts=1000,
            )
        )
        manager = LiveRiskManager(
            store=store,
            executor_gateway=gateway,
            config=RiskManagerConfig(exit_aggress_bps=10.0, default_trail_pct=0.05),
        )
        watermark_actions = manager.evaluate_price(market="KRW-ETH", last_price=110.0, ts_ms=1500)
        trigger_actions = manager.evaluate_price(market="KRW-ETH", last_price=104.4, ts_ms=2500)
        persisted = store.risk_plan_by_id(plan_id="trailing-1")

    assert any(item["type"] == "risk_trailing_watermark" for item in watermark_actions)
    assert any(item["type"] == "risk_exit_submitted" for item in trigger_actions)
    assert len(gateway.submit_calls) == 1
    assert persisted is not None
    assert persisted["state"] == "EXITING"
    assert float(persisted["trailing"]["high_watermark_price_str"]) == 110.0


def test_risk_manager_replace_and_close_recovery(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    gateway = _FakeExecutorGateway()
    with LiveStateStore(db_path) as store:
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="replace-1",
                market="KRW-XRP",
                side="long",
                entry_price_str="500",
                qty_str="100",
                tp_enabled=True,
                tp_pct=3.0,
                sl_enabled=True,
                sl_pct=2.0,
                trailing_enabled=False,
                state="EXITING",
                last_eval_ts_ms=1000,
                last_action_ts_ms=1000,
                current_exit_order_uuid="exit-prev-uuid",
                current_exit_order_identifier="AUTOBOT-RISK-old",
                replace_attempt=0,
                created_ts=1000,
                updated_ts=1000,
            )
        )
        manager = LiveRiskManager(
            store=store,
            executor_gateway=gateway,
            config=RiskManagerConfig(order_timeout_sec=1, replace_max=2, exit_aggress_bps=10.0),
        )
        replace_actions = manager.evaluate_price(market="KRW-XRP", last_price=480.0, ts_ms=4000)
        persisted_after_replace = store.risk_plan_by_id(plan_id="replace-1")
        assert persisted_after_replace is not None
        current_identifier = str(persisted_after_replace["current_exit_order_identifier"])
        close_action = manager.handle_executor_event(
            {
                "event_type": "ORDER_UPDATE",
                "ts_ms": 5000,
                "payload": {
                    "event_name": "ORDER_STATE",
                    "identifier": current_identifier,
                    "state": "done",
                },
            }
        )
        persisted_after_close = store.risk_plan_by_id(plan_id="replace-1")

    assert any(item["type"] == "risk_exit_replaced" for item in replace_actions)
    assert len(gateway.replace_calls) == 1
    assert persisted_after_replace is not None
    assert persisted_after_replace["state"] == "EXITING"
    assert persisted_after_replace["replace_attempt"] == 1
    assert close_action is not None
    assert close_action["type"] == "risk_closed"
    assert persisted_after_close is not None
    assert persisted_after_close["state"] == "CLOSED"


def test_risk_loop_helpers_forward_events(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    gateway = _FakeExecutorGateway()
    with LiveStateStore(db_path) as store:
        manager = LiveRiskManager(
            store=store,
            executor_gateway=gateway,
            config=RiskManagerConfig(default_tp_pct=1.0, default_sl_pct=0.0),
        )
        manager.attach_default_risk(market="KRW-SOL", entry_price=100.0, qty=1.0, ts_ms=1000)

        class _Ticker:
            market = "KRW-SOL"
            trade_price = 101.5
            ts_ms = 2000

        ticker_actions = apply_ticker_event(risk_manager=manager, event=_Ticker())
        assert any(item["type"] == "risk_exit_submitted" for item in ticker_actions)

        persisted = store.list_risk_plans(states=("EXITING",))
        assert len(persisted) == 1
        identifier = str(persisted[0]["current_exit_order_identifier"])
        close_action = apply_executor_event(
            risk_manager=manager,
            event={
                "event_type": "ORDER_UPDATE",
                "ts_ms": 2500,
                "payload": {
                    "event_name": "ORDER_STATE",
                    "identifier": identifier,
                    "state": "done",
                },
            },
        )
        closed = store.list_risk_plans(states=("CLOSED",))

    assert close_action is not None
    assert close_action["type"] == "risk_closed"
    assert len(closed) == 1
