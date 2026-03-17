from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from autobot.live.breakers import ACTION_FULL_KILL_SWITCH, ACTION_HALT_NEW_INTENTS, arm_breaker, record_counter_failure, breaker_status
from autobot.live.risk_loop import apply_executor_event, apply_ticker_event
from autobot.live.state_store import LiveStateStore, OrderRecord, PositionRecord, RiskPlanRecord
from autobot.risk.live_risk_manager import LiveRiskManager
from autobot.risk.models import RiskManagerConfig
from autobot.strategy.micro_snapshot import MicroSnapshot
from autobot.strategy.operational_overlay_v1 import ModelAlphaOperationalSettings


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


class _DoneOrderReplaceGateway(_FakeExecutorGateway):
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
        _ = intent_id, prev_order_uuid, prev_order_identifier, new_identifier, new_price_str, new_volume_str, new_time_in_force
        return SimpleNamespace(
            accepted=False,
            reason="이미 체결된 주문입니다. | status=400 | error=done_order | POST /v1/orders/cancel_and_new",
            cancelled_order_uuid=None,
            new_order_uuid=None,
            new_identifier=new_identifier,
        )

def _micro_snapshot(
    *,
    market: str,
    ts_ms: int,
    trade_imbalance: float,
    spread_bps_mean: float,
    depth_top5_notional_krw: float,
    trade_coverage_ms: int = 5_000,
    book_coverage_ms: int = 5_000,
) -> MicroSnapshot:
    return MicroSnapshot(
        market=market,
        snapshot_ts_ms=ts_ms,
        last_event_ts_ms=ts_ms,
        trade_events=32,
        trade_coverage_ms=trade_coverage_ms,
        trade_notional_krw=150_000.0,
        trade_imbalance=trade_imbalance,
        trade_source="ws",
        spread_bps_mean=spread_bps_mean,
        depth_top5_notional_krw=depth_top5_notional_krw,
        book_events=24,
        book_coverage_ms=book_coverage_ms,
        book_available=True,
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
        exit_order = store.order_by_uuid(uuid="exit-uuid-1")

    assert any(item["type"] == "risk_exit_submitted" for item in actions)
    assert len(gateway.submit_calls) == 1
    assert gateway.submit_calls[0].identifier.startswith("AUTOBOT-autobot-001-RISK-")
    assert gateway.submit_calls[0].side == "ask"
    assert gateway.submit_calls[0].market == "KRW-BTC"
    assert persisted is not None
    assert persisted["state"] == "EXITING"
    assert persisted["current_exit_order_uuid"] == "exit-uuid-1"
    assert exit_order is not None
    assert exit_order["tp_sl_link"] == plan.plan_id
    assert exit_order["local_state"] == "OPEN"


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


def test_risk_manager_micro_overlay_arms_profit_lock_trailing_and_exits_on_drawdown(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    gateway = _FakeExecutorGateway()
    with LiveStateStore(db_path) as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-BTC",
                base_currency="BTC",
                base_amount=1.0,
                avg_entry_price=100.0,
                updated_ts=1000,
                tp_json=json.dumps({"enabled": False, "source": "model_alpha_v1"}, ensure_ascii=False),
                sl_json=json.dumps({"enabled": True, "source": "model_alpha_v1", "sl_pct": 5.0}, ensure_ascii=False),
                trailing_json=json.dumps({"enabled": False, "source": "model_alpha_v1"}, ensure_ascii=False),
                managed=True,
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="overlay-1",
                market="KRW-BTC",
                side="long",
                entry_price_str="100",
                qty_str="1",
                tp_enabled=False,
                sl_enabled=True,
                sl_pct=5.0,
                trailing_enabled=False,
                state="ACTIVE",
                last_eval_ts_ms=0,
                last_action_ts_ms=0,
                replace_attempt=0,
                created_ts=1000,
                updated_ts=1000,
                plan_source="model_alpha_v1",
                source_intent_id="intent-overlay-1",
            )
        )
        manager = LiveRiskManager(
            store=store,
            executor_gateway=gateway,
            config=RiskManagerConfig(exit_aggress_bps=10.0),
            micro_overlay_settings=ModelAlphaOperationalSettings(enabled=True),
        )
        first_actions = manager.evaluate_price(
            market="KRW-BTC",
            last_price=104.0,
            ts_ms=2000,
            micro_snapshot=_micro_snapshot(
                market="KRW-BTC",
                ts_ms=2000,
                trade_imbalance=-0.9,
                spread_bps_mean=45.0,
                depth_top5_notional_krw=40_000.0,
            ),
        )
        persisted_mid = store.risk_plan_by_id(plan_id="overlay-1")
        position_mid = store.position_by_market(market="KRW-BTC")
        second_actions = manager.evaluate_price(
            market="KRW-BTC",
            last_price=102.8,
            ts_ms=2500,
            micro_snapshot=_micro_snapshot(
                market="KRW-BTC",
                ts_ms=2500,
                trade_imbalance=-0.9,
                spread_bps_mean=45.0,
                depth_top5_notional_krw=40_000.0,
            ),
        )
        persisted_final = store.risk_plan_by_id(plan_id="overlay-1")

    assert any(item["type"] == "risk_micro_overlay_applied" for item in first_actions)
    assert persisted_mid is not None
    assert persisted_mid["plan_source"] == "model_alpha_v1_micro_overlay"
    assert persisted_mid["trailing"]["enabled"] is True
    assert persisted_mid["trailing"]["trail_pct"] is not None
    assert persisted_mid["trailing"]["trail_pct"] < 0.02
    assert position_mid is not None
    assert position_mid["trailing"]["enabled"] is True
    assert position_mid["trailing"]["high_watermark_price_str"] == "104"
    assert any(item["type"] == "risk_exit_submitted" for item in second_actions)
    assert persisted_final is not None
    assert persisted_final["state"] == "EXITING"


def test_risk_manager_micro_overlay_does_not_compound_sl_forever_across_ticks(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    gateway = _FakeExecutorGateway()
    with LiveStateStore(db_path) as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-BTC",
                base_currency="BTC",
                base_amount=1.0,
                avg_entry_price=100.0,
                updated_ts=1000,
                tp_json=json.dumps({"enabled": False, "source": "model_alpha_v1"}, ensure_ascii=False),
                sl_json=json.dumps(
                    {"enabled": True, "source": "model_alpha_v1", "sl_pct": 1.0, "base_sl_pct": 0.01},
                    ensure_ascii=False,
                ),
                trailing_json=json.dumps({"enabled": False, "source": "model_alpha_v1"}, ensure_ascii=False),
                managed=True,
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="overlay-stable",
                market="KRW-BTC",
                side="long",
                entry_price_str="100",
                qty_str="1",
                tp_enabled=False,
                sl_enabled=True,
                sl_pct=1.0,
                trailing_enabled=False,
                state="ACTIVE",
                last_eval_ts_ms=0,
                last_action_ts_ms=0,
                replace_attempt=0,
                created_ts=1000,
                updated_ts=1000,
                plan_source="model_alpha_v1",
                source_intent_id="intent-overlay-stable",
            )
        )
        manager = LiveRiskManager(
            store=store,
            executor_gateway=gateway,
            config=RiskManagerConfig(exit_aggress_bps=10.0),
            micro_overlay_settings=ModelAlphaOperationalSettings(enabled=True),
        )
        snapshot = _micro_snapshot(
            market="KRW-BTC",
            ts_ms=2000,
            trade_imbalance=-0.9,
            spread_bps_mean=45.0,
            depth_top5_notional_krw=40_000.0,
        )
        manager.evaluate_price(market="KRW-BTC", last_price=104.0, ts_ms=2000, micro_snapshot=snapshot)
        first = store.risk_plan_by_id(plan_id="overlay-stable")
        manager.evaluate_price(market="KRW-BTC", last_price=104.0, ts_ms=2500, micro_snapshot=snapshot)
        second = store.risk_plan_by_id(plan_id="overlay-stable")

    assert first is not None
    assert second is not None
    assert float(second["sl"]["sl_pct"]) == pytest.approx(float(first["sl"]["sl_pct"]), rel=0.01)
    assert float(second["sl"]["sl_pct"]) > 0.90


def test_risk_manager_micro_overlay_does_not_change_plan_when_micro_state_is_healthy(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    gateway = _FakeExecutorGateway()
    with LiveStateStore(db_path) as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-ETH",
                base_currency="ETH",
                base_amount=1.0,
                avg_entry_price=100.0,
                updated_ts=1000,
                tp_json=json.dumps({"enabled": False, "source": "model_alpha_v1"}, ensure_ascii=False),
                sl_json=json.dumps({"enabled": True, "source": "model_alpha_v1", "sl_pct": 5.0}, ensure_ascii=False),
                trailing_json=json.dumps({"enabled": False, "source": "model_alpha_v1"}, ensure_ascii=False),
                managed=True,
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="overlay-healthy",
                market="KRW-ETH",
                side="long",
                entry_price_str="100",
                qty_str="1",
                tp_enabled=False,
                sl_enabled=True,
                sl_pct=5.0,
                trailing_enabled=False,
                state="ACTIVE",
                last_eval_ts_ms=0,
                last_action_ts_ms=0,
                replace_attempt=0,
                created_ts=1000,
                updated_ts=1000,
                plan_source="model_alpha_v1",
                source_intent_id="intent-overlay-healthy",
            )
        )
        manager = LiveRiskManager(
            store=store,
            executor_gateway=gateway,
            config=RiskManagerConfig(exit_aggress_bps=10.0),
            micro_overlay_settings=ModelAlphaOperationalSettings(enabled=True),
        )
        actions = manager.evaluate_price(
            market="KRW-ETH",
            last_price=104.0,
            ts_ms=2000,
            micro_snapshot=_micro_snapshot(
                market="KRW-ETH",
                ts_ms=2000,
                trade_imbalance=0.8,
                spread_bps_mean=2.0,
                depth_top5_notional_krw=15_000_000.0,
                trade_coverage_ms=60_000,
                book_coverage_ms=60_000,
            ),
        )
        persisted = store.risk_plan_by_id(plan_id="overlay-healthy")

    assert not any(item["type"] == "risk_micro_overlay_applied" for item in actions)
    assert persisted is not None
    assert persisted["plan_source"] == "model_alpha_v1"
    assert persisted["trailing"]["enabled"] is False


def test_risk_manager_replace_and_close_recovery(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    gateway = _FakeExecutorGateway()
    with LiveStateStore(db_path) as store:
        store.upsert_order(
            OrderRecord(
                uuid="exit-prev-uuid",
                identifier="AUTOBOT-RISK-old",
                market="KRW-XRP",
                side="ask",
                ord_type="limit",
                price=490.0,
                volume_req=100.0,
                volume_filled=0.0,
                state="wait",
                created_ts=1000,
                updated_ts=1000,
                intent_id="intent-risk-old",
                tp_sl_link="replace-1",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="SUBMIT_ACCEPTED",
                event_source="test",
                replace_seq=0,
                root_order_uuid="exit-prev-uuid",
                prev_order_uuid=None,
                prev_order_identifier=None,
            )
        )
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
            tick_size_resolver=lambda market: 1.0 if market == "KRW-XRP" else None,
        )
        replace_actions = manager.evaluate_price(market="KRW-XRP", last_price=480.0, ts_ms=4000)
        persisted_after_replace = store.risk_plan_by_id(plan_id="replace-1")
        replaced_old_order = store.order_by_uuid(uuid="exit-prev-uuid")
        replaced_new_order = store.order_by_uuid(uuid="new-exit-uuid-1")
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
    assert str(gateway.replace_calls[0].new_identifier).startswith("AUTOBOT-autobot-001-RISKREP-")
    assert gateway.replace_calls[0].new_price_str == "480"
    assert persisted_after_replace is not None
    assert persisted_after_replace["state"] == "EXITING"
    assert persisted_after_replace["replace_attempt"] == 1
    assert replaced_old_order is not None
    assert replaced_old_order["state"] == "cancel"
    assert replaced_old_order["tp_sl_link"] == "replace-1"
    assert replaced_new_order is not None
    assert replaced_new_order["identifier"] == current_identifier
    assert replaced_new_order["tp_sl_link"] == "replace-1"
    assert replaced_new_order["state"] == "wait"
    assert close_action is not None
    assert close_action["type"] == "risk_closed"
    assert persisted_after_close is not None
    assert persisted_after_close["state"] == "CLOSED"


def test_risk_manager_replace_without_uuid_tracks_identifier_and_closes_on_identifier_event(tmp_path: Path) -> None:
    class _IdentifierOnlyReplaceGateway(_FakeExecutorGateway):
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
                reason="replace_accepted_new_order_pending_lookup",
                cancelled_order_uuid=prev_order_uuid or "prev-uuid",
                new_order_uuid=None,
                new_identifier=new_identifier,
            )

    db_path = tmp_path / "live_state.db"
    gateway = _IdentifierOnlyReplaceGateway()
    with LiveStateStore(db_path) as store:
        store.upsert_order(
            OrderRecord(
                uuid="exit-prev-uuid",
                identifier="AUTOBOT-RISK-old",
                market="KRW-XRP",
                side="ask",
                ord_type="limit",
                price=490.0,
                volume_req=100.0,
                volume_filled=0.0,
                state="wait",
                created_ts=1000,
                updated_ts=1000,
                intent_id="intent-risk-old",
                tp_sl_link="replace-id-only",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="SUBMIT_ACCEPTED",
                event_source="test",
                replace_seq=0,
                root_order_uuid="exit-prev-uuid",
                prev_order_uuid=None,
                prev_order_identifier=None,
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="replace-id-only",
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
            tick_size_resolver=lambda market: 1.0 if market == "KRW-XRP" else None,
        )
        replace_actions = manager.evaluate_price(market="KRW-XRP", last_price=480.0, ts_ms=4000)
        persisted_after_replace = store.risk_plan_by_id(plan_id="replace-id-only")
        replaced_old_order = store.order_by_uuid(uuid="exit-prev-uuid")
        followup_actions = manager.evaluate_price(market="KRW-XRP", last_price=479.0, ts_ms=4500)
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
        persisted_after_close = store.risk_plan_by_id(plan_id="replace-id-only")

    assert any(item["type"] == "risk_exit_replaced" for item in replace_actions)
    assert not any(item["type"] == "risk_exit_replaced" for item in followup_actions)
    assert len(gateway.replace_calls) == 1
    assert str(gateway.replace_calls[0].new_identifier).startswith("AUTOBOT-autobot-001-RISKREP-")
    assert persisted_after_replace is not None
    assert persisted_after_replace["state"] == "EXITING"
    assert persisted_after_replace["current_exit_order_uuid"] is None
    assert persisted_after_replace["current_exit_order_identifier"] != "AUTOBOT-RISK-old"
    assert persisted_after_replace["replace_attempt"] == 1
    assert persisted_after_replace["last_action_ts_ms"] == 4000
    assert replaced_old_order is not None
    assert replaced_old_order["state"] == "cancel"
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


def test_risk_manager_blocks_new_exit_when_breaker_active(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    gateway = _FakeExecutorGateway()
    with LiveStateStore(db_path) as store:
        arm_breaker(
            store,
            reason_codes=["MANUAL_KILL_SWITCH"],
            source="test",
            ts_ms=900,
            action=ACTION_FULL_KILL_SWITCH,
        )
        manager = LiveRiskManager(
            store=store,
            executor_gateway=gateway,
            config=RiskManagerConfig(default_tp_pct=1.0, default_sl_pct=0.0),
        )
        manager.attach_default_risk(market="KRW-BTC", entry_price=100.0, qty=1.0, ts_ms=1000)
        actions = manager.evaluate_price(market="KRW-BTC", last_price=101.5, ts_ms=2000)

    assert any(item["type"] == "risk_blocked_by_breaker" for item in actions)
    assert gateway.submit_calls == []


def test_risk_manager_allows_exit_when_breaker_only_halts_new_intents(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    gateway = _FakeExecutorGateway()
    with LiveStateStore(db_path) as store:
        arm_breaker(
            store,
            reason_codes=["LIVE_TEST_ORDER_REQUIRED"],
            source="test",
            ts_ms=900,
            action=ACTION_HALT_NEW_INTENTS,
        )
        manager = LiveRiskManager(
            store=store,
            executor_gateway=gateway,
            config=RiskManagerConfig(default_tp_pct=1.0, default_sl_pct=0.0),
        )
        manager.attach_default_risk(market="KRW-BTC", entry_price=100.0, qty=1.0, ts_ms=1000)
        actions = manager.evaluate_price(market="KRW-BTC", last_price=101.5, ts_ms=2000)

    assert any(item["type"] == "risk_exit_submitted" for item in actions)
    assert len(gateway.submit_calls) == 1


def test_risk_manager_timeout_trigger_submits_exit(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    gateway = _FakeExecutorGateway()
    with LiveStateStore(db_path) as store:
        manager = LiveRiskManager(
            store=store,
            executor_gateway=gateway,
            config=RiskManagerConfig(default_tp_pct=0.0, default_sl_pct=0.0),
            tick_size_resolver=lambda market: 1.0 if market == "KRW-KITE" else None,
        )
        manager.attach_model_risk(
            market="KRW-KITE",
            entry_price=442.0,
            qty=13.5,
            tp_pct=None,
            sl_pct=None,
            trailing_pct=None,
            timeout_ts_ms=2000,
            ts_ms=1000,
            plan_id="model-risk-intent-1",
            source_intent_id="intent-1",
        )
        actions = manager.evaluate_price(market="KRW-KITE", last_price=442.0, ts_ms=2000)
        persisted = store.risk_plan_by_id(plan_id="model-risk-intent-1")

    assert any(item["type"] == "risk_exit_submitted" and item["trigger_reason"] == "TIMEOUT" for item in actions)
    assert len(gateway.submit_calls) == 1
    assert gateway.submit_calls[0].price == 442.0
    assert persisted is not None
    assert persisted["state"] == "EXITING"
    assert persisted["timeout_ts_ms"] == 2000
    assert persisted["source_intent_id"] == "intent-1"


def test_risk_manager_done_order_replace_reject_does_not_keep_breaker_active(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    gateway = _DoneOrderReplaceGateway()
    with LiveStateStore(db_path) as store:
        record_counter_failure(
            store,
            counter_name="replace_reject",
            limit=1,
            source="test_seed",
            ts_ms=900,
            details={"attempt": 1},
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="replace-done-order",
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
            tick_size_resolver=lambda market: 1.0 if market == "KRW-XRP" else None,
        )
        actions = manager.evaluate_price(market="KRW-XRP", last_price=480.0, ts_ms=4000)
        persisted = store.risk_plan_by_id(plan_id="replace-done-order")
        status = breaker_status(store)

    assert any(item["type"] == "risk_replace_already_done" for item in actions)
    assert persisted is not None
    assert persisted["state"] == "EXITING"
    assert persisted["last_action_ts_ms"] == 4000
    assert status["active"] is False
    assert status["counters"]["replace_reject"]["count"] == 0
