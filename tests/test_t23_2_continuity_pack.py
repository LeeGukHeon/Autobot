from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import pytest

from autobot.backtest.strategy_adapter import StrategyStepResult
from autobot.dashboard_server import build_dashboard_snapshot
from autobot.live.daemon import LiveDaemonSettings
from autobot.live.model_alpha_runtime import LiveModelAlphaRuntimeSettings, run_live_model_alpha_runtime
from autobot.live.model_risk_plan import build_model_derived_risk_records
from autobot.live.reconcile import resume_risk_plans_after_reconcile
from autobot.live.state_store import IntentRecord, LiveStateStore, OrderRecord
from autobot.models.predictor import load_predictor_from_registry
from autobot.models.registry import RegistrySavePayload, save_run, set_champion_pointer
from autobot.strategy.model_alpha_v1 import ModelAlphaExitSettings, ModelAlphaSettings, build_model_alpha_exit_plan_payload
from autobot.upbit.ws.models import TickerEvent


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_registry_run(project_root: Path) -> tuple[Path, Path, str]:
    registry_root = project_root / "models" / "registry"
    run_id = "run-v4-continuity"
    run_dir = save_run(
        RegistrySavePayload(
            registry_root=registry_root,
            model_family="train_v4_crypto_cs",
            run_id=run_id,
            model_bundle={"model_type": "dummy", "estimator": {"coef": [1.0]}},
            metrics={"rows": {"train": 10, "valid": 5, "test": 5}},
            thresholds={"top_5pct": 0.61},
            feature_spec={"feature_columns": ["close", "rv_12"]},
            label_spec={"label_columns": ["y_cls"]},
            train_config={"feature_columns": ["close", "rv_12"]},
            data_fingerprint={"manifest_sha256": "continuity"},
            leaderboard_row={"run_id": run_id, "test_precision_top5": 0.73},
            model_card_text="# continuity",
            selection_recommendations={
                "recommended_threshold_key": "top_5pct",
                "by_threshold_key": {
                    "top_5pct": {
                        "recommended_top_pct": 0.5,
                        "recommended_min_candidates_per_ts": 1,
                        "eligible_ratio": 0.05,
                        "recommendation_source": "optimizer",
                    }
                },
            },
            selection_policy={
                "version": 1,
                "mode": "rank_effective_quantile",
                "selection_fraction": 0.025,
                "min_candidates_per_ts": 1,
                "threshold_key": "top_5pct",
                "recommended_top_pct": 0.5,
                "eligible_ratio": 0.05,
                "selection_recommendation_source": "optimizer",
            },
            selection_calibration={
                "version": 1,
                "mode": "identity_v1",
                "reason": "OK",
            },
            runtime_recommendations={
                "version": 1,
                "exit": {
                    "version": 1,
                    "recommended_exit_mode": "risk",
                    "recommended_exit_mode_source": "execution_backtest_grid_search_compare",
                    "recommended_exit_mode_reason_code": "RISK_EXECUTION_COMPARE_EDGE",
                    "recommended_hold_bars": 6,
                    "recommendation_source": "execution_backtest_grid_search",
                    "chosen_family": "risk",
                    "chosen_rule_id": "risk_h6_rv_12_tp2_sl1_tr1p5",
                    "hold_family_status": "supported",
                    "risk_family_status": "supported",
                    "family_compare_status": "supported",
                    "recommended_risk_scaling_mode": "fixed",
                    "recommended_risk_vol_feature": "rv_12",
                    "recommended_tp_vol_multiplier": 2.0,
                    "recommended_sl_vol_multiplier": 1.0,
                    "recommended_trailing_vol_multiplier": 1.5,
                    "objective_score": 0.12,
                    "risk_objective_score": 0.18,
                    "grid_point": {"hold_bars": 6},
                    "risk_grid_point": {
                        "hold_bars": 6,
                        "risk_scaling_mode": "fixed",
                        "risk_vol_feature": "rv_12",
                        "tp_vol_multiplier": 2.0,
                        "sl_vol_multiplier": 1.0,
                        "trailing_vol_multiplier": 1.5,
                    },
                    "summary": {
                        "orders_filled": 12,
                        "realized_pnl_quote": 100.0,
                        "fill_rate": 0.90,
                        "max_drawdown_pct": 2.0,
                        "slippage_bps_mean": 5.0,
                    },
                    "risk_summary": {
                        "orders_filled": 12,
                        "realized_pnl_quote": 130.0,
                        "fill_rate": 0.94,
                        "max_drawdown_pct": 1.2,
                        "slippage_bps_mean": 3.2,
                    },
                    "hold_family": {
                        "status": "supported",
                        "rows_total": 4,
                        "comparable_rows": 4,
                        "best_rule_id": "hold_h6",
                        "best_comparable_rule_id": "hold_h6",
                    },
                    "risk_family": {
                        "status": "supported",
                        "rows_total": 4,
                        "comparable_rows": 4,
                        "best_rule_id": "risk_h6_rv_12_tp2_sl1_tr1p5",
                        "best_comparable_rule_id": "risk_h6_rv_12_tp2_sl1_tr1p5",
                    },
                    "family_compare": {
                        "status": "supported",
                        "decision": "candidate_edge",
                        "comparable": True,
                        "reason_codes": [],
                        "hold_rule_id": "hold_h6",
                        "risk_rule_id": "risk_h6_rv_12_tp2_sl1_tr1p5",
                    },
                    "exit_mode_compare": {
                        "decision": "candidate_edge",
                        "comparable": True,
                        "reasons": [],
                    },
                },
                "trade_action": {
                    "version": 1,
                    "status": "ready",
                    "source": "walk_forward_oos_trade_replay",
                    "runtime_decision_source": "continuous_conditional_action_value",
                    "risk_feature_name": "rv_12",
                    "state_feature_names": ["selection_score", "rv_12"],
                    "summary": {
                        "hold_bins_recommended": 0,
                        "risk_bins_recommended": 1,
                    },
                    "by_bin": [
                        {
                            "edge_bin": 2,
                            "risk_bin": 0,
                            "recommended_action": "risk",
                            "expected_edge": 0.012,
                            "expected_downside_deviation": 0.004,
                            "expected_es": 0.006,
                            "expected_action_value": 1.4,
                            "recommended_notional_multiplier": 1.1,
                            "sample_count": 8,
                        }
                    ],
                },
            },
        )
    )
    set_champion_pointer(
        registry_root,
        "train_v4_crypto_cs",
        run_id=run_id,
        score=0.73,
        score_key="test_precision_top5",
    )
    return registry_root, run_dir, run_id


def _seed_runtime_meta(project_root: Path) -> None:
    now_ms = int(time.time() * 1000)
    meta_dir = project_root / "data" / "raw_ws" / "upbit" / "_meta"
    _write_json(
        meta_dir / "ws_public_health.json",
        {
            "run_id": "ws-run-1",
            "updated_at_ms": now_ms,
            "connected": True,
            "last_rx_ts_ms": {"trade": now_ms, "orderbook": now_ms},
            "subscribed_markets_count": 1,
        },
    )
    _write_json(
        meta_dir / "ws_runs_summary.json",
        {"runs": [{"run_id": "ws-run-1", "rows_total": 10, "bytes_total": 100}]},
    )
    _write_json(
        project_root / "data" / "parquet" / "micro_v1" / "_meta" / "aggregate_report.json",
        {"run_id": "micro-run-1", "rows_written_total": 10},
    )


class _PrivateClient:
    def accounts(self):  # noqa: ANN201
        return [
            {"currency": "KRW", "balance": "50000", "locked": "0", "avg_buy_price": "0"},
            {"currency": "KITE", "balance": "5", "locked": "0", "avg_buy_price": "100"},
        ]

    def chance(self, *, market: str):  # noqa: ANN201
        _ = market
        return {
            "market": {
                "bid": {"min_total": "5000"},
                "ask": {"min_total": "5000"},
            },
            "bid_fee": "0.0005",
            "ask_fee": "0.0005",
        }

    def open_orders(self, *, states):  # noqa: ANN201
        _ = states
        return []

    def order(self, *, uuid: str | None = None, identifier: str | None = None):  # noqa: ANN201
        _ = uuid, identifier
        return {}


class _PublicClient:
    def markets(self, *, is_details: bool = False):  # noqa: ANN201
        _ = is_details
        return [{"market": "KRW-KITE"}]

    def orderbook_instruments(self, markets):  # noqa: ANN201
        _ = markets
        return [{"market": "KRW-KITE", "tick_size": 1}]


class _PublicWsClient:
    async def stream_ticker(self, markets, duration_sec=None):  # noqa: ANN201
        _ = markets, duration_sec
        yield TickerEvent(
            market="KRW-KITE",
            ts_ms=int(time.time() * 1000),
            trade_price=105.0,
            acc_trade_price_24h=1_000_000.0,
        )


class _FeatureProvider:
    def ingest_ticker(self, event):  # noqa: ANN201
        _ = event

    def build_frame(self, *, ts_ms: int, markets):  # noqa: ANN201
        _ = ts_ms, markets
        return None

    def last_build_stats(self):  # noqa: ANN201
        return {"provider": "continuity"}


class _BootstrapOnlyStrategy:
    def __init__(self) -> None:
        self.fills = []

    def on_ts(self, *, ts_ms: int, active_markets, latest_prices, open_markets):  # noqa: ANN201
        _ = ts_ms, active_markets, latest_prices, open_markets
        return StrategyStepResult(intents=(), scored_rows=0, eligible_rows=0, selected_rows=0)

    def on_fill(self, event):  # noqa: ANN201
        self.fills.append(event)


def test_t23_2_continuity_pack_keeps_runtime_resume_and_dashboard_consistent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    project_root = tmp_path
    registry_root, run_dir, run_id = _write_registry_run(project_root)
    _seed_runtime_meta(project_root)

    predictor = load_predictor_from_registry(
        registry_root=registry_root,
        model_ref="champion",
        model_family="train_v4_crypto_cs",
    )
    exit_doc = dict(predictor.runtime_recommendations["exit"])

    bootstrap_strategy = _BootstrapOnlyStrategy()
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: bootstrap_strategy)

    entry_ts_ms = 1_000
    now_ts_ms = 5_000
    plan_payload = build_model_alpha_exit_plan_payload(
        settings=ModelAlphaSettings(
            exit=ModelAlphaExitSettings(
                mode="risk",
                hold_bars=6,
                tp_pct=0.02,
                sl_pct=0.01,
                trailing_pct=0.015,
                expected_exit_slippage_bps=3.0,
                expected_exit_fee_bps=5.0,
            )
        ),
        row=None,
        interval_ms=300_000,
    )
    position_record, risk_plan_record = build_model_derived_risk_records(
        market="KRW-KITE",
        base_currency="KITE",
        base_amount=5.0,
        avg_entry_price=100.0,
        plan_payload=plan_payload,
        created_ts=entry_ts_ms,
        updated_ts=now_ts_ms,
        intent_id="intent-continuity-1",
    )

    settings = LiveModelAlphaRuntimeSettings(
        daemon=LiveDaemonSettings(
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            unknown_open_orders_policy="ignore",
            unknown_positions_policy="import_as_unmanaged",
            allow_cancel_external_orders=False,
            poll_interval_sec=60,
            startup_reconcile=False,
            duration_sec=1,
            registry_root=str(registry_root),
            runtime_model_ref_source="champion_v4",
            runtime_model_family="train_v4_crypto_cs",
            ws_public_raw_root=str(project_root / "data" / "raw_ws" / "upbit" / "public"),
            ws_public_meta_dir=str(project_root / "data" / "raw_ws" / "upbit" / "_meta"),
            ws_public_stale_threshold_sec=180,
            micro_aggregate_report_path=str(project_root / "data" / "parquet" / "micro_v1" / "_meta" / "aggregate_report.json"),
            rollout_mode="shadow",
            rollout_target_unit="autobot-live-alpha.service",
        )
    )

    db_path = project_root / "data" / "state" / "live" / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.set_runtime_contract(
            payload={
                "live_runtime_model_run_id": run_id,
                "champion_pointer_run_id": run_id,
                "model_family_resolved": "train_v4_crypto_cs",
            },
            ts_ms=entry_ts_ms,
        )
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-continuity-1",
                ts_ms=entry_ts_ms,
                market="KRW-KITE",
                side="bid",
                price=100.0,
                volume=5.0,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                status="SUBMITTED",
                meta_json=json.dumps(
                    {
                        "submit_result": {"accepted": True, "order_uuid": "entry-order-continuity-1"},
                        "strategy": {
                            "meta": {
                                "model_prob": 0.91,
                                "selection_policy_mode": "rank_effective_quantile",
                                "trade_action": {
                                    "recommended_action": "risk",
                                    "expected_edge": 0.012,
                                    "expected_downside_deviation": 0.004,
                                    "expected_es": 0.006,
                                    "expected_action_value": 1.4,
                                    "decision_source": "continuous_conditional_action_value",
                                    "recommended_notional_multiplier": 1.1,
                                },
                                "exit_recommendation": {
                                    "recommended_exit_mode": exit_doc.get("recommended_exit_mode"),
                                    "recommended_exit_mode_source": exit_doc.get("recommended_exit_mode_source"),
                                    "recommended_exit_mode_reason_code": exit_doc.get("recommended_exit_mode_reason_code"),
                                    "recommended_hold_bars": exit_doc.get("recommended_hold_bars"),
                                    "chosen_family": exit_doc.get("chosen_family"),
                                    "chosen_rule_id": exit_doc.get("chosen_rule_id"),
                                    "hold_family_status": exit_doc.get("hold_family_status"),
                                    "risk_family_status": exit_doc.get("risk_family_status"),
                                    "family_compare_status": exit_doc.get("family_compare_status"),
                                    "family_compare_reason_codes": list(
                                        (exit_doc.get("family_compare") or {}).get("reason_codes") or []
                                    ),
                                },
                                "model_exit_plan": plan_payload,
                            }
                        },
                        "admissibility": {
                            "decision": {
                                "expected_net_edge_bps": 88.0,
                            },
                            "sizing": {"fee_rate": 0.0005},
                            "snapshot": {"bid_fee": 0.0005, "ask_fee": 0.0005},
                        },
                        "runtime": {
                            "live_runtime_model_run_id": run_id,
                            "model_family": "train_v4_crypto_cs",
                        },
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="entry-order-continuity-1",
                identifier="AUTOBOT-autobot-001-intent-continuity-1-1000-a",
                market="KRW-KITE",
                side="bid",
                ord_type="limit",
                price=100.0,
                volume_req=5.0,
                volume_filled=5.0,
                state="done",
                created_ts=entry_ts_ms,
                updated_ts=entry_ts_ms + 100,
                intent_id="intent-continuity-1",
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="entry-order-continuity-1",
            )
        )
        store.upsert_position(position_record)
        store.upsert_risk_plan(risk_plan_record)

        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=None,
            )
        )
        journal_rows = store.list_trade_journal()
        plan_row = store.risk_plan_by_id(plan_id=risk_plan_record.plan_id)
        runtime_health = store.live_runtime_health()

    assert predictor.run_dir == run_dir
    assert predictor.runtime_recommendations["exit"]["chosen_family"] == "risk"
    assert summary["strategy_predictor_run_id"] == run_id
    assert len(bootstrap_strategy.fills) == 1
    assert bootstrap_strategy.fills[0].meta["model_exit_plan"]["tp_ratio"] == pytest.approx(0.02)
    assert bootstrap_strategy.fills[0].meta["model_exit_plan"]["sl_ratio"] == pytest.approx(0.01)
    assert bootstrap_strategy.fills[0].meta["model_exit_plan"]["trailing_ratio"] == pytest.approx(0.015)
    assert runtime_health is not None
    assert runtime_health["live_runtime_model_run_id"] == run_id
    assert journal_rows[0]["status"] == "OPEN"
    assert journal_rows[0]["entry_intent_id"] == "intent-continuity-1"
    assert journal_rows[0]["plan_id"] == risk_plan_record.plan_id
    assert journal_rows[0]["entry_meta"]["strategy"]["meta"]["model_exit_plan"]["tp_ratio"] == pytest.approx(0.02)
    assert plan_row is not None
    assert plan_row["plan_source"] == "model_alpha_v1"
    assert plan_row["source_intent_id"] == "intent-continuity-1"
    assert plan_row["tp"]["tp_pct"] == pytest.approx(2.0)
    assert plan_row["sl"]["sl_pct"] == pytest.approx(1.0)
    assert plan_row["trailing"]["trail_pct"] == pytest.approx(0.015)

    with LiveStateStore(db_path) as store:
        resume_report = resume_risk_plans_after_reconcile(store=store, ts_ms=now_ts_ms + 10_000)
        resumed_plan = store.risk_plan_by_id(plan_id=risk_plan_record.plan_id)
        resumed_journal = store.list_trade_journal()[0]

    assert resume_report["halted"] is False
    assert resume_report["counts"]["plans_kept_active"] == 1
    assert resumed_plan is not None
    assert resumed_plan["state"] == "ACTIVE"
    assert resumed_plan["plan_source"] == "model_alpha_v1"
    assert resumed_plan["source_intent_id"] == "intent-continuity-1"
    assert resumed_journal["entry_meta"]["strategy"]["meta"]["model_exit_plan"]["trailing_ratio"] == pytest.approx(0.015)

    snapshot = build_dashboard_snapshot(project_root)
    live_state = snapshot["live"]["states"][0]
    dashboard_plan = live_state["active_risk_plans"][0]
    dashboard_trade = live_state["recent_trades"][0]
    runtime_artifacts = live_state["runtime_artifacts"]["runtime_recommendations"]

    assert live_state["runtime_health"]["live_runtime_model_run_id"] == run_id
    assert runtime_artifacts["recommended_exit_mode"] == "risk"
    assert runtime_artifacts["chosen_family"] == "risk"
    assert dashboard_plan["plan_id"] == risk_plan_record.plan_id
    assert dashboard_plan["source_intent_id"] == "intent-continuity-1"
    assert dashboard_plan["tp_pct"] == pytest.approx(2.0)
    assert dashboard_plan["sl_pct"] == pytest.approx(1.0)
    assert dashboard_plan["trail_pct"] == pytest.approx(0.015)
    assert dashboard_trade["entry_intent_id"] == "intent-continuity-1"
    assert dashboard_trade["plan_id"] == risk_plan_record.plan_id
    assert dashboard_trade["exit_recommendation_chosen_family"] == "risk"
    assert dashboard_trade["entry_meta"]["strategy"]["meta"]["model_exit_plan"]["tp_ratio"] == pytest.approx(0.02)
    assert dashboard_trade["entry_meta"]["strategy"]["meta"]["model_exit_plan"]["sl_ratio"] == pytest.approx(0.01)
    assert dashboard_trade["entry_meta"]["strategy"]["meta"]["model_exit_plan"]["trailing_ratio"] == pytest.approx(0.015)
