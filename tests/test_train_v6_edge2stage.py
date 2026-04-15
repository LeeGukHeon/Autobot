from __future__ import annotations

from pathlib import Path

import polars as pl

from autobot.data.derived.market_state_training_slice_v1 import (
    MarketStateTrainingSliceBuildOptions,
    build_market_state_training_slice_v1,
)
from autobot.models.registry import load_json
from autobot.models.predictor import load_predictor_from_registry
from autobot.models.train_v6_edge2stage import (
    TrainV6Edge2StageOptions,
    _build_small_account_realism_summary,
    train_and_register_v6_edge2stage,
)


def test_train_v6_edge2stage_writes_registry_artifacts(tmp_path: Path) -> None:
    for offset in range(20):
        date_value = f"2026-04-{offset + 1:02d}"
        positive = (offset % 2) == 0
        _write_market_state_pair(
            tmp_path,
            date_value,
            "KRW-BTC",
            label_available=True,
            tradeable_value=1 if positive else 0,
            net_edge_20m_bps=8.0 if positive else -4.0,
        )
        _write_market_state_pair(
            tmp_path,
            date_value,
            "KRW-ETH",
            label_available=True,
            tradeable_value=0 if positive else 1,
            net_edge_20m_bps=-2.0 if positive else 7.0,
        )
    slice_summary = build_market_state_training_slice_v1(
        MarketStateTrainingSliceBuildOptions(
            start="2026-04-01",
            end="2026-04-20",
            markets=("KRW-BTC", "KRW-ETH"),
            market_state_root=tmp_path / "data" / "derived" / "market_state_v1",
            tradeable_label_root=tmp_path / "data" / "derived" / "tradeable_label_v1",
            net_edge_label_root=tmp_path / "data" / "derived" / "net_edge_label_v1",
            out_root=tmp_path / "data" / "derived" / "market_state_training_slice_v1",
        )
    )
    assert slice_summary.rows_total > 0

    result = train_and_register_v6_edge2stage(
        TrainV6Edge2StageOptions(
            dataset_root=tmp_path / "data" / "derived" / "market_state_training_slice_v1",
            registry_root=tmp_path / "registry",
            logs_root=tmp_path / "logs",
            model_family="train_v6_edge2stage",
            quote="KRW",
            start="2026-04-01",
            end="2026-04-20",
            seed=42,
            nthread=1,
        )
    )
    assert result.run_dir.exists()
    assert (result.run_dir / "model.bin").exists()
    assert (result.run_dir / "metrics.json").exists()
    assert (result.run_dir / "thresholds.json").exists()
    assert (result.run_dir / "feature_spec.json").exists()
    assert (result.run_dir / "label_spec.json").exists()
    assert (result.run_dir / "train_config.yaml").exists()
    assert (result.run_dir / "data_fingerprint.json").exists()
    assert (result.run_dir / "leaderboard_row.json").exists()
    assert (result.run_dir / "selection_policy.json").exists()
    assert (result.run_dir / "selection_calibration.json").exists()
    assert (result.run_dir / "runtime_recommendations.json").exists()
    assert (result.run_dir / "predictor_contract.json").exists()


def test_train_v6_edge2stage_predictor_returns_edge2stage_fields(tmp_path: Path) -> None:
    for offset in range(20):
        date_value = f"2026-04-{offset + 1:02d}"
        positive = (offset % 2) == 0
        _write_market_state_pair(
            tmp_path,
            date_value,
            "KRW-BTC",
            label_available=True,
            tradeable_value=1 if positive else 0,
            net_edge_20m_bps=8.0 if positive else -4.0,
        )
    build_market_state_training_slice_v1(
        MarketStateTrainingSliceBuildOptions(
            start="2026-04-01",
            end="2026-04-20",
            markets=("KRW-BTC",),
            market_state_root=tmp_path / "data" / "derived" / "market_state_v1",
            tradeable_label_root=tmp_path / "data" / "derived" / "tradeable_label_v1",
            net_edge_label_root=tmp_path / "data" / "derived" / "net_edge_label_v1",
            out_root=tmp_path / "data" / "derived" / "market_state_training_slice_v1",
        )
    )
    result = train_and_register_v6_edge2stage(
        TrainV6Edge2StageOptions(
            dataset_root=tmp_path / "data" / "derived" / "market_state_training_slice_v1",
            registry_root=tmp_path / "registry",
            logs_root=tmp_path / "logs",
            model_family="train_v6_edge2stage",
            quote="KRW",
            start="2026-04-01",
            end="2026-04-20",
            seed=42,
            nthread=1,
        )
    )
    predictor = load_predictor_from_registry(
        registry_root=tmp_path / "registry",
        model_ref=result.run_id,
        model_family="train_v6_edge2stage",
    )
    frame = pl.read_parquet(next((tmp_path / "data" / "derived" / "market_state_training_slice_v1" / "date=2026-04-20").glob("*.parquet")))
    x = frame.select(list(predictor.feature_columns)).to_numpy()
    payload = predictor.predict_score_contract(x)
    assert "final_tradeable_prob" in payload
    assert "final_expected_net_edge_bps" in payload
    assert "final_go_score" in payload
    report = load_json(result.train_report_path)
    assert "horizon_diagnostics" in report
    assert "label_audit" in report
    assert "architecture_bakeoff" in report
    assert set(report["horizon_diagnostics"].keys()) >= {"10m", "20m", "40m"}


def test_train_v6_edge2stage_exposes_horizon_competition_surfaces(tmp_path: Path) -> None:
    for offset in range(20):
        date_value = f"2026-04-{offset + 1:02d}"
        positive = (offset % 2) == 0
        _write_market_state_pair(
            tmp_path,
            date_value,
            "KRW-BTC",
            label_available=True,
            tradeable_value=1 if positive else 0,
            net_edge_20m_bps=8.0 if positive else -4.0,
        )
        _write_market_state_pair(
            tmp_path,
            date_value,
            "KRW-ETH",
            label_available=True,
            tradeable_value=0 if positive else 1,
            net_edge_20m_bps=-2.0 if positive else 7.0,
        )
    build_market_state_training_slice_v1(
        MarketStateTrainingSliceBuildOptions(
            start="2026-04-01",
            end="2026-04-20",
            markets=("KRW-BTC", "KRW-ETH"),
            market_state_root=tmp_path / "data" / "derived" / "market_state_v1",
            tradeable_label_root=tmp_path / "data" / "derived" / "tradeable_label_v1",
            net_edge_label_root=tmp_path / "data" / "derived" / "net_edge_label_v1",
            out_root=tmp_path / "data" / "derived" / "market_state_training_slice_v1",
        )
    )

    result = train_and_register_v6_edge2stage(
        TrainV6Edge2StageOptions(
            dataset_root=tmp_path / "data" / "derived" / "market_state_training_slice_v1",
            registry_root=tmp_path / "registry",
            logs_root=tmp_path / "logs",
            model_family="train_v6_edge2stage",
            quote="KRW",
            start="2026-04-01",
            end="2026-04-20",
            seed=42,
            nthread=1,
        )
    )

    label_spec = load_json(result.run_dir / "label_spec.json")
    predictor_contract = load_json(result.run_dir / "predictor_contract.json")
    runtime_recommendations = load_json(result.run_dir / "runtime_recommendations.json")
    selection_policy = load_json(result.run_dir / "selection_policy.json")
    train_config = load_json(result.run_dir / "train_config.yaml")
    economic_objective_profile = load_json(result.run_dir / "economic_objective_profile.json")
    diagnostics = train_config["horizon_diagnostics"]

    assert label_spec["classification_label"] == "structural_tradeable_20m"
    assert label_spec["promotion_label"] == "tradeable_20m"
    assert label_spec["regression_label"] == "net_edge_20m_bps"
    assert label_spec["auxiliary_labels"] == ["net_edge_10m_bps", "net_edge_40m_bps"]
    assert train_config["label_audit"]["stage_a_label"] == "structural_tradeable_20m"
    assert train_config["label_audit"]["promotion_label"] == "tradeable_20m"
    assert train_config["label_audit"]["tradeable_vs_edge_threshold_agreement"] == 1.0
    assert train_config["label_audit"]["stage_a_positive_ratio"] == 1.0
    assert train_config["label_audit"]["stage_a_vs_edge_threshold_agreement"] < 1.0
    assert train_config["label_audit"]["stage_a_vs_tradeable_agreement"] < 1.0
    assert predictor_contract["tradeable_prob_semantics"] == "structural_tradeable_20m"
    assert predictor_contract["promotion_label"] == "tradeable_20m"
    assert "structural_tradeable_20m" in predictor_contract["decision_rule"]
    assert runtime_recommendations["stage_a_label"] == "structural_tradeable_20m"
    assert runtime_recommendations["promotion_label"] == "tradeable_20m"
    assert "structural_tradeable_20m" in runtime_recommendations["decision_rule"]
    assert train_config["small_account_realism"]["assumptions"]["target_notional_quote"] == 10_000.0
    assert train_config["small_account_realism"]["assumptions"]["max_positions"] == 1
    assert selection_policy["mode"] == "raw_threshold"
    assert selection_policy["tradeable_prob_min"] == 0.55
    assert selection_policy["expected_net_edge_bps_min"] == 3.0
    assert selection_policy["fallback_used"] is False
    assert diagnostics["10m"]["rows"] > 0
    assert diagnostics["20m"]["rows"] > 0
    assert diagnostics["40m"]["rows"] > 0
    assert diagnostics["10m"]["above_3bps_ratio"] == 0.0
    assert diagnostics["40m"]["above_3bps_ratio"] == 1.0
    assert 0.0 < diagnostics["20m"]["above_3bps_ratio"] < 1.0
    report = load_json(result.train_report_path)
    assert report["architecture_bakeoff"]["direct_ranker_challenger"]["test"]["economic"]["selected_count"] >= 0
    assert report["architecture_bakeoff"]["default_candidate"]["test"]["economic"]["small_account"]["selected_count"] >= 0
    assert economic_objective_profile["v6_small_account_realism"]["target_notional_quote"] == 10_000.0


def test_train_v6_edge2stage_persists_small_account_runtime_contract(tmp_path: Path) -> None:
    for offset in range(20):
        date_value = f"2026-04-{offset + 1:02d}"
        positive = (offset % 2) == 0
        _write_market_state_pair(
            tmp_path,
            date_value,
            "KRW-BTC",
            label_available=True,
            tradeable_value=1 if positive else 0,
            net_edge_20m_bps=8.0 if positive else -4.0,
        )
    build_market_state_training_slice_v1(
        MarketStateTrainingSliceBuildOptions(
            start="2026-04-01",
            end="2026-04-20",
            markets=("KRW-BTC",),
            market_state_root=tmp_path / "data" / "derived" / "market_state_v1",
            tradeable_label_root=tmp_path / "data" / "derived" / "tradeable_label_v1",
            net_edge_label_root=tmp_path / "data" / "derived" / "net_edge_label_v1",
            out_root=tmp_path / "data" / "derived" / "market_state_training_slice_v1",
        )
    )

    result = train_and_register_v6_edge2stage(
        TrainV6Edge2StageOptions(
            dataset_root=tmp_path / "data" / "derived" / "market_state_training_slice_v1",
            registry_root=tmp_path / "registry",
            logs_root=tmp_path / "logs",
            model_family="train_v6_edge2stage",
            quote="KRW",
            start="2026-04-01",
            end="2026-04-20",
            seed=42,
            nthread=1,
            small_account_target_notional_quote=10_000.0,
            small_account_min_order_quote=5_000.0,
            small_account_fee_rate=0.0005,
            small_account_replace_risk_steps=2,
        )
    )

    predictor_contract = load_json(result.run_dir / "predictor_contract.json")
    runtime_recommendations = load_json(result.run_dir / "runtime_recommendations.json")

    assert predictor_contract["small_account_assumptions"]["target_notional_quote"] == 10_000.0
    assert predictor_contract["small_account_assumptions"]["min_order_quote"] == 5_000.0
    assert predictor_contract["small_account_assumptions"]["fee_rate"] == 0.0005
    assert predictor_contract["small_account_assumptions"]["replace_risk_steps"] == 2
    assert runtime_recommendations["small_account_realism"]["target_notional_quote"] == 10_000.0
    assert runtime_recommendations["small_account_realism"]["min_order_quote"] == 5_000.0
    assert runtime_recommendations["small_account_realism"]["test_summary"]["assumptions"]["max_positions"] == 1


def test_train_v6_edge2stage_report_includes_small_account_execution_feasibility(tmp_path: Path) -> None:
    for offset in range(20):
        date_value = f"2026-04-{offset + 1:02d}"
        positive = (offset % 2) == 0
        _write_market_state_pair(
            tmp_path,
            date_value,
            "KRW-BTC",
            label_available=True,
            tradeable_value=1 if positive else 0,
            net_edge_20m_bps=8.0 if positive else -4.0,
        )
    build_market_state_training_slice_v1(
        MarketStateTrainingSliceBuildOptions(
            start="2026-04-01",
            end="2026-04-20",
            markets=("KRW-BTC",),
            market_state_root=tmp_path / "data" / "derived" / "market_state_v1",
            tradeable_label_root=tmp_path / "data" / "derived" / "tradeable_label_v1",
            net_edge_label_root=tmp_path / "data" / "derived" / "net_edge_label_v1",
            out_root=tmp_path / "data" / "derived" / "market_state_training_slice_v1",
        )
    )

    result = train_and_register_v6_edge2stage(
        TrainV6Edge2StageOptions(
            dataset_root=tmp_path / "data" / "derived" / "market_state_training_slice_v1",
            registry_root=tmp_path / "registry-high-fee",
            logs_root=tmp_path / "logs-high-fee",
            model_family="train_v6_edge2stage",
            quote="KRW",
            start="2026-04-01",
            end="2026-04-20",
            seed=42,
            nthread=1,
            tradeable_prob_threshold=0.0,
            net_edge_threshold_bps=-100.0,
            small_account_target_notional_quote=10_000.0,
            small_account_min_order_quote=5_000.0,
            small_account_fee_rate=0.01,
        )
    )

    report = load_json(result.train_report_path)
    small_account = report["small_account_realism"]["test"]

    assert small_account["selected_count"] >= 0
    assert small_account["rejected_for_min_order_count"] >= 0
    assert small_account["rejected_for_cost_count"] >= 0
    assert 0.0 <= small_account["admissible_ratio"] <= 1.0
    assert 0.0 <= small_account["selected_viability_rate"] <= 1.0
    assert 0.0 <= small_account["viable_selected_ratio"] <= 1.0
    assert small_account["assumptions"]["target_notional_quote"] == 10_000.0
    assert small_account["assumptions"]["min_order_quote"] == 5_000.0
    assert small_account["assumptions"]["fee_rate"] == 0.01
    assert small_account["assumptions"]["max_positions"] == 1
    assert small_account["mean_incremental_cost_bps"] >= 0.0
    assert isinstance(small_account["cost_breakdown_samples"], list)

    baseline_small_account = _build_small_account_realism_summary(
        quote="KRW",
        target_notional_quote=10_000.0,
        min_order_quote=5_000.0,
        fee_rate=0.0,
        replace_risk_steps=2,
        max_positions=1,
        operating_date_values=["2026-04-20"],
        bucket_start_ts_ms=[1_000],
        prices=[100_000.0],
        y_edge_bps=[8.0],
        pred_edge_bps=[120.0],
        score=[120.0],
        trade_mask=[True],
    )
    high_fee_small_account = _build_small_account_realism_summary(
        quote="KRW",
        target_notional_quote=10_000.0,
        min_order_quote=5_000.0,
        fee_rate=0.01,
        replace_risk_steps=2,
        max_positions=1,
        operating_date_values=["2026-04-20"],
        bucket_start_ts_ms=[1_000],
        prices=[100_000.0],
        y_edge_bps=[8.0],
        pred_edge_bps=[120.0],
        score=[120.0],
        trade_mask=[True],
    )
    assert high_fee_small_account["mean_incremental_cost_bps"] > baseline_small_account["mean_incremental_cost_bps"]


def test_train_v6_edge2stage_small_account_summary_caps_concurrent_positions(tmp_path: Path) -> None:
    for offset in range(20):
        date_value = f"2026-04-{offset + 1:02d}"
        _write_market_state_pair(
            tmp_path,
            date_value,
            "KRW-BTC",
            label_available=True,
            tradeable_value=1,
            net_edge_20m_bps=8.0,
        )
        _write_market_state_pair(
            tmp_path,
            date_value,
            "KRW-ETH",
            label_available=True,
            tradeable_value=1,
            net_edge_20m_bps=7.0,
        )
    build_market_state_training_slice_v1(
        MarketStateTrainingSliceBuildOptions(
            start="2026-04-01",
            end="2026-04-20",
            markets=("KRW-BTC", "KRW-ETH"),
            market_state_root=tmp_path / "data" / "derived" / "market_state_v1",
            tradeable_label_root=tmp_path / "data" / "derived" / "tradeable_label_v1",
            net_edge_label_root=tmp_path / "data" / "derived" / "net_edge_label_v1",
            out_root=tmp_path / "data" / "derived" / "market_state_training_slice_v1",
        )
    )

    result = train_and_register_v6_edge2stage(
        TrainV6Edge2StageOptions(
            dataset_root=tmp_path / "data" / "derived" / "market_state_training_slice_v1",
            registry_root=tmp_path / "registry",
            logs_root=tmp_path / "logs",
            model_family="train_v6_edge2stage",
            quote="KRW",
            start="2026-04-01",
            end="2026-04-20",
            seed=42,
            nthread=1,
            small_account_max_positions=1,
        )
    )

    report = load_json(result.train_report_path)
    small_account = report["architecture_bakeoff"]["default_candidate"]["test"]["economic"]["small_account"]

    assert small_account["admissible_count"] >= small_account["viable_selected_count"]
    assert small_account["capped_by_max_positions_count"] >= 0


def test_train_v6_edge2stage_uses_usable_pairs_bootstrap_when_data_is_not_yet_adequate(tmp_path: Path) -> None:
    for offset in range(5):
        date_value = f"2026-04-{offset + 1:02d}"
        positive = (offset % 2) == 0
        _write_market_state_pair(
            tmp_path,
            date_value,
            "KRW-BTC",
            label_available=True,
            tradeable_value=1 if positive else 0,
            net_edge_20m_bps=8.0 if positive else -4.0,
        )
        _write_market_state_pair(
            tmp_path,
            date_value,
            "KRW-ETH",
            label_available=True if offset < 3 else False,
            tradeable_value=0 if positive else 1,
            net_edge_20m_bps=-2.0 if positive else 7.0,
        )
    build_market_state_training_slice_v1(
        MarketStateTrainingSliceBuildOptions(
            start="2026-04-01",
            end="2026-04-05",
            markets=("KRW-BTC", "KRW-ETH"),
            market_state_root=tmp_path / "data" / "derived" / "market_state_v1",
            tradeable_label_root=tmp_path / "data" / "derived" / "tradeable_label_v1",
            net_edge_label_root=tmp_path / "data" / "derived" / "net_edge_label_v1",
            out_root=tmp_path / "data" / "derived" / "market_state_training_slice_v1",
        )
    )

    result = train_and_register_v6_edge2stage(
        TrainV6Edge2StageOptions(
            dataset_root=tmp_path / "data" / "derived" / "market_state_training_slice_v1",
            registry_root=tmp_path / "registry",
            logs_root=tmp_path / "logs",
            model_family="train_v6_edge2stage",
            quote="KRW",
            start="2026-04-01",
            end="2026-04-05",
            seed=42,
            nthread=1,
            bootstrap_min_pair_rows=1,
            bootstrap_min_usable_pairs_per_date=1,
        )
    )
    train_config = load_json(result.run_dir / "train_config.yaml")
    assert train_config["date_selection_policy"] == "usable_pairs_bootstrap_until_adequate"
    assert train_config["effective_operating_dates"] == [
        "2026-04-01",
        "2026-04-02",
        "2026-04-03",
        "2026-04-04",
        "2026-04-05",
    ]
    assert train_config["operating_date_split"] == {
        "train_dates": ["2026-04-01", "2026-04-02", "2026-04-03"],
        "valid_dates": ["2026-04-04"],
        "test_dates": ["2026-04-05"],
    }


def _write_market_state_pair(
    root: Path,
    date_value: str,
    market: str,
    *,
    label_available: bool,
    tradeable_value: int | None = None,
    net_edge_20m_bps: float = 5.0,
) -> None:
    ms_root = root / "data" / "derived" / "market_state_v1"
    tl_root = root / "data" / "derived" / "tradeable_label_v1"
    ne_root = root / "data" / "derived" / "net_edge_label_v1"
    for dataset_root in (ms_root, tl_root, ne_root):
        (dataset_root / "_meta").mkdir(parents=True, exist_ok=True)
    key = {
        "market": [market],
        "bucket_start_ts_ms": [1_000],
        "bucket_end_ts_ms": [6_000],
        "operating_date_kst": [date_value],
        "bucket_date_utc": ["2026-04-11"],
    }
    ms = pl.DataFrame(
        {
            **key,
            "last_price": [100.0],
            "acc_trade_price_24h": [1_000_000.0],
            "signed_change_rate": [0.01],
            "ticker_age_ms": [0],
            "ticker_proxy_available": [False],
            "ticker_source_kind": ["ws_raw"],
            "ticker_source_kind_code": [2],
            "trade_events_5s": [5],
            "trade_events_15s": [8],
            "trade_events_60s": [12],
            "trade_notional_5s": [1000.0],
            "trade_notional_60s": [5000.0],
            "buy_volume_5s": [5.0],
            "sell_volume_5s": [1.0],
            "signed_volume_5s": [4.0],
            "trade_imbalance_5s": [0.66],
            "vwap_5s": [100.0],
            "large_trade_ratio_60s": [0.4],
            "best_bid": [99.9],
            "best_ask": [100.0],
            "spread_bps": [10.0],
            "bid_depth_top1_krw": [1_000_000.0],
            "ask_depth_top1_krw": [900_000.0],
            "bid_depth_top5_krw": [5_000_000.0],
            "ask_depth_top5_krw": [4_000_000.0],
            "queue_imbalance_top1": [0.1],
            "queue_imbalance_top5": [0.11],
            "microprice": [99.95],
            "microprice_bias_bps": [0.5],
            "book_update_count_5s": [3],
            "ret_1m": [0.001],
            "ret_5m": [0.002],
            "ret_15m": [0.003],
            "ret_60m": [0.004],
            "rv_1m_5m_window": [0.1],
            "rv_1m_15m_window": [0.2],
            "atr_pct_14": [0.02],
            "distance_from_15m_high_low": [0.4],
            "btc_rel_strength_5m": [0.0],
            "eth_rel_strength_5m": [0.0],
            "market_cap_rank_fixed30": [1],
            "universe_breadth_up_ratio": [0.5],
            "universe_notional_rank_pct": [0.8],
            "source_quality_score": [1.0],
            "ticker_available": [True],
            "trade_available": [True],
            "book_available": [True],
            "candle_context_available": [True],
        }
    )
    tl = pl.DataFrame(
        {
            **key,
            "label_available_20m": [label_available],
            "spread_quality_pass_20m": [True],
            "liquidity_pass_20m": [True],
            "structure_pass_20m": [True],
            "tradeable_20m": [tradeable_value if tradeable_value is not None else (1 if label_available else 0)],
        }
    )
    ne = pl.DataFrame(
        {
            **key,
            "entry_best_ask": [100.0],
            "entry_best_ask_depth_top5_krw": [4_000_000.0],
            "entry_spread_bps": [10.0],
            "gross_return_10m_bps": [8.0],
            "gross_return_20m_bps": [12.0],
            "gross_return_40m_bps": [14.0],
            "net_edge_10m_bps": [2.0],
            "net_edge_20m_bps": [net_edge_20m_bps],
            "net_edge_40m_bps": [6.0],
            "future_best_bid_10m": [100.1],
            "future_best_bid_20m": [100.2],
            "future_best_bid_40m": [100.3],
            "future_bid_depth_top5_krw_10m": [3_000_000.0],
            "future_bid_depth_top5_krw_20m": [3_000_000.0],
            "future_bid_depth_top5_krw_40m": [3_000_000.0],
        }
    )
    for dataset_root, frame in ((ms_root, ms), (tl_root, tl), (ne_root, ne)):
        date_dir = dataset_root / f"date={date_value}"
        date_dir.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(date_dir / f"part-{market}.parquet")
        manifest = pl.DataFrame(
            {
                "run_id": ["run-1"],
                "date": [date_value],
                "market": [market],
                "rows": [1],
                "min_ts_ms": [6_000],
                "max_ts_ms": [6_000],
                "part_file": [str(date_dir / f"part-{market}.parquet")],
                "built_at_ms": [1],
            }
        )
        manifest_path = dataset_root / "_meta" / "manifest.parquet"
        if manifest_path.exists():
            existing = pl.read_parquet(manifest_path)
            manifest = pl.concat([existing, manifest], how="vertical")
        manifest.write_parquet(manifest_path)
