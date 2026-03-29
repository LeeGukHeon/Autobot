from __future__ import annotations

import json
from pathlib import Path

import polars as pl

import autobot.ops.live_feature_parity_report as parity_module
from autobot.ops.live_feature_parity_report import _build_live_frames_for_sampled_ts, _resolve_bootstrap_1m_bars
from autobot.features.feature_blocks_v4_live_base import cast_feature_output_v4_live_base
from autobot.features.feature_set_v4 import (
    attach_interaction_features_v4,
    attach_order_flow_panel_v1,
    attach_periodicity_features_v4,
    attach_spillover_breadth_features_v4,
    attach_trend_volume_features_v4,
)
from autobot.paper.live_features_multitf_base import _tail_bars_from_bootstrap_1m_bars


def _write_one_m_candles(
    *,
    dataset_root: Path,
    market: str,
    start_ts_ms: int = 60_000,
    count: int = 900,
    base_close: float = 100.0,
    slope: float = 0.05,
) -> None:
    part_dir = dataset_root / "tf=1m" / f"market={market}" / "date=1970-01-01"
    part_dir.mkdir(parents=True, exist_ok=True)
    ts_values = [int(start_ts_ms) + (i * 60_000) for i in range(int(count))]
    close_values = [float(base_close) + (i * float(slope)) for i in range(len(ts_values))]
    pl.DataFrame(
        {
            "ts_ms": ts_values,
            "open": [value - 0.02 for value in close_values],
            "high": [value + 0.05 for value in close_values],
            "low": [value - 0.05 for value in close_values],
            "close": close_values,
            "volume_base": [10.0 + (i % 5) for i in range(len(ts_values))],
        }
    ).write_parquet(part_dir / "part-000.parquet")


def _write_micro_5m_snapshot(*, micro_root: Path, market: str, ts_ms: int) -> None:
    part_dir = micro_root / "tf=5m" / f"market={market}" / "date=1970-01-01"
    part_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "market": [market],
            "tf": ["5m"],
            "ts_ms": [ts_ms],
            "trade_source": ["ws"],
            "trade_events": [1],
            "book_events": [1],
            "trade_min_ts_ms": [max(int(ts_ms) - 300_000, 0)],
            "trade_max_ts_ms": [ts_ms],
            "book_min_ts_ms": [max(int(ts_ms) - 300_000, 0)],
            "book_max_ts_ms": [ts_ms],
            "trade_coverage_ms": [300_000],
            "book_coverage_ms": [300_000],
            "micro_trade_available": [True],
            "micro_book_available": [True],
            "micro_available": [True],
            "trade_count": [1],
            "buy_count": [1],
            "sell_count": [0],
            "trade_volume_total": [1.0],
            "buy_volume": [1.0],
            "sell_volume": [0.0],
            "trade_imbalance": [1.0],
            "vwap": [100.0],
            "avg_trade_size": [1.0],
            "max_trade_size": [1.0],
            "last_trade_price": [100.0],
            "mid_mean": [100.0],
            "spread_bps_mean": [1.0],
            "depth_bid_top5_mean": [1000.0],
            "depth_ask_top5_mean": [1000.0],
            "imbalance_top5_mean": [0.0],
            "microprice_bias_bps_mean": [0.0],
            "book_update_count": [1],
        }
    ).write_parquet(part_dir / "part-000.parquet")


def test_bulk_live_parity_frame_builder_matches_iterative_provider(tmp_path: Path) -> None:
    project_root = tmp_path
    parquet_root = project_root / "data" / "parquet"
    candles_root = parquet_root / "candles_api_v1"
    micro_root = parquet_root / "micro_v1"
    for market, base_close, slope in (
        ("KRW-BTC", 100.0, 0.05),
        ("KRW-ETH", 200.0, -0.03),
    ):
        _write_one_m_candles(
            dataset_root=candles_root,
            market=market,
            count=900,
            base_close=base_close,
            slope=slope,
        )
        _write_micro_5m_snapshot(micro_root=micro_root, market=market, ts_ms=900_000)
        _write_micro_5m_snapshot(micro_root=micro_root, market=market, ts_ms=1_200_000)

    feature_columns = ("logret_1", "m_mid_mean", "turnover_concentration_hhi", "market_breadth_pos_1")
    feature_spec = {
        "feature_columns": list(feature_columns),
        "base_candles_root": str(candles_root),
        "micro_root": str(micro_root),
    }
    sampled_ts_values = [900_000, 1_200_000]
    bootstrap_1m_bars = _resolve_bootstrap_1m_bars(sampled_ts_values)
    provider_bulk = parity_module._build_live_provider(
        root=project_root,
        feature_spec=feature_spec,
        feature_columns=feature_columns,
        tf="5m",
        quote="KRW",
        bootstrap_end_ts_ms=1_200_000,
        bootstrap_1m_bars=bootstrap_1m_bars,
    )
    provider_iter = parity_module._build_live_provider(
        root=project_root,
        feature_spec=feature_spec,
        feature_columns=feature_columns,
        tf="5m",
        quote="KRW",
        bootstrap_end_ts_ms=1_200_000,
        bootstrap_1m_bars=bootstrap_1m_bars,
    )

    markets = ["KRW-BTC", "KRW-ETH"]
    bulk_frames, stats_by_ts = _build_live_frames_for_sampled_ts(
        provider=provider_bulk,
        sampled_ts_values=sampled_ts_values,
        markets_by_ts={int(ts_value): list(markets) for ts_value in sampled_ts_values},
        history_start_ts_ms=sampled_ts_values[0],
    )

    for ts_value in sampled_ts_values:
        expected = provider_iter.build_frame(ts_ms=ts_value, markets=markets).sort(["market", "ts_ms"])
        actual = bulk_frames[int(ts_value)]
        if actual.height > 0 or expected.height > 0:
            actual_rows = [
                {key: parity_module._normalize_value(value) for key, value in row.items()}
                for row in actual.sort(["market", "ts_ms"]).to_dicts()
            ]
            expected_rows = [
                {key: parity_module._normalize_value(value) for key, value in row.items()}
                for row in expected.to_dicts()
            ]
            assert actual_rows == expected_rows
        else:
            assert actual.height == expected.height == 0
        assert stats_by_ts[int(ts_value)]["built_rows"] == expected.height


def test_tail_bars_from_bootstrap_1m_bars_scales_minutes_to_target_interval() -> None:
    assert _tail_bars_from_bootstrap_1m_bars(bootstrap_1m_bars=2_512, interval_ms=300_000) == 511
    assert _tail_bars_from_bootstrap_1m_bars(bootstrap_1m_bars=2_512, interval_ms=60_000) == 2520


def test_bulk_live_parity_frame_builder_filters_offline_invalid_history_rows() -> None:
    class _FakeProvider:
        def __init__(self) -> None:
            self._tf = "5m"
            self._quote = "KRW"
            self._feature_columns = ("oflow_v1_signed_volume_imbalance_12",)
            self._base_feature_columns = ("m_trade_volume_base",)
            self._extra_columns: tuple[str, ...] = ()
            self._high_tfs = ("15m",)
            self._context_history_bars = 3

        def _build_runtime_context_frame(self, **_: object) -> tuple[pl.DataFrame, dict[str, object]]:
            frame = pl.DataFrame(
                {
                    "ts_ms": [300_000, 600_000, 900_000],
                    "market": ["KRW-BTC", "KRW-BTC", "KRW-BTC"],
                    "close": [100.0, 100.0, 100.0],
                    "logret_1": [0.0, 0.0, 0.0],
                    "logret_3": [0.0, 0.0, 0.0],
                    "logret_12": [0.0, 0.0, 0.0],
                    "volume_z": [0.0, 0.0, 0.0],
                    "volume_base": [1.0, 1.0, 1.0],
                    "m_trade_volume_base": [1.0, 1.0, 1.0],
                    "m_buy_volume": [0.0, 1.0, 1.0],
                    "m_sell_volume": [1.0, 0.0, 0.0],
                    "m_trade_count": [1.0, 1.0, 1.0],
                    "m_buy_count": [0.0, 1.0, 1.0],
                    "m_sell_count": [1.0, 0.0, 0.0],
                    "m_depth_bid_top5_mean": [500.0, 500.0, 500.0],
                    "m_depth_ask_top5_mean": [500.0, 500.0, 500.0],
                    "m_microprice_bias_bps_mean": [0.0, 0.0, 0.0],
                    "m_spread_proxy": [1.0, 1.0, 1.0],
                    "m_micro_available": [1.0, 1.0, 1.0],
                    "one_m_fail": [True, False, False],
                    "tf15m_stale": [False, False, False],
                }
            )
            return frame, {}

        def _filter_context_for_micro_contract(self, frame: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, object]]:
            return frame, {}

    frames, stats_by_ts = _build_live_frames_for_sampled_ts(
        provider=_FakeProvider(),
        sampled_ts_values=[900_000],
        markets_by_ts={900_000: ["KRW-BTC"]},
        history_start_ts_ms=300_000,
    )

    frame = frames[900_000]
    assert frame.height == 1
    row = frame.row(0, named=True)
    assert float(row["oflow_v1_signed_volume_imbalance_12"]) == 1.0
    assert stats_by_ts[900_000]["base_provider_stats"]["offline_contract_rows_dropped_one_m_fail"] == 1


def test_bulk_live_parity_frame_builder_uses_canonical_base_precision_for_derived_features() -> None:
    class _FakeProvider:
        def __init__(self) -> None:
            self._tf = "5m"
            self._quote = "KRW"
            self._feature_columns = ("one_m_pressure_x_spread",)
            self._base_feature_columns = ("m_trade_volume_base",)
            self._extra_columns: tuple[str, ...] = ()
            self._high_tfs = ("15m", "60m", "240m")
            self._context_history_bars = 1

        def _build_runtime_context_frame(self, **_: object) -> tuple[pl.DataFrame, dict[str, object]]:
            return (
                pl.DataFrame(
                    {
                        "ts_ms": [900_000],
                        "market": ["KRW-XLM"],
                        "close": [100.0],
                        "logret_1": [0.0],
                        "logret_3": [0.0],
                        "logret_12": [0.0],
                        "logret_36": [0.0],
                        "volume_z": [0.0],
                        "one_m_ret_mean": [0.0],
                        "one_m_real_volume_sum": [0.0],
                        "volume_base": [0.0],
                        "m_signed_volume": [155204.64772371],
                        "m_trade_volume_base": [252589.10640368998],
                        "m_spread_proxy": [43.69504673588422],
                        "m_trade_count": [1.0],
                        "m_buy_count": [1.0],
                        "m_sell_count": [0.0],
                        "m_buy_volume": [155204.64772371],
                        "m_sell_volume": [0.0],
                        "m_depth_bid_top5_mean": [500.0],
                        "m_depth_ask_top5_mean": [500.0],
                        "m_microprice_bias_bps_mean": [0.0],
                        "m_micro_available": [1.0],
                        "one_m_fail": [False],
                    }
                ),
                {},
            )

        def _filter_context_for_micro_contract(self, frame: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, object]]:
            return frame, {}

    frame = _build_live_frames_for_sampled_ts(
        provider=_FakeProvider(),
        sampled_ts_values=[900_000],
        markets_by_ts={900_000: ["KRW-XLM"]},
        history_start_ts_ms=900_000,
    )[0][900_000]

    expected_base = cast_feature_output_v4_live_base(
        pl.DataFrame(
            {
                "ts_ms": [900_000],
                "market": ["KRW-XLM"],
                "close": [100.0],
                "logret_1": [0.0],
                "logret_3": [0.0],
                "logret_12": [0.0],
                "logret_36": [0.0],
                "volume_z": [0.0],
                "one_m_ret_mean": [0.0],
                "one_m_real_volume_sum": [0.0],
                "volume_base": [0.0],
                "m_signed_volume": [155204.64772371],
                "m_trade_volume_base": [252589.10640368998],
                "m_spread_proxy": [43.69504673588422],
                "m_trade_count": [1.0],
                "m_buy_count": [1.0],
                "m_sell_count": [0.0],
                "m_buy_volume": [155204.64772371],
                "m_sell_volume": [0.0],
                "m_depth_bid_top5_mean": [500.0],
                "m_depth_ask_top5_mean": [500.0],
                "m_microprice_bias_bps_mean": [0.0],
                "m_micro_available": [1.0],
                "one_m_fail": [False],
            }
        ),
        float_dtype="float32",
        high_tfs=("15m", "60m", "240m"),
    )
    expected_frame = attach_interaction_features_v4(
        attach_order_flow_panel_v1(
            attach_trend_volume_features_v4(
                attach_periodicity_features_v4(
                    attach_spillover_breadth_features_v4(expected_base, quote="KRW", float_dtype="float32"),
                    float_dtype="float32",
                ),
                float_dtype="float32",
            ),
            float_dtype="float32",
        ),
        float_dtype="float32",
    )

    assert frame.height == 1
    assert float(frame.row(0, named=True)["one_m_pressure_x_spread"]) == float(
        expected_frame.row(0, named=True)["one_m_pressure_x_spread"]
    )
