from __future__ import annotations

import json
from pathlib import Path

import polars as pl

import autobot.ops.live_feature_parity_report as parity_module
from autobot.ops.live_feature_parity_report import _build_live_frames_for_sampled_ts, _resolve_bootstrap_1m_bars
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
    )

    for ts_value in sampled_ts_values:
        expected = provider_iter.build_frame(ts_ms=ts_value, markets=markets).sort(["market", "ts_ms"])
        actual = bulk_frames[int(ts_value)].sort(["market", "ts_ms"])
        assert actual.to_dicts() == expected.to_dicts()
        assert stats_by_ts[int(ts_value)]["built_rows"] == expected.height


def test_tail_bars_from_bootstrap_1m_bars_scales_minutes_to_target_interval() -> None:
    assert _tail_bars_from_bootstrap_1m_bars(bootstrap_1m_bars=2_512, interval_ms=300_000) == 511
    assert _tail_bars_from_bootstrap_1m_bars(bootstrap_1m_bars=2_512, interval_ms=60_000) == 2520
