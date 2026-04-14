from __future__ import annotations

from pathlib import Path

import polars as pl

from autobot.data.derived.market_state_training_slice_v1 import (
    MarketStateTrainingSliceBuildOptions,
    build_market_state_training_slice_v1,
)


def test_build_market_state_training_slice_v1_joins_and_filters_label_available(tmp_path: Path) -> None:
    _write_market_state_pair(tmp_path, "2026-04-12", "KRW-BTC", label_available=True)
    _write_market_state_pair(tmp_path, "2026-04-12", "KRW-ETH", label_available=False)

    summary = build_market_state_training_slice_v1(
        MarketStateTrainingSliceBuildOptions(
            start="2026-04-12",
            end="2026-04-12",
            markets=("KRW-BTC", "KRW-ETH"),
            market_state_root=tmp_path / "data" / "derived" / "market_state_v1",
            tradeable_label_root=tmp_path / "data" / "derived" / "tradeable_label_v1",
            net_edge_label_root=tmp_path / "data" / "derived" / "net_edge_label_v1",
            out_root=tmp_path / "data" / "derived" / "market_state_training_slice_v1",
        )
    )

    assert summary.built_dates == 1
    frame = pl.read_parquet(next((tmp_path / "data" / "derived" / "market_state_training_slice_v1" / "date=2026-04-12").glob("*.parquet")))
    assert frame.height == 1
    row = frame.row(0, named=True)
    assert row["market"] == "KRW-BTC"
    assert row["ticker_source_kind"] == "ws_raw"
    assert row["ticker_source_kind_code"] == 2
    assert row["tradeable_20m"] == 1


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
            "ticker_available": [True],
            "trade_available": [True],
            "book_available": [True],
            "candle_context_available": [True],
            "source_quality_score": [1.0],
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
