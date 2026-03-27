from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import polars as pl
import zstandard as zstd

from autobot.strategy.micro_snapshot import OfflineMicroSnapshotProvider


def _write_micro_part(root: Path) -> None:
    ts_ms = 1_700_000_000_000
    date_value = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
    target = root / "tf=5m" / "market=KRW-BTC" / f"date={date_value}"
    target.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ts_ms": [ts_ms],
            "trade_source": ["ws"],
            "trade_events": [3],
            "trade_coverage_ms": [30_000],
            "trade_min_ts_ms": [ts_ms - 30_000],
            "trade_count": [3],
            "buy_count": [2],
            "sell_count": [1],
            "trade_volume_total": [2.0],
            "buy_volume": [1.2],
            "sell_volume": [0.8],
            "trade_imbalance": [0.2],
            "vwap": [100.0],
            "avg_trade_size": [2.0 / 3.0],
            "max_trade_size": [1.25],
            "last_trade_price": [101.0],
            "trade_max_ts_ms": [ts_ms],
            "book_events": [2],
            "book_coverage_ms": [20_000],
            "book_min_ts_ms": [ts_ms - 20_000],
            "book_max_ts_ms": [ts_ms],
            "micro_book_available": [True],
            "mid_mean": [100.5],
            "spread_bps_mean": [4.5],
            "depth_bid_top5_mean": [500_000.0],
            "depth_ask_top5_mean": [550_000.0],
            "imbalance_top5_mean": [-0.0476190476],
            "microprice_bias_bps_mean": [3.25],
        }
    ).write_parquet(target / "part-000.parquet")


def _write_orderbook_part(root: Path, *, ts_ms: int) -> None:
    date_value = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
    hour_value = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%H")
    target = root / "orderbook" / f"date={date_value}" / f"hour={hour_value}"
    target.mkdir(parents=True, exist_ok=True)
    row = {
        "channel": "orderbook",
        "market": "KRW-BTC",
        "ts_ms": ts_ms,
        "ask1_price": 101.0,
        "ask1_size": 1.0,
        "ask2_price": 102.0,
        "ask2_size": 2.0,
        "bid1_price": 99.0,
        "bid1_size": 1.5,
        "bid2_price": 98.0,
        "bid2_size": 2.5,
    }
    compressor = zstd.ZstdCompressor()
    payload = (json.dumps(row, ensure_ascii=False) + "\n").encode("utf-8")
    with (target / "part-000.jsonl.zst").open("wb") as handle:
        handle.write(compressor.compress(payload))


def test_offline_provider_reads_snapshot_and_computes_notional(tmp_path: Path) -> None:
    micro_root = tmp_path / "micro_v1"
    _write_micro_part(micro_root)
    provider = OfflineMicroSnapshotProvider(micro_root=micro_root, tf="5m")

    snapshot = provider.get("KRW-BTC", 1_700_000_000_000)
    assert snapshot is not None
    assert snapshot.trade_events == 3
    assert snapshot.trade_source == "ws"
    assert snapshot.trade_notional_krw == 200.0
    assert snapshot.buy_count == 2
    assert snapshot.sell_count == 1
    assert snapshot.buy_volume == 1.2
    assert snapshot.sell_volume == 0.8
    assert snapshot.max_trade_size == 1.25
    assert snapshot.mid_mean == 100.5
    assert snapshot.depth_bid_top5_notional_krw == 500_000.0
    assert snapshot.depth_ask_top5_notional_krw == 550_000.0
    assert snapshot.depth_top5_notional_krw == 1_050_000.0
    assert snapshot.imbalance_top5_mean == -0.0476190476
    assert snapshot.microprice_bias_bps_mean == 3.25


def test_offline_provider_allows_small_timestamp_fallback(tmp_path: Path) -> None:
    micro_root = tmp_path / "micro_v1"
    _write_micro_part(micro_root)
    provider = OfflineMicroSnapshotProvider(micro_root=micro_root, tf="5m")

    # Within one 5m interval: should fallback to the latest <= ts.
    near_snapshot = provider.get("KRW-BTC", 1_700_000_060_000)
    assert near_snapshot is not None
    assert near_snapshot.snapshot_ts_ms == 1_700_000_000_000

    # Beyond one 5m interval: should be treated as missing.
    far_snapshot = provider.get("KRW-BTC", 1_700_000_400_001)
    assert far_snapshot is None


def test_offline_provider_overlays_historical_raw_orderbook_levels_when_available(tmp_path: Path) -> None:
    micro_root = tmp_path / "data" / "parquet" / "micro_v1"
    _write_micro_part(micro_root)
    _write_orderbook_part(tmp_path / "data" / "raw_ws" / "upbit" / "public", ts_ms=1_700_000_000_000)
    provider = OfflineMicroSnapshotProvider(micro_root=micro_root, tf="5m")

    snapshot = provider.get("KRW-BTC", 1_700_000_000_000)
    assert snapshot is not None
    assert snapshot.best_bid_price == 99.0
    assert snapshot.best_ask_price == 101.0
    assert snapshot.best_bid_notional_krw == 148.5
    assert snapshot.best_ask_notional_krw == 101.0
    assert snapshot.bid_levels == ((99.0, 1.5), (98.0, 2.5))
    assert snapshot.ask_levels == ((101.0, 1.0), (102.0, 2.0))
    assert snapshot.snapshot_ts_ms == 1_700_000_000_000

    near_snapshot = provider.get("KRW-BTC", 1_700_000_030_000)
    assert near_snapshot is not None
    assert near_snapshot.snapshot_ts_ms == 1_700_000_000_000
    assert near_snapshot.bid_levels == ((99.0, 1.5), (98.0, 2.5))
