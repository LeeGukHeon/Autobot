from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl

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
            "trade_volume_total": [2.0],
            "trade_imbalance": [0.2],
            "vwap": [100.0],
            "last_trade_price": [101.0],
            "trade_max_ts_ms": [ts_ms],
            "book_events": [2],
            "book_coverage_ms": [20_000],
            "book_max_ts_ms": [ts_ms],
            "micro_book_available": [True],
            "spread_bps_mean": [4.5],
            "depth_bid_top5_mean": [500_000.0],
            "depth_ask_top5_mean": [550_000.0],
        }
    ).write_parquet(target / "part-000.parquet")


def test_offline_provider_reads_snapshot_and_computes_notional(tmp_path: Path) -> None:
    micro_root = tmp_path / "micro_v1"
    _write_micro_part(micro_root)
    provider = OfflineMicroSnapshotProvider(micro_root=micro_root, tf="5m")

    snapshot = provider.get("KRW-BTC", 1_700_000_000_000)
    assert snapshot is not None
    assert snapshot.trade_events == 3
    assert snapshot.trade_source == "ws"
    assert snapshot.trade_notional_krw == 200.0
    assert snapshot.depth_top5_notional_krw == 1_050_000.0


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
