from __future__ import annotations

from pathlib import Path

import polars as pl

from autobot.backtest.loader import CandleDataLoader


def _write_market_parquet(root: Path, *, market: str) -> None:
    target = root / "candles_v1" / "tf=1m" / f"market={market}"
    target.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame(
        {
            "ts_ms": [0, 120_000],
            "open": [100.0, 110.0],
            "high": [101.0, 111.0],
            "low": [99.0, 109.0],
            "close": [100.0, 110.0],
            "volume_base": [1.0, 2.0],
            "volume_quote": [100.0, 220.0],
            "volume_quote_est": [False, False],
        }
    )
    frame.write_parquet(target / "part-000.parquet")


def test_loader_dense_grid_inserts_missing_bars(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    _write_market_parquet(parquet_root, market="KRW-BTC")

    sparse_loader = CandleDataLoader(
        parquet_root=parquet_root,
        dataset_name="candles_v1",
        tf="1m",
        dense_grid=False,
    )
    dense_loader = CandleDataLoader(
        parquet_root=parquet_root,
        dataset_name="candles_v1",
        tf="1m",
        dense_grid=True,
    )

    sparse = sparse_loader.load_market_bars(market="KRW-BTC")
    dense = dense_loader.load_market_bars(market="KRW-BTC")

    assert len(sparse) == 2
    assert len(dense) == 3
    synthetic = dense[1]
    assert synthetic.ts_ms == 60_000
    assert synthetic.open == 100.0
    assert synthetic.high == 100.0
    assert synthetic.low == 100.0
    assert synthetic.close == 100.0
    assert synthetic.volume_base == 0.0
    assert synthetic.volume_quote == 0.0
