from __future__ import annotations

from pathlib import Path

import polars as pl

from autobot.backtest.engine import (
    _cached_feature_groups,
    _cached_market_bars,
    clear_backtest_process_caches,
)
from autobot.backtest.loader import CandleDataLoader
from autobot.models.dataset_loader import FeatureTsGroup


def _write_market_parquet(root: Path, *, market: str) -> None:
    target = root / "candles_v1" / "tf=1m" / f"market={market}"
    target.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame(
        {
            "ts_ms": [0, 60_000, 120_000],
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.0, 101.0, 102.0],
            "volume_base": [1.0, 1.5, 2.0],
            "volume_quote": [100.0, 151.5, 204.0],
            "volume_quote_est": [False, False, False],
        }
    )
    frame.write_parquet(target / "part-000.parquet")


def test_cached_market_bars_reuses_loader_reads(tmp_path: Path, monkeypatch) -> None:
    clear_backtest_process_caches()
    parquet_root = tmp_path / "parquet"
    _write_market_parquet(parquet_root, market="KRW-BTC")

    calls = {"count": 0}
    original = CandleDataLoader.load_market_bars

    def _counted(self: CandleDataLoader, *, market: str, from_ts_ms=None, to_ts_ms=None):
        calls["count"] += 1
        return original(self, market=market, from_ts_ms=from_ts_ms, to_ts_ms=to_ts_ms)

    monkeypatch.setattr(CandleDataLoader, "load_market_bars", _counted)

    first = _cached_market_bars(
        str(parquet_root.resolve()),
        "candles_v1",
        "1m",
        False,
        "KRW-BTC",
        0,
        120_000,
    )
    second = _cached_market_bars(
        str(parquet_root.resolve()),
        "candles_v1",
        "1m",
        False,
        "KRW-BTC",
        0,
        120_000,
    )

    assert calls["count"] == 1
    assert tuple(first) == tuple(second)


def test_cached_feature_groups_reuses_materialization(monkeypatch, tmp_path: Path) -> None:
    clear_backtest_process_caches()
    calls = {"count": 0}

    def _fake_iter(request, *, feature_columns=None, extra_columns=()):
        calls["count"] += 1
        yield FeatureTsGroup(
            ts_ms=60_000,
            frame=pl.DataFrame(
                {
                    "ts_ms": [60_000],
                    "market": ["KRW-BTC"],
                    "f1": [1.0],
                    "close": [100.0],
                }
            ),
        )

    monkeypatch.setattr("autobot.backtest.engine.iter_feature_rows_grouped_by_ts", _fake_iter)

    args = (
        str((tmp_path / "features").resolve()),
        "5m",
        "KRW",
        1,
        60_000,
        60_000,
        ("KRW-BTC",),
        200_000,
        ("f1",),
        ("close",),
    )

    first = _cached_feature_groups(*args)
    second = _cached_feature_groups(*args)

    assert calls["count"] == 1
    assert len(first) == len(second) == 1
    assert first[0].ts_ms == second[0].ts_ms == 60_000
    assert first[0].frame.to_dicts() == second[0].frame.to_dicts()
