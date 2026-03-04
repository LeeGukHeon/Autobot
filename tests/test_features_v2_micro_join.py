from __future__ import annotations

import math
from pathlib import Path

import polars as pl

from autobot.features.micro_join import join_market_micro, load_market_micro_frame


def test_micro_join_prefix_and_match_ratio() -> None:
    base = pl.DataFrame({"ts_ms": [0, 60_000, 120_000], "x": [1.0, 2.0, 3.0]})
    micro = pl.DataFrame(
        {
            "market": ["KRW-BTC", "KRW-BTC"],
            "tf": ["5m", "5m"],
            "ts_ms": [0, 120_000],
            "trade_source": ["ws", "none"],
            "trade_events": [3, 0],
            "book_events": [5, 1],
            "trade_coverage_ms": [55_000, 0],
            "book_coverage_ms": [57_000, 58_000],
            "micro_trade_available": [True, False],
            "micro_book_available": [True, True],
            "micro_available": [True, True],
        }
    )

    joined, stats = join_market_micro(base_frame=base, micro_frame=micro)

    assert "m_trade_source" in joined.columns
    assert "m_trade_events" in joined.columns
    assert "m_book_coverage_ms" in joined.columns
    assert stats.compared_rows == 3
    assert stats.matched_rows == 2
    assert stats.join_match_ratio is not None
    assert math.isclose(float(stats.join_match_ratio), 2.0 / 3.0, rel_tol=1e-9, abs_tol=1e-12)
    assert int(joined.filter(pl.col("ts_ms") == 60_000).item(0, "m_trade_events")) == 0
    assert str(joined.filter(pl.col("ts_ms") == 60_000).item(0, "m_trade_source")) == "none"


def test_load_market_micro_frame_handles_mixed_daily_schema(tmp_path: Path) -> None:
    market = "KRW-BTC"
    root = tmp_path / "micro_v1"
    d1 = root / "tf=5m" / f"market={market}" / "date=2026-03-01"
    d2 = root / "tf=5m" / f"market={market}" / "date=2026-03-02"
    d1.mkdir(parents=True, exist_ok=True)
    d2.mkdir(parents=True, exist_ok=True)

    pl.DataFrame(
        {
            "market": [market],
            "tf": ["5m"],
            "ts_ms": [1_772_323_200_000],
            "trade_source": ["ws"],
            "book_min_ts_ms": [None],
        }
    ).write_parquet(d1 / "part-000.parquet")
    pl.DataFrame(
        {
            "market": [market],
            "tf": ["5m"],
            "ts_ms": [1_772_409_600_000],
            "trade_source": ["ws"],
            "book_min_ts_ms": [1_772_409_580_000],
        }
    ).write_parquet(d2 / "part-000.parquet")

    loaded = load_market_micro_frame(
        micro_root=root,
        tf="5m",
        market=market,
        from_ts_ms=1_772_323_200_000,
        to_ts_ms=1_772_409_600_000,
    )

    assert loaded.height == 2
    assert "book_min_ts_ms" in loaded.columns
