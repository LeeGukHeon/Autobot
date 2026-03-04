from __future__ import annotations

import math

import polars as pl

from autobot.features.micro_join import join_market_micro


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
