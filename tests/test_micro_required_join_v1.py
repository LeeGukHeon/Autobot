from __future__ import annotations

import polars as pl

from autobot.features.micro_required_join_v1 import join_micro_required


def test_micro_required_join_drops_rows_without_micro() -> None:
    base = pl.DataFrame(
        {
            "ts_ms": [0, 300_000, 600_000],
            "x": [1.0, 2.0, 3.0],
        }
    )
    micro = pl.DataFrame(
        {
            "market": ["KRW-BTC", "KRW-BTC"],
            "tf": ["5m", "5m"],
            "ts_ms": [0, 600_000],
            "trade_source": ["ws", "rest"],
            "trade_events": [3, 2],
            "book_events": [4, 1],
            "trade_coverage_ms": [240_000, 200_000],
            "book_coverage_ms": [250_000, 180_000],
            "micro_trade_available": [True, True],
            "micro_book_available": [True, True],
            "micro_available": [True, True],
            "trade_volume_total": [10.0, 5.0],
            "buy_volume": [6.0, 1.0],
            "sell_volume": [4.0, 4.0],
            "spread_bps_mean": [1.2, 2.4],
        }
    )

    result = join_micro_required(base_frame=base, micro_frame=micro, micro_tf_used="5m")

    assert result.rows_before == 3
    assert result.rows_after == 2
    assert result.rows_dropped_no_micro == 1
    assert "m_spread_proxy" in result.frame.columns
    assert "m_trade_buy_ratio" in result.frame.columns
    assert "m_source_ws" in result.frame.columns
