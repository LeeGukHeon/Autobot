from __future__ import annotations

from pathlib import Path

import polars as pl

from autobot.data.micro.validate_micro_v1 import detect_alignment_mode


def test_detect_alignment_mode_prefers_end_when_end_matches_better(tmp_path: Path) -> None:
    candles_root = tmp_path / "candles_v1"
    market_dir = candles_root / "tf=1m" / "market=KRW-BTC"
    market_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"ts_ms": [120_000, 180_000, 240_000]}).write_parquet(market_dir / "part.parquet")

    micro = pl.DataFrame(
        {
            "market": ["KRW-BTC", "KRW-BTC", "KRW-BTC"],
            "ts_ms": [60_000, 120_000, 181_000],
            "trade_events": [1, 1, 1],
        }
    )

    result = detect_alignment_mode(
        micro_frame=micro,
        base_candles_root=candles_root,
        sample_market="KRW-BTC",
        interval_ms=60_000,
    )

    assert result["mode"] == "end"
    assert float(result["match_ratio_end"] or 0.0) > float(result["match_ratio_start"] or 0.0)
