from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import zstandard as zstd

from autobot.data.collect.backfill_trade_candles_from_raw_ws import (
    RawWsTradeCandleBackfillOptions,
    backfill_trade_candles_from_raw_ws,
)


def test_backfill_trade_candles_from_raw_ws_writes_second_and_minute_candles(tmp_path: Path) -> None:
    raw_root = tmp_path / "data" / "raw_ws" / "upbit" / "public"
    _write_trade_part(
        raw_root / "trade" / "date=2025-03-15" / "hour=00" / "part-000.jsonl.zst",
        [
            {
                "channel": "trade",
                "market": "KRW-BTC",
                "trade_ts_ms": 1_742_000_000_100,
                "price": 100.0,
                "volume": 1.0,
                "ask_bid": "BID",
                "source": "ws",
                "collected_at_ms": 1_742_000_000_200,
            },
            {
                "channel": "trade",
                "market": "KRW-BTC",
                "trade_ts_ms": 1_742_000_000_800,
                "price": 101.0,
                "volume": 2.0,
                "ask_bid": "ASK",
                "source": "ws",
                "collected_at_ms": 1_742_000_000_900,
            },
            {
                "channel": "trade",
                "market": "KRW-BTC",
                "trade_ts_ms": 1_742_000_060_100,
                "price": 102.0,
                "volume": 3.0,
                "ask_bid": "BID",
                "source": "ws",
                "collected_at_ms": 1_742_000_060_200,
            },
        ],
    )

    summary = backfill_trade_candles_from_raw_ws(
        RawWsTradeCandleBackfillOptions(
            raw_ws_root=raw_root,
            parquet_root=tmp_path / "data" / "parquet",
            meta_dir=tmp_path / "data" / "collect" / "_meta",
            start="2025-03-15",
            end="2025-03-15",
            quote="KRW",
        )
    )

    assert summary.rows_written_total > 0
    second_frame = pl.read_parquet(tmp_path / "data" / "parquet" / "candles_second_v1" / "tf=1s" / "market=KRW-BTC" / "part-000.parquet")
    ws_one_s_frame = pl.read_parquet(tmp_path / "data" / "parquet" / "ws_candle_v1" / "tf=1s" / "market=KRW-BTC" / "part-000.parquet")
    ws_one_m_frame = pl.read_parquet(tmp_path / "data" / "parquet" / "ws_candle_v1" / "tf=1m" / "market=KRW-BTC" / "part-000.parquet")

    assert second_frame.height == 2
    assert ws_one_s_frame.height == 2
    assert ws_one_m_frame.height == 2
    assert int(ws_one_m_frame.item(0, "ts_ms")) == 1_742_000_040_000
    assert float(ws_one_m_frame.item(0, "open")) == 100.0
    assert float(ws_one_m_frame.item(0, "close")) == 101.0
    assert float(ws_one_m_frame.item(0, "volume_base")) == 3.0
    assert summary.summary_path.exists()


def _write_trade_part(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows).encode("utf-8")
    path.write_bytes(zstd.ZstdCompressor(level=3).compress(payload))
