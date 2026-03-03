from __future__ import annotations

from pathlib import Path

from autobot.data.collect.ws_public_writer import WsRawRotatingWriter, read_ws_part_file


def test_ws_writer_rotation_and_read(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw_ws" / "upbit" / "quotation"
    writer = WsRawRotatingWriter(
        raw_root=raw_root,
        run_id="test-run",
        rotate_sec=3600,
        max_bytes=1024,
    )

    base_row = {
        "channel": "trade",
        "market": "KRW-BTC",
        "trade_ts_ms": 1_700_000_000_000,
        "recv_ts_ms": 1_700_000_000_010,
        "price": 100.0,
        "volume": 0.1,
        "ask_bid": "BID",
        "source": "ws",
        "collected_at_ms": 1_700_000_000_020,
        "pad": "x" * 1500,
    }

    writer.write(channel="trade", row=base_row, event_ts_ms=1_700_000_000_000)
    next_row = dict(base_row)
    next_row["trade_ts_ms"] = 1_700_000_000_100
    next_row["recv_ts_ms"] = 1_700_000_000_110
    writer.write(channel="trade", row=next_row, event_ts_ms=1_700_000_000_100)

    closed_parts = writer.close()
    assert len(closed_parts) >= 2
    assert sum(int(item["rows"]) for item in closed_parts) == 2

    first_file = Path(closed_parts[0]["part_file"])
    rows = read_ws_part_file(first_file)
    assert rows
    assert rows[0]["market"] == "KRW-BTC"

    tmp_files = list(raw_root.glob("**/*.tmp"))
    assert tmp_files == []
