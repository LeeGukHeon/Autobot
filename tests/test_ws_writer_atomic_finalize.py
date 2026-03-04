from __future__ import annotations

from pathlib import Path

from autobot.data.collect.validate_ws_public import validate_ws_public_raw_dataset
from autobot.data.collect.ws_public_writer import WsRawRotatingWriter


def test_ws_writer_atomic_finalize_and_validate_ignores_tmp(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw_ws" / "upbit" / "public"
    meta_dir = tmp_path / "raw_ws" / "upbit" / "_meta"
    meta_dir.mkdir(parents=True, exist_ok=True)

    writer = WsRawRotatingWriter(
        raw_root=raw_root,
        run_id="atomic-finalize-test",
        rotate_sec=3600,
        max_bytes=64 * 1024 * 1024,
    )
    row = {
        "channel": "trade",
        "market": "KRW-BTC",
        "trade_ts_ms": 1_700_000_000_000,
        "recv_ts_ms": 1_700_000_000_010,
        "price": 100.0,
        "volume": 0.1,
        "ask_bid": "BID",
        "source": "ws",
        "collected_at_ms": 1_700_000_000_020,
    }
    writer.write(channel="trade", row=row, event_ts_ms=int(row["trade_ts_ms"]))

    tmp_files_before_close = list(raw_root.glob("**/*.jsonl.zst.tmp"))
    assert len(tmp_files_before_close) == 1
    assert list(raw_root.glob("**/*.jsonl.zst")) == []

    closed_parts = writer.close()
    assert len(closed_parts) == 1
    part_file = Path(closed_parts[0]["part_file"])
    assert part_file.exists()
    assert list(raw_root.glob("**/*.jsonl.zst.tmp")) == []

    stray_tmp = part_file.with_name("part-stray.jsonl.zst.tmp")
    stray_tmp.write_bytes(b"corrupt-temp")

    summary = validate_ws_public_raw_dataset(
        raw_root=raw_root,
        meta_dir=meta_dir,
        report_path=meta_dir / "ws_validate_report.json",
    )
    assert summary.fail_files == 0
    assert summary.checked_files == 1

