from __future__ import annotations

import json
from pathlib import Path

from autobot.data.collect.validate_ws_public import validate_ws_public_raw_dataset
from autobot.data.collect.ws_public_collector import _normalize_public_ws_row
from autobot.data.collect.ws_public_manifest import append_ws_manifest_rows
from autobot.data.collect.ws_public_writer import WsRawRotatingWriter


def test_ws_offline_fixture_normalize_write_validate(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw_ws" / "upbit" / "quotation"
    meta_dir = tmp_path / "raw_ws" / "upbit" / "_meta"
    meta_dir.mkdir(parents=True, exist_ok=True)

    plan = {
        "codes": ["KRW-BTC", "KRW-ETH"],
        "filters": {"channels": ["ticker", "trade", "orderbook"]},
    }
    (meta_dir / "ws_public_plan.json").write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")

    writer = WsRawRotatingWriter(
        raw_root=raw_root,
        run_id="offline-fixture",
        rotate_sec=300,
        max_bytes=64 * 1024 * 1024,
    )

    ticker_message = {
        "type": "ticker",
        "code": "KRW-BTC",
        "timestamp": 1_700_000_000_050,
        "trade_price": 101.0,
        "acc_trade_price_24h": 5_000_000_000.0,
        "market_state": "ACTIVE",
        "market_warning": "NONE",
    }
    trade_message = {
        "type": "trade",
        "code": "KRW-BTC",
        "trade_timestamp": 1_700_000_000_000,
        "timestamp": 1_700_000_000_001,
        "trade_price": 101.0,
        "trade_volume": 0.12,
        "ask_bid": "ASK",
        "sequential_id": 1001,
    }
    orderbook_message = {
        "type": "orderbook",
        "code": "KRW-BTC",
        "timestamp": 1_700_000_000_500,
        "total_ask_size": 10.0,
        "total_bid_size": 9.0,
        "orderbook_units": [
            {"ask_price": 101.1, "ask_size": 1.1, "bid_price": 100.9, "bid_size": 1.2},
            {"ask_price": 101.2, "ask_size": 1.3, "bid_price": 100.8, "bid_size": 1.4},
        ],
    }

    ticker_row = _normalize_public_ws_row(
        message=ticker_message,
        orderbook_topk=5,
        orderbook_level=0,
        collected_at_ms=1_700_000_000_060,
    )
    trade_row = _normalize_public_ws_row(
        message=trade_message,
        orderbook_topk=5,
        orderbook_level=0,
        collected_at_ms=1_700_000_000_010,
    )
    orderbook_row = _normalize_public_ws_row(
        message=orderbook_message,
        orderbook_topk=5,
        orderbook_level=0,
        collected_at_ms=1_700_000_000_510,
    )
    assert ticker_row is not None
    assert trade_row is not None
    assert orderbook_row is not None

    writer.write(channel="ticker", row=ticker_row, event_ts_ms=int(ticker_row["ts_ms"]))
    writer.write(channel="trade", row=trade_row, event_ts_ms=int(trade_row["trade_ts_ms"]))
    writer.write(channel="orderbook", row=orderbook_row, event_ts_ms=int(orderbook_row["ts_ms"]))
    manifest_rows = writer.close()

    append_ws_manifest_rows(meta_dir / "ws_manifest.parquet", manifest_rows)
    summary = validate_ws_public_raw_dataset(
        raw_root=raw_root,
        meta_dir=meta_dir,
        report_path=meta_dir / "ws_validate_report.json",
    )

    assert summary.fail_files == 0
    assert summary.checked_files >= 3
    assert summary.rows_total == 3
    assert summary.parse_ok_ratio == 1.0
