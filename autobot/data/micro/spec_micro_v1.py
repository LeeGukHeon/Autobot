"""Micro v1 dataset spec helpers."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any


MICRO_SPEC_VERSION = "micro_v1"


def build_micro_spec(*, topk: int = 5) -> dict[str, Any]:
    return {
        "version": MICRO_SPEC_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "timeframes": ["1m", "5m"],
        "topk": int(max(topk, 1)),
        "key_columns": ["market", "tf", "ts_ms"],
        "coverage_columns": [
            "trade_source",
            "trade_events",
            "book_events",
            "trade_min_ts_ms",
            "trade_max_ts_ms",
            "book_min_ts_ms",
            "book_max_ts_ms",
            "trade_coverage_ms",
            "book_coverage_ms",
            "micro_trade_available",
            "micro_book_available",
            "micro_available",
        ],
        "trade_columns": [
            "trade_count",
            "buy_count",
            "sell_count",
            "trade_volume_total",
            "buy_volume",
            "sell_volume",
            "trade_imbalance",
            "vwap",
            "avg_trade_size",
            "max_trade_size",
            "last_trade_price",
        ],
        "orderbook_columns": [
            "mid_mean",
            "spread_bps_mean",
            "depth_bid_top5_mean",
            "depth_ask_top5_mean",
            "imbalance_top5_mean",
            "microprice_bias_bps_mean",
            "book_update_count",
        ],
        "notes": {
            "trade_merge": "ws-preferred-rest-fallback-on-each-bar",
            "orderbook_source": "ws-only",
            "resample_5m": "weighted-mean-for-event-based-book-metrics",
            "lookahead_safe": True,
        },
    }


def write_micro_spec(path: Path, *, topk: int = 5) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = build_micro_spec(topk=topk)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return path
