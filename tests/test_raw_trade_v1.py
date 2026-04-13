from __future__ import annotations

from autobot.data.sources.trades import (
    RAW_TRADE_V1_COLUMNS,
    canonical_trade_key,
    merge_canonical_trade_rows,
    normalize_rest_trade_row,
    normalize_ws_trade_row,
)


def test_normalize_rest_trade_row_builds_canonical_schema() -> None:
    row = normalize_rest_trade_row(
        {
            "market": "KRW-BTC",
            "timestamp_ms": 1_700_000_000_000,
            "trade_price": 100.5,
            "trade_volume": 0.1,
            "ask_bid": "BID",
            "sequential_id": 101,
            "days_ago": 1,
            "collected_at_ms": 1_700_000_000_500,
        }
    )

    assert row is not None
    assert tuple(row.keys()) == RAW_TRADE_V1_COLUMNS
    assert row["source"] == "rest"
    assert row["side"] == "buy"
    assert row["sequential_id"] == 101
    assert row["days_ago"] == 1


def test_normalize_ws_trade_row_builds_canonical_schema() -> None:
    row = normalize_ws_trade_row(
        {
            "channel": "trade",
            "market": "KRW-BTC",
            "trade_ts_ms": 1_700_000_000_000,
            "recv_ts_ms": 1_700_000_000_050,
            "price": 100.5,
            "volume": 0.1,
            "ask_bid": "ASK",
            "sequential_id": 101,
            "collected_at_ms": 1_700_000_000_060,
        }
    )

    assert row is not None
    assert tuple(row.keys()) == RAW_TRADE_V1_COLUMNS
    assert row["source"] == "ws"
    assert row["side"] == "sell"
    assert row["recv_ts_ms"] == 1_700_000_000_050
    assert row["days_ago"] is None


def test_merge_canonical_trade_rows_prefers_ws_over_rest_for_same_trade() -> None:
    rest_row = normalize_rest_trade_row(
        {
            "market": "KRW-BTC",
            "timestamp_ms": 1_700_000_000_000,
            "trade_price": 100.5,
            "trade_volume": 0.1,
            "ask_bid": "BID",
            "sequential_id": 101,
            "days_ago": 1,
            "collected_at_ms": 1_700_000_000_500,
        }
    )
    ws_row = normalize_ws_trade_row(
        {
            "channel": "trade",
            "market": "KRW-BTC",
            "trade_ts_ms": 1_700_000_000_000,
            "recv_ts_ms": 1_700_000_000_050,
            "price": 100.5,
            "volume": 0.1,
            "ask_bid": "BID",
            "sequential_id": 101,
            "collected_at_ms": 1_700_000_000_060,
        }
    )

    merged = merge_canonical_trade_rows([rest_row], [ws_row])

    assert len(merged) == 1
    assert merged[0]["source"] == "ws"
    assert canonical_trade_key(merged[0]) == ("KRW-BTC", 101)


def test_merge_canonical_trade_rows_keeps_distinct_sequential_ids() -> None:
    row_a = normalize_ws_trade_row(
        {
            "channel": "trade",
            "market": "KRW-BTC",
            "trade_ts_ms": 1_700_000_000_000,
            "recv_ts_ms": 1_700_000_000_050,
            "price": 100.5,
            "volume": 0.1,
            "ask_bid": "BID",
            "sequential_id": 101,
            "collected_at_ms": 1_700_000_000_060,
        }
    )
    row_b = normalize_rest_trade_row(
        {
            "market": "KRW-BTC",
            "timestamp_ms": 1_700_000_001_000,
            "trade_price": 101.5,
            "trade_volume": 0.2,
            "ask_bid": "ASK",
            "sequential_id": 102,
            "days_ago": 1,
            "collected_at_ms": 1_700_000_001_500,
        }
    )

    merged = merge_canonical_trade_rows([row_b], [row_a])

    assert [item["sequential_id"] for item in merged] == [101, 102]
