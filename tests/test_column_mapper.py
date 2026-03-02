from __future__ import annotations

import pytest

from autobot.data.column_mapper import ColumnMappingError, detect_column_mapping


def test_detect_column_mapping_upbit_style() -> None:
    headers = [
        "timestamp",
        "opening_price",
        "high_price",
        "low_price",
        "trade_price",
        "candle_acc_trade_volume",
        "candle_acc_trade_price",
    ]
    mapping = detect_column_mapping(headers)
    assert mapping.ts_source == "timestamp"
    assert mapping.ts_policy == "timestamp_ms"
    assert mapping.open_col == "opening_price"
    assert mapping.volume_quote_col == "candle_acc_trade_price"


def test_detect_column_mapping_generic_style() -> None:
    headers = ["datetime", "Open", "High", "Low", "Close", "Volume"]
    mapping = detect_column_mapping(headers)
    assert mapping.ts_source == "datetime"
    assert mapping.open_col == "Open"
    assert mapping.high_col == "High"
    assert mapping.low_col == "Low"
    assert mapping.close_col == "Close"
    assert mapping.volume_base_col == "Volume"
    assert mapping.volume_quote_col is None


def test_detect_column_mapping_missing_required() -> None:
    headers = ["timestamp", "open", "high", "close", "volume"]
    with pytest.raises(ColumnMappingError) as exc:
        detect_column_mapping(headers)
    assert "low" in exc.value.missing_fields
