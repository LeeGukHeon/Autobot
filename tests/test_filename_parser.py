from __future__ import annotations

import pytest

from autobot.data.filename_parser import FilenameParseError, parse_upbit_filename


def test_parse_upbit_filename_success() -> None:
    parsed = parse_upbit_filename("upbit_KRW_0G_15m_full.csv", supported_tfs=("1m", "5m", "15m"))
    assert parsed == {
        "quote": "KRW",
        "symbol": "0G",
        "tf": "15m",
        "market": "KRW-0G",
    }


def test_parse_upbit_filename_rejects_unsupported_tf() -> None:
    with pytest.raises(FilenameParseError) as exc:
        parse_upbit_filename("upbit_KRW_0G_60m_full.csv", supported_tfs=("1m", "5m"))
    assert exc.value.code == "UNSUPPORTED_TF"
