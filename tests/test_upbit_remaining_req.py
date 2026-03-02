from __future__ import annotations

from autobot.upbit.remaining_req import parse_remaining_req_header


def test_parse_remaining_req_header_success() -> None:
    parsed = parse_remaining_req_header("group=default; min=1800; sec=29")
    assert parsed is not None
    assert parsed.group == "default"
    assert parsed.min == 1800
    assert parsed.sec == 29


def test_parse_remaining_req_header_returns_none_for_invalid() -> None:
    assert parse_remaining_req_header("group=default; min=1800") is None
    assert parse_remaining_req_header("group=default; sec=abc") is None
