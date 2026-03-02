from __future__ import annotations

from autobot.upbit.querystring import build_query_string


def test_build_query_string_repeats_array_keys() -> None:
    query = build_query_string([("states[]", ["wait", "watch"])])
    assert query == "states[]=wait&states[]=watch"


def test_build_query_string_preserves_input_order() -> None:
    query = build_query_string([("market", "KRW-BTC"), ("states[]", ["wait", "watch"]), ("limit", 10)])
    assert query == "market=KRW-BTC&states[]=wait&states[]=watch&limit=10"


def test_build_query_string_keeps_dict_insertion_order() -> None:
    query = build_query_string({"b": 1, "a": 2})
    assert query == "b=1&a=2"
