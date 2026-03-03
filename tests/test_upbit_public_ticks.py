from __future__ import annotations

import pytest

from autobot.upbit.exceptions import ValidationError
from autobot.upbit.public import UpbitPublicClient


class _StubHttpClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def request_json(self, method: str, endpoint: str, **kwargs: object) -> dict[str, object]:
        self.calls.append(
            {
                "method": method,
                "endpoint": endpoint,
                "kwargs": kwargs,
            }
        )
        return {"ok": True}


def test_trades_ticks_uses_trade_group_and_normalized_params() -> None:
    stub = _StubHttpClient()
    client = UpbitPublicClient(stub)  # type: ignore[arg-type]

    payload = client.trades_ticks(
        market="krw-btc",
        count=300,
        cursor="123456789",
        days_ago=3,
    )

    assert payload == {"ok": True}
    call = stub.calls[-1]
    assert call["method"] == "GET"
    assert call["endpoint"] == "/v1/trades/ticks"
    assert call["kwargs"]["params"] == [
        ("market", "KRW-BTC"),
        ("count", 200),
        ("cursor", "123456789"),
        ("daysAgo", 3),
    ]
    assert call["kwargs"]["rate_limit_group"] == "trade"


@pytest.mark.parametrize("days_ago", [0, 8])
def test_trades_ticks_rejects_out_of_range_days_ago(days_ago: int) -> None:
    stub = _StubHttpClient()
    client = UpbitPublicClient(stub)  # type: ignore[arg-type]

    with pytest.raises(ValidationError):
        client.trades_ticks(market="KRW-BTC", days_ago=days_ago)
