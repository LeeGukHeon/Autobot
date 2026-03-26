from __future__ import annotations

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


def test_candles_seconds_uses_second_endpoint_and_candle_group() -> None:
    stub = _StubHttpClient()
    client = UpbitPublicClient(stub)  # type: ignore[arg-type]

    payload = client.candles_seconds(
        market="krw-btc",
        count=500,
        to="2026-03-27T00:00:10+00:00",
    )

    assert payload == {"ok": True}
    call = stub.calls[-1]
    assert call["method"] == "GET"
    assert call["endpoint"] == "/v1/candles/seconds"
    assert call["kwargs"]["params"] == [
        ("market", "KRW-BTC"),
        ("count", 200),
        ("to", "2026-03-27T00:00:10+00:00"),
    ]
    assert call["kwargs"]["rate_limit_group"] == "candle"
