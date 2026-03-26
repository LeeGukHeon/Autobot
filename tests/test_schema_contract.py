from __future__ import annotations

import pytest

from autobot.data.schema_contract import expected_interval_ms


@pytest.mark.parametrize(
    ("tf", "expected"),
    [
        ("1s", 1_000),
        ("1m", 60_000),
        ("3m", 180_000),
        ("5m", 300_000),
        ("10m", 600_000),
        ("15m", 900_000),
        ("30m", 1_800_000),
        ("60m", 3_600_000),
        ("240m", 14_400_000),
    ],
)
def test_expected_interval_ms(tf: str, expected: int) -> None:
    assert expected_interval_ms(tf) == expected


def test_expected_interval_ms_rejects_unknown_tf() -> None:
    with pytest.raises(ValueError):
        expected_interval_ms("7m")
