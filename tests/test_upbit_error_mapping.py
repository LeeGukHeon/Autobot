from __future__ import annotations

import pytest

from autobot.upbit.error_policy import classify_status


@pytest.mark.parametrize(
    ("status_code", "category", "retriable"),
    [
        (400, "validation", False),
        (401, "auth", False),
        (429, "rate_limit", True),
        (418, "rate_limit", True),
        (500, "server", True),
    ],
)
def test_classify_status(status_code: int, category: str, retriable: bool) -> None:
    assert classify_status(status_code) == (category, retriable)
