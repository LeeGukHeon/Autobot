from __future__ import annotations

from autobot.live.identifier import is_bot_identifier, new_order_identifier


def test_identifier_generation_and_classification() -> None:
    identifier = new_order_identifier(
        prefix="AUTOBOT",
        bot_id="autobot-001",
        intent_id="intent-123",
        nonce="abc123",
        ts_ms=1_700_000_000_000,
    )

    assert identifier.startswith("AUTOBOT-autobot-001-intent-123-1700000000000-")
    assert is_bot_identifier(identifier, prefix="AUTOBOT", bot_id="autobot-001")
    assert not is_bot_identifier(identifier, prefix="AUTOBOT", bot_id="autobot-999")
