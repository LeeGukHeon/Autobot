from __future__ import annotations

from autobot.live.identifier import extract_intent_id_from_identifier, extract_run_token_from_identifier, is_bot_identifier, new_order_identifier


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


def test_identifier_generation_supports_run_token() -> None:
    identifier = new_order_identifier(
        prefix="AUTOBOT",
        bot_id="autobot-001",
        intent_id="intent-123",
        run_token="20260312T193538Z-s42-d443dd89",
        nonce="abc123",
        ts_ms=1_700_000_000_000,
    )

    assert identifier.endswith("-rid_20260312t193538z-s42-d443dd89")
    assert extract_intent_id_from_identifier(identifier, prefix="AUTOBOT", bot_id="autobot-001") == "intent-123"
    assert (
        extract_run_token_from_identifier(identifier, prefix="AUTOBOT", bot_id="autobot-001")
        == "20260312t193538z-s42-d443dd89"
    )


def test_identifier_classification_accepts_risk_and_supervisor_prefixes() -> None:
    assert is_bot_identifier("AUTOBOT-RISK-model-risk-1773391515252", prefix="AUTOBOT", bot_id="autobot-001")
    assert is_bot_identifier("AUTOBOT-RISKREP-model-ri-1-1773391515252", prefix="AUTOBOT", bot_id="autobot-001")
    assert is_bot_identifier("AUTOBOT-SUPREP-intent-1-1773391515252", prefix="AUTOBOT", bot_id="autobot-001")
