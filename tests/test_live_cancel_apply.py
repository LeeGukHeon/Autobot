from __future__ import annotations

from autobot.live.reconcile import apply_cancel_actions


def test_apply_cancel_actions_skips_external_without_double_opt_in() -> None:
    calls: list[tuple[str | None, str | None]] = []

    def _cancel(uuid: str | None, identifier: str | None) -> dict[str, str | None]:
        calls.append((uuid, identifier))
        return {"uuid": uuid, "identifier": identifier}

    summary = apply_cancel_actions(
        report={
            "actions": [
                {"type": "cancel_external_open_order", "uuid": "x-1", "identifier": "MANUAL-1"},
                {"type": "cancel_bot_open_order", "uuid": "b-1", "identifier": "AUTOBOT-1"},
            ]
        },
        cancel_order=_cancel,
        apply=True,
        allow_cancel_external_cli=False,
        allow_cancel_external_config=False,
    )

    assert summary["attempted"] == 2
    assert summary["executed"] == 1
    assert summary["skipped"] == 1
    assert calls == [("b-1", "AUTOBOT-1")]


def test_apply_cancel_actions_dry_run_executes_nothing() -> None:
    calls: list[tuple[str | None, str | None]] = []

    def _cancel(uuid: str | None, identifier: str | None) -> dict[str, str | None]:
        calls.append((uuid, identifier))
        return {"uuid": uuid, "identifier": identifier}

    summary = apply_cancel_actions(
        report={"actions": [{"type": "cancel_bot_open_order", "uuid": "b-2", "identifier": "AUTOBOT-2"}]},
        cancel_order=_cancel,
        apply=False,
        allow_cancel_external_cli=False,
        allow_cancel_external_config=False,
    )

    assert summary["attempted"] == 1
    assert summary["executed"] == 0
    assert summary["skipped"] == 1
    assert calls == []
