from __future__ import annotations

from pathlib import Path

from autobot.data.collect.ticks_checkpoint import (
    checkpoint_key,
    load_ticks_checkpoint,
    save_ticks_checkpoint,
    update_ticks_checkpoint,
)


def test_ticks_checkpoint_roundtrip(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "ticks_checkpoint.json"
    state: dict[str, dict] = {}
    update_ticks_checkpoint(
        state,
        market="KRW-BTC",
        days_ago=1,
        last_cursor="12345",
        last_success_ts_ms=1_700_000_000_000,
        pages_collected=5,
        updated_at_ms=1_700_000_001_000,
    )
    save_ticks_checkpoint(checkpoint_path, state)

    loaded = load_ticks_checkpoint(checkpoint_path)
    key = checkpoint_key("KRW-BTC", 1)
    assert key in loaded
    assert loaded[key]["market"] == "KRW-BTC"
    assert loaded[key]["days_ago"] == 1
    assert loaded[key]["last_cursor"] == "12345"
    assert loaded[key]["pages_collected"] == 5


def test_ticks_checkpoint_uses_target_date_when_present(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "ticks_checkpoint.json"
    state: dict[str, dict] = {}
    update_ticks_checkpoint(
        state,
        market="KRW-BTC",
        days_ago=1,
        target_date="2026-03-29",
        last_cursor="999",
        last_success_ts_ms=1_700_000_000_000,
        pages_collected=2,
        updated_at_ms=1_700_000_001_000,
    )
    save_ticks_checkpoint(checkpoint_path, state)

    loaded = load_ticks_checkpoint(checkpoint_path)
    key = checkpoint_key("KRW-BTC", target_date="2026-03-29")
    assert key in loaded
    assert loaded[key]["target_date"] == "2026-03-29"
    assert loaded[key]["last_cursor"] == "999"


def test_load_ticks_checkpoint_missing_file_returns_empty(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "missing.json"
    assert load_ticks_checkpoint(checkpoint_path) == {}
