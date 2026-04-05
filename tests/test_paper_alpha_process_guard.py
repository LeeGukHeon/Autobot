from __future__ import annotations

from autobot.ops.paper_alpha_process_guard import (
    find_stale_live_v5_paper_alpha_processes,
    terminate_process_rows,
)


def test_find_stale_live_v5_paper_alpha_processes_matches_only_target_command() -> None:
    raw = "\n".join(
        [
            "101 python -m autobot.cli paper alpha --preset live_v5 --duration-sec 0 --quote KRW",
            "102 python -m autobot.cli paper alpha --preset live_v5 --duration-sec 180 --quote KRW",
            "103 python -m autobot.cli paper run --paper-feature-provider live_v5 --duration-sec 0",
        ]
    )

    rows = find_stale_live_v5_paper_alpha_processes(raw, current_pid=9999)

    assert rows == [
        {
            "pid": 101,
            "args": "python -m autobot.cli paper alpha --preset live_v5 --duration-sec 0 --quote KRW",
        }
    ]


def test_terminate_process_rows_is_safe_for_noop_and_kill_paths() -> None:
    calls: list[tuple[int, int]] = []

    def _fake_kill(pid: int, sig: int) -> None:
        calls.append((pid, sig))

    empty = terminate_process_rows([], kill_fn=_fake_kill)
    assert empty["terminated_count"] == 0
    assert empty["failed_count"] == 0

    result = terminate_process_rows(
        [{"pid": 301, "args": "python -m autobot.cli paper alpha --preset live_v5 --duration-sec 0"}],
        kill_fn=_fake_kill,
    )
    assert result["terminated_pids"] == [301]
    assert result["failed"] == []
    assert calls and calls[0][0] == 301
