"""Detect and optionally terminate stale live_v5 paper alpha processes."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
from typing import Any, Callable


def _parse_process_table(raw_text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw_line in str(raw_text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split(maxsplit=1)
        if len(parts) != 2:
            continue
        pid_text, args = parts
        try:
            pid = int(pid_text)
        except ValueError:
            continue
        rows.append({"pid": pid, "args": str(args).strip()})
    return rows


def _is_target_live_v5_paper_alpha(args: str) -> bool:
    normalized = f" {str(args or '').strip()} ".lower()
    required_tokens = (
        " autobot.cli ",
        " paper ",
        " alpha ",
        " --preset ",
        " live_v5 ",
        " --duration-sec ",
        " 0 ",
    )
    return all(token in normalized for token in required_tokens)


def find_stale_live_v5_paper_alpha_processes(
    raw_process_table: str,
    *,
    current_pid: int | None = None,
) -> list[dict[str, Any]]:
    rows = _parse_process_table(raw_process_table)
    stale: list[dict[str, Any]] = []
    for row in rows:
        pid = int(row["pid"])
        if current_pid is not None and pid == int(current_pid):
            continue
        args = str(row.get("args") or "")
        if _is_target_live_v5_paper_alpha(args):
            stale.append({"pid": pid, "args": args})
    return stale


def terminate_process_rows(
    rows: list[dict[str, Any]],
    *,
    kill_fn: Callable[[int, int], None] | None = None,
) -> dict[str, Any]:
    resolved_kill = kill_fn or os.kill
    terminated: list[int] = []
    failed: list[dict[str, Any]] = []
    for row in rows:
        pid = int(row["pid"])
        try:
            resolved_kill(pid, signal.SIGTERM)
            terminated.append(pid)
        except Exception as exc:  # pragma: no cover - exercised with injected kill_fn
            failed.append({"pid": pid, "error": str(exc)})
    return {
        "terminated_pids": terminated,
        "failed": failed,
        "terminated_count": len(terminated),
        "failed_count": len(failed),
    }


def collect_unix_process_table() -> str:
    completed = subprocess.run(
        ["ps", "-eo", "pid=,args="],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return completed.stdout or ""


def run_cleanup(*, apply: bool) -> dict[str, Any]:
    if os.name == "nt":
        return {
            "policy": "paper_alpha_process_guard_v1",
            "platform_supported": False,
            "platform": os.name,
            "apply": bool(apply),
            "reason": "WINDOWS_PROCESS_SCAN_NOT_IMPLEMENTED",
            "matches": [],
            "terminated_pids": [],
            "failed": [],
            "terminated_count": 0,
            "failed_count": 0,
        }
    raw_process_table = collect_unix_process_table()
    matches = find_stale_live_v5_paper_alpha_processes(raw_process_table, current_pid=os.getpid())
    termination = terminate_process_rows(matches) if apply else {
        "terminated_pids": [],
        "failed": [],
        "terminated_count": 0,
        "failed_count": 0,
    }
    return {
        "policy": "paper_alpha_process_guard_v1",
        "platform_supported": True,
        "platform": os.name,
        "apply": bool(apply),
        "matches": matches,
        **termination,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Cleanup stale live_v5 paper alpha processes")
    parser.add_argument("--apply", action="store_true", help="Terminate matching processes")
    parser.add_argument("--out", default="", help="Optional JSON output path")
    args = parser.parse_args()

    report = run_cleanup(apply=bool(args.apply))
    output = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    out_path = str(args.out or "").strip()
    if out_path:
        path = Path(out_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(output, encoding="utf-8")
    print(output, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
