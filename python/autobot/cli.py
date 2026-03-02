"""Compatibility notice for deprecated python/autobot package mirror."""

from __future__ import annotations


def main() -> int:
    print("[deprecated] python/autobot is not a supported execution target.")
    print("Use repo root package instead: python -m autobot.cli ...")
    print("If you need a mirror copy, run: powershell -File scripts/sync_python_autobot.ps1")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
