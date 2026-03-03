"""Backtest report writing helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def write_summary_json(run_dir: Path, summary: dict[str, Any]) -> Path:
    path = Path(run_dir) / "summary.json"
    payload = json.dumps(summary, ensure_ascii=False, indent=2)
    path.write_text(payload + "\n", encoding="utf-8")
    return path
