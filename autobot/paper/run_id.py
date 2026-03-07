"""Run-id helpers for paper runtime."""

from __future__ import annotations

import time
import uuid


def build_paper_run_id() -> str:
    token = uuid.uuid4().hex[:8]
    return f"paper-{time.strftime('%Y%m%d-%H%M%S')}-{token}"
