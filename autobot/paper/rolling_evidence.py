"""Rolling evidence helpers for paper-run summaries."""

from __future__ import annotations

from typing import Any, Sequence

from autobot.common.rolling_window_evidence import compute_rolling_window_evidence


def compute_rolling_paper_evidence(
    *,
    equity_samples: Sequence[dict[str, Any]],
    fill_records: Sequence[dict[str, Any]],
    window_ms: int = 3_600_000,
) -> dict[str, Any]:
    return compute_rolling_window_evidence(
        equity_samples=equity_samples,
        fill_records=fill_records,
        window_ms=window_ms,
    )
