"""Rolling evidence helpers shared by paper and backtest validation."""

from __future__ import annotations

from typing import Any, Sequence
import math


def compute_rolling_window_evidence(
    *,
    equity_samples: Sequence[dict[str, Any]],
    fill_records: Sequence[dict[str, Any]],
    window_ms: int = 3_600_000,
) -> dict[str, Any]:
    samples = sorted(
        [
            {
                "ts_ms": int(item.get("ts_ms", 0)),
                "equity_quote": float(item.get("equity_quote", 0.0)),
                "realized_pnl_quote": float(item.get("realized_pnl_quote", 0.0)),
            }
            for item in equity_samples
            if isinstance(item, dict) and item.get("ts_ms") is not None
        ],
        key=lambda item: int(item["ts_ms"]),
    )
    fills = sorted(
        [
            {
                "ts_ms": int(item.get("ts_ms", 0)),
                "market": str(item.get("market", "")).strip().upper(),
            }
            for item in fill_records
            if isinstance(item, dict) and item.get("ts_ms") is not None
        ],
        key=lambda item: int(item["ts_ms"]),
    )
    if not samples:
        return {
            "window_minutes": max(int(window_ms // 60_000), 1),
            "windows_total": 0,
            "active_windows": 0,
            "nonnegative_active_window_ratio": 0.0,
            "positive_active_window_ratio": 0.0,
            "max_fill_concentration_ratio": 0.0,
            "max_window_drawdown_pct": 0.0,
            "worst_window_realized_pnl_quote": 0.0,
            "windows": [],
        }

    step_ms = max(int(window_ms), 60_000)
    start_ts = int(samples[0]["ts_ms"])
    end_ts = int(samples[-1]["ts_ms"])
    windows_total = max(int(math.ceil(max(end_ts - start_ts, 0) / float(step_ms))) + 1, 1)
    windows: list[dict[str, Any]] = []
    total_fills = len(fills)
    fill_index = 0

    for idx in range(windows_total):
        window_start = start_ts + (idx * step_ms)
        window_end = window_start + step_ms
        sample_rows = [item for item in samples if window_start <= int(item["ts_ms"]) < window_end]
        if not sample_rows:
            continue
        fill_count = 0
        while fill_index < total_fills and int(fills[fill_index]["ts_ms"]) < window_end:
            if int(fills[fill_index]["ts_ms"]) >= window_start:
                fill_count += 1
            fill_index += 1
        start_equity = float(sample_rows[0]["equity_quote"])
        end_equity = float(sample_rows[-1]["equity_quote"])
        start_realized = float(sample_rows[0]["realized_pnl_quote"])
        end_realized = float(sample_rows[-1]["realized_pnl_quote"])
        window_equity = [float(item["equity_quote"]) for item in sample_rows]
        windows.append(
            {
                "window_index": int(idx),
                "start_ts_ms": int(window_start),
                "end_ts_ms": int(min(window_end - 1, end_ts)),
                "fills": int(fill_count),
                "start_equity_quote": start_equity,
                "end_equity_quote": end_equity,
                "realized_pnl_delta_quote": end_realized - start_realized,
                "max_drawdown_pct": _max_drawdown_pct(window_equity),
            }
        )

    active_windows = [
        item
        for item in windows
        if int(item["fills"]) > 0 or abs(float(item["realized_pnl_delta_quote"])) > 1e-9
    ]
    active_count = len(active_windows)
    nonnegative_ratio = (
        float(sum(1 for item in active_windows if float(item["realized_pnl_delta_quote"]) >= 0.0)) / float(active_count)
        if active_count > 0
        else 0.0
    )
    positive_ratio = (
        float(sum(1 for item in active_windows if float(item["realized_pnl_delta_quote"]) > 0.0)) / float(active_count)
        if active_count > 0
        else 0.0
    )
    max_fill_concentration = (
        max(int(item["fills"]) for item in windows) / float(total_fills)
        if total_fills > 0 and windows
        else 0.0
    )
    max_window_drawdown = max((float(item["max_drawdown_pct"]) for item in windows), default=0.0)
    worst_window_realized = min((float(item["realized_pnl_delta_quote"]) for item in windows), default=0.0)
    return {
        "window_minutes": max(int(step_ms // 60_000), 1),
        "windows_total": int(len(windows)),
        "active_windows": int(active_count),
        "nonnegative_active_window_ratio": float(nonnegative_ratio),
        "positive_active_window_ratio": float(positive_ratio),
        "max_fill_concentration_ratio": float(max_fill_concentration),
        "max_window_drawdown_pct": float(max_window_drawdown),
        "worst_window_realized_pnl_quote": float(worst_window_realized),
        "windows": windows,
    }


def _max_drawdown_pct(equity_curve: Sequence[float]) -> float:
    peak = 0.0
    max_drawdown = 0.0
    for raw_value in equity_curve:
        equity = max(float(raw_value), 0.0)
        if equity > peak:
            peak = equity
        if peak <= 0:
            continue
        drawdown = (peak - equity) / peak * 100.0
        if drawdown > max_drawdown:
            max_drawdown = drawdown
    return float(max_drawdown)
