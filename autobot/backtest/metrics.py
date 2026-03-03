"""Backtest metric helpers."""

from __future__ import annotations

from typing import Sequence


def max_drawdown_pct(equity_curve: Sequence[float]) -> float:
    max_dd = 0.0
    peak = 0.0
    for equity in equity_curve:
        equity_value = float(equity)
        if equity_value > peak:
            peak = equity_value
        if peak <= 0:
            continue
        drawdown = (peak - equity_value) / peak * 100.0
        if drawdown > max_dd:
            max_dd = drawdown
    return max_dd


def win_rate(realized_trade_pnls: Sequence[float]) -> float:
    if not realized_trade_pnls:
        return 0.0
    wins = sum(1 for pnl in realized_trade_pnls if float(pnl) > 0.0)
    return wins / len(realized_trade_pnls)
