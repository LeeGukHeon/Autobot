"""Backtest runtime components."""

from .fill_model import CandleFillModel
from .loader import CandleDataLoader
from .types import CandleBar
from .universe import StaticUniverseProvider, build_static_start_universe

__all__ = [
    "BacktestRunEngine",
    "BacktestRunSettings",
    "BacktestRunSummary",
    "CandleBar",
    "CandleDataLoader",
    "CandleFillModel",
    "StaticUniverseProvider",
    "build_static_start_universe",
    "run_backtest_sync",
]


def __getattr__(name: str):  # pragma: no cover - lazy export bridge
    if name in {
        "BacktestRunEngine",
        "BacktestRunSettings",
        "BacktestRunSummary",
        "run_backtest_sync",
    }:
        from .engine import BacktestRunEngine, BacktestRunSettings, BacktestRunSummary, run_backtest_sync

        exports = {
            "BacktestRunEngine": BacktestRunEngine,
            "BacktestRunSettings": BacktestRunSettings,
            "BacktestRunSummary": BacktestRunSummary,
            "run_backtest_sync": run_backtest_sync,
        }
        return exports[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
