"""Live risk planning and management utilities."""

from __future__ import annotations

from .models import RiskManagerConfig, RiskPlan

__all__ = [
    "LiveRiskManager",
    "RiskManagerConfig",
    "RiskPlan",
]


def __getattr__(name: str):
    if name == "LiveRiskManager":
        from .live_risk_manager import LiveRiskManager

        return LiveRiskManager
    raise AttributeError(name)
