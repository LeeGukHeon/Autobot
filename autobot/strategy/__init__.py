"""Strategy support components exposed via lazy package exports."""

from __future__ import annotations

from importlib import import_module
from typing import Any


_EXPORTS: dict[str, tuple[str, str]] = {
    "Candidate": (".candidates_v1", "Candidate"),
    "CandidateGeneratorV1": (".candidates_v1", "CandidateGeneratorV1"),
    "CandidateSettings": (".candidates_v1", "CandidateSettings"),
    "MicroGateBookSettings": (".micro_gate_v1", "MicroGateBookSettings"),
    "MicroGateDecision": (".micro_gate_v1", "MicroGateDecision"),
    "MicroGateSettings": (".micro_gate_v1", "MicroGateSettings"),
    "MicroGateTradeSettings": (".micro_gate_v1", "MicroGateTradeSettings"),
    "MicroGateV1": (".micro_gate_v1", "MicroGateV1"),
    "MicroOrderPolicyDecision": (".micro_order_policy", "MicroOrderPolicyDecision"),
    "MicroOrderPolicySafetySettings": (".micro_order_policy", "MicroOrderPolicySafetySettings"),
    "MicroOrderPolicySettings": (".micro_order_policy", "MicroOrderPolicySettings"),
    "MicroOrderPolicyTieringSettings": (".micro_order_policy", "MicroOrderPolicyTieringSettings"),
    "MicroOrderPolicyTiersSettings": (".micro_order_policy", "MicroOrderPolicyTiersSettings"),
    "MicroOrderPolicyTierSettings": (".micro_order_policy", "MicroOrderPolicyTierSettings"),
    "MicroOrderPolicyV1": (".micro_order_policy", "MicroOrderPolicyV1"),
    "ModelAlphaExecutionSettings": (".model_alpha_v1", "ModelAlphaExecutionSettings"),
    "ModelAlphaExitSettings": (".model_alpha_v1", "ModelAlphaExitSettings"),
    "ModelAlphaPositionSettings": (".model_alpha_v1", "ModelAlphaPositionSettings"),
    "ModelAlphaSelectionSettings": (".model_alpha_v1", "ModelAlphaSelectionSettings"),
    "ModelAlphaSettings": (".model_alpha_v1", "ModelAlphaSettings"),
    "ModelAlphaStrategyV1": (".model_alpha_v1", "ModelAlphaStrategyV1"),
    "LiveWsMicroSnapshotProvider": (".micro_snapshot", "LiveWsMicroSnapshotProvider"),
    "LiveWsProviderSettings": (".micro_snapshot", "LiveWsProviderSettings"),
    "LiveWsRateLimitGuard": (".micro_snapshot", "LiveWsRateLimitGuard"),
    "MicroSnapshot": (".micro_snapshot", "MicroSnapshot"),
    "OfflineMicroSnapshotProvider": (".micro_snapshot", "OfflineMicroSnapshotProvider"),
    "MarketTopItem": (".top20_scanner", "MarketTopItem"),
    "TopTradeValueScanner": (".top20_scanner", "TopTradeValueScanner"),
    "GateDecision": (".trade_gate_v1", "GateDecision"),
    "GateSettings": (".trade_gate_v1", "GateSettings"),
    "TradeGateV1": (".trade_gate_v1", "TradeGateV1"),
}

__all__ = sorted(_EXPORTS.keys())


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = target
    module = import_module(module_name, __name__)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | set(__all__))
