from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ModelAlphaEvaluationContract:
    contract_id: str
    contract_role: str
    selection_policy_mode: str | None = None
    use_learned_selection_recommendations: bool | None = None
    use_learned_exit_mode: bool | None = None
    use_learned_hold_bars: bool | None = None
    use_learned_risk_recommendations: bool | None = None
    use_trade_level_action_policy: bool | None = None
    use_learned_execution_recommendations: bool | None = None
    micro_order_policy: str | None = None
    micro_order_policy_mode: str | None = None
    micro_order_policy_on_missing: str | None = None

    def as_cli_overrides(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "evaluation_contract_id": str(self.contract_id).strip(),
            "evaluation_contract_role": str(self.contract_role).strip(),
        }
        optional_fields = {
            "selection_policy_mode": self.selection_policy_mode,
            "use_learned_selection_recommendations": self.use_learned_selection_recommendations,
            "use_learned_exit_mode": self.use_learned_exit_mode,
            "use_learned_hold_bars": self.use_learned_hold_bars,
            "use_learned_risk_recommendations": self.use_learned_risk_recommendations,
            "use_trade_level_action_policy": self.use_trade_level_action_policy,
            "use_learned_execution_recommendations": self.use_learned_execution_recommendations,
            "micro_order_policy": self.micro_order_policy,
            "micro_order_policy_mode": self.micro_order_policy_mode,
            "micro_order_policy_on_missing": self.micro_order_policy_on_missing,
        }
        for key, value in optional_fields.items():
            if value is not None:
                payload[key] = value
        return payload


_CONTRACTS_BY_ID: dict[str, ModelAlphaEvaluationContract] = {
    "acceptance_frozen_compare_v1": ModelAlphaEvaluationContract(
        contract_id="acceptance_frozen_compare_v1",
        contract_role="frozen_compare",
        selection_policy_mode="raw_threshold",
        use_learned_selection_recommendations=False,
        use_learned_exit_mode=False,
        use_learned_hold_bars=False,
        use_learned_risk_recommendations=False,
        use_trade_level_action_policy=False,
        use_learned_execution_recommendations=False,
        micro_order_policy="off",
    ),
    "runtime_deploy_contract_v1": ModelAlphaEvaluationContract(
        contract_id="runtime_deploy_contract_v1",
        contract_role="deploy_runtime",
        selection_policy_mode="auto",
        use_learned_selection_recommendations=True,
        use_learned_exit_mode=True,
        use_learned_hold_bars=True,
        use_learned_risk_recommendations=True,
        use_trade_level_action_policy=True,
        use_learned_execution_recommendations=True,
        micro_order_policy="on",
        micro_order_policy_mode="trade_only",
        micro_order_policy_on_missing="static_fallback",
    ),
    "paper_offline_contract_v1": ModelAlphaEvaluationContract(
        contract_id="paper_offline_contract_v1",
        contract_role="offline_runtime",
        selection_policy_mode="auto",
        use_learned_selection_recommendations=True,
        use_learned_exit_mode=True,
        use_learned_hold_bars=True,
        use_learned_risk_recommendations=True,
        use_trade_level_action_policy=True,
        use_learned_execution_recommendations=True,
    ),
    "backtest_default_contract_v1": ModelAlphaEvaluationContract(
        contract_id="backtest_default_contract_v1",
        contract_role="config_default",
    ),
    "paper_default_contract_v1": ModelAlphaEvaluationContract(
        contract_id="paper_default_contract_v1",
        contract_role="config_default",
    ),
}


def load_evaluation_contract(*, contract_id: str | None) -> ModelAlphaEvaluationContract | None:
    key = str(contract_id or "").strip()
    if not key:
        return None
    return _CONTRACTS_BY_ID.get(key)


def resolve_backtest_evaluation_contract(*, preset: str) -> ModelAlphaEvaluationContract:
    name = str(preset).strip().lower() or "default"
    if name == "acceptance":
        return _CONTRACTS_BY_ID["acceptance_frozen_compare_v1"]
    if name == "runtime_parity":
        return _CONTRACTS_BY_ID["runtime_deploy_contract_v1"]
    return _CONTRACTS_BY_ID["backtest_default_contract_v1"]


def resolve_paper_evaluation_contract(*, preset: str) -> ModelAlphaEvaluationContract:
    name = str(preset).strip().lower() or "default"
    if name in {
        "live_v3",
        "live_v4",
        "live_v4_native",
        "live_v5",
        "candidate_v4",
        "candidate_v5",
        "default",
    }:
        return _CONTRACTS_BY_ID["runtime_deploy_contract_v1"]
    if name in {"offline", "offline_v3", "offline_v4", "offline_v5"}:
        return _CONTRACTS_BY_ID["paper_offline_contract_v1"]
    return _CONTRACTS_BY_ID["paper_default_contract_v1"]
