"""Execution-aware acceptance helpers for research-lane model comparison."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, TYPE_CHECKING

from autobot.models.live_execution_policy import DEFAULT_EXECUTION_CONTRACT_ARTIFACT_PATH
from autobot.models.registry import load_json
from autobot.strategy.micro_gate_v1 import MicroGateSettings
from autobot.strategy.micro_order_policy import MicroOrderPolicySettings
from autobot.strategy.model_alpha_v1 import ModelAlphaSettings

from .economic_objective import resolve_v4_execution_compare_contract
from .execution_validation import build_execution_validation_summary
from .research_acceptance import compare_execution_balanced_pareto

if TYPE_CHECKING:
    from autobot.backtest.engine import BacktestRunSummary


@dataclass(frozen=True)
class ExecutionAcceptanceOptions:
    registry_root: Path
    model_family: str
    candidate_ref: str
    parquet_root: Path
    dataset_name: str
    output_root_dir: Path
    tf: str
    quote: str
    top_n: int
    start_ts_ms: int
    end_ts_ms: int
    feature_set: str
    dense_grid: bool = False
    starting_krw: float = 50_000.0
    per_trade_krw: float = 10_000.0
    max_positions: int = 2
    min_order_krw: float = 5_000.0
    order_timeout_bars: int = 5
    reprice_max_attempts: int = 1
    reprice_tick_steps: int = 1
    rules_ttl_sec: int = 86_400
    model_alpha_settings: ModelAlphaSettings = ModelAlphaSettings()
    micro_gate: MicroGateSettings = MicroGateSettings()
    micro_order_policy: MicroOrderPolicySettings = MicroOrderPolicySettings()
    execution_contract_artifact_path: str = DEFAULT_EXECUTION_CONTRACT_ARTIFACT_PATH


def run_execution_acceptance(options: ExecutionAcceptanceOptions) -> dict[str, Any]:
    compare_contract = resolve_v4_execution_compare_contract()
    report: dict[str, Any] = {
        "policy": str(compare_contract.get("policy", "")).strip() or "paired_sortino_lpm_execution_v1",
        "enabled": True,
        "candidate_ref": str(options.candidate_ref).strip(),
        "candidate_summary": {},
        "champion_ref": "champion",
        "champion_summary": {},
        "compare_to_champion": compare_execution_balanced_pareto({}, {}),
        "run_settings": _snapshot_run_settings(options),
    }

    try:
        candidate_summary = _run_model_backtest(
            options=options,
            model_ref=str(options.candidate_ref).strip(),
        )
    except Exception as exc:
        report["status"] = "skipped"
        report["skip_reason"] = f"{type(exc).__name__}: {exc}"
        return report

    report["candidate_summary"] = candidate_summary
    report["status"] = "candidate_only"

    champion_doc = load_json(Path(options.registry_root) / options.model_family / "champion.json")
    champion_ref = str(champion_doc.get("run_id", "")).strip()
    if not champion_ref:
        report["skip_reason"] = "NO_EXISTING_CHAMPION"
        return report

    report["champion_ref"] = champion_ref
    try:
        champion_summary = _run_model_backtest(
            options=options,
            model_ref=champion_ref,
        )
    except Exception as exc:
        report["status"] = "candidate_only"
        report["skip_reason"] = f"CHAMPION_BACKTEST_FAILED:{type(exc).__name__}: {exc}"
        return report

    compare_doc = compare_execution_balanced_pareto(candidate_summary, champion_summary)
    report["champion_summary"] = champion_summary
    report["compare_to_champion"] = compare_doc
    report["policy"] = str(compare_doc.get("policy", report["policy"])).strip() or report["policy"]
    report["status"] = "compared"
    return report


def run_model_execution_backtest(
    *,
    options: ExecutionAcceptanceOptions,
    model_ref: str,
    model_alpha_settings: ModelAlphaSettings | None = None,
) -> dict[str, Any]:
    from autobot.backtest.engine import BacktestRunSettings, run_backtest_sync

    effective_model_alpha_settings = replace(
        model_alpha_settings or options.model_alpha_settings,
        model_ref=str(model_ref).strip(),
        model_family=str(options.model_family).strip() or None,
        feature_set=str(options.feature_set).strip().lower() or "v4",
    )
    summary = run_backtest_sync(
        run_settings=BacktestRunSettings(
            dataset_name=str(options.dataset_name).strip(),
            parquet_root=str(options.parquet_root),
            tf=str(options.tf).strip().lower(),
            from_ts_ms=int(options.start_ts_ms),
            to_ts_ms=int(options.end_ts_ms),
            universe_mode="static_start",
            quote=str(options.quote).strip().upper(),
            top_n=max(int(options.top_n), 1),
            dense_grid=bool(options.dense_grid),
            starting_krw=max(float(options.starting_krw), 0.0),
            per_trade_krw=max(float(options.per_trade_krw), 1.0),
            max_positions=max(int(options.max_positions), 1),
            min_order_krw=max(float(options.min_order_krw), 0.0),
            order_timeout_bars=max(int(options.order_timeout_bars), 1),
            reprice_max_attempts=max(int(options.reprice_max_attempts), 0),
            reprice_tick_steps=max(int(options.reprice_tick_steps), 1),
            rules_ttl_sec=max(int(options.rules_ttl_sec), 1),
            strategy="model_alpha_v1",
            model_ref=str(model_ref).strip(),
            model_family=str(options.model_family).strip() or None,
            feature_set=str(options.feature_set).strip().lower() or "v4",
            model_registry_root=str(options.registry_root),
            model_feature_dataset_root=None,
            model_alpha=effective_model_alpha_settings,
            output_root_dir=str(options.output_root_dir),
            micro_gate=options.micro_gate,
            micro_order_policy=options.micro_order_policy,
            execution_contract_artifact_path=str(options.execution_contract_artifact_path),
            artifact_mode="summary_only",
        ),
        upbit_settings=None,
    )
    return _summary_to_doc(summary)


def _run_model_backtest(*, options: ExecutionAcceptanceOptions, model_ref: str) -> dict[str, Any]:
    return run_model_execution_backtest(options=options, model_ref=model_ref)


def _summary_to_doc(summary: BacktestRunSummary) -> dict[str, Any]:
    payload = asdict(summary)
    payload["run_dir"] = str(payload.get("run_dir", ""))
    compare_contract = resolve_v4_execution_compare_contract()
    payload["execution_validation"] = build_execution_validation_summary(
        payload,
        window_minutes=int(compare_contract.get("validation_window_minutes", 60) or 60),
        fold_count=int(compare_contract.get("validation_fold_count", 6) or 6),
        min_active_windows=int(compare_contract.get("validation_min_active_windows", 12) or 12),
        target_return=float(compare_contract.get("validation_target_return", 0.0) or 0.0),
        lpm_order=int(compare_contract.get("validation_lpm_order", 2) or 2),
    )
    return payload


def _snapshot_run_settings(options: ExecutionAcceptanceOptions) -> dict[str, Any]:
    return {
        "dataset_name": str(options.dataset_name).strip(),
        "parquet_root": str(options.parquet_root),
        "output_root_dir": str(options.output_root_dir),
        "tf": str(options.tf).strip().lower(),
        "quote": str(options.quote).strip().upper(),
        "top_n": max(int(options.top_n), 1),
        "start_ts_ms": int(options.start_ts_ms),
        "end_ts_ms": int(options.end_ts_ms),
        "feature_set": str(options.feature_set).strip().lower() or "v4",
        "dense_grid": bool(options.dense_grid),
        "starting_krw": max(float(options.starting_krw), 0.0),
        "per_trade_krw": max(float(options.per_trade_krw), 1.0),
        "max_positions": max(int(options.max_positions), 1),
        "min_order_krw": max(float(options.min_order_krw), 0.0),
        "order_timeout_bars": max(int(options.order_timeout_bars), 1),
        "reprice_max_attempts": max(int(options.reprice_max_attempts), 0),
        "reprice_tick_steps": max(int(options.reprice_tick_steps), 1),
        "rules_ttl_sec": max(int(options.rules_ttl_sec), 1),
        "micro_gate_enabled": bool(options.micro_gate.enabled),
        "micro_order_policy_enabled": bool(options.micro_order_policy.enabled),
        "execution_contract_artifact_path": str(options.execution_contract_artifact_path),
    }
