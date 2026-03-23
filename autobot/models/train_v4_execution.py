"""Execution acceptance and runtime recommendation helpers for trainer=v4_crypto_cs."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import shutil
from typing import Any, Callable

from autobot.features.feature_spec import parse_date_to_ts_ms
from autobot.strategy.model_alpha_v1 import ModelAlphaExecutionSettings, ModelAlphaExitSettings

from .exit_path_risk import build_exit_path_risk_summary
from .execution_acceptance import ExecutionAcceptanceOptions
from .execution_risk_control import build_execution_risk_control_from_oos_rows


def build_duplicate_candidate_execution_acceptance(
    *,
    run_id: str,
    duplicate_artifacts: dict[str, Any],
) -> dict[str, Any]:
    return {
        "candidate_ref": run_id,
        "champion_ref": str(duplicate_artifacts.get("champion_ref", "")),
        "status": "skipped",
        "policy": "duplicate_candidate_short_circuit",
        "reason": "DUPLICATE_CANDIDATE",
        "duplicate_candidate": True,
        "duplicate_artifacts": duplicate_artifacts,
    }


def build_duplicate_candidate_runtime_recommendations(
    *,
    run_id: str,
    duplicate_artifacts: dict[str, Any],
    utc_now_fn: Callable[[], str],
) -> dict[str, Any]:
    return {
        "candidate_ref": run_id,
        "created_at_utc": utc_now_fn(),
        "status": "skipped",
        "reason": "DUPLICATE_CANDIDATE",
        "duplicate_candidate": True,
        "duplicate_artifacts": duplicate_artifacts,
    }


def build_trade_action_policy_v4(
    *,
    options: Any,
    runtime_recommendations: dict[str, Any],
    selection_calibration: dict[str, Any],
    oos_rows: list[dict[str, Any]] | None,
    build_trade_action_policy_from_oos_rows_fn: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    exit_doc = dict((runtime_recommendations or {}).get("exit") or {})
    hold_grid_point = dict(exit_doc.get("grid_point") or {})
    risk_grid_point = dict(exit_doc.get("risk_grid_point") or {})
    if not hold_grid_point or not risk_grid_point:
        return {
            "version": 1,
            "policy": "trade_level_hold_risk_oos_bins_v1",
            "status": "skipped",
            "reason": "MISSING_EXIT_GRID_POINTS",
        }

    base_exit = options.execution_acceptance_model_alpha.exit
    base_execution = options.execution_acceptance_model_alpha.execution
    hold_bars = max(
        int(hold_grid_point.get("hold_bars", exit_doc.get("recommended_hold_bars", base_exit.hold_bars)) or base_exit.hold_bars),
        1,
    )
    risk_hold_bars = max(int(risk_grid_point.get("hold_bars", hold_bars) or hold_bars), 1)
    expected_exit_fee_rate = _resolve_trade_action_expected_exit_fee_rate(base_exit)
    expected_exit_slippage_bps = _resolve_trade_action_expected_exit_slippage_bps(
        exit_settings=base_exit,
        execution_settings=base_execution,
    )
    hold_policy_template = {
        "mode": "hold",
        "hold_bars": int(hold_bars),
        "risk_scaling_mode": str(base_exit.risk_scaling_mode),
        "risk_vol_feature": str(base_exit.risk_vol_feature),
        "tp_vol_multiplier": base_exit.tp_vol_multiplier,
        "sl_vol_multiplier": base_exit.sl_vol_multiplier,
        "trailing_vol_multiplier": base_exit.trailing_vol_multiplier,
        "tp_pct": float(base_exit.tp_pct),
        "sl_pct": float(base_exit.sl_pct),
        "trailing_pct": float(base_exit.trailing_pct),
        "expected_exit_fee_rate": float(expected_exit_fee_rate),
        "expected_exit_slippage_bps": float(expected_exit_slippage_bps),
    }
    risk_policy_template = {
        "mode": "risk",
        "hold_bars": int(risk_hold_bars),
        "risk_scaling_mode": str(
            risk_grid_point.get("risk_scaling_mode", exit_doc.get("recommended_risk_scaling_mode", base_exit.risk_scaling_mode))
        ),
        "risk_vol_feature": str(
            risk_grid_point.get("risk_vol_feature", exit_doc.get("recommended_risk_vol_feature", base_exit.risk_vol_feature))
        ),
        "tp_vol_multiplier": risk_grid_point.get(
            "tp_vol_multiplier",
            exit_doc.get("recommended_tp_vol_multiplier", base_exit.tp_vol_multiplier),
        ),
        "sl_vol_multiplier": risk_grid_point.get(
            "sl_vol_multiplier",
            exit_doc.get("recommended_sl_vol_multiplier", base_exit.sl_vol_multiplier),
        ),
        "trailing_vol_multiplier": risk_grid_point.get(
            "trailing_vol_multiplier",
            exit_doc.get("recommended_trailing_vol_multiplier", base_exit.trailing_vol_multiplier),
        ),
        "tp_pct": float(base_exit.tp_pct),
        "sl_pct": float(base_exit.sl_pct),
        "trailing_pct": float(base_exit.trailing_pct),
        "expected_exit_fee_rate": float(expected_exit_fee_rate),
        "expected_exit_slippage_bps": float(expected_exit_slippage_bps),
    }
    return build_trade_action_policy_from_oos_rows_fn(
        oos_rows=list(oos_rows or []),
        selection_calibration=selection_calibration,
        hold_policy_template=hold_policy_template,
        risk_policy_template=risk_policy_template,
        size_multiplier_min=float(options.execution_acceptance_model_alpha.position.size_multiplier_min),
        size_multiplier_max=float(options.execution_acceptance_model_alpha.position.size_multiplier_max),
    )


def build_execution_risk_control_v4(
    *,
    options: Any,
    runtime_recommendations: dict[str, Any],
    selection_calibration: dict[str, Any],
    oos_rows: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    exit_doc = dict((runtime_recommendations or {}).get("exit") or {})
    trade_action_doc = dict((runtime_recommendations or {}).get("trade_action") or {})
    base_exit = options.execution_acceptance_model_alpha.exit
    base_execution = options.execution_acceptance_model_alpha.execution
    hold_bars = max(
        int(((exit_doc.get("grid_point") or {}).get("hold_bars", exit_doc.get("recommended_hold_bars", base_exit.hold_bars)) or base_exit.hold_bars)),
        1,
    )
    risk_hold_bars = max(
        int(
            ((exit_doc.get("risk_grid_point") or {}).get("hold_bars", exit_doc.get("recommended_hold_bars", hold_bars)) or hold_bars)
        ),
        1,
    )
    expected_exit_fee_rate = _resolve_trade_action_expected_exit_fee_rate(base_exit)
    expected_exit_slippage_bps = _resolve_trade_action_expected_exit_slippage_bps(
        exit_settings=base_exit,
        execution_settings=base_execution,
    )
    hold_policy_template = {
        "mode": "hold",
        "hold_bars": int(hold_bars),
        "risk_scaling_mode": str(base_exit.risk_scaling_mode),
        "risk_vol_feature": str(base_exit.risk_vol_feature),
        "tp_vol_multiplier": base_exit.tp_vol_multiplier,
        "sl_vol_multiplier": base_exit.sl_vol_multiplier,
        "trailing_vol_multiplier": base_exit.trailing_vol_multiplier,
        "tp_pct": float(base_exit.tp_pct),
        "sl_pct": float(base_exit.sl_pct),
        "trailing_pct": float(base_exit.trailing_pct),
        "expected_exit_fee_rate": float(expected_exit_fee_rate),
        "expected_exit_slippage_bps": float(expected_exit_slippage_bps),
    }
    risk_policy_template = {
        "mode": "risk",
        "hold_bars": int(risk_hold_bars),
        "risk_scaling_mode": str(
            ((exit_doc.get("risk_grid_point") or {}).get("risk_scaling_mode", exit_doc.get("recommended_risk_scaling_mode", base_exit.risk_scaling_mode)))
        ),
        "risk_vol_feature": str(
            ((exit_doc.get("risk_grid_point") or {}).get("risk_vol_feature", exit_doc.get("recommended_risk_vol_feature", base_exit.risk_vol_feature)))
        ),
        "tp_vol_multiplier": (exit_doc.get("risk_grid_point") or {}).get(
            "tp_vol_multiplier",
            exit_doc.get("recommended_tp_vol_multiplier"),
        ),
        "sl_vol_multiplier": (exit_doc.get("risk_grid_point") or {}).get(
            "sl_vol_multiplier",
            exit_doc.get("recommended_sl_vol_multiplier"),
        ),
        "trailing_vol_multiplier": (exit_doc.get("risk_grid_point") or {}).get(
            "trailing_vol_multiplier",
            exit_doc.get("recommended_trailing_vol_multiplier"),
        ),
        "tp_pct": float(base_exit.tp_pct),
        "sl_pct": float(base_exit.sl_pct),
        "trailing_pct": float(base_exit.trailing_pct),
        "expected_exit_fee_rate": float(expected_exit_fee_rate),
        "expected_exit_slippage_bps": float(expected_exit_slippage_bps),
    }
    return build_execution_risk_control_from_oos_rows(
        oos_rows=list(oos_rows or []),
        selection_calibration=selection_calibration,
        trade_action_policy=trade_action_doc,
        hold_policy_template=hold_policy_template,
        risk_policy_template=risk_policy_template,
        weighting_covariate_similarity_enabled=True,
        weighting_density_ratio_enabled=True,
    )


def build_exit_path_risk_summary_v4(
    *,
    runtime_recommendations: dict[str, Any],
    selection_calibration: dict[str, Any],
    oos_rows: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    exit_doc = dict((runtime_recommendations or {}).get("exit") or {})
    risk_feature_name = str(exit_doc.get("recommended_risk_vol_feature", "rv_12") or "rv_12").strip() or "rv_12"
    recommended_hold_bars = int(exit_doc.get("recommended_hold_bars", 0) or 0) or None
    horizons = sorted(
        {
            max(int(item), 1)
            for item in [1, 3, 6, 9, 12, int(exit_doc.get("recommended_hold_bars", 0) or 0)]
            if int(item) > 0
        }
    )
    return build_exit_path_risk_summary(
        oos_rows=list(oos_rows or []),
        selection_calibration=selection_calibration,
        risk_feature_name=risk_feature_name,
        horizons=tuple(horizons),
        expected_exit_fee_rate=max(float(exit_doc.get("expected_exit_fee_rate", 0.0) or 0.0), 0.0),
        expected_exit_slippage_bps=max(float(exit_doc.get("expected_exit_slippage_bps", 0.0) or 0.0), 0.0),
        recommended_hold_bars=recommended_hold_bars,
    )


def purge_execution_artifact_run_dirs(
    *,
    output_root: Path,
    execution_acceptance: dict[str, Any] | None,
    runtime_recommendations: dict[str, Any] | None,
) -> dict[str, Any]:
    allowed_root = (Path(output_root) / "runs").resolve()
    discovered = sorted(
        {
            run_dir
            for run_dir in _collect_reported_run_dirs(execution_acceptance)
            + _collect_reported_run_dirs(runtime_recommendations)
            if run_dir
        }
    )
    payload: dict[str, Any] = {
        "evaluated": True,
        "allowed_root": str(allowed_root),
        "discovered_run_dirs": discovered,
        "removed_paths": [],
        "missing_paths": [],
        "skipped_outside_root": [],
    }
    for raw_path in discovered:
        candidate = Path(raw_path)
        try:
            resolved = candidate.resolve()
        except OSError:
            payload["missing_paths"].append(str(candidate))
            continue
        try:
            resolved.relative_to(allowed_root)
        except ValueError:
            payload["skipped_outside_root"].append(str(resolved))
            continue
        if not resolved.exists() or not resolved.is_dir():
            payload["missing_paths"].append(str(resolved))
            continue
        shutil.rmtree(resolved, ignore_errors=False)
        payload["removed_paths"].append(str(resolved))
    payload["removed_count"] = len(payload["removed_paths"])
    payload["missing_count"] = len(payload["missing_paths"])
    payload["skipped_count"] = len(payload["skipped_outside_root"])
    return payload


def run_execution_acceptance_v4(
    *,
    options: Any,
    run_id: str,
    resolve_v4_execution_compare_contract_fn: Callable[[], dict[str, Any]],
    run_execution_acceptance_fn: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    execution_compare_contract = resolve_v4_execution_compare_contract_fn()
    if not bool(options.execution_acceptance_enabled):
        return {
            "policy": str(execution_compare_contract.get("policy", "")).strip() or "paired_sortino_lpm_execution_v1",
            "enabled": False,
            "status": "skipped",
            "skip_reason": "DISABLED",
            "compare_to_champion": {},
        }
    try:
        selection = replace(
            options.execution_acceptance_model_alpha.selection,
            use_learned_recommendations=False,
        )
        execution_model_alpha = replace(
            options.execution_acceptance_model_alpha,
            selection=selection,
        )
        return run_execution_acceptance_fn(
            ExecutionAcceptanceOptions(
                registry_root=options.registry_root,
                model_family=options.model_family,
                candidate_ref=run_id,
                parquet_root=options.execution_acceptance_parquet_root,
                dataset_name=str(options.execution_acceptance_dataset_name).strip() or "candles_v1",
                output_root_dir=options.execution_acceptance_output_root,
                tf=str(options.tf).strip().lower(),
                quote=str(options.quote).strip().upper(),
                top_n=max(
                    int(options.execution_acceptance_top_n)
                    if int(options.execution_acceptance_top_n) > 0
                    else int(options.top_n),
                    1,
                ),
                start_ts_ms=parse_date_to_ts_ms(options.start),
                end_ts_ms=parse_date_to_ts_ms(options.end, end_of_day=True),
                feature_set=str(options.feature_set).strip().lower() or "v4",
                dense_grid=bool(options.execution_acceptance_dense_grid),
                starting_krw=max(float(options.execution_acceptance_starting_krw), 0.0),
                per_trade_krw=max(float(options.execution_acceptance_per_trade_krw), 1.0),
                max_positions=max(int(options.execution_acceptance_max_positions), 1),
                min_order_krw=max(float(options.execution_acceptance_min_order_krw), 0.0),
                order_timeout_bars=max(int(options.execution_acceptance_order_timeout_bars), 1),
                reprice_max_attempts=max(int(options.execution_acceptance_reprice_max_attempts), 0),
                reprice_tick_steps=max(int(options.execution_acceptance_reprice_tick_steps), 1),
                rules_ttl_sec=max(int(options.execution_acceptance_rules_ttl_sec), 1),
                model_alpha_settings=execution_model_alpha,
            )
        )
    except Exception as exc:
        return {
            "policy": str(execution_compare_contract.get("policy", "")).strip() or "paired_sortino_lpm_execution_v1",
            "enabled": True,
            "status": "skipped",
            "skip_reason": f"{type(exc).__name__}: {exc}",
            "compare_to_champion": {},
        }


def build_runtime_recommendations_v4(
    *,
    options: Any,
    run_id: str,
    search_budget_decision: dict[str, Any],
    optimize_runtime_recommendations_fn: Callable[..., dict[str, Any]],
    runtime_recommendation_grid_for_profile_fn: Callable[[str], Any],
) -> dict[str, Any]:
    if not bool(options.execution_acceptance_enabled):
        return {
            "version": 1,
            "status": "skipped",
            "reason": "EXECUTION_ACCEPTANCE_DISABLED",
        }
    try:
        selection = replace(
            options.execution_acceptance_model_alpha.selection,
            use_learned_recommendations=True,
        )
        exit_settings = replace(
            options.execution_acceptance_model_alpha.exit,
            use_learned_exit_mode=False,
            use_learned_hold_bars=False,
        )
        execution_settings = replace(
            options.execution_acceptance_model_alpha.execution,
            use_learned_recommendations=False,
        )
        runtime_model_alpha = replace(
            options.execution_acceptance_model_alpha,
            selection=selection,
            exit=exit_settings,
            execution=execution_settings,
        )
        return optimize_runtime_recommendations_fn(
            options=ExecutionAcceptanceOptions(
                registry_root=options.registry_root,
                model_family=options.model_family,
                candidate_ref=run_id,
                parquet_root=options.execution_acceptance_parquet_root,
                dataset_name=str(options.execution_acceptance_dataset_name).strip() or "candles_v1",
                output_root_dir=options.execution_acceptance_output_root,
                tf=str(options.tf).strip().lower(),
                quote=str(options.quote).strip().upper(),
                top_n=max(
                    int(options.execution_acceptance_top_n)
                    if int(options.execution_acceptance_top_n) > 0
                    else int(options.top_n),
                    1,
                ),
                start_ts_ms=parse_date_to_ts_ms(options.start),
                end_ts_ms=parse_date_to_ts_ms(options.end, end_of_day=True),
                feature_set=str(options.feature_set).strip().lower() or "v4",
                dense_grid=bool(options.execution_acceptance_dense_grid),
                starting_krw=max(float(options.execution_acceptance_starting_krw), 0.0),
                per_trade_krw=max(float(options.execution_acceptance_per_trade_krw), 1.0),
                max_positions=max(int(options.execution_acceptance_max_positions), 1),
                min_order_krw=max(float(options.execution_acceptance_min_order_krw), 0.0),
                order_timeout_bars=max(int(options.execution_acceptance_order_timeout_bars), 1),
                reprice_max_attempts=max(int(options.execution_acceptance_reprice_max_attempts), 0),
                reprice_tick_steps=max(int(options.execution_acceptance_reprice_tick_steps), 1),
                rules_ttl_sec=max(int(options.execution_acceptance_rules_ttl_sec), 1),
                model_alpha_settings=runtime_model_alpha,
            ),
            candidate_ref=run_id,
            grid=runtime_recommendation_grid_for_profile_fn(
                str((search_budget_decision.get("applied") or {}).get("runtime_recommendation_profile", "full"))
            ),
        )
    except Exception as exc:
        return {
            "version": 1,
            "status": "skipped",
            "reason": f"{type(exc).__name__}: {exc}",
        }


def _resolve_trade_action_expected_exit_fee_rate(exit_settings: ModelAlphaExitSettings) -> float:
    if exit_settings.expected_exit_fee_bps is not None:
        return max(float(exit_settings.expected_exit_fee_bps), 0.0) / 10_000.0
    return 0.0


def _resolve_trade_action_expected_exit_slippage_bps(
    *,
    exit_settings: ModelAlphaExitSettings,
    execution_settings: ModelAlphaExecutionSettings,
) -> float:
    if exit_settings.expected_exit_slippage_bps is not None:
        return max(float(exit_settings.expected_exit_slippage_bps), 0.0)
    price_mode = str(execution_settings.price_mode).strip().upper()
    if price_mode == "PASSIVE_MAKER":
        return 0.0
    if price_mode == "CROSS_1T":
        return 6.0
    return 2.5


def _collect_reported_run_dirs(payload: dict[str, Any] | None) -> list[str]:
    found: list[str] = []

    def _visit(value: Any) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if key == "run_dir":
                    text = str(child).strip()
                    if text:
                        found.append(text)
                _visit(child)
            return
        if isinstance(value, list):
            for child in value:
                _visit(child)

    _visit(payload if isinstance(payload, dict) else {})
    return found
