"""Execution-aware runtime recommendation optimizer for model_alpha_v1."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from autobot.common.execution_structure import summarize_trades_csv
from autobot.strategy.model_alpha_v1 import (
    ModelAlphaExecutionSettings,
    ModelAlphaExitSettings,
    ModelAlphaSettings,
    ModelAlphaSelectionSettings,
)

from .economic_objective import resolve_v4_execution_compare_contract
from .execution_acceptance import ExecutionAcceptanceOptions, run_model_execution_backtest
from .execution_validation import build_execution_validation_summary
from .research_acceptance import compare_execution_balanced_pareto
from .runtime_recommendation_contract import (
    normalize_runtime_recommendations_payload,
    resolve_exit_mode_recommendation,
)

_EXECUTION_STAGE_ORDER: tuple[str, ...] = ("PASSIVE_MAKER", "JOIN", "CROSS_1T")


@dataclass(frozen=True)
class RuntimeRecommendationGrid:
    hold_bars_grid: tuple[int, ...] = (3, 6, 9, 12)
    risk_vol_feature_grid: tuple[str, ...] = ("rv_12", "rv_36", "atr_pct_14")
    tp_vol_multiplier_grid: tuple[float, ...] = (1.5, 2.0, 2.5, 3.0)
    sl_vol_multiplier_grid: tuple[float, ...] = (1.0, 1.5, 2.0)
    trailing_vol_multiplier_grid: tuple[float, ...] = (0.0, 0.75, 1.0, 1.25)
    price_mode_grid: tuple[str, ...] = ("PASSIVE_MAKER", "JOIN", "CROSS_1T")
    timeout_bars_grid: tuple[int, ...] = (1, 2, 4)
    replace_max_grid: tuple[int, ...] = (0, 1, 2)


def runtime_recommendation_grid_for_profile(profile: str | None) -> RuntimeRecommendationGrid:
    normalized = str(profile or "").strip().lower() or "full"
    if normalized == "tiny":
        return RuntimeRecommendationGrid(
            hold_bars_grid=(3, 6),
            risk_vol_feature_grid=("rv_12", "atr_pct_14"),
            tp_vol_multiplier_grid=(1.5, 2.0),
            sl_vol_multiplier_grid=(1.0, 1.5),
            trailing_vol_multiplier_grid=(0.0, 1.0),
            price_mode_grid=("PASSIVE_MAKER", "JOIN"),
            timeout_bars_grid=(1, 2),
            replace_max_grid=(0, 1),
        )
    if normalized == "compact":
        return RuntimeRecommendationGrid(
            hold_bars_grid=(3, 6, 9),
            risk_vol_feature_grid=("rv_12", "rv_36"),
            tp_vol_multiplier_grid=(1.5, 2.5),
            sl_vol_multiplier_grid=(1.0, 1.5),
            trailing_vol_multiplier_grid=(0.0, 0.75),
            price_mode_grid=("PASSIVE_MAKER", "JOIN"),
            timeout_bars_grid=(1, 2),
            replace_max_grid=(0, 1),
        )
    return RuntimeRecommendationGrid()


def optimize_runtime_recommendations(
    *,
    options: ExecutionAcceptanceOptions,
    candidate_ref: str,
    grid: RuntimeRecommendationGrid | None = None,
) -> dict[str, Any]:
    active_grid = grid or RuntimeRecommendationGrid()
    execution_compare_contract = resolve_v4_execution_compare_contract()
    candidate_id = str(candidate_ref).strip()
    if not candidate_id:
        return {
            "version": 1,
            "status": "skipped",
            "reason": "EMPTY_CANDIDATE_REF",
        }

    base_selection = replace(
        options.model_alpha_settings.selection,
        use_learned_recommendations=True,
    )
    base_settings = replace(
        options.model_alpha_settings,
        model_ref=candidate_id,
        model_family=str(options.model_family).strip() or None,
        feature_set=str(options.feature_set).strip().lower() or "v4",
        selection=base_selection,
    )

    hold_rows: list[dict[str, Any]] = []
    for hold_bars in _dedupe_positive_ints(active_grid.hold_bars_grid):
        grid_point = {"hold_bars": int(hold_bars)}
        candidate_settings = replace(
            base_settings,
            exit=replace(
                base_settings.exit,
                mode="hold",
                hold_bars=int(hold_bars),
                use_learned_hold_bars=False,
            ),
            execution=replace(
                base_settings.execution,
                use_learned_recommendations=False,
            ),
        )
        summary = run_model_execution_backtest(
            options=options,
            model_ref=candidate_id,
            model_alpha_settings=candidate_settings,
        )
        hold_rows.append(
            {
                "kind": "hold",
                "rule_id": _build_exit_rule_id(kind="hold", grid_point=grid_point),
                "grid_point": grid_point,
                "summary": summary,
            }
        )

    ranked_holds = _rank_execution_rows(hold_rows)
    hold_family = _build_exit_family_doc(family="hold", rows=ranked_holds)
    best_hold = _resolve_family_representative_row(hold_family)

    risk_exit_rows: list[dict[str, Any]] = []
    for hold_bars in _dedupe_positive_ints(active_grid.hold_bars_grid):
        for vol_feature in _dedupe_texts(active_grid.risk_vol_feature_grid):
            for tp_mult in _dedupe_positive_floats(active_grid.tp_vol_multiplier_grid):
                for sl_mult in _dedupe_positive_floats(active_grid.sl_vol_multiplier_grid):
                    for trailing_mult in _dedupe_nonnegative_floats(active_grid.trailing_vol_multiplier_grid):
                        grid_point = {
                            "hold_bars": int(hold_bars),
                            "risk_scaling_mode": "volatility_scaled",
                            "risk_vol_feature": str(vol_feature),
                            "tp_vol_multiplier": float(tp_mult),
                            "sl_vol_multiplier": float(sl_mult),
                            "trailing_vol_multiplier": float(trailing_mult),
                        }
                        candidate_settings = replace(
                            base_settings,
                            exit=ModelAlphaExitSettings(
                                mode="risk",
                                hold_bars=int(hold_bars),
                                use_learned_exit_mode=False,
                                use_learned_hold_bars=False,
                                use_learned_risk_recommendations=False,
                                risk_scaling_mode="volatility_scaled",
                                risk_vol_feature=str(vol_feature),
                                tp_vol_multiplier=float(tp_mult),
                                sl_vol_multiplier=float(sl_mult),
                                trailing_vol_multiplier=float(trailing_mult),
                                tp_pct=float(base_settings.exit.tp_pct),
                                sl_pct=float(base_settings.exit.sl_pct),
                                trailing_pct=float(base_settings.exit.trailing_pct),
                                expected_exit_slippage_bps=base_settings.exit.expected_exit_slippage_bps,
                                expected_exit_fee_bps=base_settings.exit.expected_exit_fee_bps,
                            ),
                            execution=replace(
                                base_settings.execution,
                                use_learned_recommendations=False,
                            ),
                        )
                        summary = run_model_execution_backtest(
                            options=options,
                            model_ref=candidate_id,
                            model_alpha_settings=candidate_settings,
                        )
                        risk_exit_rows.append(
                            {
                                "kind": "risk_exit",
                                "rule_id": _build_exit_rule_id(kind="risk_exit", grid_point=grid_point),
                                "grid_point": grid_point,
                                "summary": summary,
                            }
                        )

    ranked_risk_exit = _rank_execution_rows(risk_exit_rows)
    risk_family = _build_exit_family_doc(family="risk", rows=ranked_risk_exit)
    best_risk_exit = _resolve_family_representative_row(risk_family)
    exit_family_compare = _build_exit_family_compare_doc(
        hold_family=hold_family,
        risk_family=risk_family,
    )
    exit_recommendation = _resolve_family_aware_exit_recommendation(
        family_compare=exit_family_compare,
        best_hold_row=best_hold,
        best_risk_row=best_risk_exit,
    )
    execution_exit_settings = _resolve_execution_backtest_exit_settings(
        base_exit=base_settings.exit,
        best_hold_row=best_hold,
        best_risk_row=best_risk_exit,
        exit_recommendation=exit_recommendation,
    )

    execution_rows: list[dict[str, Any]] = []
    for price_mode in _dedupe_price_modes(active_grid.price_mode_grid):
        for timeout_bars in _dedupe_positive_ints(active_grid.timeout_bars_grid):
            for replace_max in _dedupe_nonnegative_ints(active_grid.replace_max_grid):
                candidate_settings = replace(
                    base_settings,
                    exit=execution_exit_settings,
                    execution=ModelAlphaExecutionSettings(
                        price_mode=str(price_mode),
                        timeout_bars=int(timeout_bars),
                        replace_max=int(replace_max),
                        use_learned_recommendations=False,
                    ),
                )
                summary = run_model_execution_backtest(
                    options=options,
                    model_ref=candidate_id,
                    model_alpha_settings=candidate_settings,
                )
                execution_rows.append(
                    {
                        "kind": "execution",
                        "rule_id": (
                            f"execution_{str(price_mode).strip().upper().lower()}"
                            f"_t{int(timeout_bars)}_r{int(replace_max)}"
                        ),
                        "grid_point": {
                            "price_mode": str(price_mode),
                            "timeout_bars": int(timeout_bars),
                            "replace_max": int(replace_max),
                        },
                        "summary": summary,
                    }
                )

    ranked_execution = _rank_execution_rows(execution_rows)
    best_execution = ranked_execution[0] if ranked_execution else None

    selected_threshold_key = _safe_text(
        ((base_settings.selection.registry_threshold_key if not base_settings.selection.use_learned_recommendations else "") or ""),
        "",
    )
    return normalize_runtime_recommendations_payload(
        {
        "version": 1,
        "status": "ready" if best_hold is not None and best_execution is not None else "fallback",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "recommendation_source": "execution_backtest_grid_search_v1",
        "objective": str(execution_compare_contract.get("policy", "")).strip() or "paired_sortino_lpm_execution_v1",
        "candidate_ref": candidate_id,
        "selection_context": {
            "use_learned_recommendations": bool(base_settings.selection.use_learned_recommendations),
            "registry_threshold_key_fallback": str(base_settings.selection.registry_threshold_key),
            "selected_threshold_key": selected_threshold_key,
        },
        "exit": _build_exit_doc(
            hold_family=hold_family,
            risk_family=risk_family,
            family_compare=exit_family_compare,
            fallback_hold_bars=int(base_settings.exit.hold_bars),
            fallback_exit=base_settings.exit,
            resolved_exit_recommendation=exit_recommendation,
        ),
        "execution": _build_execution_doc(
            best_execution,
            ranked_rows=ranked_execution,
            fallback_settings=base_settings.execution,
        ),
        "hold_grid_results": ranked_holds,
        "risk_exit_grid_results": ranked_risk_exit,
        "execution_grid_results": ranked_execution,
        }
    )


def _build_exit_doc(
    *,
    hold_family: dict[str, Any],
    risk_family: dict[str, Any],
    family_compare: dict[str, Any],
    fallback_hold_bars: int,
    fallback_exit: ModelAlphaExitSettings,
    resolved_exit_recommendation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    best_row = _resolve_family_representative_row(hold_family)
    best_risk_row = _resolve_family_representative_row(risk_family)
    payload: dict[str, Any] = {
        "mode": "hold",
        "recommended_hold_bars": int(fallback_hold_bars),
        "recommendation_source": "manual_fallback",
    }
    if isinstance(best_row, dict):
        payload = {
            "recommended_hold_bars": int(best_row["grid_point"]["hold_bars"]),
            "recommendation_source": "execution_backtest_grid_search",
            "objective_score": float(best_row.get("utility_total", 0.0)),
            "wins": int(best_row.get("wins", 0)),
            "losses": int(best_row.get("losses", 0)),
            "comparable_pairs": int(best_row.get("comparable_pairs", 0)),
            "summary": dict(best_row.get("summary", {})),
            "grid_point": dict(best_row.get("grid_point", {})),
        }
    if isinstance(best_risk_row, dict):
        risk_grid_point = dict(best_risk_row.get("grid_point", {}))
        payload.update(
            {
                "recommended_risk_scaling_mode": str(risk_grid_point.get("risk_scaling_mode", "volatility_scaled")),
                "recommended_risk_vol_feature": str(
                    risk_grid_point.get("risk_vol_feature", fallback_exit.risk_vol_feature)
                ),
                "recommended_tp_vol_multiplier": float(
                    risk_grid_point.get("tp_vol_multiplier", fallback_exit.tp_vol_multiplier or 0.0)
                ),
                "recommended_sl_vol_multiplier": float(
                    risk_grid_point.get("sl_vol_multiplier", fallback_exit.sl_vol_multiplier or 0.0)
                ),
                "recommended_trailing_vol_multiplier": float(
                    risk_grid_point.get("trailing_vol_multiplier", fallback_exit.trailing_vol_multiplier or 0.0)
                ),
                "risk_recommendation_source": "execution_backtest_grid_search",
                "risk_objective_score": float(best_risk_row.get("utility_total", 0.0)),
                "risk_summary": dict(best_risk_row.get("summary", {})),
                "risk_grid_point": risk_grid_point,
            }
        )
    else:
        payload.update(
            {
                "recommended_risk_scaling_mode": str(fallback_exit.risk_scaling_mode),
                "recommended_risk_vol_feature": str(fallback_exit.risk_vol_feature),
                "recommended_tp_vol_multiplier": _safe_optional_float(fallback_exit.tp_vol_multiplier),
                "recommended_sl_vol_multiplier": _safe_optional_float(fallback_exit.sl_vol_multiplier),
                "recommended_trailing_vol_multiplier": _safe_optional_float(fallback_exit.trailing_vol_multiplier),
                "risk_recommendation_source": "manual_fallback",
            }
        )
    exit_recommendation = dict(resolved_exit_recommendation or resolve_exit_mode_recommendation(best_row, best_risk_row))
    payload.update(exit_recommendation)
    payload["hold_family"] = dict(hold_family)
    payload["risk_family"] = dict(risk_family)
    payload["family_compare"] = dict(family_compare)
    payload["hold_family_status"] = str(hold_family.get("status", "")).strip()
    payload["risk_family_status"] = str(risk_family.get("status", "")).strip()
    payload["family_compare_status"] = str(family_compare.get("status", "")).strip()
    payload["mode"] = str(payload.get("recommended_exit_mode") or payload.get("mode") or "hold").strip().lower() or "hold"
    selected_row = _select_resolved_exit_row(best_row, best_risk_row, exit_recommendation)
    payload["recommended_hold_bars"] = _resolve_recommended_hold_bars(
        selected_row=selected_row,
        best_hold_row=best_row,
        fallback_hold_bars=fallback_hold_bars,
    )
    if isinstance(selected_row, dict):
        chosen_family = "risk" if selected_row is best_risk_row else "hold"
        payload["chosen_family"] = chosen_family
        payload["chosen_rule_id"] = str(selected_row.get("rule_id", "")).strip()
        payload["selected_policy_kind"] = _resolved_exit_row_kind(
            selected_row=selected_row,
            best_hold_row=best_row,
            best_risk_row=best_risk_row,
            exit_recommendation=exit_recommendation,
        )
        payload["selected_objective_score"] = float(selected_row.get("utility_total", 0.0))
        payload["selected_summary"] = dict(selected_row.get("summary", {}))
        payload["selected_grid_point"] = dict(selected_row.get("grid_point", {}))
    else:
        payload["chosen_family"] = ""
        payload["chosen_rule_id"] = ""
    return payload


def _build_execution_doc(
    best_row: dict[str, Any] | None,
    *,
    ranked_rows: list[dict[str, Any]] | None = None,
    fallback_settings: ModelAlphaExecutionSettings,
) -> dict[str, Any]:
    execution_frontier = _build_execution_stage_frontier(rows=ranked_rows or [], fallback_settings=fallback_settings)
    payload: dict[str, Any]
    if not isinstance(best_row, dict):
        payload = {
            "recommended_price_mode": str(fallback_settings.price_mode),
            "recommended_timeout_bars": int(fallback_settings.timeout_bars),
            "recommended_replace_max": int(fallback_settings.replace_max),
            "recommendation_source": "manual_fallback",
        }
    else:
        grid_point = dict(best_row.get("grid_point", {}))
        payload = {
            "recommended_price_mode": str(grid_point.get("price_mode", fallback_settings.price_mode)),
            "recommended_timeout_bars": int(grid_point.get("timeout_bars", fallback_settings.timeout_bars)),
            "recommended_replace_max": int(grid_point.get("replace_max", fallback_settings.replace_max)),
            "recommendation_source": "execution_backtest_grid_search",
            "objective_score": float(best_row.get("utility_total", 0.0)),
            "wins": int(best_row.get("wins", 0)),
            "losses": int(best_row.get("losses", 0)),
            "comparable_pairs": int(best_row.get("comparable_pairs", 0)),
            "summary": dict(best_row.get("summary", {})),
            "grid_point": grid_point,
        }
    payload.update(execution_frontier)
    return payload


def _build_exit_family_doc(*, family: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    normalized_rows = [dict(row) for row in rows if isinstance(row, dict)]
    best_overall = normalized_rows[0] if normalized_rows else None
    best_comparable = next((dict(row) for row in normalized_rows if bool(row.get("validation_comparable", False))), None)
    status = "missing"
    if normalized_rows:
        status = "supported" if best_comparable is not None else "insufficient_support"
    reason_codes: list[str] = []
    if status == "missing":
        reason_codes.append(f"{str(family).strip().upper()}_FAMILY_EMPTY")
    elif status == "insufficient_support":
        reason_codes.append(f"{str(family).strip().upper()}_FAMILY_NO_COMPARABLE_RULE")
    top_rules = [_compact_exit_rule_row(row) for row in normalized_rows[:6]]
    return {
        "family": str(family).strip().lower() or "hold",
        "status": status,
        "rows_total": int(len(normalized_rows)),
        "comparable_rows": int(sum(1 for row in normalized_rows if bool(row.get("validation_comparable", False)))),
        "reason_codes": reason_codes,
        "best_rule_id": str((best_overall or {}).get("rule_id", "")).strip(),
        "best_comparable_rule_id": str((best_comparable or {}).get("rule_id", "")).strip(),
        "best_rule": _compact_exit_rule_row(best_overall) if isinstance(best_overall, dict) else {},
        "best_comparable_rule": _compact_exit_rule_row(best_comparable) if isinstance(best_comparable, dict) else {},
        "top_rules": top_rules,
    }


def _build_exit_family_compare_doc(
    *,
    hold_family: dict[str, Any],
    risk_family: dict[str, Any],
) -> dict[str, Any]:
    hold_row = _resolve_family_comparable_row(hold_family)
    risk_row = _resolve_family_comparable_row(risk_family)
    if isinstance(hold_row, dict) and isinstance(risk_row, dict):
        compare_doc = compare_execution_balanced_pareto(
            risk_row.get("summary", {}),
            hold_row.get("summary", {}),
        )
        return {
            "status": "supported" if bool(compare_doc.get("comparable", False)) else "insufficient_support",
            "decision": str(compare_doc.get("decision", "")).strip(),
            "comparable": bool(compare_doc.get("comparable", False)),
            "reason_codes": [str(item).strip() for item in (compare_doc.get("reasons") or []) if str(item).strip()],
            "hold_rule_id": str(hold_row.get("rule_id", "")).strip(),
            "risk_rule_id": str(risk_row.get("rule_id", "")).strip(),
        }
    reason_codes: list[str] = []
    if not isinstance(hold_row, dict):
        reason_codes.append("HOLD_FAMILY_NO_COMPARABLE_RULE")
    if not isinstance(risk_row, dict):
        reason_codes.append("RISK_FAMILY_NO_COMPARABLE_RULE")
    return {
        "status": "insufficient_support",
        "decision": "not_comparable",
        "comparable": False,
        "reason_codes": reason_codes,
        "hold_rule_id": str((hold_family.get("best_rule_id") or "")).strip(),
        "risk_rule_id": str((risk_family.get("best_rule_id") or "")).strip(),
    }


def _resolve_family_aware_exit_recommendation(
    *,
    family_compare: dict[str, Any],
    best_hold_row: dict[str, Any] | None,
    best_risk_row: dict[str, Any] | None,
) -> dict[str, Any]:
    compare_status = str((family_compare or {}).get("status", "")).strip().lower()
    comparable = bool((family_compare or {}).get("comparable", False))
    if compare_status != "supported" or not comparable:
        return {
            "recommended_exit_mode": "",
            "recommended_exit_mode_source": "execution_backtest_family_compare",
            "recommended_exit_mode_reason_code": "EXIT_FAMILY_INSUFFICIENT_EVIDENCE",
            "exit_mode_compare": {
                "decision": str((family_compare or {}).get("decision", "")).strip() or "not_comparable",
                "comparable": False,
                "reasons": list((family_compare or {}).get("reason_codes") or []),
            },
        }
    return resolve_exit_mode_recommendation(best_hold_row, best_risk_row)


def _resolve_family_representative_row(family_doc: dict[str, Any]) -> dict[str, Any] | None:
    best_comparable = family_doc.get("best_comparable_rule")
    if isinstance(best_comparable, dict) and best_comparable:
        return dict(best_comparable)
    best_rule = family_doc.get("best_rule")
    if isinstance(best_rule, dict) and best_rule:
        return dict(best_rule)
    return None


def _resolve_family_comparable_row(family_doc: dict[str, Any]) -> dict[str, Any] | None:
    best_comparable = family_doc.get("best_comparable_rule")
    if isinstance(best_comparable, dict) and best_comparable:
        return dict(best_comparable)
    return None


def _compact_exit_rule_row(row: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(row, dict):
        return {}
    summary = dict(row.get("summary", {}))
    execution_structure = dict(row.get("execution_structure", {}))
    return {
        "rule_id": str(row.get("rule_id", "")).strip(),
        "kind": str(row.get("kind", "")).strip(),
        "grid_point": dict(row.get("grid_point", {})),
        "utility_total": float(row.get("utility_total", 0.0) or 0.0),
        "utility_total_raw": float(row.get("utility_total_raw", row.get("utility_total", 0.0)) or 0.0),
        "execution_structure_penalty_total": float(row.get("execution_structure_penalty_total", 0.0) or 0.0),
        "objective_score": float(row.get("utility_total", 0.0) or 0.0),
        "comparable_pairs": int(row.get("comparable_pairs", 0) or 0),
        "validation_comparable": bool(row.get("validation_comparable", False)),
        "realized_pnl_quote": float(summary.get("realized_pnl_quote", 0.0) or 0.0),
        "fill_rate": float(summary.get("fill_rate", 0.0) or 0.0),
        "avg_time_to_fill_ms": float(summary.get("avg_time_to_fill_ms", 0.0) or 0.0),
        "p50_time_to_fill_ms": float(summary.get("p50_time_to_fill_ms", 0.0) or 0.0),
        "p90_time_to_fill_ms": float(summary.get("p90_time_to_fill_ms", 0.0) or 0.0),
        "max_drawdown_pct": float(summary.get("max_drawdown_pct", 0.0) or 0.0),
        "slippage_bps_mean": float(summary.get("slippage_bps_mean", 0.0) or 0.0),
        "orders_filled": int(summary.get("orders_filled", 0) or 0),
        "payoff_ratio": float(execution_structure.get("payoff_ratio", 0.0) or 0.0),
        "avg_win_quote": float(execution_structure.get("avg_win_quote", 0.0) or 0.0),
        "avg_loss_quote": float(execution_structure.get("avg_loss_quote", 0.0) or 0.0),
        "tp_exit_share": float(execution_structure.get("tp_exit_share", 0.0) or 0.0),
        "sl_exit_share": float(execution_structure.get("sl_exit_share", 0.0) or 0.0),
        "timeout_exit_share": float(execution_structure.get("timeout_exit_share", 0.0) or 0.0),
        "closed_trade_count": int(execution_structure.get("closed_trade_count", 0) or 0),
        "execution_structure": execution_structure,
        "summary": summary,
    }


def _rank_execution_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = [dict(item) for item in rows if isinstance(item, dict) and isinstance(item.get("summary"), dict)]
    compare_contract = resolve_v4_execution_compare_contract()
    for row in normalized:
        row.setdefault("wins", 0)
        row.setdefault("losses", 0)
        row.setdefault("holds", 0)
        row.setdefault("comparable_pairs", 0)
        row.setdefault("utility_total", 0.0)
        row.setdefault("implementation_utility_total", 0.0)
        summary = dict(row.get("summary", {}))
        validation = summary.get("execution_validation")
        if not isinstance(validation, dict) or not validation:
            validation = build_execution_validation_summary(
                summary,
                window_minutes=int(compare_contract.get("validation_window_minutes", 60) or 60),
                fold_count=int(compare_contract.get("validation_fold_count", 6) or 6),
                min_active_windows=int(compare_contract.get("validation_min_active_windows", 12) or 12),
                target_return=float(compare_contract.get("validation_target_return", 0.0) or 0.0),
                lpm_order=int(compare_contract.get("validation_lpm_order", 2) or 2),
            )
            summary["execution_validation"] = validation
            row["summary"] = summary
        row["validation"] = dict(validation)
        row["comparable_pairs"] = int(validation.get("comparable_fold_count", 0) or 0)
        row["utility_total_raw"] = float(validation.get("objective_score", 0.0) or 0.0)
        execution_structure = _resolve_execution_structure_metrics(summary)
        row["execution_structure"] = execution_structure
        execution_structure_penalty = _build_execution_structure_penalty(execution_structure)
        row["execution_structure_penalty"] = execution_structure_penalty
        row["execution_structure_penalty_total"] = float(execution_structure_penalty.get("total", 0.0) or 0.0)
        row["utility_total"] = float(row.get("utility_total_raw", 0.0)) - float(
            row.get("execution_structure_penalty_total", 0.0) or 0.0
        )
        row["implementation_utility_total"] = -float(validation.get("objective_std", 0.0) or 0.0)
        row["validation_nonnegative_ratio_mean"] = float(validation.get("nonnegative_ratio_mean", 0.0) or 0.0)
        row["validation_max_window_drawdown_pct"] = float(validation.get("max_window_drawdown_pct", 0.0) or 0.0)
        row["validation_worst_window_return"] = float(validation.get("worst_window_return", 0.0) or 0.0)
        row["validation_comparable"] = bool(validation.get("comparable", False))

    normalized.sort(
        key=lambda item: (
            1 if bool(item.get("validation_comparable", False)) else 0,
            float(item.get("utility_total", 0.0)),
            float(item.get("implementation_utility_total", 0.0)),
            float(item.get("validation_nonnegative_ratio_mean", 0.0)),
            float((item.get("summary") or {}).get("realized_pnl_quote", 0.0)),
            float((item.get("summary") or {}).get("fill_rate", 0.0)),
            -float((item.get("summary") or {}).get("avg_time_to_fill_ms", 0.0)),
            -float((item.get("summary") or {}).get("p90_time_to_fill_ms", 0.0)),
            -float(item.get("validation_max_window_drawdown_pct", 0.0)),
            float(item.get("validation_worst_window_return", 0.0)),
            -float((item.get("summary") or {}).get("max_drawdown_pct", 0.0)),
            -float((item.get("summary") or {}).get("slippage_bps_mean", 0.0)),
        ),
        reverse=True,
    )
    return normalized


def _build_execution_stage_frontier(
    *,
    rows: list[dict[str, Any]],
    fallback_settings: ModelAlphaExecutionSettings,
) -> dict[str, Any]:
    mode_rows: dict[str, list[dict[str, Any]]] = {mode: [] for mode in _EXECUTION_STAGE_ORDER}
    for row in rows:
        if not isinstance(row, dict):
            continue
        grid_point = dict(row.get("grid_point", {}))
        mode = str(grid_point.get("price_mode", "")).strip().upper()
        if mode in mode_rows:
            mode_rows[mode].append(dict(row))
    stage_docs = [
        _build_execution_stage_doc(
            mode=mode,
            rows=mode_rows.get(mode, []),
            fallback_settings=fallback_settings,
        )
        for mode in _EXECUTION_STAGE_ORDER
    ]
    supported_stages = [item for item in stage_docs if bool(item.get("supported"))]
    best_by_objective = max(
        supported_stages,
        key=lambda item: float(item.get("objective_score", 0.0) or 0.0),
        default=None,
    )
    best_by_fill_probability = max(
        supported_stages,
        key=lambda item: float(item.get("expected_fill_probability", 0.0) or 0.0),
        default=None,
    )
    best_by_time_to_fill = min(
        (
            item
            for item in supported_stages
            if float(item.get("expected_time_to_fill_ms", 0.0) or 0.0) > 0.0
        ),
        key=lambda item: float(item.get("expected_time_to_fill_ms", 0.0) or 0.0),
        default=None,
    )
    return {
        "policy": "empirical_fill_frontier_v1",
        "dynamic_stage_selection_enabled": bool(supported_stages),
        "stage_decision_mode": "sequential_positive_net_edge_v1",
        "stage_order": list(_EXECUTION_STAGE_ORDER),
        "stages": stage_docs,
        "frontier_summary": {
            "supported_stage_count": int(len(supported_stages)),
            "best_stage_by_objective": str((best_by_objective or {}).get("stage", "")).strip(),
            "best_stage_by_fill_probability": str((best_by_fill_probability or {}).get("stage", "")).strip(),
            "best_stage_by_time_to_fill": str((best_by_time_to_fill or {}).get("stage", "")).strip(),
        },
        "no_trade_region": {
            "policy": "implementation_shortfall_proxy_v1",
            "decision_metric": "expected_edge_bps * expected_fill_probability - expected_slippage_bps",
            "positive_net_edge_required": True,
            "fallback_action": "use_recommended_execution_profile_when_edge_missing",
        },
    }


def _build_execution_stage_doc(
    *,
    mode: str,
    rows: list[dict[str, Any]],
    fallback_settings: ModelAlphaExecutionSettings,
) -> dict[str, Any]:
    comparable_row = next((dict(row) for row in rows if bool(row.get("validation_comparable", False))), None)
    selected = comparable_row or (dict(rows[0]) if rows else None)
    if not isinstance(selected, dict):
        return {
            "stage": str(mode).strip().upper(),
            "supported": False,
            "validation_comparable": False,
            "recommended_price_mode": str(mode).strip().upper(),
            "recommended_timeout_bars": int(fallback_settings.timeout_bars),
            "recommended_replace_max": int(fallback_settings.replace_max),
            "reason_code": "NO_EXECUTION_STAGE_EVIDENCE",
        }
    summary = dict(selected.get("summary", {}))
    grid_point = dict(selected.get("grid_point", {}))
    return {
        "stage": str(mode).strip().upper(),
        "supported": True,
        "validation_comparable": bool(selected.get("validation_comparable", False)),
        "rule_id": str(selected.get("rule_id", "")).strip(),
        "recommended_price_mode": str(grid_point.get("price_mode", mode)).strip().upper() or str(mode).strip().upper(),
        "recommended_timeout_bars": int(grid_point.get("timeout_bars", fallback_settings.timeout_bars)),
        "recommended_replace_max": int(grid_point.get("replace_max", fallback_settings.replace_max)),
        "objective_score": float(selected.get("utility_total", 0.0) or 0.0),
        "expected_fill_probability": float(summary.get("fill_rate", 0.0) or 0.0),
        "expected_time_to_fill_ms": float(summary.get("avg_time_to_fill_ms", 0.0) or 0.0),
        "p50_time_to_fill_ms": float(summary.get("p50_time_to_fill_ms", 0.0) or 0.0),
        "p90_time_to_fill_ms": float(summary.get("p90_time_to_fill_ms", 0.0) or 0.0),
        "expected_slippage_bps": float(summary.get("slippage_bps_mean", 0.0) or 0.0),
        "orders_filled": int(summary.get("orders_filled", 0) or 0),
        "realized_pnl_quote": float(summary.get("realized_pnl_quote", 0.0) or 0.0),
        "max_drawdown_pct": float(summary.get("max_drawdown_pct", 0.0) or 0.0),
        "summary": summary,
    }


def _resolve_execution_structure_metrics(summary: dict[str, Any]) -> dict[str, Any]:
    existing = summary.get("execution_structure")
    if isinstance(existing, dict) and existing:
        return _normalize_execution_structure_metrics(existing)
    run_dir_raw = str(summary.get("run_dir", "")).strip()
    if not run_dir_raw:
        return _normalize_execution_structure_metrics({})
    summary_json_path = Path(run_dir_raw) / "summary.json"
    if summary_json_path.exists():
        try:
            parsed = json.loads(summary_json_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            parsed = {}
        if isinstance(parsed, dict):
            summary_metrics = parsed.get("execution_structure")
            if isinstance(summary_metrics, dict) and summary_metrics:
                normalized = _normalize_execution_structure_metrics(summary_metrics)
                summary["execution_structure"] = normalized
                return normalized
    trades_path = Path(run_dir_raw) / "trades.csv"
    if not trades_path.exists():
        return _normalize_execution_structure_metrics({})
    metrics = summarize_trades_csv(trades_path)
    summary["execution_structure"] = metrics
    return metrics


def _build_execution_structure_metrics_from_trades(trades_path: Path) -> dict[str, Any]:
    try:
        with trades_path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
    except OSError:
        return _normalize_execution_structure_metrics({})

    position_queues: dict[str, deque[dict[str, float]]] = {}
    round_trip_pnls: list[float] = []
    exit_reason_counts: dict[str, int] = {}
    market_loss_abs_totals: dict[str, float] = {}

    for row in rows:
        market = str(row.get("market", "")).strip().upper()
        side = str(row.get("side", "")).strip().lower()
        volume = _safe_optional_float(row.get("volume")) or 0.0
        price = _safe_optional_float(row.get("fill_price"))
        if price is None:
            price = _safe_optional_float(row.get("price")) or 0.0
        fee_quote = _safe_optional_float(row.get("fee_quote")) or 0.0
        if not market or volume <= 0.0 or price <= 0.0:
            continue

        if side == "bid":
            queue = position_queues.setdefault(market, deque())
            queue.append(
                {
                    "remaining_volume": float(volume),
                    "price": float(price),
                    "fee_per_unit": float(fee_quote) / float(volume) if float(volume) > 0.0 else 0.0,
                }
            )
            continue

        if side != "ask":
            continue

        queue = position_queues.setdefault(market, deque())
        remaining_volume = float(volume)
        gross_pnl_quote = 0.0
        entry_fee_quote = 0.0
        matched_volume = 0.0
        while queue and remaining_volume > 1e-12:
            entry_lot = queue[0]
            lot_remaining = max(float(entry_lot.get("remaining_volume", 0.0)), 0.0)
            if lot_remaining <= 1e-12:
                queue.popleft()
                continue
            matched = min(lot_remaining, remaining_volume)
            gross_pnl_quote += (float(price) - float(entry_lot.get("price", 0.0))) * float(matched)
            entry_fee_quote += float(entry_lot.get("fee_per_unit", 0.0)) * float(matched)
            matched_volume += float(matched)
            remaining_volume -= float(matched)
            next_remaining = max(lot_remaining - float(matched), 0.0)
            if next_remaining <= 1e-12:
                queue.popleft()
            else:
                entry_lot["remaining_volume"] = float(next_remaining)
                queue[0] = entry_lot

        if matched_volume <= 1e-12:
            continue

        exit_fee_quote = float(fee_quote) * (float(matched_volume) / float(volume)) if float(volume) > 0.0 else 0.0
        net_pnl_quote = float(gross_pnl_quote) - float(entry_fee_quote) - float(exit_fee_quote)
        round_trip_pnls.append(float(net_pnl_quote))
        if net_pnl_quote < 0.0:
            market_loss_abs_totals[market] = float(market_loss_abs_totals.get(market, 0.0)) + abs(float(net_pnl_quote))
        reason_code = str(row.get("reason_code", "")).strip().upper() or "UNKNOWN"
        exit_reason_counts[reason_code] = int(exit_reason_counts.get(reason_code, 0)) + 1

    wins = [float(value) for value in round_trip_pnls if float(value) > 0.0]
    losses = [abs(float(value)) for value in round_trip_pnls if float(value) < 0.0]
    closed_trade_count = len(round_trip_pnls)
    avg_win_quote = (sum(wins) / len(wins)) if wins else 0.0
    avg_loss_quote = (sum(losses) / len(losses)) if losses else 0.0
    payoff_ratio = (avg_win_quote / avg_loss_quote) if avg_loss_quote > 0.0 else (float("inf") if avg_win_quote > 0.0 else 0.0)
    tp_exit_count = sum(count for reason, count in exit_reason_counts.items() if reason.endswith("_TP"))
    sl_exit_count = sum(count for reason, count in exit_reason_counts.items() if reason.endswith("_SL"))
    trailing_exit_count = sum(count for reason, count in exit_reason_counts.items() if reason.endswith("_TRAILING"))
    timeout_exit_count = sum(count for reason, count in exit_reason_counts.items() if "TIMEOUT" in reason)
    total_negative_abs = sum(float(value) for value in market_loss_abs_totals.values())
    market_loss_concentration = (
        max((float(value) / total_negative_abs) for value in market_loss_abs_totals.values())
        if total_negative_abs > 0.0 and market_loss_abs_totals
        else 0.0
    )
    return _normalize_execution_structure_metrics(
        {
            "closed_trade_count": closed_trade_count,
            "wins": len(wins),
            "losses": len(losses),
            "avg_win_quote": avg_win_quote,
            "avg_loss_quote": avg_loss_quote,
            "payoff_ratio": payoff_ratio if payoff_ratio != float("inf") else 9_999.0,
            "tp_exit_count": tp_exit_count,
            "sl_exit_count": sl_exit_count,
            "trailing_exit_count": trailing_exit_count,
            "timeout_exit_count": timeout_exit_count,
            "exit_reason_counts": exit_reason_counts,
            "market_loss_concentration": market_loss_concentration,
        }
    )


def _normalize_execution_structure_metrics(payload: dict[str, Any]) -> dict[str, Any]:
    closed_trade_count = max(int(payload.get("closed_trade_count", 0) or 0), 0)
    tp_exit_count = max(int(payload.get("tp_exit_count", 0) or 0), 0)
    sl_exit_count = max(int(payload.get("sl_exit_count", 0) or 0), 0)
    trailing_exit_count = max(int(payload.get("trailing_exit_count", 0) or 0), 0)
    timeout_exit_count = max(int(payload.get("timeout_exit_count", 0) or 0), 0)
    denom = max(closed_trade_count, 1)
    return {
        "closed_trade_count": closed_trade_count,
        "wins": max(int(payload.get("wins", 0) or 0), 0),
        "losses": max(int(payload.get("losses", 0) or 0), 0),
        "avg_win_quote": max(float(payload.get("avg_win_quote", 0.0) or 0.0), 0.0),
        "avg_loss_quote": max(float(payload.get("avg_loss_quote", 0.0) or 0.0), 0.0),
        "payoff_ratio": max(float(payload.get("payoff_ratio", 0.0) or 0.0), 0.0),
        "tp_exit_count": tp_exit_count,
        "sl_exit_count": sl_exit_count,
        "trailing_exit_count": trailing_exit_count,
        "timeout_exit_count": timeout_exit_count,
        "tp_exit_share": float(tp_exit_count) / float(denom),
        "sl_exit_share": float(sl_exit_count) / float(denom),
        "trailing_exit_share": float(trailing_exit_count) / float(denom),
        "timeout_exit_share": float(timeout_exit_count) / float(denom),
        "market_loss_concentration": max(min(float(payload.get("market_loss_concentration", 0.0) or 0.0), 1.0), 0.0),
        "exit_reason_counts": {
            str(key).strip().upper(): int(value)
            for key, value in dict(payload.get("exit_reason_counts", {})).items()
            if str(key).strip()
        },
    }


def _build_execution_structure_penalty(metrics: dict[str, Any]) -> dict[str, float]:
    closed_trade_count = max(int(metrics.get("closed_trade_count", 0) or 0), 0)
    payoff_ratio = max(float(metrics.get("payoff_ratio", 0.0) or 0.0), 0.0)
    tp_exit_share = max(float(metrics.get("tp_exit_share", 0.0) or 0.0), 0.0)
    sl_exit_share = max(float(metrics.get("sl_exit_share", 0.0) or 0.0), 0.0)
    timeout_exit_share = max(float(metrics.get("timeout_exit_share", 0.0) or 0.0), 0.0)
    market_loss_concentration = max(float(metrics.get("market_loss_concentration", 0.0) or 0.0), 0.0)

    payoff_ratio_penalty = 0.0
    if closed_trade_count >= 2:
        payoff_ratio_penalty = max(1.0 - min(float(payoff_ratio), 1.0), 0.0) * 0.30

    sl_share_penalty = 0.0
    if closed_trade_count >= 3:
        sl_share_penalty = max(float(sl_exit_share) - 0.40, 0.0) * 0.20

    timeout_share_penalty = 0.0
    if closed_trade_count >= 3:
        timeout_share_penalty = max(float(timeout_exit_share) - 0.60, 0.0) * 0.10

    tp_absence_penalty = 0.0
    if closed_trade_count >= 3 and tp_exit_share <= 0.0 and (sl_exit_share + timeout_exit_share) >= 0.99:
        tp_absence_penalty = 0.10

    loss_concentration_penalty = 0.0
    if closed_trade_count >= 3:
        loss_concentration_penalty = max(float(market_loss_concentration) - 0.75, 0.0) * 0.10

    total = (
        float(payoff_ratio_penalty)
        + float(sl_share_penalty)
        + float(timeout_share_penalty)
        + float(tp_absence_penalty)
        + float(loss_concentration_penalty)
    )
    return {
        "payoff_ratio_penalty": float(payoff_ratio_penalty),
        "sl_share_penalty": float(sl_share_penalty),
        "timeout_share_penalty": float(timeout_share_penalty),
        "tp_absence_penalty": float(tp_absence_penalty),
        "loss_concentration_penalty": float(loss_concentration_penalty),
        "total": float(total),
    }


def _build_exit_rule_id(*, kind: str, grid_point: dict[str, Any]) -> str:
    kind_value = str(kind).strip().lower() or "hold"
    if kind_value == "hold":
        return f"hold_h{int(grid_point.get('hold_bars', 0) or 0)}"
    return (
        f"risk_h{int(grid_point.get('hold_bars', 0) or 0)}"
        f"_{str(grid_point.get('risk_vol_feature', '')).strip().lower()}"
        f"_tp{_rule_float_token(grid_point.get('tp_vol_multiplier'))}"
        f"_sl{_rule_float_token(grid_point.get('sl_vol_multiplier'))}"
        f"_tr{_rule_float_token(grid_point.get('trailing_vol_multiplier'))}"
    )


def _rule_float_token(value: Any) -> str:
    numeric = _safe_optional_float(value)
    if numeric is None:
        return "na"
    return str(numeric).replace(".", "p")


def _dedupe_positive_ints(values: tuple[int, ...]) -> tuple[int, ...]:
    seen: set[int] = set()
    rows: list[int] = []
    for raw in values:
        value = max(int(raw), 1)
        if value in seen:
            continue
        seen.add(value)
        rows.append(value)
    return tuple(rows)


def _dedupe_nonnegative_ints(values: tuple[int, ...]) -> tuple[int, ...]:
    seen: set[int] = set()
    rows: list[int] = []
    for raw in values:
        value = max(int(raw), 0)
        if value in seen:
            continue
        seen.add(value)
        rows.append(value)
    return tuple(rows)


def _dedupe_price_modes(values: tuple[str, ...]) -> tuple[str, ...]:
    seen: set[str] = set()
    rows: list[str] = []
    for raw in values:
        value = str(raw).strip().upper() or "JOIN"
        if value in seen:
            continue
        seen.add(value)
        rows.append(value)
    return tuple(rows)


def _dedupe_positive_floats(values: tuple[float, ...]) -> tuple[float, ...]:
    seen: set[float] = set()
    rows: list[float] = []
    for raw in values:
        value = max(float(raw), 1e-9)
        if value in seen:
            continue
        seen.add(value)
        rows.append(value)
    return tuple(rows)


def _dedupe_nonnegative_floats(values: tuple[float, ...]) -> tuple[float, ...]:
    seen: set[float] = set()
    rows: list[float] = []
    for raw in values:
        value = max(float(raw), 0.0)
        if value in seen:
            continue
        seen.add(value)
        rows.append(value)
    return tuple(rows)


def _dedupe_texts(values: tuple[str, ...]) -> tuple[str, ...]:
    seen: set[str] = set()
    rows: list[str] = []
    for raw in values:
        value = str(raw).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        rows.append(value)
    return tuple(rows)


def _safe_text(value: Any, default: str) -> str:
    text = str(value).strip()
    return text or str(default)


def _safe_optional_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _resolve_execution_backtest_exit_settings(
    *,
    base_exit: ModelAlphaExitSettings,
    best_hold_row: dict[str, Any] | None,
    best_risk_row: dict[str, Any] | None,
    exit_recommendation: dict[str, Any],
) -> ModelAlphaExitSettings:
    selected_row = _select_resolved_exit_row(best_hold_row, best_risk_row, exit_recommendation)
    resolved_hold_bars = _resolve_recommended_hold_bars(
        selected_row=selected_row,
        best_hold_row=best_hold_row,
        fallback_hold_bars=int(base_exit.hold_bars),
    )
    recommended_mode = str((exit_recommendation or {}).get("recommended_exit_mode", "")).strip().lower()
    if recommended_mode == "risk" and isinstance(best_risk_row, dict):
        grid_point = dict(best_risk_row.get("grid_point", {}))
        return ModelAlphaExitSettings(
            mode="risk",
            hold_bars=int(resolved_hold_bars),
            use_learned_exit_mode=False,
            use_learned_hold_bars=False,
            use_learned_risk_recommendations=False,
            risk_scaling_mode=str(grid_point.get("risk_scaling_mode", "volatility_scaled")),
            risk_vol_feature=str(grid_point.get("risk_vol_feature", base_exit.risk_vol_feature)).strip()
            or str(base_exit.risk_vol_feature),
            tp_vol_multiplier=_safe_optional_float(grid_point.get("tp_vol_multiplier", base_exit.tp_vol_multiplier)),
            sl_vol_multiplier=_safe_optional_float(grid_point.get("sl_vol_multiplier", base_exit.sl_vol_multiplier)),
            trailing_vol_multiplier=_safe_optional_float(
                grid_point.get("trailing_vol_multiplier", base_exit.trailing_vol_multiplier)
            ),
            tp_pct=float(base_exit.tp_pct),
            sl_pct=float(base_exit.sl_pct),
            trailing_pct=float(base_exit.trailing_pct),
            expected_exit_slippage_bps=base_exit.expected_exit_slippage_bps,
            expected_exit_fee_bps=base_exit.expected_exit_fee_bps,
        )
    return replace(
        base_exit,
        mode="hold",
        hold_bars=int(resolved_hold_bars),
        use_learned_exit_mode=False,
        use_learned_hold_bars=False,
        use_learned_risk_recommendations=False,
    )


def _select_resolved_exit_row(
    best_hold_row: dict[str, Any] | None,
    best_risk_row: dict[str, Any] | None,
    exit_recommendation: dict[str, Any] | None,
) -> dict[str, Any] | None:
    recommended_mode = str((exit_recommendation or {}).get("recommended_exit_mode", "")).strip().lower()
    if recommended_mode == "risk" and isinstance(best_risk_row, dict):
        return best_risk_row
    if isinstance(best_hold_row, dict):
        return best_hold_row
    if isinstance(best_risk_row, dict):
        return best_risk_row
    return None


def _resolved_exit_row_kind(
    *,
    selected_row: dict[str, Any],
    best_hold_row: dict[str, Any] | None,
    best_risk_row: dict[str, Any] | None,
    exit_recommendation: dict[str, Any] | None,
) -> str:
    explicit_kind = str(selected_row.get("kind", "")).strip()
    if explicit_kind:
        return explicit_kind
    recommended_mode = str((exit_recommendation or {}).get("recommended_exit_mode", "")).strip().lower()
    if recommended_mode == "risk" and selected_row is best_risk_row:
        return "risk_exit"
    if selected_row is best_hold_row:
        return "hold"
    if selected_row is best_risk_row:
        return "risk_exit"
    return recommended_mode or "hold"


def _resolve_recommended_hold_bars(
    *,
    selected_row: dict[str, Any] | None,
    best_hold_row: dict[str, Any] | None,
    fallback_hold_bars: int,
) -> int:
    for row in (selected_row, best_hold_row):
        if not isinstance(row, dict):
            continue
        grid_point = dict(row.get("grid_point", {}))
        try:
            hold_bars = int(grid_point.get("hold_bars", 0) or 0)
        except (TypeError, ValueError):
            hold_bars = 0
        if hold_bars > 0:
            return hold_bars
    return max(int(fallback_hold_bars), 1)
