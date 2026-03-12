"""Execution-aware runtime recommendation optimizer for model_alpha_v1."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any

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
    exit_recommendation = resolve_exit_mode_recommendation(best_hold, best_risk_exit)
    exit_family_compare = _build_exit_family_compare_doc(
        hold_family=hold_family,
        risk_family=risk_family,
        exit_recommendation=exit_recommendation,
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
        "execution": _build_execution_doc(best_execution, fallback_settings=base_settings.execution),
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
    fallback_settings: ModelAlphaExecutionSettings,
) -> dict[str, Any]:
    if not isinstance(best_row, dict):
        return {
            "recommended_price_mode": str(fallback_settings.price_mode),
            "recommended_timeout_bars": int(fallback_settings.timeout_bars),
            "recommended_replace_max": int(fallback_settings.replace_max),
            "recommendation_source": "manual_fallback",
        }
    grid_point = dict(best_row.get("grid_point", {}))
    return {
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
    exit_recommendation: dict[str, Any],
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
            "recommended_exit_mode": str((exit_recommendation or {}).get("recommended_exit_mode", "")).strip(),
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
        "recommended_exit_mode": str((exit_recommendation or {}).get("recommended_exit_mode", "")).strip(),
        "hold_rule_id": str((hold_family.get("best_rule_id") or "")).strip(),
        "risk_rule_id": str((risk_family.get("best_rule_id") or "")).strip(),
    }


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
    return {
        "rule_id": str(row.get("rule_id", "")).strip(),
        "kind": str(row.get("kind", "")).strip(),
        "grid_point": dict(row.get("grid_point", {})),
        "utility_total": float(row.get("utility_total", 0.0) or 0.0),
        "objective_score": float(row.get("utility_total", 0.0) or 0.0),
        "comparable_pairs": int(row.get("comparable_pairs", 0) or 0),
        "validation_comparable": bool(row.get("validation_comparable", False)),
        "realized_pnl_quote": float(summary.get("realized_pnl_quote", 0.0) or 0.0),
        "fill_rate": float(summary.get("fill_rate", 0.0) or 0.0),
        "max_drawdown_pct": float(summary.get("max_drawdown_pct", 0.0) or 0.0),
        "slippage_bps_mean": float(summary.get("slippage_bps_mean", 0.0) or 0.0),
        "orders_filled": int(summary.get("orders_filled", 0) or 0),
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
        row["utility_total"] = float(validation.get("objective_score", 0.0) or 0.0)
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
            -float(item.get("validation_max_window_drawdown_pct", 0.0)),
            float(item.get("validation_worst_window_return", 0.0)),
            float((item.get("summary") or {}).get("realized_pnl_quote", 0.0)),
            float((item.get("summary") or {}).get("fill_rate", 0.0)),
            -float((item.get("summary") or {}).get("max_drawdown_pct", 0.0)),
            -float((item.get("summary") or {}).get("slippage_bps_mean", 0.0)),
        ),
        reverse=True,
    )
    return normalized


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
