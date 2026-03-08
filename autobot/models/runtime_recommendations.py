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

from .execution_acceptance import ExecutionAcceptanceOptions, run_model_execution_backtest
from .research_acceptance import compare_execution_balanced_pareto


@dataclass(frozen=True)
class RuntimeRecommendationGrid:
    hold_bars_grid: tuple[int, ...] = (3, 6, 9, 12)
    price_mode_grid: tuple[str, ...] = ("PASSIVE_MAKER", "JOIN", "CROSS_1T")
    timeout_bars_grid: tuple[int, ...] = (1, 2, 4)
    replace_max_grid: tuple[int, ...] = (0, 1, 2)


def optimize_runtime_recommendations(
    *,
    options: ExecutionAcceptanceOptions,
    candidate_ref: str,
    grid: RuntimeRecommendationGrid | None = None,
) -> dict[str, Any]:
    active_grid = grid or RuntimeRecommendationGrid()
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
                "grid_point": {"hold_bars": int(hold_bars)},
                "summary": summary,
            }
        )

    ranked_holds = _rank_execution_rows(hold_rows)
    best_hold = ranked_holds[0] if ranked_holds else None
    selected_hold_bars = (
        int(best_hold["grid_point"]["hold_bars"])
        if best_hold is not None
        else int(base_settings.exit.hold_bars)
    )

    execution_rows: list[dict[str, Any]] = []
    for price_mode in _dedupe_price_modes(active_grid.price_mode_grid):
        for timeout_bars in _dedupe_positive_ints(active_grid.timeout_bars_grid):
            for replace_max in _dedupe_nonnegative_ints(active_grid.replace_max_grid):
                candidate_settings = replace(
                    base_settings,
                    exit=replace(
                        base_settings.exit,
                        mode="hold",
                        hold_bars=int(selected_hold_bars),
                        use_learned_hold_bars=False,
                    ),
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
    return {
        "version": 1,
        "status": "ready" if best_hold is not None and best_execution is not None else "fallback",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "recommendation_source": "execution_backtest_grid_search_v1",
        "objective": "balanced_pareto_execution_tournament",
        "candidate_ref": candidate_id,
        "selection_context": {
            "use_learned_recommendations": bool(base_settings.selection.use_learned_recommendations),
            "registry_threshold_key_fallback": str(base_settings.selection.registry_threshold_key),
            "selected_threshold_key": selected_threshold_key,
        },
        "exit": _build_exit_doc(best_hold, fallback_hold_bars=int(base_settings.exit.hold_bars)),
        "execution": _build_execution_doc(best_execution, fallback_settings=base_settings.execution),
        "hold_grid_results": ranked_holds,
        "execution_grid_results": ranked_execution,
    }


def _build_exit_doc(best_row: dict[str, Any] | None, *, fallback_hold_bars: int) -> dict[str, Any]:
    if not isinstance(best_row, dict):
        return {
            "mode": "hold",
            "recommended_hold_bars": int(fallback_hold_bars),
            "recommendation_source": "manual_fallback",
        }
    return {
        "mode": "hold",
        "recommended_hold_bars": int(best_row["grid_point"]["hold_bars"]),
        "recommendation_source": "execution_backtest_grid_search",
        "objective_score": float(best_row.get("utility_total", 0.0)),
        "wins": int(best_row.get("wins", 0)),
        "losses": int(best_row.get("losses", 0)),
        "comparable_pairs": int(best_row.get("comparable_pairs", 0)),
        "summary": dict(best_row.get("summary", {})),
        "grid_point": dict(best_row.get("grid_point", {})),
    }


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


def _rank_execution_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = [dict(item) for item in rows if isinstance(item, dict) and isinstance(item.get("summary"), dict)]
    for row in normalized:
        row.setdefault("wins", 0)
        row.setdefault("losses", 0)
        row.setdefault("holds", 0)
        row.setdefault("comparable_pairs", 0)
        row.setdefault("utility_total", 0.0)

    for index, left in enumerate(normalized):
        for other_index, right in enumerate(normalized):
            if index == other_index:
                continue
            compare_doc = compare_execution_balanced_pareto(left.get("summary", {}), right.get("summary", {}))
            if not bool(compare_doc.get("comparable")):
                continue
            left["comparable_pairs"] = int(left.get("comparable_pairs", 0)) + 1
            decision = str(compare_doc.get("decision", "")).strip().lower()
            if decision == "candidate_edge":
                left["wins"] = int(left.get("wins", 0)) + 1
            elif decision == "champion_edge":
                left["losses"] = int(left.get("losses", 0)) + 1
            else:
                left["holds"] = int(left.get("holds", 0)) + 1
            left["utility_total"] = float(left.get("utility_total", 0.0)) + float(compare_doc.get("utility_score", 0.0))

    normalized.sort(
        key=lambda item: (
            int(item.get("wins", 0)),
            -int(item.get("losses", 0)),
            float(item.get("utility_total", 0.0)),
            float((item.get("summary") or {}).get("realized_pnl_quote", 0.0)),
            float((item.get("summary") or {}).get("fill_rate", 0.0)),
            -float((item.get("summary") or {}).get("max_drawdown_pct", 0.0)),
            -float((item.get("summary") or {}).get("slippage_bps_mean", 0.0)),
        ),
        reverse=True,
    )
    return normalized


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


def _safe_text(value: Any, default: str) -> str:
    text = str(value).strip()
    return text or str(default)
