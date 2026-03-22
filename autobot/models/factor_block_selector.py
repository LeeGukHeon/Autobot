"""Economically-significant factor block selection for v4 research and training."""

from __future__ import annotations

from dataclasses import dataclass
import json
import math
from pathlib import Path
from typing import Any, Sequence

import numpy as np

from autobot.features.feature_blocks_v4_live_base import (
    base_feature_columns_v4_live_base,
    high_tf_feature_columns_v4_live_base,
    micro_feature_columns_v4_live_base,
    one_m_feature_columns_v4_live_base,
)
from autobot.features.feature_set_v4 import (
    interaction_feature_columns_v4,
    order_flow_feature_columns_v4,
    periodicity_feature_columns_v4,
    spillover_breadth_feature_columns_v4,
    trend_volume_feature_columns_v4,
)

from .registry import load_json
from .selection_optimizer import SelectionGridConfig, build_window_selection_objectives
from .train_v1 import _evaluate_split, _predict_scores


_MODE_OFF = "off"
_MODE_REPORT_ONLY = "report_only"
_MODE_USE_LATEST = "use_latest"
_MODE_GUARDED_AUTO = "guarded_auto"
_DEFAULT_SELECTION_THRESHOLD_KEY = "top_5pct"
_LATEST_SELECTOR_POINTER = "latest_factor_block_selection.json"
_LATEST_SELECTOR_POLICY = "latest_factor_block_policy.json"
_SELECTOR_HISTORY_PATH = "factor_block_selection_history.jsonl"
_DEFAULT_RUN_SCOPE = "scheduled_daily"
_DEFAULT_POLICY_HISTORY_WINDOW_RUNS = 8
_DEFAULT_POLICY_MIN_ELIGIBLE_RUNS = 4
_DEFAULT_POLICY_MIN_ACCEPT_RATIO = 0.75
_DEFAULT_POLICY_MIN_POSITIVE_DELTA_RATIO = 0.60
_DEFAULT_POLICY_MIN_MEAN_DELTA_EV = 0.0
_DEFAULT_POLICY_MAX_COVERAGE_COST_PROXY = 0.35
_DEFAULT_POLICY_MAX_TURNOVER_COST_PROXY = 0.60
_DEFAULT_POLICY_MIN_OPTIONAL_BLOCKS = 1
_EVIDENCE_MODE_MEDIAN_ABLATION = "median_ablation"
_EVIDENCE_MODE_REFIT_DROP_BLOCK = "refit_drop_block"


@dataclass(frozen=True)
class FactorBlockDefinition:
    block_id: str
    label: str
    feature_columns: tuple[str, ...]
    protected: bool
    source_contracts: tuple[str, ...]


def normalize_factor_block_selection_mode(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"", _MODE_OFF, "false", "0", "disabled"}:
        return _MODE_OFF
    if raw in {"report", "report_only", "summary"}:
        return _MODE_REPORT_ONLY
    if raw in {"use_latest", "latest", "apply_latest"}:
        return _MODE_USE_LATEST
    if raw in {"guarded_auto", "guarded", "auto", "policy_auto"}:
        return _MODE_GUARDED_AUTO
    raise ValueError("factor_block_selection_mode must be one of off|report_only|use_latest|guarded_auto")


def normalize_run_scope(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return _DEFAULT_RUN_SCOPE
    normalized = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in raw)
    normalized = normalized.strip("_-")
    return normalized or _DEFAULT_RUN_SCOPE


def v4_factor_block_registry(
    *,
    feature_columns: Sequence[str],
    high_tfs: tuple[str, ...] = ("15m", "60m", "240m"),
) -> list[FactorBlockDefinition]:
    selected = {str(item).strip() for item in feature_columns if str(item).strip()}

    raw_blocks = (
        FactorBlockDefinition(
            block_id="v3_base_core",
            label="v4 live base core",
            feature_columns=tuple(col for col in base_feature_columns_v4_live_base() if col in selected),
            protected=True,
            source_contracts=("feature_blocks_v4_live_base.base",),
        ),
        FactorBlockDefinition(
            block_id="v3_one_m_core",
            label="v4 live one-minute core",
            feature_columns=tuple(col for col in one_m_feature_columns_v4_live_base() if col in selected),
            protected=True,
            source_contracts=("feature_blocks_v4_live_base.one_m",),
        ),
        FactorBlockDefinition(
            block_id="v3_high_tf_core",
            label="v4 live multi-timeframe core",
            feature_columns=tuple(col for col in high_tf_feature_columns_v4_live_base(high_tfs=high_tfs) if col in selected),
            protected=True,
            source_contracts=("feature_blocks_v4_live_base.high_tf",),
        ),
        FactorBlockDefinition(
            block_id="v3_micro_core",
            label="v4 live micro core",
            feature_columns=tuple(col for col in micro_feature_columns_v4_live_base() if col in selected),
            protected=True,
            source_contracts=("feature_blocks_v4_live_base.micro",),
        ),
        FactorBlockDefinition(
            block_id="v4_spillover_breadth",
            label="v4 spillover breadth",
            feature_columns=tuple(col for col in spillover_breadth_feature_columns_v4() if col in selected),
            protected=False,
            source_contracts=("feature_set_v4.spillover_breadth",),
        ),
        FactorBlockDefinition(
            block_id="v4_periodicity",
            label="v4 periodicity",
            feature_columns=tuple(col for col in periodicity_feature_columns_v4() if col in selected),
            protected=False,
            source_contracts=("feature_set_v4.periodicity",),
        ),
        FactorBlockDefinition(
            block_id="v4_trend_volume",
            label="v4 trend volume",
            feature_columns=tuple(col for col in trend_volume_feature_columns_v4() if col in selected),
            protected=False,
            source_contracts=("feature_set_v4.trend_volume",),
        ),
        FactorBlockDefinition(
            block_id="v4_order_flow_panel_v1",
            label="v4 order flow panel",
            feature_columns=tuple(col for col in order_flow_feature_columns_v4() if col in selected),
            protected=False,
            source_contracts=("order_flow_panel_v1",),
        ),
        FactorBlockDefinition(
            block_id="v4_interactions",
            label="v4 interactions",
            feature_columns=tuple(col for col in interaction_feature_columns_v4() if col in selected),
            protected=False,
            source_contracts=("feature_set_v4.interactions",),
        ),
    )
    return [block for block in raw_blocks if block.feature_columns]


def resolve_selected_feature_columns_from_latest(
    *,
    registry_root: Path,
    model_family: str,
    mode: str,
    run_scope: str | None = None,
    all_feature_columns: Sequence[str],
    high_tfs: tuple[str, ...] = ("15m", "60m", "240m"),
) -> tuple[tuple[str, ...], dict[str, Any]]:
    normalized_mode = normalize_factor_block_selection_mode(mode)
    normalized_scope = normalize_run_scope(run_scope)
    full_cols = tuple(str(item).strip() for item in all_feature_columns if str(item).strip())
    registry = v4_factor_block_registry(feature_columns=full_cols, high_tfs=high_tfs)
    block_map = {block.block_id: block for block in registry}
    base_context: dict[str, Any] = {
        "mode": normalized_mode,
        "run_scope": normalized_scope,
        "registry_blocks": [serialize_factor_block_definition(block) for block in registry],
        "applied": False,
        "resolved_run_id": "",
        "resolution_source": "full_set",
        "reasons": [],
    }
    if normalized_mode == _MODE_GUARDED_AUTO:
        pointer = load_json(registry_root / model_family / _scoped_filename(_LATEST_SELECTOR_POLICY, normalized_scope))
        if not isinstance(pointer, dict) or not pointer:
            base_context["reasons"] = ["MISSING_LATEST_FACTOR_BLOCK_POLICY"]
            return full_cols, base_context
        base_context["resolved_run_id"] = str(pointer.get("updated_by_run_id", "")).strip()
        base_context["policy_status"] = str(((pointer.get("summary") or {}).get("status", ""))).strip()
        if not bool(pointer.get("apply_pruned_feature_set", False)):
            base_context["reasons"] = [str(item) for item in (pointer.get("policy_reasons") or []) if str(item).strip()]
            if not base_context["reasons"]:
                base_context["reasons"] = ["GUARDED_POLICY_NOT_ACTIVE"]
            return full_cols, base_context
        selected_blocks = [
            str(item).strip()
            for item in (pointer.get("accepted_blocks") or [])
            if str(item).strip() and str(item).strip() in block_map
        ]
        selected_feature_columns = tuple(
            col
            for block_id in selected_blocks
            for col in block_map[block_id].feature_columns
        )
        if not selected_feature_columns or tuple(selected_feature_columns) == full_cols:
            base_context["reasons"] = ["GUARDED_POLICY_RESOLVED_TO_FULL_SET"]
            return full_cols, base_context
        base_context["applied"] = True
        base_context["resolution_source"] = "guarded_policy"
        base_context["accepted_blocks"] = list(selected_blocks)
        base_context["selected_feature_columns"] = list(selected_feature_columns)
        base_context["policy_reasons"] = [str(item) for item in (pointer.get("policy_reasons") or []) if str(item).strip()]
        return selected_feature_columns, base_context

    if normalized_mode != _MODE_USE_LATEST:
        base_context["reasons"] = ["MODE_DOES_NOT_PRUNE"]
        return full_cols, base_context

    pointer = load_json(registry_root / model_family / _scoped_filename(_LATEST_SELECTOR_POINTER, normalized_scope))
    if not isinstance(pointer, dict) or not pointer:
        base_context["reasons"] = ["MISSING_LATEST_FACTOR_BLOCK_SELECTION"]
        return full_cols, base_context
    selected_blocks = [
        str(item).strip()
        for item in (pointer.get("accepted_blocks") or [])
        if str(item).strip() and str(item).strip() in block_map
    ]
    selected_feature_columns = tuple(
        col
        for block_id in selected_blocks
        for col in block_map[block_id].feature_columns
    )
    if not selected_feature_columns:
        base_context["reasons"] = ["EMPTY_ACCEPTED_BLOCK_SET"]
        base_context["resolved_run_id"] = str(pointer.get("run_id", "")).strip()
        return full_cols, base_context
    base_context["applied"] = True
    base_context["resolved_run_id"] = str(pointer.get("run_id", "")).strip()
    base_context["resolution_source"] = "latest_selector"
    base_context["accepted_blocks"] = list(selected_blocks)
    base_context["selected_feature_columns"] = list(selected_feature_columns)
    return selected_feature_columns, base_context


def build_factor_block_window_baseline(
    *,
    scores: np.ndarray,
    y_reg: np.ndarray,
    ts_ms: np.ndarray,
    thresholds: dict[str, Any],
    fee_bps_est: float,
    safety_bps: float,
    threshold_key: str = _DEFAULT_SELECTION_THRESHOLD_KEY,
) -> dict[str, Any] | None:
    threshold_value = _safe_optional_float(thresholds.get(threshold_key))
    if threshold_value is None:
        return None
    selection_config = SelectionGridConfig()
    full_selection_doc = build_window_selection_objectives(
        scores=scores,
        y_reg=y_reg,
        ts_ms=ts_ms,
        thresholds={threshold_key: threshold_value},
        fee_bps_est=fee_bps_est,
        safety_bps=safety_bps,
        config=selection_config,
    )
    full_threshold_doc = (full_selection_doc.get("by_threshold_key") or {}).get(threshold_key, {})
    full_grid_choice = _choose_window_grid_choice(full_threshold_doc.get("grid_results"))
    if full_grid_choice is None:
        return None
    full_signature = _selection_signature(
        scores=scores,
        y_reg=y_reg,
        ts_ms=ts_ms,
        threshold=float(threshold_value),
        top_pct=float(full_grid_choice["top_pct"]),
        min_candidates=int(full_grid_choice["min_candidates_per_ts"]),
        fee_frac=float(fee_bps_est + safety_bps) / 10_000.0,
    )
    return {
        "threshold_key": str(threshold_key),
        "threshold_value": float(threshold_value),
        "top_pct": float(full_grid_choice["top_pct"]),
        "min_candidates_per_ts": int(full_grid_choice["min_candidates_per_ts"]),
        "selection_profile": {
            "threshold_key": str(threshold_key),
            "threshold_value": float(threshold_value),
            "top_pct": float(full_grid_choice["top_pct"]),
            "min_candidates_per_ts": int(full_grid_choice["min_candidates_per_ts"]),
        },
        "selection_signature": full_signature,
    }


def build_factor_block_window_row(
    *,
    window_index: int,
    block: FactorBlockDefinition,
    feature_count: int,
    full_top5: dict[str, Any],
    candidate_top5: dict[str, Any],
    full_signature: dict[str, Any],
    candidate_signature: dict[str, Any],
    selection_profile: dict[str, Any],
    evidence_mode: str,
    diagnostic_only: bool,
) -> dict[str, Any]:
    return {
        "window_index": int(window_index),
        "block_id": block.block_id,
        "feature_count": int(feature_count),
        "delta_ev_net_top5": _safe_float(full_top5.get("ev_net")) - _safe_float(candidate_top5.get("ev_net")),
        "delta_precision_top5": _safe_float(full_top5.get("precision")) - _safe_float(candidate_top5.get("precision")),
        "coverage_cost_proxy": abs(
            float(full_signature["active_ts_ratio"]) - float(candidate_signature["active_ts_ratio"])
        ),
        "turnover_cost_proxy": _selection_turnover_cost(
            full_selected=full_signature["selected_index_by_ts"],
            ablated_selected=candidate_signature["selected_index_by_ts"],
        ),
        "full_active_ts_ratio": float(full_signature["active_ts_ratio"]),
        "candidate_active_ts_ratio": float(candidate_signature["active_ts_ratio"]),
        "full_selected_rows": int(full_signature["selected_rows"]),
        "candidate_selected_rows": int(candidate_signature["selected_rows"]),
        "selection_profile": dict(selection_profile or {}),
        "evidence_mode": str(evidence_mode).strip() or _EVIDENCE_MODE_MEDIAN_ABLATION,
        "diagnostic_only": bool(diagnostic_only),
    }


def build_factor_block_selection_signature(
    *,
    scores: np.ndarray,
    y_reg: np.ndarray,
    ts_ms: np.ndarray,
    threshold: float,
    top_pct: float,
    min_candidates: int,
    fee_bps_est: float,
    safety_bps: float,
) -> dict[str, Any]:
    return _selection_signature(
        scores=scores,
        y_reg=y_reg,
        ts_ms=ts_ms,
        threshold=float(threshold),
        top_pct=float(top_pct),
        min_candidates=int(min_candidates),
        fee_frac=float(fee_bps_est + safety_bps) / 10_000.0,
    )


def evaluate_factor_block_window_rows(
    *,
    window_index: int,
    model_bundle: dict[str, Any],
    feature_names: Sequence[str],
    x_window: np.ndarray,
    y_cls: np.ndarray,
    y_reg: np.ndarray,
    ts_ms: np.ndarray,
    thresholds: dict[str, Any],
    fee_bps_est: float,
    safety_bps: float,
    block_registry: Sequence[FactorBlockDefinition],
    threshold_key: str = _DEFAULT_SELECTION_THRESHOLD_KEY,
) -> list[dict[str, Any]]:
    if x_window.size <= 0 or len(feature_names) <= 0:
        return []
    feature_index = {str(name): idx for idx, name in enumerate(feature_names)}
    full_scores = _predict_scores(model_bundle, x_window)
    full_metrics = _evaluate_split(
        y_cls=y_cls,
        y_reg=y_reg,
        scores=full_scores,
        markets=np.array(["_ALL_"] * int(len(y_cls)), dtype=object),
        fee_bps_est=fee_bps_est,
        safety_bps=safety_bps,
    )
    baseline = build_factor_block_window_baseline(
        scores=full_scores,
        y_reg=y_reg,
        ts_ms=ts_ms,
        thresholds=thresholds,
        fee_bps_est=fee_bps_est,
        safety_bps=safety_bps,
        threshold_key=threshold_key,
    )
    if baseline is None:
        return []
    full_signature = dict(baseline.get("selection_signature") or {})
    full_top5 = ((full_metrics.get("trading", {}) or {}).get("top_5pct", {})) if isinstance(full_metrics, dict) else {}

    rows: list[dict[str, Any]] = []
    for block in block_registry:
        indices = [feature_index[name] for name in block.feature_columns if name in feature_index]
        if not indices:
            continue
        x_ablated = np.array(x_window, copy=True)
        medians = np.nanmedian(x_window[:, np.asarray(indices, dtype=np.int64)], axis=0)
        medians = np.nan_to_num(np.asarray(medians, dtype=np.float32), nan=0.0, posinf=0.0, neginf=0.0)
        x_ablated[:, np.asarray(indices, dtype=np.int64)] = medians
        ablated_scores = _predict_scores(model_bundle, x_ablated)
        ablated_metrics = _evaluate_split(
            y_cls=y_cls,
            y_reg=y_reg,
            scores=ablated_scores,
            markets=np.array(["_ALL_"] * int(len(y_cls)), dtype=object),
            fee_bps_est=fee_bps_est,
            safety_bps=safety_bps,
        )
        ablated_top5 = (
            ((ablated_metrics.get("trading", {}) or {}).get("top_5pct", {}))
            if isinstance(ablated_metrics, dict)
            else {}
        )
        ablated_signature = _selection_signature(
            scores=ablated_scores,
            y_reg=y_reg,
            ts_ms=ts_ms,
            threshold=float(baseline["threshold_value"]),
            top_pct=float(baseline["top_pct"]),
            min_candidates=int(baseline["min_candidates_per_ts"]),
            fee_frac=float(fee_bps_est + safety_bps) / 10_000.0,
        )
        rows.append(
            build_factor_block_window_row(
                window_index=int(window_index),
                block=block,
                feature_count=int(len(indices)),
                full_top5=full_top5,
                candidate_top5=ablated_top5,
                full_signature=full_signature,
                candidate_signature=ablated_signature,
                selection_profile=dict(baseline.get("selection_profile") or {}),
                evidence_mode=_EVIDENCE_MODE_MEDIAN_ABLATION,
                diagnostic_only=True,
            )
        )
    return rows


def build_factor_block_refit_support_summary(
    *,
    block_registry: Sequence[FactorBlockDefinition],
    window_support: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    windows = [dict(item) for item in window_support if isinstance(item, dict)]
    optional_blocks = [block for block in block_registry if not bool(block.protected)]
    aggregated_by_block: dict[str, Any] = {}
    aggregate_reasons: list[str] = []
    windows_with_rows = 0
    windows_without_rows = 0

    for window_doc in windows:
        rows_emitted = int(window_doc.get("optional_blocks_with_rows", 0) or 0)
        if rows_emitted > 0:
            windows_with_rows += 1
        else:
            windows_without_rows += 1
        aggregate_reasons.extend(
            str(item).strip()
            for item in (window_doc.get("reason_codes") or [])
            if str(item).strip()
        )

    for block in optional_blocks:
        support_rows = []
        for window_doc in windows:
            block_doc = (window_doc.get("by_block") or {}).get(block.block_id)
            if isinstance(block_doc, dict):
                support_rows.append(dict(block_doc))
        rows_emitted = int(sum(int(item.get("rows_emitted", 0) or 0) for item in support_rows))
        windows_considered = int(len(support_rows))
        windows_with_block_rows = int(sum(1 for item in support_rows if int(item.get("rows_emitted", 0) or 0) > 0))
        reason_codes = _dedupe_reason_codes(
            str(item).strip()
            for row in support_rows
            for item in (row.get("reason_codes") or [])
            if str(item).strip()
        )
        if windows_considered <= 0:
            status = "insufficient"
            if not reason_codes:
                reason_codes = ["NO_WINDOW_REFIT_SUPPORT"]
        elif rows_emitted <= 0:
            status = "insufficient"
        elif windows_with_block_rows < windows_considered:
            status = "partial"
        else:
            status = "supported"
        aggregated_by_block[block.block_id] = {
            "block_id": block.block_id,
            "label": block.label,
            "feature_count": int(len(block.feature_columns)),
            "windows_considered": windows_considered,
            "windows_with_rows": windows_with_block_rows,
            "rows_emitted": rows_emitted,
            "status": status,
            "reason_codes": reason_codes,
        }

    optional_blocks_with_rows = int(
        sum(1 for item in aggregated_by_block.values() if int(item.get("rows_emitted", 0) or 0) > 0)
    )
    if not optional_blocks:
        status = "not_applicable"
        aggregate_reasons = ["NO_OPTIONAL_BLOCKS"]
    elif optional_blocks_with_rows <= 0:
        status = "insufficient"
    elif optional_blocks_with_rows < len(optional_blocks):
        status = "partial"
    else:
        status = "supported"

    if not aggregate_reasons and status == "supported":
        aggregate_reasons = ["BOUNDED_REFIT_SUPPORT_AVAILABLE"]

    return {
        "policy": "bounded_drop_block_refit_v1",
        "bound_mode": "reuse_window_best_params",
        "summary": {
            "status": status,
            "windows_recorded": int(len(windows)),
            "windows_with_rows": int(windows_with_rows),
            "windows_without_rows": int(windows_without_rows),
            "optional_block_count": int(len(optional_blocks)),
            "optional_blocks_with_rows": int(optional_blocks_with_rows),
            "optional_blocks_without_rows": int(max(len(optional_blocks) - optional_blocks_with_rows, 0)),
            "reason_codes": _dedupe_reason_codes(aggregate_reasons),
        },
        "by_block": aggregated_by_block,
        "windows": windows,
    }


def build_factor_block_selection_report(
    *,
    block_registry: Sequence[FactorBlockDefinition],
    window_rows: Sequence[dict[str, Any]],
    selection_mode: str,
    feature_columns: Sequence[str],
    run_id: str,
    refit_support: dict[str, Any] | None = None,
) -> dict[str, Any]:
    universe = [serialize_factor_block_definition(block) for block in block_registry]
    normalized_mode = normalize_factor_block_selection_mode(selection_mode)
    rows = [dict(item) for item in window_rows if isinstance(item, dict)]
    refit_support_doc = (
        dict(refit_support)
        if isinstance(refit_support, dict) and refit_support
        else build_factor_block_refit_support_summary(block_registry=block_registry, window_support=[])
    )
    refit_support_by_block = dict(refit_support_doc.get("by_block") or {})
    refit_support_summary = dict(refit_support_doc.get("summary") or {})
    windows_evaluated = sorted({int(item.get("window_index", -1)) for item in rows if int(item.get("window_index", -1)) >= 0})
    weak_sample = len(windows_evaluated) < 2
    weak_reasons = ["INSUFFICIENT_WINDOWS_FOR_PRUNING"] if weak_sample else []

    accepted: list[str] = []
    rejected: list[str] = []
    decisions: dict[str, Any] = {}
    optional_refit_certified_count = 0
    for block in block_registry:
        block_rows = [item for item in rows if str(item.get("block_id", "")).strip() == block.block_id]
        available_evidence_modes = sorted(
            {
                str(item.get("evidence_mode", _EVIDENCE_MODE_MEDIAN_ABLATION)).strip() or _EVIDENCE_MODE_MEDIAN_ABLATION
                for item in block_rows
            }
        )
        refit_rows = [
            item
            for item in block_rows
            if (str(item.get("evidence_mode", _EVIDENCE_MODE_MEDIAN_ABLATION)).strip() or _EVIDENCE_MODE_MEDIAN_ABLATION)
            == _EVIDENCE_MODE_REFIT_DROP_BLOCK
        ]
        aggregate_rows = refit_rows if refit_rows else block_rows
        aggregate = _aggregate_block_rows(aggregate_rows)
        reason_codes: list[str] = []
        status = "accepted"
        evidence_mode_used = _EVIDENCE_MODE_REFIT_DROP_BLOCK if refit_rows else (
            available_evidence_modes[0] if available_evidence_modes else "none"
        )
        refit_certified = bool(refit_rows) and not bool(block.protected)
        support_reason_codes = [
            str(item).strip()
            for item in ((refit_support_by_block.get(block.block_id) or {}).get("reason_codes") or [])
            if str(item).strip()
        ]
        if block.protected:
            reason_codes.append("PROTECTED_BASE_BLOCK")
        elif weak_sample:
            reason_codes.append("INSUFFICIENT_SAMPLE_KEEP_FULL_SET")
            reason_codes.extend(support_reason_codes)
        elif not refit_rows:
            reason_codes.append("NO_REFIT_EVIDENCE_KEEP_FULL_SET")
            reason_codes.extend(support_reason_codes)
            if _EVIDENCE_MODE_MEDIAN_ABLATION in available_evidence_modes:
                reason_codes.append("MEDIAN_ABLATION_DIAGNOSTIC_ONLY")
        else:
            if aggregate["delta_ev_net_top5_mean"] <= 0.0:
                status = "rejected"
                reason_codes.append("ECONOMIC_EDGE_NONPOSITIVE")
            if aggregate["positive_delta_ev_ratio"] < 0.50:
                status = "rejected"
                reason_codes.append("UNSTABLE_OOS_EDGE")
            if not reason_codes:
                reason_codes.append("REFIT_CERTIFIED_ECONOMIC_EDGE_POSITIVE")
            if aggregate["coverage_cost_proxy_mean"] > 0.25:
                reason_codes.append("COVERAGE_SHIFT_HIGH")
            if aggregate["turnover_cost_proxy_mean"] > 0.50:
                reason_codes.append("TURNOVER_SHIFT_HIGH")
        if refit_certified:
            optional_refit_certified_count += 1
        decisions[block.block_id] = {
            "block_id": block.block_id,
            "label": block.label,
            "protected": bool(block.protected),
            "feature_count": int(len(block.feature_columns)),
            "feature_columns": list(block.feature_columns),
            "source_contracts": list(block.source_contracts),
            "status": status,
            "reason_codes": reason_codes,
            "available_evidence_modes": available_evidence_modes,
            "evidence_mode_used": evidence_mode_used,
            "refit_certified": bool(refit_certified),
            "evidence_row_count": int(len(aggregate_rows)),
            "diagnostic_row_count": int(max(len(block_rows) - len(refit_rows), 0)),
            "contribution_summary": aggregate,
        }
        if status == "accepted":
            accepted.append(block.block_id)
        else:
            rejected.append(block.block_id)

    selected_feature_columns = tuple(
        col
        for block in block_registry
        if block.block_id in set(accepted)
        for col in block.feature_columns
    )
    summary_status = "trusted"
    if weak_sample:
        summary_status = "insufficient"
    elif normalized_mode == _MODE_REPORT_ONLY:
        summary_status = "report_only"
    report = {
        "version": 2,
        "policy": "economically_significant_factor_selector_v2",
        "run_id": str(run_id),
        "selection_mode": normalized_mode,
        "evidence_contract": {
            "required_for_optional_block_rejection": _EVIDENCE_MODE_REFIT_DROP_BLOCK,
            "median_ablation_diagnostic_only": True,
        },
        "sample_support": {
            "windows_evaluated": int(len(windows_evaluated)),
            "window_indices": list(windows_evaluated),
            "weak_sample": bool(weak_sample),
            "weak_sample_reasons": weak_reasons,
        },
        "candidate_block_universe": universe,
        "accepted_blocks": accepted,
        "rejected_blocks": rejected,
        "decision_by_block": decisions,
        "selected_feature_columns": list(selected_feature_columns),
        "selected_feature_column_count": int(len(selected_feature_columns)),
        "summary": {
            "status": summary_status,
            "accepted_block_count": int(len(accepted)),
            "rejected_block_count": int(len(rejected)),
            "windows_evaluated": int(len(windows_evaluated)),
            "weak_sample": bool(weak_sample),
            "optional_refit_certified_block_count": int(optional_refit_certified_count),
            "refit_support_status": str(refit_support_summary.get("status", "")).strip() or "unknown",
            "refit_windows_recorded": int(refit_support_summary.get("windows_recorded", 0) or 0),
        },
        "refit_support": refit_support_doc,
    }
    return report


def write_latest_factor_block_selection_pointer(
    *,
    registry_root: Path,
    model_family: str,
    run_id: str,
    report: dict[str, Any],
    run_scope: str | None = None,
) -> Path | None:
    if not isinstance(report, dict):
        return None
    if normalize_factor_block_selection_mode(report.get("selection_mode")) == _MODE_OFF:
        return None
    selected_blocks = [str(item).strip() for item in (report.get("accepted_blocks") or []) if str(item).strip()]
    selected_feature_columns = [
        str(item).strip()
        for item in (report.get("selected_feature_columns") or [])
        if str(item).strip()
    ]
    if not selected_blocks or not selected_feature_columns:
        return None
    path = registry_root / model_family / _scoped_filename(_LATEST_SELECTOR_POINTER, normalize_run_scope(run_scope))
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_id": str(run_id),
        "accepted_blocks": selected_blocks,
        "selected_feature_columns": selected_feature_columns,
        "summary": dict(report.get("summary") or {}),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def load_factor_block_selection_history(
    *,
    registry_root: Path,
    model_family: str,
    run_scope: str | None = None,
) -> list[dict[str, Any]]:
    path = registry_root / model_family / _scoped_filename(_SELECTOR_HISTORY_PATH, normalize_run_scope(run_scope))
    if not path.exists():
        return []
    deduped: dict[str, dict[str, Any]] = {}
    ordered: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = str(line).strip()
        if not text:
            continue
        try:
            record = json.loads(text)
        except json.JSONDecodeError:
            continue
        if not isinstance(record, dict):
            continue
        run_id = str(record.get("run_id", "")).strip()
        if not run_id:
            continue
        if run_id not in deduped:
            ordered.append(run_id)
        deduped[run_id] = record
    return [deduped[run_id] for run_id in ordered]


def append_factor_block_selection_history(
    *,
    registry_root: Path,
    model_family: str,
    report: dict[str, Any],
    run_scope: str | None = None,
) -> Path | None:
    if not isinstance(report, dict) or not str(report.get("run_id", "")).strip():
        return None
    if normalize_factor_block_selection_mode(report.get("selection_mode")) == _MODE_OFF:
        return None
    normalized_scope = normalize_run_scope(run_scope)
    path = registry_root / model_family / _scoped_filename(_SELECTOR_HISTORY_PATH, normalized_scope)
    records = load_factor_block_selection_history(
        registry_root=registry_root,
        model_family=model_family,
        run_scope=normalized_scope,
    )
    compact = _compact_factor_block_history_record(report)
    run_id = str(compact.get("run_id", "")).strip()
    records = [record for record in records if str(record.get("run_id", "")).strip() != run_id]
    records.append(compact)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "\n".join(json.dumps(record, ensure_ascii=False, sort_keys=True) for record in records)
    path.write_text((payload + "\n") if payload else "", encoding="utf-8")
    return path


def build_guarded_factor_block_policy(
    *,
    block_registry: Sequence[FactorBlockDefinition],
    history_records: Sequence[dict[str, Any]],
    history_window_runs: int = _DEFAULT_POLICY_HISTORY_WINDOW_RUNS,
    min_eligible_runs: int = _DEFAULT_POLICY_MIN_ELIGIBLE_RUNS,
    min_accept_ratio: float = _DEFAULT_POLICY_MIN_ACCEPT_RATIO,
    min_positive_delta_ratio: float = _DEFAULT_POLICY_MIN_POSITIVE_DELTA_RATIO,
    min_mean_delta_ev: float = _DEFAULT_POLICY_MIN_MEAN_DELTA_EV,
    max_coverage_cost_proxy: float = _DEFAULT_POLICY_MAX_COVERAGE_COST_PROXY,
    max_turnover_cost_proxy: float = _DEFAULT_POLICY_MAX_TURNOVER_COST_PROXY,
    min_optional_blocks: int = _DEFAULT_POLICY_MIN_OPTIONAL_BLOCKS,
) -> dict[str, Any]:
    window_runs = max(int(history_window_runs), 1)
    records = [dict(item) for item in history_records if isinstance(item, dict)]
    recent_records = records[-window_runs:]
    eligible_records = [
        record
        for record in recent_records
        if not bool(((record.get("sample_support") or {}).get("weak_sample", False)))
        and int((record.get("sample_support") or {}).get("windows_evaluated", 0) or 0) >= 2
    ]
    full_feature_columns = tuple(
        col
        for block in block_registry
        for col in block.feature_columns
    )
    block_policy: dict[str, Any] = {}
    accepted_blocks: list[str] = []
    rejected_blocks: list[str] = []

    for block in block_registry:
        stats = _summarize_history_for_block(block=block, history_records=eligible_records)
        reason_codes: list[str] = []
        accepted = True
        if block.protected:
            reason_codes.append("PROTECTED_BASE_BLOCK")
        else:
            if int(stats["refit_certified_record_count"]) < max(int(min_eligible_runs), 1):
                accepted = True
                reason_codes.append("INSUFFICIENT_REFIT_HISTORY_KEEP_FULL_SET")
            else:
                if float(stats["accept_ratio"]) < float(min_accept_ratio):
                    accepted = False
                    reason_codes.append("ACCEPT_RATIO_TOO_LOW")
                if float(stats["mean_delta_ev_net_top5"]) <= float(min_mean_delta_ev):
                    accepted = False
                    reason_codes.append("MEAN_DELTA_EV_NONPOSITIVE")
                if float(stats["mean_positive_delta_ev_ratio"]) < float(min_positive_delta_ratio):
                    accepted = False
                    reason_codes.append("POSITIVE_DELTA_RATIO_TOO_LOW")
                if float(stats["mean_coverage_cost_proxy"]) > float(max_coverage_cost_proxy):
                    accepted = False
                    reason_codes.append("COVERAGE_SHIFT_TOO_HIGH")
                if float(stats["mean_turnover_cost_proxy"]) > float(max_turnover_cost_proxy):
                    accepted = False
                    reason_codes.append("TURNOVER_SHIFT_TOO_HIGH")
                if accepted:
                    reason_codes.append("GUARDED_REFIT_HISTORY_ACCEPTED")
        block_policy[block.block_id] = {
            "block_id": block.block_id,
            "label": block.label,
            "protected": bool(block.protected),
            "feature_count": int(len(block.feature_columns)),
            "reason_codes": reason_codes,
            "history_summary": stats,
            "status": "accepted" if accepted else "rejected",
        }
        if accepted:
            accepted_blocks.append(block.block_id)
        else:
            rejected_blocks.append(block.block_id)

    optional_accepted = [block_id for block_id in accepted_blocks if not bool(block_policy[block_id]["protected"])]
    optional_rejected = [block_id for block_id in rejected_blocks if not bool(block_policy[block_id]["protected"])]
    policy_reasons: list[str] = []
    status = "warming"
    apply_pruned_feature_set = False

    if len(eligible_records) < max(int(min_eligible_runs), 1):
        policy_reasons.append("INSUFFICIENT_ELIGIBLE_HISTORY")
    elif not optional_rejected:
        status = "stable_full"
        policy_reasons.append("NO_PRUNING_SIGNAL")
    elif len(optional_accepted) < max(int(min_optional_blocks), 0):
        policy_reasons.append("OPTIONAL_BLOCK_SET_TOO_SMALL")
    else:
        status = "stable"
        apply_pruned_feature_set = True
        policy_reasons.append("GUARDED_AUTO_POLICY_ACTIVE")

    selected_feature_columns = tuple(
        col
        for block in block_registry
        if block.block_id in set(accepted_blocks)
        for col in block.feature_columns
    )
    if tuple(selected_feature_columns) == tuple(full_feature_columns):
        apply_pruned_feature_set = False
        if status == "stable":
            status = "stable_full"
            if "NO_PRUNING_SIGNAL" not in policy_reasons:
                policy_reasons.append("NO_PRUNING_SIGNAL")

    return {
        "version": 2,
        "policy": "economically_significant_factor_selector_guarded_auto_v2",
        "history_window_runs": int(window_runs),
        "evidence_contract": {
            "required_for_optional_block_rejection": _EVIDENCE_MODE_REFIT_DROP_BLOCK,
            "median_ablation_diagnostic_only": True,
        },
        "criteria": {
            "min_eligible_runs": int(max(int(min_eligible_runs), 1)),
            "min_accept_ratio": float(min_accept_ratio),
            "min_positive_delta_ev_ratio": float(min_positive_delta_ratio),
            "min_mean_delta_ev_net_top5": float(min_mean_delta_ev),
            "max_coverage_cost_proxy_mean": float(max_coverage_cost_proxy),
            "max_turnover_cost_proxy_mean": float(max_turnover_cost_proxy),
            "min_optional_blocks": int(max(int(min_optional_blocks), 0)),
        },
        "history_support": {
            "records_considered": int(len(recent_records)),
            "eligible_records": int(len(eligible_records)),
            "eligible_run_ids": [str(record.get("run_id", "")).strip() for record in eligible_records if str(record.get("run_id", "")).strip()],
        },
        "accepted_blocks": list(accepted_blocks),
        "rejected_blocks": list(rejected_blocks),
        "selected_feature_columns": list(selected_feature_columns),
        "selected_feature_column_count": int(len(selected_feature_columns)),
        "full_feature_column_count": int(len(full_feature_columns)),
        "apply_pruned_feature_set": bool(apply_pruned_feature_set),
        "policy_reasons": list(dict.fromkeys(reason for reason in policy_reasons if str(reason).strip())),
        "decision_by_block": block_policy,
        "summary": {
            "status": status,
            "apply_pruned_feature_set": bool(apply_pruned_feature_set),
            "accepted_block_count": int(len(accepted_blocks)),
            "rejected_block_count": int(len(rejected_blocks)),
            "optional_accepted_block_count": int(len(optional_accepted)),
            "optional_rejected_block_count": int(len(optional_rejected)),
            "eligible_records": int(len(eligible_records)),
        },
    }


def write_latest_guarded_factor_block_policy(
    *,
    registry_root: Path,
    model_family: str,
    run_id: str,
    policy: dict[str, Any],
    run_scope: str | None = None,
) -> Path | None:
    if not isinstance(policy, dict):
        return None
    path = registry_root / model_family / _scoped_filename(_LATEST_SELECTOR_POLICY, normalize_run_scope(run_scope))
    payload = dict(policy)
    payload["updated_by_run_id"] = str(run_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def serialize_factor_block_definition(block: FactorBlockDefinition) -> dict[str, Any]:
    return {
        "block_id": block.block_id,
        "label": block.label,
        "protected": bool(block.protected),
        "feature_count": int(len(block.feature_columns)),
        "feature_columns": list(block.feature_columns),
        "source_contracts": list(block.source_contracts),
    }


def _compact_factor_block_history_record(report: dict[str, Any]) -> dict[str, Any]:
    decisions = report.get("decision_by_block") or {}
    compact_decisions: dict[str, Any] = {}
    if isinstance(decisions, dict):
        for block_id, decision in decisions.items():
            if not isinstance(decision, dict):
                continue
            compact_decisions[str(block_id)] = {
                "status": str(decision.get("status", "")).strip(),
                "protected": bool(decision.get("protected", False)),
                "available_evidence_modes": [
                    str(item).strip()
                    for item in (decision.get("available_evidence_modes") or [])
                    if str(item).strip()
                ],
                "evidence_mode_used": str(decision.get("evidence_mode_used", "")).strip(),
                "refit_certified": bool(decision.get("refit_certified", False)),
                "reason_codes": [str(item).strip() for item in (decision.get("reason_codes") or []) if str(item).strip()],
                "contribution_summary": dict(decision.get("contribution_summary") or {}),
            }
    return {
        "version": 1,
        "run_id": str(report.get("run_id", "")).strip(),
        "selection_mode": normalize_factor_block_selection_mode(report.get("selection_mode")),
        "summary": dict(report.get("summary") or {}),
        "sample_support": dict(report.get("sample_support") or {}),
        "accepted_blocks": [str(item).strip() for item in (report.get("accepted_blocks") or []) if str(item).strip()],
        "rejected_blocks": [str(item).strip() for item in (report.get("rejected_blocks") or []) if str(item).strip()],
        "selected_feature_column_count": int(report.get("selected_feature_column_count", 0) or 0),
        "decision_by_block": compact_decisions,
    }


def _summarize_history_for_block(
    *,
    block: FactorBlockDefinition,
    history_records: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    relevant = []
    for record in history_records:
        decision = (record.get("decision_by_block") or {}).get(block.block_id)
        if isinstance(decision, dict):
            relevant.append(decision)
    certified = [
        item
        for item in relevant
        if bool(item.get("protected", False)) or bool(item.get("refit_certified", False))
    ]
    if not certified:
        return {
            "eligible_record_count": 0,
            "refit_certified_record_count": 0,
            "uncertified_record_count": int(len(relevant)),
            "accept_ratio": 0.0,
            "mean_delta_ev_net_top5": 0.0,
            "mean_positive_delta_ev_ratio": 0.0,
            "mean_coverage_cost_proxy": 0.0,
            "mean_turnover_cost_proxy": 0.0,
        }
    accepted = np.asarray(
        [1.0 if str(item.get("status", "")).strip() == "accepted" else 0.0 for item in certified],
        dtype=np.float64,
    )
    contribution = [dict(item.get("contribution_summary") or {}) for item in certified]
    return {
        "eligible_record_count": int(len(certified)),
        "refit_certified_record_count": int(len(certified)),
        "uncertified_record_count": int(max(len(relevant) - len(certified), 0)),
        "accept_ratio": float(np.mean(accepted)) if accepted.size > 0 else 0.0,
        "mean_delta_ev_net_top5": float(
            np.mean(np.asarray([_safe_float(item.get("delta_ev_net_top5_mean")) for item in contribution], dtype=np.float64))
        )
        if contribution
        else 0.0,
        "mean_positive_delta_ev_ratio": float(
            np.mean(np.asarray([_safe_float(item.get("positive_delta_ev_ratio")) for item in contribution], dtype=np.float64))
        )
        if contribution
        else 0.0,
        "mean_coverage_cost_proxy": float(
            np.mean(np.asarray([_safe_float(item.get("coverage_cost_proxy_mean")) for item in contribution], dtype=np.float64))
        )
        if contribution
        else 0.0,
        "mean_turnover_cost_proxy": float(
            np.mean(np.asarray([_safe_float(item.get("turnover_cost_proxy_mean")) for item in contribution], dtype=np.float64))
        )
        if contribution
        else 0.0,
    }


def _aggregate_block_rows(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "windows_present": 0,
            "delta_ev_net_top5_mean": 0.0,
            "delta_precision_top5_mean": 0.0,
            "positive_delta_ev_ratio": 0.0,
            "nonnegative_precision_ratio": 0.0,
            "coverage_cost_proxy_mean": 0.0,
            "turnover_cost_proxy_mean": 0.0,
        }
    delta_ev = np.asarray([_safe_float(item.get("delta_ev_net_top5")) for item in rows], dtype=np.float64)
    delta_precision = np.asarray([_safe_float(item.get("delta_precision_top5")) for item in rows], dtype=np.float64)
    coverage = np.asarray([_safe_float(item.get("coverage_cost_proxy")) for item in rows], dtype=np.float64)
    turnover = np.asarray([_safe_float(item.get("turnover_cost_proxy")) for item in rows], dtype=np.float64)
    return {
        "windows_present": int(len(rows)),
        "delta_ev_net_top5_mean": float(np.mean(delta_ev)) if delta_ev.size > 0 else 0.0,
        "delta_precision_top5_mean": float(np.mean(delta_precision)) if delta_precision.size > 0 else 0.0,
        "positive_delta_ev_ratio": float(np.mean(delta_ev > 0.0)) if delta_ev.size > 0 else 0.0,
        "nonnegative_precision_ratio": float(np.mean(delta_precision >= 0.0)) if delta_precision.size > 0 else 0.0,
        "coverage_cost_proxy_mean": float(np.mean(coverage)) if coverage.size > 0 else 0.0,
        "turnover_cost_proxy_mean": float(np.mean(turnover)) if turnover.size > 0 else 0.0,
    }


def _dedupe_reason_codes(values: Sequence[Any]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


def _choose_window_grid_choice(grid_results: Any) -> dict[str, Any] | None:
    rows = [dict(item) for item in (grid_results or []) if isinstance(item, dict)]
    if not rows:
        return None
    feasible = [row for row in rows if bool(row.get("feasible", False))]
    candidate_rows = feasible or rows
    candidate_rows.sort(
        key=lambda row: (
            float(row.get("ev_net", 0.0) or 0.0),
            float(row.get("active_ts_ratio", 0.0) or 0.0),
            float(row.get("selected_rows", 0.0) or 0.0),
            -float(row.get("top_pct", 1.0) or 1.0),
            -float(row.get("min_candidates_per_ts", 0) or 0),
        ),
        reverse=True,
    )
    return candidate_rows[0]


def _selection_signature(
    *,
    scores: np.ndarray,
    y_reg: np.ndarray,
    ts_ms: np.ndarray,
    threshold: float,
    top_pct: float,
    min_candidates: int,
    fee_frac: float,
) -> dict[str, Any]:
    score_values = np.asarray(scores, dtype=np.float64)
    reg_values = np.asarray(y_reg, dtype=np.float64)
    ts_values = np.asarray(ts_ms, dtype=np.int64)
    by_ts = _group_indices_by_ts(ts_values)
    total_ts_count = max(len(by_ts), 1)
    active_ts_count = 0
    selected_rows = 0
    selected_values: list[float] = []
    selected_index_by_ts: dict[int, tuple[int, ...]] = {}
    for ts_value, indices in by_ts:
        window_scores = score_values[indices]
        eligible_local = np.flatnonzero(window_scores >= threshold)
        eligible_count = int(eligible_local.size)
        if eligible_count < int(min_candidates):
            selected_index_by_ts[int(ts_value)] = ()
            continue
        select_count = int(np.floor(float(eligible_count) * float(top_pct)))
        if select_count <= 0:
            selected_index_by_ts[int(ts_value)] = ()
            continue
        select_count = min(select_count, eligible_count)
        if select_count >= eligible_count:
            selected_local = eligible_local
        else:
            eligible_scores = window_scores[eligible_local]
            selected_slice = np.argpartition(eligible_scores, -select_count)[-select_count:]
            selected_local = eligible_local[selected_slice]
        selected_indices = tuple(int(indices[item]) for item in np.asarray(selected_local, dtype=np.int64).tolist())
        selected_index_by_ts[int(ts_value)] = tuple(sorted(selected_indices))
        if not selected_indices:
            continue
        selected_reg = np.asarray(reg_values[list(selected_indices)], dtype=np.float64)
        active_ts_count += 1
        selected_rows += int(selected_reg.size)
        selected_values.extend(float(value) for value in selected_reg.tolist())
    mean_y_reg_selected = float(np.mean(selected_values)) if selected_values else 0.0
    return {
        "selected_rows": int(selected_rows),
        "active_ts_ratio": float(active_ts_count) / float(total_ts_count),
        "mean_y_reg_selected": float(mean_y_reg_selected),
        "ev_net": float(mean_y_reg_selected - fee_frac) if selected_values else -float(fee_frac),
        "selected_index_by_ts": selected_index_by_ts,
    }


def _group_indices_by_ts(ts_ms: np.ndarray) -> list[tuple[int, np.ndarray]]:
    if ts_ms.size <= 0:
        return []
    unique, inverse = np.unique(ts_ms.astype(np.int64, copy=False), return_inverse=True)
    out: list[tuple[int, np.ndarray]] = []
    for idx, ts_value in enumerate(unique):
        out.append((int(ts_value), np.flatnonzero(inverse == idx).astype(np.int64, copy=False)))
    return out


def _selection_turnover_cost(
    *,
    full_selected: dict[int, tuple[int, ...]],
    ablated_selected: dict[int, tuple[int, ...]],
) -> float:
    ts_keys = sorted(set(full_selected.keys()) | set(ablated_selected.keys()))
    if not ts_keys:
        return 0.0
    costs: list[float] = []
    for ts_key in ts_keys:
        left = set(full_selected.get(ts_key, ()))
        right = set(ablated_selected.get(ts_key, ()))
        union = left | right
        if not union:
            costs.append(0.0)
            continue
        costs.append(float(len(left ^ right)) / float(len(union)))
    return float(np.mean(np.asarray(costs, dtype=np.float64))) if costs else 0.0


def _safe_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except Exception:
        return None
    if not math.isfinite(result):
        return None
    return result


def _safe_float(value: Any) -> float:
    result = _safe_optional_float(value)
    return float(result) if result is not None else 0.0


def _scoped_filename(filename: str, run_scope: str) -> str:
    normalized_scope = normalize_run_scope(run_scope)
    if normalized_scope == _DEFAULT_RUN_SCOPE:
        return filename
    path = Path(filename)
    return f"{path.stem}.{normalized_scope}{path.suffix}"
