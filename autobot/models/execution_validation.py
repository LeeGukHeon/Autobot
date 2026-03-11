from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from statistics import fmean, pstdev
from typing import Any

from autobot.common.rolling_window_evidence import compute_rolling_window_evidence


def build_execution_validation_summary(
    summary: dict[str, Any] | None,
    *,
    window_minutes: int,
    fold_count: int,
    min_active_windows: int,
    target_return: float = 0.0,
    lpm_order: int = 2,
    numerical_floor: float = 1e-12,
) -> dict[str, Any]:
    payload = dict(summary or {})
    cached = payload.get("execution_validation")
    if isinstance(cached, dict) and cached:
        return dict(cached)

    run_dir_text = str(payload.get("run_dir", "")).strip()
    run_dir = Path(run_dir_text) if run_dir_text else None
    if run_dir is None or not run_dir.exists():
        return {
            "method": "rolling_window_sortino_lpm_cv_v1",
            "comparable": False,
            "reasons": ["RUN_DIR_MISSING"],
            "window_minutes": max(int(window_minutes), 1),
            "requested_fold_count": max(int(fold_count), 1),
            "min_active_windows_required": max(int(min_active_windows), 1),
            "target_return": float(target_return),
            "lpm_order": max(int(lpm_order), 1),
            "numerical_floor": float(numerical_floor),
            "objective_score": 0.0,
            "objective_std": 0.0,
            "active_windows": 0,
            "windows_total": 0,
            "folds": [],
        }

    equity_path = run_dir / "equity.csv"
    fills_path = run_dir / "fills.jsonl"
    if not equity_path.exists():
        return {
            "method": "rolling_window_sortino_lpm_cv_v1",
            "comparable": False,
            "reasons": ["EQUITY_CSV_MISSING"],
            "window_minutes": max(int(window_minutes), 1),
            "requested_fold_count": max(int(fold_count), 1),
            "min_active_windows_required": max(int(min_active_windows), 1),
            "target_return": float(target_return),
            "lpm_order": max(int(lpm_order), 1),
            "numerical_floor": float(numerical_floor),
            "objective_score": 0.0,
            "objective_std": 0.0,
            "active_windows": 0,
            "windows_total": 0,
            "folds": [],
        }

    equity_samples = _load_equity_samples(equity_path)
    fill_records = _load_fill_records(fills_path) if fills_path.exists() else []
    rolling = compute_rolling_window_evidence(
        equity_samples=equity_samples,
        fill_records=fill_records,
        window_ms=max(int(window_minutes), 1) * 60_000,
    )
    windows = [dict(item) for item in (rolling.get("windows") or []) if isinstance(item, dict)]
    active_windows = [_execution_window_row(item) for item in windows if _is_active_window(item)]
    if len(active_windows) < max(int(min_active_windows), 1):
        return {
            "method": "rolling_window_sortino_lpm_cv_v1",
            "comparable": False,
            "reasons": ["INSUFFICIENT_ACTIVE_WINDOWS"],
            "window_minutes": int(rolling.get("window_minutes", max(int(window_minutes), 1)) or max(int(window_minutes), 1)),
            "requested_fold_count": max(int(fold_count), 1),
            "min_active_windows_required": max(int(min_active_windows), 1),
            "target_return": float(target_return),
            "lpm_order": max(int(lpm_order), 1),
            "numerical_floor": float(numerical_floor),
            "objective_score": 0.0,
            "objective_std": 0.0,
            "active_windows": int(len(active_windows)),
            "windows_total": int(len(windows)),
            "folds": [],
            "rolling_evidence": _rolling_summary_doc(rolling),
        }

    effective_fold_count = min(max(int(fold_count), 2), len(active_windows))
    folds = _split_contiguous_folds(active_windows, fold_count=effective_fold_count)
    fold_docs = [
        _fold_validation_doc(
            fold_index=index,
            windows=fold_rows,
            target_return=float(target_return),
            lpm_order=max(int(lpm_order), 1),
            numerical_floor=float(numerical_floor),
        )
        for index, fold_rows in enumerate(folds)
        if fold_rows
    ]
    comparable_fold_docs = [dict(item) for item in fold_docs if bool(item.get("comparable"))]
    if len(comparable_fold_docs) < 2:
        return {
            "method": "rolling_window_sortino_lpm_cv_v1",
            "comparable": False,
            "reasons": ["INSUFFICIENT_VALIDATION_FOLDS"],
            "window_minutes": int(rolling.get("window_minutes", max(int(window_minutes), 1)) or max(int(window_minutes), 1)),
            "requested_fold_count": max(int(fold_count), 1),
            "effective_fold_count": int(effective_fold_count),
            "min_active_windows_required": max(int(min_active_windows), 1),
            "target_return": float(target_return),
            "lpm_order": max(int(lpm_order), 1),
            "numerical_floor": float(numerical_floor),
            "objective_score": 0.0,
            "objective_std": 0.0,
            "active_windows": int(len(active_windows)),
            "windows_total": int(len(windows)),
            "folds": fold_docs,
            "rolling_evidence": _rolling_summary_doc(rolling),
        }

    scores = [float(item.get("sortino_score", 0.0)) for item in comparable_fold_docs]
    nonnegative_values = [float(item.get("nonnegative_ratio", 0.0)) for item in comparable_fold_docs]
    downside_values = [float(item.get("downside_deviation", 0.0)) for item in comparable_fold_docs]
    mean_return_values = [float(item.get("mean_return", 0.0)) for item in comparable_fold_docs]
    worst_return_values = [float(item.get("worst_window_return", 0.0)) for item in comparable_fold_docs]
    max_drawdown_values = [float(item.get("max_window_drawdown_pct", 0.0)) for item in comparable_fold_docs]
    return {
        "method": "rolling_window_sortino_lpm_cv_v1",
        "comparable": True,
        "reasons": [],
        "window_minutes": int(rolling.get("window_minutes", max(int(window_minutes), 1)) or max(int(window_minutes), 1)),
        "requested_fold_count": max(int(fold_count), 1),
        "effective_fold_count": int(effective_fold_count),
        "comparable_fold_count": int(len(comparable_fold_docs)),
        "min_active_windows_required": max(int(min_active_windows), 1),
        "target_return": float(target_return),
        "lpm_order": max(int(lpm_order), 1),
        "numerical_floor": float(numerical_floor),
        "objective_score": float(fmean(scores)),
        "objective_std": float(pstdev(scores) if len(scores) > 1 else 0.0),
        "mean_return": float(fmean(mean_return_values)),
        "downside_deviation": float(fmean(downside_values)),
        "nonnegative_ratio_mean": float(fmean(nonnegative_values)),
        "worst_window_return": float(min(worst_return_values) if worst_return_values else 0.0),
        "max_window_drawdown_pct": float(max(max_drawdown_values) if max_drawdown_values else 0.0),
        "active_windows": int(len(active_windows)),
        "windows_total": int(len(windows)),
        "folds": fold_docs,
        "rolling_evidence": _rolling_summary_doc(rolling),
    }


def compare_execution_validation_folds(
    candidate_validation: dict[str, Any] | None,
    champion_validation: dict[str, Any] | None,
    *,
    alpha: float,
) -> dict[str, Any]:
    candidate = dict(candidate_validation or {})
    champion = dict(champion_validation or {})
    candidate_folds = {
        int(item.get("fold_index", -1)): dict(item)
        for item in (candidate.get("folds") or [])
        if isinstance(item, dict) and int(item.get("fold_index", -1)) >= 0 and bool(item.get("comparable"))
    }
    champion_folds = {
        int(item.get("fold_index", -1)): dict(item)
        for item in (champion.get("folds") or [])
        if isinstance(item, dict) and int(item.get("fold_index", -1)) >= 0 and bool(item.get("comparable"))
    }
    common_indices = sorted(set(candidate_folds).intersection(champion_folds))
    if len(common_indices) < 2:
        return {
            "comparable": False,
            "reasons": ["INSUFFICIENT_COMMON_VALIDATION_FOLDS"],
            "decision": "insufficient_evidence",
            "alpha": float(alpha),
            "fold_count": int(len(common_indices)),
            "mean_diff": 0.0,
            "p_value_upper": 1.0,
            "p_value_lower": 1.0,
            "studentized_mean_t": 0.0,
            "fold_indices": common_indices,
        }
    diffs = [
        float(candidate_folds[index].get("sortino_score", 0.0)) - float(champion_folds[index].get("sortino_score", 0.0))
        for index in common_indices
    ]
    mean_diff = sum(diffs) / float(len(diffs))
    p_value_upper = _exact_sign_flip_tail_probability(diffs, upper_tail=True)
    p_value_lower = _exact_sign_flip_tail_probability(diffs, upper_tail=False)
    t_stat = _studentized_mean(diffs)
    if mean_diff > 0.0 and p_value_upper <= float(alpha):
        decision = "candidate_edge"
        reasons = ["DOWNSIDE_VALIDATED_CV_PASS"]
    elif mean_diff < 0.0 and p_value_lower <= float(alpha):
        decision = "champion_edge"
        reasons = ["DOWNSIDE_VALIDATED_CV_FAIL"]
    else:
        decision = "indeterminate"
        reasons = ["DOWNSIDE_VALIDATED_CV_HOLD"]
    return {
        "comparable": True,
        "reasons": reasons,
        "decision": decision,
        "alpha": float(alpha),
        "fold_count": int(len(common_indices)),
        "mean_diff": float(mean_diff),
        "p_value_upper": float(p_value_upper),
        "p_value_lower": float(p_value_lower),
        "studentized_mean_t": float(t_stat),
        "fold_indices": common_indices,
    }


def _load_equity_samples(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            rows.append(
                {
                    "ts_ms": int(float(row.get("ts_ms", 0) or 0)),
                    "equity_quote": float(row.get("equity_quote", 0.0) or 0.0),
                    "realized_pnl_quote": float(row.get("realized_pnl_quote", 0.0) or 0.0),
                }
            )
    return rows


def _load_fill_records(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fp:
        for raw_line in fp:
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            rows.append(
                {
                    "ts_ms": int(payload.get("ts_ms", 0) or 0),
                    "market": str(payload.get("market", "")).strip().upper(),
                }
            )
    return rows


def _rolling_summary_doc(rolling: dict[str, Any]) -> dict[str, Any]:
    return {
        "window_minutes": int(rolling.get("window_minutes", 0) or 0),
        "windows_total": int(rolling.get("windows_total", 0) or 0),
        "active_windows": int(rolling.get("active_windows", 0) or 0),
        "nonnegative_active_window_ratio": float(rolling.get("nonnegative_active_window_ratio", 0.0) or 0.0),
        "positive_active_window_ratio": float(rolling.get("positive_active_window_ratio", 0.0) or 0.0),
        "max_fill_concentration_ratio": float(rolling.get("max_fill_concentration_ratio", 0.0) or 0.0),
        "max_window_drawdown_pct": float(rolling.get("max_window_drawdown_pct", 0.0) or 0.0),
        "worst_window_realized_pnl_quote": float(rolling.get("worst_window_realized_pnl_quote", 0.0) or 0.0),
    }


def _is_active_window(window: dict[str, Any]) -> bool:
    return int(window.get("fills", 0) or 0) > 0 or abs(float(window.get("realized_pnl_delta_quote", 0.0) or 0.0)) > 1e-9


def _execution_window_row(window: dict[str, Any]) -> dict[str, Any]:
    start_equity = max(float(window.get("start_equity_quote", 0.0) or 0.0), 1e-12)
    realized_pnl = float(window.get("realized_pnl_delta_quote", 0.0) or 0.0)
    window_return = realized_pnl / start_equity
    return {
        "window_index": int(window.get("window_index", -1) or -1),
        "fills": int(window.get("fills", 0) or 0),
        "start_equity_quote": float(start_equity),
        "realized_pnl_delta_quote": float(realized_pnl),
        "window_return": float(window_return),
        "max_drawdown_pct": float(window.get("max_drawdown_pct", 0.0) or 0.0),
    }


def _split_contiguous_folds(windows: list[dict[str, Any]], *, fold_count: int) -> list[list[dict[str, Any]]]:
    total = len(windows)
    folds: list[list[dict[str, Any]]] = []
    for index in range(max(int(fold_count), 1)):
        start = int(math.floor(index * total / float(fold_count)))
        end = int(math.floor((index + 1) * total / float(fold_count)))
        folds.append(windows[start:end])
    return [fold for fold in folds if fold]


def _fold_validation_doc(
    *,
    fold_index: int,
    windows: list[dict[str, Any]],
    target_return: float,
    lpm_order: int,
    numerical_floor: float,
) -> dict[str, Any]:
    if not windows:
        return {
            "fold_index": int(fold_index),
            "comparable": False,
            "reasons": ["EMPTY_FOLD"],
            "window_count": 0,
            "sortino_score": 0.0,
        }
    returns = [float(item.get("window_return", 0.0)) for item in windows]
    count = float(len(returns))
    mean_return = sum(returns) / count
    downside_terms = [max(float(target_return) - value, 0.0) ** max(int(lpm_order), 1) for value in returns]
    downside_lpm = sum(downside_terms) / count
    downside_deviation = downside_lpm ** (1.0 / float(max(int(lpm_order), 1))) if downside_lpm > 0.0 else 0.0
    sortino_raw = mean_return / max(downside_deviation, float(numerical_floor))
    sortino_score = math.copysign(math.log1p(abs(sortino_raw)), sortino_raw)
    nonnegative_ratio = float(sum(1 for value in returns if value >= float(target_return))) / count
    worst_window_return = min(returns) if returns else 0.0
    max_window_drawdown = max(float(item.get("max_drawdown_pct", 0.0)) for item in windows)
    worst_window_realized = min(float(item.get("realized_pnl_delta_quote", 0.0)) for item in windows)
    return {
        "fold_index": int(fold_index),
        "comparable": True,
        "reasons": [],
        "window_count": int(len(windows)),
        "mean_return": float(mean_return),
        "downside_lpm": float(downside_lpm),
        "downside_deviation": float(downside_deviation),
        "sortino_score": float(sortino_score),
        "nonnegative_ratio": float(nonnegative_ratio),
        "worst_window_return": float(worst_window_return),
        "worst_window_realized_pnl_quote": float(worst_window_realized),
        "max_window_drawdown_pct": float(max_window_drawdown),
    }


def _studentized_mean(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean_value = sum(values) / float(len(values))
    variance = sum((float(value) - mean_value) ** 2 for value in values) / float(len(values) - 1)
    std_value = variance ** 0.5
    if std_value <= 0.0:
        return 0.0 if mean_value == 0.0 else float("inf")
    return float(mean_value / (std_value / (float(len(values)) ** 0.5)))


def _exact_sign_flip_tail_probability(values: list[float], *, upper_tail: bool) -> float:
    if not values:
        return 1.0
    observed = sum(values) / float(len(values))
    total = 1 << len(values)
    extreme = 0
    for mask in range(total):
        signed_total = 0.0
        for index, value in enumerate(values):
            sign = -1.0 if ((mask >> index) & 1) else 1.0
            signed_total += sign * float(value)
        candidate_mean = signed_total / float(len(values))
        if upper_tail:
            if candidate_mean >= observed - 1e-12:
                extreme += 1
        else:
            if candidate_mean <= observed + 1e-12:
                extreme += 1
    return float(extreme) / float(total)
