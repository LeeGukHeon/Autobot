"""Live candidate domain reweighting helpers for trainer=v4_crypto_cs."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss
from sklearn.model_selection import StratifiedKFold


DEFAULT_LIVE_DOMAIN_REWEIGHTING_CLIP_MIN = 0.5
DEFAULT_LIVE_DOMAIN_REWEIGHTING_CLIP_MAX = 3.0
DEFAULT_LIVE_DOMAIN_REWEIGHTING_MIN_TARGET_ROWS = 32
DEFAULT_LIVE_DOMAIN_REWEIGHTING_MAX_TARGET_ROWS = 1024
DEFAULT_LIVE_DOMAIN_REWEIGHTING_MAX_SOURCE_FIT_ROWS = 20_000

_CANONICAL_SOURCE_FEATURE_ALIASES: dict[str, tuple[str, ...]] = {
    "rv_12": ("rv_12", "vol_12"),
    "rv_36": ("rv_36", "vol_36"),
    "atr_pct_14": ("atr_pct_14",),
    "trade_coverage_ms": ("m_trade_coverage_ms",),
    "book_coverage_ms": ("m_book_coverage_ms",),
    "spread_bps": ("m_spread_proxy", "m_spread_bps", "m_spread_bps_mean", "m_book_spread_bps"),
    "depth_top5_notional_krw": ("m_depth_top5_notional_krw",),
}


def resolve_default_live_candidate_db_path(*, project_root: Path) -> Path:
    return Path(project_root) / "data" / "state" / "live_candidate" / "live_state.db"


def build_live_candidate_domain_reweighting(
    *,
    enabled: bool,
    project_root: Path,
    candidate_db_path: Path | None,
    source_matrix: np.ndarray,
    source_feature_names: tuple[str, ...],
    source_aux_features: dict[str, np.ndarray] | None,
    base_sample_weight: np.ndarray,
    seed: int,
    clip_min: float = DEFAULT_LIVE_DOMAIN_REWEIGHTING_CLIP_MIN,
    clip_max: float = DEFAULT_LIVE_DOMAIN_REWEIGHTING_CLIP_MAX,
    min_target_rows: int = DEFAULT_LIVE_DOMAIN_REWEIGHTING_MIN_TARGET_ROWS,
    max_target_rows: int = DEFAULT_LIVE_DOMAIN_REWEIGHTING_MAX_TARGET_ROWS,
    max_source_fit_rows: int = DEFAULT_LIVE_DOMAIN_REWEIGHTING_MAX_SOURCE_FIT_ROWS,
) -> tuple[np.ndarray, dict[str, Any]]:
    base_weight = _resolve_base_weight(base_sample_weight=base_sample_weight, size=int(source_matrix.shape[0]))
    resolved_db_path = (
        Path(candidate_db_path)
        if candidate_db_path is not None
        else resolve_default_live_candidate_db_path(project_root=project_root)
    )
    diagnostics: dict[str, Any] = {
        "enabled": bool(enabled),
        "status": "disabled" if not enabled else "skipped",
        "policy": "live_candidate_logistic_density_ratio_v1",
        "source": "live_candidate_intents_bid_v1",
        "db_path": str(resolved_db_path),
        "clip_min": float(max(float(clip_min), 1e-6)),
        "clip_max": float(max(float(clip_max), max(float(clip_min), 1e-6))),
        "min_target_rows": int(max(int(min_target_rows), 1)),
        "max_target_rows": int(max(int(max_target_rows), 1)),
        "max_source_fit_rows": int(max(int(max_source_fit_rows), 1)),
        "source_rows_total": int(source_matrix.shape[0]),
        "target_rows_used": 0,
        "feature_names": [],
    }
    if not enabled:
        diagnostics["reason"] = "DISABLED_BY_OPTION"
        diagnostics["base_weight_summary"] = _weight_summary(base_weight)
        diagnostics["final_weight_summary"] = _weight_summary(base_weight)
        return base_weight, diagnostics
    if not resolved_db_path.exists():
        diagnostics["reason"] = "LIVE_CANDIDATE_DB_MISSING"
        diagnostics["base_weight_summary"] = _weight_summary(base_weight)
        diagnostics["final_weight_summary"] = _weight_summary(base_weight)
        return base_weight, diagnostics

    target_rows = _load_live_candidate_target_rows(
        db_path=resolved_db_path,
        max_rows=int(max_target_rows),
    )
    diagnostics["target_rows_total"] = int(len(target_rows))
    diagnostics["target_status_counts"] = _count_statuses(target_rows)
    if len(target_rows) < max(int(min_target_rows), 1):
        diagnostics["reason"] = "INSUFFICIENT_TARGET_ROWS"
        diagnostics["base_weight_summary"] = _weight_summary(base_weight)
        diagnostics["final_weight_summary"] = _weight_summary(base_weight)
        return base_weight, diagnostics

    source_feature_map = _build_source_feature_map(
        source_matrix=source_matrix,
        source_feature_names=source_feature_names,
        source_aux_features=source_aux_features,
    )
    active_specs = _resolve_active_feature_specs(
        source_feature_map=source_feature_map,
        target_rows=target_rows,
    )
    if not active_specs:
        diagnostics["reason"] = "NO_OVERLAP_FEATURES"
        diagnostics["base_weight_summary"] = _weight_summary(base_weight)
        diagnostics["final_weight_summary"] = _weight_summary(base_weight)
        return base_weight, diagnostics

    usable_target_rows = [
        row
        for row in target_rows
        if all(row.get(str(spec["canonical_name"])) is not None for spec in active_specs)
    ]
    diagnostics["target_rows_complete_case"] = int(len(usable_target_rows))
    if len(usable_target_rows) < max(int(min_target_rows), 1):
        diagnostics["reason"] = "INSUFFICIENT_TARGET_ROWS_AFTER_FILTERING"
        diagnostics["base_weight_summary"] = _weight_summary(base_weight)
        diagnostics["final_weight_summary"] = _weight_summary(base_weight)
        return base_weight, diagnostics

    source_all_matrix = np.column_stack([np.asarray(spec["source_values"], dtype=np.float64) for spec in active_specs])
    target_matrix = np.asarray(
        [[float(row[spec["canonical_name"]]) for spec in active_specs] for row in usable_target_rows],
        dtype=np.float64,
    )
    (
        source_all_scaled,
        source_fit_scaled,
        target_fit_scaled,
        scaler_center,
        scaler_scale,
        source_fit_indices,
        target_fit_indices,
    ) = _prepare_scaled_fit_matrices(
        source_all_matrix=source_all_matrix,
        target_matrix=target_matrix,
        max_source_fit_rows=int(max_source_fit_rows),
        max_target_rows=int(max_target_rows),
        seed=int(seed),
    )
    if source_fit_scaled.size <= 0 or target_fit_scaled.size <= 0:
        diagnostics["reason"] = "INSUFFICIENT_FIT_ROWS"
        diagnostics["base_weight_summary"] = _weight_summary(base_weight)
        diagnostics["final_weight_summary"] = _weight_summary(base_weight)
        return base_weight, diagnostics

    model, crossfit = _fit_crossfit_domain_classifier(
        source_fit_scaled=source_fit_scaled,
        target_fit_scaled=target_fit_scaled,
        seed=int(seed),
    )
    if model is None:
        diagnostics["reason"] = "DOMAIN_CLASSIFIER_FIT_FAILED"
        diagnostics["base_weight_summary"] = _weight_summary(base_weight)
        diagnostics["final_weight_summary"] = _weight_summary(base_weight)
        return base_weight, diagnostics

    source_prob = np.asarray(model.predict_proba(source_all_scaled)[:, 1], dtype=np.float64)
    source_prob = np.clip(source_prob, 1e-6, 1.0 - 1e-6)
    prior_ratio = float(source_fit_scaled.shape[0]) / float(max(target_fit_scaled.shape[0], 1))
    raw_multiplier = prior_ratio * (source_prob / np.clip(1.0 - source_prob, 1e-6, None))
    raw_multiplier = np.clip(raw_multiplier, diagnostics["clip_min"], diagnostics["clip_max"])
    multiplier_mean = float(np.nanmean(raw_multiplier)) if raw_multiplier.size > 0 else 1.0
    if not np.isfinite(multiplier_mean) or multiplier_mean <= 0.0:
        multiplier_mean = 1.0
    normalized_multiplier = raw_multiplier / multiplier_mean
    normalized_multiplier = np.clip(normalized_multiplier, diagnostics["clip_min"], diagnostics["clip_max"])
    final_weight = np.clip(base_weight * normalized_multiplier, 1e-6, None)

    diagnostics.update(
        {
            "status": "ready",
            "reason": "",
            "feature_names": [str(spec["source_name"]) for spec in active_specs],
            "canonical_feature_names": [str(spec["canonical_name"]) for spec in active_specs],
            "feature_sources": {
                str(spec["canonical_name"]): str(spec["source_name"])
                for spec in active_specs
            },
            "target_rows_used": int(target_matrix.shape[0]),
            "source_rows_fit": int(source_fit_scaled.shape[0]),
            "target_rows_fit": int(target_fit_scaled.shape[0]),
            "source_fit_indices_sampled": int(source_fit_indices.size),
            "target_fit_indices_sampled": int(target_fit_indices.size),
            "fit_prior_ratio": float(prior_ratio),
            "crossfit_fold_count": int(crossfit.get("fold_count", 0) or 0),
            "crossfit_log_loss": _safe_optional_float(crossfit.get("log_loss")),
            "classifier_intercept": float(model.intercept_[0]) if getattr(model, "intercept_", np.array([])).size > 0 else 0.0,
            "classifier_coefs": [float(value) for value in np.asarray(model.coef_[0], dtype=np.float64).tolist()],
            "scaler_center": [float(value) for value in scaler_center.tolist()],
            "scaler_scale": [float(value) for value in scaler_scale.tolist()],
            "raw_multiplier_summary": _weight_summary(raw_multiplier),
            "multiplier_summary": _weight_summary(normalized_multiplier),
            "base_weight_summary": _weight_summary(base_weight),
            "final_weight_summary": _weight_summary(final_weight),
            "target_ts_ms_min": min((int(row["ts_ms"]) for row in usable_target_rows), default=0),
            "target_ts_ms_max": max((int(row["ts_ms"]) for row in usable_target_rows), default=0),
        }
    )
    return final_weight, diagnostics


def _load_live_candidate_target_rows(*, db_path: Path, max_rows: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    query_limit = max(int(max_rows), 1) * 6
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        raw_rows = conn.execute(
            """
            SELECT ts_ms, market, side, status, meta_json
            FROM intents
            WHERE UPPER(COALESCE(side, '')) = 'BID'
            ORDER BY ts_ms DESC, intent_id DESC
            LIMIT ?
            """,
            (int(query_limit),),
        ).fetchall()
    finally:
        conn.close()
    for raw in raw_rows:
        item = _extract_target_row(dict(raw))
        if item is None:
            continue
        rows.append(item)
        if len(rows) >= max(int(max_rows), 1):
            break
    rows.reverse()
    return rows


def _extract_target_row(raw: dict[str, Any]) -> dict[str, Any] | None:
    meta = _parse_json_object(raw.get("meta_json"))
    strategy = dict(meta.get("strategy") or {}) if isinstance(meta.get("strategy"), dict) else {}
    strategy_meta = dict(strategy.get("meta") or {}) if isinstance(strategy.get("meta"), dict) else {}
    state_features = dict(strategy_meta.get("state_features") or {}) if isinstance(strategy_meta.get("state_features"), dict) else {}
    micro_state = dict(meta.get("micro_state") or {}) if isinstance(meta.get("micro_state"), dict) else {}
    target_row = {
        "ts_ms": int(raw.get("ts_ms") or 0),
        "market": str(raw.get("market") or "").strip().upper(),
        "status": str(raw.get("status") or "").strip().upper(),
        "rv_12": _coalesce_float(state_features.get("rv_12"), state_features.get("vol_12")),
        "rv_36": _coalesce_float(state_features.get("rv_36"), state_features.get("vol_36")),
        "atr_pct_14": _coalesce_float(state_features.get("atr_pct_14")),
        "trade_coverage_ms": _coalesce_float(
            state_features.get("m_trade_coverage_ms"),
            state_features.get("trade_coverage_ms"),
            micro_state.get("trade_coverage_ms"),
        ),
        "book_coverage_ms": _coalesce_float(
            state_features.get("m_book_coverage_ms"),
            state_features.get("book_coverage_ms"),
            micro_state.get("book_coverage_ms"),
        ),
        "spread_bps": _coalesce_float(
            state_features.get("m_spread_proxy"),
            state_features.get("spread_bps"),
            micro_state.get("spread_bps"),
        ),
        "depth_top5_notional_krw": _coalesce_float(
            state_features.get("m_depth_top5_notional_krw"),
            state_features.get("depth_top5_notional_krw"),
            micro_state.get("depth_top5_notional_krw"),
        ),
    }
    active_feature_count = sum(
        1 for key in _CANONICAL_SOURCE_FEATURE_ALIASES if target_row.get(key) is not None
    )
    if active_feature_count <= 0:
        return None
    return target_row


def _build_source_feature_map(
    *,
    source_matrix: np.ndarray,
    source_feature_names: tuple[str, ...],
    source_aux_features: dict[str, np.ndarray] | None,
) -> dict[str, dict[str, Any]]:
    feature_index = {str(name): idx for idx, name in enumerate(source_feature_names)}
    aux_features = dict(source_aux_features or {})
    source_rows = int(source_matrix.shape[0])
    feature_map: dict[str, dict[str, Any]] = {}
    for canonical_name, aliases in _CANONICAL_SOURCE_FEATURE_ALIASES.items():
        source_name: str | None = None
        values: np.ndarray | None = None
        if canonical_name in {"rv_12", "rv_36", "atr_pct_14"}:
            aux_values = np.asarray(aux_features.get(canonical_name, np.array([])), dtype=np.float64)
            if aux_values.size == source_rows:
                source_name = canonical_name
                values = aux_values
        if values is None:
            for alias in aliases:
                if alias in feature_index:
                    source_name = alias
                    values = np.asarray(source_matrix[:, feature_index[alias]], dtype=np.float64)
                    break
        if source_name is None or values is None or values.size != source_rows:
            continue
        feature_map[canonical_name] = {
            "canonical_name": canonical_name,
            "source_name": source_name,
            "source_values": values,
        }
    return feature_map


def _resolve_active_feature_specs(
    *,
    source_feature_map: dict[str, dict[str, Any]],
    target_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    active: list[dict[str, Any]] = []
    for canonical_name in _CANONICAL_SOURCE_FEATURE_ALIASES:
        spec = source_feature_map.get(canonical_name)
        if spec is None:
            continue
        target_values = np.asarray(
            [row.get(canonical_name) for row in target_rows if row.get(canonical_name) is not None],
            dtype=np.float64,
        )
        source_values = np.asarray(spec["source_values"], dtype=np.float64)
        if target_values.size <= 0 or not np.any(np.isfinite(source_values)):
            continue
        spec = dict(spec)
        spec["target_values"] = target_values
        active.append(spec)
    return active


def _prepare_scaled_fit_matrices(
    *,
    source_all_matrix: np.ndarray,
    target_matrix: np.ndarray,
    max_source_fit_rows: int,
    max_target_rows: int,
    seed: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    rng = np.random.default_rng(int(seed))
    source_count = int(source_all_matrix.shape[0])
    target_count = int(target_matrix.shape[0])
    source_fit_size = min(source_count, max(int(max_source_fit_rows), 1))
    target_fit_size = min(target_count, max(int(max_target_rows), 1))
    if source_fit_size >= source_count:
        source_fit_indices = np.arange(source_count, dtype=np.int64)
    else:
        source_fit_indices = np.sort(rng.choice(source_count, size=source_fit_size, replace=False)).astype(np.int64, copy=False)
    if target_fit_size >= target_count:
        target_fit_indices = np.arange(target_count, dtype=np.int64)
    else:
        target_fit_indices = np.arange(target_count - target_fit_size, target_count, dtype=np.int64)
    source_fit_matrix = np.asarray(source_all_matrix[source_fit_indices], dtype=np.float64)
    target_fit_matrix = np.asarray(target_matrix[target_fit_indices], dtype=np.float64)
    combined_fit = np.vstack([source_fit_matrix, target_fit_matrix])
    center = np.nanmedian(combined_fit, axis=0)
    center = np.where(np.isfinite(center), center, 0.0)
    combined_filled = np.where(np.isfinite(combined_fit), combined_fit, center)
    scale = np.nanstd(combined_filled, axis=0)
    scale = np.where(np.isfinite(scale) & (scale > 1e-6), scale, 1.0)
    active_cols = np.asarray(scale > 0.0, dtype=bool)
    source_all_filled = np.where(np.isfinite(source_all_matrix), source_all_matrix, center)
    source_fit_filled = np.where(np.isfinite(source_fit_matrix), source_fit_matrix, center)
    target_fit_filled = np.where(np.isfinite(target_fit_matrix), target_fit_matrix, center)
    source_all_scaled = (source_all_filled[:, active_cols] - center[active_cols]) / scale[active_cols]
    source_fit_scaled = (source_fit_filled[:, active_cols] - center[active_cols]) / scale[active_cols]
    target_fit_scaled = (target_fit_filled[:, active_cols] - center[active_cols]) / scale[active_cols]
    return (
        np.asarray(source_all_scaled, dtype=np.float64),
        np.asarray(source_fit_scaled, dtype=np.float64),
        np.asarray(target_fit_scaled, dtype=np.float64),
        np.asarray(center[active_cols], dtype=np.float64),
        np.asarray(scale[active_cols], dtype=np.float64),
        source_fit_indices,
        target_fit_indices,
    )


def _fit_crossfit_domain_classifier(
    *,
    source_fit_scaled: np.ndarray,
    target_fit_scaled: np.ndarray,
    seed: int,
) -> tuple[LogisticRegression | None, dict[str, Any]]:
    x_fit = np.vstack([source_fit_scaled, target_fit_scaled])
    y_fit = np.concatenate(
        [
            np.zeros(source_fit_scaled.shape[0], dtype=np.int8),
            np.ones(target_fit_scaled.shape[0], dtype=np.int8),
        ]
    )
    if x_fit.shape[0] <= 2 or np.unique(y_fit).size < 2:
        return None, {"fold_count": 0, "log_loss": None}
    model = LogisticRegression(
        C=1.0,
        solver="lbfgs",
        max_iter=1_000,
        random_state=int(seed),
    )
    try:
        model.fit(x_fit, y_fit)
    except Exception:
        return None, {"fold_count": 0, "log_loss": None}

    fold_count = min(3, int(np.sum(y_fit == 0)), int(np.sum(y_fit == 1)))
    if fold_count < 2:
        return model, {"fold_count": 0, "log_loss": None}

    splitter = StratifiedKFold(n_splits=int(fold_count), shuffle=True, random_state=int(seed))
    oof_probability = np.zeros(y_fit.size, dtype=np.float64)
    completed_folds = 0
    for train_index, test_index in splitter.split(x_fit, y_fit):
        try:
            fold_model = LogisticRegression(
                C=1.0,
                solver="lbfgs",
                max_iter=1_000,
                random_state=int(seed + completed_folds + 1),
            )
            fold_model.fit(x_fit[train_index], y_fit[train_index])
            oof_probability[test_index] = fold_model.predict_proba(x_fit[test_index])[:, 1]
            completed_folds += 1
        except Exception:
            continue
    if completed_folds <= 0:
        return model, {"fold_count": 0, "log_loss": None}
    clipped_probability = np.clip(oof_probability, 1e-6, 1.0 - 1e-6)
    return model, {
        "fold_count": int(completed_folds),
        "log_loss": float(log_loss(y_fit, clipped_probability, labels=[0, 1])),
    }


def _resolve_base_weight(*, base_sample_weight: np.ndarray, size: int) -> np.ndarray:
    weight = np.asarray(base_sample_weight, dtype=np.float64)
    if weight.size != int(size):
        return np.ones(int(size), dtype=np.float64)
    return np.clip(weight, 1e-6, None)


def _count_statuses(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get("status") or "").strip().upper() or "UNKNOWN"
        counts[status] = counts.get(status, 0) + 1
    return counts


def _weight_summary(values: np.ndarray) -> dict[str, float]:
    arr = np.asarray(values, dtype=np.float64)
    if arr.size <= 0:
        return {"min": 0.0, "max": 0.0, "mean": 0.0, "median": 0.0}
    finite = arr[np.isfinite(arr)]
    if finite.size <= 0:
        return {"min": 0.0, "max": 0.0, "mean": 0.0, "median": 0.0}
    return {
        "min": float(np.min(finite)),
        "max": float(np.max(finite)),
        "mean": float(np.mean(finite)),
        "median": float(np.median(finite)),
    }


def _parse_json_object(value: Any) -> dict[str, Any]:
    if value in (None, ""):
        return {}
    try:
        payload = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _coalesce_float(*values: Any) -> float | None:
    for value in values:
        coerced = _safe_optional_float(value)
        if coerced is not None:
            return coerced
    return None


def _safe_optional_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
