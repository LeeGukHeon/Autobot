"""Independent metric audit helpers for registered model runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.metrics import average_precision_score, brier_score_loss, log_loss, roc_auc_score

from autobot.data import expected_interval_ms
from autobot.features.feature_spec import parse_date_to_ts_ms

from .dataset_loader import DatasetRequest, load_feature_dataset
from .registry import load_json, load_model_bundle, resolve_run_dir
from .split import SPLIT_TEST, SPLIT_TRAIN, SPLIT_VALID, compute_time_splits, split_masks
from .train_v1 import _predict_scores


@dataclass(frozen=True)
class MetricAuditOptions:
    registry_root: Path
    logs_root: Path
    model_ref: str
    model_family: str | None = None
    split: str = SPLIT_TEST
    start: str | None = None
    end: str | None = None
    tolerance_warn: float = 1e-6
    tolerance_fail: float = 1e-3


@dataclass(frozen=True)
class MetricAuditResult:
    status: str
    run_id: str
    run_dir: Path
    split: str
    rows_in_split: int
    payload: dict[str, Any]
    output_path: Path


def audit_predictions(
    *,
    y_true: np.ndarray,
    y_pred_proba: np.ndarray,
    y_pred_margin: np.ndarray | None = None,
    stored_classification: dict[str, Any] | None = None,
    tolerance_warn: float = 1e-6,
    tolerance_fail: float = 1e-3,
) -> dict[str, Any]:
    y = np.asarray(y_true)
    proba = np.asarray(y_pred_proba, dtype=np.float64)
    margin = np.asarray(y_pred_margin, dtype=np.float64) if y_pred_margin is not None else None

    issues: list[dict[str, Any]] = []
    checks: dict[str, Any] = {
        "y_true_binary": False,
        "y_true_non_empty": bool(y.size > 0),
        "shape_match": bool(y.ndim == 1 and proba.ndim == 1 and y.size == proba.size),
        "proba_finite": bool(np.isfinite(proba).all()) if proba.size > 0 else False,
        "proba_in_range_0_1": False,
    }

    if not checks["shape_match"]:
        issues.append(
            {
                "severity": "FAIL",
                "code": "SHAPE_MISMATCH",
                "message": f"shape mismatch y={tuple(y.shape)} proba={tuple(proba.shape)}",
            }
        )
    if not checks["y_true_non_empty"]:
        issues.append(
            {
                "severity": "FAIL",
                "code": "EMPTY_LABELS",
                "message": "y_true is empty",
            }
        )

    unique_labels = set(int(item) for item in np.unique(y).tolist()) if y.size > 0 else set()
    checks["y_true_binary"] = unique_labels.issubset({0, 1}) and len(unique_labels) > 0
    if not checks["y_true_binary"]:
        issues.append(
            {
                "severity": "FAIL",
                "code": "NON_BINARY_LABELS",
                "message": f"y_true labels must be subset of {{0,1}}, got {sorted(unique_labels)}",
            }
        )

    if not checks["proba_finite"]:
        issues.append(
            {
                "severity": "FAIL",
                "code": "PROBA_NON_FINITE",
                "message": "y_pred_proba has NaN/inf",
            }
        )

    in_range = bool(np.all((proba >= 0.0) & (proba <= 1.0))) if proba.size > 0 else False
    checks["proba_in_range_0_1"] = in_range
    if not in_range:
        issues.append(
            {
                "severity": "FAIL",
                "code": "PROBA_OUT_OF_RANGE",
                "message": "y_pred_proba must be within [0,1]",
            }
        )

    if margin is not None:
        checks["margin_shape_match"] = bool(margin.ndim == 1 and margin.size == proba.size)
        checks["margin_finite"] = bool(np.isfinite(margin).all()) if margin.size > 0 else False
        if not checks["margin_shape_match"]:
            issues.append(
                {
                    "severity": "WARN",
                    "code": "MARGIN_SHAPE_MISMATCH",
                    "message": f"margin shape mismatch margin={tuple(margin.shape)} proba={tuple(proba.shape)}",
                }
            )
        if not checks["margin_finite"]:
            issues.append(
                {
                    "severity": "WARN",
                    "code": "MARGIN_NON_FINITE",
                    "message": "y_pred_margin has NaN/inf",
                }
            )

    hard_fail = any(item.get("severity") == "FAIL" for item in issues)
    recomputed: dict[str, float | None] = {
        "roc_auc": None,
        "pr_auc": None,
        "log_loss": None,
        "brier_score": None,
    }
    if not hard_fail:
        y_binary = y.astype(np.int8, copy=False)
        clipped = np.clip(proba, 1e-7, 1.0 - 1e-7)
        try:
            recomputed["roc_auc"] = float(roc_auc_score(y_binary, clipped))
        except ValueError:
            recomputed["roc_auc"] = None
        try:
            recomputed["pr_auc"] = float(average_precision_score(y_binary, clipped))
        except ValueError:
            recomputed["pr_auc"] = None
        recomputed["log_loss"] = float(log_loss(y_binary, clipped, labels=[0, 1]))
        recomputed["brier_score"] = float(brier_score_loss(y_binary, clipped))

    stored = _normalize_stored_classification(stored_classification)
    diffs, mismatch_issues = _compare_classification_metrics(
        recomputed=recomputed,
        stored=stored,
        tolerance_warn=float(tolerance_warn),
        tolerance_fail=float(tolerance_fail),
    )
    issues.extend(mismatch_issues)

    status = "PASS"
    if any(item.get("severity") == "FAIL" for item in issues):
        status = "FAIL"
    elif any(item.get("severity") == "WARN" for item in issues):
        status = "WARN"

    return {
        "status": status,
        "checks": checks,
        "rows": int(y.size),
        "stored_classification": stored,
        "recomputed_classification": recomputed,
        "diffs": diffs,
        "issues": issues,
        "tolerance": {"warn": float(tolerance_warn), "fail": float(tolerance_fail)},
    }


def audit_registered_model(options: MetricAuditOptions) -> MetricAuditResult:
    split = str(options.split).strip().lower()
    if split not in {SPLIT_TRAIN, SPLIT_VALID, SPLIT_TEST}:
        raise ValueError("split must be one of train|valid|test")

    run_dir = resolve_run_dir(
        options.registry_root,
        model_ref=str(options.model_ref).strip(),
        model_family=(str(options.model_family).strip() if options.model_family else None),
    )
    run_id = run_dir.name

    train_config = load_json(run_dir / "train_config.yaml")
    if not train_config:
        raise ValueError(f"invalid train_config.yaml at {run_dir}")

    start_ts_ms = (
        parse_date_to_ts_ms(str(options.start).strip()) if options.start else int(train_config.get("start_ts_ms"))
    )
    end_ts_ms = (
        parse_date_to_ts_ms(str(options.end).strip(), end_of_day=True)
        if options.end
        else int(train_config.get("end_ts_ms"))
    )
    if end_ts_ms < start_ts_ms:
        raise ValueError("audit end must be >= start")

    request = DatasetRequest(
        dataset_root=Path(str(train_config.get("dataset_root", ""))),
        tf=str(train_config.get("tf", "5m")).strip().lower(),
        quote=str(train_config.get("quote", "KRW")).strip().upper(),
        top_n=int(train_config.get("top_n", 20)),
        start_ts_ms=int(start_ts_ms),
        end_ts_ms=int(end_ts_ms),
        markets=tuple(str(item).strip().upper() for item in train_config.get("markets", []) if str(item).strip()),
        batch_rows=max(int(train_config.get("batch_rows", 200_000)), 1),
    )
    feature_cols = tuple(str(item) for item in train_config.get("feature_columns", []))
    dataset = load_feature_dataset(request, feature_columns=feature_cols if feature_cols else None)

    labels, split_info = compute_time_splits(
        dataset.ts_ms,
        train_ratio=float(train_config.get("train_ratio", 0.70)),
        valid_ratio=float(train_config.get("valid_ratio", 0.15)),
        test_ratio=float(train_config.get("test_ratio", 0.15)),
        embargo_bars=int(train_config.get("embargo_bars", 12)),
        interval_ms=expected_interval_ms(str(train_config.get("tf", "5m")).strip().lower()),
    )
    mask = split_masks(labels)[split]
    rows_in_split = int(np.sum(mask))
    if rows_in_split <= 0:
        raise ValueError(f"{split} split has no rows")

    model_bundle = load_model_bundle(run_dir)
    proba = _predict_scores(model_bundle, dataset.X[mask])
    y_true = dataset.y_cls[mask]

    metrics_doc = load_json(run_dir / "metrics.json")
    stored_classification = _extract_stored_classification(metrics_doc=metrics_doc, split=split)
    audit_payload = audit_predictions(
        y_true=y_true,
        y_pred_proba=proba,
        stored_classification=stored_classification,
        tolerance_warn=float(options.tolerance_warn),
        tolerance_fail=float(options.tolerance_fail),
    )
    audit_payload["run_id"] = run_id
    audit_payload["run_dir"] = str(run_dir)
    audit_payload["split"] = split
    audit_payload["rows_in_split"] = rows_in_split
    audit_payload["window"] = {"start_ts_ms": int(start_ts_ms), "end_ts_ms": int(end_ts_ms)}
    audit_payload["split_counts"] = {
        "train": int(split_info.counts.get(SPLIT_TRAIN, 0)),
        "valid": int(split_info.counts.get(SPLIT_VALID, 0)),
        "test": int(split_info.counts.get(SPLIT_TEST, 0)),
        "drop": int(split_info.counts.get("drop", 0)),
    }
    audit_payload["created_at_utc"] = _utc_now()

    options.logs_root.mkdir(parents=True, exist_ok=True)
    output_path = options.logs_root / f"metric_audit_{run_id}.json"
    output_path.write_text(
        json.dumps(audit_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return MetricAuditResult(
        status=str(audit_payload.get("status", "FAIL")),
        run_id=run_id,
        run_dir=run_dir,
        split=split,
        rows_in_split=rows_in_split,
        payload=audit_payload,
        output_path=output_path,
    )


def _compare_classification_metrics(
    *,
    recomputed: dict[str, float | None],
    stored: dict[str, float | None],
    tolerance_warn: float,
    tolerance_fail: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    diffs: dict[str, Any] = {}
    issues: list[dict[str, Any]] = []
    for key in ("roc_auc", "pr_auc", "log_loss", "brier_score"):
        rec = recomputed.get(key)
        old = stored.get(key)
        if rec is None or old is None:
            diffs[key] = None
            continue
        delta = abs(float(rec) - float(old))
        diffs[key] = delta
        if delta > float(tolerance_fail):
            issues.append(
                {
                    "severity": "FAIL",
                    "code": "METRIC_MISMATCH",
                    "metric": key,
                    "stored": float(old),
                    "recomputed": float(rec),
                    "abs_delta": float(delta),
                }
            )
        elif delta > float(tolerance_warn):
            issues.append(
                {
                    "severity": "WARN",
                    "code": "METRIC_MISMATCH",
                    "metric": key,
                    "stored": float(old),
                    "recomputed": float(rec),
                    "abs_delta": float(delta),
                }
            )
    return diffs, issues


def _extract_stored_classification(*, metrics_doc: dict[str, Any], split: str) -> dict[str, Any]:
    if not metrics_doc:
        return {}

    if split == SPLIT_TEST:
        champion_metrics = metrics_doc.get("champion_metrics", {}) if isinstance(metrics_doc, dict) else {}
        cls = champion_metrics.get("classification", {}) if isinstance(champion_metrics, dict) else {}
        if isinstance(cls, dict) and cls:
            return cls

    champion = metrics_doc.get("champion", {}) if isinstance(metrics_doc, dict) else {}
    champion_name = str(champion.get("name", "")).strip().lower() if isinstance(champion, dict) else ""
    if champion_name:
        block = metrics_doc.get(champion_name, {})
        if isinstance(block, dict):
            split_metrics = block.get(split, {})
            cls = split_metrics.get("classification", {}) if isinstance(split_metrics, dict) else {}
            if isinstance(cls, dict) and cls:
                return cls

    booster = metrics_doc.get("booster", {})
    if isinstance(booster, dict):
        split_metrics = booster.get(split, {})
        cls = split_metrics.get("classification", {}) if isinstance(split_metrics, dict) else {}
        if isinstance(cls, dict) and cls:
            return cls
    baseline = metrics_doc.get("baseline", {})
    if isinstance(baseline, dict):
        split_metrics = baseline.get(split, {})
        cls = split_metrics.get("classification", {}) if isinstance(split_metrics, dict) else {}
        if isinstance(cls, dict) and cls:
            return cls
    return {}


def _normalize_stored_classification(payload: dict[str, Any] | None) -> dict[str, float | None]:
    src = payload if isinstance(payload, dict) else {}
    out: dict[str, float | None] = {}
    for key in ("roc_auc", "pr_auc", "log_loss", "brier_score"):
        value = src.get(key)
        if value is None:
            out[key] = None
            continue
        try:
            out[key] = float(value)
        except (TypeError, ValueError):
            out[key] = None
    return out


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
