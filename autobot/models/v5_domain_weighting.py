"""Shared domain-weighting helpers and artifacts for v5 trainers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import json
import numpy as np


def build_v5_domain_weighting_report(
    *,
    run_id: str,
    trainer_name: str,
    model_family: str,
    component_order: list[str] | tuple[str, ...],
    final_sample_weight: np.ndarray,
    base_sample_weight: np.ndarray | None = None,
    data_quality_weight: np.ndarray | None = None,
    support_weight: np.ndarray | None = None,
    domain_weight: np.ndarray | None = None,
    domain_details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    final_weight = _to_float_array(final_sample_weight)
    base_weight = _to_float_array(base_sample_weight) if base_sample_weight is not None else final_weight.copy()
    data_quality = _to_float_array(data_quality_weight) if data_quality_weight is not None else np.ones(final_weight.size, dtype=np.float64)
    support = _to_float_array(support_weight) if support_weight is not None else np.ones(final_weight.size, dtype=np.float64)
    domain = _to_float_array(domain_weight) if domain_weight is not None else np.ones(final_weight.size, dtype=np.float64)
    details = dict(domain_details or {})
    enabled = bool(details.get("enabled", False)) or bool(domain_weight is not None)
    policy = str(details.get("policy") or "v5_domain_weighting_v1").strip() or "v5_domain_weighting_v1"
    return {
        "policy": policy,
        "run_id": str(run_id).strip(),
        "trainer": str(trainer_name).strip(),
        "model_family": str(model_family).strip(),
        "domain_weighting_enabled": bool(enabled),
        "component_order": [str(item).strip() for item in component_order if str(item).strip()],
        "base_sample_weight_summary": _weight_summary(base_weight),
        "data_quality_weight_summary": _weight_summary(data_quality),
        "support_level_weight_summary": _weight_summary(support),
        "domain_weight_summary": _weight_summary(domain),
        "effective_sample_weight_summary": _weight_summary(final_weight),
        "sample_count": int(final_weight.size),
        "domain_details": details,
    }


def resolve_v5_domain_weighting_components(
    *,
    markets: np.ndarray | list[str] | tuple[str, ...],
    ts_ms: np.ndarray | list[int] | tuple[int, ...] | None = None,
    split_labels: np.ndarray | list[str] | tuple[str, ...] | None = None,
    base_sample_weight: np.ndarray | list[float] | tuple[float, ...] | None = None,
    data_quality_weight: np.ndarray | list[float] | tuple[float, ...] | None = None,
    support_weight: np.ndarray | list[float] | tuple[float, ...] | None = None,
    clip_min: float = 0.5,
    clip_max: float = 2.0,
    temporal_bucket_ms: int = 7 * 24 * 60 * 60 * 1000,
) -> dict[str, Any]:
    market_arr = np.asarray(markets, dtype=object).reshape(-1)
    size = int(market_arr.size)
    if size <= 0:
        empty = np.asarray([], dtype=np.float64)
        details = {
            "enabled": False,
            "policy": "v5_domain_weighting_v1",
            "source_kind": "regime_inverse_frequency_v1",
            "status": "empty",
            "clip_min": float(max(float(clip_min), 1e-6)),
            "clip_max": float(max(float(clip_max), max(float(clip_min), 1e-6))),
            "temporal_bucket_ms": int(max(int(temporal_bucket_ms), 1)),
            "bucket_count": 0,
            "bucket_examples": [],
        }
        return {
            "base_sample_weight": empty,
            "data_quality_weight": empty,
            "support_weight": empty,
            "domain_weight": empty,
            "final_sample_weight": empty,
            "domain_details": details,
        }

    clip_floor = float(max(float(clip_min), 1e-6))
    clip_ceiling = float(max(float(clip_max), clip_floor))
    bucket_width = int(max(int(temporal_bucket_ms), 1))
    base = _resolve_weight_component(base_sample_weight, size=size, default_value=1.0)
    data_quality = _resolve_weight_component(data_quality_weight, size=size, default_value=1.0)
    support = _resolve_weight_component(support_weight, size=size, default_value=1.0)
    split_arr = (
        np.asarray(split_labels, dtype=object).reshape(-1)
        if split_labels is not None
        else np.full(size, "all", dtype=object)
    )
    if split_arr.size != size:
        raise ValueError("split_labels size must match markets size")
    ts_arr = (
        np.asarray(ts_ms, dtype=np.int64).reshape(-1)
        if ts_ms is not None
        else np.zeros(size, dtype=np.int64)
    )
    if ts_arr.size != size:
        raise ValueError("ts_ms size must match markets size")

    bucket_labels: list[str] = []
    bucket_counts: dict[str, int] = {}
    for idx in range(size):
        market = str(market_arr[idx] or "").strip().upper() or "UNKNOWN"
        split = str(split_arr[idx] or "").strip().lower() or "all"
        bucket_id = int(max(int(ts_arr[idx]), 0) // bucket_width)
        bucket_key = f"{market}|{split}|{bucket_id}"
        bucket_labels.append(bucket_key)
        bucket_counts[bucket_key] = bucket_counts.get(bucket_key, 0) + 1

    mean_bucket_count = float(np.mean(list(bucket_counts.values()))) if bucket_counts else 1.0
    raw_domain = np.asarray(
        [mean_bucket_count / float(max(bucket_counts.get(key, 1), 1)) for key in bucket_labels],
        dtype=np.float64,
    )
    raw_domain = np.clip(raw_domain, clip_floor, clip_ceiling)
    raw_mean = float(np.mean(raw_domain)) if raw_domain.size > 0 else 1.0
    if not np.isfinite(raw_mean) or raw_mean <= 0.0:
        raw_mean = 1.0
    domain = np.clip(raw_domain / raw_mean, clip_floor, clip_ceiling)
    final_weight = np.clip(base * data_quality * support * domain, 1e-6, None)
    non_trivial = bool(bucket_counts) and (
        len(bucket_counts) > 1 or np.any(np.abs(domain - 1.0) > 1e-6)
    )
    top_buckets = sorted(bucket_counts.items(), key=lambda item: (-int(item[1]), item[0]))[:8]
    details = {
        "enabled": bool(non_trivial),
        "policy": "v5_domain_weighting_v1",
        "source_kind": "regime_inverse_frequency_v1",
        "status": "inverse_frequency_ready",
        "clip_min": clip_floor,
        "clip_max": clip_ceiling,
        "temporal_bucket_ms": bucket_width,
        "bucket_count": int(len(bucket_counts)),
        "bucket_examples": [
            {"bucket": str(bucket), "rows": int(count)}
            for bucket, count in top_buckets
        ],
    }
    return {
        "base_sample_weight": base,
        "data_quality_weight": data_quality,
        "support_weight": support,
        "domain_weight": domain,
        "final_sample_weight": final_weight,
        "domain_details": details,
    }


def write_v5_domain_weighting_report(
    *,
    run_dir: Path,
    payload: dict[str, Any],
) -> Path:
    path = Path(run_dir) / "domain_weighting_report.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _to_float_array(values: np.ndarray | list[float] | tuple[float, ...]) -> np.ndarray:
    return np.asarray(values, dtype=np.float64).reshape(-1)


def _resolve_weight_component(
    values: np.ndarray | list[float] | tuple[float, ...] | None,
    *,
    size: int,
    default_value: float,
) -> np.ndarray:
    if values is None:
        return np.full(int(size), float(default_value), dtype=np.float64)
    arr = np.asarray(values, dtype=np.float64).reshape(-1)
    if arr.size != int(size):
        raise ValueError("weight component size mismatch")
    arr = np.where(np.isfinite(arr), arr, float(default_value))
    return np.clip(arr, 1e-6, None)


def _weight_summary(values: np.ndarray) -> dict[str, float]:
    arr = np.asarray(values, dtype=np.float64).reshape(-1)
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
