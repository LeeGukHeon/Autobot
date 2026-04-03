from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np


def build_ood_generalization_report(
    *,
    run_id: str,
    trainer_name: str,
    model_family: str,
    source_kind: str,
    markets: np.ndarray | list[str] | tuple[str, ...],
    split_labels: np.ndarray | list[str] | tuple[str, ...],
    effective_sample_weight: np.ndarray | list[float] | tuple[float, ...],
    invariant_penalty_enabled: bool,
    regime_bucket_labels: np.ndarray | list[str] | tuple[str, ...] | None = None,
    extra_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    market_arr = np.asarray(markets, dtype=object).reshape(-1)
    split_arr = np.asarray(split_labels, dtype=object).reshape(-1)
    weight_arr = np.asarray(effective_sample_weight, dtype=np.float64).reshape(-1)
    bucket_arr = (
        np.asarray(regime_bucket_labels, dtype=object).reshape(-1)
        if regime_bucket_labels is not None
        else np.asarray(["all"] * int(market_arr.size), dtype=object)
    )
    train_count = int(np.sum(split_arr == "train"))
    future_count = int(np.sum(np.isin(split_arr, ["valid", "test"])))
    market_counts: dict[str, dict[str, int]] = {}
    for market, split in zip(market_arr, split_arr):
        market_text = str(market or "").strip().upper() or "UNKNOWN"
        split_text = str(split or "").strip().lower() or "unknown"
        market_counts.setdefault(market_text, {})
        market_counts[market_text][split_text] = int(market_counts[market_text].get(split_text, 0) + 1)
    bucket_counts: dict[str, int] = {}
    for bucket in bucket_arr:
        bucket_text = str(bucket or "").strip() or "all"
        bucket_counts[bucket_text] = int(bucket_counts.get(bucket_text, 0) + 1)
    return {
        "policy": "ood_generalization_report_v1",
        "run_id": str(run_id),
        "trainer": str(trainer_name),
        "model_family": str(model_family),
        "status": "informative_ready",
        "source_kind": str(source_kind),
        "invariant_penalty_enabled": bool(invariant_penalty_enabled),
        "train_vs_future_domain_gap_summary": {
            "train_rows": train_count,
            "future_rows": future_count,
            "future_to_train_ratio": float(future_count / max(train_count, 1)),
            "per_market_split_counts": market_counts,
        },
        "regime_bucket_balance": {
            "bucket_count": int(len(bucket_counts)),
            "bucket_examples": [
                {"bucket": str(bucket), "rows": int(count)}
                for bucket, count in sorted(bucket_counts.items(), key=lambda item: (-int(item[1]), item[0]))[:8]
            ],
        },
        "effective_ood_weight_summary": _weight_summary(weight_arr),
        "extra_summary": dict(extra_summary or {}),
    }


def write_ood_generalization_report(*, run_dir: Path, payload: dict[str, Any]) -> Path:
    path = Path(run_dir) / "ood_generalization_report.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


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
