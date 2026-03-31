"""Helpers for writing and inspecting runtime-loadable feature datasets."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl


def write_runtime_feature_dataset(
    *,
    output_root: Path,
    tf: str,
    feature_columns: tuple[str, ...],
    markets: np.ndarray,
    ts_ms: np.ndarray,
    x: np.ndarray,
    y_cls: np.ndarray,
    y_reg: np.ndarray,
    y_rank: np.ndarray,
    sample_weight: np.ndarray,
    extra_columns: dict[str, np.ndarray] | None = None,
) -> Path:
    root = Path(output_root)
    meta_root = root / "_meta"
    meta_root.mkdir(parents=True, exist_ok=True)
    tf_value = str(tf).strip().lower() or "5m"
    feature_names = tuple(str(name).strip() for name in feature_columns if str(name).strip())
    extras = {str(name).strip(): np.asarray(values) for name, values in dict(extra_columns or {}).items() if str(name).strip()}

    frame_payload: dict[str, Any] = {
        "market": np.asarray(markets, dtype=object),
        "ts_ms": np.asarray(ts_ms, dtype=np.int64),
        "y_cls": np.asarray(y_cls, dtype=np.int64),
        "y_reg": np.asarray(y_reg, dtype=np.float64),
        "y_rank": np.asarray(y_rank, dtype=np.float64),
        "sample_weight": np.asarray(sample_weight, dtype=np.float64),
    }
    for idx, name in enumerate(feature_names):
        frame_payload[name] = np.asarray(x[:, idx], dtype=np.float64)
    for name, values in extras.items():
        frame_payload[name] = np.asarray(values)
    frame = pl.DataFrame(frame_payload).sort(["market", "ts_ms"])

    manifest_rows: list[dict[str, Any]] = []
    for market in sorted({str(item).strip().upper() for item in frame.get_column("market").to_list() if str(item).strip()}):
        market_frame = frame.filter(pl.col("market") == market).sort("ts_ms")
        market_dir = root / f"tf={tf_value}" / f"market={market}"
        market_dir.mkdir(parents=True, exist_ok=True)
        part_path = market_dir / "part-000.parquet"
        market_frame.write_parquet(part_path)
        manifest_rows.append(
            {
                "tf": tf_value,
                "market": market,
                "rows": int(market_frame.height),
                "start_ts_ms": int(market_frame.get_column("ts_ms").min()) if market_frame.height > 0 else None,
                "end_ts_ms": int(market_frame.get_column("ts_ms").max()) if market_frame.height > 0 else None,
                "part_path": str(part_path),
            }
        )

    feature_spec = {
        "feature_columns": list(feature_names),
        "tf": tf_value,
        "extra_runtime_columns": sorted(extras.keys()),
    }
    label_spec = {
        "training_default_columns": {
            "y_cls": "y_cls",
            "y_reg": "y_reg",
            "y_rank": "y_rank",
        },
        "label_columns": ["y_cls", "y_reg", "y_rank"],
    }
    (meta_root / "feature_spec.json").write_text(json.dumps(feature_spec, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (meta_root / "label_spec.json").write_text(json.dumps(label_spec, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    pl.DataFrame(manifest_rows).write_parquet(meta_root / "manifest.parquet")
    return root


def summarize_runtime_feature_dataset(dataset_root: Path) -> dict[str, Any]:
    root = Path(dataset_root)
    manifest_path = root / "_meta" / "manifest.parquet"
    data_files = sorted(root.glob("tf=*/market=*/part-*.parquet"))
    summary: dict[str, Any] = {
        "dataset_root": str(root),
        "manifest_path": str(manifest_path),
        "data_file_count": len(data_files),
        "rows": 0,
        "min_ts_ms": None,
        "max_ts_ms": None,
        "markets": [],
        "exists": root.exists(),
        "manifest_exists": manifest_path.exists(),
    }
    if not root.exists() or not manifest_path.exists():
        return summary

    manifest = pl.read_parquet(manifest_path)
    if manifest.height <= 0:
        return summary

    rows = 0
    if "rows" in manifest.columns:
        rows = int(sum(int(value or 0) for value in manifest.get_column("rows").to_list()))

    min_ts_ms = None
    if "start_ts_ms" in manifest.columns:
        start_values = [int(value) for value in manifest.get_column("start_ts_ms").drop_nulls().to_list()]
        if start_values:
            min_ts_ms = min(start_values)

    max_ts_ms = None
    if "end_ts_ms" in manifest.columns:
        end_values = [int(value) for value in manifest.get_column("end_ts_ms").drop_nulls().to_list()]
        if end_values:
            max_ts_ms = max(end_values)

    markets: list[str] = []
    if "market" in manifest.columns:
        markets = sorted({str(value).strip() for value in manifest.get_column("market").drop_nulls().to_list() if str(value).strip()})

    summary.update(
        {
            "rows": rows,
            "min_ts_ms": min_ts_ms,
            "max_ts_ms": max_ts_ms,
            "markets": markets,
        }
    )
    return summary
