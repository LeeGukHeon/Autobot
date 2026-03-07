from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import polars as pl

from autobot.models.dataset_loader import build_dataset_request, load_feature_dataset


def test_dataset_loader_reads_sample_weight_column(tmp_path: Path) -> None:
    dataset_root = tmp_path / "features_v3"
    meta_dir = dataset_root / "_meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    (meta_dir / "feature_spec.json").write_text(json.dumps({"feature_columns": ["f_num"]}), encoding="utf-8")
    (meta_dir / "label_spec.json").write_text(json.dumps({"label_columns": ["y_reg", "y_cls"]}), encoding="utf-8")

    part_dir = dataset_root / "tf=5m" / "market=KRW-BTC" / "date=2026-01-01"
    part_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ts_ms": [1_700_000_000_000, 1_700_000_300_000, 1_700_000_600_000],
            "f_num": [0.1, 0.2, 0.3],
            "sample_weight": [1.0, 0.5, 0.25],
            "y_reg": [0.0, 0.01, 0.02],
            "y_cls": [0, 1, 1],
        }
    ).write_parquet(part_dir / "part-000.parquet")

    req = build_dataset_request(
        dataset_root=dataset_root,
        tf="5m",
        quote="KRW",
        top_n=20,
        start="2023-11-14",
        end="2023-11-14",
    )
    dataset = load_feature_dataset(req)

    assert dataset.rows == 3
    assert dataset.sample_weight.tolist() == [1.0, 0.5, 0.25]


def test_dataset_loader_supports_custom_label_columns(tmp_path: Path) -> None:
    dataset_root = tmp_path / "features_v4"
    meta_dir = dataset_root / "_meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    (meta_dir / "feature_spec.json").write_text(json.dumps({"feature_columns": ["f_num"]}), encoding="utf-8")
    (meta_dir / "label_spec.json").write_text(
        json.dumps({"label_columns": ["y_reg_net_12", "y_cls_topq_12"]}),
        encoding="utf-8",
    )

    part_dir = dataset_root / "tf=5m" / "market=KRW-BTC" / "date=2026-01-01"
    part_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ts_ms": [1_700_000_000_000, 1_700_000_300_000, 1_700_000_600_000],
            "f_num": [0.1, 0.2, 0.3],
            "sample_weight": [1.0, 0.5, 0.25],
            "y_reg_net_12": [0.0, 0.01, 0.02],
            "y_cls_topq_12": [0, 1, 1],
        }
    ).write_parquet(part_dir / "part-000.parquet")

    req = build_dataset_request(
        dataset_root=dataset_root,
        tf="5m",
        quote="KRW",
        top_n=20,
        start="2023-11-14",
        end="2023-11-14",
    )
    dataset = load_feature_dataset(req, y_cls_column="y_cls_topq_12", y_reg_column="y_reg_net_12")

    assert dataset.rows == 3
    assert dataset.y_cls.tolist() == [0, 1, 1]
    assert np.allclose(dataset.y_reg, np.array([0.0, 0.01, 0.02], dtype=np.float32))
