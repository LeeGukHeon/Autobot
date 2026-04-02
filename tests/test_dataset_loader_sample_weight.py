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

    part_dir = dataset_root / "tf=5m" / "market=KRW-BTC" / "date=2023-11-14"
    part_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ts_ms": [1_699_920_000_000, 1_699_920_300_000, 1_699_920_600_000],
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

    part_dir = dataset_root / "tf=5m" / "market=KRW-BTC" / "date=2023-11-14"
    part_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ts_ms": [1_699_920_000_000, 1_699_920_300_000, 1_699_920_600_000],
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


def test_dataset_loader_applies_certification_market_weight_and_quality_ordering(tmp_path: Path) -> None:
    dataset_root = tmp_path / "features_v4"
    meta_dir = dataset_root / "_meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    (meta_dir / "feature_spec.json").write_text(json.dumps({"feature_columns": ["f_num"]}), encoding="utf-8")
    (meta_dir / "label_spec.json").write_text(
        json.dumps({"label_columns": ["y_reg_net_12", "y_cls_topq_12"]}),
        encoding="utf-8",
    )
    (meta_dir / "feature_dataset_certification.json").write_text(
        json.dumps(
            {
                "market_quality_budget": [
                    {
                        "market": "KRW-BTC",
                        "training_weight_multiplier": 0.25,
                        "universe_quality_score": 1.0,
                        "selected_for_universe": False,
                    },
                    {
                        "market": "KRW-ETH",
                        "training_weight_multiplier": 0.5,
                        "universe_quality_score": 10.0,
                        "selected_for_universe": True,
                    },
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    btc_part = dataset_root / "tf=5m" / "market=KRW-BTC" / "date=2023-11-14"
    btc_part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ts_ms": [1_699_920_000_000],
            "f_num": [0.1],
            "sample_weight": [1.0],
            "y_reg_net_12": [0.0],
            "y_cls_topq_12": [0],
        }
    ).write_parquet(btc_part / "part-000.parquet")

    eth_part = dataset_root / "tf=5m" / "market=KRW-ETH" / "date=2023-11-14"
    eth_part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ts_ms": [1_699_920_300_000],
            "f_num": [0.2],
            "sample_weight": [1.0],
            "y_reg_net_12": [0.01],
            "y_cls_topq_12": [1],
        }
    ).write_parquet(eth_part / "part-000.parquet")

    req = build_dataset_request(
        dataset_root=dataset_root,
        tf="5m",
        quote="KRW",
        top_n=1,
        start="2023-11-14",
        end="2023-11-14",
    )
    dataset = load_feature_dataset(req, y_cls_column="y_cls_topq_12", y_reg_column="y_reg_net_12")

    assert dataset.selected_markets == ("KRW-ETH",)
    assert dataset.rows == 1
    assert dataset.sample_weight.tolist() == [0.5]
