from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from autobot.models.dataset_loader import build_dataset_request, load_feature_dataset


def test_dataset_loader_reads_date_partitions_and_encodes_trade_source(tmp_path: Path) -> None:
    dataset_root = tmp_path / "features_v2"
    meta_dir = dataset_root / "_meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    (meta_dir / "feature_spec.json").write_text(
        json.dumps({"feature_columns": ["f_num", "m_trade_source", "m_micro_available"]}),
        encoding="utf-8",
    )
    (meta_dir / "label_spec.json").write_text(
        json.dumps({"label_columns": ["y_reg", "y_cls"]}),
        encoding="utf-8",
    )

    part_dir = dataset_root / "tf=5m" / "market=KRW-BTC" / "date=2026-01-01"
    part_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ts_ms": [1_700_000_000_000, 1_700_000_300_000, 1_700_000_600_000],
            "f_num": [0.1, 0.2, 0.3],
            "m_trade_source": ["none", "rest", "ws"],
            "m_micro_available": [False, True, True],
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
    assert dataset.feature_names == ("f_num", "m_trade_source", "m_micro_available")
    encoded_trade_source = dataset.X[:, 1].tolist()
    assert encoded_trade_source == [0.0, 1.0, 2.0]
