from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import polars as pl

from autobot.models.registry import RegistrySavePayload, save_run
from autobot.models.train_v2_micro import compare_registered_models


class DummyEstimator:
    def __init__(self, *, bias: float) -> None:
        self.bias = float(bias)

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        logits = x[:, 0].astype(np.float64) + self.bias
        probs = 1.0 / (1.0 + np.exp(-logits))
        probs = np.clip(probs, 1e-6, 1.0 - 1e-6)
        return np.column_stack([1.0 - probs, probs])


def _write_dataset(dataset_root: Path) -> None:
    part_dir = dataset_root / "tf=5m" / "market=KRW-BTC"
    part_dir.mkdir(parents=True, exist_ok=True)
    ts = np.array([1_700_000_000_000 + (i * 300_000) for i in range(240)], dtype=np.int64)
    f1 = np.linspace(-1.5, 1.5, num=240, dtype=np.float64)
    y_cls = (f1 > 0.1).astype(np.int8)
    y_reg = np.where(y_cls == 1, 0.012, -0.009).astype(np.float32)
    pl.DataFrame(
        {
            "ts_ms": ts,
            "f1": f1.astype(np.float32),
            "y_cls_topq_12": y_cls,
            "y_reg_net_12": y_reg,
        }
    ).write_parquet(part_dir / "part.parquet")


def test_compare_registered_models_supports_custom_v4_label_columns(tmp_path: Path) -> None:
    dataset_root = tmp_path / "features_v4"
    registry_root = tmp_path / "registry"
    _write_dataset(dataset_root)

    base_train_config = {
        "dataset_root": str(dataset_root),
        "tf": "5m",
        "quote": "KRW",
        "top_n": 20,
        "start_ts_ms": 1_700_000_000_000,
        "end_ts_ms": 1_700_000_000_000 + (239 * 300_000),
        "markets": ["KRW-BTC"],
        "batch_rows": 1000,
        "feature_columns": ["f1"],
        "train_ratio": 0.70,
        "valid_ratio": 0.15,
        "test_ratio": 0.15,
        "embargo_bars": 3,
        "fee_bps_est": 10.0,
        "safety_bps": 5.0,
        "y_cls_column": "y_cls_topq_12",
        "y_reg_column": "y_reg_net_12",
        "label_columns": ["y_cls_topq_12", "y_reg_net_12"],
    }

    save_run(
        RegistrySavePayload(
            registry_root=registry_root,
            model_family="train_v4_crypto_cs",
            run_id="run_a",
            model_bundle={"model_type": "xgboost", "scaler": None, "estimator": DummyEstimator(bias=-0.2)},
            metrics={},
            thresholds={},
            feature_spec={"feature_columns": ["f1"]},
            label_spec={"label_columns": ["y_cls_topq_12", "y_reg_net_12"]},
            train_config=base_train_config,
            data_fingerprint={},
            leaderboard_row={"run_id": "run_a", "test_precision_top5": 0.1},
            model_card_text="# a",
        )
    )
    save_run(
        RegistrySavePayload(
            registry_root=registry_root,
            model_family="train_v4_crypto_cs",
            run_id="run_b",
            model_bundle={"model_type": "xgboost", "scaler": None, "estimator": DummyEstimator(bias=0.2)},
            metrics={},
            thresholds={},
            feature_spec={"feature_columns": ["f1"]},
            label_spec={"label_columns": ["y_cls_topq_12", "y_reg_net_12"]},
            train_config=base_train_config,
            data_fingerprint={},
            leaderboard_row={"run_id": "run_b", "test_precision_top5": 0.2},
            model_card_text="# b",
        )
    )

    compare = compare_registered_models(
        registry_root=registry_root,
        a_ref="run_a",
        b_ref="run_b",
        a_family="train_v4_crypto_cs",
        b_family="train_v4_crypto_cs",
        split="test",
    )

    assert compare["a"]["metrics"]["label_columns"]["y_cls"] == "y_cls_topq_12"
    assert compare["b"]["metrics"]["label_columns"]["y_reg"] == "y_reg_net_12"
    assert not math.isnan(float(compare["delta"]["precision_top5"]))
