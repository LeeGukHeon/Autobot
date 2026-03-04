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
    f1 = np.linspace(-2.0, 2.0, num=240, dtype=np.float64)
    y_cls = (f1 > 0.0).astype(np.int8)
    y_reg = np.where(y_cls == 1, 0.01, -0.01).astype(np.float32)
    trade_source = np.where((np.arange(240) % 2) == 0, "rest", "ws")
    pl.DataFrame(
        {
            "ts_ms": ts,
            "f1": f1.astype(np.float32),
            "m_trade_source": trade_source,
            "m_micro_available": np.ones(240, dtype=bool),
            "y_cls": y_cls,
            "y_reg": y_reg,
        }
    ).write_parquet(part_dir / "part.parquet")


def test_compare_registered_models_returns_delta_payload(tmp_path: Path) -> None:
    dataset_root = tmp_path / "features_v2"
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
        "feature_columns": ["f1", "m_trade_source", "m_micro_available"],
        "train_ratio": 0.70,
        "valid_ratio": 0.15,
        "test_ratio": 0.15,
        "embargo_bars": 3,
        "fee_bps_est": 10.0,
        "safety_bps": 5.0,
    }

    save_run(
        RegistrySavePayload(
            registry_root=registry_root,
            model_family="train_v1",
            run_id="run_v1",
            model_bundle={"model_type": "xgboost", "scaler": None, "estimator": DummyEstimator(bias=-0.3)},
            metrics={},
            thresholds={},
            feature_spec={"feature_columns": ["f1", "m_trade_source", "m_micro_available"]},
            label_spec={"label_columns": ["y_reg", "y_cls"]},
            train_config=base_train_config,
            data_fingerprint={},
            leaderboard_row={"run_id": "run_v1", "test_precision_top5": 0.1},
            model_card_text="# v1",
        )
    )
    save_run(
        RegistrySavePayload(
            registry_root=registry_root,
            model_family="train_v2_micro",
            run_id="run_v2",
            model_bundle={"model_type": "xgboost", "scaler": None, "estimator": DummyEstimator(bias=0.3)},
            metrics={},
            thresholds={},
            feature_spec={"feature_columns": ["f1", "m_trade_source", "m_micro_available"]},
            label_spec={"label_columns": ["y_reg", "y_cls"]},
            train_config=base_train_config,
            data_fingerprint={},
            leaderboard_row={"run_id": "run_v2", "test_precision_top5": 0.2},
            model_card_text="# v2",
        )
    )

    compare = compare_registered_models(
        registry_root=registry_root,
        a_ref="run_v1",
        b_ref="run_v2",
        a_family="train_v1",
        b_family="train_v2_micro",
        split="test",
    )

    assert "delta" in compare
    assert "per_market" in compare
    assert compare["per_market"]["common_market_count"] == 1
    assert not math.isnan(float(compare["delta"]["precision_top5"]))
