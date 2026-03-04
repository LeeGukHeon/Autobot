from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl

from autobot.models.modelbt_proxy import ModelBtProxyOptions, run_modelbt_proxy
from autobot.models.registry import RegistrySavePayload, save_run


class DummyEstimator:
    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        logits = x[:, 0].astype(np.float64)
        probs = 1.0 / (1.0 + np.exp(-logits))
        probs = np.clip(probs, 1e-6, 1.0 - 1e-6)
        return np.column_stack([1.0 - probs, probs])


def test_modelbt_proxy_writes_outputs(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    dataset_root = tmp_path / "features_v3"
    registry_root = tmp_path / "registry"
    out_root = tmp_path / "backtest"

    _write_feature_dataset(dataset_root)
    _write_candles_dataset(parquet_root / "candles_api_v1")

    save_run(
        RegistrySavePayload(
            registry_root=registry_root,
            model_family="train_v3_mtf_micro",
            run_id="run_v3",
            model_bundle={"model_type": "xgboost", "scaler": None, "estimator": DummyEstimator()},
            metrics={},
            thresholds={},
            feature_spec={"feature_columns": ["f1"]},
            label_spec={"label_columns": ["y_reg", "y_cls"]},
            train_config={
                "dataset_root": str(dataset_root),
                "feature_columns": ["f1"],
                "batch_rows": 1000,
            },
            data_fingerprint={},
            leaderboard_row={"run_id": "run_v3", "test_precision_top5": 0.1},
            model_card_text="# v3",
        )
    )

    result = run_modelbt_proxy(
        ModelBtProxyOptions(
            registry_root=registry_root,
            parquet_root=parquet_root,
            base_candles_dataset="candles_api_v1",
            out_root=out_root,
            model_ref="run_v3",
            model_family="train_v3_mtf_micro",
            tf="5m",
            quote="KRW",
            top_n=20,
            start="2026-01-01",
            end="2026-01-01",
            top_pct=0.5,
            hold_bars=1,
            fee_bps=5.0,
        )
    )

    assert result.equity_csv.exists()
    assert result.summary_json.exists()
    assert result.trades_csv.exists()
    assert result.diagnostics_json.exists()
    assert int(result.summary.get("trades_count", 0)) > 0


def _write_feature_dataset(dataset_root: Path) -> None:
    part_dir = dataset_root / "tf=5m" / "market=KRW-BTC"
    part_dir.mkdir(parents=True, exist_ok=True)
    base_ts = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    ts = [base_ts + i * 300_000 for i in range(20)]
    pl.DataFrame(
        {
            "ts_ms": ts,
            "f1": np.linspace(-1.0, 1.0, num=20, dtype=np.float64),
            "sample_weight": np.ones(20, dtype=np.float64),
            "y_reg": np.linspace(-0.01, 0.02, num=20, dtype=np.float64),
            "y_cls": [0 if i < 10 else 1 for i in range(20)],
        }
    ).write_parquet(part_dir / "part.parquet")


def _write_candles_dataset(dataset_root: Path) -> None:
    part_dir = dataset_root / "tf=5m" / "market=KRW-BTC"
    part_dir.mkdir(parents=True, exist_ok=True)
    base_ts = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    ts = [base_ts + i * 300_000 for i in range(30)]
    close = [100.0 + i * 0.2 for i in range(30)]
    pl.DataFrame(
        {
            "ts_ms": ts,
            "open": [value - 0.1 for value in close],
            "high": [value + 0.2 for value in close],
            "low": [value - 0.2 for value in close],
            "close": close,
            "volume_base": [100.0 + i for i in range(30)],
            "volume_quote": [close[i] * (100.0 + i) for i in range(30)],
            "volume_quote_est": [False for _ in close],
        }
    ).write_parquet(part_dir / "part.parquet")
