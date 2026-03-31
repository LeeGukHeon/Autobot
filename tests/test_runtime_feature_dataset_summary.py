from __future__ import annotations

import numpy as np

from autobot.models.runtime_feature_dataset import summarize_runtime_feature_dataset, write_runtime_feature_dataset


def test_summarize_runtime_feature_dataset_reports_actual_bounds(tmp_path) -> None:
    dataset_root = tmp_path / "runtime_feature_dataset"
    write_runtime_feature_dataset(
        output_root=dataset_root,
        tf="5m",
        feature_columns=("feature_a",),
        markets=np.asarray(["KRW-BTC", "KRW-BTC", "KRW-ETH"], dtype=object),
        ts_ms=np.asarray([1_774_656_000_000, 1_774_656_300_000, 1_774_656_600_000], dtype=np.int64),
        x=np.asarray([[0.1], [0.2], [0.3]], dtype=np.float64),
        y_cls=np.asarray([1, 0, 1], dtype=np.int64),
        y_reg=np.asarray([0.1, -0.1, 0.2], dtype=np.float64),
        y_rank=np.asarray([0.5, 0.4, 0.6], dtype=np.float64),
        sample_weight=np.asarray([1.0, 1.0, 1.0], dtype=np.float64),
        extra_columns={"runtime_score": np.asarray([0.7, 0.1, 0.8], dtype=np.float64)},
    )

    summary = summarize_runtime_feature_dataset(dataset_root)

    assert summary["exists"] is True
    assert summary["manifest_exists"] is True
    assert summary["data_file_count"] == 2
    assert summary["rows"] == 3
    assert summary["min_ts_ms"] == 1_774_656_000_000
    assert summary["max_ts_ms"] == 1_774_656_600_000
    assert summary["markets"] == ["KRW-BTC", "KRW-ETH"]
