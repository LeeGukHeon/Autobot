from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np

from autobot import cli as cli_module
from autobot.models.runtime_feature_dataset import write_runtime_feature_dataset


def test_cli_model_inspect_runtime_dataset_outputs_summary(monkeypatch, tmp_path, capsys) -> None:
    dataset_root = tmp_path / "runtime_feature_dataset"
    write_runtime_feature_dataset(
        output_root=dataset_root,
        tf="5m",
        feature_columns=("feature_a",),
        markets=np.asarray(["KRW-BTC", "KRW-BTC"], dtype=object),
        ts_ms=np.asarray([1_774_656_000_000, 1_774_656_300_000], dtype=np.int64),
        x=np.asarray([[0.1], [0.2]], dtype=np.float64),
        y_cls=np.asarray([1, 0], dtype=np.int64),
        y_reg=np.asarray([0.1, -0.1], dtype=np.float64),
        y_rank=np.asarray([0.5, 0.4], dtype=np.float64),
        sample_weight=np.asarray([1.0, 1.0], dtype=np.float64),
    )
    monkeypatch.chdir(Path(__file__).resolve().parents[1])
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "autobot.cli",
            "model",
            "inspect-runtime-dataset",
            "--dataset-root",
            str(dataset_root),
        ],
    )

    exit_code = cli_module.main()

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["exists"] is True
    assert payload["manifest_exists"] is True
    assert payload["rows"] == 2
    assert payload["data_file_count"] == 1
