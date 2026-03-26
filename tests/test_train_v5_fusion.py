from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from autobot.models.registry import load_json
from autobot.models.train_v5_fusion import TrainV5FusionOptions, train_and_register_v5_fusion


def test_train_v5_fusion_writes_core_contract_artifacts(tmp_path: Path) -> None:
    panel_path = tmp_path / "panel.parquet"
    sequence_path = tmp_path / "sequence.parquet"
    lob_path = tmp_path / "lob.parquet"

    rows = []
    for idx in range(18):
        ts_ms = 1_774_569_600_000 + (idx * 60_000)
        split = "train" if idx < 10 else ("valid" if idx < 14 else "test")
        y_reg = 0.01 * idx
        y_cls = 1 if y_reg > 0.05 else 0
        rows.append(
            {
                "market": "KRW-BTC",
                "ts_ms": ts_ms,
                "split": split,
                "y_cls": y_cls,
                "y_reg": y_reg,
                "final_rank_score": 0.2 + (idx * 0.02),
                "final_expected_return": y_reg * 0.9,
                "final_expected_es": abs(y_reg) * 0.3,
                "final_tradability": 0.8,
                "final_uncertainty": 0.05,
                "final_alpha_lcb": (y_reg * 0.9) - (abs(y_reg) * 0.3) - 0.05,
            }
        )
    pl.DataFrame(rows).write_parquet(panel_path)

    seq_rows = []
    lob_rows = []
    for row in rows:
        seq_rows.append(
            {
                "market": row["market"],
                "ts_ms": row["ts_ms"],
                "split": row["split"],
                "y_cls": row["y_cls"],
                "y_reg": row["y_reg"],
                "directional_probability_primary": 0.3 + (0.01 * (row["ts_ms"] % 7)),
                "final_expected_return": row["y_reg"] * 0.8,
                "final_expected_es": abs(row["y_reg"]) * 0.25,
                "final_tradability": 0.7,
                "final_uncertainty": 0.04,
                "final_alpha_lcb": (row["y_reg"] * 0.8) - (abs(row["y_reg"]) * 0.25) - 0.04,
            }
        )
        lob_rows.append(
            {
                "market": row["market"],
                "ts_ms": row["ts_ms"],
                "split": row["split"],
                "y_cls": row["y_cls"],
                "y_reg": row["y_reg"],
                "micro_alpha_1s": row["y_reg"] * 0.2,
                "micro_alpha_5s": row["y_reg"] * 0.4,
                "micro_alpha_30s": row["y_reg"] * 0.6,
                "micro_uncertainty": 0.03,
                "final_expected_return": row["y_reg"] * 0.6,
                "final_expected_es": abs(row["y_reg"]) * 0.2,
                "final_tradability": 0.75,
                "final_uncertainty": 0.03,
                "final_alpha_lcb": (row["y_reg"] * 0.6) - (abs(row["y_reg"]) * 0.2) - 0.03,
            }
        )
    pl.DataFrame(seq_rows).write_parquet(sequence_path)
    pl.DataFrame(lob_rows).write_parquet(lob_path)

    options = TrainV5FusionOptions(
        panel_input_path=panel_path,
        sequence_input_path=sequence_path,
        lob_input_path=lob_path,
        registry_root=tmp_path / "registry",
        logs_root=tmp_path / "logs",
        model_family="train_v5_fusion",
        quote="KRW",
        start="2026-03-27",
        end="2026-03-27",
        seed=7,
        stacker_family="linear",
    )
    result = train_and_register_v5_fusion(options)

    assert result.run_dir.exists()
    assert result.fusion_model_contract_path.exists()
    assert result.predictor_contract_path.exists()
    assert result.walk_forward_report_path.exists()
    assert result.promotion_path.exists()
    assert load_json(result.run_dir / "train_config.yaml")["trainer"] == "v5_fusion"
    assert load_json(result.fusion_model_contract_path)["policy"] == "v5_fusion_v1"
    assert load_json(result.predictor_contract_path)["final_rank_score_field"] == "final_rank_score"
