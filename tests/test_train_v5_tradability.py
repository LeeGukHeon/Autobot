from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from autobot.models.train_v5_tradability import (
    TrainV5TradabilityOptions,
    _load_private_execution_rows,
    materialize_v5_tradability_runtime_export,
    train_and_register_v5_tradability,
)


def _write_table(path: Path, rows: list[dict[str, object]], *, train_config: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)
    (path.parent / "train_config.yaml").write_text(json.dumps(train_config, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    (path.parent / "runtime_recommendations.json").write_text(json.dumps({"status": "ready"}, ensure_ascii=False), encoding="utf-8")


def test_train_v5_tradability_writes_core_contract_artifacts(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    logs_root = tmp_path / "logs"
    private_root = tmp_path / "data" / "parquet" / "private_execution_v1"
    panel_path = tmp_path / "panel" / "expert_prediction_table.parquet"
    sequence_path = tmp_path / "sequence" / "expert_prediction_table.parquet"
    lob_path = tmp_path / "lob" / "expert_prediction_table.parquet"
    base_rows = [
        {"market": "KRW-BTC", "ts_ms": 1_774_569_600_000, "split": "train", "y_cls": 1, "y_reg": 0.1, "final_rank_score": 0.7, "final_expected_return": 0.12, "final_expected_es": 0.02, "final_tradability": 0.6, "final_uncertainty": 0.05, "final_alpha_lcb": 0.05},
        {"market": "KRW-BTC", "ts_ms": 1_774_570_200_000, "split": "valid", "y_cls": 1, "y_reg": 0.1, "final_rank_score": 0.72, "final_expected_return": 0.13, "final_expected_es": 0.02, "final_tradability": 0.6, "final_uncertainty": 0.05, "final_alpha_lcb": 0.06},
        {"market": "KRW-BTC", "ts_ms": 1_774_570_800_000, "split": "test", "y_cls": 1, "y_reg": 0.1, "final_rank_score": 0.75, "final_expected_return": 0.15, "final_expected_es": 0.02, "final_tradability": 0.6, "final_uncertainty": 0.05, "final_alpha_lcb": 0.08},
    ]
    _write_table(
        panel_path,
        base_rows,
        train_config={"model_family": "train_v5_panel_ensemble", "trainer": "v5_panel_ensemble", "data_platform_ready_snapshot_id": "snapshot-1"},
    )
    _write_table(
        sequence_path,
        [
            {"market": row["market"], "ts_ms": row["ts_ms"], "directional_probability_primary": 0.6, "sequence_uncertainty_primary": 0.04}
            for row in base_rows
        ],
        train_config={"model_family": "train_v5_sequence", "trainer": "v5_sequence", "data_platform_ready_snapshot_id": "snapshot-1"},
    )
    _write_table(
        lob_path,
        [
            {"market": row["market"], "ts_ms": row["ts_ms"], "micro_alpha_1s": 0.01, "micro_alpha_5s": 0.02, "micro_alpha_30s": 0.03, "micro_uncertainty": 0.02}
            for row in base_rows
        ],
        train_config={"model_family": "train_v5_lob", "trainer": "v5_lob", "data_platform_ready_snapshot_id": "snapshot-1"},
    )
    private_meta = private_root / "_meta"
    private_meta.mkdir(parents=True, exist_ok=True)
    (private_meta / "build_report.json").write_text(json.dumps({"status": "PASS"}, ensure_ascii=False), encoding="utf-8")
    (private_meta / "validate_report.json").write_text(json.dumps({"status": "PASS", "pass": True}, ensure_ascii=False), encoding="utf-8")
    private_part = private_root / "market=KRW-BTC" / "date=2026-03-27"
    private_part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "market": ["KRW-BTC", "KRW-BTC", "KRW-BTC"],
            "ts_ms": [1_774_569_600_000, 1_774_570_200_000, 1_774_570_800_000],
            "decision_bucket_ts_ms": [1_774_569_600_000, 1_774_570_200_000, 1_774_570_800_000],
            "decision_bar_interval_ms": [60_000, 60_000, 60_000],
            "y_tradeable": [1, 1, 0],
            "y_fill_within_deadline": [1, 1, 1],
            "y_shortfall_bps": [1.0, 1.2, 4.0],
            "y_adverse_tolerance": [1, 1, 0],
        }
    ).write_parquet(private_part / "part-000.parquet")

    result = train_and_register_v5_tradability(
        TrainV5TradabilityOptions(
            panel_input_path=panel_path,
            sequence_input_path=sequence_path,
            lob_input_path=lob_path,
            private_execution_root=private_root,
            registry_root=registry_root,
            logs_root=logs_root,
            model_family="train_v5_tradability",
                quote="KRW",
                start="2026-03-27",
                end="2026-03-27",
                seed=42,
            )
        )

    assert result.run_dir.exists()
    assert result.tradability_model_contract_path.exists()
    assert result.predictor_contract_path.exists()
    assert (result.run_dir / "expert_prediction_table.parquet").exists()
    assert (result.run_dir / "runtime_recommendations.json").exists()
    train_config = json.loads((result.run_dir / "train_config.yaml").read_text(encoding="utf-8"))
    assert "y_tradeable" not in train_config["feature_columns"]
    assert "y_fill_within_deadline" not in train_config["feature_columns"]
    assert "y_shortfall_bps" not in train_config["feature_columns"]
    assert "y_adverse_tolerance" not in train_config["feature_columns"]


def test_materialize_v5_tradability_runtime_export_uses_runtime_inputs(tmp_path: Path) -> None:
    run_dir = tmp_path / "registry" / "train_v5_tradability" / "run-001"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "train_config.yaml").write_text(
        json.dumps(
            {
                "model_family": "train_v5_tradability",
                "feature_columns": [
                    "panel_final_rank_score",
                    "panel_final_expected_return",
                    "panel_final_expected_es",
                    "panel_final_tradability",
                    "panel_final_uncertainty",
                    "panel_final_alpha_lcb",
                    "sequence_directional_probability_primary",
                    "sequence_sequence_uncertainty_primary",
                    "lob_micro_alpha_1s",
                    "lob_micro_alpha_5s",
                    "lob_micro_alpha_30s",
                    "lob_micro_uncertainty",
                ],
                "data_platform_ready_snapshot_id": "snapshot-1",
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    import joblib
    from sklearn.linear_model import LogisticRegression, Ridge
    from autobot.models.train_v5_tradability import V5TradabilityEstimator

    x = [[0.7, 0.12, 0.02, 0.6, 0.05, 0.05, 0.6, 0.04, 0.01, 0.02, 0.03, 0.02], [0.2, 0.01, 0.05, 0.3, 0.1, -0.14, 0.2, 0.08, -0.01, 0.0, 0.01, 0.05]]
    yb = [1, 0]
    yr = [1.0, 5.0]
    estimator = V5TradabilityEstimator(
        tradability_model=LogisticRegression(max_iter=1000).fit(x, yb),
        fill_model=LogisticRegression(max_iter=1000).fit(x, yb),
        shortfall_model=Ridge(alpha=1.0).fit(x, yr),
        adverse_model=LogisticRegression(max_iter=1000).fit(x, yb),
        uncertainty_model=Ridge(alpha=1.0).fit(x, [0.1, 0.2]),
        feature_names=tuple(json.loads((run_dir / "train_config.yaml").read_text(encoding="utf-8"))["feature_columns"]),
    )
    joblib.dump({"estimator": estimator}, run_dir / "model.bin")

    panel_path = tmp_path / "panel_runtime.parquet"
    pl.DataFrame(
        {
            "market": ["KRW-BTC"],
            "ts_ms": [1_774_569_600_000],
            "split": ["runtime"],
            "y_cls": [1],
            "y_reg": [0.1],
            "final_rank_score": [0.7],
            "final_expected_return": [0.12],
            "final_expected_es": [0.02],
            "final_tradability": [0.6],
            "final_uncertainty": [0.05],
            "final_alpha_lcb": [0.05],
        }
    ).write_parquet(panel_path)
    (panel_path.parent / "train_config.yaml").write_text(json.dumps({"model_family": "train_v5_panel_ensemble", "data_platform_ready_snapshot_id": "snapshot-1"}), encoding="utf-8")
    (panel_path.parent / "runtime_recommendations.json").write_text("{}", encoding="utf-8")

    sequence_path = tmp_path / "sequence_runtime.parquet"
    pl.DataFrame({"market": ["KRW-BTC"], "ts_ms": [1_774_569_600_000], "directional_probability_primary": [0.6], "sequence_uncertainty_primary": [0.04]}).write_parquet(sequence_path)
    (sequence_path.parent / "train_config.yaml").write_text(json.dumps({"model_family": "train_v5_sequence", "data_platform_ready_snapshot_id": "snapshot-1"}), encoding="utf-8")
    (sequence_path.parent / "runtime_recommendations.json").write_text("{}", encoding="utf-8")

    lob_path = tmp_path / "lob_runtime.parquet"
    pl.DataFrame({"market": ["KRW-BTC"], "ts_ms": [1_774_569_600_000], "micro_alpha_1s": [0.01], "micro_alpha_5s": [0.02], "micro_alpha_30s": [0.03], "micro_uncertainty": [0.02]}).write_parquet(lob_path)
    (lob_path.parent / "train_config.yaml").write_text(json.dumps({"model_family": "train_v5_lob", "data_platform_ready_snapshot_id": "snapshot-1"}), encoding="utf-8")
    (lob_path.parent / "runtime_recommendations.json").write_text("{}", encoding="utf-8")

    payload = materialize_v5_tradability_runtime_export(
        run_dir=run_dir,
        start="2026-03-28",
        end="2026-03-28",
        panel_runtime_input_path=panel_path,
        sequence_runtime_input_path=sequence_path,
        lob_runtime_input_path=lob_path,
    )

    assert payload["trainer"] == "v5_tradability"
    assert Path(payload["export_path"]).exists()


def test_load_private_execution_rows_tolerates_nullable_schema_drift_between_partitions(tmp_path: Path) -> None:
    root = tmp_path / "data" / "parquet" / "private_execution_v1"
    part_a = root / "market=KRW-BTC" / "date=2026-03-27"
    part_b = root / "market=KRW-BTC" / "date=2026-03-28"
    part_a.mkdir(parents=True, exist_ok=True)
    part_b.mkdir(parents=True, exist_ok=True)

    pl.DataFrame(
        {
            "market": ["KRW-BTC"],
            "ts_ms": [1_774_569_600_000],
            "decision_bucket_ts_ms": [1_774_569_600_000],
            "decision_bar_interval_ms": [300_000],
            "first_fill_ts_ms": [None],
            "y_tradeable": [1],
            "y_fill_within_deadline": [1],
            "y_shortfall_bps": [1.0],
            "y_adverse_tolerance": [1],
        }
    ).write_parquet(part_a / "part-000.parquet")
    pl.DataFrame(
        {
            "market": ["KRW-BTC"],
            "ts_ms": [1_774_656_000_000],
            "decision_bucket_ts_ms": [1_774_656_000_000],
            "decision_bar_interval_ms": [300_000],
            "first_fill_ts_ms": [1_774_656_010_000],
            "y_tradeable": [0],
            "y_fill_within_deadline": [0],
            "y_shortfall_bps": [3.0],
            "y_adverse_tolerance": [0],
        }
    ).write_parquet(part_b / "part-000.parquet")

    frame = _load_private_execution_rows(dataset_root=root, start="2026-03-27", end="2026-03-28")

    assert frame.height == 2
    assert frame.get_column("ts_ms").to_list() == [1_774_569_600_000, 1_774_656_000_000]
    assert frame.get_column("decision_bar_interval_ms").to_list() == [300_000, 300_000]


def test_train_v5_tradability_requires_private_execution_labels_for_operating_interval(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    logs_root = tmp_path / "logs"
    private_root = tmp_path / "data" / "parquet" / "private_execution_v1"
    panel_path = tmp_path / "panel" / "expert_prediction_table.parquet"
    sequence_path = tmp_path / "sequence" / "expert_prediction_table.parquet"
    lob_path = tmp_path / "lob" / "expert_prediction_table.parquet"
    base_rows = [
        {
            "market": "KRW-BTC",
            "ts_ms": 1_774_569_600_000,
            "split": "train",
            "y_cls": 1,
            "y_reg": 0.1,
            "final_rank_score": 0.7,
            "final_expected_return": 0.12,
            "final_expected_es": 0.02,
            "final_tradability": 0.6,
            "final_uncertainty": 0.05,
            "final_alpha_lcb": 0.05,
        }
    ]
    _write_table(
        panel_path,
        base_rows,
        train_config={"model_family": "train_v5_panel_ensemble", "trainer": "v5_panel_ensemble", "data_platform_ready_snapshot_id": "snapshot-1"},
    )
    _write_table(
        sequence_path,
        [{"market": "KRW-BTC", "ts_ms": 1_774_569_600_000, "directional_probability_primary": 0.6, "sequence_uncertainty_primary": 0.04}],
        train_config={"model_family": "train_v5_sequence", "trainer": "v5_sequence", "data_platform_ready_snapshot_id": "snapshot-1"},
    )
    _write_table(
        lob_path,
        [{"market": "KRW-BTC", "ts_ms": 1_774_569_600_000, "micro_alpha_1s": 0.01, "micro_alpha_5s": 0.02, "micro_alpha_30s": 0.03, "micro_uncertainty": 0.02}],
        train_config={"model_family": "train_v5_lob", "trainer": "v5_lob", "data_platform_ready_snapshot_id": "snapshot-1"},
    )
    private_meta = private_root / "_meta"
    private_meta.mkdir(parents=True, exist_ok=True)
    (private_meta / "build_report.json").write_text(json.dumps({"status": "PASS"}, ensure_ascii=False), encoding="utf-8")
    (private_meta / "validate_report.json").write_text(json.dumps({"status": "PASS", "pass": True}, ensure_ascii=False), encoding="utf-8")
    private_part = private_root / "market=KRW-BTC" / "date=2026-03-27"
    private_part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "market": ["KRW-BTC"],
            "ts_ms": [1_774_569_600_000],
            "decision_bucket_ts_ms": [1_774_569_600_000],
            "decision_bar_interval_ms": [300_000],
            "y_tradeable": [1],
            "y_fill_within_deadline": [1],
            "y_shortfall_bps": [1.0],
            "y_adverse_tolerance": [1],
        }
    ).write_parquet(private_part / "part-000.parquet")

    with pytest.raises(ValueError, match="operating interval 60000ms"):
        train_and_register_v5_tradability(
            TrainV5TradabilityOptions(
                panel_input_path=panel_path,
                sequence_input_path=sequence_path,
                lob_input_path=lob_path,
                private_execution_root=private_root,
                registry_root=registry_root,
                logs_root=logs_root,
                model_family="train_v5_tradability",
                quote="KRW",
                start="2026-03-27",
                end="2026-03-27",
                seed=42,
            )
        )


def test_train_v5_tradability_derives_fallback_temporal_splits_when_input_split_is_missing(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    logs_root = tmp_path / "logs"
    private_root = tmp_path / "data" / "parquet" / "private_execution_v1"
    panel_path = tmp_path / "panel" / "expert_prediction_table.parquet"
    sequence_path = tmp_path / "sequence" / "expert_prediction_table.parquet"
    lob_path = tmp_path / "lob" / "expert_prediction_table.parquet"
    base_rows = []
    ts_values = [
        1_774_569_600_000,
        1_774_570_200_000,
        1_774_570_800_000,
        1_774_571_400_000,
        1_774_572_000_000,
        1_774_572_600_000,
    ]
    for idx, ts_ms in enumerate(ts_values):
        base_rows.append(
            {
                "market": "KRW-BTC",
                "ts_ms": ts_ms,
                "split": "train",
                "y_cls": 1 if idx % 2 == 0 else 0,
                "y_reg": 0.1,
                "final_rank_score": 0.70 + (idx * 0.01),
                "final_expected_return": 0.10 + (idx * 0.005),
                "final_expected_es": 0.02,
                "final_tradability": 0.60,
                "final_uncertainty": 0.05,
                "final_alpha_lcb": 0.03 + (idx * 0.01),
            }
        )
    _write_table(
        panel_path,
        base_rows,
        train_config={"model_family": "train_v5_panel_ensemble", "trainer": "v5_panel_ensemble", "data_platform_ready_snapshot_id": "snapshot-1"},
    )
    _write_table(
        sequence_path,
        [
            {"market": row["market"], "ts_ms": row["ts_ms"], "split": "train", "directional_probability_primary": 0.55 + (i * 0.01), "sequence_uncertainty_primary": 0.04}
            for i, row in enumerate(base_rows)
        ],
        train_config={"model_family": "train_v5_sequence", "trainer": "v5_sequence", "data_platform_ready_snapshot_id": "snapshot-1"},
    )
    _write_table(
        lob_path,
        [
            {"market": row["market"], "ts_ms": row["ts_ms"], "split": "train", "micro_alpha_1s": 0.01, "micro_alpha_5s": 0.02, "micro_alpha_30s": 0.03, "micro_uncertainty": 0.02}
            for row in base_rows
        ],
        train_config={"model_family": "train_v5_lob", "trainer": "v5_lob", "data_platform_ready_snapshot_id": "snapshot-1"},
    )
    private_meta = private_root / "_meta"
    private_meta.mkdir(parents=True, exist_ok=True)
    (private_meta / "build_report.json").write_text(json.dumps({"status": "PASS"}, ensure_ascii=False), encoding="utf-8")
    (private_meta / "validate_report.json").write_text(json.dumps({"status": "PASS", "pass": True}, ensure_ascii=False), encoding="utf-8")
    private_part = private_root / "market=KRW-BTC" / "date=2026-03-27"
    private_part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "market": ["KRW-BTC"] * len(ts_values),
            "ts_ms": ts_values,
            "decision_bucket_ts_ms": ts_values,
            "decision_bar_interval_ms": [60_000] * len(ts_values),
            "y_tradeable": [1, 1, 0, 1, 0, 1],
            "y_fill_within_deadline": [1, 1, 1, 0, 1, 0],
            "y_shortfall_bps": [1.0, 1.2, 2.5, 1.5, 3.0, 1.1],
            "y_adverse_tolerance": [1, 1, 0, 1, 0, 1],
        }
    ).write_parquet(private_part / "part-000.parquet")

    result = train_and_register_v5_tradability(
        TrainV5TradabilityOptions(
            panel_input_path=panel_path,
            sequence_input_path=sequence_path,
            lob_input_path=lob_path,
            private_execution_root=private_root,
            registry_root=registry_root,
            logs_root=logs_root,
            model_family="train_v5_tradability",
            quote="KRW",
            start="2026-03-27",
            end="2026-03-27",
            seed=42,
        )
    )

    assert result.run_dir.exists()
    metrics = json.loads((result.run_dir / "metrics.json").read_text(encoding="utf-8"))
    assert metrics["rows"]["train"] > 0
    assert metrics["rows"]["valid"] > 0
    assert metrics["rows"]["test"] > 0


def test_train_v5_tradability_imputes_missing_expert_values(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    logs_root = tmp_path / "logs"
    private_root = tmp_path / "data" / "parquet" / "private_execution_v1"
    panel_path = tmp_path / "panel" / "expert_prediction_table.parquet"
    sequence_path = tmp_path / "sequence" / "expert_prediction_table.parquet"
    lob_path = tmp_path / "lob" / "expert_prediction_table.parquet"
    ts_values = [
        1_774_569_600_000,
        1_774_570_200_000,
        1_774_570_800_000,
        1_774_571_400_000,
        1_774_572_000_000,
        1_774_572_600_000,
    ]
    _write_table(
        panel_path,
        [
            {
                "market": "KRW-BTC",
                "ts_ms": ts_ms,
                "split": split,
                "y_cls": 1 if idx % 2 == 0 else 0,
                "y_reg": 0.1,
                "final_rank_score": 0.7 + (idx * 0.01),
                "final_expected_return": 0.1,
                "final_expected_es": 0.02,
                "final_tradability": 0.6,
                "final_uncertainty": 0.05,
                "final_alpha_lcb": 0.04,
            }
            for idx, (ts_ms, split) in enumerate(zip(ts_values, ["train", "train", "valid", "valid", "test", "test"]))
        ],
        train_config={"model_family": "train_v5_panel_ensemble", "trainer": "v5_panel_ensemble", "data_platform_ready_snapshot_id": "snapshot-1"},
    )
    _write_table(
        sequence_path,
        [
            {
                "market": "KRW-BTC",
                "ts_ms": ts_ms,
                "split": split,
                "directional_probability_primary": None if idx in {1, 4} else 0.6,
                "sequence_uncertainty_primary": None if idx in {2, 5} else 0.04,
            }
            for idx, (ts_ms, split) in enumerate(zip(ts_values, ["train", "train", "valid", "valid", "test", "test"]))
        ],
        train_config={"model_family": "train_v5_sequence", "trainer": "v5_sequence", "data_platform_ready_snapshot_id": "snapshot-1"},
    )
    _write_table(
        lob_path,
        [
            {
                "market": "KRW-BTC",
                "ts_ms": ts_ms,
                "split": split,
                "micro_alpha_1s": None if idx in {0, 3} else 0.01,
                "micro_alpha_5s": 0.02,
                "micro_alpha_30s": 0.03,
                "micro_uncertainty": None if idx in {1, 5} else 0.02,
            }
            for idx, (ts_ms, split) in enumerate(zip(ts_values, ["train", "train", "valid", "valid", "test", "test"]))
        ],
        train_config={"model_family": "train_v5_lob", "trainer": "v5_lob", "data_platform_ready_snapshot_id": "snapshot-1"},
    )
    private_meta = private_root / "_meta"
    private_meta.mkdir(parents=True, exist_ok=True)
    (private_meta / "build_report.json").write_text(json.dumps({"status": "PASS"}, ensure_ascii=False), encoding="utf-8")
    (private_meta / "validate_report.json").write_text(json.dumps({"status": "PASS", "pass": True}, ensure_ascii=False), encoding="utf-8")
    private_part = private_root / "market=KRW-BTC" / "date=2026-03-27"
    private_part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "market": ["KRW-BTC"] * len(ts_values),
            "ts_ms": ts_values,
            "decision_bucket_ts_ms": ts_values,
            "decision_bar_interval_ms": [60_000] * len(ts_values),
            "y_tradeable": [1, 0, 1, 0, 1, 0],
            "y_fill_within_deadline": [1, 1, 0, 1, 0, 1],
            "y_shortfall_bps": [1.0, 1.4, 2.0, 1.8, 2.5, 1.1],
            "y_adverse_tolerance": [1, 0, 1, 0, 1, 0],
        }
    ).write_parquet(private_part / "part-000.parquet")

    result = train_and_register_v5_tradability(
        TrainV5TradabilityOptions(
            panel_input_path=panel_path,
            sequence_input_path=sequence_path,
            lob_input_path=lob_path,
            private_execution_root=private_root,
            registry_root=registry_root,
            logs_root=logs_root,
            model_family="train_v5_tradability",
            quote="KRW",
            start="2026-03-27",
            end="2026-03-27",
            seed=42,
        )
    )

    assert result.run_dir.exists()
    assert (result.run_dir / "expert_prediction_table.parquet").exists()
