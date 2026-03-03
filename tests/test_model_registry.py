from __future__ import annotations

from pathlib import Path

from autobot.models.registry import (
    RegistrySavePayload,
    list_runs,
    load_json,
    make_run_id,
    resolve_run_dir,
    save_run,
    update_champion_pointer,
)


def test_registry_save_and_resolve_latest(tmp_path: Path) -> None:
    registry_root = tmp_path / "models" / "registry"
    run_id = make_run_id(seed=42)
    run_dir = save_run(
        RegistrySavePayload(
            registry_root=registry_root,
            model_family="train_v1",
            run_id=run_id,
            model_bundle={"model_type": "dummy", "estimator": {"coef": [1.0]}},
            metrics={"rows": {"train": 10, "valid": 5, "test": 5}},
            thresholds={"top_5pct": 0.61},
            feature_spec={"feature_columns": ["f1"]},
            label_spec={"label_columns": ["y_reg", "y_cls"]},
            train_config={"tf": "5m"},
            data_fingerprint={"manifest_sha256": "abc"},
            leaderboard_row={"run_id": run_id, "test_precision_top5": 0.55},
            model_card_text="# card",
        )
    )

    assert (run_dir / "model.bin").exists()
    assert (run_dir / "metrics.json").exists()
    assert (run_dir / "train_config.yaml").exists()

    latest = resolve_run_dir(registry_root, model_ref="latest", model_family="train_v1")
    assert latest == run_dir


def test_registry_list_and_champion_pointer(tmp_path: Path) -> None:
    registry_root = tmp_path / "models" / "registry"
    run_id = make_run_id(seed=7)
    save_run(
        RegistrySavePayload(
            registry_root=registry_root,
            model_family="train_v1",
            run_id=run_id,
            model_bundle={"model_type": "dummy", "estimator": {"coef": [1.0]}},
            metrics={},
            thresholds={},
            feature_spec={},
            label_spec={},
            train_config={},
            data_fingerprint={},
            leaderboard_row={"run_id": run_id, "test_precision_top5": 0.61},
            model_card_text="# card",
        )
    )
    champion_path, replaced = update_champion_pointer(
        registry_root,
        "train_v1",
        run_id=run_id,
        score=0.61,
        score_key="test_precision_top5",
    )

    assert replaced is True
    champion_doc = load_json(champion_path)
    assert champion_doc.get("run_id") == run_id

    rows = list_runs(registry_root, model_family="train_v1")
    assert len(rows) == 1
    assert rows[0]["run_id"] == run_id
