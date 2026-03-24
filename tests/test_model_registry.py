from __future__ import annotations

import json
from pathlib import Path

import pytest

from autobot.models.registry import (
    RegistrySavePayload,
    list_runs,
    load_json,
    make_run_id,
    promote_run_to_champion,
    resolve_run_dir,
    save_run,
    update_champion_pointer,
    update_latest_candidate_pointer,
)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_promotable_run_artifacts(run_dir: Path, run_id: str) -> None:
    payloads = {
        "selection_recommendations.json": {"run_id": run_id, "version": 2},
        "selection_policy.json": {"mode": "rank_effective_quantile"},
        "selection_calibration.json": {"version": 1},
        "walk_forward_report.json": {"summary": {"windows_run": 4}},
        "execution_acceptance_report.json": {"status": "compared"},
        "runtime_recommendations.json": {"status": "complete"},
        "promotion_decision.json": {"promote": False, "reasons": ["MANUAL_PROMOTION_REQUIRED"], "status": "candidate"},
        "trainer_research_evidence.json": {"policy": "v4_trainer_research_evidence_v1"},
        "economic_objective_profile.json": {"profile_id": "v4_shared_economic_objective_v3"},
        "lane_governance.json": {"policy": "v4_lane_governance_v1"},
        "decision_surface.json": {"policy": "v4_decision_surface_v1"},
        "certification_report.json": {"policy": "candidate_acceptance_certification_v1", "status": "passed"},
        "artifact_status.json": {
            "run_id": run_id,
            "status": "candidate_adopted",
            "core_saved": True,
            "support_artifacts_written": True,
            "execution_acceptance_complete": True,
            "runtime_recommendations_complete": True,
            "governance_artifacts_complete": True,
            "acceptance_completed": True,
            "candidate_adoptable": True,
            "candidate_adopted": True,
            "promoted": False,
            "updated_at_utc": "2026-03-24T00:00:00+00:00",
        },
    }
    for filename, payload in payloads.items():
        _write_json(run_dir / filename, payload)


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


def test_registry_save_can_skip_pointer_publication(tmp_path: Path) -> None:
    registry_root = tmp_path / "models" / "registry"
    run_id = make_run_id(seed=24)
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
            leaderboard_row={"run_id": run_id, "test_precision_top5": 0.55},
            model_card_text="# card",
        ),
        publish_pointers=False,
    )

    assert not (registry_root / "train_v1" / "latest.json").exists()
    assert not (registry_root / "latest.json").exists()


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


def test_registry_resolves_latest_candidate_pointer(tmp_path: Path) -> None:
    registry_root = tmp_path / "models" / "registry"
    run_id = make_run_id(seed=11)
    run_dir = save_run(
        RegistrySavePayload(
            registry_root=registry_root,
            model_family="train_v3_mtf_micro",
            run_id=run_id,
            model_bundle={"model_type": "dummy", "estimator": {"coef": [1.0]}},
            metrics={},
            thresholds={},
            feature_spec={},
            label_spec={},
            train_config={},
            data_fingerprint={},
            leaderboard_row={"run_id": run_id, "test_precision_top5": 0.51},
            model_card_text="# card",
        )
    )

    update_latest_candidate_pointer(registry_root, "train_v3_mtf_micro", run_id)
    update_latest_candidate_pointer(registry_root, "_global", run_id, family="train_v3_mtf_micro")

    assert resolve_run_dir(
        registry_root,
        model_ref="latest_candidate",
        model_family="train_v3_mtf_micro",
    ) == run_dir
    assert resolve_run_dir(registry_root, model_ref="candidate", model_family="train_v3_mtf_micro") == run_dir
    assert resolve_run_dir(registry_root, model_ref="latest_candidate") == run_dir


def test_promote_run_to_champion_updates_pointer_and_decision(tmp_path: Path) -> None:
    registry_root = tmp_path / "models" / "registry"
    run_id = make_run_id(seed=13)
    run_dir = save_run(
        RegistrySavePayload(
            registry_root=registry_root,
            model_family="train_v3_mtf_micro",
            run_id=run_id,
            model_bundle={"model_type": "dummy", "estimator": {"coef": [1.0]}},
            metrics={},
            thresholds={},
            feature_spec={},
            label_spec={},
            train_config={},
            data_fingerprint={},
            leaderboard_row={"run_id": run_id, "test_precision_top5": 0.67},
            model_card_text="# card",
        )
    )
    _write_promotable_run_artifacts(run_dir, run_id)

    result = promote_run_to_champion(
        registry_root,
        model_ref=run_id,
        model_family="train_v3_mtf_micro",
    )

    champion_doc = load_json(registry_root / "train_v3_mtf_micro" / "champion.json")
    promotion_doc = load_json(run_dir / "promotion_decision.json")
    assert result["run_id"] == run_id
    assert champion_doc.get("run_id") == run_id
    assert champion_doc.get("promotion_mode") == "manual"
    assert promotion_doc.get("status") == "champion"
    assert promotion_doc.get("promote") is True
    assert load_json(run_dir / "artifact_status.json").get("promoted") is True


def test_promote_run_to_champion_rejects_incomplete_run(tmp_path: Path) -> None:
    registry_root = tmp_path / "models" / "registry"
    run_id = make_run_id(seed=29)
    save_run(
        RegistrySavePayload(
            registry_root=registry_root,
            model_family="train_v3_mtf_micro",
            run_id=run_id,
            model_bundle={"model_type": "dummy", "estimator": {"coef": [1.0]}},
            metrics={},
            thresholds={},
            feature_spec={},
            label_spec={},
            train_config={},
            data_fingerprint={},
            leaderboard_row={"run_id": run_id, "test_precision_top5": 0.67},
            model_card_text="# card",
        )
    )

    with pytest.raises(ValueError, match="incomplete run cannot be promoted"):
        promote_run_to_champion(
            registry_root,
            model_ref=run_id,
            model_family="train_v3_mtf_micro",
        )

    assert load_json(registry_root / "train_v3_mtf_micro" / "champion.json") == {}
