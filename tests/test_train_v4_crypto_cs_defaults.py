import json
from pathlib import Path
from types import SimpleNamespace

from autobot.models.train_v4_crypto_cs import TrainV4CryptoCsOptions, _detect_duplicate_candidate_artifacts


def test_walk_forward_windows_default_is_four() -> None:
    assert TrainV4CryptoCsOptions.__dataclass_fields__["walk_forward_windows"].default == 4


def test_detect_duplicate_candidate_artifacts_matches_model_and_threshold_hashes(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    family = "train_v4_crypto_cs"
    champion_run_id = "champion-run-000"
    candidate_run_id = "candidate-run-001"
    champion_dir = registry_root / family / champion_run_id
    candidate_dir = registry_root / family / candidate_run_id
    champion_dir.mkdir(parents=True, exist_ok=True)
    candidate_dir.mkdir(parents=True, exist_ok=True)
    (champion_dir / "model.bin").write_bytes(b"same-model")
    (candidate_dir / "model.bin").write_bytes(b"same-model")
    (champion_dir / "thresholds.json").write_text(json.dumps({"top_5pct": 0.75}), encoding="utf-8")
    (candidate_dir / "thresholds.json").write_text(json.dumps({"top_5pct": 0.75}), encoding="utf-8")
    (registry_root / family / "champion.json").write_text(
        json.dumps({"run_id": champion_run_id}),
        encoding="utf-8",
    )

    result = _detect_duplicate_candidate_artifacts(
        options=SimpleNamespace(registry_root=registry_root, model_family=family),
        run_id=candidate_run_id,
        run_dir=candidate_dir,
    )

    assert result["evaluated"] is True
    assert result["duplicate"] is True
    assert result["candidate_ref"] == candidate_run_id
    assert result["champion_ref"] == champion_run_id
