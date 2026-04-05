from __future__ import annotations

from pathlib import Path

from autobot.paper.live_features_v5 import LiveFeatureProviderV5


def test_live_feature_provider_v5_last_build_stats_returns_copy() -> None:
    provider = LiveFeatureProviderV5.__new__(LiveFeatureProviderV5)
    provider._last_build_stats = {  # type: ignore[attr-defined]
        "provider": "LIVE_V5",
        "built_rows": 3,
        "runtime_source_lineage": {"run_id": "fusion-run-001"},
    }

    stats = provider.last_build_stats()
    assert stats == {
        "provider": "LIVE_V5",
        "built_rows": 3,
        "runtime_source_lineage": {"run_id": "fusion-run-001"},
    }

    stats["built_rows"] = 0
    stats["runtime_source_lineage"]["run_id"] = "mutated"
    assert provider._last_build_stats["built_rows"] == 3  # type: ignore[attr-defined]
    assert provider._last_build_stats["runtime_source_lineage"]["run_id"] == "fusion-run-001"  # type: ignore[attr-defined]


def test_live_feature_provider_v5_loads_child_predictor_from_input_expert_metadata(monkeypatch) -> None:
    provider = LiveFeatureProviderV5.__new__(LiveFeatureProviderV5)
    provider._registry_root = Path("models/registry")  # type: ignore[attr-defined]

    captured: dict[str, object] = {}

    def _fake_load_predictor_from_registry(*, registry_root: Path, model_ref: str, model_family: str):
        captured["registry_root"] = registry_root
        captured["model_ref"] = model_ref
        captured["model_family"] = model_family
        return {"ok": True}

    monkeypatch.setattr("autobot.paper.live_features_v5.load_predictor_from_registry", _fake_load_predictor_from_registry)

    result = provider._load_predictor_from_input_path(  # type: ignore[attr-defined]
        {
            "model_family": "train_v5_panel_ensemble",
            "run_id": "panel-run-001",
            "path": "/tmp/models/registry/train_v5_panel_ensemble/panel-run-001/expert_prediction_table.parquet",
        }
    )

    assert result == {"ok": True}
    assert captured == {
        "registry_root": Path("models/registry"),
        "model_ref": "panel-run-001",
        "model_family": "train_v5_panel_ensemble",
    }
