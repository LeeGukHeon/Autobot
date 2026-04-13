from __future__ import annotations

from pathlib import Path

import numpy as np

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


def test_live_feature_provider_v5_defaults_to_one_minute_tf(monkeypatch) -> None:
    monkeypatch.setattr(
        LiveFeatureProviderV5,
        "_resolve_runtime_source_lineage",
        lambda self: {},
    )
    monkeypatch.setattr(
        LiveFeatureProviderV5,
        "_configure_child_predictors",
        lambda self: None,
    )

    class _BaseProvider:
        def __init__(self, **kwargs) -> None:
            self.kwargs = dict(kwargs)

    monkeypatch.setattr("autobot.paper.live_features_v5.LiveFeatureProviderV4Native", _BaseProvider)

    provider = LiveFeatureProviderV5(
        predictor=type("Predictor", (), {"model_family": "train_v5_fusion", "feature_columns": ()})(),
        registry_root=Path("models/registry"),
        feature_columns=(),
    )

    assert provider._tf == "1m"  # type: ignore[attr-defined]
    assert provider._base_provider.kwargs["tf"] == "1m"  # type: ignore[attr-defined]


def test_live_feature_provider_v5_builds_fusion_support_and_tradability_features() -> None:
    class _PanelPredictor:
        feature_columns = ("panel_base_a", "panel_base_b")

        @staticmethod
        def predict_score_contract(_matrix):
            return {
                "final_rank_score": np.asarray([0.41]),
                "final_uncertainty": np.asarray([0.05]),
                "score_mean": np.asarray([0.31]),
                "score_std": np.asarray([0.11]),
                "score_lcb": np.asarray([0.21]),
                "final_expected_return": np.asarray([0.12]),
                "final_expected_es": np.asarray([0.02]),
                "final_tradability": np.asarray([0.8]),
                "final_alpha_lcb": np.asarray([0.04]),
            }

    class _SequenceEstimator:
        horizons_minutes = (3, 6, 12, 24)
        quantile_levels = (0.1, 0.5, 0.9)

        @staticmethod
        def predict_cache_batch(_payload):
            return {
                "directional_probability_primary": np.asarray([0.61]),
                "sequence_uncertainty_primary": np.asarray([0.07]),
                "return_quantiles_by_horizon": np.asarray(
                    [[
                        [0.01, 0.02, 0.03],
                        [0.04, 0.05, 0.06],
                        [0.07, 0.08, 0.09],
                        [0.10, 0.11, 0.12],
                    ]]
                ),
                "regime_embedding": np.asarray([[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]]),
            }

    class _LobEstimator:
        @staticmethod
        def predict_lob_contract(_payload):
            return {
                "micro_alpha_1s": np.asarray([0.11]),
                "micro_alpha_5s": np.asarray([0.12]),
                "micro_alpha_30s": np.asarray([0.13]),
                "micro_alpha_60s": np.asarray([0.14]),
                "micro_uncertainty": np.asarray([0.03]),
                "adverse_excursion_30s": np.asarray([0.02]),
            }

    class _TradabilityEstimator:
        @staticmethod
        def predict_tradability_contract(_matrix):
            return {
                "tradability_prob": np.asarray([0.72]),
                "fill_within_deadline_prob": np.asarray([0.76]),
                "expected_shortfall_bps": np.asarray([1.5]),
                "adverse_tolerance_prob": np.asarray([0.81]),
                "tradability_uncertainty": np.asarray([0.04]),
            }

    class _Predictor:
        def __init__(self, feature_columns, estimator) -> None:
            self.feature_columns = feature_columns
            self.model_bundle = {"estimator": estimator}

    provider = LiveFeatureProviderV5.__new__(LiveFeatureProviderV5)
    provider._feature_columns = (  # type: ignore[attr-defined]
        "panel_final_rank_score",
        "panel_final_uncertainty",
        "panel_score_mean",
        "panel_score_std",
        "panel_score_lcb",
        "panel_final_expected_return",
        "panel_final_expected_es",
        "panel_final_tradability",
        "panel_final_alpha_lcb",
        "sequence_support_is_strict",
        "sequence_support_is_reduced",
        "sequence_support_score",
        "sequence_directional_probability_primary",
        "sequence_sequence_uncertainty_primary",
        "sequence_return_quantile_h3_q10",
        "sequence_return_quantile_h3_q50",
        "sequence_return_quantile_h3_q90",
        "sequence_return_quantile_h6_q10",
        "sequence_return_quantile_h6_q50",
        "sequence_return_quantile_h6_q90",
        "sequence_return_quantile_h12_q10",
        "sequence_return_quantile_h12_q50",
        "sequence_return_quantile_h12_q90",
        "sequence_return_quantile_h24_q10",
        "sequence_return_quantile_h24_q50",
        "sequence_return_quantile_h24_q90",
        "sequence_regime_embedding_0",
        "sequence_regime_embedding_1",
        "sequence_regime_embedding_2",
        "sequence_regime_embedding_3",
        "sequence_regime_embedding_4",
        "sequence_regime_embedding_5",
        "sequence_regime_embedding_6",
        "sequence_regime_embedding_7",
        "lob_support_is_strict",
        "lob_support_is_reduced",
        "lob_support_score",
        "lob_micro_alpha_1s",
        "lob_micro_alpha_5s",
        "lob_micro_alpha_30s",
        "lob_micro_alpha_60s",
        "lob_micro_uncertainty",
        "lob_adverse_excursion_30s",
        "tradability_tradability_prob",
        "tradability_fill_within_deadline_prob",
        "tradability_expected_shortfall_bps",
        "tradability_adverse_tolerance_prob",
        "tradability_tradability_uncertainty",
        "panel_present",
        "sequence_present",
        "lob_present",
        "tradability_present",
    )
    provider._panel_predictor = _PanelPredictor()  # type: ignore[attr-defined]
    provider._sequence_predictor = _Predictor(tuple(), _SequenceEstimator())  # type: ignore[attr-defined]
    provider._lob_predictor = _Predictor(tuple(), _LobEstimator())  # type: ignore[attr-defined]
    provider._tradability_predictor = _Predictor(  # type: ignore[attr-defined]
        (
            "panel_final_rank_score",
            "panel_final_uncertainty",
            "panel_score_mean",
            "panel_score_std",
            "panel_score_lcb",
            "panel_final_expected_return",
            "panel_final_expected_es",
            "panel_final_tradability",
            "panel_final_alpha_lcb",
            "sequence_support_is_strict",
            "sequence_support_is_reduced",
            "sequence_support_score",
            "sequence_directional_probability_primary",
            "sequence_sequence_uncertainty_primary",
            "sequence_return_quantile_h3_q10",
            "sequence_return_quantile_h3_q50",
            "sequence_return_quantile_h3_q90",
            "sequence_return_quantile_h6_q10",
            "sequence_return_quantile_h6_q50",
            "sequence_return_quantile_h6_q90",
            "sequence_return_quantile_h12_q10",
            "sequence_return_quantile_h12_q50",
            "sequence_return_quantile_h12_q90",
            "sequence_return_quantile_h24_q10",
            "sequence_return_quantile_h24_q50",
            "sequence_return_quantile_h24_q90",
            "sequence_regime_embedding_0",
            "sequence_regime_embedding_1",
            "sequence_regime_embedding_2",
            "sequence_regime_embedding_3",
            "sequence_regime_embedding_4",
            "sequence_regime_embedding_5",
            "sequence_regime_embedding_6",
            "sequence_regime_embedding_7",
            "lob_support_is_strict",
            "lob_support_is_reduced",
            "lob_support_score",
            "lob_micro_alpha_1s",
            "lob_micro_alpha_5s",
            "lob_micro_alpha_30s",
            "lob_micro_alpha_60s",
            "lob_micro_uncertainty",
            "lob_adverse_excursion_30s",
        ),
        _TradabilityEstimator(),
    )
    provider._build_online_sequence_payload = lambda **_: {  # type: ignore[attr-defined]
        "second_tensor": np.zeros((1, 120, 4), dtype=np.float32),
        "minute_tensor": np.zeros((1, 30, 4), dtype=np.float32),
        "micro_tensor": np.zeros((1, 30, 8), dtype=np.float32),
        "lob_tensor": np.zeros((1, 32, 120), dtype=np.float32),
        "lob_global_tensor": np.zeros((1, 32, 4), dtype=np.float32),
        "known_covariates": np.zeros((1, 30, 8), dtype=np.float32),
        "pooled_features": np.zeros((1, 1), dtype=np.float32),
        "support_payload": {
            "support_is_strict": 1.0,
            "support_is_reduced": 0.0,
            "support_score": 2.0,
        },
    }

    values = provider._build_fusion_feature_values(  # type: ignore[attr-defined]
        market="KRW-BTC",
        ts_ms=1_775_978_700_000,
        base_row={"panel_base_a": 0.1, "panel_base_b": 0.2},
    )

    assert values["sequence_support_is_strict"] == 1.0
    assert values["sequence_support_score"] == 2.0
    assert values["lob_support_is_strict"] == 1.0
    assert values["lob_support_score"] == 2.0
    assert values["tradability_tradability_prob"] == 0.72
    assert values["tradability_fill_within_deadline_prob"] == 0.76
    assert values["tradability_expected_shortfall_bps"] == 1.5
    assert values["tradability_adverse_tolerance_prob"] == 0.81
    assert values["tradability_tradability_uncertainty"] == 0.04
    assert values["tradability_present"] == 1.0
