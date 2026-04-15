"""Model training and registry package."""

from .train_v1 import (
    TrainRunOptions,
    TrainRunResult,
    evaluate_registered_model,
    list_registered_models,
    load_train_defaults,
    show_registered_model,
    train_and_register,
)
from .train_v2_micro import (
    TrainV2MicroOptions,
    TrainV2MicroResult,
    check_v2_micro_preconditions,
    compare_registered_models,
    evaluate_registered_model_window,
    train_and_register_v2_micro,
)
from .train_v3_mtf_micro import (
    TrainV3MtfMicroOptions,
    TrainV3MtfMicroResult,
    train_and_register_v3_mtf_micro,
)
from .train_v4_crypto_cs import (
    TrainV4CryptoCsOptions,
    TrainV4CryptoCsResult,
    train_and_register_v4_crypto_cs,
)
from .train_v6_edge2stage import (
    TrainV6Edge2StageOptions,
    TrainV6Edge2StageResult,
    train_and_register_v6_edge2stage,
)
from .modelbt_proxy import ModelBtProxyOptions, ModelBtProxyResult, run_modelbt_proxy
from .metric_audit import MetricAuditOptions, MetricAuditResult, audit_predictions, audit_registered_model
from .ablation import AblationOptions, AblationResult, run_ablation, select_ablation_feature_columns

_OPTIONAL_EXPORTS: list[str] = []

try:
    from .train_v5_panel_ensemble import (
        TrainV5PanelEnsembleOptions,
        TrainV5PanelEnsembleResult,
        materialize_v5_panel_ensemble_runtime_export,
        resume_v5_panel_ensemble_tail,
        train_and_register_v5_panel_ensemble,
    )
    _OPTIONAL_EXPORTS.extend(
        [
            "TrainV5PanelEnsembleOptions",
            "TrainV5PanelEnsembleResult",
            "materialize_v5_panel_ensemble_runtime_export",
            "resume_v5_panel_ensemble_tail",
            "train_and_register_v5_panel_ensemble",
        ]
    )
except ModuleNotFoundError:
    pass

try:
    from .train_v5_sequence import (
        TrainV5SequenceOptions,
        TrainV5SequenceResult,
        materialize_v5_sequence_runtime_export,
        resume_v5_sequence_tail,
        train_and_register_v5_sequence,
    )
    _OPTIONAL_EXPORTS.extend(
        [
            "TrainV5SequenceOptions",
            "TrainV5SequenceResult",
            "materialize_v5_sequence_runtime_export",
            "resume_v5_sequence_tail",
            "train_and_register_v5_sequence",
        ]
    )
except ModuleNotFoundError:
    pass

try:
    from .train_v5_lob import (
        TrainV5LobOptions,
        TrainV5LobResult,
        materialize_v5_lob_runtime_export,
        resume_v5_lob_tail,
        train_and_register_v5_lob,
    )
    _OPTIONAL_EXPORTS.extend(
        [
            "TrainV5LobOptions",
            "TrainV5LobResult",
            "materialize_v5_lob_runtime_export",
            "resume_v5_lob_tail",
            "train_and_register_v5_lob",
        ]
    )
except ModuleNotFoundError:
    pass

try:
    from .train_v5_fusion import (
        TrainV5FusionOptions,
        TrainV5FusionResult,
        resume_v5_fusion_tail,
        train_and_register_v5_fusion,
    )
    _OPTIONAL_EXPORTS.extend(
        [
            "TrainV5FusionOptions",
            "TrainV5FusionResult",
            "resume_v5_fusion_tail",
            "train_and_register_v5_fusion",
        ]
    )
except ModuleNotFoundError:
    pass

try:
    from .train_v5_tradability import (
        TrainV5TradabilityOptions,
        TrainV5TradabilityResult,
        materialize_v5_tradability_runtime_export,
        train_and_register_v5_tradability,
    )
    _OPTIONAL_EXPORTS.extend(
        [
            "TrainV5TradabilityOptions",
            "TrainV5TradabilityResult",
            "materialize_v5_tradability_runtime_export",
            "train_and_register_v5_tradability",
        ]
    )
except ModuleNotFoundError:
    pass

try:
    from .v5_variant_selection import (
        run_v5_fusion_input_ablation_matrix,
        run_v5_fusion_variant_matrix,
        run_v5_lob_variant_matrix,
        run_v5_sequence_variant_matrix,
    )
    _OPTIONAL_EXPORTS.extend(
        [
            "run_v5_sequence_variant_matrix",
            "run_v5_lob_variant_matrix",
            "run_v5_fusion_input_ablation_matrix",
            "run_v5_fusion_variant_matrix",
        ]
    )
except ModuleNotFoundError:
    pass

__all__ = [
    "TrainRunOptions",
    "TrainRunResult",
    "evaluate_registered_model",
    "list_registered_models",
    "load_train_defaults",
    "show_registered_model",
    "train_and_register",
    "TrainV2MicroOptions",
    "TrainV2MicroResult",
    "check_v2_micro_preconditions",
    "compare_registered_models",
    "evaluate_registered_model_window",
    "train_and_register_v2_micro",
    "TrainV3MtfMicroOptions",
    "TrainV3MtfMicroResult",
    "train_and_register_v3_mtf_micro",
    "TrainV4CryptoCsOptions",
    "TrainV4CryptoCsResult",
    "train_and_register_v4_crypto_cs",
    "TrainV6Edge2StageOptions",
    "TrainV6Edge2StageResult",
    "train_and_register_v6_edge2stage",
    "ModelBtProxyOptions",
    "ModelBtProxyResult",
    "run_modelbt_proxy",
    "MetricAuditOptions",
    "MetricAuditResult",
    "audit_predictions",
    "audit_registered_model",
    "AblationOptions",
    "AblationResult",
    "run_ablation",
    "select_ablation_feature_columns",
] + _OPTIONAL_EXPORTS
