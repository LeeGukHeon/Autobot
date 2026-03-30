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
from .train_v5_panel_ensemble import (
    TrainV5PanelEnsembleOptions,
    TrainV5PanelEnsembleResult,
    resume_v5_panel_ensemble_tail,
    train_and_register_v5_panel_ensemble,
)
from .train_v5_sequence import (
    TrainV5SequenceOptions,
    TrainV5SequenceResult,
    resume_v5_sequence_tail,
    train_and_register_v5_sequence,
)
from .train_v5_lob import (
    TrainV5LobOptions,
    TrainV5LobResult,
    resume_v5_lob_tail,
    train_and_register_v5_lob,
)
from .train_v5_fusion import (
    TrainV5FusionOptions,
    TrainV5FusionResult,
    resume_v5_fusion_tail,
    train_and_register_v5_fusion,
)
from .modelbt_proxy import ModelBtProxyOptions, ModelBtProxyResult, run_modelbt_proxy
from .metric_audit import MetricAuditOptions, MetricAuditResult, audit_predictions, audit_registered_model
from .ablation import AblationOptions, AblationResult, run_ablation, select_ablation_feature_columns

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
    "TrainV5PanelEnsembleOptions",
    "TrainV5PanelEnsembleResult",
    "resume_v5_panel_ensemble_tail",
    "train_and_register_v5_panel_ensemble",
    "TrainV5SequenceOptions",
    "TrainV5SequenceResult",
    "resume_v5_sequence_tail",
    "train_and_register_v5_sequence",
    "TrainV5LobOptions",
    "TrainV5LobResult",
    "resume_v5_lob_tail",
    "train_and_register_v5_lob",
    "TrainV5FusionOptions",
    "TrainV5FusionResult",
    "resume_v5_fusion_tail",
    "train_and_register_v5_fusion",
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
]
