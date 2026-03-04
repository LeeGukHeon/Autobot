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
    "MetricAuditOptions",
    "MetricAuditResult",
    "audit_predictions",
    "audit_registered_model",
    "AblationOptions",
    "AblationResult",
    "run_ablation",
    "select_ablation_feature_columns",
]
