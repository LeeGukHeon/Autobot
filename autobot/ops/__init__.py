"""Operational utilities for server automation."""

__all__ = [
    "build_data_contract_registry",
    "build_dataset_retention_registry",
    "build_feature_dataset_certification",
    "build_pointer_consistency_report",
    "build_raw_to_feature_lineage_report",
    "write_data_contract_registry",
    "write_dataset_retention_registry",
    "write_feature_dataset_certification",
    "write_pointer_consistency_report",
    "write_raw_to_feature_lineage_report",
    "build_runtime_topology_report",
    "write_runtime_topology_report",
]


def __getattr__(name: str):
    if name in __all__:
        from .data_contract_registry import build_data_contract_registry, write_data_contract_registry
        from .dataset_retention_registry import build_dataset_retention_registry, write_dataset_retention_registry
        from .feature_dataset_certification import build_feature_dataset_certification, write_feature_dataset_certification
        from .pointer_consistency_report import build_pointer_consistency_report, write_pointer_consistency_report
        from .raw_to_feature_lineage_report import build_raw_to_feature_lineage_report, write_raw_to_feature_lineage_report
        from .runtime_topology_report import build_runtime_topology_report, write_runtime_topology_report

        exports = {
            "build_data_contract_registry": build_data_contract_registry,
            "build_dataset_retention_registry": build_dataset_retention_registry,
            "build_feature_dataset_certification": build_feature_dataset_certification,
            "build_pointer_consistency_report": build_pointer_consistency_report,
            "build_raw_to_feature_lineage_report": build_raw_to_feature_lineage_report,
            "write_data_contract_registry": write_data_contract_registry,
            "write_dataset_retention_registry": write_dataset_retention_registry,
            "write_feature_dataset_certification": write_feature_dataset_certification,
            "write_pointer_consistency_report": write_pointer_consistency_report,
            "write_raw_to_feature_lineage_report": write_raw_to_feature_lineage_report,
            "build_runtime_topology_report": build_runtime_topology_report,
            "write_runtime_topology_report": write_runtime_topology_report,
        }
        return exports[name]
    raise AttributeError(name)
