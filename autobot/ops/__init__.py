"""Operational utilities for server automation."""

__all__ = [
    "build_data_contract_registry",
    "write_data_contract_registry",
    "build_runtime_topology_report",
    "write_runtime_topology_report",
]


def __getattr__(name: str):
    if name in __all__:
        from .data_contract_registry import build_data_contract_registry, write_data_contract_registry
        from .runtime_topology_report import build_runtime_topology_report, write_runtime_topology_report

        exports = {
            "build_data_contract_registry": build_data_contract_registry,
            "write_data_contract_registry": write_data_contract_registry,
            "build_runtime_topology_report": build_runtime_topology_report,
            "write_runtime_topology_report": write_runtime_topology_report,
        }
        return exports[name]
    raise AttributeError(name)
