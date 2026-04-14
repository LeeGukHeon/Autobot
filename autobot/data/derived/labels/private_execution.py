"""Bridge into the current `private_execution_v1` label implementation."""

from __future__ import annotations

from autobot.ops.private_execution_label_store import (
    build_private_execution_label_store as build_private_execution_labels,
)

__all__ = ["build_private_execution_labels"]

