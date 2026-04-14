"""Bridge into the current `sequence_v1` tensor store implementation."""

from __future__ import annotations

from autobot.data.collect.sequence_tensor_store import (
    SequenceTensorBuildOptions,
    SequenceTensorBuildSummary,
    SequenceTensorValidateSummary,
    build_sequence_tensor_store,
    validate_sequence_tensor_store,
)


def build_sequence_tensors(options: SequenceTensorBuildOptions) -> SequenceTensorBuildSummary:
    return build_sequence_tensor_store(options)


def validate_sequence_tensors(*, options: SequenceTensorBuildOptions) -> SequenceTensorValidateSummary:
    return validate_sequence_tensor_store(options=options)
