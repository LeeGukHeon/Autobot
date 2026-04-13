"""Source-layer completeness bridges for mutable training/runtime inputs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from autobot.data.contracts.completeness import (
    DatasetCoverageRequest,
    DatasetCoverageResult,
    SequenceDateCompletenessRequest,
    SequenceDateCompletenessResult,
    summarize_dataset_coverage,
    summarize_sequence_date_completeness,
)
from autobot.data.micro.store import manifest_path as micro_manifest_path
from autobot.data.collect.lob30_manifest import manifest_path as lob30_manifest_path
from autobot.data.collect.sequence_tensor_store import SequenceTensorBuildOptions
from autobot.data.sources.trades.manifest import manifest_path as raw_trade_manifest_path


@dataclass(frozen=True)
class MicroCoverageRequest:
    out_root: Path
    markets: tuple[str, ...]
    required_end_ts_ms: int
    tf: str = "1m"


@dataclass(frozen=True)
class LobCoverageRequest:
    dataset_root: Path
    markets: tuple[str, ...]
    required_end_ts_ms: int


@dataclass(frozen=True)
class SequenceCoverageRequest:
    out_root: Path
    date_value: str
    markets: tuple[str, ...]
    max_anchors_per_market: int
    required_validity_signature: str | None = None


@dataclass(frozen=True)
class TradeCoverageRequest:
    out_root: Path
    markets: tuple[str, ...]
    required_end_ts_ms: int


def summarize_micro_coverage(request: MicroCoverageRequest) -> DatasetCoverageResult:
    return summarize_dataset_coverage(
        DatasetCoverageRequest(
            manifest_path=micro_manifest_path(request.out_root),
            markets=request.markets,
            required_end_ts_ms=request.required_end_ts_ms,
            tf=request.tf,
        )
    )


def summarize_lob_coverage(request: LobCoverageRequest) -> DatasetCoverageResult:
    return summarize_dataset_coverage(
        DatasetCoverageRequest(
            manifest_path=lob30_manifest_path(request.dataset_root),
            markets=request.markets,
            required_end_ts_ms=request.required_end_ts_ms,
            tf=None,
        )
    )


def summarize_sequence_coverage(request: SequenceCoverageRequest) -> SequenceDateCompletenessResult:
    return summarize_sequence_date_completeness(
        SequenceDateCompletenessRequest(
            date_completeness_path=request.out_root / "_meta" / "date_completeness.json",
            date_value=request.date_value,
            markets=request.markets,
            max_anchors_per_market=request.max_anchors_per_market,
            required_validity_signature=request.required_validity_signature,
        )
    )


def summarize_trade_coverage(request: TradeCoverageRequest) -> DatasetCoverageResult:
    return summarize_dataset_coverage(
        DatasetCoverageRequest(
            manifest_path=raw_trade_manifest_path(request.out_root),
            markets=request.markets,
            required_end_ts_ms=request.required_end_ts_ms,
            tf=None,
        )
    )


def build_sequence_coverage_request(
    *,
    options: SequenceTensorBuildOptions,
    date_value: str,
    markets: tuple[str, ...],
    required_validity_signature: str | None = None,
) -> SequenceCoverageRequest:
    return SequenceCoverageRequest(
        out_root=options.out_root,
        date_value=date_value,
        markets=markets,
        max_anchors_per_market=int(options.max_anchors_per_market),
        required_validity_signature=required_validity_signature,
    )
