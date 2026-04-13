"""Shared application settings models for the vnext architecture."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class AppSettings:
    project_root: Path = Path(".")
    quote: str = "KRW"
    research_top_n: int = 100
    live_scan_top_n: int = 30
    tradeable_top_n: int = 10
    runtime_model_family: str = "train_v5_fusion"
    data_snapshot_required: bool = True
    tags: tuple[str, ...] = field(default_factory=tuple)

