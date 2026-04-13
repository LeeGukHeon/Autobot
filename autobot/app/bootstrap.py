"""Entry helpers for constructing the vnext application container."""

from __future__ import annotations

from pathlib import Path

from .container import AutobotContainer
from .settings import AppSettings


def build_default_container(*, project_root: Path | str = ".") -> AutobotContainer:
    return AutobotContainer(settings=AppSettings(project_root=Path(project_root).resolve()))

