"""Dependency container for wiring vnext services without shell-first orchestration."""

from __future__ import annotations

from dataclasses import dataclass

from .settings import AppSettings


@dataclass(frozen=True)
class AutobotContainer:
    settings: AppSettings

