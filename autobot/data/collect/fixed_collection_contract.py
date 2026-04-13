"""Shared fixed collection market contract for always-on source-plane ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class FixedCollectionContract:
    enabled: bool
    quote: str
    markets: tuple[str, ...]
    policy: str = "fixed_collection_contract_v1"


def load_fixed_collection_contract(*, config_dir: Path) -> FixedCollectionContract:
    payload = _load_yaml_doc(Path(config_dir) / "base.yaml")
    source_plane = payload.get("source_plane") if isinstance(payload.get("source_plane"), dict) else {}
    fixed = source_plane.get("fixed_collection") if isinstance(source_plane.get("fixed_collection"), dict) else {}
    quote = str(fixed.get("quote", payload.get("universe", {}).get("quote_currency", "KRW"))).strip().upper() or "KRW"
    markets = tuple(
        dict.fromkeys(
            str(item).strip().upper()
            for item in (fixed.get("markets") or [])
            if str(item).strip()
        )
    )
    enabled = bool(fixed.get("enabled", False)) and len(markets) > 0
    return FixedCollectionContract(
        enabled=enabled,
        quote=quote,
        markets=markets,
    )


def resolve_fixed_collection_markets(
    *,
    config_dir: Path,
    quote: str,
    explicit_markets: tuple[str, ...] | None = None,
) -> tuple[str, ...]:
    if explicit_markets:
        return tuple(str(item).strip().upper() for item in explicit_markets if str(item).strip())
    contract = load_fixed_collection_contract(config_dir=config_dir)
    quote_value = str(quote).strip().upper() or "KRW"
    if not contract.enabled or contract.quote != quote_value:
        return ()
    return tuple(
        market
        for market in contract.markets
        if market.startswith(f"{quote_value}-")
    )


def _load_yaml_doc(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}
