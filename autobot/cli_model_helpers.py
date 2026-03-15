"""Internal CLI helpers for model-alpha shortcuts and model-ref aliases."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


DEFAULT_PAPER_ALPHA_PRESET = "live_v4"
DEFAULT_V4_RUNTIME_REF = "champion_v4"
DEFAULT_V4_CANDIDATE_REF = "latest_candidate_v4"


def paper_alpha_preset_overrides(preset: str) -> dict[str, Any]:
    name = str(preset).strip().lower() or DEFAULT_PAPER_ALPHA_PRESET
    overrides: dict[str, Any] = {
        "strategy": "model_alpha_v1",
        "feature_set": "v4",
    }
    if name in {"default", "config"}:
        return overrides
    if name in {"live_v3", "live"}:
        overrides.update(
            {
                "feature_set": "v3",
                "model_ref": "champion_v3",
                "model_family": "train_v3_mtf_micro",
                "top_pct": 0.10,
                "min_cands_per_ts": 3,
                "use_learned_selection_recommendations": True,
                "paper_feature_provider": "live_v3",
                "paper_micro_provider": "live_ws",
                "micro_gate": "off",
                "micro_order_policy": "on",
                "micro_order_policy_mode": "trade_only",
                "micro_order_policy_on_missing": "static_fallback",
            }
        )
        return overrides
    if name in {"live_v4", "v4"}:
        overrides.update(
            {
                "feature_set": "v4",
                "model_ref": DEFAULT_V4_RUNTIME_REF,
                "model_family": "train_v4_crypto_cs",
                "top_pct": 0.50,
                "min_cands_per_ts": 1,
                "use_learned_selection_recommendations": True,
                "paper_feature_provider": "live_v4",
                "paper_micro_provider": "live_ws",
                "micro_gate": "off",
                "micro_order_policy": "on",
                "micro_order_policy_mode": "trade_only",
                "micro_order_policy_on_missing": "static_fallback",
            }
        )
        return overrides
    if name in {"candidate_v4", "live_candidate_v4"}:
        overrides.update(
            {
                "feature_set": "v4",
                "model_ref": DEFAULT_V4_CANDIDATE_REF,
                "model_family": "train_v4_crypto_cs",
                "top_pct": 0.50,
                "min_cands_per_ts": 1,
                "use_learned_selection_recommendations": True,
                "paper_feature_provider": "live_v4",
                "paper_micro_provider": "live_ws",
                "micro_gate": "off",
                "micro_order_policy": "on",
                "micro_order_policy_mode": "trade_only",
                "micro_order_policy_on_missing": "static_fallback",
            }
        )
        return overrides
    if name in {"offline", "offline_v3"}:
        overrides.update(
            {
                "feature_set": "v3",
                "model_ref": "champion_v3",
                "model_family": "train_v3_mtf_micro",
                "top_pct": 0.10,
                "min_cands_per_ts": 3,
                "use_learned_selection_recommendations": True,
                "paper_feature_provider": "offline_parquet",
                "paper_micro_provider": "offline_parquet",
            }
        )
        return overrides
    if name in {"offline_v4"}:
        overrides.update(
            {
                "feature_set": "v4",
                "model_ref": DEFAULT_V4_RUNTIME_REF,
                "model_family": "train_v4_crypto_cs",
                "top_pct": 0.50,
                "min_cands_per_ts": 1,
                "use_learned_selection_recommendations": True,
                "paper_feature_provider": "offline_parquet",
                "paper_micro_provider": "offline_parquet",
            }
        )
        return overrides
    raise ValueError(f"Unsupported paper alpha preset: {preset}")


def normalize_paper_alpha_args(args: argparse.Namespace) -> argparse.Namespace:
    preset = str(getattr(args, "preset", None) or DEFAULT_PAPER_ALPHA_PRESET).strip().lower() or DEFAULT_PAPER_ALPHA_PRESET
    overrides = paper_alpha_preset_overrides(preset)
    payload = {
        "paper_command": "run",
        "duration_sec": int(getattr(args, "duration_sec", 600)),
        "quote": getattr(args, "quote", None),
        "top_n": getattr(args, "top_n", None),
        "strategy": str(overrides.get("strategy", "model_alpha_v1")),
        "tf": getattr(args, "tf", None),
        "model_ref": getattr(args, "model_ref", None) or overrides.get("model_ref"),
        "model_family": getattr(args, "model_family", None) or overrides.get("model_family"),
        "feature_set": getattr(args, "feature_set", None) or overrides.get("feature_set"),
        "top_pct": (
            getattr(args, "top_pct", None)
            if getattr(args, "top_pct", None) is not None
            else overrides.get("top_pct")
        ),
        "min_prob": (
            getattr(args, "min_prob", None)
            if getattr(args, "min_prob", None) is not None
            else overrides.get("min_prob")
        ),
        "min_cands_per_ts": (
            getattr(args, "min_cands_per_ts", None)
            if getattr(args, "min_cands_per_ts", None) is not None
            else overrides.get("min_cands_per_ts")
        ),
        "use_learned_selection_recommendations": overrides.get("use_learned_selection_recommendations"),
        "max_positions_total": getattr(args, "max_positions_total", None),
        "cooldown_bars": getattr(args, "cooldown_bars", None),
        "exit_mode": getattr(args, "exit_mode", None),
        "hold_bars": getattr(args, "hold_bars", None),
        "tp_pct": getattr(args, "tp_pct", None),
        "sl_pct": getattr(args, "sl_pct", None),
        "trailing_pct": getattr(args, "trailing_pct", None),
        "execution_price_mode": getattr(args, "execution_price_mode", None),
        "execution_timeout_bars": getattr(args, "execution_timeout_bars", None),
        "execution_replace_max": getattr(args, "execution_replace_max", None),
        "print_every_sec": getattr(args, "print_every_sec", None),
        "starting_krw": getattr(args, "starting_krw", None),
        "per_trade_krw": getattr(args, "per_trade_krw", None),
        "max_positions": getattr(args, "max_positions", None),
        "micro_gate": getattr(args, "micro_gate", None) or overrides.get("micro_gate"),
        "micro_gate_mode": getattr(args, "micro_gate_mode", None),
        "micro_gate_on_missing": getattr(args, "micro_gate_on_missing", None),
        "micro_order_policy": getattr(args, "micro_order_policy", None) or overrides.get("micro_order_policy"),
        "micro_order_policy_mode": (
            getattr(args, "micro_order_policy_mode", None) or overrides.get("micro_order_policy_mode")
        ),
        "micro_order_policy_on_missing": (
            getattr(args, "micro_order_policy_on_missing", None)
            or overrides.get("micro_order_policy_on_missing")
        ),
        "paper_micro_provider": getattr(args, "paper_micro_provider", None) or overrides.get("paper_micro_provider"),
        "paper_micro_warmup_sec": getattr(args, "paper_micro_warmup_sec", None),
        "paper_micro_warmup_min_trade_events_per_market": getattr(
            args,
            "paper_micro_warmup_min_trade_events_per_market",
            None,
        ),
        "paper_feature_provider": getattr(args, "paper_feature_provider", None) or overrides.get("paper_feature_provider"),
        "preset": preset,
    }
    for key, value in vars(args).items():
        if key not in payload:
            payload[key] = value
    return argparse.Namespace(**payload)


def load_registry_pointer_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        parsed = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def resolve_v4_runtime_model_ref_fallback(
    model_ref: str,
    model_family: str | None,
    registry_root: Path,
) -> tuple[str, str | None, str | None]:
    ref = str(model_ref).strip()
    family = str(model_family).strip() if model_family else None
    if ref != "champion" or family != "train_v4_crypto_cs":
        return ref, family, None

    champion_payload = load_registry_pointer_payload(registry_root / family / "champion.json")
    if str(champion_payload.get("run_id", "")).strip():
        return ref, family, None

    latest_candidate_payload = load_registry_pointer_payload(registry_root / family / "latest_candidate.json")
    if str(latest_candidate_payload.get("run_id", "")).strip():
        return "latest_candidate", family, "[paper][warn] champion_v4 pointer missing; falling back to latest_candidate_v4."

    latest_payload = load_registry_pointer_payload(registry_root / family / "latest.json")
    if str(latest_payload.get("run_id", "")).strip():
        return "latest", family, "[paper][warn] champion_v4 pointer missing; falling back to latest_v4."

    return ref, family, None


def backtest_alpha_preset_overrides(preset: str) -> dict[str, Any]:
    name = str(preset).strip().lower() or "default"
    overrides: dict[str, Any] = {
        "strategy": "model_alpha_v1",
        "feature_set": "v3",
    }
    if name == "default":
        return overrides
    if name == "acceptance":
        overrides.update(
            {
                "micro_order_policy": "off",
                "use_learned_selection_recommendations": False,
                "use_trade_level_action_policy": False,
            }
        )
        return overrides
    raise ValueError(f"Unsupported backtest alpha preset: {preset}")


def normalize_backtest_alpha_args(args: argparse.Namespace) -> argparse.Namespace:
    preset = str(getattr(args, "preset", None) or "default").strip().lower() or "default"
    overrides = backtest_alpha_preset_overrides(preset)
    duration_days = getattr(args, "days", None)
    if duration_days is None:
        duration_days = getattr(args, "duration_days", None)
    payload = {
        "backtest_command": "run",
        "dataset_name": getattr(args, "dataset_name", None),
        "parquet_root": getattr(args, "parquet_root", None),
        "tf": getattr(args, "tf", None),
        "market": getattr(args, "market", None),
        "markets": getattr(args, "markets", None),
        "quote": getattr(args, "quote", None),
        "top_n": getattr(args, "top_n", None),
        "universe_mode": getattr(args, "universe_mode", None),
        "strategy": str(overrides.get("strategy", "model_alpha_v1")),
        "model_ref": getattr(args, "model_ref", None),
        "model_family": getattr(args, "model_family", None),
        "feature_set": getattr(args, "feature_set", None) or overrides.get("feature_set"),
        "entry": "top_pct",
        "top_pct": getattr(args, "top_pct", None),
        "min_prob": getattr(args, "min_prob", None),
        "min_cands_per_ts": getattr(args, "min_cands_per_ts", None),
        "use_learned_selection_recommendations": overrides.get("use_learned_selection_recommendations"),
        "use_trade_level_action_policy": overrides.get("use_trade_level_action_policy"),
        "exit_mode": getattr(args, "exit_mode", None),
        "hold_bars": getattr(args, "hold_bars", None),
        "tp_pct": getattr(args, "tp_pct", None),
        "sl_pct": getattr(args, "sl_pct", None),
        "trailing_pct": getattr(args, "trailing_pct", None),
        "cooldown_bars": getattr(args, "cooldown_bars", None),
        "max_positions_total": getattr(args, "max_positions_total", None),
        "execution_price_mode": getattr(args, "execution_price_mode", None),
        "execution_timeout_bars": getattr(args, "execution_timeout_bars", None),
        "execution_replace_max": getattr(args, "execution_replace_max", None),
        "start": getattr(args, "start", None),
        "end": getattr(args, "end", None),
        "from_ts_ms": getattr(args, "from_ts_ms", None),
        "to_ts_ms": getattr(args, "to_ts_ms", None),
        "duration_days": duration_days,
        "dense_grid": bool(getattr(args, "dense_grid", False)),
        "starting_krw": getattr(args, "starting_krw", None),
        "per_trade_krw": getattr(args, "per_trade_krw", None),
        "max_positions": getattr(args, "max_positions", None),
        "min_order_krw": getattr(args, "min_order_krw", None),
        "order_timeout_bars": getattr(args, "order_timeout_bars", None),
        "reprice_max_attempts": getattr(args, "reprice_max_attempts", None),
        "micro_gate": getattr(args, "micro_gate", None),
        "micro_gate_mode": getattr(args, "micro_gate_mode", None),
        "micro_gate_on_missing": getattr(args, "micro_gate_on_missing", None),
        "micro_order_policy": getattr(args, "micro_order_policy", None) or overrides.get("micro_order_policy"),
        "micro_order_policy_mode": getattr(args, "micro_order_policy_mode", None),
        "micro_order_policy_on_missing": getattr(args, "micro_order_policy_on_missing", None),
        "preset": preset,
    }
    for key, value in vars(args).items():
        if key not in payload:
            payload[key] = value
    return argparse.Namespace(**payload)


def resolve_model_ref_alias(model_ref: str, model_family: str | None = None) -> tuple[str, str | None]:
    ref = str(model_ref).strip()
    family = str(model_family).strip() if model_family else None
    aliases: dict[str, tuple[str, str]] = {
        "latest_v1": ("latest", "train_v1"),
        "champion_v1": ("champion", "train_v1"),
        "latest_v2": ("latest", "train_v2_micro"),
        "champion_v2": ("champion", "train_v2_micro"),
        "latest_v3": ("latest", "train_v3_mtf_micro"),
        "champion_v3": ("champion", "train_v3_mtf_micro"),
        "latest_candidate_v3": ("latest_candidate", "train_v3_mtf_micro"),
        "candidate_v3": ("latest_candidate", "train_v3_mtf_micro"),
        "latest_v4": ("latest", "train_v4_crypto_cs"),
        "champion_v4": ("champion", "train_v4_crypto_cs"),
        "latest_candidate_v4": ("latest_candidate", "train_v4_crypto_cs"),
        "candidate_v4": ("latest_candidate", "train_v4_crypto_cs"),
    }
    if ref in aliases:
        resolved_ref, resolved_family = aliases[ref]
        return resolved_ref, (family or resolved_family)
    return ref, family
