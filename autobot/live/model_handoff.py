"""Live model handoff and shared data-plane synchronization helpers."""

from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import Any

from autobot.data.collect import load_ws_public_status
from autobot.models.registry import load_json, resolve_run_dir


LIVE_RUNTIME_CONTRACT_CHECKPOINT = "live_runtime_contract"
WS_PUBLIC_CONTRACT_CHECKPOINT = "ws_public_contract"
LIVE_RUNTIME_HEALTH_CHECKPOINT = "live_runtime_health"


def resolve_live_model_ref_source(model_ref: str, model_family: str | None = None) -> tuple[str, str | None]:
    ref = str(model_ref).strip()
    family = str(model_family).strip() if model_family else None
    aliases: dict[str, tuple[str, str]] = {
        "champion_v4": ("champion", "train_v4_crypto_cs"),
        "latest_v4": ("latest", "train_v4_crypto_cs"),
        "latest_candidate_v4": ("latest_candidate", "train_v4_crypto_cs"),
        "candidate_v4": ("latest_candidate", "train_v4_crypto_cs"),
    }
    if ref in aliases:
        resolved_ref, resolved_family = aliases[ref]
        return resolved_ref, (family or resolved_family)
    return ref, family


def resolve_live_runtime_model_contract(
    *,
    registry_root: Path,
    model_ref: str,
    model_family: str | None,
    ts_ms: int,
) -> dict[str, Any]:
    resolved_ref, resolved_family = resolve_live_model_ref_source(model_ref=model_ref, model_family=model_family)
    if not resolved_family:
        raise ValueError("live runtime model family must not be blank")
    run_dir = resolve_run_dir(registry_root, model_ref=resolved_ref, model_family=resolved_family)
    champion_pointer = load_json(registry_root / resolved_family / "champion.json")
    champion_pointer_run_id = str(champion_pointer.get("run_id", "")).strip()
    pointer_name = resolved_ref if resolved_ref in {"champion", "latest", "latest_candidate"} else ""
    resolved_pointer = (
        load_json(registry_root / resolved_family / f"{pointer_name}.json")
        if pointer_name
        else {}
    )
    resolved_pointer_run_id = str(resolved_pointer.get("run_id", "")).strip()
    return {
        "model_ref_source_requested": str(model_ref).strip(),
        "model_ref_source_resolved": resolved_ref,
        "model_family_requested": str(model_family or "").strip(),
        "model_family_resolved": resolved_family,
        "resolved_pointer_name": pointer_name or None,
        "resolved_pointer_run_id": resolved_pointer_run_id or None,
        "champion_pointer_run_id": champion_pointer_run_id or None,
        "live_runtime_model_run_id": run_dir.name,
        "live_runtime_model_run_dir": str(run_dir),
        "resolved_at_ts_ms": int(ts_ms),
    }


def load_ws_public_runtime_contract(
    *,
    meta_dir: Path,
    raw_root: Path,
    stale_threshold_sec: int,
    micro_aggregate_report_path: Path | None,
    ts_ms: int,
) -> dict[str, Any]:
    status = load_ws_public_status(meta_dir=meta_dir, raw_root=raw_root)
    health_snapshot = status.get("health_snapshot") if isinstance(status.get("health_snapshot"), dict) else {}
    collect_report = status.get("collect_report") if isinstance(status.get("collect_report"), dict) else {}
    latest_run = status.get("runs_summary_latest") if isinstance(status.get("runs_summary_latest"), dict) else {}
    validate_report = _load_dict(meta_dir / "ws_validate_report.json")
    micro_aggregate_report = _load_dict(micro_aggregate_report_path) if micro_aggregate_report_path is not None else {}

    last_checkpoint_ts_ms, last_checkpoint_source = _resolve_ws_public_checkpoint_ts_ms(
        health_snapshot=health_snapshot,
        collect_report=collect_report,
        validate_report=validate_report,
        ts_ms=ts_ms,
    )
    staleness_sec = (
        max(float(int(ts_ms) - int(last_checkpoint_ts_ms)) / 1000.0, 0.0)
        if last_checkpoint_ts_ms is not None
        else None
    )
    stale_threshold_value = max(int(stale_threshold_sec), 1)
    return {
        "meta_dir": str(meta_dir),
        "raw_root": str(raw_root),
        "stale_threshold_sec": stale_threshold_value,
        "ws_public_last_checkpoint_ts_ms": last_checkpoint_ts_ms,
        "ws_public_last_checkpoint_source": last_checkpoint_source,
        "ws_public_staleness_sec": staleness_sec,
        "ws_public_stale": (
            staleness_sec is None or float(staleness_sec) > float(stale_threshold_value)
        ),
        "health_snapshot": _health_excerpt(health_snapshot),
        "collect_report": _report_excerpt(collect_report),
        "validate_report": _report_excerpt(validate_report),
        "runs_summary_latest": _report_excerpt(latest_run),
        "micro_aggregate": _micro_aggregate_excerpt(micro_aggregate_report),
        "checked_at_ts_ms": int(ts_ms),
    }


def build_live_runtime_sync_status(
    *,
    pinned_contract: dict[str, Any] | None,
    current_contract: dict[str, Any] | None,
    ws_public_contract: dict[str, Any] | None,
) -> dict[str, Any]:
    pinned = dict(pinned_contract or {})
    current = dict(current_contract or {})
    ws_public = dict(ws_public_contract or {})
    pinned_run_id = str(pinned.get("live_runtime_model_run_id", "")).strip()
    champion_pointer_run_id = str(current.get("champion_pointer_run_id", "")).strip()
    pointer_monitoring_enabled = bool(str(current.get("resolved_pointer_name") or "").strip())
    model_pointer_divergence = bool(
        pointer_monitoring_enabled
        and pinned_run_id
        and champion_pointer_run_id
        and pinned_run_id != champion_pointer_run_id
    )
    return {
        "live_runtime_model_run_id": pinned_run_id or None,
        "champion_pointer_run_id": champion_pointer_run_id or None,
        "current_resolved_model_run_id": str(current.get("live_runtime_model_run_id", "")).strip() or None,
        "pointer_monitoring_enabled": pointer_monitoring_enabled,
        "model_pointer_divergence": model_pointer_divergence,
        "model_pointer_divergence_reason": (
            "MODEL_POINTER_DIVERGENCE" if model_pointer_divergence else None
        ),
        "previous_pinned_run_id": str(pinned.get("previous_pinned_run_id", "")).strip() or None,
        "promote_happened_while_down": bool(pinned.get("promote_happened_while_down", False)),
        "ws_public_last_checkpoint_ts_ms": ws_public.get("ws_public_last_checkpoint_ts_ms"),
        "ws_public_staleness_sec": ws_public.get("ws_public_staleness_sec"),
        "ws_public_stale": bool(ws_public.get("ws_public_stale", False)),
        "ws_public_last_checkpoint_source": ws_public.get("ws_public_last_checkpoint_source"),
        "ws_public_run_id": ((ws_public.get("health_snapshot") or {}).get("run_id") if isinstance(ws_public.get("health_snapshot"), dict) else None),
        "ws_public_validate_run_id": ((ws_public.get("validate_report") or {}).get("run_id") if isinstance(ws_public.get("validate_report"), dict) else None),
        "micro_aggregate_run_id": ((ws_public.get("micro_aggregate") or {}).get("run_id") if isinstance(ws_public.get("micro_aggregate"), dict) else None),
        "pinned_contract": pinned,
        "current_contract": current,
        "ws_public_contract": ws_public,
    }


def _resolve_ws_public_checkpoint_ts_ms(
    *,
    health_snapshot: dict[str, Any],
    collect_report: dict[str, Any],
    validate_report: dict[str, Any],
    ts_ms: int,
) -> tuple[int | None, str | None]:
    _ = ts_ms
    candidates: list[tuple[int, str]] = []
    updated_at_ms = _coerce_int(health_snapshot.get("updated_at_ms"))
    if updated_at_ms is not None:
        candidates.append((updated_at_ms, "health_snapshot.updated_at_ms"))
    last_rx = health_snapshot.get("last_rx_ts_ms")
    if isinstance(last_rx, dict):
        for channel in ("trade", "orderbook"):
            value = _coerce_int(last_rx.get(channel))
            if value is not None:
                candidates.append((value, f"health_snapshot.last_rx_ts_ms.{channel}"))
    collect_generated = _parse_iso_to_ts_ms(collect_report.get("generated_at"))
    if collect_generated is not None:
        candidates.append((collect_generated, "collect_report.generated_at"))
    validate_generated = _parse_iso_to_ts_ms(validate_report.get("generated_at"))
    if validate_generated is not None:
        candidates.append((validate_generated, "validate_report.generated_at"))
    if not candidates:
        return None, None
    # Producer checkpoints can legitimately be a few milliseconds ahead of the
    # caller due to local file update/read timing. Prefer the freshest producer
    # timestamp and let staleness clamp negative deltas to zero.
    latest_ts_ms, source = max(candidates, key=lambda item: item[0])
    return latest_ts_ms, source


def _parse_iso_to_ts_ms(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return int(parsed.timestamp() * 1000)


def _coerce_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _load_dict(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    if not path.exists():
        return {}
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _health_excerpt(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        return {}
    keys = (
        "run_id",
        "updated_at_ms",
        "connected",
        "reconnect_count",
        "last_rx_ts_ms",
        "subscribed_markets_count",
        "fatal_reason",
    )
    return {key: payload.get(key) for key in keys if key in payload}


def _report_excerpt(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        return {}
    keys = (
        "run_id",
        "generated_at",
        "date_filter",
        "checked_files",
        "ok_files",
        "warn_files",
        "fail_files",
        "parse_ok_ratio",
        "parts",
        "rows_total",
        "bytes_total",
        "min_date",
        "max_date",
    )
    return {key: payload.get(key) for key in keys if key in payload}


def _micro_aggregate_excerpt(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        return {}
    keys = (
        "run_id",
        "start",
        "end",
        "dates",
        "rows_written_total",
        "micro_available_ratio",
        "raw_ws_root",
        "out_root",
    )
    return {key: payload.get(key) for key in keys if key in payload}
