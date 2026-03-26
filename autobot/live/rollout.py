"""Live rollout contract helpers for shadow/canary/live runtime gating."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import time
from typing import Any

from .breaker_taxonomy import annotate_reason_payload

LIVE_ROLLOUT_CONTRACT_CHECKPOINT = "live_rollout_contract"
LIVE_TEST_ORDER_CHECKPOINT = "live_rollout_test_order"
LIVE_ROLLOUT_STATUS_CHECKPOINT = "live_rollout_status"

DEFAULT_LIVE_TARGET_UNIT = "autobot-live-alpha.service"
DEFAULT_ROLLOUT_MODE = "shadow"
VALID_ROLLOUT_MODES = ("shadow", "canary", "live")

ROLLOUT_REASON_NOT_ARMED = "LIVE_ROLLOUT_NOT_ARMED"
ROLLOUT_REASON_UNIT_MISMATCH = "LIVE_ROLLOUT_UNIT_MISMATCH"
ROLLOUT_REASON_MODE_MISMATCH = "LIVE_ROLLOUT_MODE_MISMATCH"
ROLLOUT_REASON_TEST_ORDER_REQUIRED = "LIVE_TEST_ORDER_REQUIRED"
ROLLOUT_REASON_TEST_ORDER_STALE = "LIVE_TEST_ORDER_STALE"
ROLLOUT_REASON_BREAKER_ACTIVE = "LIVE_BREAKER_ACTIVE"
ROLLOUT_REASON_CANARY_REQUIRES_SINGLE_SLOT = "LIVE_CANARY_REQUIRES_SINGLE_SLOT"


@dataclass(frozen=True)
class LiveRolloutGate:
    mode: str
    target_unit: str
    armed: bool
    order_emission_allowed: bool
    start_allowed: bool
    test_order_required: bool
    test_order_ok: bool
    test_order_age_sec: float | None
    breaker_clear: bool
    small_account_single_slot_ready: bool
    reason_codes: tuple[str, ...]
    contract: dict[str, Any]
    test_order: dict[str, Any]


def normalize_rollout_mode(value: str | None) -> str:
    mode = str(value or DEFAULT_ROLLOUT_MODE).strip().lower() or DEFAULT_ROLLOUT_MODE
    if mode not in VALID_ROLLOUT_MODES:
        raise ValueError(f"unsupported live rollout mode: {value}")
    return mode


def resolve_rollout_gate_inputs(
    *,
    default_mode: str,
    default_target_unit: str,
    contract: dict[str, Any] | None,
) -> tuple[str, str]:
    contract_value = dict(contract or {})
    mode = normalize_rollout_mode(contract_value.get("mode")) if contract_value else normalize_rollout_mode(default_mode)
    target_unit = (
        str(contract_value.get("target_unit") or "").strip()
        if contract_value
        else str(default_target_unit).strip()
    )
    if not target_unit:
        target_unit = str(default_target_unit).strip() or DEFAULT_LIVE_TARGET_UNIT
    return mode, target_unit


def rollout_artifact_root(project_root: Path) -> Path:
    return Path(project_root) / "logs" / "live_rollout"


def _rollout_target_slug(target_unit: str | None) -> str:
    text = str(target_unit or "").strip().lower()
    if not text:
        return ""
    slug = "".join(ch if ch.isalnum() else "_" for ch in text).strip("_")
    return slug


def rollout_latest_artifact_path(project_root: Path, *, target_unit: str | None = None) -> Path:
    root = rollout_artifact_root(project_root)
    slug = _rollout_target_slug(target_unit)
    if not slug:
        return root / "latest.json"
    return root / f"latest.{slug}.json"


def hash_arm_token(value: str | None) -> str | None:
    token = str(value or "").strip()
    if not token:
        return None
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def build_rollout_contract(
    *,
    mode: str,
    target_unit: str,
    arm_token: str,
    ts_ms: int,
    note: str | None = None,
    canary_max_notional_quote: float | None = None,
) -> dict[str, Any]:
    normalized_mode = normalize_rollout_mode(mode)
    target_unit_value = str(target_unit).strip() or DEFAULT_LIVE_TARGET_UNIT
    return {
        "armed": True,
        "mode": normalized_mode,
        "target_unit": target_unit_value,
        "arm_token_sha256": hash_arm_token(arm_token),
        "armed_ts_ms": int(ts_ms),
        "armed_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts_ms / 1000.0)),
        "note": str(note).strip() if note is not None and str(note).strip() else None,
        "canary_max_notional_quote": (
            float(canary_max_notional_quote) if canary_max_notional_quote is not None else None
        ),
    }


def build_rollout_disarmed_contract(
    *,
    previous_contract: dict[str, Any] | None,
    ts_ms: int,
    note: str | None = None,
) -> dict[str, Any]:
    previous = dict(previous_contract or {})
    return {
        "armed": False,
        "mode": normalize_rollout_mode(previous.get("mode")),
        "target_unit": str(previous.get("target_unit") or DEFAULT_LIVE_TARGET_UNIT),
        "arm_token_sha256": str(previous.get("arm_token_sha256") or "") or None,
        "armed_ts_ms": int(previous.get("armed_ts_ms") or 0),
        "armed_at_utc": previous.get("armed_at_utc"),
        "disarmed_ts_ms": int(ts_ms),
        "disarmed_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts_ms / 1000.0)),
        "note": str(note).strip() if note is not None and str(note).strip() else None,
        "canary_max_notional_quote": previous.get("canary_max_notional_quote"),
    }


def build_rollout_test_order_record(
    *,
    market: str,
    side: str,
    ord_type: str,
    price: str | None,
    volume: str | None,
    ok: bool,
    response_payload: dict[str, Any] | None,
    ts_ms: int,
) -> dict[str, Any]:
    return {
        "market": str(market).strip().upper(),
        "side": str(side).strip().lower(),
        "ord_type": str(ord_type).strip().lower(),
        "price": str(price).strip() if price is not None else None,
        "volume": str(volume).strip() if volume is not None else None,
        "ok": bool(ok),
        "ts_ms": int(ts_ms),
        "checked_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts_ms / 1000.0)),
        "response_payload": dict(response_payload or {}),
    }


def evaluate_live_rollout_gate(
    *,
    mode: str,
    target_unit: str,
    contract: dict[str, Any] | None,
    test_order: dict[str, Any] | None,
    breaker_active: bool,
    require_test_order: bool,
    test_order_max_age_sec: int,
    small_account_single_slot_ready: bool,
    ts_ms: int,
) -> LiveRolloutGate:
    normalized_mode = normalize_rollout_mode(mode)
    target_unit_value = str(target_unit).strip() or DEFAULT_LIVE_TARGET_UNIT
    contract_value = dict(contract or {})
    test_order_value = dict(test_order or {})
    reasons: list[str] = []

    armed = bool(contract_value.get("armed"))
    contract_mode = normalize_rollout_mode(contract_value.get("mode")) if contract_value else normalized_mode
    contract_target_unit = str(contract_value.get("target_unit") or target_unit_value).strip() or target_unit_value
    test_order_required = bool(require_test_order) and normalized_mode in {"canary", "live"}

    test_order_ok = bool(test_order_value.get("ok"))
    test_order_age_sec: float | None = None
    test_order_ts = test_order_value.get("ts_ms")
    if test_order_ts is not None:
        try:
            test_order_age_sec = max((int(ts_ms) - int(test_order_ts)) / 1000.0, 0.0)
        except (TypeError, ValueError):
            test_order_age_sec = None

    breaker_clear = not bool(breaker_active)
    if normalized_mode in {"canary", "live"}:
        if not armed:
            reasons.append(ROLLOUT_REASON_NOT_ARMED)
        if contract_mode != normalized_mode:
            reasons.append(ROLLOUT_REASON_MODE_MISMATCH)
        if contract_target_unit != target_unit_value:
            reasons.append(ROLLOUT_REASON_UNIT_MISMATCH)
        if test_order_required and not test_order_ok:
            reasons.append(ROLLOUT_REASON_TEST_ORDER_REQUIRED)
        elif test_order_required and test_order_age_sec is not None and test_order_age_sec > max(int(test_order_max_age_sec), 1):
            reasons.append(ROLLOUT_REASON_TEST_ORDER_STALE)
        if not breaker_clear:
            reasons.append(ROLLOUT_REASON_BREAKER_ACTIVE)
        if normalized_mode == "canary" and not bool(small_account_single_slot_ready):
            reasons.append(ROLLOUT_REASON_CANARY_REQUIRES_SINGLE_SLOT)

    start_allowed = normalized_mode == "shadow" or len(reasons) == 0
    order_emission_allowed = start_allowed and normalized_mode != "shadow"
    return LiveRolloutGate(
        mode=normalized_mode,
        target_unit=target_unit_value,
        armed=armed,
        order_emission_allowed=order_emission_allowed,
        start_allowed=start_allowed,
        test_order_required=test_order_required,
        test_order_ok=test_order_ok,
        test_order_age_sec=test_order_age_sec,
        breaker_clear=breaker_clear,
        small_account_single_slot_ready=bool(small_account_single_slot_ready),
        reason_codes=tuple(reasons),
        contract=contract_value,
        test_order=test_order_value,
    )


def rollout_gate_to_payload(value: LiveRolloutGate) -> dict[str, Any]:
    payload = {
        "mode": value.mode,
        "target_unit": value.target_unit,
        "armed": value.armed,
        "start_allowed": value.start_allowed,
        "order_emission_allowed": value.order_emission_allowed,
        "test_order_required": value.test_order_required,
        "test_order_ok": value.test_order_ok,
        "test_order_age_sec": value.test_order_age_sec,
        "breaker_clear": value.breaker_clear,
        "small_account_single_slot_ready": value.small_account_single_slot_ready,
        "reason_codes": list(value.reason_codes),
        "contract": dict(value.contract),
        "test_order": dict(value.test_order),
    }
    return annotate_reason_payload(payload, reason_codes=payload["reason_codes"])


def load_rollout_latest(project_root: Path, *, target_unit: str | None = None) -> dict[str, Any]:
    path = rollout_latest_artifact_path(project_root, target_unit=target_unit)
    if not path.exists() and str(target_unit or "").strip():
        path = rollout_latest_artifact_path(project_root)
    if not path.exists():
        return {}
    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def write_rollout_latest(
    *,
    project_root: Path,
    event_kind: str,
    contract: dict[str, Any] | None,
    test_order: dict[str, Any] | None,
    status: dict[str, Any] | None,
    ts_ms: int,
    target_unit: str | None = None,
) -> Path:
    root = rollout_artifact_root(project_root)
    root.mkdir(parents=True, exist_ok=True)
    resolved_target_unit = (
        str(target_unit or "").strip()
        or str((status or {}).get("target_unit") or "").strip()
        or str((contract or {}).get("target_unit") or "").strip()
    )
    latest = {
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts_ms / 1000.0)),
        "ts_ms": int(ts_ms),
        "event_kind": str(event_kind).strip().upper(),
        "target_unit": resolved_target_unit or None,
        "contract": dict(contract or {}),
        "test_order": dict(test_order or {}),
        "status": dict(status or {}),
    }
    latest_path = rollout_latest_artifact_path(project_root)
    latest_path.write_text(json.dumps(latest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    scoped_latest_path = rollout_latest_artifact_path(project_root, target_unit=resolved_target_unit)
    if scoped_latest_path != latest_path:
        scoped_latest_path.write_text(
            json.dumps(latest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    slug = _rollout_target_slug(resolved_target_unit)
    archive_prefix = f"{slug}_" if slug else ""
    archive_name = f"{archive_prefix}{latest['event_kind'].lower()}_{int(ts_ms)}.json"
    (root / archive_name).write_text(
        json.dumps(latest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return latest_path
