"""Live confidence-sequence style monitor builders."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any


LIVE_RISK_CONFIDENCE_SEQUENCE_VERSION = 1

DEFAULT_NONPOSITIVE_RATE_REASON_CODE = "RISK_CONTROL_NONPOSITIVE_RATE_CS_BREACH"
DEFAULT_SEVERE_LOSS_RATE_REASON_CODE = "RISK_CONTROL_SEVERE_LOSS_RATE_CS_BREACH"
DEFAULT_EXECUTION_MISS_RATE_REASON_CODE = "EXECUTION_MISS_RATE_CS_BREACH"
DEFAULT_EDGE_GAP_RATE_REASON_CODE = "RISK_CONTROL_EDGE_GAP_CS_BREACH"
DEFAULT_FEATURE_DIVERGENCE_RATE_REASON_CODE = "FEATURE_DIVERGENCE_CS_BREACH"


def build_live_risk_confidence_sequence_report(
    *,
    store: Any,
    run_id: str,
    confidence_monitor_config: dict[str, Any] | None,
    runtime_health: dict[str, Any] | None,
    lane: str,
    unit_name: str,
    rollout_mode: str,
    ts_ms: int,
) -> dict[str, Any]:
    config = dict(confidence_monitor_config or {})
    run_id_value = str(run_id).strip()
    closed_trades = _load_closed_trade_rows(store=store, run_id=run_id_value)
    execution_attempts = _load_final_execution_attempt_rows(store=store, run_id=run_id_value)
    runtime_health_payload = dict(runtime_health or {})

    delta = max(float(_safe_optional_float(config.get("confidence_delta")) or 0.10), 1e-9)
    feature_divergence_threshold = max(float(_safe_optional_float(config.get("feature_divergence_rate_threshold")) or 0.10), 0.0)
    nonpositive_threshold = max(float(_safe_optional_float(config.get("nonpositive_rate_threshold")) or 0.45), 0.0)
    severe_threshold = max(float(_safe_optional_float(config.get("severe_loss_rate_threshold")) or 0.20), 0.0)
    execution_miss_threshold = max(float(_safe_optional_float(config.get("execution_miss_rate_threshold")) or 0.55), 0.0)
    edge_gap_threshold = max(float(_safe_optional_float(config.get("edge_gap_breach_rate_threshold")) or 0.60), 0.0)
    edge_gap_tolerance_bps = max(float(_safe_optional_float(config.get("edge_gap_tolerance_bps")) or 5.0), 0.0)
    severe_loss_return_threshold = max(float(_safe_optional_float(config.get("severe_loss_return_threshold")) or 0.01), 0.0)

    nonpositive_monitor = _build_binary_rate_monitor(
        monitor_name="nonpositive_return_rate",
        monitor_family="risk_performance_halt",
        observations=[float(item.get("realized_return_ratio") or 0.0) <= 0.0 for item in closed_trades],
        threshold=nonpositive_threshold,
        delta=delta,
        min_count=max(int(_safe_optional_int(config.get("min_closed_trade_count")) or 8), 1),
        reason_code=str(config.get("nonpositive_rate_reason_code") or DEFAULT_NONPOSITIVE_RATE_REASON_CODE).strip() or DEFAULT_NONPOSITIVE_RATE_REASON_CODE,
        statistic_name="nonpositive_return_rate",
        source="trade_journal.closed_verified",
        extra={
            "closed_trade_count": len(closed_trades),
        },
    )
    severe_monitor = _build_binary_rate_monitor(
        monitor_name="severe_loss_rate",
        monitor_family="risk_performance_halt",
        observations=[float(item.get("realized_return_ratio") or 0.0) <= -float(severe_loss_return_threshold) for item in closed_trades],
        threshold=severe_threshold,
        delta=delta,
        min_count=max(int(_safe_optional_int(config.get("min_closed_trade_count")) or 8), 1),
        reason_code=str(config.get("severe_loss_rate_reason_code") or DEFAULT_SEVERE_LOSS_RATE_REASON_CODE).strip() or DEFAULT_SEVERE_LOSS_RATE_REASON_CODE,
        statistic_name="severe_loss_rate",
        source="trade_journal.closed_verified",
        extra={
            "closed_trade_count": len(closed_trades),
            "severe_loss_return_threshold_ratio": float(severe_loss_return_threshold),
        },
    )
    miss_monitor = _build_binary_rate_monitor(
        monitor_name="execution_miss_rate",
        monitor_family="execution_quality_halt",
        observations=[
            str(item.get("final_state") or "").strip().upper() in {"MISSED", "PARTIAL_CANCELLED"}
            for item in execution_attempts
        ],
        threshold=execution_miss_threshold,
        delta=delta,
        min_count=max(int(_safe_optional_int(config.get("min_execution_attempt_count")) or 12), 1),
        reason_code=str(config.get("execution_miss_rate_reason_code") or DEFAULT_EXECUTION_MISS_RATE_REASON_CODE).strip() or DEFAULT_EXECUTION_MISS_RATE_REASON_CODE,
        statistic_name="execution_miss_rate",
        source="execution_attempts.final_state",
        extra={
            "execution_attempt_count": len(execution_attempts),
        },
    )
    edge_gap_monitor = _build_binary_rate_monitor(
        monitor_name="expected_vs_realized_edge_gap_rate",
        monitor_family="risk_performance_halt",
        observations=[
            (item.get("expected_net_edge_bps") is not None)
            and ((float(item.get("expected_net_edge_bps") or 0.0) - float(item.get("realized_pnl_bps") or 0.0)) > float(edge_gap_tolerance_bps))
            for item in closed_trades
            if item.get("expected_net_edge_bps") is not None
        ],
        threshold=edge_gap_threshold,
        delta=delta,
        min_count=max(int(_safe_optional_int(config.get("min_closed_trade_count")) or 8), 1),
        reason_code=str(config.get("edge_gap_rate_reason_code") or DEFAULT_EDGE_GAP_RATE_REASON_CODE).strip() or DEFAULT_EDGE_GAP_RATE_REASON_CODE,
        statistic_name="edge_gap_breach_rate",
        source="trade_journal.expected_net_edge_bps_vs_realized_pnl_bps",
        extra={
            "closed_trade_count": len(closed_trades),
            "edge_gap_tolerance_bps": float(edge_gap_tolerance_bps),
            "mean_edge_gap_bps": _mean_optional(
                [
                    float(item.get("expected_net_edge_bps") or 0.0) - float(item.get("realized_pnl_bps") or 0.0)
                    for item in closed_trades
                    if item.get("expected_net_edge_bps") is not None and item.get("realized_pnl_bps") is not None
                ]
            ),
        },
    )
    feature_divergence_monitor = {
        "monitor_name": "paper_live_feature_divergence_rate",
        "monitor_family": "model_data_divergence_halt",
        "available": False,
        "status": "source_unavailable",
        "source": "paired_paper_feature_divergence_artifact",
        "threshold": float(feature_divergence_threshold),
        "delta": float(delta),
        "reason_code": str(config.get("feature_divergence_rate_reason_code") or DEFAULT_FEATURE_DIVERGENCE_RATE_REASON_CODE).strip() or DEFAULT_FEATURE_DIVERGENCE_RATE_REASON_CODE,
        "halt_triggered": False,
        "clear_reason_codes": [],
        "message": "feature divergence source artifact is not yet available in the current runtime path",
    }

    monitors = {
        "nonpositive_return_rate": nonpositive_monitor,
        "severe_loss_rate": severe_monitor,
        "execution_miss_rate": miss_monitor,
        "expected_vs_realized_edge_gap_rate": edge_gap_monitor,
        "paper_live_feature_divergence_rate": feature_divergence_monitor,
    }
    triggered_reason_codes: list[str] = []
    clear_reason_codes: list[str] = []
    monitor_families_triggered: list[str] = []
    for payload in monitors.values():
        reason_code = str(payload.get("reason_code") or "").strip()
        if bool(payload.get("halt_triggered")) and reason_code:
            if reason_code not in triggered_reason_codes:
                triggered_reason_codes.append(reason_code)
            family = str(payload.get("monitor_family") or "").strip()
            if family and family not in monitor_families_triggered:
                monitor_families_triggered.append(family)
        for reason_code in payload.get("clear_reason_codes") or []:
            reason_code_text = str(reason_code).strip()
            if reason_code_text and reason_code_text not in clear_reason_codes:
                clear_reason_codes.append(reason_code_text)

    available_monitor_count = sum(1 for payload in monitors.values() if bool(payload.get("available")))
    triggered_monitor_count = sum(1 for payload in monitors.values() if bool(payload.get("halt_triggered")))
    return {
        "artifact_version": LIVE_RISK_CONFIDENCE_SEQUENCE_VERSION,
        "ts_ms": int(ts_ms),
        "lane": str(lane).strip(),
        "unit_name": str(unit_name).strip(),
        "rollout_mode": str(rollout_mode).strip().lower(),
        "run_id": run_id_value or None,
        "runtime_health": {
            "model_pointer_divergence": bool(runtime_health_payload.get("model_pointer_divergence", False)),
            "ws_public_stale": bool(runtime_health_payload.get("ws_public_stale", False)),
            "live_runtime_model_run_id": runtime_health_payload.get("live_runtime_model_run_id"),
        },
        "available_monitor_count": int(available_monitor_count),
        "triggered_monitor_count": int(triggered_monitor_count),
        "halt_triggered": bool(triggered_reason_codes),
        "triggered_reason_codes": triggered_reason_codes,
        "clear_reason_codes": clear_reason_codes,
        "monitor_families_triggered": monitor_families_triggered,
        "monitor_config": {
            "confidence_delta": float(delta),
            "min_closed_trade_count": max(int(_safe_optional_int(config.get("min_closed_trade_count")) or 8), 1),
            "min_execution_attempt_count": max(int(_safe_optional_int(config.get("min_execution_attempt_count")) or 12), 1),
            "nonpositive_rate_threshold": float(nonpositive_threshold),
            "severe_loss_rate_threshold": float(severe_threshold),
            "execution_miss_rate_threshold": float(execution_miss_threshold),
            "edge_gap_breach_rate_threshold": float(edge_gap_threshold),
            "edge_gap_tolerance_bps": float(edge_gap_tolerance_bps),
            "severe_loss_return_threshold_ratio": float(severe_loss_return_threshold),
        },
        "monitors": monitors,
    }


def write_live_risk_confidence_sequence_report(
    *,
    latest_path: Path,
    payload: dict[str, Any],
) -> Path:
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return latest_path


def live_risk_confidence_sequence_latest_path(
    *,
    project_root: Path,
    unit_name: str,
) -> Path:
    slug = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(unit_name).strip().lower()).strip("_")
    slug = "_".join(part for part in slug.split("_") if part)
    if not slug:
        slug = "live"
    return Path(project_root) / "logs" / "live_risk_confidence_sequence" / slug / "latest.json"


def _build_binary_rate_monitor(
    *,
    monitor_name: str,
    monitor_family: str,
    observations: list[bool],
    threshold: float,
    delta: float,
    min_count: int,
    reason_code: str,
    statistic_name: str,
    source: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    count = len(observations)
    if count <= 0:
        return {
            "monitor_name": monitor_name,
            "monitor_family": monitor_family,
            "available": False,
            "status": "insufficient_rows",
            "source": source,
            "threshold": float(threshold),
            "delta": float(delta),
            "sample_count": 0,
            "effective_sample_size": 0.0,
            "statistic_name": statistic_name,
            "empirical_rate": None,
            "cs_upper_bound": None,
            "reason_code": reason_code,
            "halt_triggered": False,
            "clear_reason_codes": [],
            **dict(extra or {}),
        }
    empirical_rate = sum(1 for value in observations if bool(value)) / float(count)
    effective_sample_size = float(count)
    cs_upper_bound = _time_uniform_rate_upper_bound(
        empirical_rate=float(empirical_rate),
        sample_count=int(count),
        delta=float(delta),
    )
    halt_triggered = bool(count >= int(min_count) and cs_upper_bound > float(threshold))
    clear_reason_codes = [] if halt_triggered else [reason_code]
    return {
        "monitor_name": monitor_name,
        "monitor_family": monitor_family,
        "available": True,
        "status": "ok",
        "source": source,
        "threshold": float(threshold),
        "delta": float(delta),
        "sample_count": int(count),
        "effective_sample_size": float(effective_sample_size),
        "statistic_name": statistic_name,
        "empirical_rate": float(empirical_rate),
        "cs_upper_bound": float(cs_upper_bound),
        "reason_code": reason_code,
        "halt_triggered": halt_triggered,
        "clear_reason_codes": clear_reason_codes,
        **dict(extra or {}),
    }


def _time_uniform_rate_upper_bound(*, empirical_rate: float, sample_count: int, delta: float) -> float:
    if sample_count <= 0:
        return 1.0
    rate = min(max(float(empirical_rate), 0.0), 1.0)
    sample_value = float(max(int(sample_count), 1))
    iterated_log = math.log(math.log(sample_value + 1.0) + math.e)
    bonus = math.sqrt(
        max(
            2.0 * (math.log(1.0 / max(float(delta), 1e-12)) + iterated_log),
            0.0,
        )
        / sample_value
    )
    return min(rate + bonus, 1.0)


def _load_closed_trade_rows(*, store: Any, run_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in store.list_trade_journal(statuses=("CLOSED",)):
        entry_meta = dict(row.get("entry_meta") or {})
        runtime = dict(entry_meta.get("runtime") or {}) if isinstance(entry_meta.get("runtime"), dict) else {}
        if str(runtime.get("live_runtime_model_run_id") or "").strip() != str(run_id).strip():
            continue
        exit_meta = dict(row.get("exit_meta") or {})
        if exit_meta.get("close_verified") is not True:
            continue
        realized_pnl_pct_points = _safe_optional_float(row.get("realized_pnl_pct"))
        if realized_pnl_pct_points is None:
            continue
        realized_return_ratio = float(realized_pnl_pct_points) / 100.0
        realized_pnl_bps = realized_return_ratio * 10_000.0
        rows.append(
            {
                "journal_id": row.get("journal_id"),
                "exit_ts_ms": _safe_optional_int(row.get("exit_ts_ms")) or _safe_optional_int(row.get("updated_ts")),
                "realized_return_ratio": float(realized_return_ratio),
                "realized_pnl_bps": float(realized_pnl_bps),
                "expected_net_edge_bps": _safe_optional_float(row.get("expected_net_edge_bps")),
            }
        )
    return rows


def _load_final_execution_attempt_rows(*, store: Any, run_id: str) -> list[dict[str, Any]]:
    journal_run_map: dict[str, str] = {}
    for row in store.list_trade_journal():
        journal_id = str(row.get("journal_id") or "").strip()
        if not journal_id:
            continue
        entry_meta = dict(row.get("entry_meta") or {})
        runtime = dict(entry_meta.get("runtime") or {}) if isinstance(entry_meta.get("runtime"), dict) else {}
        journal_run_map[journal_id] = str(runtime.get("live_runtime_model_run_id") or "").strip()
    intent_run_map: dict[str, str] = {}
    for row in store.list_intents():
        intent_id = str(row.get("intent_id") or "").strip()
        if not intent_id:
            continue
        meta = dict(row.get("meta") or {})
        runtime = dict(meta.get("runtime") or {}) if isinstance(meta.get("runtime"), dict) else {}
        intent_run_map[intent_id] = str(runtime.get("live_runtime_model_run_id") or "").strip()
    rows: list[dict[str, Any]] = []
    for row in store.list_execution_attempts(final_only=True, limit=5000):
        journal_id = str(row.get("journal_id") or "").strip()
        intent_id = str(row.get("intent_id") or "").strip()
        resolved_run_id = journal_run_map.get(journal_id) or intent_run_map.get(intent_id) or ""
        if str(resolved_run_id).strip() != str(run_id).strip():
            continue
        rows.append(dict(row))
    return rows


def _safe_optional_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_optional_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _mean_optional(values: list[float]) -> float | None:
    clean = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    if not clean:
        return None
    return float(sum(clean) / float(len(clean)))
