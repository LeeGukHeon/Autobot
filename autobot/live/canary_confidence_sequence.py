"""Canary sequential evidence artifact builders."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import numpy as np

from autobot.live.candidate_canary_report import build_candidate_canary_report
from autobot.risk.confidence_monitor import (
    DEFAULT_EDGE_GAP_RATE_REASON_CODE,
    DEFAULT_FEATURE_DIVERGENCE_RATE_REASON_CODE,
    DEFAULT_NONPOSITIVE_RATE_REASON_CODE,
    DEFAULT_SEVERE_LOSS_RATE_REASON_CODE,
    SUPPRESSOR_RESET_CHECKPOINT,
    _build_binary_rate_monitor,
    _load_closed_trade_rows,
    _mean_optional,
)


CANARY_CONFIDENCE_SEQUENCE_VERSION = 1


def canary_confidence_sequence_latest_path(*, project_root: Path, unit_name: str) -> Path:
    slug = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(unit_name).strip().lower()).strip("_")
    slug = "_".join(part for part in slug.split("_") if part)
    if not slug:
        slug = "canary"
    return Path(project_root) / "logs" / "canary_confidence_sequence" / slug / "latest.json"


def write_canary_confidence_sequence_report(*, latest_path: Path, payload: dict[str, Any]) -> Path:
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return latest_path


def build_canary_confidence_sequence_report(
    *,
    store: Any,
    run_id: str,
    confidence_monitor_config: dict[str, Any] | None,
    runtime_health: dict[str, Any] | None,
    lane: str,
    unit_name: str,
    rollout_mode: str,
    ts_ms: int,
    project_root: Path | None = None,
    feature_divergence_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    config = dict(confidence_monitor_config or {})
    run_id_value = str(run_id).strip()
    reset_baseline_ts_ms = _suppressor_reset_baseline_ts_ms(store=store, run_id=run_id_value)
    closed_trades = _load_closed_trade_rows(store=store, run_id=run_id_value, reset_baseline_ts_ms=reset_baseline_ts_ms)
    runtime_health_payload = dict(runtime_health or {})

    delta = max(float(_safe_optional_float(config.get("confidence_delta")) or 0.10), 1e-9)
    nonpositive_threshold = max(float(_safe_optional_float(config.get("nonpositive_rate_threshold")) or 0.45), 0.0)
    severe_threshold = max(float(_safe_optional_float(config.get("severe_loss_rate_threshold")) or 0.20), 0.0)
    edge_gap_threshold = max(float(_safe_optional_float(config.get("edge_gap_breach_rate_threshold")) or 0.60), 0.0)
    edge_gap_tolerance_bps = max(float(_safe_optional_float(config.get("edge_gap_tolerance_bps")) or 5.0), 0.0)
    severe_loss_return_threshold = max(float(_safe_optional_float(config.get("severe_loss_return_threshold")) or 0.01), 0.0)
    severe_loss_penalty_ratio = max(float(_safe_optional_float(config.get("severe_loss_penalty_ratio")) or severe_loss_return_threshold), 0.0)
    min_closed_trade_count = max(int(_safe_optional_int(config.get("min_closed_trade_count")) or 8), 1)

    nonpositive_monitor = _build_binary_rate_monitor(
        monitor_name="canary_nonpositive_return_rate",
        monitor_family="canary_reward_halt",
        observations=[float(item.get("realized_return_ratio") or 0.0) <= 0.0 for item in closed_trades],
        threshold=nonpositive_threshold,
        delta=delta,
        min_count=min_closed_trade_count,
        reason_code=str(config.get("nonpositive_rate_reason_code") or DEFAULT_NONPOSITIVE_RATE_REASON_CODE).strip() or DEFAULT_NONPOSITIVE_RATE_REASON_CODE,
        statistic_name="nonpositive_return_rate",
        source="trade_journal.closed_verified",
        extra={"closed_trade_count": len(closed_trades)},
    )
    severe_monitor = _build_binary_rate_monitor(
        monitor_name="canary_severe_loss_rate",
        monitor_family="canary_reward_halt",
        observations=[float(item.get("realized_return_ratio") or 0.0) <= -float(severe_loss_return_threshold) for item in closed_trades],
        threshold=severe_threshold,
        delta=delta,
        min_count=min_closed_trade_count,
        reason_code=str(config.get("severe_loss_rate_reason_code") or DEFAULT_SEVERE_LOSS_RATE_REASON_CODE).strip() or DEFAULT_SEVERE_LOSS_RATE_REASON_CODE,
        statistic_name="severe_loss_rate",
        source="trade_journal.closed_verified",
        extra={
            "closed_trade_count": len(closed_trades),
            "severe_loss_return_threshold_ratio": float(severe_loss_return_threshold),
        },
    )
    edge_gap_monitor = _build_binary_rate_monitor(
        monitor_name="canary_expected_vs_realized_edge_gap_rate",
        monitor_family="canary_divergence_halt",
        observations=[
            (item.get("expected_net_edge_bps") is not None)
            and ((float(item.get("expected_net_edge_bps") or 0.0) - float(item.get("realized_pnl_bps") or 0.0)) > float(edge_gap_tolerance_bps))
            for item in closed_trades
            if item.get("expected_net_edge_bps") is not None
        ],
        threshold=edge_gap_threshold,
        delta=delta,
        min_count=min_closed_trade_count,
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
    divergence_report = dict(feature_divergence_report or {})
    if not divergence_report and project_root is not None:
        from autobot.live.paper_live_divergence import load_paper_live_divergence_report

        divergence_report = load_paper_live_divergence_report(project_root=Path(project_root), unit_name=unit_name)
    feature_divergence_monitor = _build_canary_feature_divergence_monitor(
        report=divergence_report,
        threshold=float(max(float(_safe_optional_float(config.get("feature_divergence_rate_threshold")) or 0.10), 0.0)),
        delta=float(delta),
        min_count=min_closed_trade_count,
        reason_code=str(config.get("feature_divergence_rate_reason_code") or DEFAULT_FEATURE_DIVERGENCE_RATE_REASON_CODE).strip() or DEFAULT_FEATURE_DIVERGENCE_RATE_REASON_CODE,
    )

    reward_stream = _build_reward_stream_payload(
        closed_trades=closed_trades,
        delta=delta,
        severe_loss_rate_upper_bound=float(severe_monitor.get("cs_upper_bound") or 1.0),
        severe_loss_penalty_ratio=float(severe_loss_penalty_ratio),
    )
    reason_codes: list[str] = []
    if bool(severe_monitor.get("halt_triggered")):
        reason_codes.append(str(severe_monitor.get("reason_code") or DEFAULT_SEVERE_LOSS_RATE_REASON_CODE))
    if bool(edge_gap_monitor.get("halt_triggered")):
        reason_codes.append(str(edge_gap_monitor.get("reason_code") or DEFAULT_EDGE_GAP_RATE_REASON_CODE))
    if bool(feature_divergence_monitor.get("halt_triggered")):
        reason_codes.append(str(feature_divergence_monitor.get("reason_code") or DEFAULT_FEATURE_DIVERGENCE_RATE_REASON_CODE))

    if reward_stream["sample_count"] < min_closed_trade_count:
        decision = "continue"
        reason_codes.append("CANARY_INSUFFICIENT_EVIDENCE")
    elif reward_stream["risk_adjusted_return_lcb"] > 0.0 and not reason_codes:
        decision = "promote_eligible"
    elif reward_stream["return_ucb"] < 0.0 or reason_codes:
        decision = "abort"
    else:
        decision = "continue"

    canary_summary = _build_current_run_canary_summary(store=store, run_id=run_id_value)
    db_path = getattr(store, "db_path", None)
    cumulative_report = build_candidate_canary_report(db_path) if isinstance(db_path, Path) and db_path.exists() else {}
    return {
        "artifact_version": CANARY_CONFIDENCE_SEQUENCE_VERSION,
        "policy": "canary_confidence_sequence_v1",
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
        "reset_baseline_ts_ms": reset_baseline_ts_ms,
        "reward_stream": reward_stream,
        "monitors": {
            "nonpositive_return_rate": nonpositive_monitor,
            "severe_loss_rate": severe_monitor,
            "expected_vs_realized_edge_gap_rate": edge_gap_monitor,
            "paper_live_feature_divergence_rate": feature_divergence_monitor,
        },
        "canary_summary_current_run": canary_summary,
        "candidate_canary_report_cumulative": {
            "generated_at_utc": cumulative_report.get("generated_at_utc"),
            "closed_total": cumulative_report.get("closed_total"),
            "verified_closed_total": cumulative_report.get("verified_closed_total"),
            "realized_pnl_quote_total_verified": cumulative_report.get("realized_pnl_quote_total_verified"),
            "win_rate_verified_pct": cumulative_report.get("win_rate_verified_pct"),
            "profit_factor_verified": cumulative_report.get("profit_factor_verified"),
        },
        "decision": {
            "status": decision,
            "reason_codes": sorted({str(code).strip() for code in reason_codes if str(code).strip()}),
            "promote_eligible": decision == "promote_eligible",
            "abort": decision == "abort",
            "continue": decision == "continue",
        },
    }


def _build_reward_stream_payload(
    *,
    closed_trades: list[dict[str, Any]],
    delta: float,
    severe_loss_rate_upper_bound: float,
    severe_loss_penalty_ratio: float,
) -> dict[str, Any]:
    returns = [float(item.get("realized_return_ratio") or 0.0) for item in closed_trades]
    realized_pnl_quote = [float(item.get("realized_pnl_quote") or 0.0) for item in closed_trades if item.get("realized_pnl_quote") is not None]
    sample_count = len(returns)
    empirical_mean = float(sum(returns) / float(sample_count)) if sample_count > 0 else 0.0
    lcb, ucb = _time_uniform_mean_bounds(values=returns, delta=delta, value_bound=max(abs(float(severe_loss_penalty_ratio)), 0.20))
    risk_adjusted_lcb = float(lcb - (float(severe_loss_rate_upper_bound) * float(severe_loss_penalty_ratio)))
    return {
        "definition": "closed_trade_realized_return_ratio_stream",
        "sample_count": int(sample_count),
        "empirical_mean_return": float(empirical_mean),
        "return_lcb": float(lcb),
        "return_ucb": float(ucb),
        "risk_adjusted_return_lcb": float(risk_adjusted_lcb),
        "realized_pnl_quote_total": float(sum(realized_pnl_quote)),
        "realized_pnl_quote_mean": float(sum(realized_pnl_quote) / float(len(realized_pnl_quote))) if realized_pnl_quote else 0.0,
    }


def _time_uniform_mean_bounds(*, values: list[float], delta: float, value_bound: float) -> tuple[float, float]:
    if not values:
        return -float(value_bound), float(value_bound)
    clipped = np.clip(np.asarray(values, dtype=np.float64), -abs(float(value_bound)), abs(float(value_bound)))
    n = float(clipped.size)
    empirical_mean = float(np.mean(clipped))
    iterated_log = math.log(math.log(n + 1.0) + math.e)
    bonus = abs(float(value_bound)) * math.sqrt(max(2.0 * (math.log(1.0 / max(float(delta), 1e-12)) + iterated_log), 0.0) / n)
    return empirical_mean - bonus, empirical_mean + bonus


def _build_canary_feature_divergence_monitor(
    *,
    report: dict[str, Any] | None,
    threshold: float,
    delta: float,
    min_count: int,
    reason_code: str,
) -> dict[str, Any]:
    payload = dict(report or {})
    matched_records = [
        dict(item)
        for item in (payload.get("matched_records") or [])
        if isinstance(item, dict) and item.get("feature_hash_match") is not None
    ]
    if not matched_records:
        return {
            "monitor_name": "canary_feature_divergence_rate",
            "monitor_family": "canary_divergence_halt",
            "available": False,
            "status": str(payload.get("status") or "insufficient_source_data").strip().lower() or "insufficient_source_data",
            "source": "paper_live_divergence_artifact",
            "threshold": float(threshold),
            "delta": float(delta),
            "reason_code": str(reason_code).strip() or DEFAULT_FEATURE_DIVERGENCE_RATE_REASON_CODE,
            "halt_triggered": False,
            "clear_reason_codes": [],
            "message": "canary feature divergence artifact is unavailable or has insufficient matched data",
            "artifact_path": _safe_optional_text(payload.get("artifact_path")),
        }
    monitor = _build_binary_rate_monitor(
        monitor_name="canary_feature_divergence_rate",
        monitor_family="canary_divergence_halt",
        observations=[not bool(item.get("feature_hash_match")) for item in matched_records],
        threshold=float(threshold),
        delta=float(delta),
        min_count=max(int(min_count), 1),
        reason_code=str(reason_code).strip() or DEFAULT_FEATURE_DIVERGENCE_RATE_REASON_CODE,
        statistic_name="feature_divergence_rate",
        source="paper_live_divergence_artifact",
        extra={
            "matched_opportunities": int((payload.get("matching") or {}).get("matched_opportunities") or len(matched_records)),
            "feature_hash_match_ratio": _safe_optional_float(((payload.get("feature_divergence") or {}).get("feature_hash_match_ratio"))),
            "decision_divergence_rate": _safe_optional_float(((payload.get("decision_divergence") or {}).get("decision_divergence_rate"))),
        },
    )
    monitor["status"] = "ready"
    monitor["artifact_path"] = _safe_optional_text(payload.get("artifact_path"))
    return monitor


def _build_current_run_canary_summary(*, store: Any, run_id: str) -> dict[str, Any]:
    rows = _load_current_run_trade_rows(store=store, run_id=run_id)
    closed_rows = [row for row in rows if str(row.get("status") or "").strip().upper() == "CLOSED"]
    verified_rows = [row for row in closed_rows if (row.get("exit_meta") or {}).get("close_verified") is True]
    open_rows = [row for row in rows if str(row.get("status") or "").strip().upper() == "OPEN"]
    cancelled_rows = [row for row in rows if str(row.get("status") or "").strip().upper() == "CANCELLED_ENTRY"]
    realized_values = [float(row["realized_pnl_quote"]) for row in verified_rows if row.get("realized_pnl_quote") is not None]
    return {
        "rows_total": len(rows),
        "closed_total": len(closed_rows),
        "verified_closed_total": len(verified_rows),
        "open_total": len(open_rows),
        "cancelled_entry_total": len(cancelled_rows),
        "realized_pnl_quote_total_verified": float(sum(realized_values)),
        "wins_verified": sum(1 for value in realized_values if value > 0.0),
        "losses_verified": sum(1 for value in realized_values if value < 0.0),
        "win_rate_verified_pct": (sum(1 for value in realized_values if value > 0.0) / len(realized_values) * 100.0) if realized_values else None,
    }


def _load_current_run_trade_rows(*, store: Any, run_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in store.list_trade_journal():
        entry_meta = dict(row.get("entry_meta") or {})
        runtime = dict(entry_meta.get("runtime") or {}) if isinstance(entry_meta.get("runtime"), dict) else {}
        if str(runtime.get("live_runtime_model_run_id") or "").strip() != str(run_id).strip():
            continue
        payload = dict(row)
        payload["exit_meta"] = dict(row.get("exit_meta") or {}) if isinstance(row.get("exit_meta"), dict) else {}
        rows.append(payload)
    return rows


def _suppressor_reset_baseline_ts_ms(*, store: Any, run_id: str) -> int | None:
    if not hasattr(store, "get_checkpoint"):
        return None
    checkpoint = store.get_checkpoint(name=SUPPRESSOR_RESET_CHECKPOINT)
    payload = dict((checkpoint or {}).get("payload") or {})
    checkpoint_run_id = str(payload.get("run_id") or "").strip()
    if checkpoint_run_id and checkpoint_run_id != str(run_id).strip():
        return None
    return _safe_optional_int((checkpoint or {}).get("ts_ms"))


def _safe_optional_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_optional_int(value: Any) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None
