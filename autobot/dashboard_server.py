"""Read-only operations dashboard for training, paper, and live runtime."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import time
from typing import Any
from urllib.parse import urlparse

from autobot.live.order_state import is_open_local_state, normalize_order_state
from autobot.models.runtime_recommendation_contract import normalize_runtime_recommendations_payload


DEFAULT_DASHBOARD_HOST = "0.0.0.0"
DEFAULT_DASHBOARD_PORT = 8088
_DASHBOARD_ASSETS_DIR = Path(__file__).with_name("dashboard_assets")
_KST = timezone(timedelta(hours=9), name="KST")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        parsed = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _dig(payload: dict[str, Any] | None, *path: str, default: Any = None) -> Any:
    current: Any = payload or {}
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def _coerce_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _path_mtime_iso(path: Path | None) -> str | None:
    if path is None or not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_ws_public_status(*, meta_dir: Path, raw_root: Path) -> dict[str, Any]:
    health = _load_json(meta_dir / "ws_public_health.json")
    collect_report = _load_json(meta_dir / "ws_collect_report.json")
    runs_summary = _load_json(meta_dir / "ws_runs_summary.json")
    validate_report = _load_json(meta_dir / "ws_validate_report.json")
    latest_run = None
    runs = runs_summary.get("runs") if isinstance(runs_summary, dict) else None
    if isinstance(runs, list) and runs:
        candidate = runs[-1]
        latest_run = candidate if isinstance(candidate, dict) else None
    return {
        "meta_dir": str(meta_dir),
        "raw_root": str(raw_root),
        "health_snapshot": health,
        "collect_report": collect_report,
        "validate_report": validate_report,
        "runs_summary_latest": latest_run,
    }


def _truncate(value: str | None, limit: int = 120) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


@lru_cache(maxsize=4)
def _cached_project_size(project_root_str: str, bucket: int) -> int:
    project_root = Path(project_root_str)
    if shutil.which("du"):
        try:
            completed = subprocess.run(
                ["du", "-s", "-B1", str(project_root)],
                capture_output=True,
                text=True,
                check=False,
                timeout=10,
            )
            if completed.returncode == 0:
                first = str(completed.stdout).strip().split()[0]
                value = int(first)
                if value >= 0:
                    return value
        except (OSError, ValueError, IndexError, subprocess.TimeoutExpired):
            pass
    total = 0
    for path in project_root.rglob("*"):
        try:
            if path.is_file():
                total += path.stat().st_size
        except OSError:
            continue
    return total


def _project_size_bytes(project_root: Path) -> int:
    bucket = int(time.time() // 30)
    return _cached_project_size(str(project_root), bucket)


def _filesystem_usage(project_root: Path) -> dict[str, Any]:
    usage = shutil.disk_usage(project_root)
    return {
        "total_bytes": int(usage.total),
        "used_bytes": int(usage.used),
        "free_bytes": int(usage.free),
        "project_used_bytes": int(_project_size_bytes(project_root)),
    }


def _systemctl_show(unit_name: str, *properties: str) -> dict[str, str]:
    requested = tuple(properties) or ("ActiveState", "SubState", "UnitFileState", "MainPID")
    if not shutil.which("systemctl"):
        return {}
    args = ["systemctl", "show", unit_name, "--no-pager"]
    for prop in requested:
        args.extend(["-p", prop])
    try:
        completed = subprocess.run(
            args,
            capture_output=True,
            text=True,
            check=False,
            timeout=8,
        )
    except (OSError, subprocess.TimeoutExpired):
        return {}
    if completed.returncode != 0:
        return {}
    payload: dict[str, str] = {}
    for line in str(completed.stdout).splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        payload[str(key).strip()] = str(value).strip()
    return payload


def _unit_snapshot(unit_name: str, *, timer: bool = False) -> dict[str, Any]:
    properties = [
        "ActiveState",
        "SubState",
        "UnitFileState",
        "MainPID",
        "ExecMainStartTimestamp",
        "ExecMainExitTimestamp",
        "Description",
    ]
    if timer:
        properties.extend(["NextElapseUSecRealtime", "LastTriggerUSec"])
    payload = _systemctl_show(unit_name, *properties)
    return {
        "unit": unit_name,
        "active_state": payload.get("ActiveState") or "unknown",
        "sub_state": payload.get("SubState") or "unknown",
        "unit_file_state": payload.get("UnitFileState") or "unknown",
        "main_pid": _coerce_int(payload.get("MainPID")),
        "started_at": payload.get("ExecMainStartTimestamp") or None,
        "exited_at": payload.get("ExecMainExitTimestamp") or None,
        "description": payload.get("Description") or unit_name,
        "next_run_at": payload.get("NextElapseUSecRealtime") if timer else None,
        "last_trigger_at": payload.get("LastTriggerUSec") if timer else None,
    }


def _latest_paper_summaries(project_root: Path, limit: int = 4) -> list[dict[str, Any]]:
    runs_root = project_root / "data" / "paper" / "runs"
    if not runs_root.exists():
        return []
    summary_paths = sorted(
        runs_root.glob("paper-*/summary.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )[: max(limit, 1)]
    items: list[dict[str, Any]] = []
    for summary_path in summary_paths:
        payload = _load_json(summary_path)
        items.append(
            {
                "run_id": payload.get("run_id") or summary_path.parent.name,
                "feature_provider": payload.get("feature_provider"),
                "micro_provider": payload.get("micro_provider"),
                "orders_submitted": _coerce_int(payload.get("orders_submitted")) or 0,
                "orders_filled": _coerce_int(payload.get("orders_filled")) or 0,
                "fill_rate": _coerce_float(payload.get("fill_rate")),
                "realized_pnl_quote": _coerce_float(payload.get("realized_pnl_quote")),
                "unrealized_pnl_quote": _coerce_float(payload.get("unrealized_pnl_quote")),
                "max_drawdown_pct": _coerce_float(payload.get("max_drawdown_pct")),
                "duration_sec": _coerce_float(payload.get("duration_sec")),
                "warmup_satisfied": bool(payload.get("warmup_satisfied", False)),
                "events": _coerce_int(payload.get("events")),
                "updated_at": _path_mtime_iso(summary_path),
                "summary_path": str(summary_path),
            }
        )
    return items


def _summarize_acceptance(latest_path: Path) -> dict[str, Any]:
    payload = _load_json(latest_path)
    candidate_run_id = (
        payload.get("candidate_run_id")
        or _dig(payload, "steps", "train", "candidate_run_id")
        or _dig(payload, "candidate", "run_id")
    )
    champion_before = payload.get("champion_before_run_id") or _dig(payload, "candidate", "champion_before_run_id")
    overall_pass = payload.get("overall_pass")
    if overall_pass is None:
        overall_pass = _dig(payload, "gates", "overall_pass")
    backtest_pass = payload.get("backtest_pass")
    if backtest_pass is None:
        backtest_pass = _dig(payload, "gates", "backtest", "pass")
    paper_pass = payload.get("paper_pass")
    if paper_pass is None:
        paper_pass = _dig(payload, "gates", "paper", "pass")
    trainer_reasons = (
        _dig(payload, "gates", "backtest", "trainer_evidence_reasons", default=[])
        or _dig(payload, "steps", "train", "trainer_evidence", "reasons", default=[])
        or []
    )
    reasons = payload.get("reasons") if isinstance(payload.get("reasons"), list) else []
    notes = payload.get("notes") if isinstance(payload.get("notes"), list) else []
    return {
        "candidate_run_id": candidate_run_id,
        "candidate_run_dir": payload.get("candidate_run_dir") or _dig(payload, "steps", "train", "candidate_run_dir"),
        "champion_before_run_id": champion_before,
        "champion_after_run_id": payload.get("champion_after_run_id") or _dig(payload, "candidate", "champion_after_run_id"),
        "overall_pass": overall_pass,
        "backtest_pass": backtest_pass,
        "paper_pass": paper_pass,
        "decision_basis": _dig(payload, "gates", "backtest", "decision_basis"),
        "trainer_reasons": trainer_reasons,
        "reasons": reasons,
        "notes": notes,
        "generated_at": payload.get("generated_at"),
        "completed_at": payload.get("completed_at") or _path_mtime_iso(latest_path),
        "batch_date": payload.get("batch_date"),
        "model_family": payload.get("model_family"),
        "artifact_path": str(latest_path),
    }


def _summarize_challenger(latest_path: Path, current_state_path: Path) -> dict[str, Any]:
    payload = _load_json(latest_path)
    current_state = _load_json(current_state_path)
    start_step = _dig(payload, "steps", "start_challenger", default={}) or {}
    return {
        "candidate_run_id": start_step.get("candidate_run_id") or current_state.get("candidate_run_id"),
        "started": start_step.get("started"),
        "reason": start_step.get("reason"),
        "acceptance_notes": start_step.get("acceptance_notes") if isinstance(start_step.get("acceptance_notes"), list) else [],
        "challenger_unit": start_step.get("challenger_unit"),
        "paper_model_ref": start_step.get("paper_model_ref"),
        "paper_feature_provider": start_step.get("paper_feature_provider"),
        "generated_at": payload.get("generated_at"),
        "completed_at": _path_mtime_iso(latest_path),
        "current_state": current_state,
        "artifact_path": str(latest_path),
    }


def _summarize_rank_shadow_cycle(latest_path: Path, governance_path: Path) -> dict[str, Any]:
    payload = _load_json(latest_path)
    governance = _load_json(governance_path)
    return {
        "status": payload.get("status"),
        "next_action": payload.get("next_action"),
        "action_reason": payload.get("action_reason"),
        "candidate_run_id": payload.get("candidate_run_id"),
        "lane_id": payload.get("lane_id"),
        "lane_role": payload.get("lane_role"),
        "lane_shadow_only": payload.get("lane_shadow_only"),
        "overall_pass": payload.get("overall_pass"),
        "backtest_pass": payload.get("backtest_pass"),
        "decision_basis": payload.get("decision_basis"),
        "generated_at": payload.get("generated_at"),
        "completed_at": _path_mtime_iso(latest_path),
        "artifact_path": str(latest_path),
        "governance_action": governance,
        "governance_action_path": str(governance_path),
    }


def _query_all(conn: sqlite3.Connection, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def _query_one(conn: sqlite3.Connection, query: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    conn.row_factory = sqlite3.Row
    row = conn.execute(query, params).fetchone()
    return dict(row) if row is not None else None


def _open_ro_sqlite(db_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


def _normalize_json_text(value: Any) -> Any:
    if value in (None, ""):
        return None
    try:
        return json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return value


def _derive_live_exit_mode(plan: dict[str, Any]) -> str:
    if bool(plan.get("tp_enabled")) or bool(plan.get("sl_enabled")) or bool(plan.get("trailing_enabled")):
        return "risk"
    if _coerce_int(plan.get("timeout_ts_ms")) is not None:
        return "hold"
    return "none"


def _summarize_live_position(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "market": row.get("market"),
        "base_amount": _coerce_float(row.get("base_amount")),
        "avg_entry_price": _coerce_float(row.get("avg_entry_price")),
        "managed": bool(row.get("managed", 1)),
        "updated_ts": _coerce_int(row.get("updated_ts")),
    }


def _summarize_live_order(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "uuid": row.get("uuid"),
        "market": row.get("market"),
        "side": row.get("side"),
        "ord_type": row.get("ord_type"),
        "price": _coerce_float(row.get("price")),
        "volume_req": _coerce_float(row.get("volume_req")),
        "volume_filled": _coerce_float(row.get("volume_filled")),
        "local_state": row.get("local_state"),
        "raw_exchange_state": row.get("raw_exchange_state"),
        "intent_id": row.get("intent_id"),
        "replace_seq": _coerce_int(row.get("replace_seq")),
        "updated_ts": _coerce_int(row.get("updated_ts")),
    }


def _summarize_live_risk_plan(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "plan_id": row.get("plan_id"),
        "market": row.get("market"),
        "state": row.get("state"),
        "plan_source": row.get("plan_source"),
        "source_intent_id": row.get("source_intent_id"),
        "entry_price": _coerce_float(row.get("entry_price_str")),
        "qty": _coerce_float(row.get("qty_str")),
        "tp_enabled": bool(row.get("tp_enabled")),
        "tp_pct": _coerce_float(row.get("tp_pct")),
        "sl_enabled": bool(row.get("sl_enabled")),
        "sl_pct": _coerce_float(row.get("sl_pct")),
        "trailing_enabled": bool(row.get("trailing_enabled")),
        "trail_pct": _coerce_float(row.get("trail_pct")),
        "timeout_ts_ms": _coerce_int(row.get("timeout_ts_ms")),
        "current_exit_order_uuid": row.get("current_exit_order_uuid"),
        "replace_attempt": _coerce_int(row.get("replace_attempt")),
        "updated_ts": _coerce_int(row.get("updated_ts")),
        "exit_mode": _derive_live_exit_mode(row),
    }


def _summarize_execution_compare_metrics(summary: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(summary or {})
    realized_pnl_quote = _coerce_float(payload.get("realized_pnl_quote"))
    if realized_pnl_quote is None:
        realized_pnl_quote = _coerce_float(payload.get("realized_pnl_quote_total"))
    return {
        "realized_pnl_quote": realized_pnl_quote,
        "unrealized_pnl_quote": _coerce_float(payload.get("unrealized_pnl_quote")),
        "fill_rate": _coerce_float(payload.get("fill_rate")),
        "max_drawdown_pct": _coerce_float(payload.get("max_drawdown_pct")),
        "slippage_bps_mean": _coerce_float(payload.get("slippage_bps_mean")),
        "orders_filled": _coerce_int(payload.get("orders_filled")),
    }


def _summarize_exit_mode_compare(exit_payload: dict[str, Any]) -> dict[str, Any]:
    compare_doc = dict(exit_payload.get("exit_mode_compare") or {})
    decision = str(compare_doc.get("decision") or "").strip().lower()
    reasons = compare_doc.get("reasons") if isinstance(compare_doc.get("reasons"), list) else []
    utility_score = _coerce_float(compare_doc.get("utility_score"))
    recommended_mode = str(exit_payload.get("recommended_exit_mode") or exit_payload.get("mode") or "").strip().lower()
    winner_ko = "리스크 관리형" if recommended_mode == "risk" else "시간 보유"
    if not compare_doc:
        reason_code = str(exit_payload.get("recommended_exit_mode_reason_code") or "").strip()
        summary_ko = (
            "시간 보유와 리스크 관리형 비교 기록이 없습니다."
            if not reason_code
            else f"{winner_ko} 선택: {reason_code}"
        )
    else:
        if decision == "candidate_edge":
            decision_ko = f"{winner_ko} 우세"
        elif decision == "champion_edge":
            decision_ko = f"{winner_ko} 선택"
        elif decision == "indeterminate":
            decision_ko = f"{winner_ko} 유지"
        else:
            decision_ko = winner_ko
        reason_ko = " / ".join(str(item) for item in reasons if item) or "근거 요약 없음"
        if utility_score is None:
            summary_ko = f"{decision_ko}: {reason_ko}"
        else:
            summary_ko = f"{decision_ko}: {reason_ko}, 효용 점수 {utility_score:.3f}"
    return {
        "recommended_exit_mode": recommended_mode or None,
        "recommended_exit_mode_reason_code": exit_payload.get("recommended_exit_mode_reason_code"),
        "recommended_exit_mode_source": exit_payload.get("recommended_exit_mode_source"),
        "decision": decision or None,
        "reasons": reasons,
        "utility_score": utility_score,
        "summary_ko": summary_ko,
        "hold": _summarize_execution_compare_metrics(exit_payload.get("summary")),
        "risk": _summarize_execution_compare_metrics(exit_payload.get("risk_summary")),
    }


def _resolve_model_run_dir(project_root: Path, run_id: str | None) -> Path | None:
    run_id_value = str(run_id or "").strip()
    if not run_id_value:
        return None
    registry_root = project_root / "models" / "registry"
    if not registry_root.exists():
        return None
    for candidate in registry_root.glob(f"*/{run_id_value}"):
        if candidate.is_dir():
            return candidate
    return None


def _summarize_live_intent(row: dict[str, Any]) -> dict[str, Any]:
    meta = _normalize_json_text(row.get("meta_json"))
    meta_dict = meta if isinstance(meta, dict) else {}
    admissibility = _dig(meta_dict, "admissibility", "decision", default={}) or {}
    sizing = _dig(meta_dict, "admissibility", "sizing", default={}) or {}
    strategy_meta = _dig(meta_dict, "strategy", "meta", default={}) or {}
    trade_action = strategy_meta.get("trade_action") if isinstance(strategy_meta.get("trade_action"), dict) else {}
    trade_gate = _dig(meta_dict, "trade_gate", default={}) or {}
    requested_price = _coerce_float(row.get("price"))
    requested_volume = _coerce_float(row.get("volume"))
    inferred_notional = None
    if requested_price is not None and requested_volume is not None:
        inferred_notional = requested_price * requested_volume
    return {
        "intent_id": row.get("intent_id"),
        "ts_ms": _coerce_int(row.get("ts_ms")),
        "market": row.get("market"),
        "side": row.get("side"),
        "price": requested_price,
        "volume": requested_volume,
        "notional_quote": _coerce_float(sizing.get("target_notional_quote"))
        or _coerce_float(sizing.get("admissible_notional_quote"))
        or inferred_notional,
        "reason_code": row.get("reason_code"),
        "status": row.get("status"),
        "selection_policy_mode": strategy_meta.get("selection_policy_mode"),
        "prob": _coerce_float(strategy_meta.get("model_prob")),
        "skip_reason": meta_dict.get("skip_reason")
        or trade_gate.get("reason_code")
        or admissibility.get("reject_code"),
        "estimated_total_cost_bps": _coerce_float(admissibility.get("estimated_total_cost_bps")),
        "expected_net_edge_bps": _coerce_float(admissibility.get("expected_net_edge_bps")),
        "trade_action_recommended_action": trade_action.get("recommended_action"),
        "trade_action_expected_edge_bps": (
            (_coerce_float(trade_action.get("expected_edge")) or 0.0) * 10_000.0
            if _coerce_float(trade_action.get("expected_edge")) is not None
            else None
        ),
        "trade_action_expected_downside_bps": (
            (_coerce_float(trade_action.get("expected_downside_deviation")) or 0.0) * 10_000.0
            if _coerce_float(trade_action.get("expected_downside_deviation")) is not None
            else None
        ),
        "trade_action_objective_score": _coerce_float(trade_action.get("expected_objective_score")),
        "trade_action_notional_multiplier": _coerce_float(trade_action.get("recommended_notional_multiplier")),
    }


def _summarize_live_trade_journal(row: dict[str, Any]) -> dict[str, Any]:
    exit_meta = _normalize_json_text(row.get("exit_meta_json")) or {}
    entry_ts_ms = _coerce_int(row.get("entry_filled_ts_ms")) or _coerce_int(row.get("entry_submitted_ts_ms"))
    exit_ts_ms = _coerce_int(row.get("exit_ts_ms"))
    hold_minutes = None
    if entry_ts_ms is not None and exit_ts_ms is not None and exit_ts_ms >= entry_ts_ms:
        hold_minutes = max(0, int(round((exit_ts_ms - entry_ts_ms) / 60000)))
    return {
        "journal_id": row.get("journal_id"),
        "market": row.get("market"),
        "status": row.get("status"),
        "entry_intent_id": row.get("entry_intent_id"),
        "entry_order_uuid": row.get("entry_order_uuid"),
        "exit_order_uuid": row.get("exit_order_uuid"),
        "plan_id": row.get("plan_id"),
        "entry_ts_ms": entry_ts_ms,
        "exit_ts_ms": exit_ts_ms,
        "hold_minutes": hold_minutes,
        "entry_price": _coerce_float(row.get("entry_price")),
        "exit_price": _coerce_float(row.get("exit_price")),
        "qty": _coerce_float(row.get("qty")),
        "entry_notional_quote": _coerce_float(row.get("entry_notional_quote")),
        "exit_notional_quote": _coerce_float(row.get("exit_notional_quote")),
        "realized_pnl_quote": _coerce_float(row.get("realized_pnl_quote")),
        "realized_pnl_pct": _coerce_float(row.get("realized_pnl_pct")),
        "gross_pnl_quote": _coerce_float(exit_meta.get("gross_pnl_quote")),
        "gross_pnl_pct": _coerce_float(exit_meta.get("gross_pnl_pct")),
        "total_fee_quote": _coerce_float(exit_meta.get("total_fee_quote")),
        "entry_fee_quote": _coerce_float(exit_meta.get("entry_fee_quote")),
        "exit_fee_quote": _coerce_float(exit_meta.get("exit_fee_quote")),
        "entry_realized_slippage_bps": _coerce_float(exit_meta.get("entry_realized_slippage_bps")),
        "exit_expected_slippage_bps": _coerce_float(exit_meta.get("exit_expected_slippage_bps")),
        "pnl_basis": exit_meta.get("pnl_basis"),
        "entry_reason_code": row.get("entry_reason_code"),
        "close_reason_code": row.get("close_reason_code"),
        "close_mode": row.get("close_mode"),
        "model_prob": _coerce_float(row.get("model_prob")),
        "selection_policy_mode": row.get("selection_policy_mode"),
        "trade_action": row.get("trade_action"),
        "expected_edge_bps": _coerce_float(row.get("expected_edge_bps")),
        "expected_downside_bps": _coerce_float(row.get("expected_downside_bps")),
        "expected_net_edge_bps": _coerce_float(row.get("expected_net_edge_bps")),
        "notional_multiplier": _coerce_float(row.get("notional_multiplier")),
        "entry_meta": _normalize_json_text(row.get("entry_meta_json")) or {},
        "exit_meta": exit_meta,
    }


def _summarize_kst_trade_day(rows: list[dict[str, Any]], *, now_ts_ms: int) -> dict[str, Any]:
    now_dt = datetime.fromtimestamp(now_ts_ms / 1000.0, tz=_KST)
    start_dt = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end_dt = start_dt + timedelta(days=1)
    start_ts_ms = int(start_dt.timestamp() * 1000)
    end_ts_ms = int(end_dt.timestamp() * 1000)
    summary = {
        "date_label": start_dt.strftime("%Y-%m-%d"),
        "timezone": "KST",
        "closed_count": 0,
        "open_count": 0,
        "pending_count": 0,
        "wins": 0,
        "losses": 0,
        "flats": 0,
        "win_rate_pct": None,
        "net_pnl_quote_total": 0.0,
        "gross_pnl_quote_total": 0.0,
        "fee_quote_total": 0.0,
    }
    for row in rows:
        item = _summarize_live_trade_journal(row)
        status = str(item.get("status") or "").strip().upper()
        entry_ts_ms = _coerce_int(item.get("entry_ts_ms"))
        exit_ts_ms = _coerce_int(item.get("exit_ts_ms"))
        if status == "CLOSED":
            if exit_ts_ms is None or exit_ts_ms < start_ts_ms or exit_ts_ms >= end_ts_ms:
                continue
            summary["closed_count"] += 1
            pnl = _coerce_float(item.get("realized_pnl_quote")) or 0.0
            gross = _coerce_float(item.get("gross_pnl_quote")) or 0.0
            fee = _coerce_float(item.get("total_fee_quote")) or 0.0
            summary["net_pnl_quote_total"] += pnl
            summary["gross_pnl_quote_total"] += gross
            summary["fee_quote_total"] += fee
            if pnl > 0.0:
                summary["wins"] += 1
            elif pnl < 0.0:
                summary["losses"] += 1
            else:
                summary["flats"] += 1
        elif status == "OPEN":
            if entry_ts_ms is not None and start_ts_ms <= entry_ts_ms < end_ts_ms:
                summary["open_count"] += 1
        elif status == "PENDING_ENTRY":
            if entry_ts_ms is not None and start_ts_ms <= entry_ts_ms < end_ts_ms:
                summary["pending_count"] += 1
    closed_count = int(summary["closed_count"])
    if closed_count > 0:
        summary["win_rate_pct"] = (float(summary["wins"]) / float(closed_count)) * 100.0
    return summary


def _load_live_db_summary(db_path: Path, label: str, project_root: Path) -> dict[str, Any]:
    if not db_path.exists():
        return {"label": label, "db_path": str(db_path), "exists": False}
    try:
        conn = _open_ro_sqlite(db_path)
    except sqlite3.Error as exc:
        return {"label": label, "db_path": str(db_path), "exists": True, "error": str(exc)}
    try:
        tables = {row["name"] for row in _query_all(conn, "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")}
        orders = _query_all(conn, "SELECT * FROM orders ORDER BY updated_ts DESC LIMIT 12") if "orders" in tables else []
        intents = _query_all(conn, "SELECT * FROM intents ORDER BY ts_ms DESC, intent_id DESC LIMIT 12") if "intents" in tables else []
        positions = _query_all(conn, "SELECT * FROM positions ORDER BY market") if "positions" in tables else []
        risk_plans = _query_all(conn, "SELECT * FROM risk_plans ORDER BY updated_ts DESC LIMIT 12") if "risk_plans" in tables else []
        trade_journal = (
            _query_all(
                conn,
                "SELECT * FROM trade_journal ORDER BY COALESCE(exit_ts_ms, entry_filled_ts_ms, entry_submitted_ts_ms, updated_ts) DESC",
            )
            if "trade_journal" in tables
            else []
        )
        breaker_states = _query_all(conn, "SELECT * FROM breaker_states ORDER BY updated_ts DESC") if "breaker_states" in tables else []
        source_intent_lookup: dict[str, dict[str, Any]] = {}
        if "intents" in tables and risk_plans:
            source_ids = [str(row.get("source_intent_id") or "").strip() for row in risk_plans]
            source_ids = [value for value in source_ids if value]
            if source_ids:
                placeholders = ", ".join("?" for _ in source_ids)
                source_rows = _query_all(
                    conn,
                    f"SELECT intent_id, ts_ms FROM intents WHERE intent_id IN ({placeholders})",
                    tuple(source_ids),
                )
                source_intent_lookup = {
                    str(row.get("intent_id")): row
                    for row in source_rows
                    if row.get("intent_id")
                }
        checkpoints: dict[str, Any] = {}
        if "checkpoints" in tables:
            for name in ("live_runtime_health", "live_rollout_status", "live_rollout_contract", "last_resume"):
                row = _query_one(conn, "SELECT * FROM checkpoints WHERE name = ?", (name,))
                if row:
                    checkpoints[name] = _normalize_json_text(row.get("payload_json"))
        open_order_rows: list[dict[str, Any]] = []
        for row in orders:
            local_state = str(row.get("local_state") or "").strip()
            raw_state = str(row.get("state") or "").strip()
            if local_state:
                is_open = is_open_local_state(local_state)
            else:
                normalized = normalize_order_state(raw_state, volume_req=row.get("volume_req"), volume_filled=row.get("volume_filled"))
                is_open = is_open_local_state(normalized.local_state)
            if is_open:
                open_order_rows.append(row)
        active_risk_plans = [row for row in risk_plans if str(row.get("state") or "").upper() in {"ACTIVE", "TRIGGERED", "EXITING"}]
        active_breakers = [row for row in breaker_states if bool(row.get("active"))]
        now_ts_ms = int(time.time() * 1000)
        active_risk_plan_payloads: list[dict[str, Any]] = []
        for row in active_risk_plans[:8]:
            payload = _summarize_live_risk_plan(row)
            source_intent = source_intent_lookup.get(str(payload.get("source_intent_id") or ""))
            source_ts_ms = _coerce_int((source_intent or {}).get("ts_ms"))
            timeout_ts_ms = _coerce_int(payload.get("timeout_ts_ms"))
            if source_ts_ms is not None:
                payload["source_intent_ts_ms"] = source_ts_ms
            if source_ts_ms is not None and timeout_ts_ms is not None and timeout_ts_ms >= source_ts_ms:
                total_min = max(0, int(round((timeout_ts_ms - source_ts_ms) / 60000)))
                elapsed_min = max(0, int(round((now_ts_ms - source_ts_ms) / 60000)))
                remaining_min = max(0, int(round((timeout_ts_ms - now_ts_ms) / 60000)))
                payload["hold_total_minutes"] = total_min
                payload["hold_elapsed_minutes"] = elapsed_min
                payload["hold_remaining_minutes"] = remaining_min
            active_risk_plan_payloads.append(payload)
        runtime_health = checkpoints.get("live_runtime_health") or {}
        runtime_run_dir = _resolve_model_run_dir(project_root, runtime_health.get("live_runtime_model_run_id"))
        runtime_artifacts = _collect_recent_model_artifacts(project_root, str(runtime_run_dir)) if runtime_run_dir else {}
        return {
            "label": label,
            "db_path": str(db_path),
            "exists": True,
            "positions_count": len(positions),
            "open_orders_count": len(open_order_rows),
            "intents_count": len(intents),
            "active_risk_plans_count": len(active_risk_plans),
            "breaker_active": len(active_breakers) > 0,
            "positions": [_summarize_live_position(row) for row in positions[:8]],
            "open_orders": [_summarize_live_order(row) for row in open_order_rows[:8]],
            "recent_intents": [_summarize_live_intent(row) for row in intents[:8]],
            "recent_trades": [_summarize_live_trade_journal(row) for row in trade_journal[:8]],
            "today_trade_summary": _summarize_kst_trade_day(trade_journal, now_ts_ms=now_ts_ms),
            "active_risk_plans": active_risk_plan_payloads,
            "active_breakers": [
                {
                    **row,
                    "reason_codes": _normalize_json_text(row.get("reason_codes_json")) or [],
                    "details": _normalize_json_text(row.get("details_json")) or {},
                }
                for row in active_breakers[:8]
            ],
            "runtime_health": runtime_health,
            "runtime_artifacts": runtime_artifacts,
            "rollout_status": checkpoints.get("live_rollout_status") or {},
            "rollout_contract": checkpoints.get("live_rollout_contract") or {},
            "last_resume": checkpoints.get("last_resume") or {},
            "updated_at": _path_mtime_iso(db_path),
        }
    finally:
        conn.close()


def _resolve_live_db_candidates(project_root: Path) -> list[tuple[str, Path]]:
    candidates: list[tuple[str, Path]] = []
    main_db = project_root / "data" / "state" / "live" / "live_state.db"
    if main_db.exists():
        candidates.append(("메인 라이브", main_db))
    legacy_db = project_root / "data" / "state" / "live_state.db"
    if legacy_db.exists() and legacy_db != main_db:
        candidates.append(("레거시 라이브", legacy_db))
    candidate_db = project_root / "data" / "state" / "live_candidate" / "live_state.db"
    if candidate_db.exists():
        candidates.append(("후보 카나리아", candidate_db))
    return candidates


def _summarize_runtime_recommendations(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = normalize_runtime_recommendations_payload(payload)
    exit_payload = dict(_dig(normalized, "exit") or {})
    hold_grid_point = dict(exit_payload.get("grid_point") or {})
    risk_grid_point = dict(exit_payload.get("risk_grid_point") or {})
    trade_action = dict(normalized.get("trade_action") or {})
    trade_action_summary = {
        "status": trade_action.get("status"),
        "source": trade_action.get("source"),
        "risk_feature_name": trade_action.get("risk_feature_name"),
        "hold_bins_recommended": _dig(trade_action, "summary", "hold_bins_recommended"),
        "risk_bins_recommended": _dig(trade_action, "summary", "risk_bins_recommended"),
        "rows_total": trade_action.get("rows_total"),
        "windows_covered": trade_action.get("windows_covered"),
        "sample_bins": [],
    }
    for item in (trade_action.get("by_bin") or [])[:6]:
        if not isinstance(item, dict):
            continue
        trade_action_summary["sample_bins"].append(
            {
                "edge_bin": item.get("edge_bin"),
                "risk_bin": item.get("risk_bin"),
                "recommended_action": item.get("recommended_action"),
                "expected_edge_bps": (
                    (_coerce_float(item.get("expected_edge")) or 0.0) * 10_000.0
                    if _coerce_float(item.get("expected_edge")) is not None
                    else None
                ),
                "expected_downside_bps": (
                    (_coerce_float(item.get("expected_downside_deviation")) or 0.0) * 10_000.0
                    if _coerce_float(item.get("expected_downside_deviation")) is not None
                    else None
                ),
                "notional_multiplier": _coerce_float(item.get("recommended_notional_multiplier")),
                "sample_count": _coerce_int(item.get("sample_count")),
            }
        )
    return {
        "recommended_exit_mode": _dig(normalized, "exit", "recommended_exit_mode") or _dig(normalized, "exit", "mode"),
        "recommended_exit_mode_reason_code": _dig(normalized, "exit", "recommended_exit_mode_reason_code"),
        "recommended_hold_bars": _dig(normalized, "exit", "recommended_hold_bars"),
        "hold_objective_score": _dig(normalized, "exit", "objective_score"),
        "risk_objective_score": _dig(normalized, "exit", "risk_objective_score"),
        "hold_grid_point": hold_grid_point,
        "risk_grid_point": risk_grid_point,
        "recommended_risk_scaling_mode": _dig(normalized, "exit", "recommended_risk_scaling_mode"),
        "recommended_risk_vol_feature": _dig(normalized, "exit", "recommended_risk_vol_feature"),
        "recommended_tp_vol_multiplier": _dig(normalized, "exit", "recommended_tp_vol_multiplier"),
        "recommended_sl_vol_multiplier": _dig(normalized, "exit", "recommended_sl_vol_multiplier"),
        "recommended_trailing_vol_multiplier": _dig(normalized, "exit", "recommended_trailing_vol_multiplier"),
        "risk_multiplier": _dig(normalized, "risk", "risk_multiplier"),
        "recommendation_source": _dig(normalized, "exit", "recommended_exit_mode_source")
        or _dig(normalized, "exit", "recommendation_source"),
        "contract_status": _dig(normalized, "exit", "contract_status"),
        "contract_issues": list(_dig(normalized, "exit", "contract_issues") or []),
        "exit_mode_compare": _summarize_exit_mode_compare(exit_payload),
        "trade_action": trade_action_summary,
    }


def _summarize_selection_policy(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "mode": payload.get("mode"),
        "threshold_key": payload.get("threshold_key"),
        "rank_quantile": payload.get("rank_quantile"),
        "top_k": payload.get("top_k"),
        "min_names": payload.get("min_names"),
        "max_names": payload.get("max_names"),
        "fallback_mode": payload.get("fallback_mode"),
        "calibration_enabled": payload.get("calibration_enabled"),
    }


def _summarize_selection_calibration(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "method": payload.get("method"),
        "enabled": payload.get("enabled"),
        "sample_count": payload.get("sample_count"),
        "fold_count": payload.get("fold_count"),
        "score_range": payload.get("score_range"),
    }


def _summarize_search_budget(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "decision_mode": payload.get("decision_mode"),
        "project_used_gb": payload.get("project_used_gb"),
        "filesystem_used_gb": payload.get("filesystem_used_gb"),
        "booster_sweep_trials": payload.get("booster_sweep_trials"),
        "runtime_grid_mode": payload.get("runtime_grid_mode"),
        "reasons": payload.get("reasons"),
    }


def _summarize_factor_block_selection(payload: dict[str, Any]) -> dict[str, Any]:
    accepted = payload.get("accepted_blocks") if isinstance(payload.get("accepted_blocks"), list) else []
    rejected = payload.get("rejected_blocks") if isinstance(payload.get("rejected_blocks"), list) else []
    return {
        "mode": payload.get("mode"),
        "accepted_blocks": accepted[:8],
        "rejected_blocks": rejected[:8],
        "accepted_count": len(accepted),
        "rejected_count": len(rejected),
        "reason_codes": payload.get("reason_codes"),
    }


def _summarize_cpcv_lite(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "requested": payload.get("requested"),
        "executed": payload.get("executed"),
        "decision": payload.get("decision"),
        "fold_count": payload.get("fold_count"),
        "pbo": payload.get("pbo"),
        "dsr": payload.get("dsr"),
        "reason": payload.get("reason"),
    }


def _summarize_walk_forward(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "windows_run": payload.get("windows_run"),
        "selection_search_trial_count": payload.get("selection_search_trial_count"),
        "white_rc_comparable": payload.get("white_rc_comparable"),
        "white_rc_decision": payload.get("white_rc_decision"),
        "hansen_spa_comparable": payload.get("hansen_spa_comparable"),
        "hansen_spa_decision": payload.get("hansen_spa_decision"),
        "balanced_pareto_decision": _dig(payload, "promotion_decision", "balanced_pareto", "decision"),
        "execution_pareto_decision": _dig(payload, "promotion_decision", "execution_balanced_pareto", "decision"),
    }


def _collect_recent_model_artifacts(project_root: Path, candidate_run_dir: str | None) -> dict[str, Any]:
    if not candidate_run_dir:
        return {}
    run_dir = Path(candidate_run_dir)
    if not run_dir.is_absolute():
        run_dir = project_root / run_dir
    if not run_dir.exists():
        return {"run_dir": str(run_dir), "exists": False}
    files = {
        "runtime_recommendations": run_dir / "runtime_recommendations.json",
        "selection_policy": run_dir / "selection_policy.json",
        "selection_calibration": run_dir / "selection_calibration.json",
        "search_budget_decision": run_dir / "search_budget_decision.json",
        "factor_block_selection": run_dir / "factor_block_selection.json",
        "cpcv_lite_report": run_dir / "cpcv_lite_report.json",
        "walk_forward_report": run_dir / "walk_forward_report.json",
    }
    payload: dict[str, Any] = {"run_dir": str(run_dir), "exists": True}
    for key, path in files.items():
        raw_payload = _load_json(path)
        if key == "runtime_recommendations":
            payload[key] = _summarize_runtime_recommendations(raw_payload)
        elif key == "selection_policy":
            payload[key] = _summarize_selection_policy(raw_payload)
        elif key == "selection_calibration":
            payload[key] = _summarize_selection_calibration(raw_payload)
        elif key == "search_budget_decision":
            payload[key] = _summarize_search_budget(raw_payload)
        elif key == "factor_block_selection":
            payload[key] = _summarize_factor_block_selection(raw_payload)
        elif key == "cpcv_lite_report":
            payload[key] = _summarize_cpcv_lite(raw_payload)
        elif key == "walk_forward_report":
            payload[key] = _summarize_walk_forward(raw_payload)
        else:
            payload[key] = raw_payload
        payload[f"{key}_path"] = str(path) if path.exists() else None
    return payload


def build_dashboard_snapshot(project_root: Path) -> dict[str, Any]:
    project_root = project_root.resolve()
    acceptance_latest = project_root / "logs" / "model_v4_acceptance" / "latest.json"
    challenger_latest = project_root / "logs" / "model_v4_challenger" / "latest.json"
    challenger_state = project_root / "logs" / "model_v4_challenger" / "current_state.json"
    rank_shadow_latest = project_root / "logs" / "model_v4_rank_shadow_cycle" / "latest.json"
    rank_shadow_governance = project_root / "logs" / "model_v4_rank_shadow_cycle" / "latest_governance_action.json"
    live_rollout_latest = project_root / "logs" / "live_rollout" / "latest.json"
    ws_status = _load_ws_public_status(
        meta_dir=project_root / "data" / "raw_ws" / "upbit" / "_meta",
        raw_root=project_root / "data" / "raw_ws" / "upbit" / "public",
    )
    acceptance = _summarize_acceptance(acceptance_latest)
    return {
        "generated_at": _utc_now_iso(),
        "project_root": str(project_root),
        "system": _filesystem_usage(project_root),
        "services": {
            "paper_champion": _unit_snapshot("autobot-paper-v4.service"),
            "paper_challenger": _unit_snapshot("autobot-paper-v4-challenger.service"),
            "ws_public": _unit_snapshot("autobot-ws-public.service"),
            "live_main": _unit_snapshot("autobot-live-alpha.service"),
            "live_candidate": _unit_snapshot("autobot-live-alpha-candidate.service"),
            "spawn_service": _unit_snapshot("autobot-v4-challenger-spawn.service"),
            "promote_service": _unit_snapshot("autobot-v4-challenger-promote.service"),
            "rank_shadow_service": _unit_snapshot("autobot-v4-rank-shadow.service"),
            "spawn_timer": _unit_snapshot("autobot-v4-challenger-spawn.timer", timer=True),
            "promote_timer": _unit_snapshot("autobot-v4-challenger-promote.timer", timer=True),
            "rank_shadow_timer": _unit_snapshot("autobot-v4-rank-shadow.timer", timer=True),
        },
        "training": {
            "acceptance": acceptance,
            "candidate_artifacts": _collect_recent_model_artifacts(project_root, acceptance.get("candidate_run_dir")),
            "rank_shadow": _summarize_rank_shadow_cycle(rank_shadow_latest, rank_shadow_governance),
        },
        "challenger": _summarize_challenger(challenger_latest, challenger_state),
        "paper": {
            "recent_runs": _latest_paper_summaries(project_root),
        },
        "live": {
            "rollout_latest": _load_json(live_rollout_latest),
            "states": [_load_live_db_summary(path, label, project_root) for label, path in _resolve_live_db_candidates(project_root)],
        },
        "ws_public": ws_status,
    }


def _json_response(handler: BaseHTTPRequestHandler, payload: dict[str, Any], status: int = 200) -> None:
    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store, max-age=0")
    handler.end_headers()
    try:
        handler.wfile.write(body)
    except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):  # pragma: no cover - client closed early
        return


def _html_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False).replace("</", "<\\/")


def _load_dashboard_asset(name: str, *, binary: bool = False) -> str | bytes:
    path = _DASHBOARD_ASSETS_DIR / name
    return path.read_bytes() if binary else path.read_text(encoding="utf-8")


def _render_dashboard_index(initial_snapshot: dict[str, Any]) -> bytes:
    template = str(_load_dashboard_asset("index.html"))
    html = template.replace("__INITIAL_SNAPSHOT__", _html_json(initial_snapshot))
    return html.encode("utf-8")


INDEX_HTML = """<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Autobot 운영 대시보드</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;700&family=Noto+Sans+KR:wght@400;500;700&display=swap" rel="stylesheet">
  <style>
    :root { --bg:#f6f1e8; --ink:#13222f; --muted:#5f6d79; --card:rgba(255,255,255,.82); --line:rgba(19,34,47,.12); --good:#136f63; --warn:#c0711f; --bad:#b33a3a; --accent:#0d5c8c; --accent-soft:rgba(13,92,140,.12); --shadow:0 18px 40px rgba(19,34,47,.10); }
    * { box-sizing:border-box; } body { margin:0; color:var(--ink); font-family:"Noto Sans KR","Apple SD Gothic Neo",sans-serif; background:radial-gradient(circle at top left, rgba(13,92,140,.16), transparent 24rem),radial-gradient(circle at top right, rgba(19,111,99,.14), transparent 22rem),linear-gradient(180deg,#f8f4ed 0%,var(--bg) 100%); }
    .shell { max-width:1480px; margin:0 auto; padding:28px 22px 48px; } .hero { display:grid; grid-template-columns:1.25fr .75fr; gap:20px; margin-bottom:20px; } .hero-panel,.meta-panel,.card { background:var(--card); backdrop-filter:blur(14px); border:1px solid var(--line); border-radius:24px; box-shadow:var(--shadow); } .hero-panel { padding:28px; min-height:220px; position:relative; overflow:hidden; } .hero-panel::after { content:""; position:absolute; inset:auto -40px -60px auto; width:220px; height:220px; border-radius:999px; background:linear-gradient(180deg, rgba(13,92,140,.18), rgba(19,111,99,.05)); }
    h1,h2,h3 { margin:0; } h1 { font-family:"Space Grotesk","Noto Sans KR",sans-serif; font-size:2.15rem; line-height:1.05; letter-spacing:-.05em; max-width:12ch; margin-bottom:12px; } .sub { max-width:60ch; color:var(--muted); line-height:1.65; font-size:.98rem; } .meta-panel { padding:22px; display:grid; gap:14px; align-content:start; } .badge-row,.service-row { display:flex; flex-wrap:wrap; gap:8px; }
    .grid { display:grid; gap:18px; grid-template-columns:repeat(12,minmax(0,1fr)); } .card { padding:18px 18px 16px; grid-column:span 12; } .card.half { grid-column:span 6; } .kpis { display:grid; grid-template-columns:repeat(auto-fit,minmax(148px,1fr)); gap:12px; } .kpi { padding:14px; border-radius:18px; background:linear-gradient(180deg, rgba(255,255,255,.72), rgba(13,92,140,.03)); border:1px solid var(--line); } .kpi .label { font-size:.8rem; color:var(--muted); margin-bottom:6px; } .kpi .value { font-family:"Space Grotesk","Noto Sans KR",sans-serif; font-size:1.2rem; letter-spacing:-.04em; }
    .service-item { padding:10px 12px; border-radius:14px; border:1px solid var(--line); background:rgba(255,255,255,.58); min-width:220px; } .row { display:flex; justify-content:space-between; gap:12px; align-items:baseline; flex-wrap:wrap; } .list { display:grid; gap:10px; margin:0; padding:0; list-style:none; } .list li { border:1px solid var(--line); border-radius:16px; padding:12px 14px; background:rgba(255,255,255,.55); } .card-head { display:flex; align-items:center; justify-content:space-between; gap:12px; margin-bottom:14px; } .card-title { font-family:"Space Grotesk","Noto Sans KR",sans-serif; font-size:1.08rem; letter-spacing:-.03em; }
    .muted { color:var(--muted); } .mono { font-family:ui-monospace,SFMono-Regular,Consolas,monospace; } .badge,.pill,.refresh { border-radius:999px; } .badge { padding:8px 12px; font-size:.84rem; background:rgba(19,34,47,.06); border:1px solid rgba(19,34,47,.08); } .pill { display:inline-flex; align-items:center; gap:8px; padding:6px 10px; font-size:.82rem; font-weight:700; } .refresh { display:inline-flex; align-items:center; gap:10px; padding:10px 14px; border:1px solid rgba(13,92,140,.18); background:var(--accent-soft); color:var(--accent); font-weight:700; cursor:pointer; } .good { color:var(--good); background:rgba(19,111,99,.10); border-color:rgba(19,111,99,.18); } .warn { color:var(--warn); background:rgba(192,113,31,.12); border-color:rgba(192,113,31,.22); } .bad { color:var(--bad); background:rgba(179,58,58,.12); border-color:rgba(179,58,58,.22); } table { width:100%; border-collapse:collapse; font-size:.92rem; } th,td { text-align:left; padding:10px 8px; border-bottom:1px solid rgba(19,34,47,.08); vertical-align:top; } th { color:var(--muted); font-size:.8rem; text-transform:uppercase; letter-spacing:.04em; } .empty { padding:20px; text-align:center; color:var(--muted); border:1px dashed var(--line); border-radius:18px; } .error-box { margin-top:14px; padding:12px 14px; border-radius:16px; border:1px solid rgba(179,58,58,.24); background:rgba(179,58,58,.08); color:var(--bad); display:none; }
    @media (max-width:1180px) { .hero { grid-template-columns:1fr; } .card.half { grid-column:span 12; } }
  </style>
  <style>
    .section-nav {
      position:sticky;
      top:10px;
      z-index:10;
      display:flex;
      flex-wrap:wrap;
      gap:10px;
      padding:12px;
      margin:0 0 18px;
      border-radius:20px;
      border:1px solid rgba(19,34,47,.10);
      background:rgba(255,255,255,.82);
      backdrop-filter:blur(14px);
      box-shadow:0 12px 30px rgba(19,34,47,.08);
    }
    .section-btn {
      border:1px solid rgba(19,34,47,.08);
      background:rgba(19,34,47,.04);
      color:var(--ink);
      border-radius:999px;
      padding:10px 14px;
      font:inherit;
      cursor:pointer;
      transition:all .18s ease;
    }
    .section-btn.active {
      background:rgba(13,92,140,.14);
      border-color:rgba(13,92,140,.25);
      color:var(--accent);
      font-weight:700;
    }
    .card-head {
      display:grid;
      gap:4px;
      align-items:flex-start;
      margin-bottom:14px;
    }
    .card-head .muted {
      font-size:.82rem;
      line-height:1.55;
      max-width:none;
      text-align:left;
    }
    .card-title {
      font-size:1rem;
      line-height:1.25;
    }
    .service-row, #live-state-cards, #paper-table, #artifact-details {
      display:grid !important;
      grid-template-columns:repeat(auto-fit,minmax(280px,1fr));
      gap:14px;
    }
    #training-details, #challenger-details, #ws-details {
      display:grid;
      gap:14px;
    }
    .panel-hidden {
      display:none !important;
    }
    .service-item,
    .service-card,
    .state-card,
    .paper-card,
    .artifact-card,
    .note-card,
    .summary-box,
    .list-item,
    .detail-box {
      border:1px solid rgba(19,34,47,.10);
      background:rgba(255,255,255,.72);
      border-radius:18px;
      box-shadow:0 10px 24px rgba(19,34,47,.05);
    }
    .service-card,
    .state-card,
    .paper-card,
    .artifact-card,
    .summary-box,
    .note-card,
    .detail-box {
      padding:14px;
    }
    .summary-box strong,
    .note-card strong,
    .detail-box strong {
      display:block;
      margin-bottom:6px;
      font-size:.94rem;
    }
    .summary-box p,
    .note-card span,
    .detail-box p {
      margin:0;
      color:var(--muted);
      line-height:1.65;
      font-size:.92rem;
    }
    .mini-grid {
      display:grid;
      grid-template-columns:repeat(2,minmax(0,1fr));
      gap:10px;
      margin-top:12px;
    }
    .mini {
      border-radius:14px;
      padding:10px 11px;
      background:rgba(19,34,47,.04);
      min-height:64px;
    }
    .mini .label {
      color:var(--muted);
      font-size:.75rem;
      margin-bottom:4px;
    }
    .mini .value {
      font-size:.92rem;
      line-height:1.4;
      word-break:break-word;
    }
    .tag-row {
      display:flex;
      flex-wrap:wrap;
      gap:8px;
    }
    .detail-stack {
      display:grid;
      gap:12px;
      margin-top:12px;
    }
    .detail-title {
      font-size:.84rem;
      color:var(--muted);
      text-transform:uppercase;
      letter-spacing:.06em;
      margin:2px 0 0;
    }
    .detail-list {
      display:grid;
      gap:10px;
    }
    .detail-item {
      border-radius:14px;
      padding:11px 12px;
      background:rgba(19,34,47,.04);
      border:1px solid rgba(19,34,47,.06);
    }
    .detail-item .title-row {
      display:flex;
      justify-content:space-between;
      gap:10px;
      align-items:flex-start;
      margin-bottom:6px;
    }
    .detail-item strong {
      font-size:.92rem;
      margin:0;
    }
    .detail-item .desc,
    .detail-item .meta,
    .detail-item .path {
      color:var(--muted);
      line-height:1.55;
      font-size:.88rem;
      word-break:break-word;
    }
    .detail-item .meta-grid {
      display:grid;
      grid-template-columns:repeat(2,minmax(0,1fr));
      gap:8px;
      margin-top:8px;
    }
    .detail-item .meta-chip {
      background:rgba(255,255,255,.7);
      border:1px solid rgba(19,34,47,.06);
      border-radius:12px;
      padding:8px 9px;
    }
    .detail-item .meta-chip .k {
      color:var(--muted);
      font-size:.72rem;
      margin-bottom:3px;
    }
    .detail-item .meta-chip .v {
      font-size:.84rem;
      line-height:1.35;
      word-break:break-word;
    }
    .section-lead {
      color:var(--muted);
      line-height:1.65;
      font-size:.92rem;
      margin:0;
    }
    .section-summary-grid {
      display:grid;
      gap:14px;
      grid-template-columns:repeat(auto-fit,minmax(280px,1fr));
    }
    .list-item {
      padding:13px 14px;
      display:grid;
      gap:6px;
    }
    .list-item strong {
      font-size:.9rem;
    }
    .list-item span,
    .path {
      color:var(--muted);
      line-height:1.55;
      font-size:.88rem;
      word-break:break-all;
    }
    .muted-inline {
      color:var(--muted);
      font-size:.84rem;
    }
    .empty {
      grid-column:1 / -1;
      background:rgba(255,255,255,.55);
    }
    @media (max-width:900px) {
      .shell { padding:16px 12px 24px; }
      .section-nav {
        top:0;
        margin-bottom:14px;
        padding:10px;
      }
      .mini-grid,
      .detail-item .meta-grid {
        grid-template-columns:1fr;
      }
      .service-row, #live-state-cards, #paper-table, #artifact-details {
        grid-template-columns:1fr !important;
      }
      .section-summary-grid {
        grid-template-columns:1fr;
      }
      .card-head .muted {
        max-width:none;
        text-align:left;
      }
    }
  </style>
</head>
  <body>
  <div class="shell">
    <section class="hero">
      <div class="hero-panel">
        <h1>Autobot 운영 대시보드</h1>
        <p class="sub">학습, 검증, 챌린저, 페이퍼, 라이브, 실시간 수집 상태를 한 화면에서 읽는 읽기 전용 관제 화면입니다.</p>
        <div class="badge-row" id="hero-badges"></div>
      </div>
      <div class="meta-panel">
        <div class="row"><strong>마지막 갱신</strong><span id="generated-at" class="muted mono">-</span></div>
        <div class="row"><strong>프로젝트 경로</strong><span id="project-root" class="muted mono">-</span></div>
        <div class="row"><strong>저장공간</strong><span id="storage-summary" class="muted">-</span></div>
        <div class="row"><strong>자동 갱신</strong><span class="muted">10초</span></div>
        <button class="refresh" id="refresh-btn" type="button">지금 새로고침</button>
        <div id="fetch-error" class="error-box"></div>
      </div>
    </section>
    <nav class="section-nav" id="section-nav">
      <button class="section-btn active" type="button" data-section="all">전체</button>
      <button class="section-btn" type="button" data-section="overview">운영 개요</button>
      <button class="section-btn" type="button" data-section="training">학습 / 검증</button>
      <button class="section-btn" type="button" data-section="paper">페이퍼</button>
      <button class="section-btn" type="button" data-section="live">라이브 / 카나리아</button>
      <button class="section-btn" type="button" data-section="ingest">WS 수집</button>
    </nav>
    <section class="grid">
      <article class="card" data-sections="overview,training,live,ingest">
        <div class="card-head">
          <h2 class="card-title">서비스 상태</h2>
          <span class="muted">systemd 기준으로 현재 살아 있는 유닛, 타이머, 다음 실행 시각을 요약합니다.</span>
        </div>
        <div class="service-row" id="service-cards"></div>
      </article>
      <article class="card half" data-sections="training,overview">
        <div class="card-head">
          <h2 class="card-title">학습 / 어셉턴스</h2>
          <span class="muted">이번 후보가 왜 통과했는지, 왜 탈락했는지 사람 말로 풀어 적습니다.</span>
        </div>
        <div class="kpis" id="training-kpis"></div>
        <div id="training-details"></div>
      </article>
      <article class="card half" data-sections="training,overview">
        <div class="card-head">
          <h2 class="card-title">챌린저 루프</h2>
          <span class="muted">spawn / promote 단계에서 챌린저가 실제로 생성됐는지와 직접 사유를 보여줍니다.</span>
        </div>
        <div class="kpis" id="challenger-kpis"></div>
        <div id="challenger-details"></div>
      </article>
      <article class="card half" data-sections="paper,overview">
        <div class="card-head">
          <h2 class="card-title">페이퍼 런</h2>
          <span class="muted">최근 챔피언 / 챌린저 페이퍼 런의 주문, 체결, 손익, 낙폭을 카드형으로 정리합니다.</span>
        </div>
        <div id="paper-table"></div>
      </article>
      <article class="card half" data-sections="ingest,overview">
        <div class="card-head">
          <h2 class="card-title">WS 수집 상태</h2>
          <span class="muted">공용 WS 수집기 연결, 최신 수신 시각, 누적 적재량을 읽기 쉽게 정리합니다.</span>
        </div>
        <div class="kpis" id="ws-kpis"></div>
        <div id="ws-details"></div>
      </article>
      <article class="card" data-sections="live,overview">
        <div class="card-head">
          <h2 class="card-title">라이브 / 카나리아</h2>
          <span class="muted">보유 종목, 미체결 주문, 최근 진입 의도, 활성 리스크 플랜을 메인 / 후보 카나리아별로 분리해 보여줍니다.</span>
        </div>
        <div class="service-row" id="live-state-cards"></div>
      </article>
      <article class="card half" data-sections="training">
        <div class="card-head">
          <h2 class="card-title">후보 산출물</h2>
          <span class="muted">selection policy, calibration, runtime recommendation, factor 선택 결과를 요약합니다.</span>
        </div>
        <div id="artifact-details"></div>
      </article>
      <article class="card half" data-sections="overview,training,live">
        <div class="card-head">
          <h2 class="card-title">운영 메모</h2>
          <span class="muted">거절 사유, rollout 제한, 참고 정보처럼 사람이 바로 읽어야 하는 문장을 모읍니다.</span>
        </div>
        <div id="notes-list" class="section-summary-grid"></div>
      </article>
    </section>
  </div>
  <script>
    const INITIAL_SNAPSHOT = __INITIAL_SNAPSHOT__;
    const fmtNumber=(v,d=2)=>v===null||v===undefined||Number.isNaN(Number(v))?"-":new Intl.NumberFormat("ko-KR",{maximumFractionDigits:d,minimumFractionDigits:0}).format(Number(v));
    const fmtMoney=(v)=>v===null||v===undefined?"-":`${fmtNumber(v,0)} KRW`;
    const fmtPct=(v)=>v===null||v===undefined?"-":`${fmtNumber(v,2)}%`;
    const HTML_ESC = {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'};
    const esc=(v)=>String(v??"").replace(/[&<>"]/g,(ch)=>HTML_ESC[ch]);
    const SERVICE_LABELS = {
      paper_champion: "챔피언 페이퍼",
      paper_challenger: "챌린저 페이퍼",
      ws_public: "공용 WS 수집",
      live_main: "메인 라이브",
      live_candidate: "후보 카나리아",
      spawn_service: "후보 생성 실행",
      promote_service: "승급 평가 실행",
      spawn_timer: "00:10 생성 타이머",
      promote_timer: "23:50 승급 타이머",
    };
    const REASON_TEXT = {
      BACKTEST_ACCEPTANCE_FAILED: "백테스트 승인 기준을 통과하지 못했습니다.",
      TRAINER_EVIDENCE_REQUIRED_FAILED: "학습 증거 기준이 챔피언 우위를 뒤집지 못했습니다.",
      PAPER_SOAK_SKIPPED: "내부 paper soak는 생략했고, 통과 시 실제 챌린저 페이퍼가 대신 검증합니다.",
      OFFLINE_NOT_CANDIDATE_EDGE: "오프라인 비교에서 후보 우위가 확정되지 않았습니다.",
      SPA_LIKE_NOT_CANDIDATE_EDGE: "공통 구간 SPA 유사 검정에서 후보 우위가 확인되지 않았습니다.",
      WHITE_RC_NOT_CANDIDATE_EDGE: "White Reality Check에서 후보 우위가 확인되지 않았습니다.",
      HANSEN_SPA_NOT_CANDIDATE_EDGE: "Hansen SPA에서 후보 우위가 확인되지 않았습니다.",
      EXECUTION_NOT_CANDIDATE_EDGE: "실행 비용까지 반영한 비교에서 챔피언이 더 안정적이었습니다.",
      DUPLICATE_CANDIDATE: "기존 챔피언과 사실상 같은 모델이라 새 챌린저로 올리지 않았습니다.",
      LIVE_BREAKER_ACTIVE: "라이브 브레이커가 활성이라 주문을 막고 있습니다.",
      MODEL_POINTER_DIVERGENCE: "라이브가 물고 있는 모델과 현재 챔피언 포인터가 어긋났습니다.",
      WS_PUBLIC_STALE: "공용 WS 수집 신선도가 기준보다 오래됐습니다.",
    };
    const DECISION_TEXT = {
      TRAINER_EVIDENCE_REQUIRED_FAIL: "학습 증거 기준 미달",
      UTILITY_TIE_BREAK_FAIL: "효용 기준 동률 깨기 실패",
      CHAMPION_PARETO_DOMINANCE: "챔피언이 위험·실행·손익 기준에서 더 우세했습니다.",
    };
    const POLICY_TEXT = {
      rank_effective_quantile: "순위 기반 자동 컷",
      raw_threshold: "고정 확률 컷",
      rank_topk_budgeted: "예산 반영 상위 종목 선택",
      hold: "보유 시간 기반 종료",
      risk: "TP/SL/트레일링 기반 종료",
      canary: "카나리아",
      shadow: "섀도우",
      live: "실거래",
    };
    const maybe = (value, fallback="-") => value===null||value===undefined||value==="" ? fallback : String(value);
    const shortPath = (value) => { const text=maybe(value,""); return !text ? "-" : (text.length>86 ? `...${text.slice(-83)}` : text); };
    const shortRun = (value) => { const text=maybe(value,""); return !text ? "-" : (text.length>24 ? `${text.slice(0,18)}...${text.slice(-4)}` : text); };
    const boolLabel = (value) => value===null||value===undefined ? "-" : (value ? "예" : "아니오");
    const translate = (value, mapping=REASON_TEXT) => mapping[String(value)] || String(value ?? "-");
    const latestTimestamp = (value) => {
      if (typeof value === "number") return value;
      if (!value || typeof value !== "object") return null;
      const numbers = Object.values(value).map((item)=>Number(item)).filter((item)=>Number.isFinite(item));
      return numbers.length ? Math.max(...numbers) : null;
    };
    const fmtDateTime = (value) => {
      if (!value) return "-";
      const parsed = typeof value === "number" ? new Date(value) : new Date(String(value));
      if (Number.isNaN(parsed.getTime())) return String(value);
      return parsed.toLocaleString("ko-KR",{year:"numeric",month:"2-digit",day:"2-digit",hour:"2-digit",minute:"2-digit",second:"2-digit"});
    };
    const fmtAgeSeconds = (tsMs) => {
      if (!Number.isFinite(tsMs)) return "-";
      const diffSec=Math.max(0,(Date.now()-tsMs)/1000);
      if (diffSec < 60) return `${fmtNumber(diffSec,1)}초 전`;
      if (diffSec < 3600) return `${fmtNumber(diffSec/60,1)}분 전`;
      return `${fmtNumber(diffSec/3600,1)}시간 전`;
    };
    const joinReasons = (items) => {
      const values=(items||[]).filter(Boolean);
      return values.length ? values.map((item)=>translate(item)).join(" · ") : "없음";
    };
    const noteCard = (label, value) => `<div class="note-card"><strong>${esc(label)}</strong><span>${esc(value ?? "-")}</span></div>`;
    const listItem = (label, value) => `<li class="list-item"><strong>${esc(label)}</strong><span>${esc(value ?? "-")}</span></li>`;
    const empty = (text) => `<div class="empty">${esc(text)}</div>`;
    const stateClass=(v)=>{const t=String(v||"").toLowerCase(); if(t.includes("active")||t.includes("running")||t==="true") return "good"; if(t.includes("inactive")||t.includes("waiting")||t.includes("dead")||t==="false") return "warn"; return "bad";};
    const pill=(label,value)=>`<span class="pill ${stateClass(value)}">${esc(label)}: ${esc(value??"-")}</span>`;
    const kpi=(label,value)=>`<div class="kpi"><div class="label">${esc(label)}</div><div class="value">${esc(value??"-")}</div></div>`;
    function renderServices(services){ const entries=Object.values(services||{}); document.getElementById("hero-badges").innerHTML=entries.slice(0,6).map((i)=>pill(i.description||i.unit,i.active_state)).join(""); document.getElementById("service-cards").innerHTML=entries.map((i)=>`<div class="service-item"><div class="row"><strong>${esc(i.description||i.unit)}</strong>${pill("상태",i.active_state)}</div><div class="muted mono" style="margin-top:8px">${esc(i.unit)}</div><div class="muted" style="margin-top:8px">하위 상태: ${esc(i.sub_state||"-")}</div><div class="muted">활성화: ${esc(i.unit_file_state||"-")}</div>${i.next_run_at?`<div class="muted">다음 실행: ${esc(i.next_run_at)}</div>`:""}</div>`).join(""); }
    function renderTraining(training){ const a=training.acceptance||{}; document.getElementById("training-kpis").innerHTML=[kpi("후보 run",a.candidate_run_id||"-"),kpi("전체 통과",a.overall_pass===null||a.overall_pass===undefined?"-":String(a.overall_pass)),kpi("백테스트",a.backtest_pass===null||a.backtest_pass===undefined?"-":String(a.backtest_pass)),kpi("Paper",a.paper_pass===null||a.paper_pass===undefined?"-":String(a.paper_pass))].join(""); const items=[]; if(a.decision_basis) items.push(`<li><strong>판정 기준</strong><div class="muted">${esc(a.decision_basis)}</div></li>`); if((a.reasons||[]).length) items.push(`<li><strong>거절/상태 코드</strong><div class="muted">${a.reasons.map(esc).join(", ")}</div></li>`); if((a.trainer_reasons||[]).length) items.push(`<li><strong>Trainer evidence</strong><div class="muted">${a.trainer_reasons.map(esc).join(", ")}</div></li>`); if((a.notes||[]).length) items.push(`<li><strong>메모</strong><div class="muted">${a.notes.map(esc).join(", ")}</div></li>`); items.push(`<li><strong>완료 시각</strong><div class="muted mono">${esc(a.completed_at||a.generated_at||"-")}</div></li>`); document.getElementById("training-details").innerHTML=`<ul class="list">${items.join("")}</ul>`; }
    function renderChallenger(ch){ document.getElementById("challenger-kpis").innerHTML=[kpi("후보 run",ch.candidate_run_id||"-"),kpi("시작 여부",ch.started===null||ch.started===undefined?"-":String(ch.started)),kpi("사유",ch.reason||"-"),kpi("Paper 모델 ref",ch.paper_model_ref||"-")].join(""); const items=[]; if((ch.acceptance_notes||[]).length) items.push(`<li><strong>acceptance notes</strong><div class="muted">${ch.acceptance_notes.map(esc).join(", ")}</div></li>`); if(ch.challenger_unit) items.push(`<li><strong>챌린저 유닛</strong><div class="muted mono">${esc(ch.challenger_unit)}</div></li>`); items.push(`<li><strong>리포트 경로</strong><div class="muted mono">${esc(ch.artifact_path||"-")}</div></li>`); document.getElementById("challenger-details").innerHTML=`<ul class="list">${items.join("")}</ul>`; }
    function renderPaper(paper){ const rows=paper.recent_runs||[]; if(!rows.length){ document.getElementById("paper-table").innerHTML=`<div class="empty">최근 paper summary가 없습니다.</div>`; return; } document.getElementById("paper-table").innerHTML=`<table><thead><tr><th>run</th><th>provider</th><th>주문</th><th>체결</th><th>실현손익</th><th>최대DD</th><th>업데이트</th></tr></thead><tbody>${rows.map((r)=>`<tr><td class="mono">${esc(r.run_id)}</td><td>${esc(`${r.feature_provider||"-"} / ${r.micro_provider||"-"}`)}</td><td>${esc(String(r.orders_submitted??"-"))}</td><td>${esc(String(r.orders_filled??"-"))}</td><td>${esc(fmtMoney(r.realized_pnl_quote))}</td><td>${esc(fmtPct(r.max_drawdown_pct))}</td><td class="mono">${esc(r.updated_at||"-")}</td></tr>`).join("")}</tbody></table>`; }
    function renderWsPublic(ws){ const health=ws.health_snapshot||{}; const latest=ws.runs_summary_latest||{}; document.getElementById("ws-kpis").innerHTML=[kpi("run",health.run_id||latest.run_id||"-"),kpi("연결 상태",String(Boolean(health.connected))),kpi("구독 종목",health.subscribed_markets_count??latest.subscribed_markets_count??"-"),kpi("최근 수신",JSON.stringify(health.last_rx_ts_ms||{}))].join(""); document.getElementById("ws-details").innerHTML=`<ul class="list"><li><strong>health snapshot</strong><div class="muted mono">${esc(JSON.stringify(health))}</div></li><li><strong>최근 요약</strong><div class="muted mono">${esc(JSON.stringify(latest))}</div></li></ul>`; }
    function renderLive(live){ const rollout=live.rollout_latest||{}; const states=live.states||[]; document.getElementById("live-state-cards").innerHTML=states.map((s)=>`<div class="service-item"><div class="row"><strong>${esc(s.label)}</strong>${pill("breaker",String(Boolean(s.breaker_active)))}</div><div class="muted mono" style="margin-top:8px">${esc(s.db_path)}</div><div class="muted" style="margin-top:8px">포지션 ${esc(String(s.positions_count??0))} / 오픈주문 ${esc(String(s.open_orders_count??0))} / intent ${esc(String(s.intents_count??0))}</div><div class="muted">활성 risk plan ${esc(String(s.active_risk_plans_count??0))}</div><div class="muted">rollout ${esc(String((s.rollout_status||{}).mode||"-"))} / 주문허용 ${esc(String((s.rollout_status||{}).order_emission_allowed??"-"))}</div><div class="muted">모델 ${esc(String((s.runtime_health||{}).live_runtime_model_run_id||"-"))}</div></div>`).join("") || `<div class="empty">라이브 상태 DB를 찾지 못했습니다.</div>`; const notes=[]; if(rollout.contract) notes.push(`<li><strong>latest rollout contract</strong><div class="muted mono">${esc(JSON.stringify(rollout.contract))}</div></li>`); if(rollout.status) notes.push(`<li><strong>latest rollout status</strong><div class="muted mono">${esc(JSON.stringify(rollout.status))}</div></li>`); document.getElementById("notes-list").innerHTML=notes.join("") || `<li><div class="muted">특기할 rollout 메모가 없습니다.</div></li>`; }
    function renderArtifacts(training){ const artifacts=training.candidate_artifacts||{}; const entries=[["runtime_recommendations",artifacts.runtime_recommendations],["selection_policy",artifacts.selection_policy],["selection_calibration",artifacts.selection_calibration],["search_budget_decision",artifacts.search_budget_decision],["factor_block_selection",artifacts.factor_block_selection],["cpcv_lite_report",artifacts.cpcv_lite_report]]; document.getElementById("artifact-details").innerHTML=`<ul class="list">${entries.map(([name,payload])=>`<li><strong>${esc(name)}</strong><div class="muted mono">${esc(payload&&Object.keys(payload).length?JSON.stringify(payload).slice(0,420):"없음")}</div></li>`).join("")}</ul>`; }
    function renderMeta(data){ document.getElementById("generated-at").textContent=data.generated_at||"-"; document.getElementById("project-root").textContent=data.project_root||"-"; const s=data.system||{}; const usedGb=(Number(s.project_used_bytes||0)/(1024**3)).toFixed(1); const totalGb=(Number(s.total_bytes||0)/(1024**3)).toFixed(1); const fsUsedGb=(Number(s.used_bytes||0)/(1024**3)).toFixed(1); document.getElementById("storage-summary").textContent=`프로젝트 ${usedGb} GB / 파일시스템 ${fsUsedGb} GB / 총 ${totalGb} GB`; }
    function setError(message){ const node=document.getElementById("fetch-error"); if(!message){ node.style.display="none"; node.textContent=""; return; } node.style.display="block"; node.textContent=message; }
    function renderAll(data){ renderMeta(data); renderServices(data.services||{}); renderTraining(data.training||{}); renderChallenger(data.challenger||{}); renderPaper(data.paper||{}); renderWsPublic(data.ws_public||{}); renderLive(data.live||{}); renderArtifacts(data.training||{}); }

    function renderServices(services){
      const entries = Object.entries(services || {});
      document.getElementById("hero-badges").innerHTML = entries
        .filter(([, item]) => item)
        .slice(0, 6)
        .map(([key, item]) => pill(SERVICE_LABELS[key] || item.description || key, item.active_state))
        .join("");
      document.getElementById("service-cards").innerHTML = entries.map(([key, item]) => {
        const label = SERVICE_LABELS[key] || item.description || key;
        const extra = item.next_run_at
          ? `<div class="mini"><div class="label">다음 실행</div><div class="value">${esc(item.next_run_at)}</div></div>`
          : `<div class="mini"><div class="label">메인 PID</div><div class="value mono">${esc(maybe(item.main_pid, "-"))}</div></div>`;
        return `<div class="service-card"><div class="row"><strong>${esc(label)}</strong>${pill("상태", item.active_state)}</div><div class="path mono" style="margin-top:8px">${esc(item.unit)}</div><div class="mini-grid"><div class="mini"><div class="label">세부 상태</div><div class="value">${esc(maybe(item.sub_state))}</div></div><div class="mini"><div class="label">활성화</div><div class="value">${esc(maybe(item.unit_file_state))}</div></div>${extra}<div class="mini"><div class="label">최근 시작</div><div class="value">${esc(fmtDateTime(item.started_at))}</div></div></div></div>`;
      }).join("") || empty("서비스 상태를 읽지 못했습니다.");
    }

    function renderTraining(training){
      const acceptance = training.acceptance || {};
      const verdict = acceptance.overall_pass === true ? "통과" : acceptance.overall_pass === false ? "탈락" : "대기";
      const summary = acceptance.overall_pass === false
        ? `이번 후보 ${shortRun(acceptance.candidate_run_id)}는 챔피언을 넘지 못해 어셉턴스에서 탈락했습니다.`
        : acceptance.overall_pass === true
          ? `이번 후보 ${shortRun(acceptance.candidate_run_id)}는 어셉턴스를 통과했습니다.`
          : "최근 후보가 아직 없거나 실행 중입니다.";
      document.getElementById("training-kpis").innerHTML = [
        kpi("후보 run", shortRun(acceptance.candidate_run_id)),
        kpi("최종 판정", verdict),
        kpi("백테스트 통과", boolLabel(acceptance.backtest_pass)),
        kpi("완료 시각", fmtDateTime(acceptance.completed_at)),
      ].join("");
      document.getElementById("training-details").innerHTML = `<div class="summary-box"><strong>이번 후보 해석</strong><p>${esc(summary)}</p><div class="tag-row" style="margin-top:12px">${acceptance.decision_basis ? pill("판정 기준", translate(acceptance.decision_basis, DECISION_TEXT)) : ""}${acceptance.model_family ? pill("모델 패밀리", acceptance.model_family, "warn") : ""}${acceptance.batch_date ? pill("배치 날짜", acceptance.batch_date, "warn") : ""}</div></div><ul class="list">${listItem("어셉턴스 사유", joinReasons(acceptance.reasons))}${listItem("학습 증거 세부 사유", joinReasons(acceptance.trainer_reasons))}${listItem("운영 메모", joinReasons(acceptance.notes))}${listItem("후보 경로", shortPath(acceptance.candidate_run_dir))}${listItem("기존 챔피언", shortRun(acceptance.champion_before_run_id))}${listItem("현재 챔피언", shortRun(acceptance.champion_after_run_id))}</ul>`;
    }

    function renderChallenger(challenger){
      const started = challenger.started === true ? "기동됨" : challenger.started === false ? "미기동" : "미생성";
      document.getElementById("challenger-kpis").innerHTML = [
        kpi("후보 run", shortRun(challenger.candidate_run_id)),
        kpi("챌린저 상태", started),
        kpi("직접 사유", translate(challenger.reason)),
        kpi("완료 시각", fmtDateTime(challenger.completed_at)),
      ].join("");
      document.getElementById("challenger-details").innerHTML = `<div class="summary-box"><strong>이번 챌린저 생성 결과</strong><p>${challenger.started ? "이번 후보는 챌린저 페이퍼 유닛으로 실제 기동됐습니다." : "이번 후보는 챌린저 페이퍼로 넘어가지 못했습니다. 아래 사유를 먼저 확인하면 됩니다."}</p><div class="tag-row" style="margin-top:12px">${challenger.reason ? pill("직접 사유", translate(challenger.reason)) : ""}${challenger.paper_model_ref ? pill("페이퍼 모델 ref", shortRun(challenger.paper_model_ref), "warn") : ""}</div></div><ul class="list">${listItem("acceptance 메모", joinReasons(challenger.acceptance_notes))}${listItem("챌린저 유닛", challenger.challenger_unit || "생성되지 않음")}${listItem("리포트 경로", shortPath(challenger.artifact_path))}</ul>`;
    }

    function renderPaper(paper){
      const rows = paper.recent_runs || [];
      document.getElementById("paper-table").innerHTML = rows.length ? rows.map((run) => `<div class="paper-card"><div class="row"><strong>${esc(shortRun(run.run_id))}</strong>${pill("워밍업", boolLabel(run.warmup_satisfied), run.warmup_satisfied ? "good" : "warn")}</div><div class="path" style="margin-top:8px">${esc(`${maybe(run.feature_provider)} / ${maybe(run.micro_provider)}`)}</div><div class="mini-grid"><div class="mini"><div class="label">주문 수</div><div class="value">${esc(maybe(run.orders_submitted, "0"))}</div></div><div class="mini"><div class="label">체결 수</div><div class="value">${esc(maybe(run.orders_filled, "0"))}</div></div><div class="mini"><div class="label">실현 손익</div><div class="value">${esc(fmtMoney(run.realized_pnl_quote))}</div></div><div class="mini"><div class="label">최대 낙폭</div><div class="value">${esc(fmtPct(run.max_drawdown_pct))}</div></div><div class="mini"><div class="label">체결률</div><div class="value">${esc(fmtPct(run.fill_rate === null || run.fill_rate === undefined ? null : Number(run.fill_rate) * 100))}</div></div><div class="mini"><div class="label">업데이트</div><div class="value">${esc(fmtDateTime(run.updated_at))}</div></div></div></div>`).join("") : empty("최근 페이퍼 런 요약이 아직 없습니다.");
    }

    function renderWsPublic(ws){
      const health = ws.health_snapshot || {};
      const latestRun = ws.runs_summary_latest || {};
      const lastRxMs = latestTimestamp(health.last_rx_ts_ms);
      document.getElementById("ws-kpis").innerHTML = [
        kpi("연결 상태", boolLabel(health.connected)),
        kpi("구독 종목 수", maybe(health.subscribed_markets_count, "-")),
        kpi("최근 수신", fmtAgeSeconds(lastRxMs)),
        kpi("현재 run", shortRun(health.run_id || latestRun.run_id)),
      ].join("");
      document.getElementById("ws-details").innerHTML = `<div class="summary-box"><strong>수집기 해석</strong><p>${health.connected ? "공용 WS 수집기는 현재 연결된 상태입니다." : "공용 WS 수집기가 현재 끊겨 있어 원천 데이터 신선도를 먼저 확인해야 합니다."}</p><div class="tag-row" style="margin-top:12px">${pill("연결", boolLabel(health.connected), health.connected ? "good" : "bad")}${health.fatal_reason ? pill("치명 사유", health.fatal_reason, "bad") : ""}${pill("재연결 횟수", maybe(health.reconnect_count, "0"), "warn")}</div></div><ul class="list">${listItem("누적 적재 행 수", `총 ${fmtNumber((health.written_rows || {}).total, 0)}행 · trade ${fmtNumber((health.written_rows || {}).trade, 0)}행 · orderbook ${fmtNumber((health.written_rows || {}).orderbook, 0)}행`)}${listItem("누락·드롭 행 수", `총 ${fmtNumber((health.dropped_rows || {}).total, 0)}행 · orderbook downsample ${fmtNumber((health.dropped_rows || {}).orderbook_downsample, 0)}행`)}${listItem("최근 수신 시각", fmtDateTime(lastRxMs))}${listItem("최근 runs 요약", `parts ${fmtNumber(latestRun.parts, 0)}개 · bytes ${fmtNumber(latestRun.bytes_total, 0)} · rows ${fmtNumber(latestRun.rows_total, 0)}`)}</ul>`;
    }
    function renderLive(live){
      const rollout = live.rollout_latest || {};
      const states = live.states || [];
      document.getElementById("live-state-cards").innerHTML = states.length ? states.map((state) => {
        const runtime = state.runtime_health || {};
        const rolloutStatus = state.rollout_status || {};
        const lastResume = state.last_resume || {};
        const openMarkets = []
          .concat((state.positions || []).map((item)=>item.market))
          .concat((state.open_orders || []).map((item)=>item.market))
          .filter(Boolean);
        return `<div class="state-card"><div class="row"><strong>${esc(state.label)}</strong>${pill("브레이커", boolLabel(state.breaker_active), state.breaker_active ? "bad" : "good")}</div><div class="path mono" style="margin-top:8px">${esc(shortPath(state.db_path))}</div><div class="mini-grid"><div class="mini"><div class="label">보유 포지션</div><div class="value">${esc(maybe(state.positions_count, "0"))}</div></div><div class="mini"><div class="label">오픈 주문</div><div class="value">${esc(maybe(state.open_orders_count, "0"))}</div></div><div class="mini"><div class="label">활성 risk plan</div><div class="value">${esc(maybe(state.active_risk_plans_count, "0"))}</div></div><div class="mini"><div class="label">최근 intent 수</div><div class="value">${esc(maybe(state.intents_count, "0"))}</div></div><div class="mini"><div class="label">현재 모델</div><div class="value mono">${esc(shortRun(runtime.live_runtime_model_run_id))}</div></div><div class="mini"><div class="label">챔피언 포인터</div><div class="value mono">${esc(shortRun(runtime.champion_pointer_run_id))}</div></div><div class="mini"><div class="label">rollout 모드</div><div class="value">${esc(translate(rolloutStatus.mode, POLICY_TEXT))}</div></div><div class="mini"><div class="label">주문 방출 허용</div><div class="value">${esc(boolLabel(rolloutStatus.order_emission_allowed))}</div></div></div><div class="tag-row" style="margin-top:12px">${runtime.ws_public_stale === true ? pill("WS 신선도", "오래됨", "bad") : runtime.ws_public_stale === false ? pill("WS 신선도", "정상", "good") : ""}${runtime.model_pointer_divergence === true ? pill("모델 포인터", "어긋남", "bad") : runtime.model_pointer_divergence === false ? pill("모델 포인터", "정상", "good") : ""}${openMarkets.length ? pill("열린 시장", openMarkets.join(", "), "warn") : pill("열린 시장", "없음", "good")}</div>${lastResume && Object.keys(lastResume).length ? `<div class="path" style="margin-top:12px">마지막 resume: ${esc(fmtDateTime(lastResume.generated_at || lastResume.checked_at || lastResume.completed_at))}</div>` : ""}</div>`;
      }).join("") : empty("라이브 상태 DB를 찾지 못했습니다.");

      const noteCards = [];
      if (rollout.contract && Object.keys(rollout.contract).length) {
        noteCards.push(noteCard("최근 rollout 계약", `${translate(rollout.contract.mode, POLICY_TEXT)} · armed ${boolLabel(rollout.contract.armed)} · 목표 유닛 ${rollout.contract.target_unit || "-"}`));
      }
      if (rollout.status && Object.keys(rollout.status).length) {
        noteCards.push(noteCard("최근 rollout 상태", `시작 허용 ${boolLabel(rollout.status.start_allowed)} · 주문 허용 ${boolLabel(rollout.status.order_emission_allowed)} · 브레이커 해제 ${boolLabel(rollout.status.breaker_clear)}`));
        if ((rollout.status.reason_codes || []).length) {
          noteCards.push(noteCard("최근 rollout 제한 사유", joinReasons(rollout.status.reason_codes)));
        }
      }
      const testOrder = rollout.status ? rollout.status.test_order : null;
      if (testOrder && Object.keys(testOrder).length) {
        noteCards.push(noteCard("테스트 주문", `${testOrder.market || "-"} ${testOrder.side || "-"} · 결과 ${boolLabel(testOrder.ok)} · 확인 시각 ${fmtDateTime(testOrder.checked_at_utc)}`));
      }
      document.getElementById("notes-list").innerHTML = noteCards.join("") || empty("특기할 rollout 메모가 아직 없습니다.");
    }
    function renderArtifacts(training){
      const artifacts = training.candidate_artifacts || {};
      const runtime = artifacts.runtime_recommendations || {};
      const policy = artifacts.selection_policy || {};
      const calibration = artifacts.selection_calibration || {};
      const budget = artifacts.search_budget_decision || {};
      const factor = artifacts.factor_block_selection || {};
      const cpcv = artifacts.cpcv_lite_report || {};
      const wf = artifacts.walk_forward_report || {};
      document.getElementById("artifact-details").innerHTML = `<div class="artifact-card"><strong>런타임 추천</strong><div class="mini-grid"><div class="mini"><div class="label">종료 모드</div><div class="value">${esc(translate(runtime.recommended_exit_mode, POLICY_TEXT))}</div></div><div class="mini"><div class="label">권장 보유 bar</div><div class="value">${esc(maybe(runtime.recommended_hold_bars))}</div></div><div class="mini"><div class="label">TP / SL / 추적</div><div class="value">${esc(`${fmtPct(runtime.tp_pct)} / ${fmtPct(runtime.sl_pct)} / ${fmtPct(runtime.trailing_pct)}`)}</div></div><div class="mini"><div class="label">산출 방식</div><div class="value">${esc(maybe(runtime.recommendation_source))}</div></div></div></div><div class="artifact-card"><strong>선택 정책</strong><div class="mini-grid"><div class="mini"><div class="label">모드</div><div class="value">${esc(translate(policy.mode, POLICY_TEXT))}</div></div><div class="mini"><div class="label">기준 키</div><div class="value">${esc(maybe(policy.threshold_key))}</div></div><div class="mini"><div class="label">상위 비율</div><div class="value">${esc(policy.rank_quantile === null || policy.rank_quantile === undefined ? "-" : fmtPct(Number(policy.rank_quantile) * 100))}</div></div><div class="mini"><div class="label">보정 사용</div><div class="value">${esc(boolLabel(policy.calibration_enabled))}</div></div></div></div><div class="artifact-card"><strong>점수 보정</strong><div class="mini-grid"><div class="mini"><div class="label">활성화</div><div class="value">${esc(boolLabel(calibration.enabled))}</div></div><div class="mini"><div class="label">방법</div><div class="value">${esc(maybe(calibration.method))}</div></div><div class="mini"><div class="label">샘플 수</div><div class="value">${esc(maybe(calibration.sample_count))}</div></div><div class="mini"><div class="label">폴드 수</div><div class="value">${esc(maybe(calibration.fold_count))}</div></div></div></div><div class="artifact-card"><strong>탐색 예산</strong><div class="mini-grid"><div class="mini"><div class="label">결정 모드</div><div class="value">${esc(maybe(budget.decision_mode))}</div></div><div class="mini"><div class="label">booster sweep</div><div class="value">${esc(maybe(budget.booster_sweep_trials))}</div></div><div class="mini"><div class="label">runtime grid</div><div class="value">${esc(maybe(budget.runtime_grid_mode))}</div></div><div class="mini"><div class="label">사유</div><div class="value">${esc(joinReasons(budget.reasons))}</div></div></div></div><div class="artifact-card"><strong>팩터 블록 선택</strong><div class="mini-grid"><div class="mini"><div class="label">허용 블록 수</div><div class="value">${esc(maybe(factor.accepted_count))}</div></div><div class="mini"><div class="label">제외 블록 수</div><div class="value">${esc(maybe(factor.rejected_count))}</div></div><div class="mini"><div class="label">허용 블록</div><div class="value">${esc((factor.accepted_blocks || []).join(", ") || "-")}</div></div><div class="mini"><div class="label">제외 블록</div><div class="value">${esc((factor.rejected_blocks || []).join(", ") || "-")}</div></div></div></div><div class="artifact-card"><strong>강건성 검증</strong><div class="mini-grid"><div class="mini"><div class="label">CPCV 요청</div><div class="value">${esc(boolLabel(cpcv.requested))}</div></div><div class="mini"><div class="label">White comparable</div><div class="value">${esc(boolLabel(wf.white_rc_comparable))}</div></div><div class="mini"><div class="label">Hansen comparable</div><div class="value">${esc(boolLabel(wf.hansen_spa_comparable))}</div></div><div class="mini"><div class="label">selection trial 수</div><div class="value">${esc(maybe(wf.selection_search_trial_count))}</div></div></div></div>`;
    }
    function renderMeta(data){
      document.getElementById("generated-at").textContent = fmtDateTime(data.generated_at);
      document.getElementById("project-root").textContent = data.project_root || "-";
      const s = data.system || {};
      const usedGb=(Number(s.project_used_bytes||0)/(1024**3)).toFixed(1);
      const totalGb=(Number(s.total_bytes||0)/(1024**3)).toFixed(1);
      const fsUsedGb=(Number(s.used_bytes||0)/(1024**3)).toFixed(1);
      document.getElementById("storage-summary").textContent = `프로젝트 ${usedGb} GB · 파일시스템 사용 ${fsUsedGb} GB / 전체 ${totalGb} GB`;
    }
    function renderAll(data){
      renderMeta(data);
      renderServices(data.services||{});
      renderTraining(data.training||{});
      renderChallenger(data.challenger||{});
      renderPaper(data.paper||{});
      renderWsPublic(data.ws_public||{});
      renderLive(data.live||{});
      renderArtifacts(data.training||{});
    }
    async function refresh(){ try { const response=await fetch("/api/snapshot",{cache:"no-store"}); if(!response.ok){ throw new Error(`snapshot 응답 실패 (${response.status})`); } const data=await response.json(); renderAll(data); setError(""); } catch (err) { setError(`실시간 새로고침 실패: ${err && err.message ? err.message : err}`); } }
    document.getElementById("refresh-btn").addEventListener("click",refresh); if (INITIAL_SNAPSHOT && typeof INITIAL_SNAPSHOT === "object") { try { renderAll(INITIAL_SNAPSHOT); } catch (err) { setError(`초기 렌더링 실패: ${err && err.message ? err.message : err}`); } } refresh(); setInterval(refresh,10000);
  </script>
  <script>
    (() => {
      const reasonText = {
        BACKTEST_ACCEPTANCE_FAILED: "백테스트 승급 기준을 통과하지 못했습니다.",
        TRAINER_EVIDENCE_REQUIRED_FAILED: "학습 증거 기준에서 후보 우위가 확정되지 않았습니다.",
        PAPER_SOAK_SKIPPED: "내부 paper soak는 생략했고, 통과 시 실제 챌린저 페이퍼가 대신 검증합니다.",
        OFFLINE_NOT_CANDIDATE_EDGE: "오프라인 비교에서 후보 우위가 확인되지 않았습니다.",
        SPA_LIKE_NOT_CANDIDATE_EDGE: "공통 구간 SPA 유사 검정에서 후보 우위가 확인되지 않았습니다.",
        WHITE_RC_NOT_CANDIDATE_EDGE: "White Reality Check에서 후보 우위가 확인되지 않았습니다.",
        HANSEN_SPA_NOT_CANDIDATE_EDGE: "Hansen SPA에서 후보 우위가 확인되지 않았습니다.",
        EXECUTION_NOT_CANDIDATE_EDGE: "실행 비용을 반영한 비교에서 챔피언이 더 안정적이었습니다.",
        DUPLICATE_CANDIDATE: "기존 챔피언과 사실상 같은 모델이라 새 챌린저로 올리지 않았습니다.",
        LIVE_BREAKER_ACTIVE: "라이브 브레이커가 활성이라 새 주문을 막고 있습니다.",
        MODEL_POINTER_DIVERGENCE: "라이브가 물고 있는 모델과 현재 챔피언 포인터가 어긋났습니다.",
        WS_PUBLIC_STALE: "공용 WS 수집 신선도가 기준보다 오래됐습니다.",
        UNKNOWN_POSITIONS_DETECTED: "거래소 포지션과 로컬 상태가 어긋나 브레이커가 동작했습니다.",
        SMALL_ACCOUNT_CANARY_MULTIPLE_ACTIVE_MARKETS: "카나리아 단일 슬롯 제한을 넘는 시장이 감지됐습니다.",
        EXTERNAL_OPEN_ORDERS_DETECTED: "봇이 만들지 않은 외부 미체결 주문이 감지됐습니다.",
        LOCAL_POSITION_MISSING_ON_EXCHANGE: "로컬 포지션은 있는데 거래소 잔고가 사라졌습니다.",
        SKIPPED_SINGLE_SLOT_ACTIVE_ORDER: "이미 열린 주문이 있어 단일 슬롯 규칙에 따라 새 진입을 막았습니다.",
      };
      const decisionText = {
        TRAINER_EVIDENCE_REQUIRED_FAIL: "학습 증거 기준 미달",
        UTILITY_TIE_BREAK_FAIL: "효용 기준 동률 깨기 실패",
        CHAMPION_PARETO_DOMINANCE: "챔피언이 위험·실행·손익 기준에서 더 우세했습니다.",
      };
      const policyText = {
        rank_effective_quantile: "순위 기반 자동 컷",
        raw_threshold: "고정 확률 컷",
        rank_topk_budgeted: "예산 반영 상위 종목 선택",
        hold: "보유 시간 기반 종료",
        risk: "TP/SL/트레일링 기반 종료",
        canary: "카나리아",
        shadow: "섀도우",
        live: "실거래",
        MODEL_ALPHA_ENTRY_V1: "모델 알파 진입",
        MODEL_ALPHA_EXIT_V1: "모델 알파 종료",
        bid: "매수",
        ask: "매도",
        wait: "거래소 대기",
        done: "완료",
        cancel: "취소",
        OPEN: "로컬 오픈",
        ACTIVE: "활성",
        EXITING: "청산 진행",
        CLOSED: "종료",
        limit: "지정가",
        price: "지정가",
        market: "시장가",
      };
      const sectionButtons = () => [...document.querySelectorAll("#section-nav .section-btn")];
      const cards = () => [...document.querySelectorAll("[data-sections]")];
      const maybe = (value, fallback = "-") => value === null || value === undefined || value === "" ? fallback : String(value);
      const boolLabel = (value) => value === null || value === undefined ? "-" : (value ? "예" : "아니오");
      const fmtNumberLocal = (value, digits = 2) => value === null || value === undefined || Number.isNaN(Number(value))
        ? "-"
        : new Intl.NumberFormat("ko-KR", { maximumFractionDigits: digits, minimumFractionDigits: 0 }).format(Number(value));
      const fmtMoneyLocal = (value) => value === null || value === undefined || Number.isNaN(Number(value)) ? "-" : `${fmtNumberLocal(value, 0)} KRW`;
      const fmtPctLocal = (value) => value === null || value === undefined || Number.isNaN(Number(value)) ? "-" : `${fmtNumberLocal(value, 2)}%`;
      const fmtBpsLocal = (value) => value === null || value === undefined || Number.isNaN(Number(value)) ? "-" : `${fmtNumberLocal(value, 2)} bps`;
      const shortRunLocal = (value) => {
        const text = maybe(value, "");
        return !text ? "-" : (text.length > 24 ? `${text.slice(0, 18)}...${text.slice(-4)}` : text);
      };
      const shortPathLocal = (value) => {
        const text = maybe(value, "");
        return !text ? "-" : (text.length > 86 ? `...${text.slice(-83)}` : text);
      };
      const translateLocal = (value, mapping = reasonText) => mapping[String(value)] || String(value ?? "-");
      const uniqueLocal = (items) => [...new Set((items || []).filter(Boolean))];
      const joinReasonsLocal = (items) => {
        const values = (items || []).filter(Boolean);
        return values.length ? values.map((item) => translateLocal(item)).join(" · ") : "없음";
      };
      const latestTimestampLocal = (value) => {
        if (typeof value === "number") return value;
        if (!value || typeof value !== "object") return null;
        const numbers = Object.values(value).map((item) => Number(item)).filter((item) => Number.isFinite(item));
        return numbers.length ? Math.max(...numbers) : null;
      };
      const coerceTsLocal = (value) => {
        const numeric = Number(value);
        if (!Number.isFinite(numeric)) return null;
        return numeric < 1e12 ? numeric * 1000 : numeric;
      };
      const fmtDateTimeLocal = (value) => {
        if (!value) return "-";
        if (typeof value === "number" || /^[0-9]+$/.test(String(value))) {
          const ts = coerceTsLocal(value);
          if (!ts) return String(value);
          return new Date(ts).toLocaleString("ko-KR", {
            year: "numeric",
            month: "2-digit",
            day: "2-digit",
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
          });
        }
        const parsed = new Date(String(value));
        return Number.isNaN(parsed.getTime()) ? String(value) : parsed.toLocaleString("ko-KR", {
          year: "numeric",
          month: "2-digit",
          day: "2-digit",
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit",
        });
      };
      const fmtAgeLocal = (tsMs) => {
        if (!Number.isFinite(tsMs)) return "-";
        const diffSec = Math.max(0, (Date.now() - tsMs) / 1000);
        if (diffSec < 60) return `${fmtNumberLocal(diffSec, 1)}초 전`;
        if (diffSec < 3600) return `${fmtNumberLocal(diffSec / 60, 1)}분 전`;
        return `${fmtNumberLocal(diffSec / 3600, 1)}시간 전`;
      };
      const noteCardLocal = (label, value) => `<div class="note-card"><strong>${esc(label)}</strong><span>${esc(value ?? "-")}</span></div>`;
      const listItemLocal = (label, value) => `<div class="list-item"><strong>${esc(label)}</strong><span>${esc(value ?? "-")}</span></div>`;
      const emptyLocal = (text) => `<div class="empty">${esc(text)}</div>`;
      const metaChipLocal = (label, value) => `<div class="meta-chip"><div class="k">${esc(label)}</div><div class="v">${esc(value ?? "-")}</div></div>`;
      const detailItemLocal = ({ title, status, desc, path, meta = [] }) => `<div class="detail-item"><div class="title-row"><strong>${esc(title)}</strong>${status || ""}</div>${desc ? `<div class="desc">${esc(desc)}</div>` : ""}${path ? `<div class="path mono">${esc(path)}</div>` : ""}${meta.length ? `<div class="meta-grid">${meta.join("")}</div>` : ""}</div>`;
      const detailSectionLocal = (title, items, emptyText) => `<div class="detail-box"><strong>${esc(title)}</strong>${items.length ? `<div class="detail-list">${items.join("")}</div>` : emptyLocal(emptyText)}</div>`;
      const fmtFillRateLocal = (value) => value === null || value === undefined || Number.isNaN(Number(value)) ? "-" : fmtPctLocal(Number(value) * 100);
      const summarizeExitCandidateLocal = (runtime, mode) => {
        const payload = runtime && typeof runtime === "object" ? runtime : {};
        if (mode === "hold") {
          const holdGridPoint = payload.hold_grid_point || {};
          const holdBars = holdGridPoint.hold_bars ?? payload.recommended_hold_bars;
          if (holdBars === null || holdBars === undefined || holdBars === "") return "기록 없음";
          const score = payload.hold_objective_score;
          return score === null || score === undefined || Number.isNaN(Number(score))
            ? `hold ${maybe(holdBars)} bar`
            : `hold ${maybe(holdBars)} bar · score ${fmtNumberLocal(score, 3)}`;
        }
        const volFeature = payload.recommended_risk_vol_feature;
        const scalingMode = payload.recommended_risk_scaling_mode;
        if ((volFeature === null || volFeature === undefined || volFeature === "") && (scalingMode === null || scalingMode === undefined || scalingMode === "")) {
          return "기록 없음";
        }
        const score = payload.risk_objective_score;
        const pieces = [
          maybe(scalingMode),
          maybe(volFeature),
          `TP ${fmtNumberLocal(payload.recommended_tp_vol_multiplier, 2)}x`,
          `SL ${fmtNumberLocal(payload.recommended_sl_vol_multiplier, 2)}x`,
          `Trail ${fmtNumberLocal(payload.recommended_trailing_vol_multiplier, 2)}x`,
        ];
        if (!(score === null || score === undefined || Number.isNaN(Number(score)))) {
          pieces.push(`score ${fmtNumberLocal(score, 3)}`);
        }
        return pieces.join(" · ");
      };
      const summarizeExitMetricsLocal = (metrics) => {
        const payload = metrics && typeof metrics === "object" ? metrics : {};
        const parts = [
          `손익 ${fmtMoneyLocal(payload.realized_pnl_quote)}`,
          `체결률 ${fmtFillRateLocal(payload.fill_rate)}`,
          `최대DD ${fmtPctLocal(payload.max_drawdown_pct)}`,
          `슬리피지 ${fmtBpsLocal(payload.slippage_bps_mean)}`,
          `체결 ${maybe(payload.orders_filled, "0")}`,
        ];
        return parts.join(" · ");
      };
      const renderExitCompareCardLocal = (runtime, title) => {
        const payload = runtime && typeof runtime === "object" ? runtime : {};
        if (!Object.keys(payload).length) return "";
        const compare = payload.exit_mode_compare && typeof payload.exit_mode_compare === "object" ? payload.exit_mode_compare : {};
        const compareSummary = maybe(compare.summary_ko, "비교 기록이 없습니다.");
        return `<div class="artifact-card"><strong>${esc(title)}</strong><p class="section-lead" style="margin-top:10px">${esc(compareSummary)}</p><div class="tag-row" style="margin-top:12px">${payload.recommended_exit_mode_reason_code ? pill("판정 코드", payload.recommended_exit_mode_reason_code, "warn") : ""}${payload.recommendation_source ? pill("산출 방식", payload.recommendation_source, "warn") : ""}${compare.decision ? pill("비교 판정", String(compare.decision), compare.decision === "candidate_edge" ? "good" : "warn") : ""}</div><div class="mini-grid"><div class="mini"><div class="label">선택 모드</div><div class="value">${esc(translateLocal(payload.recommended_exit_mode, policyText))}</div></div><div class="mini"><div class="label">hold 후보</div><div class="value">${esc(summarizeExitCandidateLocal(payload, "hold"))}</div></div><div class="mini"><div class="label">risk 후보</div><div class="value">${esc(summarizeExitCandidateLocal(payload, "risk"))}</div></div><div class="mini"><div class="label">hold 성과</div><div class="value">${esc(summarizeExitMetricsLocal(compare.hold || {}))}</div></div><div class="mini"><div class="label">risk 성과</div><div class="value">${esc(summarizeExitMetricsLocal(compare.risk || {}))}</div></div></div></div>`;
      };

      window.setupSectionNav = function setupSectionNav() {
        const active = window.__dashboardActiveSection || "all";
        sectionButtons().forEach((button) => button.classList.toggle("active", button.dataset.section === active));
        cards().forEach((card) => {
          const sections = (card.dataset.sections || "").split(",").map((item) => item.trim()).filter(Boolean);
          const visible = active === "all" || sections.includes(active);
          card.classList.toggle("panel-hidden", !visible);
        });
      };

      if (!window.__dashboardNavBound) {
        document.getElementById("section-nav").addEventListener("click", (event) => {
          const button = event.target.closest(".section-btn");
          if (!button) return;
          window.__dashboardActiveSection = button.dataset.section || "all";
          window.setupSectionNav();
        });
        window.__dashboardNavBound = true;
      }

      window.renderServices = function renderServices(services) {
        const entries = Object.entries(services || {}).filter(([, item]) => item);
        document.getElementById("service-cards").innerHTML = entries.length ? entries.map(([key, item]) => {
          const label = SERVICE_LABELS[key] || item.description || key;
          const summary = item.next_run_at
            ? `다음 실행은 ${fmtDateTimeLocal(item.next_run_at)} 입니다.`
            : `${label} 서비스는 현재 ${maybe(item.active_state)} 상태입니다.`;
          return `<div class="service-card"><div class="row"><strong>${esc(label)}</strong>${pill("상태", item.active_state)}</div><p class="section-lead" style="margin-top:10px">${esc(summary)}</p><div class="mini-grid"><div class="mini"><div class="label">세부 상태</div><div class="value">${esc(maybe(item.sub_state))}</div></div><div class="mini"><div class="label">활성화</div><div class="value">${esc(maybe(item.unit_file_state))}</div></div><div class="mini"><div class="label">유닛 이름</div><div class="value mono">${esc(item.unit)}</div></div><div class="mini"><div class="label">${item.next_run_at ? "다음 실행" : "메인 PID"}</div><div class="value">${esc(item.next_run_at ? fmtDateTimeLocal(item.next_run_at) : maybe(item.main_pid, "-"))}</div></div></div></div>`;
        }).join("") : emptyLocal("서비스 상태를 읽지 못했습니다.");
        document.getElementById("hero-badges").innerHTML = entries.slice(0, 6).map(([key, item]) => pill(SERVICE_LABELS[key] || item.description || key, item.active_state)).join("");
      };

      window.renderTraining = function renderTraining(training) {
        const acceptance = training.acceptance || {};
        const verdict = acceptance.overall_pass === true ? "통과" : acceptance.overall_pass === false ? "탈락" : "대기";
        const summary = acceptance.overall_pass === false
          ? `이번 후보 ${shortRunLocal(acceptance.candidate_run_id)}는 챔피언을 넘지 못해 어셉턴스에서 탈락했습니다.`
          : acceptance.overall_pass === true
            ? `이번 후보 ${shortRunLocal(acceptance.candidate_run_id)}는 어셉턴스를 통과했습니다.`
            : "최근 후보가 아직 없거나 실행 중입니다.";
        document.getElementById("training-kpis").innerHTML = [
          kpi("후보 run", shortRunLocal(acceptance.candidate_run_id)),
          kpi("최종 판정", verdict),
          kpi("백테스트 통과", boolLabel(acceptance.backtest_pass)),
          kpi("완료 시각", fmtDateTimeLocal(acceptance.completed_at || acceptance.generated_at)),
        ].join("");
        document.getElementById("training-details").innerHTML = `<div class="summary-box"><strong>이번 후보 해석</strong><p>${esc(summary)}</p><div class="tag-row" style="margin-top:12px">${acceptance.decision_basis ? pill("판정 기준", translateLocal(acceptance.decision_basis, decisionText), "warn") : ""}${acceptance.model_family ? pill("모델 패밀리", acceptance.model_family, "warn") : ""}${acceptance.batch_date ? pill("배치 날짜", acceptance.batch_date, "warn") : ""}</div></div><div class="section-summary-grid">${listItemLocal("어셉턴스 직접 사유", joinReasonsLocal(acceptance.reasons))}${listItemLocal("학습 증거 세부 사유", joinReasonsLocal(acceptance.trainer_reasons))}${listItemLocal("운영 메모", joinReasonsLocal(acceptance.notes))}${listItemLocal("후보 경로", shortPathLocal(acceptance.candidate_run_dir))}${listItemLocal("기존 챔피언", shortRunLocal(acceptance.champion_before_run_id))}${listItemLocal("현재 챔피언", shortRunLocal(acceptance.champion_after_run_id))}</div>`;
      };

      window.renderChallenger = function renderChallenger(challenger) {
        const started = challenger.started === true ? "기동됨" : challenger.started === false ? "미기동" : "미생성";
        document.getElementById("challenger-kpis").innerHTML = [
          kpi("후보 run", shortRunLocal(challenger.candidate_run_id)),
          kpi("챌린저 상태", started),
          kpi("직접 사유", translateLocal(challenger.reason)),
          kpi("완료 시각", fmtDateTimeLocal(challenger.completed_at || challenger.generated_at)),
        ].join("");
        document.getElementById("challenger-details").innerHTML = `<div class="summary-box"><strong>이번 챌린저 생성 결과</strong><p>${challenger.started ? "이번 후보는 실제 챌린저 페이퍼로 올라가 현재 챔피언과 경쟁 중입니다." : "이번 후보는 챌린저 페이퍼로 넘어가지 못했습니다. 아래 직접 사유를 먼저 확인하면 됩니다."}</p><div class="tag-row" style="margin-top:12px">${challenger.reason ? pill("직접 사유", translateLocal(challenger.reason), challenger.started ? "good" : "bad") : ""}${challenger.paper_model_ref ? pill("페이퍼 모델 ref", shortRunLocal(challenger.paper_model_ref), "warn") : ""}</div></div><div class="section-summary-grid">${listItemLocal("acceptance 메모", joinReasonsLocal(challenger.acceptance_notes))}${listItemLocal("챌린저 유닛", challenger.challenger_unit || "생성되지 않음")}${listItemLocal("사용한 피처 공급자", challenger.paper_feature_provider || "-")}${listItemLocal("리포트 경로", shortPathLocal(challenger.artifact_path))}</div>`;
      };

      window.renderPaper = function renderPaper(paper) {
        const rows = paper.recent_runs || [];
        document.getElementById("paper-table").innerHTML = rows.length ? rows.map((run) => {
          const fillPct = run.fill_rate === null || run.fill_rate === undefined ? "-" : fmtPctLocal(Number(run.fill_rate) * 100);
          const summary = `${maybe(run.feature_provider)} / ${maybe(run.micro_provider)} 조합으로 ${fmtNumberLocal(run.duration_sec, 0)}초 동안 돌았습니다.`;
          return `<div class="paper-card"><div class="row"><strong>${esc(shortRunLocal(run.run_id))}</strong>${pill("워밍업", boolLabel(run.warmup_satisfied), run.warmup_satisfied ? "good" : "warn")}</div><p class="section-lead" style="margin-top:10px">${esc(summary)}</p><div class="mini-grid"><div class="mini"><div class="label">제출 주문</div><div class="value">${esc(maybe(run.orders_submitted, "0"))}</div></div><div class="mini"><div class="label">체결 주문</div><div class="value">${esc(maybe(run.orders_filled, "0"))}</div></div><div class="mini"><div class="label">체결률</div><div class="value">${esc(fillPct)}</div></div><div class="mini"><div class="label">실현 손익</div><div class="value">${esc(fmtMoneyLocal(run.realized_pnl_quote))}</div></div><div class="mini"><div class="label">평가 손익</div><div class="value">${esc(fmtMoneyLocal(run.unrealized_pnl_quote))}</div></div><div class="mini"><div class="label">최대 낙폭</div><div class="value">${esc(fmtPctLocal(run.max_drawdown_pct))}</div></div></div><div class="path" style="margin-top:10px">마지막 갱신: ${esc(fmtDateTimeLocal(run.updated_at))}</div></div>`;
        }).join("") : emptyLocal("최근 페이퍼 런 요약이 아직 없습니다.");
      };

      window.renderWsPublic = function renderWsPublic(ws) {
        const health = ws.health_snapshot || {};
        const latestRun = ws.runs_summary_latest || {};
        const lastRxMs = latestTimestampLocal(health.last_rx_ts_ms);
        document.getElementById("ws-kpis").innerHTML = [
          kpi("연결 상태", boolLabel(health.connected)),
          kpi("구독 종목 수", maybe(health.subscribed_markets_count, "-")),
          kpi("최근 수신", fmtAgeLocal(lastRxMs)),
          kpi("현재 run", shortRunLocal(health.run_id || latestRun.run_id)),
        ].join("");
        document.getElementById("ws-details").innerHTML = `<div class="summary-box"><strong>수집기 해석</strong><p>${health.connected ? "공용 WS 수집기는 현재 연결된 상태이며, 라이브와 학습이 같은 데이터 플레인을 공유합니다." : "공용 WS 수집기가 현재 끊겨 있습니다. 원천 데이터 신선도를 먼저 확인해야 합니다."}</p><div class="tag-row" style="margin-top:12px">${pill("연결", boolLabel(health.connected), health.connected ? "good" : "bad")}${health.fatal_reason ? pill("치명 사유", health.fatal_reason, "bad") : ""}${pill("재연결 횟수", maybe(health.reconnect_count, "0"), "warn")}</div></div><div class="section-summary-grid">${listItemLocal("누적 적재 행 수", `총 ${fmtNumberLocal((health.written_rows || {}).total, 0)}행 · trade ${fmtNumberLocal((health.written_rows || {}).trade, 0)}행 · orderbook ${fmtNumberLocal((health.written_rows || {}).orderbook, 0)}행`)}${listItemLocal("누락·드롭 행 수", `총 ${fmtNumberLocal((health.dropped_rows || {}).total, 0)}행 · orderbook downsample ${fmtNumberLocal((health.dropped_rows || {}).orderbook_downsample, 0)}행`)}${listItemLocal("최근 수신 시각", fmtDateTimeLocal(lastRxMs))}${listItemLocal("최근 run 요약", `parts ${fmtNumberLocal(latestRun.parts, 0)}개 · bytes ${fmtNumberLocal(latestRun.bytes_total, 0)} · rows ${fmtNumberLocal(latestRun.rows_total, 0)}`)}</div>`;
      };

      const summarizePosition = (position) => {
        const qty = Number(position.base_amount || 0);
        const avg = Number(position.avg_entry_price || 0);
        return detailItemLocal({
          title: position.market || "-",
          status: pill("관리 대상", boolLabel(position.managed), position.managed ? "good" : "warn"),
          desc: `보유 수량 ${fmtNumberLocal(qty, 8)} · 평균 매수가 ${fmtMoneyLocal(avg)} · 평가 원금 약 ${fmtMoneyLocal(qty * avg)}`,
          meta: [metaChipLocal("최근 갱신", fmtDateTimeLocal(position.updated_ts))],
        });
      };
      const summarizeOrder = (order) => {
        const qty = Number(order.volume_req || 0);
        const price = Number(order.price || 0);
        const filled = Number(order.volume_filled || 0);
        return detailItemLocal({
          title: `${order.market || "-"} ${translateLocal(order.side, policyText)} ${translateLocal(order.ord_type, policyText)}`,
          status: pill("거래소 상태", translateLocal(order.raw_exchange_state, policyText), order.raw_exchange_state === "done" ? "good" : "warn"),
          desc: `요청 ${fmtNumberLocal(qty, 8)}개 · 지정가 ${fmtMoneyLocal(price)} · 주문금액 약 ${fmtMoneyLocal(qty * price)}`,
          path: shortRunLocal(order.uuid),
          meta: [
            metaChipLocal("로컬 상태", translateLocal(order.local_state, policyText)),
            metaChipLocal("체결 수량", fmtNumberLocal(filled, 8)),
            metaChipLocal("replace 횟수", maybe(order.replace_seq, "0")),
            metaChipLocal("intent", shortRunLocal(order.intent_id)),
            metaChipLocal("최근 갱신", fmtDateTimeLocal(order.updated_ts)),
          ],
        });
      };
      const summarizeRiskPlan = (plan) => {
        const qty = Number(plan.qty || 0);
        const entry = Number(plan.entry_price || 0);
        return detailItemLocal({
          title: `${plan.market || "-"} · ${translateLocal(plan.exit_mode, policyText)}`,
          status: pill("플랜 상태", translateLocal(plan.state, policyText), plan.state === "ACTIVE" ? "good" : "warn"),
          desc: `진입가 ${fmtMoneyLocal(entry)} · 수량 ${fmtNumberLocal(qty, 8)} · 진입금액 약 ${fmtMoneyLocal(entry * qty)}`,
          meta: [
            metaChipLocal("익절", plan.tp_enabled ? fmtPctLocal(Number(plan.tp_pct) * 100) : "미사용"),
            metaChipLocal("손절", plan.sl_enabled ? fmtPctLocal(Number(plan.sl_pct) * 100) : "미사용"),
            metaChipLocal("추적", plan.trailing_enabled ? fmtPctLocal(Number(plan.trail_pct) * 100) : "미사용"),
            metaChipLocal("타임아웃", fmtDateTimeLocal(plan.timeout_ts_ms)),
            metaChipLocal("source", plan.plan_source || "-"),
            metaChipLocal("intent", shortRunLocal(plan.source_intent_id)),
          ],
        });
      };
      const summarizeIntent = (intent) => detailItemLocal({
        title: `${intent.market || "-"} ${translateLocal(intent.side, policyText)}`,
        status: pill("상태", translateLocal(intent.status, policyText), intent.status === "ACCEPTED" || intent.status === "SUBMITTED" ? "good" : "warn"),
        desc: `${translateLocal(intent.reason_code, policyText)} · 진입 금액 ${fmtMoneyLocal(intent.notional_quote)} · 모델 점수 ${fmtNumberLocal(intent.prob, 4)}`,
        meta: [
          metaChipLocal("선택 정책", translateLocal(intent.selection_policy_mode, policyText)),
          metaChipLocal("예상 순엣지", fmtBpsLocal(intent.expected_net_edge_bps)),
          metaChipLocal("예상 비용", fmtBpsLocal(intent.estimated_total_cost_bps)),
          metaChipLocal("건너뜀 사유", translateLocal(intent.skip_reason)),
          metaChipLocal("생성 시각", fmtDateTimeLocal(intent.ts_ms)),
        ],
      });

      window.renderLive = function renderLive(live) {
        const states = live.states || [];
        document.getElementById("live-state-cards").innerHTML = states.length ? states.map((state) => {
          const runtime = state.runtime_health || {};
          const rolloutStatus = state.rollout_status || {};
          const lastResume = state.last_resume || {};
          const runtimeArtifacts = state.runtime_artifacts || {};
          const liveRuntime = runtimeArtifacts.runtime_recommendations || {};
          const positions = (state.positions || []).map(summarizePosition);
          const openOrders = (state.open_orders || []).map(summarizeOrder);
          const plans = (state.active_risk_plans || []).map(summarizeRiskPlan);
          const intents = (state.recent_intents || []).map(summarizeIntent);
          const activeBreakers = uniqueLocal((state.active_breakers || []).map((item) => item.reason || item.code || item.name));
          const liveRuntimeCard = renderExitCompareCardLocal(liveRuntime, "현재 런타임 종료 모드 경쟁");
          return `<div class="state-card"><div class="row"><strong>${esc(state.label)}</strong>${pill("브레이커", boolLabel(state.breaker_active), state.breaker_active ? "bad" : "good")}</div><p class="section-lead" style="margin-top:10px">${esc(`${state.label}는 현재 ${translateLocal(rolloutStatus.mode, policyText)} 모드이며, 챔피언 포인터 ${shortRunLocal(runtime.champion_pointer_run_id)}를 기준으로 동작합니다.`)}</p><div class="mini-grid"><div class="mini"><div class="label">현재 모델</div><div class="value mono">${esc(shortRunLocal(runtime.live_runtime_model_run_id))}</div></div><div class="mini"><div class="label">챔피언 포인터</div><div class="value mono">${esc(shortRunLocal(runtime.champion_pointer_run_id))}</div></div><div class="mini"><div class="label">보유 포지션 / 주문</div><div class="value">${esc(`${maybe(state.positions_count, "0")} / ${maybe(state.open_orders_count, "0")}`)}</div></div><div class="mini"><div class="label">활성 리스크 플랜</div><div class="value">${esc(maybe(state.active_risk_plans_count, "0"))}</div></div><div class="mini"><div class="label">주문 방출 허용</div><div class="value">${esc(boolLabel(rolloutStatus.order_emission_allowed))}</div></div><div class="mini"><div class="label">WS 신선도</div><div class="value">${esc(runtime.ws_public_stale ? "오래됨" : "정상")}</div></div><div class="mini"><div class="label">포인터 동기화</div><div class="value">${esc(runtime.model_pointer_divergence ? "어긋남" : "정상")}</div></div><div class="mini"><div class="label">마지막 resume</div><div class="value">${esc(fmtDateTimeLocal(lastResume.generated_at || lastResume.checked_at || lastResume.completed_at))}</div></div></div><div class="tag-row" style="margin-top:12px">${rolloutStatus.mode ? pill("운용 모드", translateLocal(rolloutStatus.mode, policyText), rolloutStatus.mode === "live" ? "bad" : "warn") : ""}${runtime.ws_public_stale === true ? pill("WS 신선도", "오래됨", "bad") : pill("WS 신선도", "정상", "good")}${runtime.model_pointer_divergence === true ? pill("모델 포인터", "어긋남", "bad") : pill("모델 포인터", "정상", "good")}${activeBreakers.length ? pill("활성 브레이커", activeBreakers.length, "bad") : pill("활성 브레이커", "없음", "good")}</div><div class="detail-stack">${detailSectionLocal("보유 중인 종목", positions, "현재 보유 포지션이 없습니다.")}${detailSectionLocal("미체결 주문", openOrders, "현재 열린 주문이 없습니다.")}${detailSectionLocal("활성 리스크 플랜", plans, "현재 활성 리스크 플랜이 없습니다.")}${detailSectionLocal("최근 진입 / 종료 의도", intents, "최근 기록된 intent가 없습니다.")}</div></div>`;
        }).join("") : emptyLocal("라이브 상태 DB를 찾지 못했습니다.");
        const liveCards = Array.from(document.querySelectorAll("#live-state-cards .state-card"));
        states.forEach((state, index) => {
          const runtime = (((state || {}).runtime_artifacts || {}).runtime_recommendations) || {};
          const compareCard = renderExitCompareCardLocal(runtime, "현재 런타임 종료 모드 경쟁");
          const node = liveCards[index];
          if (!compareCard || !node) return;
          const detailStack = node.querySelector(".detail-stack");
          if (detailStack) {
            detailStack.insertAdjacentHTML("beforebegin", compareCard);
            return;
          }
          node.insertAdjacentHTML("beforeend", compareCard);
        });
      };

      window.renderArtifacts = function renderArtifacts(training) {
        const artifacts = training.candidate_artifacts || {};
        const runtime = artifacts.runtime_recommendations || {};
        const policy = artifacts.selection_policy || {};
        const calibration = artifacts.selection_calibration || {};
        const budget = artifacts.search_budget_decision || {};
        const factor = artifacts.factor_block_selection || {};
        const cpcv = artifacts.cpcv_lite_report || {};
        const wf = artifacts.walk_forward_report || {};
        document.getElementById("artifact-details").innerHTML = Object.keys(artifacts).length ? `<div class="artifact-card"><strong>런타임 추천</strong><div class="mini-grid"><div class="mini"><div class="label">종료 모드</div><div class="value">${esc(translateLocal(runtime.recommended_exit_mode, policyText))}</div></div><div class="mini"><div class="label">권장 보유 bar</div><div class="value">${esc(maybe(runtime.recommended_hold_bars))}</div></div><div class="mini"><div class="label">TP / SL / 추적</div><div class="value">${esc(`${fmtPctLocal(Number(runtime.tp_pct) * 100)} / ${fmtPctLocal(Number(runtime.sl_pct) * 100)} / ${fmtPctLocal(Number(runtime.trailing_pct) * 100)}`)}</div></div><div class="mini"><div class="label">산출 방식</div><div class="value">${esc(maybe(runtime.recommendation_source))}</div></div></div></div><div class="artifact-card"><strong>선택 정책 / 보정</strong><div class="mini-grid"><div class="mini"><div class="label">정책 모드</div><div class="value">${esc(translateLocal(policy.mode, policyText))}</div></div><div class="mini"><div class="label">기준 키</div><div class="value">${esc(maybe(policy.threshold_key))}</div></div><div class="mini"><div class="label">상위 비율</div><div class="value">${esc(policy.rank_quantile === null || policy.rank_quantile === undefined ? "-" : fmtPctLocal(Number(policy.rank_quantile) * 100))}</div></div><div class="mini"><div class="label">보정 사용</div><div class="value">${esc(boolLabel(policy.calibration_enabled))}</div></div><div class="mini"><div class="label">보정 방법</div><div class="value">${esc(maybe(calibration.method))}</div></div><div class="mini"><div class="label">보정 샘플 수</div><div class="value">${esc(maybe(calibration.sample_count))}</div></div></div></div><div class="artifact-card"><strong>탐색 예산 / 팩터 선택</strong><div class="mini-grid"><div class="mini"><div class="label">예산 결정</div><div class="value">${esc(maybe(budget.decision_mode))}</div></div><div class="mini"><div class="label">booster sweep</div><div class="value">${esc(maybe(budget.booster_sweep_trials))}</div></div><div class="mini"><div class="label">runtime grid</div><div class="value">${esc(maybe(budget.runtime_grid_mode))}</div></div><div class="mini"><div class="label">예산 사유</div><div class="value">${esc(joinReasonsLocal(budget.reasons))}</div></div><div class="mini"><div class="label">허용 블록</div><div class="value">${esc((factor.accepted_blocks || []).join(", ") || "-")}</div></div><div class="mini"><div class="label">제외 블록</div><div class="value">${esc((factor.rejected_blocks || []).join(", ") || "-")}</div></div></div></div><div class="artifact-card"><strong>강건성 검증</strong><div class="mini-grid"><div class="mini"><div class="label">CPCV 요청</div><div class="value">${esc(boolLabel(cpcv.requested))}</div></div><div class="mini"><div class="label">White comparable</div><div class="value">${esc(boolLabel(wf.white_rc_comparable))}</div></div><div class="mini"><div class="label">Hansen comparable</div><div class="value">${esc(boolLabel(wf.hansen_spa_comparable))}</div></div><div class="mini"><div class="label">selection trial 수</div><div class="value">${esc(maybe(wf.selection_search_trial_count))}</div></div></div></div>` : emptyLocal("최근 후보 산출물이 아직 없습니다.");
        if (Object.keys(artifacts).length) {
          const compareCard = renderExitCompareCardLocal(runtime, "후보 런타임 종료 모드 경쟁");
          if (compareCard) {
            document.getElementById("artifact-details").insertAdjacentHTML("afterbegin", compareCard);
          }
        }
      };

      window.renderNotes = function renderNotes(data) {
        const notes = [];
        const acceptance = (data.training || {}).acceptance || {};
        const challenger = data.challenger || {};
        const rollout = (data.live || {}).rollout_latest || {};
        const liveStates = (data.live || {}).states || [];
        if ((acceptance.reasons || []).length) notes.push(noteCardLocal("이번 후보 직접 사유", joinReasonsLocal(acceptance.reasons)));
        if ((acceptance.trainer_reasons || []).length) notes.push(noteCardLocal("학습 증거 세부 사유", joinReasonsLocal(acceptance.trainer_reasons)));
        if ((acceptance.notes || []).length) notes.push(noteCardLocal("운영 메모", joinReasonsLocal(acceptance.notes)));
        if (challenger.reason) notes.push(noteCardLocal("챌린저 미기동 사유", translateLocal(challenger.reason)));
        if (rollout.status && (rollout.status.reason_codes || []).length) notes.push(noteCardLocal("최근 rollout 제한 사유", joinReasonsLocal(rollout.status.reason_codes)));
        liveStates.forEach((state) => {
          const activeBreakers = uniqueLocal((state.active_breakers || []).map((item) => item.reason || item.code || item.name));
          if (activeBreakers.length) notes.push(noteCardLocal(`${state.label} 활성 브레이커`, joinReasonsLocal(activeBreakers)));
        });
        document.getElementById("notes-list").innerHTML = notes.length ? notes.join("") : emptyLocal("현재 바로 확인해야 할 경고나 참고 메모가 없습니다.");
      };

      window.renderMeta = function renderMeta(data) {
        document.getElementById("generated-at").textContent = fmtDateTimeLocal(data.generated_at);
        document.getElementById("project-root").textContent = data.project_root || "-";
        const system = data.system || {};
        const projectGb = Number(system.project_used_bytes || 0) / (1024 ** 3);
        const totalGb = Number(system.total_bytes || 0) / (1024 ** 3);
        const fsUsedGb = Number(system.used_bytes || 0) / (1024 ** 3);
        document.getElementById("storage-summary").textContent = `프로젝트 ${fmtNumberLocal(projectGb, 1)} GB · 파일시스템 사용 ${fmtNumberLocal(fsUsedGb, 1)} GB / 전체 ${fmtNumberLocal(totalGb, 1)} GB`;
      };

      window.setError = function setError(message) {
        const node = document.getElementById("fetch-error");
        if (!message) {
          node.style.display = "none";
          node.textContent = "";
          return;
        }
        node.style.display = "block";
        node.textContent = message;
      };

      window.renderAll = function renderAll(data) {
        window.renderMeta(data);
        window.renderServices(data.services || {});
        window.renderTraining(data.training || {});
        window.renderChallenger(data.challenger || {});
        window.renderPaper(data.paper || {});
        window.renderWsPublic(data.ws_public || {});
        window.renderLive(data.live || {});
        window.renderArtifacts(data.training || {});
        window.renderNotes(data);
        window.setupSectionNav();
      };

      try {
        renderServices = window.renderServices;
        renderTraining = window.renderTraining;
        renderChallenger = window.renderChallenger;
        renderPaper = window.renderPaper;
        renderWsPublic = window.renderWsPublic;
        renderLive = window.renderLive;
        renderArtifacts = window.renderArtifacts;
        renderMeta = window.renderMeta;
        renderAll = window.renderAll;
        setError = window.setError;
      } catch (_err) {}

      window.setupSectionNav();
      if (typeof INITIAL_SNAPSHOT !== "undefined" && INITIAL_SNAPSHOT) {
        try {
          window.renderAll(INITIAL_SNAPSHOT);
        } catch (err) {
          window.setError(`초기 렌더링 실패: ${err && err.message ? err.message : err}`);
        }
      }
    })();
  </script>
</body></html>
"""


class DashboardRequestHandler(BaseHTTPRequestHandler):
    project_root: Path = Path.cwd()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/":
            try:
                initial_snapshot = build_dashboard_snapshot(self.project_root)
            except Exception:
                initial_snapshot = {"generated_at": _utc_now_iso()}
            body = _render_dashboard_index(initial_snapshot)
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store, max-age=0")
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):  # pragma: no cover - client closed early
                return
            return
        if parsed.path == "/static/dashboard.css":
            body = bytes(_load_dashboard_asset("dashboard.css", binary=True))
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/css; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store, max-age=0")
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):  # pragma: no cover - client closed early
                return
            return
        if parsed.path == "/static/dashboard.js":
            body = bytes(_load_dashboard_asset("dashboard.js", binary=True))
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/javascript; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store, max-age=0")
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):  # pragma: no cover - client closed early
                return
            return
        if parsed.path == "/healthz":
            _json_response(self, {"ok": True, "ts": _utc_now_iso()})
            return
        if parsed.path == "/api/snapshot":
            try:
                payload = build_dashboard_snapshot(self.project_root)
            except Exception as exc:  # pragma: no cover
                _json_response(self, {"ok": False, "error": str(exc), "generated_at": _utc_now_iso()}, status=500)
                return
            _json_response(self, payload)
            return
        _json_response(self, {"ok": False, "error": "not_found"}, status=404)

    def log_message(self, fmt: str, *args: Any) -> None:
        return


def _build_handler(project_root: Path) -> type[DashboardRequestHandler]:
    class _BoundHandler(DashboardRequestHandler):
        pass

    _BoundHandler.project_root = project_root
    return _BoundHandler


def serve_dashboard(*, project_root: Path, host: str, port: int) -> None:
    server = ThreadingHTTPServer((host, port), _build_handler(project_root.resolve()))
    try:
        server.serve_forever()
    except KeyboardInterrupt:  # pragma: no cover
        pass
    finally:
        server.server_close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Autobot operations dashboard")
    parser.add_argument("--project-root", default=".", help="Autobot project root")
    parser.add_argument("--host", default=DEFAULT_DASHBOARD_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_DASHBOARD_PORT)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    serve_dashboard(
        project_root=Path(args.project_root),
        host=str(args.host).strip() or DEFAULT_DASHBOARD_HOST,
        port=max(int(args.port), 1),
    )


if __name__ == "__main__":
    main()
