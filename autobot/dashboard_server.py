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
from autobot.upbit.config import load_upbit_settings
from autobot.upbit.http_client import UpbitHttpClient
from autobot.upbit.public import UpbitPublicClient


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


def _ratio_to_bps(value: Any) -> float | None:
    numeric = _coerce_float(value)
    if numeric is None:
        return None
    return float(numeric) * 10_000.0


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


@lru_cache(maxsize=16)
def _cached_live_market_tickers(project_root_str: str, bucket: int, markets_key: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    _ = bucket
    project_root = Path(project_root_str)
    if not markets_key:
        return {}
    try:
        settings = load_upbit_settings(project_root / "config")
        with UpbitHttpClient(settings) as http_client:
            client = UpbitPublicClient(http_client)
            payload = client.ticker(markets_key)
    except Exception:
        return {}
    if not isinstance(payload, list):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue
        market = str(item.get("market") or "").strip().upper()
        if not market:
            continue
        out[market] = {
            "market": market,
            "trade_price": _coerce_float(item.get("trade_price")),
            "trade_timestamp": _coerce_int(item.get("trade_timestamp") or item.get("timestamp")),
            "signed_change_rate": _coerce_float(item.get("signed_change_rate")),
        }
    return out


def _load_live_market_tickers(project_root: Path, markets: list[str]) -> dict[str, dict[str, Any]]:
    normalized = tuple(sorted({str(item).strip().upper() for item in markets if str(item).strip()}))
    bucket = int(time.time() // 2)
    return _cached_live_market_tickers(str(project_root.resolve()), bucket, normalized)


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
        "next_run_at": (payload.get("NextElapseUSecRealtime") or None) if timer else None,
        "last_trigger_at": (payload.get("LastTriggerUSec") or None) if timer else None,
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


def _summarize_live_position(row: dict[str, Any], *, market_tickers: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    market = row.get("market")
    avg_entry_price = _coerce_float(row.get("avg_entry_price"))
    base_amount = _coerce_float(row.get("base_amount"))
    ticker = dict((market_tickers or {}).get(str(market or "").strip().upper()) or {})
    current_price = _coerce_float(ticker.get("trade_price"))
    unrealized_pnl_quote = None
    unrealized_pnl_pct = None
    if current_price is not None and avg_entry_price is not None and base_amount is not None:
        unrealized_pnl_quote = (float(current_price) - float(avg_entry_price)) * float(base_amount)
        if float(avg_entry_price) > 0:
            unrealized_pnl_pct = ((float(current_price) / float(avg_entry_price)) - 1.0) * 100.0
    return {
        "market": market,
        "base_amount": base_amount,
        "avg_entry_price": avg_entry_price,
        "managed": bool(row.get("managed", 1)),
        "updated_ts": _coerce_int(row.get("updated_ts")),
        "current_price": current_price,
        "current_price_ts_ms": _coerce_int(ticker.get("trade_timestamp")),
        "unrealized_pnl_quote": unrealized_pnl_quote,
        "unrealized_pnl_pct": unrealized_pnl_pct,
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
    exit_recommendation = (
        strategy_meta.get("exit_recommendation") if isinstance(strategy_meta.get("exit_recommendation"), dict) else {}
    )
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
        "skip_reason": (
            meta_dict.get("skip_reason")
            or admissibility.get("reject_code")
            or (
                trade_gate.get("reason_code")
                if str(trade_gate.get("reason_code") or "").strip().upper() not in {"ALLOW", "POLICY_OK"}
                else None
            )
        ),
        "estimated_total_cost_bps": _coerce_float(admissibility.get("estimated_total_cost_bps")),
        "expected_net_edge_bps": _coerce_float(admissibility.get("expected_net_edge_bps")),
        "trade_action_recommended_action": trade_action.get("recommended_action"),
        "trade_action_expected_edge_bps": _ratio_to_bps(trade_action.get("expected_edge")),
        "trade_action_expected_downside_bps": _ratio_to_bps(trade_action.get("expected_downside_deviation")),
        "trade_action_expected_es_bps": _ratio_to_bps(trade_action.get("expected_es")),
        "trade_action_expected_ctm": _coerce_float(
            trade_action.get("expected_ctm") if trade_action.get("expected_ctm") is not None else trade_action.get("expected_ctm2")
        ),
        "trade_action_expected_ctm_order": _coerce_int(trade_action.get("expected_ctm_order")),
        "trade_action_objective_score": _coerce_float(trade_action.get("expected_objective_score")),
        "trade_action_action_value": _coerce_float(
            trade_action.get("expected_action_value")
            if trade_action.get("expected_action_value") is not None
            else trade_action.get("expected_objective_score")
        ),
        "trade_action_tail_probability": _coerce_float(trade_action.get("expected_tail_probability")),
        "trade_action_decision_source": trade_action.get("decision_source") or trade_action.get("chosen_action_source"),
        "trade_action_notional_multiplier": _coerce_float(trade_action.get("recommended_notional_multiplier")),
        "exit_recommendation_mode": exit_recommendation.get("recommended_exit_mode"),
        "exit_recommendation_chosen_family": exit_recommendation.get("chosen_family"),
        "exit_recommendation_chosen_rule_id": exit_recommendation.get("chosen_rule_id"),
        "exit_recommendation_family_compare_status": exit_recommendation.get("family_compare_status"),
    }


def _summarize_live_trade_journal(row: dict[str, Any]) -> dict[str, Any]:
    entry_meta = _normalize_json_text(row.get("entry_meta_json")) or {}
    trade_action = _dig(entry_meta, "strategy", "meta", "trade_action", default={}) or {}
    exit_recommendation = _dig(entry_meta, "strategy", "meta", "exit_recommendation", default={}) or {}
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
        "expected_es_bps": _ratio_to_bps(trade_action.get("expected_es")),
        "expected_ctm": _coerce_float(
            trade_action.get("expected_ctm") if trade_action.get("expected_ctm") is not None else trade_action.get("expected_ctm2")
        ),
        "expected_ctm_order": _coerce_int(trade_action.get("expected_ctm_order")),
        "trade_action_action_value": _coerce_float(
            trade_action.get("expected_action_value")
            if trade_action.get("expected_action_value") is not None
            else trade_action.get("expected_objective_score")
        ),
        "trade_action_tail_probability": _coerce_float(trade_action.get("expected_tail_probability")),
        "trade_action_decision_source": trade_action.get("decision_source") or trade_action.get("chosen_action_source"),
        "exit_recommendation_mode": exit_recommendation.get("recommended_exit_mode"),
        "exit_recommendation_chosen_family": exit_recommendation.get("chosen_family"),
        "exit_recommendation_chosen_rule_id": exit_recommendation.get("chosen_rule_id"),
        "exit_recommendation_family_compare_status": exit_recommendation.get("family_compare_status"),
        "expected_net_edge_bps": _coerce_float(row.get("expected_net_edge_bps")),
        "notional_multiplier": _coerce_float(row.get("notional_multiplier")),
        "entry_meta": entry_meta,
        "exit_meta": exit_meta,
        "close_verified": bool(exit_meta.get("close_verified")) if exit_meta.get("close_verified") is not None else None,
        "close_verification_status": exit_meta.get("close_verification_status"),
    }


def _trade_journal_dedupe_key(
    row: dict[str, Any],
) -> tuple[Any, ...]:
    item = _summarize_live_trade_journal(row)
    status = str(item.get("status") or "").strip().upper()
    journal_id = str(item.get("journal_id") or "").strip()
    if status in {"CLOSED", "CANCELLED_ENTRY"}:
        exit_uuid = str(item.get("exit_order_uuid") or "").strip()
        return (
            status,
            "journal_id" if exit_uuid else str(item.get("market") or "").strip().upper(),
            journal_id if exit_uuid else _coerce_int(item.get("exit_ts_ms")),
            exit_uuid if exit_uuid else _coerce_float(item.get("realized_pnl_quote")),
            _coerce_float(item.get("qty")),
            _coerce_float(item.get("entry_price")),
            _coerce_float(item.get("exit_price")),
        )
    return ("journal_id", journal_id)


def _is_synthetic_closed_trade_row(item: dict[str, Any]) -> bool:
    journal_id = str(item.get("journal_id") or "").strip()
    status = str(item.get("status") or "").strip().upper()
    if status != "CLOSED":
        return False
    if str(item.get("entry_intent_id") or "").strip():
        return False
    if str(item.get("entry_order_uuid") or "").strip():
        return False
    return journal_id.startswith("imported-") or journal_id.startswith("trade-")


def _dedupe_trade_journal_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    canonical_exit_order_uuids: set[str] = set()
    for row in rows:
        item = _summarize_live_trade_journal(row)
        exit_uuid = str(item.get("exit_order_uuid") or "").strip()
        if exit_uuid and not _is_synthetic_closed_trade_row(item):
            canonical_exit_order_uuids.add(exit_uuid)
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for row in rows:
        item = _summarize_live_trade_journal(row)
        exit_uuid = str(item.get("exit_order_uuid") or "").strip()
        if _is_synthetic_closed_trade_row(item) and exit_uuid and exit_uuid in canonical_exit_order_uuids:
            continue
        key = _trade_journal_dedupe_key(row)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _summarize_kst_trade_day(rows: list[dict[str, Any]], *, now_ts_ms: int) -> dict[str, Any]:
    rows = _dedupe_trade_journal_rows(rows)
    now_dt = datetime.fromtimestamp(now_ts_ms / 1000.0, tz=_KST)
    start_dt = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end_dt = start_dt + timedelta(days=1)
    start_ts_ms = int(start_dt.timestamp() * 1000)
    end_ts_ms = int(end_dt.timestamp() * 1000)
    summary = {
        "date_label": start_dt.strftime("%Y-%m-%d"),
        "timezone": "KST",
        "closed_count": 0,
        "verified_closed_count": 0,
        "unverified_closed_count": 0,
        "open_count": 0,
        "pending_count": 0,
        "cancelled_count": 0,
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
            if item.get("close_verified") is False:
                summary["unverified_closed_count"] += 1
                continue
            summary["verified_closed_count"] += 1
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
        elif status == "CANCELLED_ENTRY":
            if exit_ts_ms is not None and start_ts_ms <= exit_ts_ms < end_ts_ms:
                summary["cancelled_count"] += 1
    verified_closed_count = int(summary["verified_closed_count"])
    if verified_closed_count > 0:
        summary["win_rate_pct"] = (float(summary["wins"]) / float(verified_closed_count)) * 100.0
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
        deduped_trade_journal = _dedupe_trade_journal_rows(trade_journal)
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
            for name in (
                "live_runtime_health",
                "live_rollout_status",
                "live_rollout_contract",
                "last_resume",
                "daemon_last_run",
                "live_model_alpha_last_run",
                "last_ws_event",
            ):
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
        market_tickers = _load_live_market_tickers(
            project_root,
            [str(row.get("market") or "").strip().upper() for row in positions],
        )
        today_trade_summary = _summarize_kst_trade_day(deduped_trade_journal, now_ts_ms=now_ts_ms)
        today_trade_summary["current_positions_count"] = len(positions)
        today_trade_summary["current_pending_orders_count"] = len(
            [
                row
                for row in open_order_rows
                if str(row.get("side") or "").strip().lower() == "bid"
            ]
        )
        today_trade_summary["current_exit_orders_count"] = len(
            [
                row
                for row in open_order_rows
                if str(row.get("side") or "").strip().lower() == "ask"
            ]
        )
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
        daemon_last_run = checkpoints.get("live_model_alpha_last_run") or checkpoints.get("daemon_last_run") or {}
        last_ws_event = checkpoints.get("last_ws_event") or {}
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
            "positions": [_summarize_live_position(row, market_tickers=market_tickers) for row in positions[:8]],
            "open_orders": [_summarize_live_order(row) for row in open_order_rows[:8]],
            "recent_intents": [_summarize_live_intent(row) for row in intents[:8]],
            "recent_trades": [
                _summarize_live_trade_journal(row)
                for row in deduped_trade_journal
                if str(row.get("status") or "").strip().upper() == "CLOSED"
            ][:8],
            "today_trade_summary": today_trade_summary,
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
            "daemon_last_run": daemon_last_run,
            "last_ws_event": last_ws_event,
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
    hold_family = dict(exit_payload.get("hold_family") or {})
    risk_family = dict(exit_payload.get("risk_family") or {})
    family_compare = dict(exit_payload.get("family_compare") or {})
    trade_action = dict(normalized.get("trade_action") or {})
    trade_action_summary = {
        "status": trade_action.get("status"),
        "source": trade_action.get("source"),
        "risk_feature_name": trade_action.get("risk_feature_name"),
        "runtime_decision_source": trade_action.get("runtime_decision_source"),
        "state_feature_names": trade_action.get("state_feature_names"),
        "tail_confidence_level": _coerce_float(trade_action.get("tail_confidence_level")),
        "ctm_order": _coerce_int(trade_action.get("ctm_order")),
        "tail_risk_method": _dig(trade_action, "tail_risk_contract", "method"),
        "conditional_action_model_status": _dig(trade_action, "conditional_action_model", "status"),
        "conditional_action_model": _dig(trade_action, "conditional_action_model", "model"),
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
                "expected_edge_bps": _ratio_to_bps(item.get("expected_edge")),
                "expected_downside_bps": _ratio_to_bps(item.get("expected_downside_deviation")),
                "expected_es_bps": _ratio_to_bps(item.get("expected_es")),
                "expected_ctm": _coerce_float(
                    item.get("expected_ctm") if item.get("expected_ctm") is not None else item.get("expected_ctm2")
                ),
                "expected_ctm_order": _coerce_int(item.get("expected_ctm_order")),
                "expected_action_value": _coerce_float(
                    item.get("expected_action_value")
                    if item.get("expected_action_value") is not None
                    else item.get("expected_objective_score")
                ),
                "expected_tail_probability": _coerce_float(item.get("expected_tail_probability")),
                "notional_multiplier": _coerce_float(item.get("recommended_notional_multiplier")),
                "sample_count": _coerce_int(item.get("sample_count")),
            }
        )
    return {
        "recommended_exit_mode": _dig(normalized, "exit", "recommended_exit_mode") or _dig(normalized, "exit", "mode"),
        "recommended_exit_mode_reason_code": _dig(normalized, "exit", "recommended_exit_mode_reason_code"),
        "recommended_hold_bars": _dig(normalized, "exit", "recommended_hold_bars"),
        "chosen_family": exit_payload.get("chosen_family"),
        "chosen_rule_id": exit_payload.get("chosen_rule_id"),
        "hold_objective_score": _dig(normalized, "exit", "objective_score"),
        "risk_objective_score": _dig(normalized, "exit", "risk_objective_score"),
        "hold_grid_point": hold_grid_point,
        "risk_grid_point": risk_grid_point,
        "hold_family": {
            "status": hold_family.get("status"),
            "rows_total": _coerce_int(hold_family.get("rows_total")),
            "comparable_rows": _coerce_int(hold_family.get("comparable_rows")),
            "best_rule_id": hold_family.get("best_rule_id"),
            "best_comparable_rule_id": hold_family.get("best_comparable_rule_id"),
        },
        "risk_family": {
            "status": risk_family.get("status"),
            "rows_total": _coerce_int(risk_family.get("rows_total")),
            "comparable_rows": _coerce_int(risk_family.get("comparable_rows")),
            "best_rule_id": risk_family.get("best_rule_id"),
            "best_comparable_rule_id": risk_family.get("best_comparable_rule_id"),
        },
        "family_compare": {
            "status": family_compare.get("status"),
            "decision": family_compare.get("decision"),
            "comparable": bool(family_compare.get("comparable", False)),
            "reason_codes": list(family_compare.get("reason_codes") or []),
            "hold_rule_id": family_compare.get("hold_rule_id"),
            "risk_rule_id": family_compare.get("risk_rule_id"),
        },
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


def _sse_response(handler: BaseHTTPRequestHandler, project_root: Path, *, interval_sec: float = 2.0) -> None:
    handler.send_response(HTTPStatus.OK)
    handler.send_header("Content-Type", "text/event-stream; charset=utf-8")
    handler.send_header("Cache-Control", "no-store, max-age=0")
    handler.send_header("Connection", "keep-alive")
    handler.end_headers()
    try:
        handler.wfile.write(b"retry: 5000\n\n")
        handler.wfile.flush()
        while True:
            payload = build_dashboard_snapshot(project_root)
            body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            handler.wfile.write(b"event: snapshot\n")
            handler.wfile.write(b"data: ")
            handler.wfile.write(body)
            handler.wfile.write(b"\n\n")
            handler.wfile.flush()
            time.sleep(max(float(interval_sec), 0.5))
    except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
        return


def _html_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False).replace("</", "<\\/")


def _load_dashboard_asset(name: str, *, binary: bool = False) -> str | bytes:
    path = _DASHBOARD_ASSETS_DIR / name
    return path.read_bytes() if binary else path.read_text(encoding="utf-8")


def _dashboard_asset_version() -> str:
    paths = [_DASHBOARD_ASSETS_DIR / "index.html", _DASHBOARD_ASSETS_DIR / "dashboard.css", _DASHBOARD_ASSETS_DIR / "dashboard.js"]
    latest = max((int(path.stat().st_mtime_ns) for path in paths if path.exists()), default=0)
    return str(latest)


def _render_dashboard_index(initial_snapshot: dict[str, Any]) -> bytes:
    template = str(_load_dashboard_asset("index.html"))
    html = template.replace("__INITIAL_SNAPSHOT__", _html_json(initial_snapshot))
    html = html.replace("__ASSET_VERSION__", _dashboard_asset_version())
    return html.encode("utf-8")


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
        if parsed.path == "/api/stream":
            try:
                _sse_response(self, self.project_root)
            except Exception:  # pragma: no cover
                return
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
