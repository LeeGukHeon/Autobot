"""Read-only operations dashboard for training, paper, and live runtime."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
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


DEFAULT_DASHBOARD_HOST = "0.0.0.0"
DEFAULT_DASHBOARD_PORT = 8088


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


def _load_live_db_summary(db_path: Path, label: str) -> dict[str, Any]:
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
        breaker_states = _query_all(conn, "SELECT * FROM breaker_states ORDER BY updated_ts DESC") if "breaker_states" in tables else []
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
        return {
            "label": label,
            "db_path": str(db_path),
            "exists": True,
            "positions_count": len(positions),
            "open_orders_count": len(open_order_rows),
            "intents_count": len(intents),
            "active_risk_plans_count": len(active_risk_plans),
            "breaker_active": len(active_breakers) > 0,
            "positions": positions[:8],
            "open_orders": open_order_rows[:8],
            "recent_intents": intents[:8],
            "active_risk_plans": active_risk_plans[:8],
            "active_breakers": [
                {
                    **row,
                    "reason_codes": _normalize_json_text(row.get("reason_codes_json")) or [],
                    "details": _normalize_json_text(row.get("details_json")) or {},
                }
                for row in active_breakers[:8]
            ],
            "runtime_health": checkpoints.get("live_runtime_health") or {},
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
    return {
        "recommended_exit_mode": _dig(payload, "exit", "mode"),
        "recommended_hold_bars": _dig(payload, "exit", "recommended_hold_bars"),
        "tp_pct": _dig(payload, "exit", "tp_pct"),
        "sl_pct": _dig(payload, "exit", "sl_pct"),
        "trailing_pct": _dig(payload, "exit", "trailing_pct"),
        "risk_multiplier": _dig(payload, "risk", "risk_multiplier"),
        "recommendation_source": _dig(payload, "exit", "recommendation_source"),
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
            "spawn_timer": _unit_snapshot("autobot-v4-challenger-spawn.timer", timer=True),
            "promote_timer": _unit_snapshot("autobot-v4-challenger-promote.timer", timer=True),
        },
        "training": {
            "acceptance": acceptance,
            "candidate_artifacts": _collect_recent_model_artifacts(project_root, acceptance.get("candidate_run_dir")),
        },
        "challenger": _summarize_challenger(challenger_latest, challenger_state),
        "paper": {
            "recent_runs": _latest_paper_summaries(project_root),
        },
        "live": {
            "rollout_latest": _load_json(live_rollout_latest),
            "states": [_load_live_db_summary(path, label) for label, path in _resolve_live_db_candidates(project_root)],
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
    <section class="grid">
      <article class="card"><div class="card-head"><h2 class="card-title">서비스 상태</h2><span class="muted">systemd 기준</span></div><div class="service-row" id="service-cards"></div></article>
      <article class="card half"><div class="card-head"><h2 class="card-title">학습 / 어셉턴스</h2><span class="muted">최신 acceptance</span></div><div class="kpis" id="training-kpis"></div><div id="training-details"></div></article>
      <article class="card half"><div class="card-head"><h2 class="card-title">챌린저 루프</h2><span class="muted">spawn / promote 결과</span></div><div class="kpis" id="challenger-kpis"></div><div id="challenger-details"></div></article>
      <article class="card half"><div class="card-head"><h2 class="card-title">페이퍼 런</h2><span class="muted">최근 요약</span></div><div id="paper-table"></div></article>
      <article class="card half"><div class="card-head"><h2 class="card-title">WS 수집 상태</h2><span class="muted">public data plane</span></div><div class="kpis" id="ws-kpis"></div><div id="ws-details"></div></article>
      <article class="card"><div class="card-head"><h2 class="card-title">라이브 상태</h2><span class="muted">메인 / 후보 카나리아 DB 요약</span></div><div class="service-row" id="live-state-cards"></div></article>
      <article class="card half"><div class="card-head"><h2 class="card-title">후보 산출물</h2><span class="muted">selection / calibration / runtime</span></div><div id="artifact-details"></div></article>
      <article class="card half"><div class="card-head"><h2 class="card-title">메모</h2><span class="muted">거절 사유 / 참고 정보</span></div><ul class="list" id="notes-list"></ul></article>
    </section>
  </div>
  <script>
    const INITIAL_SNAPSHOT = __INITIAL_SNAPSHOT__;
    const fmtNumber=(v,d=2)=>v===null||v===undefined||Number.isNaN(Number(v))?"-":new Intl.NumberFormat("ko-KR",{maximumFractionDigits:d,minimumFractionDigits:0}).format(Number(v));
    const fmtMoney=(v)=>v===null||v===undefined?"-":`${fmtNumber(v,0)} KRW`;
    const fmtPct=(v)=>v===null||v===undefined?"-":`${fmtNumber(v,2)}%`;
    const HTML_ESC = {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'};
    const esc=(v)=>String(v??"").replace(/[&<>"]/g,(ch)=>HTML_ESC[ch]);
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
    async function refresh(){ try { const response=await fetch("/api/snapshot",{cache:"no-store"}); if(!response.ok){ throw new Error(`snapshot 응답 실패 (${response.status})`); } const data=await response.json(); renderAll(data); setError(""); } catch (err) { setError(`실시간 새로고침 실패: ${err && err.message ? err.message : err}`); } }
    document.getElementById("refresh-btn").addEventListener("click",refresh); if (INITIAL_SNAPSHOT && typeof INITIAL_SNAPSHOT === "object") { try { renderAll(INITIAL_SNAPSHOT); } catch (err) { setError(`초기 렌더링 실패: ${err && err.message ? err.message : err}`); } } refresh(); setInterval(refresh,10000);
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
            html = INDEX_HTML.replace("__INITIAL_SNAPSHOT__", _html_json(initial_snapshot))
            body = html.encode("utf-8")
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
