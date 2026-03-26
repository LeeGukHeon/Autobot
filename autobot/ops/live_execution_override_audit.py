from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import time
from typing import Any

from autobot.live.breaker_taxonomy import annotate_reason_payload
from autobot.models.live_execution_policy import build_live_execution_contract
from autobot.models.registry import resolve_run_dir

DEFAULT_DB_PATH = Path("data/state/live_candidate/live_state.db")
DEFAULT_REGISTRY_ROOT = Path("models/registry")
DEFAULT_MODEL_FAMILY = "train_v4_crypto_cs"


def build_live_execution_override_audit(
    *,
    db_path: Path,
    registry_root: Path,
    model_family: str,
    model_ref: str | None = None,
    lookback_days: int = 30,
    attempt_limit: int = 5_000,
    example_limit: int = 12,
    now_ts_ms: int | None = None,
) -> dict[str, Any]:
    resolved_db_path = Path(db_path)
    if not resolved_db_path.exists():
        raise FileNotFoundError(f"db_path not found: {resolved_db_path}")

    effective_now_ts_ms = int(now_ts_ms if now_ts_ms is not None else time.time() * 1000)
    since_ts_ms = effective_now_ts_ms - (max(int(lookback_days), 1) * 86_400_000)

    runtime_contract_checkpoint = _load_checkpoint(resolved_db_path, "live_runtime_contract")
    rollout_status_checkpoint = _load_checkpoint(resolved_db_path, "live_rollout_status")
    live_execution_policy_checkpoint = _load_checkpoint(resolved_db_path, "live_execution_policy_model")

    model_resolution = _resolve_model_reference(
        db_path=resolved_db_path,
        registry_root=Path(registry_root),
        model_family=str(model_family).strip() or DEFAULT_MODEL_FAMILY,
        model_ref=model_ref,
        runtime_contract_checkpoint=runtime_contract_checkpoint,
    )
    run_dir = Path(model_resolution["run_dir"])

    runtime_recommendations = _load_json(run_dir / "runtime_recommendations.json")
    selection_policy = _load_json(run_dir / "selection_policy.json")
    trainer_research_evidence = _load_json(run_dir / "trainer_research_evidence.json")
    promotion_decision = _load_json(run_dir / "promotion_decision.json")

    attempts = _load_attempt_rows(
        db_path=resolved_db_path,
        since_ts_ms=since_ts_ms,
        limit=max(int(attempt_limit), 1),
    )
    trade_journal_summary = _load_trade_journal_summary(
        db_path=resolved_db_path,
        since_ts_ms=since_ts_ms,
    )
    breaker_events = _load_breaker_events(
        db_path=resolved_db_path,
        since_ts_ms=since_ts_ms,
        limit=max(int(example_limit) * 4, 20),
    )
    breaker_state = _load_breaker_state(db_path=resolved_db_path)

    recommended_execution = dict((runtime_recommendations or {}).get("execution") or {})
    recommended_price_mode = str(recommended_execution.get("recommended_price_mode", "")).strip().upper()
    recommended_timeout_bars = _safe_optional_int(recommended_execution.get("recommended_timeout_bars"))
    recommended_replace_max = _safe_optional_int(recommended_execution.get("recommended_replace_max"))

    attempt_summary = _summarize_attempts(
        attempts=attempts,
        recommended_price_mode=recommended_price_mode,
    )
    override_summary = _summarize_execution_overrides(
        attempts=attempts,
        recommended_price_mode=recommended_price_mode,
    )
    live_execution_contract = build_live_execution_contract(
        attempts=[_execution_contract_attempt_row(item) for item in attempts],
    )
    breaker_summary = _summarize_breakers(
        breaker_events=breaker_events,
        breaker_state=breaker_state,
        rollout_status_checkpoint=rollout_status_checkpoint,
    )
    examples = _build_examples(
        attempts=attempts,
        recommended_price_mode=recommended_price_mode,
        example_limit=max(int(example_limit), 1),
    )

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "db_path": str(resolved_db_path),
        "lookback_days": max(int(lookback_days), 1),
        "since_ts_ms": int(since_ts_ms),
        "attempt_limit": max(int(attempt_limit), 1),
        "model_resolution": model_resolution,
        "runtime_contract_checkpoint": runtime_contract_checkpoint,
        "rollout_status_checkpoint": rollout_status_checkpoint,
        "live_execution_policy_checkpoint": _summarize_execution_policy_checkpoint(live_execution_policy_checkpoint),
        "registry_run": {
            "run_dir": str(run_dir),
            "selection_policy": {
                "mode": str((selection_policy or {}).get("mode", "")).strip(),
                "selection_fraction": _safe_optional_float((selection_policy or {}).get("selection_fraction")),
                "min_candidates_per_ts": _safe_optional_int((selection_policy or {}).get("min_candidates_per_ts")),
                "threshold_key": str((selection_policy or {}).get("threshold_key", "")).strip(),
                "threshold_value": _safe_optional_float((selection_policy or {}).get("threshold_value")),
                "selection_recommendation_source": str(
                    (selection_policy or {}).get("selection_recommendation_source", "")
                ).strip(),
            },
            "runtime_recommendations": {
                "execution": {
                    "status": str(recommended_execution.get("status", "")).strip(),
                    "policy": str(recommended_execution.get("policy", "")).strip(),
                    "recommended_price_mode": recommended_price_mode,
                    "recommended_timeout_bars": recommended_timeout_bars,
                    "recommended_replace_max": recommended_replace_max,
                    "dynamic_stage_selection_enabled": bool(
                        recommended_execution.get("dynamic_stage_selection_enabled", False)
                    ),
                    "frontier_summary": dict(recommended_execution.get("frontier_summary") or {}),
                    "no_trade_region": dict(recommended_execution.get("no_trade_region") or {}),
                },
                "trade_action": {
                    "status": str(((runtime_recommendations or {}).get("trade_action") or {}).get("status", "")).strip(),
                    "runtime_decision_source": str(
                        ((runtime_recommendations or {}).get("trade_action") or {}).get("runtime_decision_source", "")
                    ).strip(),
                    "summary": dict(((runtime_recommendations or {}).get("trade_action") or {}).get("summary") or {}),
                },
                "risk_control": {
                    "status": str(((runtime_recommendations or {}).get("risk_control") or {}).get("status", "")).strip(),
                    "operating_mode": str(
                        ((runtime_recommendations or {}).get("risk_control") or {}).get("operating_mode", "")
                    ).strip(),
                    "selected_threshold": _safe_optional_float(
                        ((runtime_recommendations or {}).get("risk_control") or {}).get("selected_threshold")
                    ),
                    "selected_mean_return": _safe_optional_float(
                        ((runtime_recommendations or {}).get("risk_control") or {}).get("selected_mean_return")
                    ),
                    "live_gate": dict(((runtime_recommendations or {}).get("risk_control") or {}).get("live_gate") or {}),
                    "online_adaptation": dict(
                        ((runtime_recommendations or {}).get("risk_control") or {}).get("online_adaptation") or {}
                    ),
                },
            },
            "trainer_research_evidence": {
                "available": bool((trainer_research_evidence or {}).get("available", False)),
                "pass": bool((trainer_research_evidence or {}).get("pass", False)),
                "offline_pass": bool((trainer_research_evidence or {}).get("offline_pass", False)),
                "execution_pass": bool((trainer_research_evidence or {}).get("execution_pass", False)),
                "risk_control_pass": bool((trainer_research_evidence or {}).get("risk_control_pass", False)),
                "reasons": list((trainer_research_evidence or {}).get("reasons") or []),
            },
            "promotion_decision": {
                "status": str((promotion_decision or {}).get("status", "")).strip(),
                "promote": bool((promotion_decision or {}).get("promote", False)),
                "promotion_mode": str((promotion_decision or {}).get("promotion_mode", "")).strip(),
                "reasons": list((promotion_decision or {}).get("reasons") or []),
            },
        },
        "trade_journal_summary": trade_journal_summary,
        "execution_attempt_summary": attempt_summary,
        "execution_override_summary": override_summary,
        "observed_execution_contract": _summarize_live_execution_contract(live_execution_contract),
        "breaker_summary": breaker_summary,
        "examples": examples,
    }
    payload["findings"] = _build_findings(payload)
    return payload


def write_live_execution_override_audit(
    *,
    payload: dict[str, Any],
    output_dir: Path,
) -> dict[str, str]:
    resolved_output_dir = Path(output_dir)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    json_path = resolved_output_dir / f"live_execution_override_audit_{stamp}.json"
    md_path = resolved_output_dir / f"live_execution_override_audit_{stamp}.md"
    latest_json_path = resolved_output_dir / "latest.json"
    latest_md_path = resolved_output_dir / "latest.md"

    rendered_json = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    rendered_md = render_live_execution_override_audit_markdown(payload)

    json_path.write_text(rendered_json, encoding="utf-8")
    latest_json_path.write_text(rendered_json, encoding="utf-8")
    md_path.write_text(rendered_md, encoding="utf-8")
    latest_md_path.write_text(rendered_md, encoding="utf-8")

    return {
        "json_path": str(json_path),
        "md_path": str(md_path),
        "latest_json_path": str(latest_json_path),
        "latest_md_path": str(latest_md_path),
    }


def render_live_execution_override_audit_markdown(payload: dict[str, Any]) -> str:
    model_resolution = dict(payload.get("model_resolution") or {})
    registry_run = dict(payload.get("registry_run") or {})
    runtime_recommendations = dict(registry_run.get("runtime_recommendations") or {})
    execution = dict(runtime_recommendations.get("execution") or {})
    trade_action = dict(runtime_recommendations.get("trade_action") or {})
    risk_control = dict(runtime_recommendations.get("risk_control") or {})
    attempt_summary = dict(payload.get("execution_attempt_summary") or {})
    override_summary = dict(payload.get("execution_override_summary") or {})
    trade_journal_summary = dict(payload.get("trade_journal_summary") or {})
    breaker_summary = dict(payload.get("breaker_summary") or {})
    trainer_evidence = dict(registry_run.get("trainer_research_evidence") or {})
    promotion = dict(registry_run.get("promotion_decision") or {})

    lines: list[str] = []
    lines.append("# Live Execution Override Audit")
    lines.append("")
    lines.append(f"- generated_at_utc: {payload.get('generated_at_utc', '')}")
    lines.append(f"- db_path: {payload.get('db_path', '')}")
    lines.append(f"- lookback_days: {payload.get('lookback_days', 0)}")
    lines.append(f"- model_ref_resolved: {model_resolution.get('resolved_model_ref', '')}")
    lines.append(f"- model_ref_source: {model_resolution.get('resolved_model_ref_source', '')}")
    lines.append("")
    lines.append("## Run Contract")
    lines.append(f"- run_dir: {registry_run.get('run_dir', '')}")
    lines.append(f"- recommended_price_mode: {execution.get('recommended_price_mode', '')}")
    lines.append(f"- recommended_timeout_bars: {execution.get('recommended_timeout_bars', '')}")
    lines.append(f"- recommended_replace_max: {execution.get('recommended_replace_max', '')}")
    lines.append(f"- trade_action_status: {trade_action.get('status', '')}")
    lines.append(f"- risk_control_status: {risk_control.get('status', '')}")
    lines.append(
        f"- risk_control_live_gate_enabled: {bool((risk_control.get('live_gate') or {}).get('enabled', False))}"
    )
    lines.append(f"- trainer_evidence_pass: {trainer_evidence.get('pass', False)}")
    lines.append(f"- promotion_mode: {promotion.get('promotion_mode', '')}")
    lines.append(f"- promoted: {promotion.get('promote', False)}")
    lines.append("")
    lines.append("## Trade Journal")
    for row in trade_journal_summary.get("by_status", []):
        lines.append(
            "- "
            f"status={row.get('status', '')} n={row.get('count', 0)} "
            f"realized_pnl_quote={row.get('realized_pnl_quote_total', 0.0)} "
            f"avg_realized_pnl_pct={row.get('avg_realized_pnl_pct', 0.0)}"
        )
    lines.append("")
    lines.append("## Execution Attempts")
    lines.append(f"- attempts_total: {attempt_summary.get('attempts_total', 0)}")
    lines.append(f"- filled_count: {attempt_summary.get('filled_count', 0)}")
    lines.append(f"- missed_count: {attempt_summary.get('missed_count', 0)}")
    lines.append(f"- fill_rate: {attempt_summary.get('fill_rate', 0.0)}")
    lines.append(f"- recommended_price_mode_match_rate: {attempt_summary.get('recommended_price_mode_match_rate', 0.0)}")
    lines.append(f"- dominant_actual_price_mode: {attempt_summary.get('dominant_actual_price_mode', '')}")
    lines.append("")
    for row in attempt_summary.get("by_price_mode", []):
        lines.append(
            "- "
            f"price_mode={row.get('price_mode', '')} attempts={row.get('attempts', 0)} "
            f"fills={row.get('fills', 0)} misses={row.get('misses', 0)} "
            f"miss_rate={row.get('miss_rate', 0.0)} "
            f"closed_trade_pnl_quote_total={row.get('closed_trade_realized_pnl_quote_total', 0.0)}"
        )
    lines.append("")
    lines.append("## Override Trace")
    lines.append(f"- trace_rows_available: {override_summary.get('trace_rows_available', 0)}")
    lines.append(f"- final_submit_passive_maker_count: {override_summary.get('final_submit_passive_maker_count', 0)}")
    lines.append(f"- final_submit_join_count: {override_summary.get('final_submit_join_count', 0)}")
    lines.append(f"- operational_demote_to_passive_maker_count: {override_summary.get('operational_demote_to_passive_maker_count', 0)}")
    lines.append(f"- micro_policy_demote_to_passive_maker_count: {override_summary.get('micro_policy_demote_to_passive_maker_count', 0)}")
    lines.append(f"- execution_policy_demote_to_passive_maker_count: {override_summary.get('execution_policy_demote_to_passive_maker_count', 0)}")
    for row in override_summary.get("stage_transition_counts", []):
        lines.append(
            "- "
            f"transition={row.get('transition', '')} count={row.get('count', 0)}"
        )
    lines.append("")
    lines.append("## Breakers")
    lines.append(f"- live_breaker_active: {breaker_summary.get('live_breaker_active', False)}")
    lines.append(f"- rollout_start_allowed: {breaker_summary.get('rollout_start_allowed', False)}")
    lines.append(f"- rollout_order_emission_allowed: {breaker_summary.get('rollout_order_emission_allowed', False)}")
    for row in breaker_summary.get("reason_code_counts", []):
        lines.append(f"- reason_code={row.get('reason_code', '')} count={row.get('count', 0)}")
    lines.append("")
    lines.append("## Findings")
    for row in payload.get("findings", []):
        lines.append(f"- {row.get('code', '')}: {row.get('message', '')}")
    lines.append("")
    lines.append("## Recent Missed Examples")
    for row in (payload.get("examples") or {}).get("recent_missed_attempts", []):
        lines.append(
            "- "
            f"market={row.get('market', '')} actual_price_mode={row.get('price_mode', '')} "
            f"trace={row.get('trace_initial_price_mode', '')}->{row.get('trace_after_operational_price_mode', '')}->{row.get('trace_after_micro_policy_price_mode', '')}->{row.get('trace_final_submit_price_mode', '')} "
            f"expected_net_edge_bps={row.get('expected_net_edge_bps', '')} "
            f"micro_quality_score={row.get('micro_quality_score', '')} "
            f"submitted_ts_ms={row.get('submitted_ts_ms', '')}"
        )
    lines.append("")
    lines.append("## Recent Losing Closed Trades")
    for row in (payload.get("examples") or {}).get("recent_closed_losses", []):
        lines.append(
            "- "
            f"market={row.get('market', '')} pnl_quote={row.get('journal_realized_pnl_quote', '')} "
            f"pnl_pct={row.get('journal_realized_pnl_pct', '')} "
            f"expected_net_edge_bps={row.get('expected_net_edge_bps', '')} "
            f"price_mode={row.get('price_mode', '')} "
            f"trace={row.get('trace_initial_price_mode', '')}->{row.get('trace_after_operational_price_mode', '')}->{row.get('trace_after_micro_policy_price_mode', '')}->{row.get('trace_final_submit_price_mode', '')}"
        )
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _resolve_model_reference(
    *,
    db_path: Path,
    registry_root: Path,
    model_family: str,
    model_ref: str | None,
    runtime_contract_checkpoint: dict[str, Any] | None,
) -> dict[str, Any]:
    requested_model_ref = str(model_ref or "").strip()
    if requested_model_ref:
        resolved_model_ref = requested_model_ref
        resolved_source = "cli"
    else:
        runtime_payload = dict((runtime_contract_checkpoint or {}).get("payload") or {})
        runtime_run_id = str(runtime_payload.get("live_runtime_model_run_id", "")).strip()
        if runtime_run_id:
            resolved_model_ref = runtime_run_id
            resolved_source = "db_checkpoint:live_runtime_contract"
        elif "candidate" in str(db_path).lower():
            resolved_model_ref = "latest_candidate"
            resolved_source = "db_path_default:latest_candidate"
        else:
            resolved_model_ref = "champion"
            resolved_source = "db_path_default:champion"

    run_dir = resolve_run_dir(
        registry_root=registry_root,
        model_ref=resolved_model_ref,
        model_family=model_family,
    )
    return {
        "requested_model_ref": requested_model_ref,
        "resolved_model_ref": str(run_dir.name),
        "resolved_model_ref_source": resolved_source,
        "model_family": model_family,
        "run_dir": str(run_dir),
    }


def _load_attempt_rows(
    *,
    db_path: Path,
    since_ts_ms: int,
    limit: int,
) -> list[dict[str, Any]]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT
            e.*,
            t.status AS journal_status,
            t.realized_pnl_quote AS journal_realized_pnl_quote,
            t.realized_pnl_pct AS journal_realized_pnl_pct,
            t.close_reason_code AS journal_close_reason_code,
            t.close_mode AS journal_close_mode,
            t.entry_meta_json AS journal_entry_meta_json
        FROM execution_attempts e
        LEFT JOIN trade_journal t ON e.journal_id = t.journal_id
        WHERE e.final_state IS NOT NULL
          AND e.submitted_ts_ms >= ?
        ORDER BY e.submitted_ts_ms DESC, e.updated_ts DESC
        LIMIT ?
        """,
        (int(since_ts_ms), int(limit)),
    ).fetchall()
    conn.close()
    return [_normalize_row(dict(row)) for row in rows]


def _load_trade_journal_summary(
    *,
    db_path: Path,
    since_ts_ms: int,
) -> dict[str, Any]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT
            status,
            COUNT(*) AS count,
            AVG(realized_pnl_pct) AS avg_realized_pnl_pct,
            SUM(COALESCE(realized_pnl_quote, 0)) AS realized_pnl_quote_total
        FROM trade_journal
        WHERE COALESCE(exit_ts_ms, entry_filled_ts_ms, entry_submitted_ts_ms, updated_ts) >= ?
        GROUP BY status
        ORDER BY count DESC, status
        """,
        (int(since_ts_ms),),
    ).fetchall()
    conn.close()
    return {
        "by_status": [
            {
                "status": str(row["status"]),
                "count": int(row["count"]),
                "avg_realized_pnl_pct": _round_or_none(row["avg_realized_pnl_pct"]),
                "realized_pnl_quote_total": _round_or_none(row["realized_pnl_quote_total"], digits=6),
            }
            for row in rows
        ]
    }


def _load_breaker_events(
    *,
    db_path: Path,
    since_ts_ms: int,
    limit: int,
) -> list[dict[str, Any]]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT breaker_key, event_kind, action, source, reason_codes_json, details_json, ts_ms
        FROM breaker_events
        WHERE ts_ms >= ?
        ORDER BY ts_ms DESC
        LIMIT ?
        """,
        (int(since_ts_ms), int(limit)),
    ).fetchall()
    conn.close()
    payloads: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        payload["reason_codes"] = _safe_json_load(payload.pop("reason_codes_json"), default=[])
        payload["details"] = _safe_json_load(payload.pop("details_json"), default={})
        payloads.append(annotate_reason_payload(payload, reason_codes=payload.get("reason_codes") or []))
    return payloads


def _load_breaker_state(*, db_path: Path) -> list[dict[str, Any]]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT breaker_key, active, action, source, reason_codes_json, details_json, updated_ts, armed_ts
        FROM breaker_state
        ORDER BY breaker_key
        """
    ).fetchall()
    conn.close()
    payloads: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        payload["reason_codes"] = _safe_json_load(payload.pop("reason_codes_json"), default=[])
        payload["details"] = _safe_json_load(payload.pop("details_json"), default={})
        payload["active"] = bool(payload.get("active"))
        payloads.append(annotate_reason_payload(payload, reason_codes=payload.get("reason_codes") or []))
    return payloads


def _load_checkpoint(db_path: Path, name: str) -> dict[str, Any] | None:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT name, ts_ms, payload_json FROM checkpoints WHERE name = ?",
        (str(name),),
    ).fetchone()
    conn.close()
    if row is None:
        return None
    payload = dict(row)
    payload["payload"] = _safe_json_load(payload.pop("payload_json"), default={})
    return payload


def _summarize_attempts(
    *,
    attempts: list[dict[str, Any]],
    recommended_price_mode: str,
) -> dict[str, Any]:
    total = len(attempts)
    filled_count = sum(1 for item in attempts if str(item.get("final_state", "")).upper() == "FILLED")
    missed_count = sum(1 for item in attempts if str(item.get("final_state", "")).upper() == "MISSED")
    partial_cancelled_count = sum(
        1 for item in attempts if str(item.get("final_state", "")).upper() == "PARTIAL_CANCELLED"
    )
    match_count = sum(
        1
        for item in attempts
        if recommended_price_mode
        and str(item.get("price_mode", "")).strip().upper() == recommended_price_mode
    )
    by_price_mode = _summarize_dimension(
        attempts=attempts,
        key_name="price_mode",
    )
    by_action_code = _summarize_dimension(
        attempts=attempts,
        key_name="action_code",
    )
    by_price_mode_and_state = _summarize_dimension_pairs(
        attempts=attempts,
        left_key="price_mode",
        right_key="final_state",
    )
    dominant_actual_price_mode = by_price_mode[0]["price_mode"] if by_price_mode else ""
    positive_edge_losses = [
        item
        for item in attempts
        if _safe_optional_float(item.get("journal_realized_pnl_quote")) is not None
        and float(item.get("journal_realized_pnl_quote") or 0.0) < 0.0
        and (_safe_optional_float(item.get("expected_net_edge_bps")) or 0.0) > 0.0
    ]
    positive_edge_missed = [
        item
        for item in attempts
        if str(item.get("final_state", "")).strip().upper() == "MISSED"
        and (_safe_optional_float(item.get("expected_net_edge_bps")) or 0.0) > 0.0
    ]
    return {
        "attempts_total": int(total),
        "filled_count": int(filled_count),
        "missed_count": int(missed_count),
        "partial_cancelled_count": int(partial_cancelled_count),
        "fill_rate": _round_or_none(filled_count / float(total), digits=6) if total > 0 else 0.0,
        "recommended_price_mode": recommended_price_mode,
        "recommended_price_mode_match_count": int(match_count),
        "recommended_price_mode_match_rate": _round_or_none(match_count / float(total), digits=6) if total > 0 else None,
        "dominant_actual_price_mode": dominant_actual_price_mode,
        "by_price_mode": by_price_mode,
        "by_action_code": by_action_code,
        "by_price_mode_and_state": by_price_mode_and_state,
        "positive_expected_net_edge_closed_losses": {
            "count": int(len(positive_edge_losses)),
            "ratio": _round_or_none(len(positive_edge_losses) / float(total), digits=6) if total > 0 else None,
        },
        "positive_expected_net_edge_missed_attempts": {
            "count": int(len(positive_edge_missed)),
            "ratio": _round_or_none(len(positive_edge_missed) / float(total), digits=6) if total > 0 else None,
        },
    }


def _summarize_dimension(
    *,
    attempts: list[dict[str, Any]],
    key_name: str,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in attempts:
        key = str(item.get(key_name, "")).strip().upper() or "UNKNOWN"
        grouped[key].append(item)

    rows: list[dict[str, Any]] = []
    for key, items in grouped.items():
        total = len(items)
        fills = sum(1 for item in items if str(item.get("final_state", "")).upper() == "FILLED")
        misses = sum(1 for item in items if str(item.get("final_state", "")).upper() == "MISSED")
        partial_cancelled = sum(
            1 for item in items if str(item.get("final_state", "")).upper() == "PARTIAL_CANCELLED"
        )
        closed_trade_values = [
            float(item.get("journal_realized_pnl_quote") or 0.0)
            for item in items
            if str(item.get("journal_status", "")).strip().upper() == "CLOSED"
            and _safe_optional_float(item.get("journal_realized_pnl_quote")) is not None
        ]
        rows.append(
            {
                key_name: key,
                "attempts": int(total),
                "fills": int(fills),
                "misses": int(misses),
                "partial_cancelled": int(partial_cancelled),
                "fill_rate": _round_or_none(fills / float(total), digits=6) if total > 0 else None,
                "miss_rate": _round_or_none(misses / float(total), digits=6) if total > 0 else None,
                "avg_fill_fraction": _mean_optional(items, "fill_fraction"),
                "avg_shortfall_bps": _mean_optional(items, "shortfall_bps"),
                "avg_expected_edge_bps": _mean_optional(items, "expected_edge_bps"),
                "avg_expected_net_edge_bps": _mean_optional(items, "expected_net_edge_bps"),
                "avg_micro_quality_score": _mean_optional(items, "micro_quality_score"),
                "closed_trade_count": int(len(closed_trade_values)),
                "closed_trade_realized_pnl_quote_total": _round_or_none(sum(closed_trade_values), digits=6)
                if closed_trade_values
                else 0.0,
                "closed_trade_realized_pnl_quote_mean": _round_or_none(
                    sum(closed_trade_values) / float(len(closed_trade_values)),
                    digits=6,
                )
                if closed_trade_values
                else None,
            }
        )
    rows.sort(key=lambda item: (-int(item["attempts"]), str(item[key_name])))
    return rows


def _summarize_dimension_pairs(
    *,
    attempts: list[dict[str, Any]],
    left_key: str,
    right_key: str,
) -> list[dict[str, Any]]:
    counts: Counter[tuple[str, str]] = Counter()
    fill_fraction_totals: defaultdict[tuple[str, str], float] = defaultdict(float)
    for item in attempts:
        left = str(item.get(left_key, "")).strip().upper() or "UNKNOWN"
        right = str(item.get(right_key, "")).strip().upper() or "UNKNOWN"
        counts[(left, right)] += 1
        fill_fraction = _safe_optional_float(item.get("fill_fraction"))
        if fill_fraction is not None:
            fill_fraction_totals[(left, right)] += float(fill_fraction)

    rows: list[dict[str, Any]] = []
    for (left, right), count in sorted(counts.items(), key=lambda item: (-item[1], item[0][0], item[0][1])):
        avg_fill_fraction = fill_fraction_totals[(left, right)] / float(count) if count > 0 else None
        rows.append(
            {
                left_key: left,
                right_key: right,
                "count": int(count),
                "avg_fill_fraction": _round_or_none(avg_fill_fraction, digits=6),
            }
        )
    return rows


def _summarize_execution_overrides(
    *,
    attempts: list[dict[str, Any]],
    recommended_price_mode: str,
) -> dict[str, Any]:
    trace_rows = [
        item
        for item in attempts
        if isinstance((item.get("outcome") or {}).get("execution_trace"), dict)
        or isinstance(item.get("journal_entry_meta"), dict)
    ]
    transition_counter: Counter[str] = Counter()
    operational_demote = 0
    micro_policy_demote = 0
    execution_policy_demote = 0
    final_passive = 0
    final_join = 0

    for item in trace_rows:
        trace = dict((item.get("outcome") or {}).get("execution_trace") or {})
        entry_meta = dict(item.get("journal_entry_meta") or {})
        initial_mode = (
            _trace_price_mode(trace.get("initial_exec_profile"))
            or str(((entry_meta.get("execution_trace") or {}).get("run_recommended_price_mode", ""))).strip().upper()
            or str(item.get("run_recommended_price_mode", "")).strip().upper()
            or str(recommended_price_mode).strip().upper()
        )
        after_operational = (
            _trace_price_mode(trace.get("after_operational_overlay"))
            or _trace_price_mode(((entry_meta.get("execution_trace") or {}).get("after_operational_overlay")))
            or _trace_price_mode(((entry_meta.get("execution_trace") or {}).get("after_strategy_exec_profile")))
        )
        after_micro = (
            _trace_price_mode(trace.get("after_micro_order_policy"))
            or _trace_price_mode(((entry_meta.get("execution_trace") or {}).get("after_micro_order_policy")))
        )
        execution_policy = dict(trace.get("execution_policy") or {})
        if not execution_policy:
            execution_policy = dict((entry_meta.get("execution_trace") or {}).get("execution_policy") or {})
        if not execution_policy:
            execution_policy = dict(entry_meta.get("execution_policy") or {})
        selected_policy_mode = str(execution_policy.get("selected_price_mode", "")).strip().upper()
        final_submit = dict(trace.get("final_submit") or {})
        if not final_submit:
            final_submit = dict((entry_meta.get("execution_trace") or {}).get("final_submit") or {})
        final_submit_mode = (
            str(final_submit.get("submit_price_mode", "")).strip().upper()
            or str(((entry_meta.get("execution") or {}).get("exec_profile") or {}).get("price_mode", "")).strip().upper()
            or str(item.get("price_mode", "")).strip().upper()
        )

        transition_counter[f"initial:{initial_mode or 'UNKNOWN'}"] += 1
        if after_operational:
            transition_counter[f"operational:{initial_mode or 'UNKNOWN'}->{after_operational}"] += 1
        if after_micro:
            transition_counter[f"micro_policy:{after_operational or initial_mode or 'UNKNOWN'}->{after_micro}"] += 1
        if selected_policy_mode:
            transition_counter[f"execution_policy:{after_micro or after_operational or initial_mode or 'UNKNOWN'}->{selected_policy_mode}"] += 1
        if final_submit_mode:
            transition_counter[f"final_submit:{selected_policy_mode or after_micro or after_operational or initial_mode or 'UNKNOWN'}->{final_submit_mode}"] += 1

        if initial_mode and after_operational == "PASSIVE_MAKER" and initial_mode != "PASSIVE_MAKER":
            operational_demote += 1
        if (after_operational or initial_mode) and after_micro == "PASSIVE_MAKER" and (after_operational or initial_mode) != "PASSIVE_MAKER":
            micro_policy_demote += 1
        if (after_micro or after_operational or initial_mode) and selected_policy_mode == "PASSIVE_MAKER" and (after_micro or after_operational or initial_mode) != "PASSIVE_MAKER":
            execution_policy_demote += 1
        if final_submit_mode == "PASSIVE_MAKER":
            final_passive += 1
        if final_submit_mode == "JOIN":
            final_join += 1

    return {
        "recommended_price_mode": recommended_price_mode,
        "trace_rows_available": int(len(trace_rows)),
        "operational_demote_to_passive_maker_count": int(operational_demote),
        "micro_policy_demote_to_passive_maker_count": int(micro_policy_demote),
        "execution_policy_demote_to_passive_maker_count": int(execution_policy_demote),
        "final_submit_passive_maker_count": int(final_passive),
        "final_submit_join_count": int(final_join),
        "stage_transition_counts": [
            {"transition": transition, "count": int(count)}
            for transition, count in sorted(transition_counter.items(), key=lambda item: (-item[1], item[0]))
        ][:20],
    }


def _summarize_breakers(
    *,
    breaker_events: list[dict[str, Any]],
    breaker_state: list[dict[str, Any]],
    rollout_status_checkpoint: dict[str, Any] | None,
) -> dict[str, Any]:
    reason_counter: Counter[str] = Counter()
    source_counter: Counter[str] = Counter()
    type_counter: Counter[str] = Counter()
    for event in breaker_events:
        source_counter[str(event.get("source", "")).strip() or "UNKNOWN"] += 1
        for reason_code in event.get("reason_codes") or []:
            normalized = str(reason_code).strip().upper()
            if normalized:
                reason_counter[normalized] += 1
        for reason_type in event.get("reason_types") or []:
            normalized_type = str(reason_type).strip().upper()
            if normalized_type:
                type_counter[normalized_type] += 1

    active_live_breaker = False
    active_live_reasons: list[str] = []
    active_live_reason_types: list[str] = []
    active_live_primary_reason_type: str | None = None
    active_live_typed_reason_codes: list[dict[str, Any]] = []
    for row in breaker_state:
        if str(row.get("breaker_key", "")).strip().lower() != "live":
            continue
        if bool(row.get("active")):
            active_live_breaker = True
            active_live_reasons = [str(item).strip() for item in (row.get("reason_codes") or []) if str(item).strip()]
            active_live_reason_types = [str(item).strip() for item in (row.get("reason_types") or []) if str(item).strip()]
            active_live_primary_reason_type = str(row.get("primary_reason_type") or "").strip() or None
            active_live_typed_reason_codes = list(row.get("typed_reason_codes") or [])
            break

    rollout_payload = dict((rollout_status_checkpoint or {}).get("payload") or {})
    rollout_status = dict(rollout_payload.get("status") or {})
    return {
        "live_breaker_active": bool(active_live_breaker),
        "live_breaker_reason_codes": active_live_reasons,
        "live_breaker_reason_types": active_live_reason_types,
        "live_breaker_primary_reason_type": active_live_primary_reason_type,
        "live_breaker_typed_reason_codes": active_live_typed_reason_codes,
        "rollout_mode": str(rollout_status.get("mode", "")).strip(),
        "rollout_start_allowed": bool(rollout_status.get("start_allowed", False)),
        "rollout_order_emission_allowed": bool(rollout_status.get("order_emission_allowed", False)),
        "rollout_reason_codes": [
            str(item).strip() for item in (rollout_status.get("reason_codes") or []) if str(item).strip()
        ],
        "reason_type_counts": [
            {"reason_type": reason_type, "count": int(count)}
            for reason_type, count in sorted(type_counter.items(), key=lambda item: (-item[1], item[0]))
        ],
        "reason_code_counts": [
            {"reason_code": code, "count": int(count)}
            for code, count in sorted(reason_counter.items(), key=lambda item: (-item[1], item[0]))
        ],
        "source_counts": [
            {"source": source, "count": int(count)}
            for source, count in sorted(source_counter.items(), key=lambda item: (-item[1], item[0]))
        ],
        "recent_events": breaker_events[:12],
    }


def _build_examples(
    *,
    attempts: list[dict[str, Any]],
    recommended_price_mode: str,
    example_limit: int,
) -> dict[str, Any]:
    recent_missed = [
        _example_attempt_payload(item)
        for item in attempts
        if str(item.get("final_state", "")).strip().upper() == "MISSED"
    ][:example_limit]
    recent_closed_losses = [
        _example_attempt_payload(item)
        for item in attempts
        if str(item.get("journal_status", "")).strip().upper() == "CLOSED"
        and float(item.get("journal_realized_pnl_quote") or 0.0) < 0.0
    ][:example_limit]
    recent_mode_mismatches = [
        _example_attempt_payload(item)
        for item in attempts
        if recommended_price_mode
        and str(item.get("price_mode", "")).strip().upper() != recommended_price_mode
    ][:example_limit]
    return {
        "recent_missed_attempts": recent_missed,
        "recent_closed_losses": recent_closed_losses,
        "recent_mode_mismatches": recent_mode_mismatches,
    }


def _build_findings(payload: dict[str, Any]) -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []
    registry_run = dict(payload.get("registry_run") or {})
    execution_summary = dict(payload.get("execution_attempt_summary") or {})
    breaker_summary = dict(payload.get("breaker_summary") or {})
    trainer_evidence = dict(registry_run.get("trainer_research_evidence") or {})
    promotion = dict(registry_run.get("promotion_decision") or {})
    execution_contract = dict(((registry_run.get("runtime_recommendations") or {}).get("execution")) or {})

    if not bool(trainer_evidence.get("pass", False)):
        findings.append(
            {
                "code": "TRAINER_EVIDENCE_NOT_PASSING",
                "message": "Current champion run does not pass trainer research evidence, so champion status should not be treated as fully validated research status.",
            }
        )
    if (
        bool(promotion.get("promote", False))
        and str(promotion.get("promotion_mode", "")).strip().lower() == "manual"
        and not bool(trainer_evidence.get("pass", False))
    ):
        findings.append(
            {
                "code": "MANUAL_PROMOTION_OVERRIDES_RESEARCH_EVIDENCE",
                "message": "Champion pointer appears to have been advanced through manual promotion after weak trainer evidence.",
            }
        )

    match_rate = _safe_optional_float(execution_summary.get("recommended_price_mode_match_rate")) or 0.0
    if execution_contract.get("recommended_price_mode") and match_rate < 0.60:
        findings.append(
            {
                "code": "LIVE_PRICE_MODE_DIVERGES_FROM_RUN_RECOMMENDATION",
                "message": f"Observed live price_mode matches the run recommendation only {match_rate:.1%} of the time.",
            }
        )

    missed_count = _safe_optional_float(execution_summary.get("missed_count"))
    attempts_total = _safe_optional_float(execution_summary.get("attempts_total"))
    if missed_count is not None and attempts_total and attempts_total > 0:
        ratio = float(missed_count) / float(attempts_total)
        if ratio >= 0.20:
            findings.append(
                {
                    "code": "LIVE_MISS_RATE_HIGH",
                    "message": f"Recent final execution attempts have a high miss rate of {ratio:.1%}.",
                }
            )

    losses = dict(execution_summary.get("positive_expected_net_edge_closed_losses") or {})
    if int(losses.get("count", 0) or 0) > 0:
        findings.append(
            {
                "code": "EXPECTED_EDGE_NOT_REALIZED",
                "message": "There are closed losing trades with positive expected_net_edge_bps at entry, so execution/runtime conversion is degrading modeled edge.",
            }
        )

    missed_positive_edge = dict(execution_summary.get("positive_expected_net_edge_missed_attempts") or {})
    if int(missed_positive_edge.get("count", 0) or 0) > 0:
        findings.append(
            {
                "code": "POSITIVE_EDGE_ENTRIES_ARE_BEING_MISSED",
                "message": "Recent missed entries still show positive expected_net_edge_bps, which points to execution posture or fill realism issues rather than pure alpha absence.",
            }
        )

    if bool(breaker_summary.get("live_breaker_active", False)) or not bool(
        breaker_summary.get("rollout_order_emission_allowed", True)
    ):
        findings.append(
            {
                "code": "BREAKER_OR_ROLLOUT_SUPPRESSES_NEW_INTENTS",
                "message": "Current breaker/rollout state is suppressing live order emission, which can distort observed runtime behavior.",
            }
        )

    if not bool(((registry_run.get("runtime_recommendations") or {}).get("risk_control") or {}).get("live_gate", {}).get("enabled", False)):
        findings.append(
            {
                "code": "RISK_CONTROL_LIVE_GATE_DISABLED_BY_DESIGN",
                "message": "The runtime risk-control contract is present, but the static live gate is disabled by design, so only part of the contract is enforced before submit.",
            }
        )

    if not findings:
        findings.append(
            {
                "code": "NO_HIGH_SEVERITY_FINDINGS",
                "message": "No high-severity execution override findings were detected in the selected lookback window.",
            }
        )
    return findings


def _summarize_execution_policy_checkpoint(payload: dict[str, Any] | None) -> dict[str, Any]:
    doc = dict((payload or {}).get("payload") or {})
    execution_contract = dict(doc.get("execution_contract") or {})
    fill_model = dict(execution_contract.get("fill_model") or doc.get("model") or {})
    return {
        "present": bool(payload),
        "ts_ms": int((payload or {}).get("ts_ms", 0) or 0) if payload else 0,
        "policy": str(doc.get("policy", "")).strip(),
        "rows_total": int(doc.get("rows_total", execution_contract.get("rows_total", 0)) or 0),
        "contract_status": str(execution_contract.get("status", fill_model.get("status", ""))).strip(),
    }


def _summarize_live_execution_contract(payload: dict[str, Any]) -> dict[str, Any]:
    fill_model = dict(payload.get("fill_model") or {})
    action_stats = dict(fill_model.get("action_stats") or {})
    summarized_actions: list[dict[str, Any]] = []
    for action_code, row in sorted(action_stats.items()):
        summarized_actions.append(
            {
                "action_code": str(action_code),
                "sample_count": int(row.get("sample_count", 0) or 0),
                "mean_time_to_first_fill_ms": _round_or_none(row.get("mean_time_to_first_fill_ms")),
                "mean_shortfall_bps": _round_or_none(row.get("mean_shortfall_bps")),
                "p_fill_within_1000ms": _round_or_none(row.get("p_fill_within_1000ms")),
                "p_fill_within_3000ms": _round_or_none(row.get("p_fill_within_3000ms")),
                "p_fill_within_10000ms": _round_or_none(row.get("p_fill_within_10000ms")),
            }
        )
    return {
        "policy": str(payload.get("policy", "")).strip(),
        "status": str(payload.get("status", "")).strip(),
        "rows_total": int(payload.get("rows_total", 0) or 0),
        "action_stats": summarized_actions,
    }


def _execution_contract_attempt_row(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "action_code": item.get("action_code"),
        "price_mode": item.get("price_mode"),
        "trade_coverage_ms": item.get("trade_coverage_ms"),
        "book_coverage_ms": item.get("book_coverage_ms"),
        "snapshot_age_ms": item.get("snapshot_age_ms"),
        "spread_bps": item.get("spread_bps"),
        "depth_top5_notional_krw": item.get("depth_top5_notional_krw"),
        "expected_edge_bps": item.get("expected_edge_bps"),
        "expected_net_edge_bps": item.get("expected_net_edge_bps"),
        "submitted_ts_ms": item.get("submitted_ts_ms"),
        "first_fill_ts_ms": item.get("first_fill_ts_ms"),
        "shortfall_bps": item.get("shortfall_bps"),
        "final_state": item.get("final_state"),
    }


def _example_attempt_payload(item: dict[str, Any]) -> dict[str, Any]:
    trace = dict(((item.get("outcome") or {}).get("execution_trace") or {}))
    entry_meta = dict(item.get("journal_entry_meta") or {})
    entry_trace = dict((entry_meta.get("execution_trace") or {}))
    return {
        "market": str(item.get("market", "")).strip().upper(),
        "action_code": str(item.get("action_code", "")).strip().upper(),
        "price_mode": str(item.get("price_mode", "")).strip().upper(),
        "final_state": str(item.get("final_state", "")).strip().upper(),
        "submitted_ts_ms": _safe_optional_int(item.get("submitted_ts_ms")),
        "final_ts_ms": _safe_optional_int(item.get("final_ts_ms")),
        "fill_fraction": _round_or_none(item.get("fill_fraction")),
        "shortfall_bps": _round_or_none(item.get("shortfall_bps")),
        "expected_edge_bps": _round_or_none(item.get("expected_edge_bps")),
        "expected_net_edge_bps": _round_or_none(item.get("expected_net_edge_bps")),
        "micro_quality_score": _round_or_none(item.get("micro_quality_score")),
        "journal_status": str(item.get("journal_status", "")).strip().upper(),
        "journal_realized_pnl_quote": _round_or_none(item.get("journal_realized_pnl_quote"), digits=6),
        "journal_realized_pnl_pct": _round_or_none(item.get("journal_realized_pnl_pct")),
        "journal_close_reason_code": str(item.get("journal_close_reason_code", "")).strip().upper(),
        "journal_close_mode": str(item.get("journal_close_mode", "")).strip(),
        "trace_initial_price_mode": (
            _trace_price_mode(trace.get("initial_exec_profile"))
            or str(entry_trace.get("run_recommended_price_mode", "")).strip().upper()
        ),
        "trace_after_operational_price_mode": (
            _trace_price_mode(trace.get("after_operational_overlay"))
            or _trace_price_mode(entry_trace.get("after_operational_overlay"))
        ),
        "trace_after_micro_policy_price_mode": (
            _trace_price_mode(trace.get("after_micro_order_policy"))
            or _trace_price_mode(entry_trace.get("after_micro_order_policy"))
        ),
        "trace_final_submit_price_mode": (
            str((trace.get("final_submit") or {}).get("submit_price_mode", "")).strip().upper()
            or str((entry_trace.get("final_submit") or {}).get("submit_price_mode", "")).strip().upper()
            or str(((entry_meta.get("execution") or {}).get("exec_profile") or {}).get("price_mode", "")).strip().upper()
        ),
    }


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _mean_optional(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [_safe_optional_float(item.get(key)) for item in rows]
    filtered = [float(item) for item in values if item is not None]
    if not filtered:
        return None
    return _round_or_none(sum(filtered) / float(len(filtered)), digits=6)


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = {str(key): row[key] for key in row}
    if "outcome_json" in normalized:
        normalized["outcome"] = _safe_json_load(normalized.get("outcome_json"), default={})
    if "journal_entry_meta_json" in normalized:
        normalized["journal_entry_meta"] = _safe_json_load(normalized.get("journal_entry_meta_json"), default={})
    return normalized


def _safe_json_load(text: Any, *, default: Any) -> Any:
    if text in {None, ""}:
        return default
    try:
        payload = json.loads(str(text))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default
    return payload


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


def _round_or_none(value: Any, *, digits: int = 6) -> float | None:
    parsed = _safe_optional_float(value)
    if parsed is None:
        return None
    return round(float(parsed), int(digits))


def _trace_price_mode(payload: Any) -> str:
    if isinstance(payload, dict):
        return str(payload.get("price_mode", payload.get("output_price_mode", ""))).strip().upper()
    return ""
