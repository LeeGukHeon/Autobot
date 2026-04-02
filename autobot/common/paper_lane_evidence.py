from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from statistics import fmean
from typing import Any


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _weighted_mean(pairs: list[tuple[float, float]], default: float = 0.0) -> float:
    weighted_total = 0.0
    weight_total = 0.0
    for value, weight in pairs:
        w = max(float(weight), 0.0)
        if w <= 0.0:
            continue
        weighted_total += float(value) * w
        weight_total += w
    if weight_total <= 0.0:
        return default
    return weighted_total / weight_total


def load_paper_lane_runs(
    *,
    paper_root: Path,
    lane: str,
    runtime_role: str,
    since_ts_ms: int = 0,
    until_ts_ms: int | None = None,
    model_ref: str | None = None,
    model_run_id: str | None = None,
) -> list[dict[str, Any]]:
    lane_value = str(lane).strip().lower()
    role_value = str(runtime_role).strip().lower()
    model_ref_value = str(model_ref).strip() if model_ref else ""
    model_run_id_value = str(model_run_id).strip() if model_run_id else ""
    results: list[dict[str, Any]] = []
    runs_root = paper_root / "runs"
    if not runs_root.exists():
        return results
    for summary_path in runs_root.glob("paper-*/summary.json"):
        summary = load_paper_lane_run_summary(run_dir=summary_path.parent)
        if not summary:
            continue
        if str(summary.get("paper_lane", "")).strip().lower() != lane_value:
            continue
        if str(summary.get("paper_runtime_role", "")).strip().lower() != role_value:
            continue
        run_started_ts_ms = _safe_int(summary.get("run_started_ts_ms"))
        if run_started_ts_ms < int(since_ts_ms):
            continue
        if until_ts_ms is not None and run_started_ts_ms > int(until_ts_ms):
            continue
        if model_ref_value:
            run_model_ref = str(summary.get("paper_runtime_model_ref_pinned") or summary.get("paper_runtime_model_ref") or "").strip()
            if run_model_ref != model_ref_value:
                continue
        if model_run_id_value:
            run_model_run_id = str(summary.get("paper_runtime_model_run_id") or "").strip()
            if run_model_run_id != model_run_id_value:
                continue
        results.append(summary)
    results.sort(key=lambda item: _safe_int(item.get("run_started_ts_ms")))
    return results


def load_paper_lane_run_summary(*, run_dir: Path) -> dict[str, Any]:
    summary_path = Path(run_dir) / "summary.json"
    try:
        summary = _load_json(summary_path)
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(summary, dict) or not summary:
        return {}
    summary["summary_path"] = str(summary_path)
    summary["run_dir"] = str(Path(run_dir))
    summary["decision_language"] = _load_decision_language_summary(run_dir=Path(run_dir))
    return summary


def aggregate_paper_lane_runs(runs: list[dict[str, Any]]) -> dict[str, Any]:
    if not runs:
        return {
            "runs_completed": 0,
            "run_ids": [],
            "orders_submitted": 0,
            "orders_filled": 0,
            "fill_rate": 0.0,
            "realized_pnl_quote_total": 0.0,
            "max_drawdown_pct_max": 0.0,
            "micro_quality_score_mean": 0.0,
            "rolling_nonnegative_active_window_ratio": 0.0,
            "rolling_positive_active_window_ratio": 0.0,
            "avg_time_to_fill_ms_mean": 0.0,
            "p50_time_to_fill_ms_mean": 0.0,
            "p90_time_to_fill_ms_mean": 0.0,
            "runtime_risk_multiplier_mean": 1.0,
            "operational_regime_score_mean": 0.0,
            "operational_breadth_ratio_mean": 0.0,
            "operational_max_positions_mean": 0.0,
            "execution_structure_closed_trade_count_total": 0,
            "execution_structure_payoff_ratio": 0.0,
            "execution_structure_market_loss_concentration_mean": 0.0,
            "execution_structure_tp_exit_share": 0.0,
            "execution_structure_sl_exit_share": 0.0,
            "execution_structure_timeout_exit_share": 0.0,
            "decision_language_primary_reason_counts": {},
            "decision_language_alpha_gate_fail_total": 0,
            "decision_language_safety_veto_total": 0,
            "decision_language_no_trade_total": 0,
            "decision_language_continuation_exit_total": 0,
            "decision_language_continuation_exit_share": 0.0,
            "decision_language_liquidation_tier_counts": {},
            "duration_sec_total": 0.0,
            "run_started_ts_ms_min": 0,
            "run_completed_ts_ms_max": 0,
        }

    orders_submitted = sum(_safe_int(item.get("orders_submitted")) for item in runs)
    orders_filled = sum(_safe_int(item.get("orders_filled")) for item in runs)
    realized_pnl_quote_total = sum(_safe_float(item.get("realized_pnl_quote")) for item in runs)
    max_drawdown_pct_max = max((_safe_float(item.get("max_drawdown_pct")) for item in runs), default=0.0)
    duration_sec_total = sum(_safe_float(item.get("duration_sec")) for item in runs)
    fill_rate = (orders_filled / orders_submitted) if orders_submitted > 0 else 0.0
    active_window_pairs = [
        (
            _safe_float(item.get("rolling_nonnegative_active_window_ratio")),
            max(_safe_int(item.get("rolling_active_windows")), 0),
        )
        for item in runs
    ]
    positive_window_pairs = [
        (
            _safe_float(item.get("rolling_positive_active_window_ratio")),
            max(_safe_int(item.get("rolling_active_windows")), 0),
        )
        for item in runs
    ]
    order_weight_pairs = [(item, max(_safe_int(item.get("orders_filled")), 1)) for item in runs]
    execution_structures = [
        dict(item.get("execution_structure") or {})
        for item in runs
        if isinstance(item.get("execution_structure"), dict)
    ]
    closed_trade_count_total = sum(_safe_int(item.get("closed_trade_count")) for item in execution_structures)
    win_pnl_quote_total = sum(_safe_float(item.get("win_pnl_quote_total")) for item in execution_structures)
    loss_pnl_quote_total_abs = sum(_safe_float(item.get("loss_pnl_quote_total_abs")) for item in execution_structures)
    execution_payoff_ratio = (
        win_pnl_quote_total / loss_pnl_quote_total_abs
        if loss_pnl_quote_total_abs > 0.0
        else (9999.0 if win_pnl_quote_total > 0.0 else 0.0)
    )
    tp_exit_count_total = sum(_safe_int(item.get("tp_exit_count")) for item in execution_structures)
    sl_exit_count_total = sum(_safe_int(item.get("sl_exit_count")) for item in execution_structures)
    timeout_exit_count_total = sum(_safe_int(item.get("timeout_exit_count")) for item in execution_structures)
    market_loss_concentration_mean = _weighted_mean(
        [
            (
                _safe_float(item.get("market_loss_concentration")),
                max(_safe_float(item.get("loss_pnl_quote_total_abs")), 0.0),
            )
            for item in execution_structures
        ],
        default=0.0,
    )
    primary_reason_counts: dict[str, int] = {}
    liquidation_tier_counts: dict[str, int] = {}
    alpha_gate_fail_total = 0
    safety_veto_total = 0
    no_trade_total = 0
    continuation_exit_total = 0
    for item in runs:
        decision_language = dict(item.get("decision_language") or {})
        for reason_code, count in dict(decision_language.get("primary_reason_counts") or {}).items():
            _inc_reason(primary_reason_counts, str(reason_code), int(count))
        for tier_name, count in dict(decision_language.get("liquidation_tier_counts") or {}).items():
            _inc_reason(liquidation_tier_counts, str(tier_name), int(count))
        alpha_gate_fail_total += int(decision_language.get("alpha_gate_fail_total") or 0)
        safety_veto_total += int(decision_language.get("safety_veto_total") or 0)
        no_trade_total += int(decision_language.get("no_trade_total") or 0)
        continuation_exit_total += int(decision_language.get("continuation_exit_total") or 0)
    aggregate = {
        "runs_completed": len(runs),
        "run_ids": [str(item.get("run_id", "")) for item in runs],
        "orders_submitted": orders_submitted,
        "orders_filled": orders_filled,
        "fill_rate": fill_rate,
        "realized_pnl_quote_total": realized_pnl_quote_total,
        "max_drawdown_pct_max": max_drawdown_pct_max,
        "micro_quality_score_mean": _weighted_mean(
            [(_safe_float(item.get("micro_quality_score_mean")), weight) for item, weight in order_weight_pairs],
            default=fmean([_safe_float(item.get("micro_quality_score_mean")) for item in runs]),
        ),
        "rolling_nonnegative_active_window_ratio": _weighted_mean(
            active_window_pairs,
            default=fmean([_safe_float(item.get("rolling_nonnegative_active_window_ratio")) for item in runs]),
        ),
        "rolling_positive_active_window_ratio": _weighted_mean(
            positive_window_pairs,
            default=fmean([_safe_float(item.get("rolling_positive_active_window_ratio")) for item in runs]),
        ),
        "avg_time_to_fill_ms_mean": _weighted_mean(
            [(_safe_float(item.get("avg_time_to_fill_ms")), weight) for item, weight in order_weight_pairs],
            default=fmean([_safe_float(item.get("avg_time_to_fill_ms")) for item in runs]),
        ),
        "p50_time_to_fill_ms_mean": _weighted_mean(
            [(_safe_float(item.get("p50_time_to_fill_ms")), weight) for item, weight in order_weight_pairs],
            default=fmean([_safe_float(item.get("p50_time_to_fill_ms")) for item in runs]),
        ),
        "p90_time_to_fill_ms_mean": _weighted_mean(
            [(_safe_float(item.get("p90_time_to_fill_ms")), weight) for item, weight in order_weight_pairs],
            default=fmean([_safe_float(item.get("p90_time_to_fill_ms")) for item in runs]),
        ),
        "runtime_risk_multiplier_mean": _weighted_mean(
            [(_safe_float(item.get("runtime_risk_multiplier_mean"), 1.0), weight) for item, weight in order_weight_pairs],
            default=1.0,
        ),
        "operational_regime_score_mean": _weighted_mean(
            [(_safe_float(item.get("operational_regime_score_mean")), weight) for item, weight in order_weight_pairs]
        ),
        "operational_breadth_ratio_mean": _weighted_mean(
            [(_safe_float(item.get("operational_breadth_ratio_mean")), weight) for item, weight in order_weight_pairs]
        ),
        "operational_max_positions_mean": _weighted_mean(
            [(_safe_float(item.get("operational_max_positions_mean")), weight) for item, weight in order_weight_pairs]
        ),
        "execution_structure_closed_trade_count_total": int(closed_trade_count_total),
        "execution_structure_payoff_ratio": float(max(execution_payoff_ratio, 0.0)),
        "execution_structure_market_loss_concentration_mean": float(
            max(min(market_loss_concentration_mean, 1.0), 0.0)
        ),
        "execution_structure_tp_exit_share": (tp_exit_count_total / closed_trade_count_total) if closed_trade_count_total > 0 else 0.0,
        "execution_structure_sl_exit_share": (sl_exit_count_total / closed_trade_count_total) if closed_trade_count_total > 0 else 0.0,
        "execution_structure_timeout_exit_share": (timeout_exit_count_total / closed_trade_count_total) if closed_trade_count_total > 0 else 0.0,
        "decision_language_primary_reason_counts": primary_reason_counts,
        "decision_language_alpha_gate_fail_total": int(alpha_gate_fail_total),
        "decision_language_safety_veto_total": int(safety_veto_total),
        "decision_language_no_trade_total": int(no_trade_total),
        "decision_language_continuation_exit_total": int(continuation_exit_total),
        "decision_language_continuation_exit_share": (
            float(continuation_exit_total) / float(closed_trade_count_total)
            if closed_trade_count_total > 0
            else 0.0
        ),
        "decision_language_liquidation_tier_counts": liquidation_tier_counts,
        "duration_sec_total": duration_sec_total,
        "run_started_ts_ms_min": min((_safe_int(item.get("run_started_ts_ms")) for item in runs), default=0),
        "run_completed_ts_ms_max": max((_safe_int(item.get("run_completed_ts_ms")) for item in runs), default=0),
    }
    return aggregate


def compare_champion_challenger(
    *,
    champion: dict[str, Any],
    challenger: dict[str, Any],
    min_challenger_hours: float,
    min_orders_filled: int,
    min_realized_pnl_quote: float,
    min_micro_quality_score: float,
    min_nonnegative_ratio: float,
    max_drawdown_deterioration_factor: float,
    micro_quality_tolerance: float,
    nonnegative_ratio_tolerance: float,
    max_time_to_fill_deterioration_factor: float = 1.25,
) -> dict[str, Any]:
    hard_failures: list[str] = []
    challenger_hours = max(_safe_float(challenger.get("duration_sec_total")) / 3600.0, 0.0)
    if challenger_hours < float(min_challenger_hours):
        hard_failures.append("INSUFFICIENT_CHALLENGER_HOURS")
    if _safe_int(challenger.get("orders_filled")) < int(min_orders_filled):
        hard_failures.append("MIN_ORDERS_FILLED_FAILED")
    if _safe_float(challenger.get("realized_pnl_quote_total")) < float(min_realized_pnl_quote):
        hard_failures.append("NEGATIVE_REALIZED_PNL")
    if _safe_float(challenger.get("micro_quality_score_mean")) < float(min_micro_quality_score):
        hard_failures.append("MICRO_QUALITY_TOO_LOW")
    if _safe_float(challenger.get("rolling_nonnegative_active_window_ratio")) < float(min_nonnegative_ratio):
        hard_failures.append("NONNEGATIVE_WINDOW_RATIO_TOO_LOW")
    challenger_closed_trades = _safe_int(challenger.get("execution_structure_closed_trade_count_total"))
    challenger_payoff_ratio = _safe_float(challenger.get("execution_structure_payoff_ratio"))
    challenger_loss_concentration = _safe_float(challenger.get("execution_structure_market_loss_concentration_mean"))
    if challenger_closed_trades >= 3 and challenger_payoff_ratio < 0.75:
        hard_failures.append("PAYOFF_RATIO_TOO_LOW")
    if challenger_closed_trades >= 3 and challenger_loss_concentration > 0.85:
        hard_failures.append("LOSS_CONCENTRATION_TOO_HIGH")

    champion_pnl = _safe_float(champion.get("realized_pnl_quote_total"))
    challenger_pnl = _safe_float(challenger.get("realized_pnl_quote_total"))
    champion_dd = max(_safe_float(champion.get("max_drawdown_pct_max")), 0.0)
    challenger_dd = max(_safe_float(challenger.get("max_drawdown_pct_max")), 0.0)
    champion_micro = _safe_float(champion.get("micro_quality_score_mean"))
    challenger_micro = _safe_float(challenger.get("micro_quality_score_mean"))
    champion_nonnegative = _safe_float(champion.get("rolling_nonnegative_active_window_ratio"))
    challenger_nonnegative = _safe_float(challenger.get("rolling_nonnegative_active_window_ratio"))
    champion_fill_rate = _safe_float(champion.get("fill_rate"))
    challenger_fill_rate = _safe_float(challenger.get("fill_rate"))
    champion_p90_time_to_fill_ms = _safe_float(champion.get("p90_time_to_fill_ms_mean"))
    challenger_p90_time_to_fill_ms = _safe_float(challenger.get("p90_time_to_fill_ms_mean"))
    champion_alpha_gate_fail_rate = _decision_language_rate(champion, "decision_language_alpha_gate_fail_total")
    challenger_alpha_gate_fail_rate = _decision_language_rate(challenger, "decision_language_alpha_gate_fail_total")
    champion_safety_veto_rate = _decision_language_rate(champion, "decision_language_safety_veto_total")
    challenger_safety_veto_rate = _decision_language_rate(challenger, "decision_language_safety_veto_total")
    champion_urgent_liquidation_share = _urgent_liquidation_share(champion)
    challenger_urgent_liquidation_share = _urgent_liquidation_share(challenger)
    time_to_fill_not_worse = True
    if champion_p90_time_to_fill_ms > 0.0 and challenger_p90_time_to_fill_ms > 0.0:
        time_to_fill_not_worse = challenger_p90_time_to_fill_ms <= (
            champion_p90_time_to_fill_ms * float(max_time_to_fill_deterioration_factor)
        )

    pairwise_checks = {
        "pnl_not_worse": challenger_pnl >= champion_pnl,
        "drawdown_not_much_worse": challenger_dd <= (champion_dd * float(max_drawdown_deterioration_factor) if champion_dd > 0 else challenger_dd <= 1.0),
        "micro_quality_not_worse": challenger_micro >= (champion_micro - float(micro_quality_tolerance)),
        "nonnegative_ratio_not_worse": challenger_nonnegative >= (champion_nonnegative - float(nonnegative_ratio_tolerance)),
        "fill_rate_not_worse": challenger_fill_rate >= (champion_fill_rate - 0.02),
        "time_to_fill_not_worse": time_to_fill_not_worse,
        "alpha_gate_fail_concentration_not_worse": challenger_alpha_gate_fail_rate <= (champion_alpha_gate_fail_rate + 0.05),
        "safety_veto_concentration_not_worse": challenger_safety_veto_rate <= (champion_safety_veto_rate + 0.05),
        "urgent_liquidation_not_worse": challenger_urgent_liquidation_share <= (champion_urgent_liquidation_share + 0.05),
    }
    evidence_score = sum(1.0 for passed in pairwise_checks.values() if passed) / max(len(pairwise_checks), 1)
    promote = (len(hard_failures) == 0) and all(pairwise_checks.values())
    decision = "promote_challenger" if promote else ("hold_for_review" if len(hard_failures) == 0 else "keep_champion")
    alpha_failure_summary = {
        "challenger_alpha_gate_fail_total": int(challenger.get("decision_language_alpha_gate_fail_total", 0) or 0),
        "champion_alpha_gate_fail_total": int(champion.get("decision_language_alpha_gate_fail_total", 0) or 0),
        "challenger_alpha_gate_fail_rate": float(challenger_alpha_gate_fail_rate),
        "champion_alpha_gate_fail_rate": float(champion_alpha_gate_fail_rate),
        "challenger_primary_reason_counts": dict(challenger.get("decision_language_primary_reason_counts") or {}),
    }
    safety_failure_summary = {
        "challenger_safety_veto_total": int(challenger.get("decision_language_safety_veto_total", 0) or 0),
        "champion_safety_veto_total": int(champion.get("decision_language_safety_veto_total", 0) or 0),
        "challenger_no_trade_total": int(challenger.get("decision_language_no_trade_total", 0) or 0),
        "challenger_safety_veto_rate": float(challenger_safety_veto_rate),
        "champion_safety_veto_rate": float(champion_safety_veto_rate),
    }
    execution_liquidation_summary = {
        "challenger_continuation_exit_share": float(challenger.get("decision_language_continuation_exit_share", 0.0) or 0.0),
        "champion_continuation_exit_share": float(champion.get("decision_language_continuation_exit_share", 0.0) or 0.0),
        "challenger_liquidation_tier_counts": dict(challenger.get("decision_language_liquidation_tier_counts") or {}),
        "champion_liquidation_tier_counts": dict(champion.get("decision_language_liquidation_tier_counts") or {}),
        "challenger_urgent_liquidation_share": float(challenger_urgent_liquidation_share),
        "champion_urgent_liquidation_share": float(champion_urgent_liquidation_share),
    }
    return {
        "promote": promote,
        "decision": decision,
        "hard_failures": hard_failures,
        "pairwise_checks": pairwise_checks,
        "evidence_score": evidence_score,
        "challenger_hours": challenger_hours,
        "challenger_payoff_ratio": challenger_payoff_ratio,
        "challenger_market_loss_concentration": challenger_loss_concentration,
        "challenger_p90_time_to_fill_ms": challenger_p90_time_to_fill_ms,
        "champion_p90_time_to_fill_ms": champion_p90_time_to_fill_ms,
        "alpha_failure_summary": alpha_failure_summary,
        "safety_failure_summary": safety_failure_summary,
        "execution_liquidation_summary": execution_liquidation_summary,
    }


def build_lane_comparison_report(
    *,
    paper_root: Path,
    lane: str,
    challenger_model_ref: str,
    since_ts_ms: int,
    until_ts_ms: int | None,
    champion_model_run_id: str | None,
    min_challenger_hours: float,
    min_orders_filled: int,
    min_realized_pnl_quote: float,
    min_micro_quality_score: float,
    min_nonnegative_ratio: float,
    max_drawdown_deterioration_factor: float,
    micro_quality_tolerance: float,
    nonnegative_ratio_tolerance: float,
    max_time_to_fill_deterioration_factor: float = 1.25,
) -> dict[str, Any]:
    champion_runs = load_paper_lane_runs(
        paper_root=paper_root,
        lane=lane,
        runtime_role="champion",
        since_ts_ms=since_ts_ms,
        until_ts_ms=until_ts_ms,
        model_run_id=champion_model_run_id,
    )
    challenger_runs = load_paper_lane_runs(
        paper_root=paper_root,
        lane=lane,
        runtime_role="challenger",
        since_ts_ms=since_ts_ms,
        until_ts_ms=until_ts_ms,
        model_ref=challenger_model_ref,
    )
    champion_agg = aggregate_paper_lane_runs(champion_runs)
    challenger_agg = aggregate_paper_lane_runs(challenger_runs)
    decision = compare_champion_challenger(
        champion=champion_agg,
        challenger=challenger_agg,
        min_challenger_hours=min_challenger_hours,
        min_orders_filled=min_orders_filled,
        min_realized_pnl_quote=min_realized_pnl_quote,
        min_micro_quality_score=min_micro_quality_score,
        min_nonnegative_ratio=min_nonnegative_ratio,
        max_drawdown_deterioration_factor=max_drawdown_deterioration_factor,
        micro_quality_tolerance=micro_quality_tolerance,
        nonnegative_ratio_tolerance=nonnegative_ratio_tolerance,
        max_time_to_fill_deterioration_factor=max_time_to_fill_deterioration_factor,
    )
    return {
        "lane": lane,
        "challenger_model_ref": challenger_model_ref,
        "champion_model_run_id": str(champion_model_run_id or "").strip(),
        "since_ts_ms": int(since_ts_ms),
        "until_ts_ms": int(until_ts_ms) if until_ts_ms is not None else None,
        "champion": champion_agg,
        "challenger": challenger_agg,
        "decision": decision,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare champion vs challenger paper lane evidence.")
    parser.add_argument("--paper-root", default="data/paper")
    parser.add_argument("--lane", default="v4")
    parser.add_argument("--challenger-model-ref", required=True)
    parser.add_argument("--champion-model-run-id", default="")
    parser.add_argument("--since-ts-ms", type=int, required=True)
    parser.add_argument("--until-ts-ms", type=int, default=None)
    parser.add_argument("--min-challenger-hours", type=float, default=12.0)
    parser.add_argument("--min-orders-filled", type=int, default=2)
    parser.add_argument("--min-realized-pnl-quote", type=float, default=0.0)
    parser.add_argument("--min-micro-quality-score", type=float, default=0.25)
    parser.add_argument("--min-nonnegative-ratio", type=float, default=0.34)
    parser.add_argument("--max-drawdown-deterioration-factor", type=float, default=1.10)
    parser.add_argument("--micro-quality-tolerance", type=float, default=0.02)
    parser.add_argument("--nonnegative-ratio-tolerance", type=float, default=0.05)
    return parser


def _load_decision_language_summary(*, run_dir: Path) -> dict[str, Any]:
    opportunity_rows = _load_jsonl(run_dir / "opportunity_log.jsonl")
    event_rows = _load_jsonl(run_dir / "events.jsonl")
    primary_reason_counts: dict[str, int] = {}
    alpha_gate_fail_total = 0
    safety_veto_total = 0
    no_trade_total = 0
    continuation_exit_total = 0
    liquidation_tier_counts: dict[str, int] = {}
    for row in opportunity_rows:
        meta = dict(row.get("meta") or {}) if isinstance(row.get("meta"), dict) else {}
        entry_decision = dict(meta.get("entry_decision") or {}) if isinstance(meta.get("entry_decision"), dict) else {}
        safety_vetoes = dict(meta.get("safety_vetoes") or {}) if isinstance(meta.get("safety_vetoes"), dict) else {}
        primary_reason_code = str(entry_decision.get("primary_reason_code") or row.get("skip_reason_code") or "").strip()
        if primary_reason_code:
            _inc_reason(primary_reason_counts, primary_reason_code, 1)
        if primary_reason_code == "ENTRY_GATE_ALPHA_LCB_NOT_POSITIVE":
            alpha_gate_fail_total += 1
        if (
            str(entry_decision.get("primary_reason_family") or "").strip() == "safety_veto"
            or any(isinstance(payload, dict) and (payload.get("reason_codes") or []) for payload in safety_vetoes.values())
        ):
            safety_veto_total += 1
        if str(row.get("skip_reason_code") or "").strip():
            no_trade_total += 1
    for row in event_rows:
        if str(row.get("event_type") or "").strip().upper() != "INTENT_CREATED":
            continue
        payload = dict(row.get("payload") or {}) if isinstance(row.get("payload"), dict) else {}
        if str(payload.get("side") or "").strip().lower() != "ask":
            continue
        meta = dict(payload.get("meta") or {}) if isinstance(payload.get("meta"), dict) else {}
        liquidation_policy = dict(meta.get("liquidation_policy") or {}) if isinstance(meta.get("liquidation_policy"), dict) else {}
        exit_decision = dict(meta.get("exit_decision") or {}) if isinstance(meta.get("exit_decision"), dict) else {}
        tier_name = str(liquidation_policy.get("tier_name") or "").strip()
        if tier_name:
            _inc_reason(liquidation_tier_counts, tier_name, 1)
        if str(exit_decision.get("decision_reason_code") or "").strip() == "CONTINUATION_VALUE_EXIT":
            continuation_exit_total += 1
    return {
        "primary_reason_counts": primary_reason_counts,
        "alpha_gate_fail_total": int(alpha_gate_fail_total),
        "safety_veto_total": int(safety_veto_total),
        "no_trade_total": int(no_trade_total),
        "continuation_exit_total": int(continuation_exit_total),
        "liquidation_tier_counts": liquidation_tier_counts,
    }


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                rows.append(payload)
    except (OSError, json.JSONDecodeError):
        return []
    return rows


def _inc_reason(target: dict[str, int], key: str, delta: int) -> None:
    text = str(key or "").strip()
    if not text:
        return
    target[text] = int(target.get(text, 0)) + int(delta)


def _decision_language_rate(payload: dict[str, Any], numerator_key: str) -> float:
    denominator = _decision_language_observation_total(payload)
    if denominator <= 0:
        return 0.0
    return float(_safe_int(payload.get(numerator_key))) / float(denominator)


def _decision_language_observation_total(payload: dict[str, Any]) -> int:
    primary_reason_counts = dict(payload.get("decision_language_primary_reason_counts") or {})
    primary_reason_total = sum(_safe_int(value) for value in primary_reason_counts.values())
    if primary_reason_total > 0:
        return int(primary_reason_total)
    fallback_total = max(
        _safe_int(payload.get("decision_language_no_trade_total")) + _safe_int(payload.get("orders_filled")),
        0,
    )
    return int(fallback_total)


def _urgent_liquidation_share(payload: dict[str, Any]) -> float:
    liquidation_tier_counts = dict(payload.get("decision_language_liquidation_tier_counts") or {})
    total = sum(_safe_int(value) for value in liquidation_tier_counts.values())
    if total <= 0:
        return 0.0
    urgent_total = sum(
        _safe_int(liquidation_tier_counts.get(name))
        for name in ("urgent_defensive", "emergency_flatten")
    )
    return float(urgent_total) / float(total)


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    report = build_lane_comparison_report(
        paper_root=Path(args.paper_root),
        lane=str(args.lane).strip().lower(),
        challenger_model_ref=str(args.challenger_model_ref).strip(),
        champion_model_run_id=str(args.champion_model_run_id).strip() or None,
        since_ts_ms=int(args.since_ts_ms),
        until_ts_ms=args.until_ts_ms,
        min_challenger_hours=float(args.min_challenger_hours),
        min_orders_filled=int(args.min_orders_filled),
        min_realized_pnl_quote=float(args.min_realized_pnl_quote),
        min_micro_quality_score=float(args.min_micro_quality_score),
        min_nonnegative_ratio=float(args.min_nonnegative_ratio),
        max_drawdown_deterioration_factor=float(args.max_drawdown_deterioration_factor),
        micro_quality_tolerance=float(args.micro_quality_tolerance),
        nonnegative_ratio_tolerance=float(args.nonnegative_ratio_tolerance),
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
