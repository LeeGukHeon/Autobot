from __future__ import annotations

import csv
import json
from collections import defaultdict, deque
from pathlib import Path
from statistics import fmean
from typing import Any

from autobot.common.execution_structure import build_intent_outcomes_from_trade_csv


PAIRED_PAPER_REPORT_VERSION = 1


def build_paired_paper_report(
    *,
    champion_run_dir: Path,
    challenger_run_dir: Path,
) -> dict[str, Any]:
    champion_index = _build_run_index(run_dir=Path(champion_run_dir))
    challenger_index = _build_run_index(run_dir=Path(challenger_run_dir))

    champion_opportunities = champion_index["opportunities"]
    challenger_opportunities = challenger_index["opportunities"]
    champion_ids = set(champion_opportunities.keys())
    challenger_ids = set(challenger_opportunities.keys())
    matched_ids = sorted(champion_ids & challenger_ids)
    champion_only_ids = sorted(champion_ids - challenger_ids)
    challenger_only_ids = sorted(challenger_ids - champion_ids)

    taxonomy_counts: dict[str, int] = {
        "both_trade_same_action": 0,
        "both_trade_different_action": 0,
        "champion_trade_challenger_no_trade": 0,
        "challenger_trade_champion_no_trade": 0,
        "both_no_trade_same_reason": 0,
        "both_no_trade_different_reason": 0,
        "both_filled": 0,
        "champion_fill_only": 0,
        "challenger_fill_only": 0,
        "neither_filled": 0,
    }
    decision_language_counts: dict[str, int] = {
        "both_same_primary_decision_reason": 0,
        "both_different_primary_decision_reason": 0,
        "champion_safety_veto_only": 0,
        "challenger_safety_veto_only": 0,
    }
    disagreement_examples: list[dict[str, Any]] = []
    champion_fill_total = 0
    challenger_fill_total = 0
    champion_trade_total = 0
    challenger_trade_total = 0
    champion_notional_total = 0.0
    challenger_notional_total = 0.0
    champion_slippage_values: list[float] = []
    challenger_slippage_values: list[float] = []
    feature_hash_matches = 0

    for opportunity_id in matched_ids:
        champion_item = champion_opportunities[opportunity_id]
        challenger_item = challenger_opportunities[opportunity_id]
        champion_trade = bool(champion_item.get("has_trade"))
        challenger_trade = bool(challenger_item.get("has_trade"))
        champion_filled = int(champion_item.get("fill_count") or 0) > 0
        challenger_filled = int(challenger_item.get("fill_count") or 0) > 0

        champion_fill_total += int(champion_item.get("fill_count") or 0)
        challenger_fill_total += int(challenger_item.get("fill_count") or 0)
        champion_trade_total += 1 if champion_trade else 0
        challenger_trade_total += 1 if challenger_trade else 0
        champion_notional_total += _safe_float(champion_item.get("filled_notional_quote"))
        challenger_notional_total += _safe_float(challenger_item.get("filled_notional_quote"))

        champion_slippage = _safe_optional_float(champion_item.get("mean_slippage_bps"))
        challenger_slippage = _safe_optional_float(challenger_item.get("mean_slippage_bps"))
        if champion_slippage is not None:
            champion_slippage_values.append(champion_slippage)
        if challenger_slippage is not None:
            challenger_slippage_values.append(challenger_slippage)
        if str(champion_item.get("feature_hash") or "").strip() == str(challenger_item.get("feature_hash") or "").strip():
            feature_hash_matches += 1

        if champion_trade and challenger_trade:
            if str(champion_item.get("chosen_action") or "").strip() == str(challenger_item.get("chosen_action") or "").strip():
                taxonomy_counts["both_trade_same_action"] += 1
            else:
                taxonomy_counts["both_trade_different_action"] += 1
        elif champion_trade and not challenger_trade:
            taxonomy_counts["champion_trade_challenger_no_trade"] += 1
            if challenger_item.get("primary_decision_family") == "safety_veto":
                decision_language_counts["challenger_safety_veto_only"] += 1
        elif challenger_trade and not champion_trade:
            taxonomy_counts["challenger_trade_champion_no_trade"] += 1
            if champion_item.get("primary_decision_family") == "safety_veto":
                decision_language_counts["champion_safety_veto_only"] += 1
        else:
            champion_primary_reason = str(
                champion_item.get("primary_decision_reason_code") or champion_item.get("skip_reason_code") or ""
            ).strip()
            challenger_primary_reason = str(
                challenger_item.get("primary_decision_reason_code") or challenger_item.get("skip_reason_code") or ""
            ).strip()
            if champion_primary_reason == challenger_primary_reason:
                taxonomy_counts["both_no_trade_same_reason"] += 1
                decision_language_counts["both_same_primary_decision_reason"] += 1
            else:
                taxonomy_counts["both_no_trade_different_reason"] += 1
                decision_language_counts["both_different_primary_decision_reason"] += 1
            if champion_item.get("primary_decision_family") == "safety_veto" and challenger_item.get("primary_decision_family") != "safety_veto":
                decision_language_counts["champion_safety_veto_only"] += 1
            if challenger_item.get("primary_decision_family") == "safety_veto" and champion_item.get("primary_decision_family") != "safety_veto":
                decision_language_counts["challenger_safety_veto_only"] += 1

        if champion_filled and challenger_filled:
            taxonomy_counts["both_filled"] += 1
        elif champion_filled and not challenger_filled:
            taxonomy_counts["champion_fill_only"] += 1
        elif challenger_filled and not champion_filled:
            taxonomy_counts["challenger_fill_only"] += 1
        else:
            taxonomy_counts["neither_filled"] += 1

        if len(disagreement_examples) >= 20:
            continue
        if (
            str(champion_item.get("chosen_action") or "").strip() != str(challenger_item.get("chosen_action") or "").strip()
            or str(champion_item.get("skip_reason_code") or "").strip()
            != str(challenger_item.get("skip_reason_code") or "").strip()
            or champion_filled != challenger_filled
        ):
            disagreement_examples.append(
                {
                    "opportunity_id": opportunity_id,
                    "market": str(champion_item.get("market") or challenger_item.get("market") or "").strip().upper(),
                    "ts_ms": int(champion_item.get("ts_ms") or challenger_item.get("ts_ms") or 0),
                    "champion": _example_projection(champion_item),
                    "challenger": _example_projection(challenger_item),
                }
            )

    champion_summary = champion_index["summary"]
    challenger_summary = challenger_index["summary"]
    champion_total = len(champion_ids)
    challenger_total = len(challenger_ids)
    matched_total = len(matched_ids)

    matched_realized_pnl_pairs = []
    for opportunity_id in matched_ids:
        champion_realized = _safe_optional_float(champion_opportunities[opportunity_id].get("realized_pnl_quote"))
        challenger_realized = _safe_optional_float(challenger_opportunities[opportunity_id].get("realized_pnl_quote"))
        if champion_realized is None and challenger_realized is None:
            continue
        matched_realized_pnl_pairs.append((champion_realized, challenger_realized))
    paired_deltas = {
        "aggregate_realized_pnl_delta_quote": _safe_float(challenger_summary.get("realized_pnl_quote"))
        - _safe_float(champion_summary.get("realized_pnl_quote")),
        "matched_pnl_delta_quote": float(
            sum(float(challenger or 0.0) - float(champion or 0.0) for champion, challenger in matched_realized_pnl_pairs)
        ),
        "matched_pnl_status": "matched_entry_realized_pnl",
        "matched_pnl_covered_opportunity_count": int(len(matched_realized_pnl_pairs)),
        "matched_fill_delta": int(challenger_fill_total - champion_fill_total),
        "matched_slippage_delta_bps": _delta_optional_mean(
            challenger_slippage_values,
            champion_slippage_values,
        ),
        "matched_no_trade_delta": int(
            taxonomy_counts["challenger_trade_champion_no_trade"]
            - taxonomy_counts["champion_trade_challenger_no_trade"]
        ),
        "matched_filled_notional_delta_quote": float(challenger_notional_total - champion_notional_total),
    }

    return {
        "artifact_version": PAIRED_PAPER_REPORT_VERSION,
        "comparison_mode": "paired_paper_opportunity_v1",
        "champion": _run_header(summary=champion_summary, run_dir=Path(champion_run_dir)),
        "challenger": _run_header(summary=challenger_summary, run_dir=Path(challenger_run_dir)),
        "clock_alignment": {
            "matched_opportunities": matched_total,
            "champion_total_opportunities": champion_total,
            "challenger_total_opportunities": challenger_total,
            "champion_only_opportunities": len(champion_only_ids),
            "challenger_only_opportunities": len(challenger_only_ids),
            "matched_ratio_vs_champion": _safe_ratio(matched_total, champion_total),
            "matched_ratio_vs_challenger": _safe_ratio(matched_total, challenger_total),
            "feature_hash_match_ratio": _safe_ratio(feature_hash_matches, matched_total),
            "pair_ready": matched_total > 0 and len(champion_only_ids) == 0 and len(challenger_only_ids) == 0,
        },
        "matched_summary": {
            "champion_trade_selected_total": champion_trade_total,
            "challenger_trade_selected_total": challenger_trade_total,
            "champion_fill_total": champion_fill_total,
            "challenger_fill_total": challenger_fill_total,
            "champion_filled_notional_quote_total": float(champion_notional_total),
            "challenger_filled_notional_quote_total": float(challenger_notional_total),
            "champion_mean_slippage_bps": _optional_mean(champion_slippage_values),
            "challenger_mean_slippage_bps": _optional_mean(challenger_slippage_values),
        },
        "paired_deltas": paired_deltas,
        "taxonomy_counts": taxonomy_counts,
        "decision_language_counts": decision_language_counts,
        "opportunity_sets": {
            "matched_ids": matched_ids,
            "champion_only_ids": champion_only_ids,
            "challenger_only_ids": challenger_only_ids,
        },
        "disagreement_examples": disagreement_examples,
    }


def write_paired_paper_report(*, report: dict[str, Any], output_path: Path) -> None:
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _build_run_index(*, run_dir: Path) -> dict[str, Any]:
    summary = _load_json(run_dir / "summary.json")
    opportunities = _load_jsonl(run_dir / "opportunity_log.jsonl")
    intent_queue_exact, intent_queue_loose, intent_queue_reason, intent_queue_market = _build_intent_queues(run_dir=run_dir)
    trade_metrics_by_intent = _load_trade_metrics_by_intent(run_dir=run_dir)

    indexed: dict[str, dict[str, Any]] = {}
    for row in opportunities:
        opportunity_id = str(row.get("opportunity_id") or "").strip()
        if not opportunity_id:
            continue
        intent_id = _resolve_intent_id(
            row=row,
            exact_queue=intent_queue_exact,
            loose_queue=intent_queue_loose,
            reason_queue=intent_queue_reason,
            market_queue=intent_queue_market,
        )
        metrics = trade_metrics_by_intent.get(intent_id or "", {})
        indexed[opportunity_id] = {
            "opportunity_id": opportunity_id,
            "ts_ms": _safe_int(row.get("ts_ms")),
            "market": str(row.get("market") or "").strip().upper(),
            "side": str(row.get("side") or "").strip().lower(),
            "feature_hash": str(row.get("feature_hash") or "").strip(),
            "chosen_action": str(row.get("chosen_action") or "").strip(),
            "skip_reason_code": _optional_text(row.get("skip_reason_code")),
            "primary_decision_reason_code": _primary_decision_reason_code(row),
            "primary_decision_family": _primary_decision_family(row),
            "entry_decision_reason_codes": _entry_decision_reason_codes(row),
            "safety_veto_reason_codes": _safety_veto_reason_codes(row),
            "exit_decision_reason_code": _exit_decision_reason_code(row),
            "liquidation_policy_tier": _liquidation_policy_tier(row),
            "reason_code": str(row.get("reason_code") or "").strip().upper(),
            "intent_id": intent_id,
            "selection_score": _safe_optional_float(row.get("selection_score")),
            "expected_edge_bps": _safe_optional_float(row.get("expected_edge_bps")),
            "fill_count": int(metrics.get("fill_count", 0)),
            "filled_notional_quote": float(metrics.get("filled_notional_quote", 0.0)),
            "mean_slippage_bps": _safe_optional_float(metrics.get("mean_slippage_bps")),
            "realized_pnl_quote": _safe_optional_float(metrics.get("realized_pnl_quote")),
            "closed": bool(metrics.get("closed", False)),
            "total_fee_quote": float(metrics.get("total_fee_quote", 0.0)),
            "price_modes": dict(metrics.get("price_modes") or {}),
            "has_trade": bool(intent_id) or _is_trade_selected(row),
        }
    return {
        "summary": summary,
        "opportunities": indexed,
    }


def _build_intent_queues(
    *,
    run_dir: Path,
) -> tuple[
    dict[tuple[int, str, str, str], deque[str]],
    dict[tuple[int, str, str], deque[str]],
    dict[tuple[str, str, str], deque[str]],
    dict[tuple[str, str], deque[str]],
]:
    exact_queue: dict[tuple[int, str, str, str], deque[str]] = defaultdict(deque)
    loose_queue: dict[tuple[int, str, str], deque[str]] = defaultdict(deque)
    reason_queue: dict[tuple[str, str, str], deque[str]] = defaultdict(deque)
    market_queue: dict[tuple[str, str], deque[str]] = defaultdict(deque)
    for item in _load_jsonl(run_dir / "events.jsonl"):
        if str(item.get("event_type") or "").strip().upper() != "INTENT_CREATED":
            continue
        payload = item.get("payload")
        if not isinstance(payload, dict):
            continue
        intent_id = _optional_text(payload.get("intent_id"))
        if intent_id is None:
            continue
        ts_ms = _safe_int(item.get("ts_ms") or payload.get("ts_ms"))
        market = str(payload.get("market") or "").strip().upper()
        side = str(payload.get("side") or "").strip().lower()
        reason_code = str(payload.get("reason_code") or "").strip().upper()
        if not market or not side:
            continue
        exact_queue[(ts_ms, market, side, reason_code)].append(intent_id)
        loose_queue[(ts_ms, market, side)].append(intent_id)
        reason_queue[(market, side, reason_code)].append(intent_id)
        market_queue[(market, side)].append(intent_id)
    return exact_queue, loose_queue, reason_queue, market_queue


def _resolve_intent_id(
    *,
    row: dict[str, Any],
    exact_queue: dict[tuple[int, str, str, str], deque[str]],
    loose_queue: dict[tuple[int, str, str], deque[str]],
    reason_queue: dict[tuple[str, str, str], deque[str]],
    market_queue: dict[tuple[str, str], deque[str]],
) -> str | None:
    explicit_intent_id = _optional_text(row.get("intent_id"))
    if explicit_intent_id is not None:
        return explicit_intent_id
    if _optional_text(row.get("skip_reason_code")) is not None:
        return None
    ts_ms = _safe_int(row.get("ts_ms"))
    market = str(row.get("market") or "").strip().upper()
    side = str(row.get("side") or "").strip().lower()
    reason_code = str(row.get("reason_code") or "").strip().upper()
    exact_key = (ts_ms, market, side, reason_code)
    if exact_key in exact_queue and exact_queue[exact_key]:
        return exact_queue[exact_key].popleft()
    loose_key = (ts_ms, market, side)
    if loose_key in loose_queue and loose_queue[loose_key]:
        return loose_queue[loose_key].popleft()
    reason_key = (market, side, reason_code)
    if reason_key in reason_queue and reason_queue[reason_key]:
        return reason_queue[reason_key].popleft()
    market_key = (market, side)
    if market_key in market_queue and market_queue[market_key]:
        return market_queue[market_key].popleft()
    return None


def _load_trade_metrics_by_intent(*, run_dir: Path) -> dict[str, dict[str, Any]]:
    rows_path = run_dir / "trades.csv"
    if not rows_path.exists():
        return {}
    outcomes_by_intent = build_intent_outcomes_from_trade_csv(rows_path)
    metrics_by_intent: dict[str, dict[str, Any]] = {}
    with rows_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            intent_id = _optional_text(row.get("intent_id"))
            if intent_id is None:
                continue
            item = metrics_by_intent.setdefault(
                intent_id,
                {
                    "fill_count": 0,
                    "filled_notional_quote": 0.0,
                    "total_fee_quote": 0.0,
                    "slippages": [],
                    "price_modes": {},
                },
            )
            item["fill_count"] = int(item.get("fill_count", 0)) + 1
            item["filled_notional_quote"] = float(item.get("filled_notional_quote", 0.0)) + _safe_float(
                row.get("notional_quote")
            )
            item["total_fee_quote"] = float(item.get("total_fee_quote", 0.0)) + _safe_float(row.get("fee_quote"))
            slippage = _safe_optional_float(row.get("slippage_bps"))
            if slippage is not None and isinstance(item.get("slippages"), list):
                item["slippages"].append(slippage)
            price_mode = str(row.get("price_mode") or "").strip().upper()
            if price_mode:
                price_modes = item.setdefault("price_modes", {})
                if isinstance(price_modes, dict):
                    price_modes[price_mode] = int(price_modes.get(price_mode, 0)) + 1
    for item in metrics_by_intent.values():
        item["mean_slippage_bps"] = _optional_mean(item.get("slippages", []))
        item.pop("slippages", None)
    for intent_id, outcome in outcomes_by_intent.items():
        item = metrics_by_intent.setdefault(
            intent_id,
            {
                "fill_count": 0,
                "filled_notional_quote": 0.0,
                "total_fee_quote": 0.0,
                "price_modes": {},
            },
        )
        item["realized_pnl_quote"] = _safe_optional_float(outcome.get("realized_pnl_quote"))
        item["closed"] = bool(outcome.get("closed", False))
        exit_reason_counts = dict(outcome.get("exit_reason_counts") or {})
        if exit_reason_counts:
            item["exit_reason_counts"] = exit_reason_counts
    return metrics_by_intent


def _run_header(*, summary: dict[str, Any], run_dir: Path) -> dict[str, Any]:
    return {
        "run_dir": str(run_dir),
        "run_id": str(summary.get("run_id") or run_dir.name).strip(),
        "paper_runtime_role": str(summary.get("paper_runtime_role") or "").strip().lower(),
        "paper_runtime_model_ref": str(
            summary.get("paper_runtime_model_ref_pinned") or summary.get("paper_runtime_model_ref") or ""
        ).strip(),
        "paper_runtime_model_run_id": str(summary.get("paper_runtime_model_run_id") or "").strip(),
        "run_started_ts_ms": _safe_int(summary.get("run_started_ts_ms")),
        "run_completed_ts_ms": _safe_int(summary.get("run_completed_ts_ms")),
        "realized_pnl_quote": _safe_float(summary.get("realized_pnl_quote")),
        "orders_filled": _safe_int(summary.get("orders_filled")),
    }


def _example_projection(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "chosen_action": str(item.get("chosen_action") or "").strip(),
        "skip_reason_code": _optional_text(item.get("skip_reason_code")),
        "primary_decision_reason_code": _optional_text(item.get("primary_decision_reason_code")),
        "primary_decision_family": _optional_text(item.get("primary_decision_family")),
        "entry_decision_reason_codes": list(item.get("entry_decision_reason_codes") or []),
        "safety_veto_reason_codes": list(item.get("safety_veto_reason_codes") or []),
        "exit_decision_reason_code": _optional_text(item.get("exit_decision_reason_code")),
        "liquidation_policy_tier": _optional_text(item.get("liquidation_policy_tier")),
        "intent_id": _optional_text(item.get("intent_id")),
        "fill_count": int(item.get("fill_count") or 0),
        "filled_notional_quote": float(item.get("filled_notional_quote") or 0.0),
        "mean_slippage_bps": _safe_optional_float(item.get("mean_slippage_bps")),
        "realized_pnl_quote": _safe_optional_float(item.get("realized_pnl_quote")),
        "closed": bool(item.get("closed", False)),
    }


def _strategy_meta_from_row(row: dict[str, Any]) -> dict[str, Any]:
    meta = dict(row.get("meta") or {}) if isinstance(row.get("meta"), dict) else {}
    return meta


def _entry_decision_reason_codes(row: dict[str, Any]) -> list[str]:
    strategy_meta = _strategy_meta_from_row(row)
    entry_decision = dict(strategy_meta.get("entry_decision") or {}) if isinstance(strategy_meta.get("entry_decision"), dict) else {}
    return [str(item).strip() for item in (entry_decision.get("reason_codes") or []) if str(item).strip()]


def _safety_veto_reason_codes(row: dict[str, Any]) -> list[str]:
    strategy_meta = _strategy_meta_from_row(row)
    safety_vetoes = dict(strategy_meta.get("safety_vetoes") or {}) if isinstance(strategy_meta.get("safety_vetoes"), dict) else {}
    codes: list[str] = []
    for payload in safety_vetoes.values():
        if not isinstance(payload, dict):
            continue
        for item in (payload.get("reason_codes") or []):
            code = str(item).strip()
            if code and code not in codes:
                codes.append(code)
    return codes


def _exit_decision_reason_code(row: dict[str, Any]) -> str | None:
    strategy_meta = _strategy_meta_from_row(row)
    exit_decision = dict(strategy_meta.get("exit_decision") or {}) if isinstance(strategy_meta.get("exit_decision"), dict) else {}
    return _optional_text(exit_decision.get("decision_reason_code"))


def _liquidation_policy_tier(row: dict[str, Any]) -> str | None:
    strategy_meta = _strategy_meta_from_row(row)
    liquidation_policy = dict(strategy_meta.get("liquidation_policy") or {}) if isinstance(strategy_meta.get("liquidation_policy"), dict) else {}
    return _optional_text(liquidation_policy.get("tier_name"))


def _primary_decision_reason_code(row: dict[str, Any]) -> str | None:
    safety_codes = _safety_veto_reason_codes(row)
    if safety_codes:
        return safety_codes[0]
    entry_codes = _entry_decision_reason_codes(row)
    if entry_codes:
        return entry_codes[0]
    return _optional_text(row.get("skip_reason_code"))


def _primary_decision_family(row: dict[str, Any]) -> str | None:
    if _safety_veto_reason_codes(row):
        return "safety_veto"
    if _entry_decision_reason_codes(row):
        return "entry_decision"
    if _optional_text(row.get("skip_reason_code")) is not None:
        return "skip_reason"
    return None


def _is_trade_selected(row: dict[str, Any]) -> bool:
    chosen_action = str(row.get("chosen_action") or "").strip().upper()
    if not chosen_action:
        return False
    return chosen_action not in {"NO_TRADE", "SKIP", "INTENT_CREATED"}


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
    except OSError:
        return []
    return rows


def _safe_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def _safe_float(value: Any) -> float:
    try:
        if value is None or value == "":
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        if value is None or value == "":
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _safe_optional_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_mean(values: Any) -> float | None:
    cleaned = [float(value) for value in values if isinstance(value, (int, float))]
    if not cleaned:
        return None
    return float(fmean(cleaned))


def _delta_optional_mean(values_a: Any, values_b: Any) -> float | None:
    mean_a = _optional_mean(values_a)
    mean_b = _optional_mean(values_b)
    if mean_a is None or mean_b is None:
        return None
    return float(mean_a - mean_b)
