from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from autobot.execution.order_supervisor import slippage_bps
from autobot.models.live_execution_policy import ACTION_CONFIGS

from .state_store import ExecutionAttemptRecord, LiveStateStore


def backfill_execution_attempts(
    *,
    store: LiveStateStore,
    lookback_days: int = 14,
    limit: int = 5000,
) -> dict[str, Any]:
    now_ts_ms = int(time.time() * 1000)
    since_ts_ms = now_ts_ms - (max(int(lookback_days), 1) * 86_400_000)
    journals = store.list_trade_journal(limit=max(int(limit), 1) * 4)
    recent_journals = [
        dict(item)
        for item in journals
        if _journal_event_ts_ms(item) >= since_ts_ms
    ]
    orders = store.list_orders(open_only=False)
    orders_by_uuid = {
        str(item.get("uuid") or "").strip(): dict(item)
        for item in orders
        if str(item.get("uuid") or "").strip()
    }
    orders_by_intent: dict[str, list[dict[str, Any]]] = {}
    for order in orders:
        intent_id = str(order.get("intent_id") or "").strip()
        if intent_id:
            orders_by_intent.setdefault(intent_id, []).append(dict(order))

    report = {
        "policy": "execution_attempts_backfill_v1",
        "db_path": str(store.db_path),
        "lookback_days": max(int(lookback_days), 1),
        "journals_scanned": int(len(recent_journals)),
        "attempts_upserted": 0,
        "filled": 0,
        "missed": 0,
        "partial_cancelled": 0,
        "skipped": 0,
        "skipped_reasons": {},
    }
    for journal in recent_journals:
        order = _resolve_entry_order(journal=journal, orders_by_uuid=orders_by_uuid, orders_by_intent=orders_by_intent)
        record, skip_reason = _build_attempt_record(journal=journal, order=order)
        if record is None:
            report["skipped"] += 1
            _inc_reason(report["skipped_reasons"], skip_reason or "UNSUPPORTED_JOURNAL")
            continue
        store.upsert_execution_attempt(record)
        report["attempts_upserted"] += 1
        if str(record.final_state or "") == "FILLED":
            report["filled"] += 1
        elif str(record.final_state or "") == "PARTIAL_CANCELLED":
            report["partial_cancelled"] += 1
        elif str(record.final_state or "") == "MISSED":
            report["missed"] += 1
    return report


def backfill_execution_attempts_for_db(
    *,
    db_path: Path,
    lookback_days: int = 14,
    limit: int = 5000,
) -> dict[str, Any]:
    with LiveStateStore(db_path) as store:
        return backfill_execution_attempts(
            store=store,
            lookback_days=lookback_days,
            limit=limit,
        )


def _build_attempt_record(
    *,
    journal: dict[str, Any],
    order: dict[str, Any] | None,
) -> tuple[ExecutionAttemptRecord | None, str | None]:
    journal_id = _as_optional_str(journal.get("journal_id"))
    market = _as_optional_str(journal.get("market"))
    if journal_id is None or market is None:
        return None, "MISSING_JOURNAL_ID_OR_MARKET"
    submitted_ts_ms = _safe_optional_int(journal.get("entry_submitted_ts_ms"))
    filled_ts_ms = _safe_optional_int(journal.get("entry_filled_ts_ms"))
    exit_ts_ms = _safe_optional_int(journal.get("exit_ts_ms"))
    if submitted_ts_ms is None and filled_ts_ms is None:
        return None, "ENTRY_TIMESTAMPS_MISSING"
    entry_order_uuid = _as_optional_str(journal.get("entry_order_uuid"))
    entry_intent_id = _as_optional_str(journal.get("entry_intent_id"))
    if entry_order_uuid is None and entry_intent_id is None:
        return None, "ENTRY_IDENTITY_MISSING"
    entry_meta = dict(journal.get("entry_meta") or {}) if isinstance(journal.get("entry_meta"), dict) else {}
    execution_meta = dict(entry_meta.get("execution") or {}) if isinstance(entry_meta.get("execution"), dict) else {}
    strategy_meta = dict(((entry_meta.get("strategy") or {}).get("meta")) or {}) if isinstance(entry_meta.get("strategy"), dict) else {}
    trade_action = dict(strategy_meta.get("trade_action") or {}) if isinstance(strategy_meta.get("trade_action"), dict) else {}
    execution_policy = (
        dict(entry_meta.get("execution_policy") or {}) if isinstance(entry_meta.get("execution_policy"), dict) else {}
    )
    micro_state = dict(entry_meta.get("micro_state") or {}) if isinstance(entry_meta.get("micro_state"), dict) else {}
    requested_price = _safe_optional_float(execution_meta.get("requested_price", journal.get("entry_price")))
    requested_volume = _safe_optional_float(execution_meta.get("requested_volume", journal.get("qty")))
    if requested_volume is None:
        requested_volume = _safe_optional_float((order or {}).get("volume_req", journal.get("qty")))
    if requested_price is None:
        requested_price = _safe_optional_float((order or {}).get("price", journal.get("entry_price")))
    filled_volume = _safe_optional_float((order or {}).get("volume_filled"))
    if filled_volume is None and filled_ts_ms is not None:
        filled_volume = _safe_optional_float(journal.get("qty"))
    fill_fraction = _resolve_fill_fraction(requested_volume=requested_volume, filled_volume=filled_volume)
    full_fill = bool(
        filled_ts_ms is not None
        or (fill_fraction is not None and fill_fraction >= 0.999999)
        or str((order or {}).get("local_state") or "").strip().upper() == "DONE"
    )
    partial_fill = bool((fill_fraction is not None and fill_fraction > 0.0 and fill_fraction < 0.999999) and not full_fill)
    status = str(journal.get("status") or "").strip().upper()
    if full_fill:
        final_state = "FILLED"
        final_ts_ms = filled_ts_ms or _safe_optional_int((order or {}).get("updated_ts")) or submitted_ts_ms or int(time.time() * 1000)
    elif status == "CANCELLED_ENTRY":
        final_state = "PARTIAL_CANCELLED" if partial_fill or (filled_volume or 0.0) > 0.0 else "MISSED"
        final_ts_ms = exit_ts_ms or _safe_optional_int((order or {}).get("updated_ts")) or submitted_ts_ms or int(time.time() * 1000)
    elif status == "OPEN":
        final_state = "FILLED" if filled_ts_ms is not None else "OPEN_PENDING"
        final_ts_ms = filled_ts_ms
    else:
        return None, "UNSUPPORTED_STATUS"
    if final_state == "OPEN_PENDING":
        return None, "ENTRY_NOT_FINALIZED"
    ord_type = str((order or {}).get("ord_type") or "limit").strip().lower() or "limit"
    price_mode = _as_optional_str(((execution_meta.get("exec_profile") or {}).get("price_mode"))) or "JOIN"
    time_in_force = _as_optional_str((order or {}).get("time_in_force")) or _infer_time_in_force(
        execution_policy=execution_policy,
        ord_type=ord_type,
    )
    action_code = _infer_action_code(
        execution_policy=execution_policy,
        ord_type=ord_type,
        time_in_force=time_in_force,
        price_mode=price_mode,
    )
    filled_price = _safe_optional_float(journal.get("entry_price"))
    reference_price = _safe_optional_float(
        execution_meta.get("effective_ref_price", execution_meta.get("initial_ref_price", execution_meta.get("requested_price")))
    )
    shortfall = None
    if filled_price is not None and reference_price is not None and reference_price > 0.0:
        shortfall = slippage_bps(side="bid", fill_price=float(filled_price), ref_price=float(reference_price))
    requested_notional_quote = None
    if requested_price is not None and requested_volume is not None:
        requested_notional_quote = float(requested_price) * float(requested_volume)
    attempt_id = _build_attempt_id(
        intent_id=entry_intent_id or journal_id,
        order_uuid=entry_order_uuid,
        submitted_ts_ms=submitted_ts_ms or filled_ts_ms or final_ts_ms,
    )
    return (
        ExecutionAttemptRecord(
            attempt_id=attempt_id,
            journal_id=journal_id,
            intent_id=entry_intent_id,
            order_uuid=entry_order_uuid,
            order_identifier=_as_optional_str((order or {}).get("identifier")),
            market=market,
            side="bid",
            ord_type=ord_type,
            time_in_force=time_in_force,
            action_code=action_code,
            price_mode=price_mode,
            requested_price=requested_price,
            requested_volume=requested_volume,
            requested_notional_quote=requested_notional_quote,
            reference_price=reference_price,
            tick_size=_safe_optional_float(((entry_meta.get("admissibility") or {}).get("snapshot") or {}).get("tick_size")),
            spread_bps=_safe_optional_float(micro_state.get("spread_bps")),
            depth_top5_notional_krw=_safe_optional_float(micro_state.get("depth_top5_notional_krw")),
            trade_coverage_ms=_safe_optional_int(micro_state.get("trade_coverage_ms")),
            book_coverage_ms=_safe_optional_int(micro_state.get("book_coverage_ms")),
            snapshot_age_ms=_safe_optional_int(micro_state.get("snapshot_age_ms")),
            micro_quality_score=_safe_optional_float(micro_state.get("micro_quality_score")),
            model_prob=_safe_optional_float(strategy_meta.get("model_prob")),
            expected_edge_bps=_safe_optional_float(journal.get("expected_edge_bps")),
            expected_net_edge_bps=_safe_optional_float(journal.get("expected_net_edge_bps")),
            expected_es_bps=_es_bps_from_trade_action(trade_action),
            submitted_ts_ms=int(submitted_ts_ms or filled_ts_ms or final_ts_ms),
            acknowledged_ts_ms=_safe_optional_int((order or {}).get("created_ts")) or _safe_optional_int(submitted_ts_ms),
            first_fill_ts_ms=_safe_optional_int(filled_ts_ms),
            full_fill_ts_ms=_safe_optional_int(filled_ts_ms) if full_fill else None,
            cancelled_ts_ms=_safe_optional_int(exit_ts_ms) if final_state in {"MISSED", "PARTIAL_CANCELLED"} else None,
            final_ts_ms=_safe_optional_int(final_ts_ms),
            final_state=final_state,
            filled_price=filled_price,
            shortfall_bps=_safe_optional_float(shortfall),
            filled_volume=filled_volume,
            fill_fraction=fill_fraction,
            partial_fill=bool(final_state == "PARTIAL_CANCELLED"),
            full_fill=bool(final_state == "FILLED"),
            outcome_json=json.dumps(
                {
                    "source": "trade_journal_backfill",
                    "journal_status": status,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            updated_ts=int(final_ts_ms),
        ),
        None,
    )


def _resolve_entry_order(
    *,
    journal: dict[str, Any],
    orders_by_uuid: dict[str, dict[str, Any]],
    orders_by_intent: dict[str, list[dict[str, Any]]],
) -> dict[str, Any] | None:
    entry_order_uuid = _as_optional_str(journal.get("entry_order_uuid"))
    if entry_order_uuid is not None and entry_order_uuid in orders_by_uuid:
        return dict(orders_by_uuid[entry_order_uuid])
    entry_intent_id = _as_optional_str(journal.get("entry_intent_id"))
    if entry_intent_id is None:
        return None
    candidates = [
        dict(item)
        for item in (orders_by_intent.get(entry_intent_id) or [])
        if str(item.get("side") or "").strip().lower() == "bid"
        and str(item.get("market") or "").strip().upper() == str(journal.get("market") or "").strip().upper()
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda item: int(item.get("updated_ts") or item.get("created_ts") or 0), reverse=True)
    return candidates[0]


def _infer_action_code(
    *,
    execution_policy: dict[str, Any],
    ord_type: str,
    time_in_force: str | None,
    price_mode: str,
) -> str:
    selected = _as_optional_str(execution_policy.get("selected_action_code"))
    if selected and selected in ACTION_CONFIGS:
        return selected
    ord_type_value = str(ord_type or "limit").strip().lower()
    tif = str(time_in_force or "gtc").strip().lower()
    mode = str(price_mode or "JOIN").strip().upper()
    if ord_type_value == "best":
        return "BEST_FOK" if tif == "fok" else "BEST_IOC"
    if tif == "post_only":
        return "LIMIT_POST_ONLY"
    if tif == "ioc":
        return "LIMIT_IOC_JOIN"
    if tif == "fok":
        return "LIMIT_FOK_JOIN"
    if mode == "PASSIVE_MAKER":
        return "LIMIT_GTC_PASSIVE_MAKER"
    return "LIMIT_GTC_JOIN"


def _infer_time_in_force(*, execution_policy: dict[str, Any], ord_type: str) -> str:
    selected_tif = _as_optional_str(execution_policy.get("selected_time_in_force"))
    if selected_tif:
        return selected_tif.lower()
    ord_type_value = str(ord_type or "limit").strip().lower()
    if ord_type_value == "best":
        return "ioc"
    return "gtc"


def _build_attempt_id(*, intent_id: str, order_uuid: str | None, submitted_ts_ms: int) -> str:
    if order_uuid:
        return f"exec-{intent_id}-{order_uuid}"
    return f"exec-{intent_id}-submitted-{int(submitted_ts_ms)}"


def _journal_event_ts_ms(journal: dict[str, Any]) -> int:
    for key in ("exit_ts_ms", "entry_filled_ts_ms", "entry_submitted_ts_ms", "updated_ts"):
        value = _safe_optional_int(journal.get(key))
        if value is not None and value > 0:
            return int(value)
    return 0


def _es_bps_from_trade_action(trade_action: dict[str, Any]) -> float | None:
    value = _safe_optional_float(
        trade_action.get("expected_es", trade_action.get("expected_downside_deviation"))
    )
    if value is None:
        return None
    return float(value) * 10_000.0


def _resolve_fill_fraction(*, requested_volume: float | None, filled_volume: float | None) -> float | None:
    if requested_volume is None or requested_volume <= 0.0 or filled_volume is None:
        return None
    return min(max(float(filled_volume) / float(requested_volume), 0.0), 1.0)


def _inc_reason(reason_counts: dict[str, int], reason_code: str) -> None:
    key = str(reason_code or "UNKNOWN").strip().upper() or "UNKNOWN"
    reason_counts[key] = int(reason_counts.get(key, 0)) + 1


def _safe_optional_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_optional_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_optional_str(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill execution_attempts from trade_journal and orders.")
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--lookback-days", type=int, default=14)
    parser.add_argument("--limit", type=int, default=5000)
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    payload = backfill_execution_attempts_for_db(
        db_path=Path(str(args.db_path)),
        lookback_days=max(int(args.lookback_days), 1),
        limit=max(int(args.limit), 1),
    )
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
