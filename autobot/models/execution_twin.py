from __future__ import annotations

import math
from collections import defaultdict
from typing import Any

DEFAULT_FILL_HORIZONS_MS: tuple[int, ...] = (1_000, 3_000, 10_000, 30_000, 60_000, 180_000, 300_000, 600_000)
DEFAULT_SHORTFALL_TAIL_ALPHA = 0.10
EXECUTION_TWIN_POLICY = "personalized_execution_twin_v1"


def build_execution_twin(
    *,
    attempts: list[dict[str, Any]] | None,
    horizons_ms: tuple[int, ...] = DEFAULT_FILL_HORIZONS_MS,
    shortfall_tail_alpha: float = DEFAULT_SHORTFALL_TAIL_ALPHA,
) -> dict[str, Any]:
    normalized_horizons = tuple(sorted({max(int(value), 1) for value in horizons_ms}))
    normalized_attempts = [dict(item) for item in (attempts or []) if isinstance(item, dict)]
    enriched_rows = _enrich_attempt_rows(normalized_attempts)
    by_action: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_price_mode: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_state_action: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in enriched_rows:
        action_code = _normalize_action_code(row.get("action_code"))
        price_mode = str(row.get("price_mode") or _infer_price_mode(action_code)).strip().upper() or _infer_price_mode(action_code)
        state_key = _state_bucket_key(row)
        by_action[action_code].append(row)
        by_price_mode[price_mode].append(row)
        by_state_action[(state_key, action_code)].append(row)

    return {
        "policy": EXECUTION_TWIN_POLICY,
        "status": "ready" if enriched_rows else "insufficient_history",
        "rows_total": int(len(enriched_rows)),
        "horizons_ms": [int(value) for value in normalized_horizons],
        "shortfall_tail_alpha": float(max(min(float(shortfall_tail_alpha), 0.5), 1e-6)),
        "action_stats": {
            action_code: _summarize_execution_twin_rows(
                rows=rows,
                horizons_ms=normalized_horizons,
                shortfall_tail_alpha=shortfall_tail_alpha,
            )
            for action_code, rows in sorted(by_action.items())
        },
        "price_mode_stats": {
            price_mode: _summarize_execution_twin_rows(
                rows=rows,
                horizons_ms=normalized_horizons,
                shortfall_tail_alpha=shortfall_tail_alpha,
            )
            for price_mode, rows in sorted(by_price_mode.items())
        },
        "state_action_stats": {
            f"{state_key}|{action_code}": _summarize_execution_twin_rows(
                rows=rows,
                horizons_ms=normalized_horizons,
                shortfall_tail_alpha=shortfall_tail_alpha,
            )
            for (state_key, action_code), rows in sorted(by_state_action.items())
        },
        "global_stats": _summarize_execution_twin_rows(
            rows=enriched_rows,
            horizons_ms=normalized_horizons,
            shortfall_tail_alpha=shortfall_tail_alpha,
        ),
    }


def _enrich_attempt_rows(attempts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    chains: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in attempts:
        chains[_chain_key(item)].append(dict(item))
    enriched: list[dict[str, Any]] = []
    for rows in chains.values():
        ordered = sorted(
            rows,
            key=lambda item: (
                int(item.get("submitted_ts_ms") or 0),
                int(item.get("updated_ts") or 0),
            ),
        )
        chain_count = len(ordered)
        for index, row in enumerate(ordered):
            first_fill_delay_ms = _delay_ms(row.get("submitted_ts_ms"), row.get("first_fill_ts_ms"))
            full_fill_delay_ms = _delay_ms(row.get("submitted_ts_ms"), row.get("full_fill_ts_ms"))
            fill_fraction = _safe_optional_float(row.get("fill_fraction"))
            final_state = str(row.get("final_state") or "").strip().upper()
            partial_fill = bool(row.get("partial_fill")) or (
                fill_fraction is not None and fill_fraction > 0.0 and fill_fraction < 0.999999
            ) or final_state == "PARTIAL_CANCELLED"
            cancel_event = final_state in {"MISSED", "PARTIAL_CANCELLED", "CANCELLED"} or row.get("cancelled_ts_ms") is not None
            full_fill_event = bool(row.get("full_fill")) or final_state == "FILLED" or full_fill_delay_ms is not None
            first_fill_event = first_fill_delay_ms is not None
            replace_event = index < (chain_count - 1)
            enriched_row = dict(row)
            enriched_row.update(
                {
                    "chain_attempt_count": int(chain_count),
                    "chain_attempt_index": int(index),
                    "first_fill_delay_ms": first_fill_delay_ms,
                    "full_fill_delay_ms": full_fill_delay_ms,
                    "first_fill_event": bool(first_fill_event),
                    "full_fill_event": bool(full_fill_event),
                    "partial_fill_event": bool(partial_fill),
                    "cancel_event": bool(cancel_event),
                    "replace_event": bool(replace_event),
                    "price_mode": str(row.get("price_mode") or _infer_price_mode(row.get("action_code"))).strip().upper()
                    or _infer_price_mode(row.get("action_code")),
                }
            )
            enriched.append(enriched_row)
    return enriched


def _summarize_execution_twin_rows(
    *,
    rows: list[dict[str, Any]],
    horizons_ms: tuple[int, ...],
    shortfall_tail_alpha: float,
) -> dict[str, Any]:
    sample_count = len(rows)
    first_fill_delays = [int(item["first_fill_delay_ms"]) for item in rows if item.get("first_fill_delay_ms") is not None]
    full_fill_delays = [int(item["full_fill_delay_ms"]) for item in rows if item.get("full_fill_delay_ms") is not None]
    shortfalls = [
        float(item.get("shortfall_bps"))
        for item in rows
        if item.get("shortfall_bps") is not None and math.isfinite(float(item.get("shortfall_bps")))
    ]
    summary: dict[str, Any] = {
        "sample_count": int(sample_count),
        "first_fill_count": int(sum(1 for item in rows if bool(item.get("first_fill_event")))),
        "full_fill_count": int(sum(1 for item in rows if bool(item.get("full_fill_event")))),
        "partial_fill_count": int(sum(1 for item in rows if bool(item.get("partial_fill_event")))),
        "cancel_count": int(sum(1 for item in rows if bool(item.get("cancel_event")))),
        "replace_count": int(sum(1 for item in rows if bool(item.get("replace_event")))),
        "first_fill_probability": _rate(rows, "first_fill_event"),
        "full_fill_probability": _rate(rows, "full_fill_event"),
        "partial_fill_probability": _rate(rows, "partial_fill_event"),
        "cancel_probability": _rate(rows, "cancel_event"),
        "replace_probability": _rate(rows, "replace_event"),
        "mean_time_to_first_fill_ms": _mean(first_fill_delays),
        "mean_time_to_full_fill_ms": _mean(full_fill_delays),
        "mean_shortfall_bps": _mean(shortfalls),
        "expected_shortfall_bps": _expected_shortfall(shortfalls, alpha=shortfall_tail_alpha),
    }
    for horizon_ms in horizons_ms:
        summary[f"p_first_fill_within_{int(horizon_ms)}ms"] = _rate_within_delay(first_fill_delays, horizon_ms, sample_count)
        summary[f"p_full_fill_within_{int(horizon_ms)}ms"] = _rate_within_delay(full_fill_delays, horizon_ms, sample_count)
    return summary


def _chain_key(row: dict[str, Any]) -> str:
    for key in ("journal_id", "intent_id", "attempt_id"):
        text = str(row.get(key) or "").strip()
        if text:
            return f"{key}:{text}"
    return f"attempt:{id(row)}"


def _delay_ms(start_value: Any, end_value: Any) -> int | None:
    start_ts = _safe_optional_int(start_value)
    end_ts = _safe_optional_int(end_value)
    if start_ts is None or end_ts is None:
        return None
    return max(int(end_ts) - int(start_ts), 0)


def _normalize_action_code(value: Any) -> str:
    text = str(value or "").strip().upper()
    return text or "UNKNOWN"


def _state_bucket_key(payload: dict[str, Any] | None) -> str:
    doc = dict(payload or {})
    spread_bps = _safe_optional_float(doc.get("spread_bps"))
    depth_krw = _safe_optional_float(doc.get("depth_top5_notional_krw"))
    snapshot_age_ms = _safe_optional_float(doc.get("snapshot_age_ms"))
    expected_edge_bps = _safe_optional_float(doc.get("expected_edge_bps"))
    return "|".join(
        [
            _spread_bucket(spread_bps),
            _depth_bucket(depth_krw),
            _age_bucket(snapshot_age_ms),
            _edge_bucket(expected_edge_bps),
        ]
    )


def _infer_price_mode(action_code: Any) -> str:
    text = _normalize_action_code(action_code)
    if "PASSIVE_MAKER" in text or "POST_ONLY" in text:
        return "PASSIVE_MAKER"
    if "CROSS" in text or text.startswith("BEST_"):
        return "CROSS_1T"
    if "JOIN" in text:
        return "JOIN"
    return "JOIN"


def _spread_bucket(value: float | None) -> str:
    if value is None:
        return "spread_unknown"
    if float(value) <= 5.0:
        return "spread_tight"
    if float(value) <= 15.0:
        return "spread_mid"
    return "spread_wide"


def _depth_bucket(value: float | None) -> str:
    if value is None:
        return "depth_unknown"
    if float(value) >= 2_000_000.0:
        return "depth_deep"
    if float(value) >= 500_000.0:
        return "depth_mid"
    return "depth_shallow"


def _age_bucket(value: float | None) -> str:
    if value is None:
        return "age_unknown"
    if float(value) <= 500.0:
        return "age_fresh"
    if float(value) <= 2_000.0:
        return "age_warm"
    return "age_stale"


def _edge_bucket(value: float | None) -> str:
    if value is None:
        return "edge_unknown"
    if float(value) <= 5.0:
        return "edge_weak"
    if float(value) <= 15.0:
        return "edge_mid"
    return "edge_strong"


def _safe_optional_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_optional_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _rate(rows: list[dict[str, Any]], key: str) -> float:
    if not rows:
        return 0.0
    hits = sum(1 for item in rows if bool(item.get(key)))
    return float(hits) / float(len(rows))


def _rate_within_delay(delays: list[int], horizon_ms: int, sample_count: int) -> float:
    if sample_count <= 0:
        return 0.0
    hits = sum(1 for delay in delays if int(delay) <= int(horizon_ms))
    return float(hits) / float(sample_count)


def _mean(values: list[float | int]) -> float | None:
    clean = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    if not clean:
        return None
    return float(sum(clean) / float(len(clean)))


def _expected_shortfall(values: list[float], *, alpha: float) -> float | None:
    clean = sorted(float(value) for value in values if math.isfinite(float(value)))
    if not clean:
        return None
    tail_fraction = max(min(float(alpha), 0.5), 1e-6)
    tail_count = max(int(math.ceil(len(clean) * tail_fraction)), 1)
    tail_values = clean[-tail_count:]
    return float(sum(tail_values) / float(len(tail_values)))
