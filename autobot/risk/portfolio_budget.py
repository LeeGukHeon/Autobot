"""Minimal white-box portfolio budget engine for live entry sizing."""

from __future__ import annotations

from dataclasses import is_dataclass
from typing import Any

from .confidence_monitor import SUPPRESSOR_RESET_CHECKPOINT
from autobot.common.portfolio_signal_haircuts import (
    resolve_portfolio_signal_haircuts as _resolve_portfolio_signal_haircuts,
)


def resolve_portfolio_risk_budget(
    *,
    store: Any,
    market: str,
    side: str,
    target_notional_quote: float | None,
    base_budget_quote: float | None,
    quote_free: float | None,
    min_total_krw: float,
    effective_max_positions: int,
    rollout_mode: str,
    state_features: dict[str, Any] | None = None,
    micro_state: dict[str, Any] | None = None,
    uncertainty: float | None = None,
    expected_return_bps: float | None = None,
    expected_es_bps: float | None = None,
    tradability_prob: float | None = None,
    alpha_lcb_bps: float | None = None,
    runtime_model_run_id: str | None = None,
    platform_quality_budget: dict[str, Any] | None = None,
) -> dict[str, Any]:
    market_value = str(market).strip().upper()
    side_value = str(side).strip().lower()
    target_quote = max(float(target_notional_quote or 0.0), 0.0)
    base_budget = max(float(base_budget_quote or 0.0), 0.0)
    quote_free_value = max(float(quote_free or 0.0), 0.0)
    min_total_value = max(float(min_total_krw), 0.0)
    if side_value != "bid":
        return {
            "enabled": False,
            "allowed": True,
            "risk_reason_codes": [],
            "primary_reason_code": "",
            "target_notional_quote": target_quote,
            "max_notional_quote": target_quote,
            "resolved_notional_quote": target_quote,
            "position_budget_fraction": (target_quote / base_budget) if base_budget > 0.0 and target_quote > 0.0 else None,
        }

    exposure = summarize_portfolio_exposure(store=store, decision_market=market_value, decision_target_notional_quote=target_quote)
    current_total = float(exposure["current_total_cash_at_risk_quote"])
    cluster_utilization = dict(exposure["cluster_utilization"])
    current_cluster = float(cluster_utilization.get("decision_cluster_current_notional_quote", 0.0) or 0.0)

    gross_slot_count = max(int(effective_max_positions), 1)
    gross_cap_quote = max(base_budget * float(gross_slot_count), min_total_value)
    rollout_mode_value = str(rollout_mode).strip().lower()
    cluster_slot_count = 1 if rollout_mode_value == "canary" else max(int(effective_max_positions), 1)
    cluster_cap_quote = max(base_budget * float(cluster_slot_count), min_total_value)

    gross_budget_remaining_quote = max(gross_cap_quote - current_total, 0.0)
    cluster_budget_remaining_quote = max(cluster_cap_quote - current_cluster, 0.0)
    structural_cap_quote = min(gross_budget_remaining_quote, cluster_budget_remaining_quote, quote_free_value)

    confidence_haircut = _confidence_haircut(uncertainty)
    alpha_strength_haircut, alpha_reason_codes = _alpha_strength_haircut(
        expected_return_bps=expected_return_bps,
        alpha_lcb_bps=alpha_lcb_bps,
    )
    expected_es_haircut, expected_es_reason_codes = _expected_es_haircut(
        expected_return_bps=expected_return_bps,
        expected_es_bps=expected_es_bps,
    )
    tradability_haircut, tradability_reason_codes = _tradability_haircut(tradability_prob)
    liquidity_haircut, liquidity_reason_codes = _liquidity_haircut(
        target_notional_quote=target_quote,
        micro_state=state_features if isinstance(state_features, dict) and state_features else micro_state,
    )
    data_quality_haircut, data_quality_reason_codes = _data_quality_haircut(platform_quality_budget)
    streak_haircut, streak_reason_codes = _recent_loss_streak_haircut(
        store=store,
        runtime_model_run_id=runtime_model_run_id,
    )

    soft_haircut = min(
        confidence_haircut,
        alpha_strength_haircut,
        expected_es_haircut,
        tradability_haircut,
        liquidity_haircut,
        data_quality_haircut,
        streak_haircut,
    )
    max_notional_quote = max(structural_cap_quote, 0.0)
    structural_resolved_notional_quote = max(min(target_quote, max_notional_quote), 0.0)
    diagnostic_resolved_notional_quote = structural_resolved_notional_quote * float(soft_haircut)
    diagnostic_resolved_notional_quote = min(diagnostic_resolved_notional_quote, max_notional_quote)
    diagnostic_resolved_notional_quote = max(diagnostic_resolved_notional_quote, 0.0)

    reason_codes: list[str] = []
    if gross_budget_remaining_quote + 1e-12 < target_quote and "PORTFOLIO_GROSS_BUDGET_CLAMP" not in reason_codes:
        reason_codes.append("PORTFOLIO_GROSS_BUDGET_CLAMP")
    if cluster_budget_remaining_quote + 1e-12 < target_quote and "PORTFOLIO_CLUSTER_BUDGET_CLAMP" not in reason_codes:
        reason_codes.append("PORTFOLIO_CLUSTER_BUDGET_CLAMP")
    if quote_free_value + 1e-12 < target_quote and "PORTFOLIO_AVAILABLE_QUOTE_CLAMP" not in reason_codes:
        reason_codes.append("PORTFOLIO_AVAILABLE_QUOTE_CLAMP")
    if gross_budget_remaining_quote + 1e-12 < min(target_quote, min_total_value if min_total_value > 0.0 else target_quote):
        reason_codes.append("PORTFOLIO_GROSS_BUDGET_EXHAUSTED")
    if cluster_budget_remaining_quote + 1e-12 < min(target_quote, min_total_value if min_total_value > 0.0 else target_quote):
        if "PORTFOLIO_CLUSTER_BUDGET_EXHAUSTED" not in reason_codes:
            reason_codes.append("PORTFOLIO_CLUSTER_BUDGET_EXHAUSTED")
    if quote_free_value + 1e-12 < min(target_quote, min_total_value if min_total_value > 0.0 else target_quote):
        reason_codes.append("PORTFOLIO_AVAILABLE_QUOTE_EXHAUSTED")
    for code in liquidity_reason_codes + data_quality_reason_codes + streak_reason_codes:
        if code not in reason_codes:
            reason_codes.append(code)
    for code in alpha_reason_codes + expected_es_reason_codes + tradability_reason_codes:
        if code not in reason_codes:
            reason_codes.append(code)
    if confidence_haircut < 0.999999 and "PORTFOLIO_CONFIDENCE_HAIRCUT" not in reason_codes:
        reason_codes.append("PORTFOLIO_CONFIDENCE_HAIRCUT")

    enforcement_mode = "enforced"
    warning_only = False
    warning_reason_codes: list[str] = []
    resolved_notional_quote = float(diagnostic_resolved_notional_quote)

    allowed = bool(resolved_notional_quote >= max(min_total_value, 1.0))
    primary_reason_code = ""
    if not allowed:
        if "PORTFOLIO_GROSS_BUDGET_EXHAUSTED" in reason_codes:
            primary_reason_code = "PORTFOLIO_GROSS_BUDGET_EXHAUSTED"
        elif "PORTFOLIO_CLUSTER_BUDGET_EXHAUSTED" in reason_codes:
            primary_reason_code = "PORTFOLIO_CLUSTER_BUDGET_EXHAUSTED"
        elif "PORTFOLIO_AVAILABLE_QUOTE_EXHAUSTED" in reason_codes:
            primary_reason_code = "PORTFOLIO_AVAILABLE_QUOTE_EXHAUSTED"
        else:
            primary_reason_code = "PORTFOLIO_BUDGET_BELOW_MIN_TOTAL"

    position_budget_fraction = None
    if base_budget > 0.0 and resolved_notional_quote > 0.0:
        position_budget_fraction = float(resolved_notional_quote) / float(base_budget)

    cluster_utilization["gross_cap_quote"] = float(gross_cap_quote)
    cluster_utilization["cluster_cap_quote"] = float(cluster_cap_quote)
    cluster_utilization["gross_budget_remaining_quote"] = float(gross_budget_remaining_quote)
    cluster_utilization["cluster_budget_remaining_quote"] = float(cluster_budget_remaining_quote)

    return {
        "enabled": True,
        "allowed": bool(allowed),
        "enforcement_mode": str(enforcement_mode),
        "warning_only": bool(warning_only),
        "warning_reason_codes": warning_reason_codes,
        "primary_reason_code": str(primary_reason_code).strip(),
        "risk_reason_codes": reason_codes,
        "market": market_value,
        "side": side_value,
        "target_notional_quote": float(target_quote),
        "resolved_notional_quote": float(resolved_notional_quote),
        "diagnostic_resolved_notional_quote": float(diagnostic_resolved_notional_quote),
        "structural_resolved_notional_quote": float(structural_resolved_notional_quote),
        "max_notional_quote": float(max_notional_quote),
        "position_budget_fraction": position_budget_fraction,
        "gross_cap_quote": float(gross_cap_quote),
        "gross_budget_remaining_quote": float(gross_budget_remaining_quote),
        "cluster_cap_quote": float(cluster_cap_quote),
        "cluster_budget_remaining_quote": float(cluster_budget_remaining_quote),
        "available_quote_to_use": float(quote_free_value),
        "budget_clamped": bool(resolved_notional_quote + 1e-12 < target_quote),
        "soft_budget_clamped": bool(diagnostic_resolved_notional_quote + 1e-12 < target_quote),
        "confidence_haircut": float(confidence_haircut),
        "alpha_strength_haircut": float(alpha_strength_haircut),
        "expected_es_haircut": float(expected_es_haircut),
        "tradability_haircut": float(tradability_haircut),
        "expected_return_bps": _safe_optional_float(expected_return_bps),
        "expected_es_bps": _safe_optional_float(expected_es_bps),
        "tradability_prob": _safe_optional_float(tradability_prob),
        "alpha_lcb_bps": _safe_optional_float(alpha_lcb_bps),
        "liquidity_haircut": float(liquidity_haircut),
        "data_quality_haircut": float(data_quality_haircut),
        "recent_loss_streak_haircut": float(streak_haircut),
        "platform_quality_budget": dict(platform_quality_budget or {}),
        "cluster_utilization": cluster_utilization,
        "current_total_cash_at_risk_quote": float(current_total),
        "projected_total_cash_at_risk_quote": float(current_total + resolved_notional_quote),
    }


def summarize_portfolio_exposure(
    *,
    store: Any,
    decision_market: str,
    decision_target_notional_quote: float | None,
) -> dict[str, Any]:
    positions = []
    open_orders = []
    if hasattr(store, "list_positions"):
        try:
            positions = list(store.list_positions())
        except Exception:
            positions = []
    if hasattr(store, "list_orders"):
        try:
            open_orders = list(store.list_orders(open_only=True))
        except TypeError:
            try:
                open_orders = list(store.list_orders())
            except Exception:
                open_orders = []
        except Exception:
            open_orders = []
    elif hasattr(store, "list_open_orders"):
        try:
            open_orders = list(store.list_open_orders())
        except Exception:
            open_orders = []

    current_total = 0.0
    cluster_map: dict[str, dict[str, Any]] = {}
    for row in positions:
        market = _optional_upper_text(_field(row, "market"))
        base_amount = max(_safe_optional_float(_field(row, "base_amount")) or 0.0, 0.0)
        avg_entry_price = max(_safe_optional_float(_field(row, "avg_entry_price")) or 0.0, 0.0)
        notional_quote = float(base_amount) * float(avg_entry_price)
        if not market or notional_quote <= 0.0:
            continue
        current_total += notional_quote
        cluster_id = classify_market_cluster(market)
        bucket = cluster_map.setdefault(
            cluster_id,
            {
                "cluster_id": cluster_id,
                "position_notional_quote": 0.0,
                "open_bid_order_notional_quote": 0.0,
                "gross_notional_quote": 0.0,
                "position_count": 0,
                "open_bid_order_count": 0,
                "markets": [],
            },
        )
        bucket["position_notional_quote"] = float(bucket["position_notional_quote"]) + float(notional_quote)
        bucket["gross_notional_quote"] = float(bucket["gross_notional_quote"]) + float(notional_quote)
        bucket["position_count"] = int(bucket["position_count"]) + 1
        if market not in bucket["markets"]:
            bucket["markets"].append(market)

    for row in open_orders:
        market = _optional_upper_text(_field(row, "market"))
        exposure_quote = estimate_open_bid_order_cash_at_risk_quote(row)
        if not market or exposure_quote <= 0.0:
            continue
        current_total += exposure_quote
        cluster_id = classify_market_cluster(market)
        bucket = cluster_map.setdefault(
            cluster_id,
            {
                "cluster_id": cluster_id,
                "position_notional_quote": 0.0,
                "open_bid_order_notional_quote": 0.0,
                "gross_notional_quote": 0.0,
                "position_count": 0,
                "open_bid_order_count": 0,
                "markets": [],
            },
        )
        bucket["open_bid_order_notional_quote"] = float(bucket["open_bid_order_notional_quote"]) + float(exposure_quote)
        bucket["gross_notional_quote"] = float(bucket["gross_notional_quote"]) + float(exposure_quote)
        bucket["open_bid_order_count"] = int(bucket["open_bid_order_count"]) + 1
        if market not in bucket["markets"]:
            bucket["markets"].append(market)

    decision_cluster_id = classify_market_cluster(decision_market)
    projected_total = float(current_total) + max(float(decision_target_notional_quote or 0.0), 0.0)
    decision_cluster_current = float(cluster_map.get(decision_cluster_id, {}).get("gross_notional_quote", 0.0) or 0.0)
    decision_cluster_projected = decision_cluster_current + max(float(decision_target_notional_quote or 0.0), 0.0)
    cluster_rows = []
    for cluster_id in sorted(cluster_map):
        item = dict(cluster_map[cluster_id])
        item["markets"] = sorted(str(value).strip().upper() for value in (item.get("markets") or []))
        cluster_rows.append(item)
    if decision_cluster_id not in {str(item.get("cluster_id")) for item in cluster_rows}:
        cluster_rows.append(
            {
                "cluster_id": decision_cluster_id,
                "position_notional_quote": 0.0,
                "open_bid_order_notional_quote": 0.0,
                "gross_notional_quote": 0.0,
                "position_count": 0,
                "open_bid_order_count": 0,
                "markets": [],
            }
        )
        cluster_rows = sorted(cluster_rows, key=lambda item: str(item.get("cluster_id") or ""))

    return {
        "current_total_cash_at_risk_quote": float(current_total),
        "projected_total_cash_at_risk_quote": float(projected_total),
        "cluster_utilization": {
            "clusters": cluster_rows,
            "decision_cluster_id": decision_cluster_id,
            "decision_cluster_current_notional_quote": float(decision_cluster_current),
            "decision_cluster_projected_notional_quote": float(decision_cluster_projected),
        },
    }


def resolve_portfolio_signal_haircuts(
    *,
    uncertainty: float | None = None,
    expected_return_bps: float | None = None,
    expected_es_bps: float | None = None,
    tradability_prob: float | None = None,
    alpha_lcb_bps: float | None = None,
) -> dict[str, Any]:
    return _resolve_portfolio_signal_haircuts(
        uncertainty=uncertainty,
        expected_return_bps=expected_return_bps,
        expected_es_bps=expected_es_bps,
        tradability_prob=tradability_prob,
        alpha_lcb_bps=alpha_lcb_bps,
    )


def classify_market_cluster(market: str) -> str:
    market_value = str(market).strip().upper()
    if not market_value or "-" not in market_value:
        return "UNKNOWN"
    _, base = market_value.split("-", 1)
    if base == "BTC":
        return "BTC_LED"
    if base == "ETH":
        return "ETH_LED"
    return "ALT_CLUSTER"


def estimate_open_bid_order_cash_at_risk_quote(order: dict[str, Any] | None) -> float:
    payload = order
    if _optional_lower_text(_field(payload, "side")) != "bid":
        return 0.0
    locked_quote = _safe_optional_float(_field(payload, "locked_quote"))
    if locked_quote is not None and locked_quote > 0.0:
        return max(float(locked_quote), 0.0)
    ord_type = _optional_lower_text(_field(payload, "ord_type")) or ""
    executed_funds = max(_safe_optional_float(_field(payload, "executed_funds")) or 0.0, 0.0)
    remaining_fee = _safe_optional_float(_field(payload, "remaining_fee"))
    reserved_fee = _safe_optional_float(_field(payload, "reserved_fee"))
    paid_fee = max(_safe_optional_float(_field(payload, "paid_fee")) or 0.0, 0.0)
    fee_reserve_quote = 0.0
    if remaining_fee is not None:
        fee_reserve_quote = max(float(remaining_fee), 0.0)
    elif reserved_fee is not None:
        fee_reserve_quote = max(float(reserved_fee) - float(paid_fee), 0.0)

    if ord_type == "best":
        quote_budget = max(_safe_optional_float(_field(payload, "price")) or 0.0, 0.0)
        return max(float(quote_budget) - float(executed_funds), 0.0) + float(fee_reserve_quote)

    price = max(_safe_optional_float(_field(payload, "price")) or 0.0, 0.0)
    volume_req = max(_safe_optional_float(_field(payload, "volume_req")) or 0.0, 0.0)
    volume_filled = max(_safe_optional_float(_field(payload, "volume_filled")) or 0.0, 0.0)
    remaining_volume = max(float(volume_req) - float(volume_filled), 0.0)
    return max(float(price) * float(remaining_volume), 0.0) + float(fee_reserve_quote)


def _confidence_haircut(uncertainty: float | None) -> float:
    resolved = _safe_optional_float(uncertainty)
    if resolved is None:
        return 1.0
    return max(min(1.0 / (1.0 + abs(float(resolved))), 1.0), 0.25)


def _alpha_strength_haircut(
    *,
    expected_return_bps: float | None,
    alpha_lcb_bps: float | None,
) -> tuple[float, list[str]]:
    reasons: list[str] = []
    lcb = _safe_optional_float(alpha_lcb_bps)
    edge = _safe_optional_float(expected_return_bps)
    if lcb is not None:
        if float(lcb) <= 0.0:
            reasons.append("PORTFOLIO_ALPHA_LCB_HAIRCUT")
            return 0.50, reasons
        if float(lcb) < 10.0:
            reasons.append("PORTFOLIO_ALPHA_LCB_HAIRCUT")
            return 0.75, reasons
        return 1.0, reasons
    if edge is not None and float(edge) < 10.0:
        reasons.append("PORTFOLIO_LOW_EXPECTED_RETURN_HAIRCUT")
        return 0.85, reasons
    return 1.0, reasons


def _expected_es_haircut(
    *,
    expected_return_bps: float | None,
    expected_es_bps: float | None,
) -> tuple[float, list[str]]:
    reasons: list[str] = []
    expected_es_value = _safe_optional_float(expected_es_bps)
    expected_return_value = _safe_optional_float(expected_return_bps)
    if expected_es_value is None or expected_es_value <= 0.0:
        return 1.0, reasons
    if expected_return_value is None or expected_return_value <= 0.0:
        reasons.append("PORTFOLIO_EXPECTED_ES_HAIRCUT")
        return 0.50, reasons
    loss_ratio = float(expected_es_value) / max(float(expected_return_value), 1e-12)
    if loss_ratio >= 2.0:
        reasons.append("PORTFOLIO_EXPECTED_ES_HAIRCUT")
        return 0.50, reasons
    if loss_ratio >= 1.0:
        reasons.append("PORTFOLIO_EXPECTED_ES_HAIRCUT")
        return 0.75, reasons
    return 1.0, reasons


def _tradability_haircut(tradability_prob: float | None) -> tuple[float, list[str]]:
    reasons: list[str] = []
    resolved = _safe_optional_float(tradability_prob)
    if resolved is None:
        return 1.0, reasons
    value = max(min(float(resolved), 1.0), 0.0)
    if value < 0.25:
        reasons.append("PORTFOLIO_TRADABILITY_HAIRCUT")
        return 0.25, reasons
    if value < 0.50:
        reasons.append("PORTFOLIO_TRADABILITY_HAIRCUT")
        return 0.50, reasons
    if value < 0.75:
        reasons.append("PORTFOLIO_TRADABILITY_HAIRCUT")
        return 0.75, reasons
    return 1.0, reasons


def _liquidity_haircut(
    *,
    target_notional_quote: float,
    micro_state: dict[str, Any] | None,
) -> tuple[float, list[str]]:
    state = dict(micro_state or {})
    haircut = 1.0
    reason_codes: list[str] = []
    spread_bps = _safe_optional_float(state.get("spread_bps"))
    if spread_bps is None:
        spread_bps = _safe_optional_float(state.get("m_spread_proxy"))
    if spread_bps is not None:
        if float(spread_bps) >= 40.0:
            haircut = min(haircut, 0.5)
            reason_codes.append("PORTFOLIO_SPREAD_HAIRCUT")
        elif float(spread_bps) >= 20.0:
            haircut = min(haircut, 0.75)
            reason_codes.append("PORTFOLIO_SPREAD_HAIRCUT")

    depth = _safe_optional_float(state.get("depth_top5_notional_krw"))
    if depth is None:
        depth = _safe_optional_float(state.get("m_depth_top5_notional_krw"))
    if depth is not None and target_notional_quote > 0.0:
        depth_ratio = float(depth) / float(target_notional_quote)
        if depth_ratio < 3.0:
            haircut = min(haircut, max(depth_ratio / 3.0, 0.25))
            if "PORTFOLIO_LIQUIDITY_HAIRCUT" not in reason_codes:
                reason_codes.append("PORTFOLIO_LIQUIDITY_HAIRCUT")

    volatility = _safe_optional_float(state.get("rv_36"))
    if volatility is None:
        volatility = _safe_optional_float(state.get("atr_pct_14"))
    if volatility is not None and float(volatility) >= 0.05:
        haircut = min(haircut, 0.75)
        if "PORTFOLIO_VOLATILITY_HAIRCUT" not in reason_codes:
            reason_codes.append("PORTFOLIO_VOLATILITY_HAIRCUT")
    return float(haircut), reason_codes


def _recent_loss_streak_haircut(
    *,
    store: Any,
    runtime_model_run_id: str | None,
) -> tuple[float, list[str]]:
    run_id = str(runtime_model_run_id or "").strip()
    if not run_id or not hasattr(store, "list_trade_journal"):
        return 1.0, []
    try:
        rows = list(store.list_trade_journal(statuses=("CLOSED",), limit=8))
    except Exception:
        return 1.0, []
    reset_baseline_ts_ms = None
    if hasattr(store, "get_checkpoint"):
        checkpoint = store.get_checkpoint(name=SUPPRESSOR_RESET_CHECKPOINT)
        payload = dict((checkpoint or {}).get("payload") or {})
        checkpoint_run_id = str(payload.get("run_id") or "").strip()
        if not checkpoint_run_id or checkpoint_run_id == run_id:
            reset_baseline_ts_ms = _safe_optional_float(payload.get("history_reset_ts_ms"))
    streak = 0
    for row in rows:
        entry_meta = dict((row or {}).get("entry_meta") or {})
        runtime = dict(entry_meta.get("runtime") or {}) if isinstance(entry_meta.get("runtime"), dict) else {}
        if str(runtime.get("live_runtime_model_run_id", "")).strip() != run_id:
            continue
        exit_ts_ms = _safe_optional_float((row or {}).get("exit_ts_ms")) or _safe_optional_float((row or {}).get("updated_ts"))
        if reset_baseline_ts_ms is not None and exit_ts_ms is not None and float(exit_ts_ms) <= float(reset_baseline_ts_ms):
            continue
        pnl_quote = _safe_optional_float((row or {}).get("realized_pnl_quote"))
        if pnl_quote is None:
            break
        if float(pnl_quote) <= 0.0:
            streak += 1
            continue
        break
    if streak >= 3:
        return 0.5, ["PORTFOLIO_RECENT_LOSS_STREAK_HAIRCUT"]
    if streak >= 2:
        return 0.75, ["PORTFOLIO_RECENT_LOSS_STREAK_HAIRCUT"]
    return 1.0, []


def _data_quality_haircut(platform_quality_budget: dict[str, Any] | None) -> tuple[float, list[str]]:
    payload = dict(platform_quality_budget or {})
    if not payload:
        return 1.0, []
    haircut = 1.0
    reason_codes: list[str] = []

    validate_fail_files = _safe_optional_float(payload.get("validate_fail_files"))
    parity_hard_gate_fail_count = _safe_optional_float(payload.get("parity_hard_gate_fail_count"))
    parity_missing_feature_columns_total = _safe_optional_float(payload.get("parity_missing_feature_columns_total"))
    leakage_smoke = str(payload.get("leakage_smoke") or "").strip().upper()
    if (
        (validate_fail_files is not None and validate_fail_files > 0.0)
        or (parity_hard_gate_fail_count is not None and parity_hard_gate_fail_count > 0.0)
        or (parity_missing_feature_columns_total is not None and parity_missing_feature_columns_total > 0.0)
        or (leakage_smoke and leakage_smoke != "PASS")
    ):
        return 0.0, ["PORTFOLIO_DATA_QUALITY_HARD_BLOCK"]

    micro_available_ratio = _safe_optional_float(payload.get("micro_available_ratio"))
    if micro_available_ratio is not None and micro_available_ratio < 0.95:
        haircut = min(haircut, max(float(micro_available_ratio) / 0.95, 0.25))
        reason_codes.append("PORTFOLIO_DATA_QUALITY_MICRO_AVAILABILITY_HAIRCUT")

    parse_ok_ratio = _safe_optional_float(payload.get("micro_validate_parse_ok_ratio"))
    if parse_ok_ratio is not None and parse_ok_ratio < 0.99:
        haircut = min(haircut, max(float(parse_ok_ratio) / 0.99, 0.25))
        reason_codes.append("PORTFOLIO_DATA_QUALITY_PARSE_OK_HAIRCUT")

    short_trade_coverage_ratio = _safe_optional_float(payload.get("micro_validate_short_trade_coverage_ratio"))
    if short_trade_coverage_ratio is not None:
        if short_trade_coverage_ratio >= 0.90:
            haircut = min(haircut, 0.50)
            reason_codes.append("PORTFOLIO_DATA_QUALITY_SHORT_TRADE_COVERAGE_SEVERE")
        elif short_trade_coverage_ratio >= 0.50:
            haircut = min(haircut, 0.75)
            reason_codes.append("PORTFOLIO_DATA_QUALITY_SHORT_TRADE_COVERAGE_HIGH")

    short_book_coverage_ratio = _safe_optional_float(payload.get("micro_validate_short_book_coverage_ratio"))
    if short_book_coverage_ratio is not None:
        if short_book_coverage_ratio >= 0.90:
            haircut = min(haircut, 0.50)
            reason_codes.append("PORTFOLIO_DATA_QUALITY_SHORT_BOOK_COVERAGE_SEVERE")
        elif short_book_coverage_ratio >= 0.50:
            haircut = min(haircut, 0.75)
            reason_codes.append("PORTFOLIO_DATA_QUALITY_SHORT_BOOK_COVERAGE_HIGH")

    synth_ratio_p90 = _safe_optional_float(payload.get("one_m_synth_ratio_p90"))
    if synth_ratio_p90 is not None:
        if synth_ratio_p90 >= 0.75:
            haircut = min(haircut, 0.5)
            reason_codes.append("PORTFOLIO_DATA_QUALITY_SYNTH_P90_SEVERE")
        elif synth_ratio_p90 >= 0.50:
            haircut = min(haircut, 0.75)
            reason_codes.append("PORTFOLIO_DATA_QUALITY_SYNTH_P90_HIGH")

    dropped_no_micro = _safe_optional_float(payload.get("rows_dropped_no_micro"))
    if dropped_no_micro is not None and dropped_no_micro > 0.0:
        haircut = min(haircut, 0.75)
        reason_codes.append("PORTFOLIO_DATA_QUALITY_MICRO_DROPS_PRESENT")

    return float(haircut), list(dict.fromkeys(reason_codes))


def _safe_optional_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _field(row: Any, name: str) -> Any:
    if row is None:
        return None
    if isinstance(row, dict):
        return row.get(name)
    if is_dataclass(row):
        return getattr(row, name, None)
    return getattr(row, name, None)


def _optional_upper_text(value: Any) -> str:
    return str(value or "").strip().upper()


def _optional_lower_text(value: Any) -> str:
    return str(value or "").strip().lower()
