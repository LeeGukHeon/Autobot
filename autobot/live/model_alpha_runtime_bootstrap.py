"""Bootstrap and provider helpers for live model_alpha runtime."""

from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
from typing import Any, Callable, Sequence


def load_quote_markets(*, public_client: Any, quote: str, allowed_markets: Sequence[str] | None = None) -> list[str]:
    payload = public_client.markets(is_details=True)
    if not isinstance(payload, list):
        return []
    prefix = f"{str(quote).strip().upper()}-"
    allowed = {
        str(item).strip().upper()
        for item in (allowed_markets or [])
        if str(item).strip()
    }
    markets: list[str] = []
    seen: set[str] = set()
    for item in payload:
        if not isinstance(item, dict):
            continue
        market = str(item.get("market", "")).strip().upper()
        if not market.startswith(prefix) or market in seen:
            continue
        if allowed and market not in allowed:
            continue
        seen.add(market)
        markets.append(market)
    return markets


def resolve_runtime_allowed_markets(*, predictor: Any) -> list[str]:
    run_dir = Path(getattr(predictor, "run_dir", "") or "")
    if not str(run_dir):
        return []
    contract_path = run_dir / "fusion_runtime_input_contract.json"
    if not contract_path.exists():
        return []
    try:
        payload = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    values = payload.get("common_runtime_markets") or []
    return [str(item).strip().upper() for item in values if str(item).strip()]


def load_market_instruments(*, public_client: Any, markets: Sequence[str]) -> dict[str, dict[str, Any]]:
    payload = public_client.orderbook_instruments(list(markets))
    if not isinstance(payload, list):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue
        market = str(item.get("market", "")).strip().upper()
        if market:
            out[market] = dict(item)
    return out


def build_live_feature_provider(
    *,
    predictor: Any,
    settings: Any,
    micro_snapshot_provider: Any,
    resolve_model_alpha_runtime_row_columns_fn: Callable[..., tuple[str, ...]],
    live_feature_provider_v3_cls: type,
    live_feature_provider_v5_cls: type,
    live_feature_provider_v4_cls: type,
    live_feature_provider_v4_native_cls: type | None = None,
) -> Any:
    feature_set = str(settings.model_alpha.feature_set).strip().lower() or "v4"
    provider_mode = str(getattr(settings, "paper_feature_provider", "") or "").strip().upper()
    resolved_family_lower = str(getattr(predictor, "model_family", "") or "").strip().lower()
    common_kwargs = {
        "feature_columns": predictor.feature_columns,
        "extra_columns": resolve_model_alpha_runtime_row_columns_fn(predictor=predictor),
        "tf": str(settings.tf).strip().lower() or "5m",
        "quote": str(settings.quote).strip().upper() or "KRW",
        "micro_snapshot_provider": micro_snapshot_provider,
        "micro_max_age_ms": int(settings.paper_live_micro_max_age_ms),
        "parquet_root": str(settings.paper_live_parquet_root),
        "candles_dataset_name": str(settings.paper_live_candles_dataset),
        "bootstrap_1m_bars": int(settings.paper_live_bootstrap_1m_bars),
    }
    if provider_mode in {"LIVE_V5", "V5"} or resolved_family_lower in {"train_v5_sequence", "train_v5_lob", "train_v5_fusion"}:
        return live_feature_provider_v5_cls(
            predictor=predictor,
            registry_root=str(settings.daemon.registry_root),
            **common_kwargs,
        )
    if feature_set == "v4" and provider_mode in {"LIVE_V4_NATIVE", "V4_NATIVE", "NATIVE_V4"}:
        if live_feature_provider_v4_native_cls is None:
            raise ValueError("runtime LIVE_V4_NATIVE provider requested without native provider class")
        return live_feature_provider_v4_native_cls(**common_kwargs)
    if feature_set == "v4":
        return live_feature_provider_v4_cls(**common_kwargs)
    return live_feature_provider_v3_cls(**common_kwargs)


def build_live_strategy(
    *,
    predictor: Any,
    settings: Any,
    feature_provider: Any,
    interval_ms_from_tf_fn: Callable[[str], int],
    model_alpha_strategy_cls: type,
) -> Any:
    interval_ms = interval_ms_from_tf_fn(settings.tf)
    return model_alpha_strategy_cls(
        predictor=predictor,
        feature_groups=None,
        settings=replace(
            settings.model_alpha,
            position=replace(
                settings.model_alpha.position,
                base_budget_quote=float(settings.per_trade_krw),
            ),
        ),
        interval_ms=interval_ms,
        live_frame_provider=lambda ts_ms, markets: feature_provider.build_frame(ts_ms=ts_ms, markets=markets),
        enable_operational_overlay=True,
    )


def snapshot_position_state(store: Any, *, safe_float_fn: Callable[..., float]) -> dict[str, dict[str, Any]]:
    positions: dict[str, dict[str, Any]] = {}
    for item in store.list_positions():
        market = str(item.get("market", "")).strip().upper()
        if not market:
            continue
        if safe_float_fn(item.get("base_amount"), default=0.0) <= 0:
            continue
        positions[market] = dict(item)
    return positions


def snapshot_open_order_markets(store: Any) -> set[str]:
    markets: set[str] = set()
    for item in store.list_orders(open_only=True):
        market = str(item.get("market", "")).strip().upper()
        if market:
            markets.add(market)
    return markets


def snapshot_open_markets(
    store: Any,
    *,
    snapshot_position_state_fn: Callable[[Any], dict[str, dict[str, Any]]],
    snapshot_open_order_markets_fn: Callable[[Any], set[str]],
) -> set[str]:
    return set(snapshot_position_state_fn(store).keys()) | snapshot_open_order_markets_fn(store)


def bootstrap_strategy_positions(
    *,
    store: Any,
    strategy: Any,
    risk_manager: Any,
    known_positions: dict[str, dict[str, Any]],
    ts_ms: int,
    resolve_strategy_entry_ts_ms_fn: Callable[..., int],
    strategy_bid_fill_fn: Callable[..., None],
    ensure_live_risk_plan_fn: Callable[..., None],
    find_latest_model_entry_intent_fn: Callable[..., dict[str, Any] | None],
    as_optional_str_fn: Callable[[object], str | None],
    activate_trade_journal_for_position_fn: Callable[..., None],
) -> None:
    for market, payload in known_positions.items():
        entry_ts_ms = resolve_strategy_entry_ts_ms_fn(store=store, market=market, position=payload, default_ts_ms=ts_ms)
        strategy_bid_fill_fn(strategy=strategy, market=market, position=payload, ts_ms=entry_ts_ms)
        ensure_live_risk_plan_fn(store=store, risk_manager=risk_manager, market=market, position=payload, ts_ms=ts_ms)
        entry_intent = find_latest_model_entry_intent_fn(store=store, market=market, position=payload)
        active_plan = max(
            store.list_risk_plans(market=market, states=("ACTIVE", "TRIGGERED", "EXITING")),
            key=lambda item: (
                int(item.get("updated_ts") or 0),
                int(item.get("created_ts") or 0),
                str(item.get("plan_id") or ""),
            ),
            default=None,
        )
        activate_trade_journal_for_position_fn(
            store=store,
            market=market,
            position=payload,
            ts_ms=ts_ms,
            entry_intent=entry_intent,
            plan_id=as_optional_str_fn((active_plan or {}).get("plan_id")),
        )


def load_predictor_for_runtime(*, store: Any, settings: Any, load_predictor_from_registry_fn: Callable[..., Any]) -> Any:
    runtime_contract = store.runtime_contract() or {}
    run_id = str(runtime_contract.get("live_runtime_model_run_id", "")).strip()
    if not run_id:
        raise ValueError("runtime contract missing live_runtime_model_run_id")
    model_family = (
        str(runtime_contract.get("model_family_resolved", "")).strip() or settings.daemon.runtime_model_family
    )
    return load_predictor_from_registry_fn(
        registry_root=Path(str(settings.daemon.registry_root)),
        model_ref=run_id,
        model_family=model_family,
    )
