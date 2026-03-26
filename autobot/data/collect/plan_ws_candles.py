"""Plan generator for Upbit websocket candle collection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from ...upbit.ws.payloads import VALID_WS_FORMATS
from ..inventory import build_candle_inventory, default_inventory_window, estimate_recent_value_by_market


DEFAULT_WS_CANDLE_TFS: tuple[str, ...] = ("1s", "1m")
VALID_WS_CANDLE_TFS: tuple[str, ...] = ("1s", "1m", "3m", "5m", "10m", "15m", "30m", "60m", "240m")
VALID_MARKET_MODES = {"fixed_list", "top_n_by_recent_value_est", "one_m_existing_only"}


@dataclass(frozen=True)
class WsCandlePlanOptions:
    parquet_root: Path = Path("data/parquet")
    base_dataset: str = "ws_candle_v1"
    market_source_dataset: str | None = None
    output_path: Path = Path("data/collect/_meta/ws_candle_plan.json")
    quote: str = "KRW"
    market_mode: str = "top_n_by_recent_value_est"
    top_n: int = 20
    fixed_markets: tuple[str, ...] | None = None
    tf_set: tuple[str, ...] = DEFAULT_WS_CANDLE_TFS
    format: str = "DEFAULT"
    is_only_snapshot: bool = False
    is_only_realtime: bool = False
    value_est_lookback_days: int = 30
    end_ts_ms: int | None = None
    enforce_no_origin_header: bool = True
    max_subscribe_messages_per_min: int = 20

    @property
    def base_dataset_root(self) -> Path:
        return self.parquet_root / self.base_dataset

    @property
    def market_source_dataset_root(self) -> Path:
        dataset_name = str(self.market_source_dataset or self.base_dataset).strip() or self.base_dataset
        return self.parquet_root / dataset_name


def generate_ws_candle_collection_plan(options: WsCandlePlanOptions) -> dict[str, Any]:
    market_mode = str(options.market_mode).strip().lower()
    if market_mode not in VALID_MARKET_MODES:
        allowed = ", ".join(sorted(VALID_MARKET_MODES))
        raise ValueError(f"market_mode must be one of: {allowed}")
    if bool(options.is_only_snapshot) and bool(options.is_only_realtime):
        raise ValueError("is_only_snapshot and is_only_realtime cannot both be true")

    quote = str(options.quote).strip().upper() or "KRW"
    end_ts_ms = int(options.end_ts_ms or int(datetime.now(timezone.utc).timestamp() * 1000))
    selected_markets, market_selection_meta = _select_markets(
        options=options,
        quote=quote,
        end_ts_ms=end_ts_ms,
    )
    tf_set = _normalize_tf_set(options.tf_set)

    fmt = str(options.format).strip().upper() or "DEFAULT"
    if fmt not in VALID_WS_FORMATS:
        allowed = ", ".join(sorted(VALID_WS_FORMATS))
        raise ValueError(f"format must be one of: {allowed}")

    subscription_types = [f"candle.{tf}" for tf in tf_set]
    plan = {
        "version": "ws_candle_plan_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "base_dataset": options.base_dataset,
        "base_dataset_root": str(options.base_dataset_root),
        "filters": {
            "quote": quote,
            "market_mode": market_mode,
            "top_n": max(int(options.top_n), 1),
            "fixed_markets": list(options.fixed_markets or ()),
            "market_source_dataset": str(options.market_source_dataset or options.base_dataset),
            "value_est_lookback_days": max(int(options.value_est_lookback_days), 1),
            "tf_set": list(tf_set),
        },
        "runtime_policy": {
            "format": fmt,
            "subscription_types": subscription_types,
            "is_only_snapshot": bool(options.is_only_snapshot),
            "is_only_realtime": bool(options.is_only_realtime),
            "dedupe_policy": "latest_by_market_tf_ts_ms",
        },
        "constraints": {
            "endpoint": "wss://api.upbit.com/websocket/v1",
            "delivery_interval_sec": 1,
            "sparse_intervals_expected": True,
            "duplicate_candle_updates_expected": True,
            "supported_tf_set": list(VALID_WS_CANDLE_TFS),
            "rate_limit_policy": {
                "websocket_connect_rps": 5,
                "websocket_message_rps": 5,
                "websocket_message_rpm": 100,
            },
            "request_structure": ["ticket", "type", "codes", "format"],
        },
        "safety": {
            "codes_uppercase_required": True,
            "enforce_no_origin_header": bool(options.enforce_no_origin_header),
            "max_subscribe_messages_per_min": max(int(options.max_subscribe_messages_per_min), 1),
        },
        "market_selection": market_selection_meta,
        "selected_markets": selected_markets,
        "codes": selected_markets,
        "summary": {
            "selected_markets": len(selected_markets),
            "codes_count": len(selected_markets),
            "tf_count": len(tf_set),
            "subscription_count": len(subscription_types),
        },
    }

    options.output_path.parent.mkdir(parents=True, exist_ok=True)
    options.output_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return plan


def _select_markets(
    *,
    options: WsCandlePlanOptions,
    quote: str,
    end_ts_ms: int,
) -> tuple[list[str], dict[str, Any]]:
    market_mode = str(options.market_mode).strip().lower()
    quote_prefix = f"{quote}-"

    window_start_ts_ms, window_end_ts_ms = default_inventory_window(
        lookback_months=24,
        end_ts_ms=end_ts_ms,
    )
    inventory = build_candle_inventory(
        options.market_source_dataset_root,
        quote=quote,
        window_start_ts_ms=window_start_ts_ms,
        window_end_ts_ms=window_end_ts_ms,
    )
    inventory_markets = sorted(
        {
            str(item.get("market", "")).strip().upper()
            for item in inventory.get("entries", [])
            if str(item.get("market", "")).strip().upper().startswith(quote_prefix)
        }
    )

    if market_mode == "fixed_list":
        fixed_markets = [
            value.strip().upper()
            for value in (options.fixed_markets or ())
            if value.strip() and value.strip().upper().startswith(quote_prefix)
        ]
        deduped = _dedupe_preserve(fixed_markets)
        return deduped, {"mode": market_mode, "count": len(deduped)}

    if market_mode == "one_m_existing_only":
        selected = sorted(
            {
                str(item.get("market", "")).strip().upper()
                for item in inventory.get("entries", [])
                if str(item.get("tf", "")).strip().lower() == "1m"
                and int(item.get("rows", 0)) > 0
                and str(item.get("market", "")).strip().upper().startswith(quote_prefix)
            }
        )
        return selected, {"mode": market_mode, "count": len(selected)}

    top_n = max(int(options.top_n), 1)
    estimates, tf_used = estimate_recent_value_by_market(
        options.market_source_dataset_root,
        end_ts_ms=end_ts_ms,
        lookback_days=max(int(options.value_est_lookback_days), 1),
        quote=quote,
    )
    ranked = sorted(estimates.items(), key=lambda item: (-float(item[1]), item[0]))
    if ranked:
        selected = [market for market, _ in ranked[:top_n]]
        return selected, {
            "mode": market_mode,
            "count": len(selected),
            "top_n": top_n,
            "value_est_tf": tf_used,
            "value_est_lookback_days": max(int(options.value_est_lookback_days), 1),
        }

    fallback = inventory_markets[:top_n]
    return fallback, {
        "mode": market_mode,
        "count": len(fallback),
        "top_n": top_n,
        "value_est_tf": None,
        "fallback": "inventory_alphabetical",
    }


def _normalize_tf_set(tf_set: tuple[str, ...] | None) -> tuple[str, ...]:
    seen: set[str] = set()
    normalized: list[str] = []
    for raw in tf_set or DEFAULT_WS_CANDLE_TFS:
        tf = str(raw).strip().lower()
        if not tf:
            continue
        if tf not in VALID_WS_CANDLE_TFS:
            allowed = ", ".join(VALID_WS_CANDLE_TFS)
            raise ValueError(f"tf_set must be within: {allowed}")
        if tf in seen:
            continue
        seen.add(tf)
        normalized.append(tf)
    if not normalized:
        raise ValueError("at least one websocket candle timeframe is required")
    return tuple(normalized)


def _dedupe_preserve(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for item in values:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped
