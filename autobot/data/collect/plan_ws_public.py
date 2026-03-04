"""Plan generator for Upbit public websocket (trade/orderbook) collection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from ...upbit.ws.payloads import VALID_WS_FORMATS
from ..inventory import build_candle_inventory, default_inventory_window, estimate_recent_value_by_market


DEFAULT_WS_CHANNELS: tuple[str, ...] = ("trade", "orderbook")
VALID_MARKET_MODES = {"fixed_list", "top_n_by_recent_value_est", "one_m_existing_only"}
VALID_CHANNELS = {"trade", "orderbook"}


@dataclass(frozen=True)
class WsPublicPlanOptions:
    parquet_root: Path = Path("data/parquet")
    base_dataset: str = "candles_v1"
    output_path: Path = Path("data/raw_ws/upbit/_meta/ws_public_plan.json")
    quote: str = "KRW"
    market_mode: str = "top_n_by_recent_value_est"
    top_n: int = 20
    fixed_markets: tuple[str, ...] | None = None
    channels: tuple[str, ...] = DEFAULT_WS_CHANNELS
    format: str = "DEFAULT"
    orderbook_topk: int = 5
    orderbook_level: str | int | None = 0
    orderbook_min_write_interval_ms: int = 200
    trade_store_all: bool = True
    value_est_lookback_days: int = 30
    end_ts_ms: int | None = None
    enforce_no_origin_header: bool = True
    max_subscribe_messages_per_min: int = 5

    @property
    def base_dataset_root(self) -> Path:
        return self.parquet_root / self.base_dataset


def generate_ws_public_collection_plan(options: WsPublicPlanOptions) -> dict[str, Any]:
    market_mode = str(options.market_mode).strip().lower()
    if market_mode not in VALID_MARKET_MODES:
        allowed = ", ".join(sorted(VALID_MARKET_MODES))
        raise ValueError(f"market_mode must be one of: {allowed}")

    quote = str(options.quote).strip().upper() or "KRW"
    end_ts_ms = int(options.end_ts_ms or int(datetime.now(timezone.utc).timestamp() * 1000))
    selected_markets, market_selection_meta = _select_markets(
        options=options,
        quote=quote,
        end_ts_ms=end_ts_ms,
    )
    channels = _normalize_channels(options.channels)

    fmt = str(options.format).strip().upper() or "DEFAULT"
    if fmt not in VALID_WS_FORMATS:
        allowed = ", ".join(sorted(VALID_WS_FORMATS))
        raise ValueError(f"format must be one of: {allowed}")

    plan = {
        "version": "t13.1c-ws-public-plan-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "base_dataset": options.base_dataset,
        "base_dataset_root": str(options.base_dataset_root),
        "filters": {
            "quote": quote,
            "market_mode": market_mode,
            "top_n": max(int(options.top_n), 1),
            "fixed_markets": list(options.fixed_markets or ()),
            "value_est_lookback_days": max(int(options.value_est_lookback_days), 1),
            "channels": list(channels),
        },
        "runtime_policy": {
            "format": fmt,
            "trade_store_all": bool(options.trade_store_all),
            "orderbook_topk": max(int(options.orderbook_topk), 1),
            "orderbook_level": _normalize_level(options.orderbook_level),
            "orderbook_min_write_interval_ms": max(int(options.orderbook_min_write_interval_ms), 1),
        },
        "constraints": {
            "endpoint": "wss://api.upbit.com/websocket/v1",
            "idle_timeout_sec": 120,
            "rate_limit_policy": {
                "websocket_connect_rps": 5,
                "websocket_message_rps": 5,
                "websocket_message_rpm": 100,
            },
            "request_structure": ["ticket", "type", "format"],
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
            "channels_count": len(channels),
        },
    }

    options.output_path.parent.mkdir(parents=True, exist_ok=True)
    options.output_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return plan


def _select_markets(
    *,
    options: WsPublicPlanOptions,
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
        options.base_dataset_root,
        tf_filter=("1m", "5m", "15m", "60m", "240m"),
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
        options.base_dataset_root,
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


def _normalize_channels(values: tuple[str, ...] | None) -> tuple[str, ...]:
    seen: set[str] = set()
    normalized: list[str] = []
    for raw in values or DEFAULT_WS_CHANNELS:
        channel = str(raw).strip().lower()
        if not channel:
            continue
        if channel not in VALID_CHANNELS:
            allowed = ", ".join(sorted(VALID_CHANNELS))
            raise ValueError(f"channels must be one of: {allowed}")
        if channel in seen:
            continue
        seen.add(channel)
        normalized.append(channel)
    if not normalized:
        raise ValueError("at least one channel is required")
    return tuple(normalized)


def _normalize_level(value: str | int | None) -> str | int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return text


def _dedupe_preserve(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for item in values:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped
