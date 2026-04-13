"""Plan generator for Upbit REST trades/ticks collection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from .active_markets import filter_markets_by_active_set, resolve_active_quote_markets
from .fixed_collection_contract import resolve_fixed_collection_markets
from ..inventory import build_candle_inventory, default_inventory_window, estimate_recent_value_by_market


DEFAULT_DAYS_AGO: tuple[int, ...] = (1, 2, 3, 4, 5, 6, 7)
VALID_MARKET_MODES = {"fixed_list", "top_n_by_recent_value_est", "one_m_existing_only"}


@dataclass(frozen=True)
class TicksPlanOptions:
    parquet_root: Path = Path("data/parquet")
    base_dataset: str = "candles_v1"
    output_path: Path = Path("data/raw_ticks/upbit/_meta/ticks_plan.json")
    quote: str = "KRW"
    market_mode: str = "top_n_by_recent_value_est"
    top_n: int = 20
    fixed_markets: tuple[str, ...] | None = None
    days_ago: tuple[int, ...] = DEFAULT_DAYS_AGO
    value_est_lookback_days: int = 30
    end_ts_ms: int | None = None
    config_dir: Path = Path("config")
    resolve_active_markets: bool = False
    active_markets_override: tuple[str, ...] | None = None

    @property
    def base_dataset_root(self) -> Path:
        return self.parquet_root / self.base_dataset


def generate_ticks_collection_plan(options: TicksPlanOptions) -> dict[str, Any]:
    market_mode = str(options.market_mode).strip().lower()
    if market_mode not in VALID_MARKET_MODES:
        allowed = ", ".join(sorted(VALID_MARKET_MODES))
        raise ValueError(f"market_mode must be one of: {allowed}")

    quote = str(options.quote).strip().upper() or "KRW"
    days_ago = _normalize_days_ago(options.days_ago)
    end_ts_ms = int(options.end_ts_ms or int(datetime.now(timezone.utc).timestamp() * 1000))
    selected_markets, market_selection_meta = _select_markets(
        options=options,
        quote=quote,
        end_ts_ms=end_ts_ms,
    )

    targets = [
        {
            "market": market,
            "days_ago": day,
            "target_key": f"{market}|{day}",
            "reason": "REQUESTED_DAYS_AGO",
        }
        for market in selected_markets
        for day in days_ago
    ]
    targets.sort(key=lambda item: (int(item["days_ago"]), str(item["market"])))

    plan = {
        "version": "t13.1b-rest-ticks-plan-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "base_dataset": options.base_dataset,
        "base_dataset_root": str(options.base_dataset_root),
        "filters": {
            "quote": quote,
            "market_mode": market_mode,
            "top_n": max(int(options.top_n), 1),
            "fixed_markets": list(options.fixed_markets or ()),
            "days_ago": list(days_ago),
            "value_est_lookback_days": max(int(options.value_est_lookback_days), 1),
        },
        "constraints": {
            "endpoint": "/v1/trades/ticks",
            "days_ago_supported_range": [1, 7],
            "count_per_request": 200,
            "rate_limit_policy": {
                "rest_trade_group_rps": 10,
                "remaining_req_enforced": True,
            },
        },
        "market_selection": market_selection_meta,
        "selected_markets": selected_markets,
        "targets": targets,
        "summary": {
            "selected_markets": len(selected_markets),
            "targets": len(targets),
            "days_ago_count": len(days_ago),
        },
    }

    options.output_path.parent.mkdir(parents=True, exist_ok=True)
    options.output_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return plan


def _select_markets(
    *,
    options: TicksPlanOptions,
    quote: str,
    end_ts_ms: int,
) -> tuple[list[str], dict[str, Any]]:
    market_mode = str(options.market_mode).strip().lower()
    quote_prefix = f"{quote}-"
    fixed_collection_markets = resolve_fixed_collection_markets(
        config_dir=Path(options.config_dir),
        quote=quote,
        explicit_markets=options.fixed_markets,
    )
    if fixed_collection_markets:
        return _finalize_market_selection(
            candidates=list(fixed_collection_markets),
            options=options,
            quote=quote,
            top_n=None,
            meta={"mode": "fixed_collection_contract"},
        )

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
        return _finalize_market_selection(
            candidates=deduped,
            options=options,
            quote=quote,
            top_n=None,
            meta={"mode": market_mode},
        )

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
        return _finalize_market_selection(
            candidates=selected,
            options=options,
            quote=quote,
            top_n=None,
            meta={"mode": market_mode},
        )

    top_n = max(int(options.top_n), 1)
    estimates, tf_used = estimate_recent_value_by_market(
        options.base_dataset_root,
        end_ts_ms=end_ts_ms,
        lookback_days=max(int(options.value_est_lookback_days), 1),
        quote=quote,
    )
    ranked = sorted(estimates.items(), key=lambda item: (-float(item[1]), item[0]))
    if ranked:
        candidates = [market for market, _ in ranked]
        return _finalize_market_selection(
            candidates=candidates,
            options=options,
            quote=quote,
            top_n=top_n,
            meta={
                "mode": market_mode,
                "top_n": top_n,
                "value_est_tf": tf_used,
                "value_est_lookback_days": max(int(options.value_est_lookback_days), 1),
            },
        )

    fallback = inventory_markets
    return _finalize_market_selection(
        candidates=fallback,
        options=options,
        quote=quote,
        top_n=top_n,
        meta={
            "mode": market_mode,
            "top_n": top_n,
            "value_est_tf": None,
            "fallback": "inventory_alphabetical",
        },
    )


def _finalize_market_selection(
    *,
    candidates: list[str],
    options: TicksPlanOptions,
    quote: str,
    top_n: int | None,
    meta: dict[str, Any],
) -> tuple[list[str], dict[str, Any]]:
    active_markets, active_meta = resolve_active_quote_markets(
        quote=quote,
        config_dir=Path(options.config_dir),
        active_markets_override=options.active_markets_override,
        enabled=bool(options.resolve_active_markets),
    )
    selected, dropped = filter_markets_by_active_set(
        markets=list(candidates),
        active_markets=active_markets,
        top_n=top_n,
    )
    finalized = dict(meta)
    finalized["count"] = len(selected)
    finalized["active_market_filter"] = {
        **dict(active_meta),
        "dropped_count": len(dropped),
        "dropped_sample": list(dropped[:20]),
    }
    return selected, finalized


def _normalize_days_ago(values: tuple[int, ...] | None) -> tuple[int, ...]:
    deduped: list[int] = []
    seen: set[int] = set()
    for raw in values or DEFAULT_DAYS_AGO:
        day = int(raw)
        if day < 1 or day > 7:
            raise ValueError("days_ago values must be between 1 and 7")
        if day in seen:
            continue
        seen.add(day)
        deduped.append(day)
    if not deduped:
        return DEFAULT_DAYS_AGO
    return tuple(sorted(deduped))


def _dedupe_preserve(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for item in values:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped
