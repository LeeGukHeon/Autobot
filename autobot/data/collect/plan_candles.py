"""Top-up plan generator for Upbit minute candles."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
from pathlib import Path
from typing import Any

from .active_markets import filter_markets_by_active_set, resolve_active_quote_markets
from .fixed_collection_contract import resolve_fixed_collection_markets
from ..inventory import (
    DAY_MS,
    build_candle_inventory,
    default_inventory_window,
    estimate_recent_value_by_market,
    parse_utc_ts_ms,
    ts_ms_to_utc_text,
)
from ..schema_contract import expected_interval_ms


DEFAULT_TFS: tuple[str, ...] = ("1m", "5m", "15m", "60m", "240m")
VALID_MARKET_MODES = {"fixed_list", "top_n_by_recent_value_est", "one_m_existing_only"}


@dataclass(frozen=True)
class CandlePlanOptions:
    parquet_root: Path = Path("data/parquet")
    base_dataset: str = "candles_v1"
    market_source_dataset: str | None = None
    output_path: Path = Path("data/collect/_meta/candle_topup_plan.json")
    lookback_months: int = 24
    tf_set: tuple[str, ...] = DEFAULT_TFS
    quote: str = "KRW"
    market_mode: str = "top_n_by_recent_value_est"
    top_n: int = 50
    fixed_markets: tuple[str, ...] | None = None
    max_backfill_days_1s: int = 90
    max_backfill_days_1m: int = 90
    end_ts_ms: int | None = None
    config_dir: Path = Path("config")
    resolve_active_markets: bool = False
    active_markets_override: tuple[str, ...] | None = None

    @property
    def base_dataset_root(self) -> Path:
        return self.parquet_root / self.base_dataset

    @property
    def market_source_dataset_root(self) -> Path:
        dataset_name = str(self.market_source_dataset or self.base_dataset).strip() or self.base_dataset
        return self.parquet_root / dataset_name


def generate_candle_topup_plan(options: CandlePlanOptions) -> dict[str, Any]:
    market_mode = str(options.market_mode).strip().lower()
    if market_mode not in VALID_MARKET_MODES:
        allowed = ", ".join(sorted(VALID_MARKET_MODES))
        raise ValueError(f"market_mode must be one of: {allowed}")

    tf_set = _normalize_tf_set(options.tf_set)
    quote = str(options.quote).strip().upper() or "KRW"
    lookback_months = max(int(options.lookback_months), 1)

    window_start_ts_ms, window_end_ts_ms = default_inventory_window(
        lookback_months=lookback_months,
        end_ts_ms=options.end_ts_ms,
    )
    inventory = build_candle_inventory(
        options.base_dataset_root,
        tf_filter=tf_set,
        quote=quote,
        window_start_ts_ms=window_start_ts_ms,
        window_end_ts_ms=window_end_ts_ms,
    )
    inventory_entries = inventory.get("entries", [])
    entry_index = {(item["market"], item["tf"]): item for item in inventory_entries}
    market_source_inventory = build_candle_inventory(
        options.market_source_dataset_root,
        quote=quote,
        window_start_ts_ms=window_start_ts_ms,
        window_end_ts_ms=window_end_ts_ms,
    )
    market_source_inventory_entries = market_source_inventory.get("entries", [])

    selected_markets, market_selection_meta = _select_markets(
        options=options,
        inventory_entries=inventory_entries,
        market_source_inventory_entries=market_source_inventory_entries,
        quote=quote,
        window_end_ts_ms=window_end_ts_ms,
    )

    targets: list[dict[str, Any]] = []
    skipped_ranges: list[dict[str, Any]] = []
    for market in selected_markets:
        for tf in tf_set:
            key = (market, tf)
            entry = entry_index.get(key)
            missing_ranges = list(entry.get("missing_ranges", [])) if entry else []
            if not missing_ranges:
                if entry is None:
                    missing_ranges = [
                        {
                            "from_ts_ms": window_start_ts_ms,
                            "to_ts_ms": window_end_ts_ms,
                            "reason": "NO_INVENTORY_ENTRY",
                        }
                    ]
                else:
                    continue

            for missing in missing_ranges:
                planned = _build_target_range(
                    market=market,
                    tf=tf,
                    missing=missing,
                    window_start_ts_ms=window_start_ts_ms,
                    window_end_ts_ms=window_end_ts_ms,
                    max_backfill_days_1s=options.max_backfill_days_1s,
                    max_backfill_days_1m=options.max_backfill_days_1m,
                )
                if planned is None:
                    limit_reason = "OUTSIDE_RECENT_WINDOW_LIMIT"
                    if tf == "1s":
                        limit_reason = "OUTSIDE_1S_BACKFILL_LIMIT"
                    elif tf == "1m":
                        limit_reason = "OUTSIDE_1M_BACKFILL_LIMIT"
                    skipped_ranges.append(
                        {
                            "market": market,
                            "tf": tf,
                            "reason": limit_reason,
                            "missing": missing,
                        }
                    )
                    continue
                if entry is not None:
                    planned["existing_rows"] = int(entry.get("rows", 0))
                    planned["existing_min_ts_ms"] = entry.get("min_ts_ms")
                    planned["existing_max_ts_ms"] = entry.get("max_ts_ms")
                    planned["coverage_before_pct"] = float(entry.get("coverage_pct", 0.0))
                targets.append(planned)

    targets.sort(key=_target_priority_key)
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    plan = {
        "version": "t13.1a-candle-topup-plan-v1",
        "generated_at": generated_at,
        "base_dataset": options.base_dataset,
        "base_dataset_root": str(options.base_dataset_root),
        "window": {
            "start_ts_ms": window_start_ts_ms,
            "end_ts_ms": window_end_ts_ms,
            "start_utc": ts_ms_to_utc_text(window_start_ts_ms),
            "end_utc": ts_ms_to_utc_text(window_end_ts_ms),
            "lookback_months": lookback_months,
        },
        "filters": {
            "quote": quote,
            "tf_set": list(tf_set),
            "market_mode": market_mode,
            "top_n": int(options.top_n),
            "fixed_markets": list(options.fixed_markets or ()),
            "market_source_dataset": str(options.market_source_dataset or options.base_dataset),
            "max_backfill_days_1s": int(options.max_backfill_days_1s),
            "max_backfill_days_1m": int(options.max_backfill_days_1m),
        },
        "market_selection": market_selection_meta,
        "constraints": {
            "rate_limit_policy": {
                "rest_candle_group_rps": 10,
                "remaining_req_enforced": True,
                "count_per_request": 200,
            },
            "per_tf_limits": {
                "1s_max_backfill_days": int(options.max_backfill_days_1s),
                "1m_max_backfill_days": int(options.max_backfill_days_1m),
            },
        },
        "discovered_inventory_pairs": int(inventory.get("total_pairs", 0)),
        "selected_markets": selected_markets,
        "targets": targets,
        "skipped_ranges": skipped_ranges,
        "summary": {
            "selected_markets": len(selected_markets),
            "targets": len(targets),
            "skipped_ranges": len(skipped_ranges),
        },
    }

    options.output_path.parent.mkdir(parents=True, exist_ok=True)
    options.output_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return plan


def plan_options_from_args(
    *,
    parquet_root: str,
    base_dataset: str,
    market_source_dataset: str | None,
    output_path: str,
    lookback_months: int,
    tf_csv: str | None,
    quote: str,
    market_mode: str,
    top_n: int | None,
    markets_csv: str | None,
    max_backfill_days_1s: int,
    max_backfill_days_1m: int,
    end: str | None = None,
) -> CandlePlanOptions:
    tf_set = _normalize_tf_set(tuple(item.strip().lower() for item in str(tf_csv or "").split(",") if item.strip()))
    if not tf_set:
        tf_set = DEFAULT_TFS
    fixed = tuple(item.strip().upper() for item in str(markets_csv or "").split(",") if item.strip()) or None
    return CandlePlanOptions(
        parquet_root=Path(parquet_root),
        base_dataset=str(base_dataset).strip() or "candles_v1",
        market_source_dataset=(str(market_source_dataset).strip() if market_source_dataset else None) or None,
        output_path=Path(output_path),
        lookback_months=max(int(lookback_months), 1),
        tf_set=tf_set,
        quote=str(quote).strip().upper() or "KRW",
        market_mode=str(market_mode).strip().lower(),
        top_n=max(int(top_n or 0), 0),
        fixed_markets=fixed,
        max_backfill_days_1s=max(int(max_backfill_days_1s), 1),
        max_backfill_days_1m=max(int(max_backfill_days_1m), 1),
        end_ts_ms=parse_utc_ts_ms(end, end_of_day=True),
    )


def _normalize_tf_set(tf_set: tuple[str, ...] | None) -> tuple[str, ...]:
    normalized = tuple(str(item).strip().lower() for item in (tf_set or ()) if str(item).strip())
    if not normalized:
        return DEFAULT_TFS
    deduped: list[str] = []
    seen: set[str] = set()
    for item in normalized:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return tuple(deduped)


def _select_markets(
    *,
    options: CandlePlanOptions,
    inventory_entries: list[dict[str, Any]],
    market_source_inventory_entries: list[dict[str, Any]],
    quote: str,
    window_end_ts_ms: int,
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
    inventory_markets = sorted(
        {
            str(item.get("market", "")).strip().upper()
            for item in inventory_entries
            if str(item.get("market", "")).strip().upper().startswith(quote_prefix)
        }
    )
    source_inventory_markets = sorted(
        {
            str(item.get("market", "")).strip().upper()
            for item in market_source_inventory_entries
            if str(item.get("market", "")).strip().upper().startswith(quote_prefix)
        }
    )
    fallback_inventory_markets = sorted(set(inventory_markets) | set(source_inventory_markets))

    if market_mode == "fixed_list":
        fixed_markets = [
            item.strip().upper()
            for item in (options.fixed_markets or ())
            if item.strip() and item.strip().upper().startswith(quote_prefix)
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
                for item in market_source_inventory_entries
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

    estimates, tf_used = estimate_recent_value_by_market(
        options.market_source_dataset_root,
        end_ts_ms=window_end_ts_ms,
        lookback_days=30,
        quote=quote,
    )
    ranked = sorted(estimates.items(), key=lambda item: (-float(item[1]), item[0]))
    top_n = max(int(options.top_n), 1)
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
                "value_est_lookback_days": 30,
            },
        )

    return _finalize_market_selection(
        candidates=fallback_inventory_markets,
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
    options: CandlePlanOptions,
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


def _build_target_range(
    *,
    market: str,
    tf: str,
    missing: dict[str, Any],
    window_start_ts_ms: int,
    window_end_ts_ms: int,
    max_backfill_days_1s: int,
    max_backfill_days_1m: int,
) -> dict[str, Any] | None:
    raw_from = max(int(missing.get("from_ts_ms", window_start_ts_ms)), window_start_ts_ms)
    raw_to = min(int(missing.get("to_ts_ms", window_end_ts_ms)), window_end_ts_ms)
    if raw_to < raw_from:
        return None

    reason = str(missing.get("reason", "MISSING_RANGE")).strip().upper() or "MISSING_RANGE"
    need_from_ts_ms = raw_from
    need_to_ts_ms = raw_to
    if tf == "1s":
        cap_from = int(window_end_ts_ms - (max(int(max_backfill_days_1s), 1) * DAY_MS))
        if need_to_ts_ms < cap_from:
            return None
        if need_from_ts_ms < cap_from:
            need_from_ts_ms = cap_from
            reason = f"{reason}|1S_BACKFILL_LIMIT"
    elif tf == "1m":
        cap_from = int(window_end_ts_ms - (max(int(max_backfill_days_1m), 1) * DAY_MS))
        if need_to_ts_ms < cap_from:
            return None
        if need_from_ts_ms < cap_from:
            need_from_ts_ms = cap_from
            reason = f"{reason}|1M_BACKFILL_LIMIT"

    interval_ms = expected_interval_ms(tf)
    estimated_bars = max(int(math.floor((need_to_ts_ms - need_from_ts_ms) / interval_ms) + 1), 1)
    max_calls = int(math.ceil(float(estimated_bars) / 200.0))
    return {
        "market": market,
        "tf": tf,
        "need_from_ts_ms": need_from_ts_ms,
        "need_to_ts_ms": need_to_ts_ms,
        "need_from_utc": ts_ms_to_utc_text(need_from_ts_ms),
        "need_to_utc": ts_ms_to_utc_text(need_to_ts_ms),
        "reason": reason,
        "estimated_bars": estimated_bars,
        "max_calls_budget_hint": max_calls,
    }


def _target_priority_key(item: dict[str, Any]) -> tuple[int, int, str, int]:
    tf = str(item.get("tf", "")).strip().lower()
    try:
        interval_ms = int(expected_interval_ms(tf))
    except Exception:
        interval_ms = 0
    max_calls_hint = max(int(item.get("max_calls_budget_hint", 0) or 0), 0)
    market = str(item.get("market", "")).strip().upper()
    need_from_ts_ms = int(item.get("need_from_ts_ms", 0) or 0)
    # Under a global request budget, cover cheap higher-timeframe tails first.
    return (max_calls_hint, -interval_ms, market, need_from_ts_ms)


def _dedupe_preserve(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for item in values:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped
