from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from autobot.execution.order_supervisor import PRICE_MODE_CROSS_1T, PRICE_MODE_JOIN, mean, percentile


def write_trade_artifacts(*, run_root: Path, fill_records: Any) -> None:
    trades_path = run_root / "trades.csv"
    per_market_path = run_root / "per_market.csv"
    slippage_by_market_path = run_root / "slippage_by_market.csv"
    price_mode_by_market_path = run_root / "price_mode_by_market.csv"
    rows = [item for item in fill_records if isinstance(item, dict)] if isinstance(fill_records, list) else []
    rows.sort(key=lambda item: (int(item.get("ts_ms", 0)), str(item.get("market", "")), str(item.get("side", ""))))

    trade_fields = [
        "ts_ms",
        "market",
        "side",
        "ref_price",
        "tick_bps",
        "order_price",
        "fill_price",
        "slippage_bps",
        "price_mode",
        "price",
        "volume",
        "notional_quote",
        "fee_quote",
        "order_id",
        "intent_id",
        "reason_code",
    ]
    trades_path.parent.mkdir(parents=True, exist_ok=True)
    with trades_path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=trade_fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in trade_fields})

    aggregates: dict[str, dict[str, float]] = {}
    for row in rows:
        market = str(row.get("market", "")).strip().upper()
        if not market:
            continue
        item = aggregates.setdefault(
            market,
            {
                "market": market,
                "fills_total": 0.0,
                "entry_fills": 0.0,
                "exit_fills": 0.0,
                "notional_bid": 0.0,
                "notional_ask": 0.0,
                "fees_quote": 0.0,
                "net_flow_quote": 0.0,
            },
        )
        side = str(row.get("side", "")).strip().lower()
        notional = float(row.get("notional_quote", 0.0) or 0.0)
        fees = float(row.get("fee_quote", 0.0) or 0.0)
        item["fills_total"] += 1.0
        item["fees_quote"] += fees
        if side == "bid":
            item["entry_fills"] += 1.0
            item["notional_bid"] += notional
            item["net_flow_quote"] -= notional + fees
        elif side == "ask":
            item["exit_fills"] += 1.0
            item["notional_ask"] += notional
            item["net_flow_quote"] += notional - fees

    per_market_fields = [
        "market",
        "fills_total",
        "entry_fills",
        "exit_fills",
        "notional_bid",
        "notional_ask",
        "fees_quote",
        "net_flow_quote",
    ]
    with per_market_path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=per_market_fields)
        writer.writeheader()
        for market in sorted(aggregates):
            item = aggregates[market]
            writer.writerow(
                {
                    "market": market,
                    "fills_total": int(item["fills_total"]),
                    "entry_fills": int(item["entry_fills"]),
                    "exit_fills": int(item["exit_fills"]),
                    "notional_bid": float(item["notional_bid"]),
                    "notional_ask": float(item["notional_ask"]),
                    "fees_quote": float(item["fees_quote"]),
                    "net_flow_quote": float(item["net_flow_quote"]),
                }
            )

    mode_aggregates: dict[str, dict[str, Any]] = {}
    for row in rows:
        market = str(row.get("market", "")).strip().upper()
        if not market:
            continue
        item = mode_aggregates.setdefault(
            market,
            {
                "fills": 0,
                "cross": 0,
                "join": 0,
                "passive": 0,
                "other": 0,
                "slippages": [],
            },
        )
        item["fills"] = int(item["fills"]) + 1
        mode = str(row.get("price_mode", "")).strip().upper()
        if mode == PRICE_MODE_CROSS_1T:
            item["cross"] = int(item["cross"]) + 1
        elif mode == PRICE_MODE_JOIN:
            item["join"] = int(item["join"]) + 1
        elif mode.startswith("PASSIVE"):
            item["passive"] = int(item["passive"]) + 1
        else:
            item["other"] = int(item["other"]) + 1
        slip_raw = row.get("slippage_bps")
        try:
            slip_value = float(slip_raw) if slip_raw is not None else None
        except (TypeError, ValueError):
            slip_value = None
        if slip_value is not None and isinstance(item.get("slippages"), list):
            item["slippages"].append(slip_value)

    slippage_by_market_fields = [
        "market",
        "fills",
        "mean_bps",
        "p50_bps",
        "p90_bps",
        "max_bps",
        "cross_ratio",
    ]
    with slippage_by_market_path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=slippage_by_market_fields)
        writer.writeheader()
        for market in sorted(mode_aggregates):
            item = mode_aggregates[market]
            fills = int(item["fills"])
            slippages = [float(value) for value in item.get("slippages", []) if isinstance(value, (int, float))]
            writer.writerow(
                {
                    "market": market,
                    "fills": fills,
                    "mean_bps": mean(slippages),
                    "p50_bps": percentile(slippages, 0.50),
                    "p90_bps": percentile(slippages, 0.90),
                    "max_bps": max(slippages) if slippages else 0.0,
                    "cross_ratio": (int(item["cross"]) / fills) if fills > 0 else 0.0,
                }
            )

    price_mode_by_market_fields = [
        "market",
        "JOIN_count",
        "CROSS_1T_count",
        "PASSIVE_count",
        "OTHER_count",
        "total_fills",
    ]
    with price_mode_by_market_path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=price_mode_by_market_fields)
        writer.writeheader()
        for market in sorted(mode_aggregates):
            item = mode_aggregates[market]
            writer.writerow(
                {
                    "market": market,
                    "JOIN_count": int(item["join"]),
                    "CROSS_1T_count": int(item["cross"]),
                    "PASSIVE_count": int(item["passive"]),
                    "OTHER_count": int(item["other"]),
                    "total_fills": int(item["fills"]),
                }
            )
