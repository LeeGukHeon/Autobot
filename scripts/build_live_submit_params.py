"""Build executor submit-test parameters using Upbit chance + ticker + tick size."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from autobot.upbit import (
    UpbitError,
    UpbitHttpClient,
    UpbitPrivateClient,
    UpbitPublicClient,
    load_upbit_settings,
    require_upbit_credentials,
)


def _safe_optional_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number <= 0:
        return None
    return number


def _extract_min_total(chance_payload: dict[str, Any], *, side: str, market: str) -> float:
    market_payload = chance_payload.get("market")
    if not isinstance(market_payload, dict):
        return 5000.0 if market.startswith("KRW-") else 0.0

    side_payload = market_payload.get(side)
    if isinstance(side_payload, dict):
        side_min_total = _safe_optional_float(side_payload.get("min_total"))
        if side_min_total is not None:
            return side_min_total

    candidate_values: list[float] = []
    for side_key in ("bid", "ask"):
        candidate_side = market_payload.get(side_key)
        if not isinstance(candidate_side, dict):
            continue
        candidate_min_total = _safe_optional_float(candidate_side.get("min_total"))
        if candidate_min_total is not None:
            candidate_values.append(candidate_min_total)
    if candidate_values:
        return max(candidate_values)
    return 5000.0 if market.startswith("KRW-") else 0.0


def _extract_trade_price(ticker_payload: Any, *, market: str) -> float:
    if not isinstance(ticker_payload, list):
        raise RuntimeError("ticker payload must be a list")
    for item in ticker_payload:
        if not isinstance(item, dict):
            continue
        item_market = str(item.get("market", "")).strip().upper()
        if item_market != market:
            continue
        trade_price = _safe_optional_float(item.get("trade_price"))
        if trade_price is not None:
            return trade_price
    raise RuntimeError(f"trade_price not found for market={market}")


def _extract_tick_size(instruments_payload: Any, *, market: str, reference_price: float) -> float:
    if isinstance(instruments_payload, list):
        for item in instruments_payload:
            if not isinstance(item, dict):
                continue
            item_market = str(item.get("market", "")).strip().upper()
            if item_market != market:
                continue
            tick_size = _safe_optional_float(item.get("tick_size"))
            if tick_size is not None:
                return tick_size
    return _infer_tick_size(reference_price=reference_price, quote=market.split("-", 1)[0])


def _infer_tick_size(*, reference_price: float, quote: str) -> float:
    quote_value = str(quote).strip().upper()
    if quote_value != "KRW":
        return 0.00000001

    price = max(float(reference_price), 0.0)
    if price >= 2_000_000:
        return 1000.0
    if price >= 1_000_000:
        return 500.0
    if price >= 500_000:
        return 100.0
    if price >= 100_000:
        return 50.0
    if price >= 10_000:
        return 10.0
    if price >= 1_000:
        return 1.0
    if price >= 100:
        return 0.1
    if price >= 10:
        return 0.01
    if price >= 1:
        return 0.001
    return 0.0001


def _decimal_places(value: float) -> int:
    text = f"{value:.16f}".rstrip("0")
    if "." not in text:
        return 0
    return len(text.split(".", 1)[1])


def _round_price_to_tick(*, price: float, tick_size: float, side: str) -> float:
    if price <= 0:
        raise ValueError("price must be positive")
    tick = max(float(tick_size), 1e-12)
    scaled = float(price) / tick
    side_value = side.strip().lower()
    if side_value == "bid":
        rounded_ticks = math.floor(scaled + 1e-12)
    elif side_value == "ask":
        rounded_ticks = math.ceil(scaled - 1e-12)
    else:
        raise ValueError("side must be bid or ask")
    rounded_price = max(rounded_ticks * tick, tick)
    return round(rounded_price, _decimal_places(tick))


def _round_up(value: float, digits: int) -> float:
    precision = max(int(digits), 0)
    factor = 10 ** precision
    return math.ceil(value * factor - 1e-12) / factor


def main() -> int:
    parser = argparse.ArgumentParser(description="Build live submit-test params from chance/min_total.")
    parser.add_argument("--config-dir", default="config", help="Path to config directory.")
    parser.add_argument("--market", required=True, help="Market, ex: KRW-BTC")
    parser.add_argument("--side", default="bid", choices=("bid", "ask"))
    parser.add_argument(
        "--offset-pct",
        type=float,
        default=2.0,
        help="Price offset percent from trade_price for near-unfillable test orders.",
    )
    parser.add_argument(
        "--min-total-buffer",
        type=float,
        default=1.02,
        help="Multiply min_total by this buffer before volume calculation.",
    )
    parser.add_argument("--volume-precision", type=int, default=8, help="Decimal digits for volume rounding-up.")
    parser.add_argument("--identifier", default="", help="Optional identifier for suggested command.")
    args = parser.parse_args()

    market = str(args.market).strip().upper()
    side = str(args.side).strip().lower()
    if not market:
        raise SystemExit("market is required")
    if side not in {"bid", "ask"}:
        raise SystemExit("side must be bid or ask")

    settings = load_upbit_settings(Path(args.config_dir))
    credentials = require_upbit_credentials(settings)

    try:
        with UpbitHttpClient(settings, credentials=credentials) as private_http:
            chance_payload = UpbitPrivateClient(private_http).chance(market=market)
        with UpbitHttpClient(settings) as public_http:
            public_client = UpbitPublicClient(public_http)
            ticker_payload = public_client.ticker([market])
            instruments_payload = public_client.orderbook_instruments([market])
    except UpbitError as exc:
        print(f"[error] {exc}")
        return 2

    if not isinstance(chance_payload, dict):
        print("[error] chance payload is not an object")
        return 2

    reference_price = _extract_trade_price(ticker_payload, market=market)
    tick_size = _extract_tick_size(instruments_payload, market=market, reference_price=reference_price)
    min_total = _extract_min_total(chance_payload, side=side, market=market)

    offset_pct = max(float(args.offset_pct), 0.0) / 100.0
    raw_price = reference_price * (1.0 - offset_pct if side == "bid" else 1.0 + offset_pct)
    price = _round_price_to_tick(price=max(raw_price, tick_size), tick_size=tick_size, side=side)

    target_notional = max(min_total, 0.0) * max(float(args.min_total_buffer), 1.0)
    if target_notional <= 0.0:
        print("[error] could not resolve positive min_total")
        return 2

    volume = _round_up(target_notional / price, int(args.volume_precision))
    estimated_notional = price * volume
    if estimated_notional + 1e-9 < target_notional:
        step = 10 ** max(int(args.volume_precision), 0)
        volume = (math.floor(volume * step) + 1) / step
        estimated_notional = price * volume

    price_str = f"{price:.16f}".rstrip("0").rstrip(".")
    volume_str = f"{volume:.16f}".rstrip("0").rstrip(".")

    command = (
        f"python -m autobot.cli exec submit-test --market {market} --side {side} "
        f"--price {price_str} --volume {volume_str}"
    )
    identifier = str(args.identifier).strip()
    if identifier:
        command += f" --identifier {identifier}"

    output = {
        "market": market,
        "side": side,
        "reference_price": reference_price,
        "tick_size": tick_size,
        "min_total": min_total,
        "target_notional": target_notional,
        "price": price,
        "volume": volume,
        "estimated_notional": estimated_notional,
        "submit_command": command,
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
