"""Rule-based candidate generator for paper run v1."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Protocol, Sequence


@dataclass(frozen=True)
class Candidate:
    market: str
    score: float
    proposed_side: str
    ref_price: float
    meta: dict[str, object]


@dataclass(frozen=True)
class CandidateSettings:
    momentum_window_sec: int = 60
    min_momentum_pct: float = 0.2


class MarketDataReader(Protocol):
    def get_latest_ticker(self, market: str) -> object | None: ...

    def get_momentum_pct(self, market: str, *, window_sec: int) -> float | None: ...

    def get_acc_trade_delta(self, market: str, *, window_sec: int) -> float | None: ...


class CandidateGeneratorV1:
    def __init__(self, settings: CandidateSettings) -> None:
        self._settings = settings

    def generate(self, *, markets: Sequence[str], market_data: MarketDataReader) -> list[Candidate]:
        candidates: list[Candidate] = []
        for market in markets:
            latest = market_data.get_latest_ticker(market)
            if latest is None:
                continue

            # `latest` comes from MarketDataHub ticker snapshot.
            price = _safe_optional_float(getattr(latest, "trade_price", None))
            acc_trade_price_24h = _safe_optional_float(getattr(latest, "acc_trade_price_24h", None))
            if price is None or price <= 0:
                continue
            acc_trade_price_24h = float(acc_trade_price_24h or 0.0)

            momentum_pct = market_data.get_momentum_pct(
                market,
                window_sec=max(int(self._settings.momentum_window_sec), 1),
            )
            if momentum_pct is None:
                continue
            if momentum_pct < float(self._settings.min_momentum_pct):
                continue

            acc_delta = market_data.get_acc_trade_delta(
                market,
                window_sec=max(int(self._settings.momentum_window_sec), 1),
            )
            acc_delta_value = float(acc_delta or 0.0)

            # Keep scoring simple and monotonic for transparent debug.
            volume_score = math.log10(max(acc_trade_price_24h, 1.0))
            score = momentum_pct + (0.05 * volume_score) + (0.000000001 * max(acc_delta_value, 0.0))
            side = "bid"

            candidates.append(
                Candidate(
                    market=market,
                    score=float(score),
                    proposed_side=side,
                    ref_price=price,
                    meta={
                        "momentum_pct": float(momentum_pct),
                        "acc_trade_price_24h": acc_trade_price_24h,
                        "acc_trade_delta": acc_delta_value,
                    },
                )
            )

        candidates.sort(key=lambda item: item.score, reverse=True)
        return candidates


def _safe_optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
