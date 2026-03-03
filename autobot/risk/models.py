from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RiskManagerConfig:
    exit_aggress_bps: float = 8.0
    order_timeout_sec: int = 20
    replace_max: int = 2
    default_sl_pct: float = 2.0
    default_tp_pct: float = 3.0
    default_trailing_enabled: bool = False
    default_trail_pct: float = 1.0
    price_digits: int = 8
    volume_digits: int = 8


@dataclass(frozen=True)
class RiskPlan:
    plan_id: str
    market: str
    side: str
    entry_price: float
    qty: float
    tp_enabled: bool = False
    tp_price: float | None = None
    tp_pct: float | None = None
    sl_enabled: bool = False
    sl_price: float | None = None
    sl_pct: float | None = None
    trailing_enabled: bool = False
    trail_pct: float | None = None
    high_watermark_price: float | None = None
    armed_ts_ms: int | None = None
    state: str = "ACTIVE"
    last_eval_ts_ms: int = 0
    last_action_ts_ms: int = 0
    current_exit_order_uuid: str | None = None
    current_exit_order_identifier: str | None = None
    replace_attempt: int = 0
    created_ts: int = 0
    updated_ts: int = 0

    def resolve_tp_price(self) -> float | None:
        if not self.tp_enabled:
            return None
        if self.tp_price is not None and self.tp_price > 0:
            return float(self.tp_price)
        if self.tp_pct is None or self.tp_pct <= 0:
            return None
        return float(self.entry_price) * (1.0 + float(self.tp_pct) / 100.0)

    def resolve_sl_price(self) -> float | None:
        if not self.sl_enabled:
            return None
        if self.sl_price is not None and self.sl_price > 0:
            return float(self.sl_price)
        if self.sl_pct is None or self.sl_pct <= 0:
            return None
        return float(self.entry_price) * (1.0 - float(self.sl_pct) / 100.0)
