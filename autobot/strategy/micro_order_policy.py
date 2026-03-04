"""MicroAdaptive order policy v1 for execution parameter tuning."""

from __future__ import annotations

from dataclasses import dataclass, field
import math
from typing import Any

from autobot.execution.order_supervisor import (
    PRICE_MODE_CROSS_1T,
    PRICE_MODE_JOIN,
    PRICE_MODE_PASSIVE_MAKER,
    OrderExecProfile,
    normalize_order_exec_profile,
)

from .micro_snapshot import MicroSnapshot

TIER_LOW = "LOW"
TIER_MID = "MID"
TIER_HIGH = "HIGH"

REASON_POLICY_OK = "POLICY_OK"
REASON_MICRO_MISSING_FALLBACK = "MICRO_MISSING_FALLBACK"
REASON_MICRO_MISSING_ABORT = "MICRO_MISSING_ABORT"


@dataclass(frozen=True)
class MicroOrderPolicyTieringSettings:
    w_notional: float = 1.0
    w_events: float = 0.5
    t1: float = 6.0
    t2: float = 9.0


@dataclass(frozen=True)
class MicroOrderPolicyTierSettings:
    timeout_ms: int
    replace_interval_ms: int
    max_replaces: int
    price_mode: str
    max_chase_bps: int
    post_only: bool = False


@dataclass(frozen=True)
class MicroOrderPolicyTiersSettings:
    low: MicroOrderPolicyTierSettings = field(
        default_factory=lambda: MicroOrderPolicyTierSettings(
            timeout_ms=120_000,
            replace_interval_ms=60_000,
            max_replaces=1,
            price_mode=PRICE_MODE_PASSIVE_MAKER,
            max_chase_bps=10,
            post_only=False,
        )
    )
    mid: MicroOrderPolicyTierSettings = field(
        default_factory=lambda: MicroOrderPolicyTierSettings(
            timeout_ms=45_000,
            replace_interval_ms=15_000,
            max_replaces=3,
            price_mode=PRICE_MODE_JOIN,
            max_chase_bps=15,
            post_only=False,
        )
    )
    high: MicroOrderPolicyTierSettings = field(
        default_factory=lambda: MicroOrderPolicyTierSettings(
            timeout_ms=15_000,
            replace_interval_ms=5_000,
            max_replaces=5,
            price_mode=PRICE_MODE_CROSS_1T,
            max_chase_bps=20,
            post_only=False,
        )
    )


@dataclass(frozen=True)
class MicroOrderPolicySafetySettings:
    min_replace_interval_ms_global: int = 1_500
    max_replaces_per_min_per_market: int = 10
    forbid_post_only_with_cross: bool = True


@dataclass(frozen=True)
class MicroOrderPolicySettings:
    enabled: bool = False
    mode: str = "trade_only"  # trade_only | trade_and_book
    on_missing: str = "static_fallback"  # static_fallback | conservative | abort
    tiering: MicroOrderPolicyTieringSettings = field(default_factory=MicroOrderPolicyTieringSettings)
    tiers: MicroOrderPolicyTiersSettings = field(default_factory=MicroOrderPolicyTiersSettings)
    safety: MicroOrderPolicySafetySettings = field(default_factory=MicroOrderPolicySafetySettings)


@dataclass(frozen=True)
class MicroOrderPolicyDecision:
    allow: bool
    reason_code: str
    detail: str
    tier: str | None
    profile: OrderExecProfile | None
    diagnostics: dict[str, Any]


class MicroOrderPolicyV1:
    def __init__(self, settings: MicroOrderPolicySettings) -> None:
        self._settings = _normalize_settings(settings)

    @property
    def settings(self) -> MicroOrderPolicySettings:
        return self._settings

    def evaluate(self, *, micro_snapshot: MicroSnapshot | None) -> MicroOrderPolicyDecision:
        if not self._settings.enabled:
            profile = _profile_for_tier(self._settings, TIER_MID)
            return MicroOrderPolicyDecision(
                allow=True,
                reason_code=REASON_POLICY_OK,
                detail="policy_disabled",
                tier=TIER_MID,
                profile=profile,
                diagnostics={"enabled": False, "tier": TIER_MID},
            )

        if micro_snapshot is None:
            return self._on_missing(reason="snapshot_missing")

        liq_score = (
            float(self._settings.tiering.w_notional) * math.log1p(max(float(micro_snapshot.trade_notional_krw), 0.0))
            + float(self._settings.tiering.w_events) * math.log1p(max(int(micro_snapshot.trade_events), 0))
        )
        tier = _tier_from_score(liq_score, tiering=self._settings.tiering)
        tier = self._maybe_adjust_tier_for_book(micro_snapshot=micro_snapshot, tier=tier)
        profile = _profile_for_tier(self._settings, tier)
        diagnostics = {
            "enabled": True,
            "mode": self._settings.mode,
            "on_missing": self._settings.on_missing,
            "tier": tier,
            "liq_score": liq_score,
            "trade_events": int(micro_snapshot.trade_events),
            "trade_notional_krw": float(micro_snapshot.trade_notional_krw),
            "trade_coverage_ms": int(micro_snapshot.trade_coverage_ms),
            "trade_source": str(micro_snapshot.trade_source),
            "book_available": bool(micro_snapshot.book_available),
            "spread_bps_mean": micro_snapshot.spread_bps_mean,
            "depth_top5_notional_krw": micro_snapshot.depth_top5_notional_krw,
        }
        return MicroOrderPolicyDecision(
            allow=True,
            reason_code=REASON_POLICY_OK,
            detail=f"tier={tier}",
            tier=tier,
            profile=profile,
            diagnostics=diagnostics,
        )

    def _on_missing(self, *, reason: str) -> MicroOrderPolicyDecision:
        mode = self._settings.on_missing
        if mode == "abort":
            return MicroOrderPolicyDecision(
                allow=False,
                reason_code=REASON_MICRO_MISSING_ABORT,
                detail=reason,
                tier=None,
                profile=None,
                diagnostics={"enabled": True, "on_missing": mode, "reason": reason},
            )
        fallback_tier = TIER_LOW if mode == "conservative" else TIER_MID
        profile = _profile_for_tier(self._settings, fallback_tier)
        return MicroOrderPolicyDecision(
            allow=True,
            reason_code=REASON_MICRO_MISSING_FALLBACK,
            detail=reason,
            tier=fallback_tier,
            profile=profile,
            diagnostics={"enabled": True, "on_missing": mode, "reason": reason, "tier": fallback_tier},
        )

    def _maybe_adjust_tier_for_book(self, *, micro_snapshot: MicroSnapshot, tier: str) -> str:
        if self._settings.mode != "trade_and_book":
            return tier
        spread = micro_snapshot.spread_bps_mean
        if spread is None:
            return tier
        # Conservative spread penalty for very wide spread conditions.
        if float(spread) <= 30.0:
            return tier
        if tier == TIER_HIGH:
            return TIER_MID
        if tier == TIER_MID:
            return TIER_LOW
        return TIER_LOW


def _profile_for_tier(settings: MicroOrderPolicySettings, tier: str) -> OrderExecProfile:
    if tier == TIER_LOW:
        cfg = settings.tiers.low
    elif tier == TIER_HIGH:
        cfg = settings.tiers.high
    else:
        cfg = settings.tiers.mid
    return normalize_order_exec_profile(
        OrderExecProfile(
            timeout_ms=max(int(cfg.timeout_ms), 1),
            replace_interval_ms=max(int(cfg.replace_interval_ms), 1),
            max_replaces=max(int(cfg.max_replaces), 0),
            price_mode=str(cfg.price_mode),
            max_chase_bps=max(int(cfg.max_chase_bps), 0),
            min_replace_interval_ms_global=max(int(settings.safety.min_replace_interval_ms_global), 1),
            post_only=bool(cfg.post_only),
        ),
        forbid_post_only_with_cross=bool(settings.safety.forbid_post_only_with_cross),
    )


def _tier_from_score(score: float, *, tiering: MicroOrderPolicyTieringSettings) -> str:
    t1 = float(tiering.t1)
    t2 = float(tiering.t2)
    if score < t1:
        return TIER_LOW
    if score < t2:
        return TIER_MID
    return TIER_HIGH


def _normalize_settings(settings: MicroOrderPolicySettings) -> MicroOrderPolicySettings:
    mode = str(settings.mode).strip().lower()
    if mode not in {"trade_only", "trade_and_book"}:
        mode = "trade_only"

    on_missing = str(settings.on_missing).strip().lower()
    if on_missing not in {"static_fallback", "conservative", "abort"}:
        on_missing = "static_fallback"

    tiering = settings.tiering
    t1 = float(tiering.t1)
    t2 = float(tiering.t2)
    if t2 < t1:
        t2 = t1

    safety = settings.safety
    return MicroOrderPolicySettings(
        enabled=bool(settings.enabled),
        mode=mode,
        on_missing=on_missing,
        tiering=MicroOrderPolicyTieringSettings(
            w_notional=float(tiering.w_notional),
            w_events=float(tiering.w_events),
            t1=t1,
            t2=t2,
        ),
        tiers=settings.tiers,
        safety=MicroOrderPolicySafetySettings(
            min_replace_interval_ms_global=max(int(safety.min_replace_interval_ms_global), 1),
            max_replaces_per_min_per_market=max(int(safety.max_replaces_per_min_per_market), 1),
            forbid_post_only_with_cross=bool(safety.forbid_post_only_with_cross),
        ),
    )
