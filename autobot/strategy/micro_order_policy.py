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
REASON_TICK_BPS_ABORT = "TICK_BPS_ABORT"

CROSS_BLOCK_TICK_BPS_TOO_HIGH = "TICK_BPS_TOO_HIGH"
CROSS_BLOCK_PROB_TOO_LOW = "PROB_TOO_LOW"
CROSS_BLOCK_MICRO_STALE = "MICRO_STALE"
CROSS_BLOCK_RESOLVER_FAILED = "RESOLVER_FAILED_FALLBACK_USED"


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
    cross_tick_bps_max: float = 10.0
    cross_escalate_after_timeouts: int = 2
    cross_min_prob: float | None = None
    cross_micro_stale_ms: int | None = None
    abort_if_tick_bps_gt: float | None = None
    tick_size_resolver: str = "auto"  # upbit_rules | krw_table | auto


@dataclass(frozen=True)
class MicroOrderPolicyDecision:
    allow: bool
    reason_code: str
    detail: str
    tier: str | None
    profile: OrderExecProfile | None
    diagnostics: dict[str, Any]


@dataclass(frozen=True)
class TickBpsGuardDecision:
    profile: OrderExecProfile
    diagnostics: dict[str, Any]
    abort_reason: str | None = None


@dataclass(frozen=True)
class _TickSizeResolution:
    tick_size: float
    source: str
    resolver_failed_fallback_used: bool


class MicroOrderPolicyV1:
    def __init__(self, settings: MicroOrderPolicySettings) -> None:
        self._settings = _normalize_settings(settings)

    @property
    def settings(self) -> MicroOrderPolicySettings:
        return self._settings

    def evaluate(
        self,
        *,
        micro_snapshot: MicroSnapshot | None,
        base_profile: OrderExecProfile | None = None,
        market: str | None = None,
        ref_price: float | None = None,
        tick_size: float | None = None,
        replace_attempt: int = 0,
        model_prob: float | None = None,
        now_ts_ms: int | None = None,
    ) -> MicroOrderPolicyDecision:
        normalized_base_profile = (
            normalize_order_exec_profile(
                base_profile,
                forbid_post_only_with_cross=bool(self._settings.safety.forbid_post_only_with_cross),
            )
            if base_profile is not None
            else None
        )
        if not self._settings.enabled:
            profile = normalized_base_profile if normalized_base_profile is not None else _profile_for_tier(self._settings, TIER_MID)
            return MicroOrderPolicyDecision(
                allow=True,
                reason_code=REASON_POLICY_OK,
                detail="policy_disabled",
                tier=TIER_MID,
                profile=profile,
                diagnostics={
                    "enabled": False,
                    "tier": TIER_MID,
                    "profile_source": "base_profile" if normalized_base_profile is not None else "static_mid",
                },
            )

        if micro_snapshot is None:
            base_decision = self._on_missing(reason="snapshot_missing", base_profile=normalized_base_profile)
        else:
            liq_score = (
                float(self._settings.tiering.w_notional) * math.log1p(max(float(micro_snapshot.trade_notional_krw), 0.0))
                + float(self._settings.tiering.w_events) * math.log1p(max(int(micro_snapshot.trade_events), 0))
            )
            tier = _tier_from_score(liq_score, tiering=self._settings.tiering)
            tier = self._maybe_adjust_tier_for_book(micro_snapshot=micro_snapshot, tier=tier)
            tier_profile = _profile_for_tier(self._settings, tier)
            profile = (
                _merge_profiles_conservative(
                    base_profile=normalized_base_profile,
                    tier_profile=tier_profile,
                    forbid_post_only_with_cross=bool(self._settings.safety.forbid_post_only_with_cross),
                )
                if normalized_base_profile is not None
                else tier_profile
            )
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
                "profile_source": "base_with_tier_guard" if normalized_base_profile is not None else "tier_profile",
                "base_profile_price_mode": (
                    str(normalized_base_profile.price_mode) if normalized_base_profile is not None else None
                ),
                "tier_profile_price_mode": str(tier_profile.price_mode),
            }
            base_decision = MicroOrderPolicyDecision(
                allow=True,
                reason_code=REASON_POLICY_OK,
                detail=f"tier={tier}",
                tier=tier,
                profile=profile,
                diagnostics=diagnostics,
            )

        if not base_decision.allow or base_decision.profile is None:
            return base_decision

        guard = self.resolve_guarded_profile(
            profile=base_decision.profile,
            market=market or (micro_snapshot.market if micro_snapshot is not None else None),
            ref_price=ref_price,
            tick_size=tick_size,
            replace_attempt=replace_attempt,
            model_prob=model_prob,
            micro_snapshot=micro_snapshot,
            now_ts_ms=now_ts_ms,
        )
        diagnostics = dict(base_decision.diagnostics or {})
        diagnostics.update(guard.diagnostics)
        if guard.abort_reason is not None:
            return MicroOrderPolicyDecision(
                allow=False,
                reason_code=guard.abort_reason,
                detail="tick_bps_abort",
                tier=base_decision.tier,
                profile=None,
                diagnostics=diagnostics,
            )
        return MicroOrderPolicyDecision(
            allow=True,
            reason_code=base_decision.reason_code,
            detail=base_decision.detail,
            tier=base_decision.tier,
            profile=guard.profile,
            diagnostics=diagnostics,
        )

    def resolve_guarded_profile(
        self,
        *,
        profile: OrderExecProfile,
        market: str | None,
        ref_price: float | None,
        tick_size: float | None,
        replace_attempt: int,
        model_prob: float | None,
        micro_snapshot: MicroSnapshot | None,
        now_ts_ms: int | None,
    ) -> TickBpsGuardDecision:
        base_profile = normalize_order_exec_profile(
            profile,
            forbid_post_only_with_cross=bool(self._settings.safety.forbid_post_only_with_cross),
        )
        market_value = str(market or "").strip().upper()
        quote = market_value.split("-", 1)[0] if "-" in market_value else ""
        ref_value = _safe_positive_float(ref_price)
        resolution = _resolve_tick_size(
            resolver=self._settings.tick_size_resolver,
            provided_tick_size=tick_size,
            reference_price=ref_value,
            quote=quote,
        )
        tick_bps_value = _tick_bps(tick_size=resolution.tick_size, ref_price=ref_value)
        snapshot_age_ms = _snapshot_age_ms(micro_snapshot=micro_snapshot, now_ts_ms=now_ts_ms)

        replace_value = max(int(replace_attempt), 0)
        escalate_after = max(int(self._settings.cross_escalate_after_timeouts), 0)
        cross_candidate = replace_value >= escalate_after
        base_mode = str(base_profile.price_mode).strip().upper()
        selected_mode = base_mode if base_mode in {PRICE_MODE_PASSIVE_MAKER, PRICE_MODE_JOIN} else PRICE_MODE_JOIN
        cross_allowed = False
        cross_block_reason: str | None = None
        abort_reason: str | None = None

        abort_limit = _safe_positive_float(self._settings.abort_if_tick_bps_gt)
        if abort_limit is not None and tick_bps_value is not None and tick_bps_value > abort_limit:
            abort_reason = REASON_TICK_BPS_ABORT

        if abort_reason is None and cross_candidate:
            allow_cross = True
            if resolution.resolver_failed_fallback_used:
                allow_cross = False
                cross_block_reason = CROSS_BLOCK_RESOLVER_FAILED
            elif tick_bps_value is None:
                allow_cross = False
                cross_block_reason = CROSS_BLOCK_RESOLVER_FAILED
            elif tick_bps_value > max(float(self._settings.cross_tick_bps_max), 0.0):
                allow_cross = False
                cross_block_reason = CROSS_BLOCK_TICK_BPS_TOO_HIGH

            min_prob = _safe_optional_float(self._settings.cross_min_prob)
            prob_value = _safe_optional_float(model_prob)
            if allow_cross and min_prob is not None and (prob_value is None or prob_value < min_prob):
                allow_cross = False
                cross_block_reason = CROSS_BLOCK_PROB_TOO_LOW

            stale_limit = _safe_optional_int(self._settings.cross_micro_stale_ms)
            if allow_cross and stale_limit is not None:
                if snapshot_age_ms is None or snapshot_age_ms > max(stale_limit, 0):
                    allow_cross = False
                    cross_block_reason = CROSS_BLOCK_MICRO_STALE

            if allow_cross:
                cross_allowed = True
                selected_mode = PRICE_MODE_CROSS_1T

        guarded_profile = normalize_order_exec_profile(
            OrderExecProfile(
                timeout_ms=int(base_profile.timeout_ms),
                replace_interval_ms=int(base_profile.replace_interval_ms),
                max_replaces=int(base_profile.max_replaces),
                price_mode=selected_mode,
                max_chase_bps=int(base_profile.max_chase_bps),
                min_replace_interval_ms_global=int(base_profile.min_replace_interval_ms_global),
                post_only=bool(base_profile.post_only),
            ),
            forbid_post_only_with_cross=bool(self._settings.safety.forbid_post_only_with_cross),
        )
        diagnostics: dict[str, Any] = {
            "replace_attempt": replace_value,
            "cross_escalate_after_timeouts": escalate_after,
            "cross_candidate": bool(cross_candidate),
            "cross_allowed": bool(cross_allowed),
            "cross_block_reason": cross_block_reason,
            "selected_price_mode": str(guarded_profile.price_mode),
            "tick_size_resolver": str(self._settings.tick_size_resolver),
            "tick_size_source": resolution.source,
            "tick_size": float(resolution.tick_size),
            "tick_bps": tick_bps_value,
            "resolver_failed_fallback_used": bool(resolution.resolver_failed_fallback_used),
            "model_prob": _safe_optional_float(model_prob),
            "cross_min_prob": _safe_optional_float(self._settings.cross_min_prob),
            "micro_snapshot_age_ms": snapshot_age_ms,
            "cross_micro_stale_ms": _safe_optional_int(self._settings.cross_micro_stale_ms),
            "abort_if_tick_bps_gt": _safe_optional_float(self._settings.abort_if_tick_bps_gt),
        }
        return TickBpsGuardDecision(
            profile=guarded_profile,
            diagnostics=diagnostics,
            abort_reason=abort_reason,
        )

    def _on_missing(self, *, reason: str, base_profile: OrderExecProfile | None = None) -> MicroOrderPolicyDecision:
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
        tier_profile = _profile_for_tier(self._settings, fallback_tier)
        profile = (
            _merge_profiles_conservative(
                base_profile=base_profile,
                tier_profile=tier_profile,
                forbid_post_only_with_cross=bool(self._settings.safety.forbid_post_only_with_cross),
            )
            if base_profile is not None
            else tier_profile
        )
        return MicroOrderPolicyDecision(
            allow=True,
            reason_code=REASON_MICRO_MISSING_FALLBACK,
            detail=reason,
            tier=fallback_tier,
            profile=profile,
            diagnostics={
                "enabled": True,
                "on_missing": mode,
                "reason": reason,
                "tier": fallback_tier,
                "profile_source": "base_with_missing_fallback" if base_profile is not None else "tier_fallback",
                "base_profile_price_mode": str(base_profile.price_mode) if base_profile is not None else None,
                "tier_profile_price_mode": str(tier_profile.price_mode),
            },
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


def _merge_profiles_conservative(
    *,
    base_profile: OrderExecProfile,
    tier_profile: OrderExecProfile,
    forbid_post_only_with_cross: bool,
) -> OrderExecProfile:
    base = normalize_order_exec_profile(
        base_profile,
        forbid_post_only_with_cross=forbid_post_only_with_cross,
    )
    tier = normalize_order_exec_profile(
        tier_profile,
        forbid_post_only_with_cross=forbid_post_only_with_cross,
    )
    return normalize_order_exec_profile(
        OrderExecProfile(
            timeout_ms=max(int(base.timeout_ms), int(tier.timeout_ms)),
            replace_interval_ms=max(int(base.replace_interval_ms), int(tier.replace_interval_ms)),
            max_replaces=min(int(base.max_replaces), int(tier.max_replaces)),
            price_mode=_more_conservative_price_mode(base.price_mode, tier.price_mode),
            max_chase_bps=min(int(base.max_chase_bps), int(tier.max_chase_bps)),
            min_replace_interval_ms_global=max(
                int(base.min_replace_interval_ms_global),
                int(tier.min_replace_interval_ms_global),
            ),
            post_only=bool(base.post_only or tier.post_only),
        ),
        forbid_post_only_with_cross=forbid_post_only_with_cross,
    )


def _more_conservative_price_mode(left: str, right: str) -> str:
    rank = {
        PRICE_MODE_PASSIVE_MAKER: 0,
        PRICE_MODE_JOIN: 1,
        PRICE_MODE_CROSS_1T: 2,
    }
    left_mode = str(left).strip().upper()
    right_mode = str(right).strip().upper()
    left_rank = rank.get(left_mode, rank[PRICE_MODE_JOIN])
    right_rank = rank.get(right_mode, rank[PRICE_MODE_JOIN])
    return left_mode if left_rank <= right_rank else right_mode


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

    resolver = str(settings.tick_size_resolver).strip().lower()
    if resolver not in {"upbit_rules", "krw_table", "auto"}:
        resolver = "auto"

    cross_min_prob = _safe_optional_float(settings.cross_min_prob)
    if cross_min_prob is not None:
        cross_min_prob = min(max(cross_min_prob, 0.0), 1.0)

    cross_micro_stale_ms = _safe_optional_int(settings.cross_micro_stale_ms)
    if cross_micro_stale_ms is not None:
        cross_micro_stale_ms = max(cross_micro_stale_ms, 0)

    abort_if_tick_bps_gt = _safe_optional_float(settings.abort_if_tick_bps_gt)
    if abort_if_tick_bps_gt is not None:
        abort_if_tick_bps_gt = max(abort_if_tick_bps_gt, 0.0)

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
        cross_tick_bps_max=max(float(settings.cross_tick_bps_max), 0.0),
        cross_escalate_after_timeouts=max(int(settings.cross_escalate_after_timeouts), 0),
        cross_min_prob=cross_min_prob,
        cross_micro_stale_ms=cross_micro_stale_ms,
        abort_if_tick_bps_gt=abort_if_tick_bps_gt,
        tick_size_resolver=resolver,
    )


def _resolve_tick_size(
    *,
    resolver: str,
    provided_tick_size: float | None,
    reference_price: float | None,
    quote: str,
) -> _TickSizeResolution:
    resolver_value = str(resolver).strip().lower()
    provided = _safe_positive_float(provided_tick_size)
    ref_value = _safe_positive_float(reference_price)
    quote_value = str(quote).strip().upper()

    if resolver_value == "upbit_rules":
        if provided is not None:
            return _TickSizeResolution(
                tick_size=provided,
                source="upbit_rules",
                resolver_failed_fallback_used=False,
            )
        return _TickSizeResolution(
            tick_size=1.0,
            source="fallback_krw_1",
            resolver_failed_fallback_used=True,
        )

    if resolver_value == "krw_table":
        table_tick = _infer_tick_size_from_krw_table(reference_price=ref_value, quote=quote_value)
        if table_tick is not None:
            return _TickSizeResolution(
                tick_size=table_tick,
                source="krw_table",
                resolver_failed_fallback_used=False,
            )
        return _TickSizeResolution(
            tick_size=1.0,
            source="fallback_krw_1",
            resolver_failed_fallback_used=True,
        )

    if provided is not None:
        return _TickSizeResolution(
            tick_size=provided,
            source="upbit_rules",
            resolver_failed_fallback_used=False,
        )
    table_tick = _infer_tick_size_from_krw_table(reference_price=ref_value, quote=quote_value)
    if table_tick is not None:
        return _TickSizeResolution(
            tick_size=table_tick,
            source="krw_table",
            resolver_failed_fallback_used=False,
        )
    return _TickSizeResolution(
        tick_size=1.0,
        source="fallback_krw_1",
        resolver_failed_fallback_used=True,
    )


def _infer_tick_size_from_krw_table(*, reference_price: float | None, quote: str) -> float | None:
    if str(quote).strip().upper() != "KRW":
        return None
    if reference_price is None or reference_price <= 0:
        return None

    px = max(float(reference_price), 0.0)
    if px >= 2_000_000:
        return 1000.0
    if px >= 1_000_000:
        return 500.0
    if px >= 500_000:
        return 100.0
    if px >= 100_000:
        return 50.0
    if px >= 10_000:
        return 10.0
    if px >= 1_000:
        return 1.0
    if px >= 100:
        return 0.1
    if px >= 10:
        return 0.01
    if px >= 1:
        return 0.001
    return 0.0001


def _tick_bps(*, tick_size: float, ref_price: float | None) -> float | None:
    ref_value = _safe_positive_float(ref_price)
    if ref_value is None:
        return None
    return float(tick_size) / ref_value * 10_000.0


def _snapshot_age_ms(*, micro_snapshot: MicroSnapshot | None, now_ts_ms: int | None) -> int | None:
    if micro_snapshot is None:
        return None
    now_value = int(now_ts_ms) if now_ts_ms is not None else int(micro_snapshot.snapshot_ts_ms)
    age = now_value - int(micro_snapshot.last_event_ts_ms)
    return max(age, 0)


def _safe_positive_float(value: Any) -> float | None:
    number = _safe_optional_float(value)
    if number is None or number <= 0:
        return None
    return number


def _safe_optional_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
