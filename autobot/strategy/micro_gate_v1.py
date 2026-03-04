"""MicroGate v1: execution-risk filter driven by micro snapshots."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .micro_snapshot import LiveWsProviderSettings, MicroSnapshot


@dataclass(frozen=True)
class MicroGateTradeSettings:
    min_trade_events: int = 1
    min_trade_coverage_ms: int = 0
    min_trade_notional_krw: float = 0.0


@dataclass(frozen=True)
class MicroGateBookSettings:
    max_spread_bps: float = 0.0
    min_depth_top5_krw: float = 0.0
    min_book_events: int = 0
    min_book_coverage_ms: int = 0


@dataclass(frozen=True)
class MicroGateSettings:
    enabled: bool = False
    mode: str = "trade_only"  # trade_only | trade_and_book
    on_missing: str = "warn_allow"  # warn_allow | block | allow
    stale_ms: int = 120_000
    dataset_name: str = "micro_v1"
    tf: str | None = None
    cache_entries: int = 64
    trade: MicroGateTradeSettings = field(default_factory=MicroGateTradeSettings)
    book: MicroGateBookSettings = field(default_factory=MicroGateBookSettings)
    live_ws: LiveWsProviderSettings = field(default_factory=LiveWsProviderSettings)


@dataclass(frozen=True)
class MicroGateDecision:
    allow: bool
    severity: str  # OK | WARN | BLOCK
    reasons: tuple[str, ...]
    diagnostics: dict[str, Any]


class MicroGateV1:
    def __init__(self, settings: MicroGateSettings) -> None:
        self._settings = _normalize_settings(settings)

    @property
    def settings(self) -> MicroGateSettings:
        return self._settings

    def evaluate(
        self,
        *,
        candidate: Any | None = None,
        micro_snapshot: MicroSnapshot | None,
        now_ts_ms: int,
    ) -> MicroGateDecision:
        _ = candidate
        if not self._settings.enabled:
            return MicroGateDecision(allow=True, severity="OK", reasons=(), diagnostics={})

        if micro_snapshot is None:
            return self._on_missing(reason="MICRO_MISSING")

        diagnostics = {
            "snapshot_ts_ms": int(micro_snapshot.snapshot_ts_ms),
            "last_event_ts_ms": int(micro_snapshot.last_event_ts_ms),
            "trade_events": int(micro_snapshot.trade_events),
            "trade_coverage_ms": int(micro_snapshot.trade_coverage_ms),
            "trade_notional_krw": float(micro_snapshot.trade_notional_krw),
            "trade_source": str(micro_snapshot.trade_source),
            "trade_imbalance": micro_snapshot.trade_imbalance,
            "book_events": int(micro_snapshot.book_events),
            "book_coverage_ms": int(micro_snapshot.book_coverage_ms),
            "book_available": bool(micro_snapshot.book_available),
            "spread_bps_mean": micro_snapshot.spread_bps_mean,
            "depth_top5_notional_krw": micro_snapshot.depth_top5_notional_krw,
            "mode": self._settings.mode,
        }

        block_reasons: list[str] = []
        warn_reasons: list[str] = []

        if int(now_ts_ms) - int(micro_snapshot.last_event_ts_ms) > int(self._settings.stale_ms):
            block_reasons.append("STALE_MICRO")

        trade_cfg = self._settings.trade
        if (
            int(micro_snapshot.trade_events) < int(trade_cfg.min_trade_events)
            or int(micro_snapshot.trade_coverage_ms) < int(trade_cfg.min_trade_coverage_ms)
            or float(micro_snapshot.trade_notional_krw) < float(trade_cfg.min_trade_notional_krw)
        ):
            block_reasons.append("LOW_LIQUIDITY_TRADE")

        if self._settings.mode == "trade_and_book":
            if _book_rules_enabled(self._settings.book):
                if not bool(micro_snapshot.book_available):
                    missing = self._on_missing(reason="BOOK_MISSING")
                    if missing.severity == "BLOCK":
                        block_reasons.extend(list(missing.reasons))
                    elif missing.severity == "WARN":
                        warn_reasons.extend(list(missing.reasons))
                else:
                    book_cfg = self._settings.book
                    spread = micro_snapshot.spread_bps_mean
                    if float(book_cfg.max_spread_bps) > 0 and spread is not None and float(spread) > float(book_cfg.max_spread_bps):
                        block_reasons.append("WIDE_SPREAD")

                    if int(book_cfg.min_book_events) > 0 and int(micro_snapshot.book_events) < int(book_cfg.min_book_events):
                        block_reasons.append("THIN_BOOK")
                    if (
                        int(book_cfg.min_book_coverage_ms) > 0
                        and int(micro_snapshot.book_coverage_ms) < int(book_cfg.min_book_coverage_ms)
                    ):
                        block_reasons.append("THIN_BOOK")
                    depth = micro_snapshot.depth_top5_notional_krw
                    if (
                        float(book_cfg.min_depth_top5_krw) > 0
                        and (depth is None or float(depth) < float(book_cfg.min_depth_top5_krw))
                    ):
                        block_reasons.append("THIN_BOOK")

        if block_reasons:
            return MicroGateDecision(
                allow=False,
                severity="BLOCK",
                reasons=tuple(_dedupe_reasons(block_reasons)),
                diagnostics=diagnostics,
            )
        if warn_reasons:
            return MicroGateDecision(
                allow=True,
                severity="WARN",
                reasons=tuple(_dedupe_reasons(warn_reasons)),
                diagnostics=diagnostics,
            )
        return MicroGateDecision(allow=True, severity="OK", reasons=(), diagnostics=diagnostics)

    def _on_missing(self, *, reason: str) -> MicroGateDecision:
        diagnostics = {"mode": self._settings.mode, "on_missing": self._settings.on_missing}
        if self._settings.on_missing == "block":
            return MicroGateDecision(allow=False, severity="BLOCK", reasons=(reason,), diagnostics=diagnostics)
        if self._settings.on_missing == "warn_allow":
            return MicroGateDecision(allow=True, severity="WARN", reasons=(reason,), diagnostics=diagnostics)
        return MicroGateDecision(allow=True, severity="OK", reasons=(), diagnostics=diagnostics)


def _normalize_settings(settings: MicroGateSettings) -> MicroGateSettings:
    mode = str(settings.mode).strip().lower()
    if mode not in {"trade_only", "trade_and_book"}:
        mode = "trade_only"
    on_missing = str(settings.on_missing).strip().lower()
    if on_missing not in {"warn_allow", "block", "allow"}:
        on_missing = "warn_allow"

    trade = settings.trade
    book = settings.book
    live_ws = settings.live_ws
    return MicroGateSettings(
        enabled=bool(settings.enabled),
        mode=mode,
        on_missing=on_missing,
        stale_ms=max(int(settings.stale_ms), 0),
        dataset_name=str(settings.dataset_name).strip() or "micro_v1",
        tf=(str(settings.tf).strip().lower() if settings.tf is not None and str(settings.tf).strip() else None),
        cache_entries=max(int(settings.cache_entries), 1),
        trade=MicroGateTradeSettings(
            min_trade_events=max(int(trade.min_trade_events), 0),
            min_trade_coverage_ms=max(int(trade.min_trade_coverage_ms), 0),
            min_trade_notional_krw=max(float(trade.min_trade_notional_krw), 0.0),
        ),
        book=MicroGateBookSettings(
            max_spread_bps=max(float(book.max_spread_bps), 0.0),
            min_depth_top5_krw=max(float(book.min_depth_top5_krw), 0.0),
            min_book_events=max(int(book.min_book_events), 0),
            min_book_coverage_ms=max(int(book.min_book_coverage_ms), 0),
        ),
        live_ws=LiveWsProviderSettings(
            enabled=bool(live_ws.enabled),
            window_sec=max(int(live_ws.window_sec), 1),
            orderbook_topk=max(int(live_ws.orderbook_topk), 1),
            orderbook_level=live_ws.orderbook_level,
            subscribe_format=str(live_ws.subscribe_format).strip().upper() or "DEFAULT",
            max_markets=max(int(live_ws.max_markets), 1),
            reconnect_max_per_min=max(int(live_ws.reconnect_max_per_min), 1),
            backoff_base_sec=max(float(live_ws.backoff_base_sec), 0.0),
            backoff_max_sec=max(float(live_ws.backoff_max_sec), 0.0),
            connect_rps=max(int(live_ws.connect_rps), 1),
            message_rps=max(int(live_ws.message_rps), 1),
            message_rpm=max(int(live_ws.message_rpm), 1),
            max_subscribe_messages_per_min=max(int(live_ws.max_subscribe_messages_per_min), 1),
        ),
    )


def _book_rules_enabled(settings: MicroGateBookSettings) -> bool:
    return (
        float(settings.max_spread_bps) > 0.0
        or float(settings.min_depth_top5_krw) > 0.0
        or int(settings.min_book_events) > 0
        or int(settings.min_book_coverage_ms) > 0
    )


def _dedupe_reasons(reasons: list[str]) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for reason in reasons:
        normalized = str(reason).strip().upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        values.append(normalized)
    return values
