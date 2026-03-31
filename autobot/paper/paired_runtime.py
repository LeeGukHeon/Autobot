from __future__ import annotations

import argparse
import asyncio
from collections import deque
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
from typing import Any, AsyncIterator, Callable, Sequence
import uuid

from autobot.cli_model_helpers import paper_alpha_preset_overrides
from autobot.common.paper_lane_evidence import aggregate_paper_lane_runs, compare_champion_challenger
from autobot.paper.engine import PaperRunEngine, PaperRunSettings
from autobot.paper.paired_reporting import build_paired_paper_report, write_paired_paper_report
from autobot.strategy.micro_gate_v1 import MicroGateSettings
from autobot.strategy.micro_order_policy import MicroOrderPolicySettings
from autobot.strategy.model_alpha_v1 import (
    ModelAlphaExecutionSettings,
    ModelAlphaExitSettings,
    ModelAlphaPositionSettings,
    ModelAlphaSelectionSettings,
    ModelAlphaSettings,
)
from autobot.upbit import UpbitHttpClient, UpbitPublicClient, load_upbit_settings
from autobot.upbit.ws import UpbitWebSocketPublicClient
from autobot.upbit.ws.models import OrderbookEvent, TickerEvent, TradeEvent


PAIRED_RUNTIME_ARTIFACT_VERSION = 1
DEFAULT_PAIRED_PRESET = "live_v5"


_STREAM_SENTINEL = object()


@dataclass(frozen=True)
class RecordedPublicEventTape:
    markets: tuple[str, ...]
    ticker_events: tuple[TickerEvent, ...]
    trade_events: tuple[TradeEvent, ...]
    orderbook_events: tuple[OrderbookEvent, ...]
    duration_sec_requested: int


class ReplayPublicWsClient:
    def __init__(
        self,
        *,
        tape: RecordedPublicEventTape,
        replay_time_scale: float = 0.001,
        max_sleep_sec: float = 0.25,
    ) -> None:
        self._tape = tape
        self._replay_time_scale = max(float(replay_time_scale), 0.0)
        self._max_sleep_sec = max(float(max_sleep_sec), 0.0)

    async def stream_ticker(
        self,
        markets: Sequence[str],
        *,
        duration_sec: float | None = None,
    ) -> AsyncIterator[TickerEvent]:
        _ = duration_sec
        async for event in self._replay_events(self._tape.ticker_events, markets):
            yield event

    async def stream_trade(
        self,
        markets: Sequence[str],
        *,
        duration_sec: float | None = None,
    ) -> AsyncIterator[TradeEvent]:
        _ = duration_sec
        async for event in self._replay_events(self._tape.trade_events, markets):
            yield event

    async def stream_orderbook(
        self,
        markets: Sequence[str],
        *,
        duration_sec: float | None = None,
        level: int | str | None = 0,
    ) -> AsyncIterator[OrderbookEvent]:
        _ = (duration_sec, level)
        async for event in self._replay_events(self._tape.orderbook_events, markets):
            yield event

    async def _replay_events(self, events: Sequence[Any], markets: Sequence[str]) -> AsyncIterator[Any]:
        allowed = {str(item).strip().upper() for item in markets if str(item).strip()}
        previous_ts_ms: int | None = None
        for event in events:
            market = str(getattr(event, "market", "")).strip().upper()
            if allowed and market not in allowed:
                continue
            current_ts_ms = int(getattr(event, "ts_ms", 0) or 0)
            if previous_ts_ms is not None and current_ts_ms > previous_ts_ms:
                delta_ms = current_ts_ms - previous_ts_ms
                sleep_sec = min((delta_ms / 1000.0) * self._replay_time_scale, self._max_sleep_sec)
                if sleep_sec >= 0.005:
                    await asyncio.sleep(sleep_sec)
                else:
                    await asyncio.sleep(0)
            else:
                await asyncio.sleep(0)
            previous_ts_ms = current_ts_ms
            yield event


@dataclass
class _FanoutSubscriber:
    queue: asyncio.Queue[Any]
    allowed_markets: set[str]


class FanoutPublicWsClient:
    def __init__(
        self,
        *,
        source_client: UpbitWebSocketPublicClient,
        source_markets: Sequence[str],
        duration_sec: int,
        orderbook_level: int | str | None = 0,
        history_sec: int = 30,
        max_ticker_history: int = 2048,
        max_trade_history: int = 8192,
        max_orderbook_history: int = 2048,
    ) -> None:
        self._source_client = source_client
        self._source_markets = tuple(str(item).strip().upper() for item in source_markets if str(item).strip())
        self._duration_sec = max(int(duration_sec), 0)
        self._orderbook_level = orderbook_level
        self._history_window_ms = max(int(history_sec), 1) * 1000
        self._ticker_history: deque[TickerEvent] = deque(maxlen=max(int(max_ticker_history), 1))
        self._trade_history: deque[TradeEvent] = deque(maxlen=max(int(max_trade_history), 1))
        self._orderbook_history: deque[OrderbookEvent] = deque(maxlen=max(int(max_orderbook_history), 1))
        self._ticker_events_captured_total = 0
        self._trade_events_captured_total = 0
        self._orderbook_events_captured_total = 0
        self._ticker_subscribers: list[_FanoutSubscriber] = []
        self._trade_subscribers: list[_FanoutSubscriber] = []
        self._orderbook_subscribers: list[_FanoutSubscriber] = []
        self._ticker_task: asyncio.Task[None] | None = None
        self._trade_task: asyncio.Task[None] | None = None
        self._orderbook_task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        self._closed = False

    @property
    def capture_counts(self) -> dict[str, int]:
        return {
            "ticker_events_captured": int(self._ticker_events_captured_total),
            "trade_events_captured": int(self._trade_events_captured_total),
            "orderbook_events_captured": int(self._orderbook_events_captured_total),
            "ticker_events_buffered": len(self._ticker_history),
            "trade_events_buffered": len(self._trade_history),
            "orderbook_events_buffered": len(self._orderbook_history),
        }

    async def stream_ticker(
        self,
        markets: Sequence[str],
        *,
        duration_sec: float | None = None,
    ) -> AsyncIterator[TickerEvent]:
        _ = duration_sec
        async for event in self._stream(
            event_type="ticker",
            markets=markets,
            history=self._ticker_history,
            subscribers=self._ticker_subscribers,
            ensure_task=self._ensure_ticker_task,
        ):
            yield event

    async def stream_trade(
        self,
        markets: Sequence[str],
        *,
        duration_sec: float | None = None,
    ) -> AsyncIterator[TradeEvent]:
        _ = duration_sec
        async for event in self._stream(
            event_type="trade",
            markets=markets,
            history=self._trade_history,
            subscribers=self._trade_subscribers,
            ensure_task=self._ensure_trade_task,
        ):
            yield event

    async def stream_orderbook(
        self,
        markets: Sequence[str],
        *,
        duration_sec: float | None = None,
        level: int | str | None = 0,
    ) -> AsyncIterator[OrderbookEvent]:
        _ = (duration_sec, level)
        async for event in self._stream(
            event_type="orderbook",
            markets=markets,
            history=self._orderbook_history,
            subscribers=self._orderbook_subscribers,
            ensure_task=self._ensure_orderbook_task,
        ):
            yield event

    async def _stream(
        self,
        *,
        event_type: str,
        markets: Sequence[str],
        history: deque[Any],
        subscribers: list[_FanoutSubscriber],
        ensure_task: Callable[[], asyncio.Task[None]],
    ) -> AsyncIterator[Any]:
        allowed_markets = {str(item).strip().upper() for item in markets if str(item).strip()}
        queue: asyncio.Queue[Any] = asyncio.Queue()
        subscriber = _FanoutSubscriber(queue=queue, allowed_markets=allowed_markets)
        async with self._lock:
            if self._closed:
                return
            for event in list(history):
                event_market = str(getattr(event, "market", "")).strip().upper()
                if allowed_markets and event_market not in allowed_markets:
                    continue
                queue.put_nowait(event)
            subscribers.append(subscriber)
            ensure_task()
        try:
            while True:
                item = await queue.get()
                if item is _STREAM_SENTINEL:
                    break
                yield item
        finally:
            async with self._lock:
                if subscriber in subscribers:
                    subscribers.remove(subscriber)

    def _ensure_ticker_task(self) -> asyncio.Task[None]:
        if self._ticker_task is None:
            self._ticker_task = asyncio.create_task(
                self._pump_stream(
                    stream="ticker",
                    history=self._ticker_history,
                    subscribers=self._ticker_subscribers,
                )
            )
        return self._ticker_task

    def _ensure_trade_task(self) -> asyncio.Task[None]:
        if self._trade_task is None:
            self._trade_task = asyncio.create_task(
                self._pump_stream(
                    stream="trade",
                    history=self._trade_history,
                    subscribers=self._trade_subscribers,
                )
            )
        return self._trade_task

    def _ensure_orderbook_task(self) -> asyncio.Task[None]:
        if self._orderbook_task is None:
            self._orderbook_task = asyncio.create_task(
                self._pump_stream(
                    stream="orderbook",
                    history=self._orderbook_history,
                    subscribers=self._orderbook_subscribers,
                )
            )
        return self._orderbook_task

    async def _pump_stream(
        self,
        *,
        stream: str,
        history: deque[Any],
        subscribers: list[_FanoutSubscriber],
    ) -> None:
        try:
            if stream == "ticker":
                generator = self._source_client.stream_ticker(
                    self._source_markets,
                    duration_sec=(float(self._duration_sec) if self._duration_sec > 0 else None),
                )
            elif stream == "trade":
                generator = self._source_client.stream_trade(
                    self._source_markets,
                    duration_sec=(float(self._duration_sec) if self._duration_sec > 0 else None),
                )
            else:
                generator = self._source_client.stream_orderbook(
                    self._source_markets,
                    duration_sec=(float(self._duration_sec) if self._duration_sec > 0 else None),
                    level=self._orderbook_level,
                )
            async for event in generator:
                if self._closed:
                    break
                if stream == "ticker":
                    self._ticker_events_captured_total += 1
                elif stream == "trade":
                    self._trade_events_captured_total += 1
                else:
                    self._orderbook_events_captured_total += 1
                history.append(event)
                self._trim_history(history)
                event_market = str(getattr(event, "market", "")).strip().upper()
                async with self._lock:
                    targets = list(subscribers)
                for subscriber in targets:
                    if subscriber.allowed_markets and event_market not in subscriber.allowed_markets:
                        continue
                    subscriber.queue.put_nowait(event)
        finally:
            async with self._lock:
                targets = list(subscribers)
            for subscriber in targets:
                subscriber.queue.put_nowait(_STREAM_SENTINEL)

    async def close(self) -> None:
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            ticker_subscribers = list(self._ticker_subscribers)
            trade_subscribers = list(self._trade_subscribers)
            orderbook_subscribers = list(self._orderbook_subscribers)
            tasks = [task for task in (self._ticker_task, self._trade_task, self._orderbook_task) if task is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for subscriber in ticker_subscribers + trade_subscribers + orderbook_subscribers:
            subscriber.queue.put_nowait(_STREAM_SENTINEL)

    def _trim_history(self, history: deque[Any]) -> None:
        if not history:
            return
        newest_ts_ms = int(getattr(history[-1], "ts_ms", 0) or 0)
        if newest_ts_ms <= 0:
            return
        cutoff_ts_ms = newest_ts_ms - self._history_window_ms
        while history:
            oldest_ts_ms = int(getattr(history[0], "ts_ms", 0) or 0)
            if oldest_ts_ms <= 0 or oldest_ts_ms >= cutoff_ts_ms:
                break
            history.popleft()


async def run_recorded_paired_paper(
    *,
    upbit_settings: Any,
    champion_run_settings: PaperRunSettings,
    challenger_run_settings: PaperRunSettings,
    tape: RecordedPublicEventTape,
    output_root: Path,
    min_matched_opportunities: int = 1,
    min_challenger_hours: float = 12.0,
    min_orders_filled: int = 2,
    min_realized_pnl_quote: float = 0.0,
    min_micro_quality_score: float = 0.25,
    min_nonnegative_ratio: float = 0.34,
    max_drawdown_deterioration_factor: float = 1.10,
    micro_quality_tolerance: float = 0.02,
    nonnegative_ratio_tolerance: float = 0.05,
    max_time_to_fill_deterioration_factor: float = 1.25,
    replay_time_scale: float = 0.001,
    replay_max_sleep_sec: float = 0.25,
    engine_factory: Callable[..., Any] | None = None,
    rules_provider: Any | None = None,
    market_loader: Callable[[str], list[str]] | None = None,
    lane: str = "v4",
) -> dict[str, Any]:
    output_root = Path(output_root)
    run_root = output_root / "runs" / ("paired-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8])
    run_root.mkdir(parents=True, exist_ok=True)
    engine_ctor = engine_factory or PaperRunEngine
    resolved_markets = list(market_loader(champion_run_settings.quote)) if market_loader is not None else list(tape.markets)
    if not resolved_markets:
        resolved_markets = list(tape.markets)
    if not resolved_markets:
        raise RuntimeError("paired paper tape contains no markets")

    champion_settings = replace(champion_run_settings, out_root_dir=str(run_root / "champion"))
    challenger_settings = replace(challenger_run_settings, out_root_dir=str(run_root / "challenger"))

    champion_engine = engine_ctor(
        upbit_settings=upbit_settings,
        run_settings=champion_settings,
        ws_client=ReplayPublicWsClient(
            tape=tape,
            replay_time_scale=replay_time_scale,
            max_sleep_sec=replay_max_sleep_sec,
        ),
        market_loader=lambda quote: list(resolved_markets),
        rules_provider=rules_provider,
    )
    challenger_engine = engine_ctor(
        upbit_settings=upbit_settings,
        run_settings=challenger_settings,
        ws_client=ReplayPublicWsClient(
            tape=tape,
            replay_time_scale=replay_time_scale,
            max_sleep_sec=replay_max_sleep_sec,
        ),
        market_loader=lambda quote: list(resolved_markets),
        rules_provider=rules_provider,
    )

    with _paper_runtime_env(
        unit_name="autobot-paper-v5-paired.service",
        runtime_role="champion",
        lane=lane,
        pinned_model_ref=str(champion_settings.model_ref or champion_settings.model_alpha.model_ref or "").strip(),
    ):
        champion_summary = await champion_engine.run()

    with _paper_runtime_env(
        unit_name="autobot-paper-v5-paired.service",
        runtime_role="challenger",
        lane=lane,
        pinned_model_ref=str(challenger_settings.model_ref or challenger_settings.model_alpha.model_ref or "").strip(),
    ):
        challenger_summary = await challenger_engine.run()

    paired_report_path = run_root / "paired_paper_report.json"
    paired_report = build_paired_paper_report(
        champion_run_dir=Path(champion_summary.run_dir),
        challenger_run_dir=Path(challenger_summary.run_dir),
    )
    gate = _build_paired_gate(report=paired_report, min_matched_opportunities=min_matched_opportunities)
    promotion_decision = _build_paired_promotion_decision(
        champion_run_dir=Path(champion_summary.run_dir),
        challenger_run_dir=Path(challenger_summary.run_dir),
        paired_report=paired_report,
        paired_gate=gate,
        min_challenger_hours=min_challenger_hours,
        min_orders_filled=min_orders_filled,
        min_realized_pnl_quote=min_realized_pnl_quote,
        min_micro_quality_score=min_micro_quality_score,
        min_nonnegative_ratio=min_nonnegative_ratio,
        max_drawdown_deterioration_factor=max_drawdown_deterioration_factor,
        micro_quality_tolerance=micro_quality_tolerance,
        nonnegative_ratio_tolerance=nonnegative_ratio_tolerance,
        max_time_to_fill_deterioration_factor=max_time_to_fill_deterioration_factor,
    )
    payload = {
        "artifact_version": PAIRED_RUNTIME_ARTIFACT_VERSION,
        "mode": "paired_paper_live_runtime_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_root": str(run_root),
        "report_path": str(paired_report_path),
        "champion_run_dir": str(champion_summary.run_dir),
        "challenger_run_dir": str(challenger_summary.run_dir),
        "capture": {
            "duration_sec_requested": int(tape.duration_sec_requested),
            "markets_subscribed": int(len(tape.markets)),
            "ticker_events_captured": int(len(tape.ticker_events)),
            "trade_events_captured": int(len(tape.trade_events)),
            "orderbook_events_captured": int(len(tape.orderbook_events)),
            "replay_time_scale": float(replay_time_scale),
            "replay_max_sleep_sec": float(replay_max_sleep_sec),
        },
        "gate": gate,
        "paired_report": paired_report,
        "promotion_decision": promotion_decision,
    }
    write_paired_paper_report(report=paired_report, output_path=paired_report_path)
    _write_json(output_root / "latest.json", payload)
    archive_root = output_root / "archive"
    archive_root.mkdir(parents=True, exist_ok=True)
    _write_json(archive_root / (run_root.name + ".json"), payload)
    return payload


async def run_live_paired_paper(
    *,
    config_dir: Path,
    duration_sec: int,
    quote: str,
    top_n: int,
    tf: str,
    champion_model_ref: str,
    challenger_model_ref: str,
    model_family: str,
    champion_model_family: str | None = None,
    challenger_model_family: str | None = None,
    feature_set: str,
    preset: str,
    paper_feature_provider: str,
    paper_micro_provider: str,
    paper_micro_warmup_sec: int,
    paper_micro_warmup_min_trade_events_per_market: int,
    out_dir: Path,
    min_matched_opportunities: int,
    min_challenger_hours: float,
    min_orders_filled: int,
    min_realized_pnl_quote: float,
    min_micro_quality_score: float,
    min_nonnegative_ratio: float,
    max_drawdown_deterioration_factor: float,
    micro_quality_tolerance: float,
    nonnegative_ratio_tolerance: float,
    max_time_to_fill_deterioration_factor: float,
    replay_time_scale: float,
    replay_max_sleep_sec: float,
    source_ws_client: UpbitWebSocketPublicClient | None = None,
    engine_factory: Callable[..., Any] | None = None,
    rules_provider: Any | None = None,
) -> dict[str, Any]:
    if int(duration_sec) <= 0:
        raise ValueError("duration_sec must be positive for paired live paper")
    upbit_settings = load_upbit_settings(config_dir)
    markets = _load_quote_markets(upbit_settings, quote=str(quote).strip().upper())
    if not markets:
        raise RuntimeError(f"no quote markets available for quote={quote}")
    resolved_champion_model_family = str(champion_model_family or model_family).strip() or str(model_family).strip()
    resolved_challenger_model_family = str(challenger_model_family or model_family).strip() or str(model_family).strip()
    champion_settings = _build_paper_run_settings(
        model_ref=champion_model_ref,
        model_family=resolved_champion_model_family,
        feature_set=feature_set,
        preset=preset,
        quote=quote,
        top_n=top_n,
        tf=tf,
        duration_sec=duration_sec,
        paper_feature_provider=paper_feature_provider,
        paper_micro_provider=paper_micro_provider,
        paper_micro_warmup_sec=paper_micro_warmup_sec,
        paper_micro_warmup_min_trade_events_per_market=paper_micro_warmup_min_trade_events_per_market,
    )
    challenger_settings = _build_paper_run_settings(
        model_ref=challenger_model_ref,
        model_family=resolved_challenger_model_family,
        feature_set=feature_set,
        preset=preset,
        quote=quote,
        top_n=top_n,
        tf=tf,
        duration_sec=duration_sec,
        paper_feature_provider=paper_feature_provider,
        paper_micro_provider=paper_micro_provider,
        paper_micro_warmup_sec=paper_micro_warmup_sec,
        paper_micro_warmup_min_trade_events_per_market=paper_micro_warmup_min_trade_events_per_market,
    )
    source_client = source_ws_client or UpbitWebSocketPublicClient(upbit_settings.websocket)
    fanout_client = FanoutPublicWsClient(
        source_client=source_client,
        source_markets=markets,
        duration_sec=int(duration_sec),
        orderbook_level=0,
    )
    output_root = Path(out_dir)
    run_root = output_root / "runs" / ("paired-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8])
    run_root.mkdir(parents=True, exist_ok=True)
    engine_ctor = engine_factory or PaperRunEngine
    champion_settings = replace(champion_settings, out_root_dir=str(run_root / "champion"))
    challenger_settings = replace(challenger_settings, out_root_dir=str(run_root / "challenger"))

    async def _run_engine_with_env(*, run_settings: PaperRunSettings, runtime_role: str) -> Any:
        engine = engine_ctor(
            upbit_settings=upbit_settings,
            run_settings=run_settings,
            ws_client=fanout_client,
            market_loader=lambda quote_value: list(markets),
            rules_provider=rules_provider,
        )
        with _paper_runtime_env(
            unit_name="autobot-paper-v5-paired.service",
            runtime_role=runtime_role,
            lane="v5",
            pinned_model_ref=str(run_settings.model_ref or run_settings.model_alpha.model_ref or "").strip(),
        ):
            return await engine.run()

    champion_summary, challenger_summary = await asyncio.gather(
        _run_engine_with_env(run_settings=champion_settings, runtime_role="champion"),
        _run_engine_with_env(run_settings=challenger_settings, runtime_role="challenger"),
    )

    paired_report_path = run_root / "paired_paper_report.json"
    paired_report = build_paired_paper_report(
        champion_run_dir=Path(champion_summary.run_dir),
        challenger_run_dir=Path(challenger_summary.run_dir),
    )
    gate = _build_paired_gate(report=paired_report, min_matched_opportunities=min_matched_opportunities)
    promotion_decision = _build_paired_promotion_decision(
        champion_run_dir=Path(champion_summary.run_dir),
        challenger_run_dir=Path(challenger_summary.run_dir),
        paired_report=paired_report,
        paired_gate=gate,
        min_challenger_hours=min_challenger_hours,
        min_orders_filled=min_orders_filled,
        min_realized_pnl_quote=min_realized_pnl_quote,
        min_micro_quality_score=min_micro_quality_score,
        min_nonnegative_ratio=min_nonnegative_ratio,
        max_drawdown_deterioration_factor=max_drawdown_deterioration_factor,
        micro_quality_tolerance=micro_quality_tolerance,
        nonnegative_ratio_tolerance=nonnegative_ratio_tolerance,
        max_time_to_fill_deterioration_factor=max_time_to_fill_deterioration_factor,
    )
    payload = {
        "artifact_version": PAIRED_RUNTIME_ARTIFACT_VERSION,
        "mode": "paired_paper_live_fanout_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_root": str(run_root),
        "report_path": str(paired_report_path),
        "champion_run_dir": str(champion_summary.run_dir),
        "challenger_run_dir": str(challenger_summary.run_dir),
        "champion_model_family": resolved_champion_model_family,
        "challenger_model_family": resolved_challenger_model_family,
        "capture": {
            "duration_sec_requested": int(duration_sec),
            "markets_subscribed": int(len(markets)),
            "ticker_events_captured": int(fanout_client.capture_counts["ticker_events_captured"]),
            "trade_events_captured": int(fanout_client.capture_counts["trade_events_captured"]),
            "orderbook_events_captured": int(fanout_client.capture_counts["orderbook_events_captured"]),
            "source_mode": "live_ws_fanout",
        },
        "gate": gate,
        "paired_report": paired_report,
        "promotion_decision": promotion_decision,
    }
    write_paired_paper_report(report=paired_report, output_path=paired_report_path)
    _write_json(output_root / "latest.json", payload)
    archive_root = output_root / "archive"
    archive_root.mkdir(parents=True, exist_ok=True)
    _write_json(archive_root / (run_root.name + ".json"), payload)
    return payload


async def run_service_paired_paper(
    *,
    project_root: Path,
    config_dir: Path,
    quote: str,
    top_n: int,
    tf: str,
    model_family: str,
    champion_model_family: str | None = None,
    challenger_model_family: str | None = None,
    feature_set: str,
    preset: str,
    paper_feature_provider: str,
    paper_micro_provider: str,
    paper_micro_warmup_sec: int,
    paper_micro_warmup_min_trade_events_per_market: int,
    out_dir: Path,
    min_matched_opportunities: int,
    min_challenger_hours: float,
    min_orders_filled: int,
    min_realized_pnl_quote: float,
    min_micro_quality_score: float,
    min_nonnegative_ratio: float,
    max_drawdown_deterioration_factor: float,
    micro_quality_tolerance: float,
    nonnegative_ratio_tolerance: float,
    max_time_to_fill_deterioration_factor: float,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    state_path = root / "logs" / "model_v5_candidate" / "current_state.json"
    if not state_path.exists():
        state_path = root / "logs" / "model_v4_challenger" / "current_state.json"
    state = _load_json(state_path)
    candidate_run_id = str(state.get("candidate_run_id") or "").strip()
    champion_run_id = str(state.get("champion_run_id_at_start") or "").strip()
    state_candidate_model_family = str(state.get("candidate_model_family") or state.get("model_family") or "").strip()
    state_champion_model_family = str(
        state.get("champion_model_family_at_start")
        or state.get("champion_compare_model_family")
        or state.get("model_family")
        or ""
    ).strip()
    if not candidate_run_id:
        raise RuntimeError(f"paired paper service requires candidate_run_id in {state_path}")
    if not champion_run_id:
        raise RuntimeError(f"paired paper service requires champion_run_id_at_start in {state_path}")
    resolved_champion_model_family = (
        str(champion_model_family or state_champion_model_family or model_family).strip()
        or str(model_family).strip()
    )
    resolved_challenger_model_family = (
        str(challenger_model_family or state_candidate_model_family or model_family).strip()
        or str(model_family).strip()
    )

    upbit_settings = load_upbit_settings(config_dir)
    markets = _load_quote_markets(upbit_settings, quote=str(quote).strip().upper())
    if not markets:
        raise RuntimeError(f"no quote markets available for quote={quote}")

    champion_settings = _build_paper_run_settings(
        model_ref=champion_run_id,
        model_family=resolved_champion_model_family,
        feature_set=feature_set,
        preset=preset,
        quote=quote,
        top_n=top_n,
        tf=tf,
        duration_sec=0,
        paper_feature_provider=paper_feature_provider,
        paper_micro_provider=paper_micro_provider,
        paper_micro_warmup_sec=paper_micro_warmup_sec,
        paper_micro_warmup_min_trade_events_per_market=paper_micro_warmup_min_trade_events_per_market,
        allow_unbounded_duration=True,
    )
    challenger_settings = _build_paper_run_settings(
        model_ref=candidate_run_id,
        model_family=resolved_challenger_model_family,
        feature_set=feature_set,
        preset=preset,
        quote=quote,
        top_n=top_n,
        tf=tf,
        duration_sec=0,
        paper_feature_provider=paper_feature_provider,
        paper_micro_provider=paper_micro_provider,
        paper_micro_warmup_sec=paper_micro_warmup_sec,
        paper_micro_warmup_min_trade_events_per_market=paper_micro_warmup_min_trade_events_per_market,
        allow_unbounded_duration=True,
    )

    fanout_client = FanoutPublicWsClient(
        source_client=UpbitWebSocketPublicClient(upbit_settings.websocket),
        source_markets=markets,
        duration_sec=0,
        orderbook_level=0,
    )
    output_root = Path(out_dir)
    run_root = output_root / "runs" / ("paired-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8])
    run_root.mkdir(parents=True, exist_ok=True)
    champion_settings = replace(champion_settings, out_root_dir=str(run_root / "champion"))
    challenger_settings = replace(challenger_settings, out_root_dir=str(run_root / "challenger"))

    async def _run_engine_with_env(*, run_settings: PaperRunSettings, runtime_role: str) -> Any:
        engine = PaperRunEngine(
            upbit_settings=upbit_settings,
            run_settings=run_settings,
            ws_client=fanout_client,
            market_loader=lambda quote_value: list(markets),
        )
        with _paper_runtime_env(
            unit_name="autobot-paper-v5-paired.service",
            runtime_role=runtime_role,
            lane="v5",
            pinned_model_ref=str(run_settings.model_ref or run_settings.model_alpha.model_ref or "").strip(),
        ):
            return await engine.run()

    loop = asyncio.get_running_loop()
    stop_requested = asyncio.Event()

    def _request_stop() -> None:
        if stop_requested.is_set():
            return
        stop_requested.set()
        loop.create_task(fanout_client.close())

    for signal_name in ("SIGTERM", "SIGINT"):
        signal_value = getattr(signal, signal_name, None)
        if signal_value is None:
            continue
        try:
            loop.add_signal_handler(signal_value, _request_stop)
        except NotImplementedError:
            pass

    try:
        champion_summary, challenger_summary = await asyncio.gather(
            _run_engine_with_env(run_settings=champion_settings, runtime_role="champion"),
            _run_engine_with_env(run_settings=challenger_settings, runtime_role="challenger"),
        )
    finally:
        await fanout_client.close()

    paired_report_path = run_root / "paired_paper_report.json"
    paired_report = build_paired_paper_report(
        champion_run_dir=Path(champion_summary.run_dir),
        challenger_run_dir=Path(challenger_summary.run_dir),
    )
    gate = _build_paired_gate(report=paired_report, min_matched_opportunities=min_matched_opportunities)
    promotion_decision = _build_paired_promotion_decision(
        champion_run_dir=Path(champion_summary.run_dir),
        challenger_run_dir=Path(challenger_summary.run_dir),
        paired_report=paired_report,
        paired_gate=gate,
        min_challenger_hours=min_challenger_hours,
        min_orders_filled=min_orders_filled,
        min_realized_pnl_quote=min_realized_pnl_quote,
        min_micro_quality_score=min_micro_quality_score,
        min_nonnegative_ratio=min_nonnegative_ratio,
        max_drawdown_deterioration_factor=max_drawdown_deterioration_factor,
        micro_quality_tolerance=micro_quality_tolerance,
        nonnegative_ratio_tolerance=nonnegative_ratio_tolerance,
        max_time_to_fill_deterioration_factor=max_time_to_fill_deterioration_factor,
    )
    payload = {
        "artifact_version": PAIRED_RUNTIME_ARTIFACT_VERSION,
        "mode": "paired_paper_live_service_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "project_root": str(root),
        "state_path": str(state_path),
        "run_root": str(run_root),
        "report_path": str(paired_report_path),
        "champion_run_dir": str(champion_summary.run_dir),
        "challenger_run_dir": str(challenger_summary.run_dir),
        "champion_model_family": resolved_champion_model_family,
        "challenger_model_family": resolved_challenger_model_family,
        "capture": {
            "duration_sec_requested": 0,
            "markets_subscribed": int(len(markets)),
            "ticker_events_captured": int(fanout_client.capture_counts["ticker_events_captured"]),
            "trade_events_captured": int(fanout_client.capture_counts["trade_events_captured"]),
            "orderbook_events_captured": int(fanout_client.capture_counts["orderbook_events_captured"]),
            "source_mode": "live_ws_fanout_service",
        },
        "gate": gate,
        "paired_report": paired_report,
        "promotion_decision": promotion_decision,
    }
    write_paired_paper_report(report=paired_report, output_path=paired_report_path)
    _write_json(output_root / "latest.json", payload)
    archive_root = output_root / "archive"
    archive_root.mkdir(parents=True, exist_ok=True)
    _write_json(archive_root / (run_root.name + ".json"), payload)
    return payload


def _build_paper_run_settings(
    *,
    model_ref: str,
    model_family: str,
    feature_set: str,
    preset: str,
    quote: str,
    top_n: int,
    tf: str,
    duration_sec: int,
    paper_feature_provider: str,
    paper_micro_provider: str,
    paper_micro_warmup_sec: int,
    paper_micro_warmup_min_trade_events_per_market: int,
    allow_unbounded_duration: bool = False,
) -> PaperRunSettings:
    overrides = paper_alpha_preset_overrides(preset)
    selection_top_pct = float(overrides.get("top_pct", 0.50))
    selection_min_prob = None
    selection_min_candidates = int(overrides.get("min_cands_per_ts", 1))
    execution_price_mode = str(ModelAlphaExecutionSettings().price_mode).strip().upper()
    if str(overrides.get("micro_order_policy_mode", "")).strip().lower() == "trade_only":
        micro_order_policy = MicroOrderPolicySettings(
            enabled=True,
            mode="trade_only",
            on_missing=str(overrides.get("micro_order_policy_on_missing", "static_fallback")).strip().lower()
            or "static_fallback",
        )
    else:
        micro_order_policy = MicroOrderPolicySettings(enabled=True)
    return PaperRunSettings(
        duration_sec=max(int(duration_sec), 0) if allow_unbounded_duration else max(int(duration_sec), 1),
        quote=str(quote).strip().upper() or "KRW",
        top_n=max(int(top_n), 1),
        tf=str(tf).strip().lower() or "5m",
        strategy="model_alpha_v1",
        model_ref=str(model_ref).strip(),
        model_family=str(model_family).strip() or "train_v4_crypto_cs",
        feature_set=str(feature_set).strip().lower() or "v4",
        model_alpha=ModelAlphaSettings(
            model_ref=str(model_ref).strip(),
            model_family=str(model_family).strip() or "train_v4_crypto_cs",
            feature_set=str(feature_set).strip().lower() or "v4",
            selection=ModelAlphaSelectionSettings(
                top_pct=max(min(selection_top_pct, 1.0), 0.0),
                min_prob=selection_min_prob,
                min_candidates_per_ts=max(selection_min_candidates, 1),
                use_learned_recommendations=True,
            ),
            position=ModelAlphaPositionSettings(),
            exit=ModelAlphaExitSettings(),
            execution=ModelAlphaExecutionSettings(price_mode=execution_price_mode),
        ),
        micro_gate=MicroGateSettings(enabled=False),
        micro_order_policy=micro_order_policy,
        paper_micro_provider=str(paper_micro_provider).strip().lower() or str(overrides.get("paper_micro_provider", "live_ws")),
        paper_micro_warmup_sec=max(int(paper_micro_warmup_sec), 0),
        paper_micro_warmup_min_trade_events_per_market=max(int(paper_micro_warmup_min_trade_events_per_market), 1),
        paper_feature_provider=str(paper_feature_provider).strip().lower()
        or str(overrides.get("paper_feature_provider", "live_v4")).strip().lower(),
    )


def _build_paired_gate(*, report: dict[str, Any], min_matched_opportunities: int) -> dict[str, Any]:
    clock_alignment = dict(report.get("clock_alignment") or {})
    matched_opportunities = int(clock_alignment.get("matched_opportunities") or 0)
    pair_ready = bool(clock_alignment.get("pair_ready"))
    min_required = max(int(min_matched_opportunities), 1)
    gate_pass = pair_ready and matched_opportunities >= min_required
    if gate_pass:
        reason = "PAIRED_PAPER_READY"
    elif not pair_ready:
        reason = "PAIRED_PAPER_NOT_READY"
    else:
        reason = "INSUFFICIENT_MATCHED_OPPORTUNITIES"
    return {
        "evaluated": True,
        "pair_ready": pair_ready,
        "matched_opportunities": matched_opportunities,
        "min_matched_opportunities": min_required,
        "pass": gate_pass,
        "reason": reason,
    }


def _build_paired_promotion_decision(
    *,
    champion_run_dir: Path,
    challenger_run_dir: Path,
    paired_report: dict[str, Any],
    paired_gate: dict[str, Any],
    min_challenger_hours: float,
    min_orders_filled: int,
    min_realized_pnl_quote: float,
    min_micro_quality_score: float,
    min_nonnegative_ratio: float,
    max_drawdown_deterioration_factor: float,
    micro_quality_tolerance: float,
    nonnegative_ratio_tolerance: float,
    max_time_to_fill_deterioration_factor: float,
) -> dict[str, Any]:
    champion_summary = _load_json(champion_run_dir / "summary.json")
    challenger_summary = _load_json(challenger_run_dir / "summary.json")
    champion_agg = aggregate_paper_lane_runs([champion_summary] if champion_summary else [])
    challenger_agg = aggregate_paper_lane_runs([challenger_summary] if challenger_summary else [])
    base_decision = compare_champion_challenger(
        champion=champion_agg,
        challenger=challenger_agg,
        min_challenger_hours=min_challenger_hours,
        min_orders_filled=min_orders_filled,
        min_realized_pnl_quote=min_realized_pnl_quote,
        min_micro_quality_score=min_micro_quality_score,
        min_nonnegative_ratio=min_nonnegative_ratio,
        max_drawdown_deterioration_factor=max_drawdown_deterioration_factor,
        micro_quality_tolerance=micro_quality_tolerance,
        nonnegative_ratio_tolerance=nonnegative_ratio_tolerance,
        max_time_to_fill_deterioration_factor=max_time_to_fill_deterioration_factor,
    )
    hard_failures = list(base_decision.get("hard_failures") or [])
    gate_pass = bool(paired_gate.get("pass"))
    gate_reason = str(paired_gate.get("reason") or "").strip()
    promote = bool(base_decision.get("promote", False)) and gate_pass
    if not gate_pass and gate_reason:
        hard_failures.append(gate_reason)
    decision_text = "promote_challenger" if promote else ("keep_champion" if hard_failures else "hold_for_review")
    return {
        "comparison_mode": "paired_paper_runtime_decision_v1",
        "paired_gate": dict(paired_gate),
        "paired_report_excerpt": {
            "clock_alignment": dict(paired_report.get("clock_alignment") or {}),
            "paired_deltas": dict(paired_report.get("paired_deltas") or {}),
            "taxonomy_counts": dict(paired_report.get("taxonomy_counts") or {}),
            "report_path": str(paired_report.get("report_path") or ""),
        },
        "champion": champion_agg,
        "challenger": challenger_agg,
        "decision": {
            **dict(base_decision),
            "promote": promote,
            "decision": decision_text,
            "hard_failures": hard_failures,
        },
    }


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_quote_markets(upbit_settings: Any, *, quote: str) -> list[str]:
    quote_prefix = f"{str(quote).strip().upper()}-"
    with UpbitHttpClient(upbit_settings) as http_client:
        payload = UpbitPublicClient(http_client).markets(is_details=True)
    if not isinstance(payload, list):
        return []
    markets: list[str] = []
    seen: set[str] = set()
    for item in payload:
        if not isinstance(item, dict):
            continue
        market = str(item.get("market", "")).strip().upper()
        if not market.startswith(quote_prefix):
            continue
        if market in seen:
            continue
        seen.add(market)
        markets.append(market)
    return markets


@contextmanager
def _paper_runtime_env(
    *,
    unit_name: str,
    runtime_role: str,
    lane: str,
    pinned_model_ref: str,
):
    keys = {
        "AUTOBOT_PAPER_UNIT_NAME": unit_name,
        "AUTOBOT_PAPER_RUNTIME_ROLE": runtime_role,
        "AUTOBOT_PAPER_LANE": lane,
        "AUTOBOT_PAPER_MODEL_REF_PINNED": pinned_model_ref,
    }
    previous = {key: os.environ.get(key) for key in keys}
    try:
        for key, value in keys.items():
            os.environ[key] = str(value)
        yield
    finally:
        for key, old_value in previous.items():
            if old_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old_value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run or report paired paper comparisons.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    report_parser = subparsers.add_parser("report", help="Build a paired paper report from existing run directories.")
    report_parser.add_argument("--champion-run-dir", required=True)
    report_parser.add_argument("--challenger-run-dir", required=True)
    report_parser.add_argument("--output-path", default="")

    live_parser = subparsers.add_parser("run-live", help="Capture one live feed window and replay it into champion/challenger paper runs.")
    live_parser.add_argument("--config-dir", default="config")
    live_parser.add_argument("--duration-sec", type=int, required=True)
    live_parser.add_argument("--quote", default="KRW")
    live_parser.add_argument("--top-n", type=int, default=20)
    live_parser.add_argument("--tf", default="5m")
    live_parser.add_argument("--champion-model-ref", required=True)
    live_parser.add_argument("--challenger-model-ref", required=True)
    live_parser.add_argument("--model-family", default="train_v4_crypto_cs")
    live_parser.add_argument("--champion-model-family", default="")
    live_parser.add_argument("--challenger-model-family", default="")
    live_parser.add_argument("--feature-set", default="v4")
    live_parser.add_argument("--preset", default=DEFAULT_PAIRED_PRESET)
    live_parser.add_argument("--paper-feature-provider", default="live_v4")
    live_parser.add_argument("--paper-micro-provider", default="live_ws")
    live_parser.add_argument("--paper-micro-warmup-sec", type=int, default=60)
    live_parser.add_argument("--paper-micro-warmup-min-trade-events-per-market", type=int, default=1)
    live_parser.add_argument("--out-dir", default="logs/paired_paper")
    live_parser.add_argument("--min-matched-opportunities", type=int, default=1)
    live_parser.add_argument("--min-challenger-hours", type=float, default=12.0)
    live_parser.add_argument("--min-orders-filled", type=int, default=2)
    live_parser.add_argument("--min-realized-pnl-quote", type=float, default=0.0)
    live_parser.add_argument("--min-micro-quality-score", type=float, default=0.25)
    live_parser.add_argument("--min-nonnegative-ratio", type=float, default=0.34)
    live_parser.add_argument("--max-drawdown-deterioration-factor", type=float, default=1.10)
    live_parser.add_argument("--micro-quality-tolerance", type=float, default=0.02)
    live_parser.add_argument("--nonnegative-ratio-tolerance", type=float, default=0.05)
    live_parser.add_argument("--max-time-to-fill-deterioration-factor", type=float, default=1.25)
    live_parser.add_argument("--replay-time-scale", type=float, default=0.001)
    live_parser.add_argument("--replay-max-sleep-sec", type=float, default=0.25)

    service_parser = subparsers.add_parser("run-service", help="Run long-lived paired paper lane until externally stopped.")
    service_parser.add_argument("--project-root", required=True)
    service_parser.add_argument("--config-dir", default="config")
    service_parser.add_argument("--quote", default="KRW")
    service_parser.add_argument("--top-n", type=int, default=20)
    service_parser.add_argument("--tf", default="5m")
    service_parser.add_argument("--model-family", default="train_v4_crypto_cs")
    service_parser.add_argument("--champion-model-family", default="")
    service_parser.add_argument("--challenger-model-family", default="")
    service_parser.add_argument("--feature-set", default="v4")
    service_parser.add_argument("--preset", default=DEFAULT_PAIRED_PRESET)
    service_parser.add_argument("--paper-feature-provider", default="live_v4")
    service_parser.add_argument("--paper-micro-provider", default="live_ws")
    service_parser.add_argument("--paper-micro-warmup-sec", type=int, default=60)
    service_parser.add_argument("--paper-micro-warmup-min-trade-events-per-market", type=int, default=1)
    service_parser.add_argument("--out-dir", default="logs/paired_paper")
    service_parser.add_argument("--min-matched-opportunities", type=int, default=1)
    service_parser.add_argument("--min-challenger-hours", type=float, default=12.0)
    service_parser.add_argument("--min-orders-filled", type=int, default=2)
    service_parser.add_argument("--min-realized-pnl-quote", type=float, default=0.0)
    service_parser.add_argument("--min-micro-quality-score", type=float, default=0.25)
    service_parser.add_argument("--min-nonnegative-ratio", type=float, default=0.34)
    service_parser.add_argument("--max-drawdown-deterioration-factor", type=float, default=1.10)
    service_parser.add_argument("--micro-quality-tolerance", type=float, default=0.02)
    service_parser.add_argument("--nonnegative-ratio-tolerance", type=float, default=0.05)
    service_parser.add_argument("--max-time-to-fill-deterioration-factor", type=float, default=1.25)
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    if args.command == "report":
        report = build_paired_paper_report(
            champion_run_dir=Path(args.champion_run_dir),
            challenger_run_dir=Path(args.challenger_run_dir),
        )
        output_path = Path(args.output_path) if str(args.output_path).strip() else None
        if output_path is not None:
            write_paired_paper_report(report=report, output_path=output_path)
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    if args.command == "run-service":
        payload = asyncio.run(
            run_service_paired_paper(
                project_root=Path(args.project_root),
                config_dir=Path(args.config_dir),
                quote=str(args.quote).strip().upper(),
                top_n=int(args.top_n),
                tf=str(args.tf).strip().lower(),
                model_family=str(args.model_family).strip(),
                champion_model_family=str(args.champion_model_family).strip() or None,
                challenger_model_family=str(args.challenger_model_family).strip() or None,
                feature_set=str(args.feature_set).strip().lower(),
                preset=str(args.preset).strip().lower(),
                paper_feature_provider=str(args.paper_feature_provider).strip().lower(),
                paper_micro_provider=str(args.paper_micro_provider).strip().lower(),
                paper_micro_warmup_sec=int(args.paper_micro_warmup_sec),
                paper_micro_warmup_min_trade_events_per_market=int(args.paper_micro_warmup_min_trade_events_per_market),
                out_dir=Path(args.out_dir),
                min_matched_opportunities=int(args.min_matched_opportunities),
                min_challenger_hours=float(args.min_challenger_hours),
                min_orders_filled=int(args.min_orders_filled),
                min_realized_pnl_quote=float(args.min_realized_pnl_quote),
                min_micro_quality_score=float(args.min_micro_quality_score),
                min_nonnegative_ratio=float(args.min_nonnegative_ratio),
                max_drawdown_deterioration_factor=float(args.max_drawdown_deterioration_factor),
                micro_quality_tolerance=float(args.micro_quality_tolerance),
                nonnegative_ratio_tolerance=float(args.nonnegative_ratio_tolerance),
                max_time_to_fill_deterioration_factor=float(args.max_time_to_fill_deterioration_factor),
            )
        )
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    payload = asyncio.run(
        run_live_paired_paper(
            config_dir=Path(args.config_dir),
            duration_sec=int(args.duration_sec),
            quote=str(args.quote).strip().upper(),
            top_n=int(args.top_n),
            tf=str(args.tf).strip().lower(),
            champion_model_ref=str(args.champion_model_ref).strip(),
            challenger_model_ref=str(args.challenger_model_ref).strip(),
            model_family=str(args.model_family).strip(),
            champion_model_family=str(args.champion_model_family).strip() or None,
            challenger_model_family=str(args.challenger_model_family).strip() or None,
            feature_set=str(args.feature_set).strip().lower(),
            preset=str(args.preset).strip().lower(),
            paper_feature_provider=str(args.paper_feature_provider).strip().lower(),
            paper_micro_provider=str(args.paper_micro_provider).strip().lower(),
            paper_micro_warmup_sec=int(args.paper_micro_warmup_sec),
            paper_micro_warmup_min_trade_events_per_market=int(args.paper_micro_warmup_min_trade_events_per_market),
            out_dir=Path(args.out_dir),
            min_matched_opportunities=int(args.min_matched_opportunities),
            min_challenger_hours=float(args.min_challenger_hours),
            min_orders_filled=int(args.min_orders_filled),
            min_realized_pnl_quote=float(args.min_realized_pnl_quote),
            min_micro_quality_score=float(args.min_micro_quality_score),
            min_nonnegative_ratio=float(args.min_nonnegative_ratio),
            max_drawdown_deterioration_factor=float(args.max_drawdown_deterioration_factor),
            micro_quality_tolerance=float(args.micro_quality_tolerance),
            nonnegative_ratio_tolerance=float(args.nonnegative_ratio_tolerance),
            max_time_to_fill_deterioration_factor=float(args.max_time_to_fill_deterioration_factor),
            replay_time_scale=float(args.replay_time_scale),
            replay_max_sleep_sec=float(args.replay_max_sleep_sec),
        )
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
