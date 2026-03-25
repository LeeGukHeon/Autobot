from __future__ import annotations

import argparse
import asyncio
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Sequence
import uuid

from autobot.cli_model_helpers import paper_alpha_preset_overrides
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
DEFAULT_PAIRED_PRESET = "live_v4"


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


async def run_recorded_paired_paper(
    *,
    upbit_settings: Any,
    champion_run_settings: PaperRunSettings,
    challenger_run_settings: PaperRunSettings,
    tape: RecordedPublicEventTape,
    output_root: Path,
    min_matched_opportunities: int = 1,
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
        unit_name="autobot-paper-v4-paired.service",
        runtime_role="champion",
        lane=lane,
        pinned_model_ref=str(champion_settings.model_ref or champion_settings.model_alpha.model_ref or "").strip(),
    ):
        champion_summary = await champion_engine.run()

    with _paper_runtime_env(
        unit_name="autobot-paper-v4-paired.service",
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
    feature_set: str,
    preset: str,
    paper_feature_provider: str,
    paper_micro_provider: str,
    paper_micro_warmup_sec: int,
    paper_micro_warmup_min_trade_events_per_market: int,
    out_dir: Path,
    min_matched_opportunities: int,
    replay_time_scale: float,
    replay_max_sleep_sec: float,
) -> dict[str, Any]:
    if int(duration_sec) <= 0:
        raise ValueError("duration_sec must be positive for paired live paper")
    upbit_settings = load_upbit_settings(config_dir)
    markets = _load_quote_markets(upbit_settings, quote=str(quote).strip().upper())
    if not markets:
        raise RuntimeError(f"no quote markets available for quote={quote}")

    tape = await _collect_public_event_tape(
        upbit_settings=upbit_settings,
        markets=markets,
        duration_sec=int(duration_sec),
    )
    champion_settings = _build_paper_run_settings(
        model_ref=champion_model_ref,
        model_family=model_family,
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
        model_family=model_family,
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
    return await run_recorded_paired_paper(
        upbit_settings=upbit_settings,
        champion_run_settings=champion_settings,
        challenger_run_settings=challenger_settings,
        tape=tape,
        output_root=Path(out_dir),
        min_matched_opportunities=min_matched_opportunities,
        replay_time_scale=replay_time_scale,
        replay_max_sleep_sec=replay_max_sleep_sec,
        market_loader=lambda quote_value: list(markets),
    )


async def _collect_public_event_tape(
    *,
    upbit_settings: Any,
    markets: Sequence[str],
    duration_sec: int,
) -> RecordedPublicEventTape:
    client = UpbitWebSocketPublicClient(upbit_settings.websocket)
    ticker_events: list[TickerEvent] = []
    trade_events: list[TradeEvent] = []
    orderbook_events: list[OrderbookEvent] = []

    async def _collect(generator: AsyncIterator[Any], sink: list[Any]) -> None:
        async for item in generator:
            sink.append(item)

    await asyncio.gather(
        _collect(client.stream_ticker(markets, duration_sec=float(duration_sec)), ticker_events),
        _collect(client.stream_trade(markets, duration_sec=float(duration_sec)), trade_events),
        _collect(client.stream_orderbook(markets, duration_sec=float(duration_sec), level=0), orderbook_events),
    )
    ticker_events.sort(key=lambda item: int(getattr(item, "ts_ms", 0) or 0))
    trade_events.sort(key=lambda item: int(getattr(item, "ts_ms", 0) or 0))
    orderbook_events.sort(key=lambda item: int(getattr(item, "ts_ms", 0) or 0))
    return RecordedPublicEventTape(
        markets=tuple(str(item).strip().upper() for item in markets if str(item).strip()),
        ticker_events=tuple(ticker_events),
        trade_events=tuple(trade_events),
        orderbook_events=tuple(orderbook_events),
        duration_sec_requested=int(duration_sec),
    )


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
        duration_sec=max(int(duration_sec), 1),
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
    live_parser.add_argument("--feature-set", default="v4")
    live_parser.add_argument("--preset", default=DEFAULT_PAIRED_PRESET)
    live_parser.add_argument("--paper-feature-provider", default="live_v4")
    live_parser.add_argument("--paper-micro-provider", default="live_ws")
    live_parser.add_argument("--paper-micro-warmup-sec", type=int, default=60)
    live_parser.add_argument("--paper-micro-warmup-min-trade-events-per-market", type=int, default=1)
    live_parser.add_argument("--out-dir", default="logs/paired_paper")
    live_parser.add_argument("--min-matched-opportunities", type=int, default=1)
    live_parser.add_argument("--replay-time-scale", type=float, default=0.001)
    live_parser.add_argument("--replay-max-sleep-sec", type=float, default=0.25)
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
            feature_set=str(args.feature_set).strip().lower(),
            preset=str(args.preset).strip().lower(),
            paper_feature_provider=str(args.paper_feature_provider).strip().lower(),
            paper_micro_provider=str(args.paper_micro_provider).strip().lower(),
            paper_micro_warmup_sec=int(args.paper_micro_warmup_sec),
            paper_micro_warmup_min_trade_events_per_market=int(args.paper_micro_warmup_min_trade_events_per_market),
            out_dir=Path(args.out_dir),
            min_matched_opportunities=int(args.min_matched_opportunities),
            replay_time_scale=float(args.replay_time_scale),
            replay_max_sleep_sec=float(args.replay_max_sleep_sec),
        )
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
