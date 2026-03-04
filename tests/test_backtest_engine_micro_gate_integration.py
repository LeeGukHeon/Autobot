from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from autobot.backtest.engine import BacktestRunEngine, BacktestRunSettings
from autobot.paper.sim_exchange import MarketRules
from autobot.strategy.micro_gate_v1 import MicroGateSettings, MicroGateTradeSettings
from autobot.strategy.micro_snapshot import MicroSnapshot


class _StaticRulesProvider:
    def get_rules(self, *, market: str, reference_price: float, ts_ms: int) -> MarketRules:
        _ = (market, reference_price, ts_ms)
        return MarketRules(
            bid_fee=0.0005,
            ask_fee=0.0005,
            maker_bid_fee=0.0002,
            maker_ask_fee=0.0002,
            min_total=5_000.0,
            tick_size=1.0,
        )


class _LowLiquidityProvider:
    def get(self, market: str, ts_ms: int) -> MicroSnapshot | None:
        _ = market
        return MicroSnapshot(
            market="KRW-BTC",
            snapshot_ts_ms=ts_ms,
            last_event_ts_ms=ts_ms,
            trade_events=0,
            trade_coverage_ms=0,
            trade_notional_krw=0.0,
            trade_imbalance=None,
            trade_source="ws",
            spread_bps_mean=None,
            depth_top5_notional_krw=None,
            book_events=0,
            book_coverage_ms=0,
            book_available=False,
        )


def _write_sample_parquet(root: Path) -> None:
    target = root / "candles_v1" / "tf=1m" / "market=KRW-BTC"
    target.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame(
        {
            "ts_ms": [0, 60_000, 120_000],
            "open": [100.0, 100.0, 101.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 99.0, 100.0],
            "close": [100.0, 101.0, 102.0],
            "volume_base": [1.0, 1.5, 1.8],
            "volume_quote": [100.0, 151.5, 183.6],
            "volume_quote_est": [False, False, False],
        }
    )
    frame.write_parquet(target / "part-000.parquet")


def test_backtest_engine_summarizes_micro_blocked_counts(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    _write_sample_parquet(parquet_root)

    settings = BacktestRunSettings(
        dataset_name="candles_v1",
        parquet_root=str(parquet_root),
        tf="1m",
        market="KRW-BTC",
        duration_days=1,
        starting_krw=50_000.0,
        per_trade_krw=10_000.0,
        max_positions=2,
        output_root_dir=str(tmp_path / "backtest"),
        micro_gate=MicroGateSettings(
            enabled=True,
            mode="trade_only",
            on_missing="warn_allow",
            trade=MicroGateTradeSettings(min_trade_events=1),
        ),
    )
    engine = BacktestRunEngine(
        run_settings=settings,
        upbit_settings=None,
        rules_provider=_StaticRulesProvider(),  # type: ignore[arg-type]
        micro_snapshot_provider=_LowLiquidityProvider(),  # type: ignore[arg-type]
    )

    summary = engine.run()
    assert summary.candidates_blocked_by_micro >= 1
    assert summary.micro_blocked_reasons.get("LOW_LIQUIDITY_TRADE", 0) >= 1

    run_dir = Path(summary.run_dir)
    summary_json = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary_json["candidates_blocked_by_micro"] == summary.candidates_blocked_by_micro
