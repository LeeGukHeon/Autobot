from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from autobot.backtest.engine import BacktestRunEngine, BacktestRunSettings
from autobot.paper.sim_exchange import MarketRules
from autobot.strategy.micro_order_policy import (
    MicroOrderPolicySettings,
    MicroOrderPolicyTierSettings,
    MicroOrderPolicyTieringSettings,
    MicroOrderPolicyTiersSettings,
)
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


class _StableMicroProvider:
    def get(self, market: str, ts_ms: int) -> MicroSnapshot | None:
        return MicroSnapshot(
            market=market,
            snapshot_ts_ms=ts_ms,
            last_event_ts_ms=ts_ms,
            trade_events=1,
            trade_coverage_ms=60_000,
            trade_notional_krw=100.0,
            trade_imbalance=0.0,
            trade_source="ws",
            spread_bps_mean=5.0,
            depth_top5_notional_krw=100_000.0,
            book_events=1,
            book_coverage_ms=60_000,
            book_available=True,
        )


def _write_sample_parquet(root: Path) -> None:
    target = root / "candles_v1" / "tf=1m" / "market=KRW-BTC"
    target.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame(
        {
            "ts_ms": [0, 60_000, 120_000, 180_000],
            "open": [100.0, 101.0, 102.0, 99.0],
            "high": [101.0, 102.0, 103.0, 100.0],
            "low": [100.0, 101.0, 102.0, 98.0],
            "close": [100.0, 101.0, 102.0, 99.0],
            "volume_base": [1.0, 1.0, 1.0, 1.0],
            "volume_quote": [100.0, 101.0, 102.0, 99.0],
            "volume_quote_est": [False, False, False, False],
        }
    )
    frame.write_parquet(target / "part-000.parquet")


def test_backtest_engine_emits_replace_events_with_micro_order_policy(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    _write_sample_parquet(parquet_root)

    policy = MicroOrderPolicySettings(
        enabled=True,
        on_missing="static_fallback",
        tiering=MicroOrderPolicyTieringSettings(w_notional=1.0, w_events=0.5, t1=100.0, t2=200.0),
        tiers=MicroOrderPolicyTiersSettings(
            low=MicroOrderPolicyTierSettings(
                timeout_ms=600_000,
                replace_interval_ms=120_000,
                max_replaces=1,
                price_mode="PASSIVE_MAKER",
                max_chase_bps=1_000,
            )
        ),
    )
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
        micro_order_policy=policy,
    )
    engine = BacktestRunEngine(
        run_settings=settings,
        upbit_settings=None,
        rules_provider=_StaticRulesProvider(),  # type: ignore[arg-type]
        micro_snapshot_provider=_StableMicroProvider(),  # type: ignore[arg-type]
    )

    summary = engine.run()
    assert summary.replaces_total >= 1
    assert summary.cancels_total >= 1
    assert summary.aborted_timeout_total == 0

    run_dir = Path(summary.run_dir)
    events_payloads = [
        json.loads(line)
        for line in (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert any(item.get("event_type") == "ORDER_REPLACED" for item in events_payloads)

    report = json.loads((run_dir / "micro_order_policy_report.json").read_text(encoding="utf-8"))
    assert report["tiers"].get("LOW", 0) >= 1
    assert report["replace_reasons"].get("TIMEOUT_REPLACE", 0) >= 1
    assert "tick_bps_stats" in report
    assert "cross_block_reasons" in report
    assert "cross_allowed_count" in report
    assert "cross_used_count" in report
    assert "resolver_failed_fallback_used_count" in report
    assert (run_dir / "slippage_by_market.csv").exists()
    assert (run_dir / "price_mode_by_market.csv").exists()
