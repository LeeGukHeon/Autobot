from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from autobot.backtest.engine import BacktestRunEngine, BacktestRunSettings
from autobot.paper.sim_exchange import MarketRules


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


def test_backtest_run_generates_artifacts(tmp_path: Path) -> None:
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
    )
    engine = BacktestRunEngine(run_settings=settings, upbit_settings=None, rules_provider=_StaticRulesProvider())  # type: ignore[arg-type]

    summary = engine.run()
    assert summary.orders_submitted >= 1
    assert summary.orders_filled >= 1

    run_dir = Path(summary.run_dir)
    assert (run_dir / "events.jsonl").exists()
    assert (run_dir / "orders.jsonl").exists()
    assert (run_dir / "fills.jsonl").exists()
    assert (run_dir / "equity.csv").exists()
    assert (run_dir / "summary.json").exists()

    summary_json = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary_json["run_id"] == summary.run_id
    assert summary_json["bars_processed"] >= 1
    assert "execution_structure" in summary_json


def test_backtest_run_summary_only_skips_heavy_artifacts(tmp_path: Path) -> None:
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
        artifact_mode="summary_only",
    )
    engine = BacktestRunEngine(run_settings=settings, upbit_settings=None, rules_provider=_StaticRulesProvider())  # type: ignore[arg-type]

    summary = engine.run()
    run_dir = Path(summary.run_dir)

    assert (run_dir / "summary.json").exists()
    assert (run_dir / "fills.jsonl").exists()
    assert (run_dir / "equity.csv").exists()
    assert not (run_dir / "events.jsonl").exists()
    assert not (run_dir / "orders.jsonl").exists()
    assert not (run_dir / "trades.csv").exists()
    assert not (run_dir / "per_market.csv").exists()
    assert not (run_dir / "selection_stats.json").exists()
    assert not (run_dir / "debug_mismatch.json").exists()
