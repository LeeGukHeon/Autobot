from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import polars as pl
import pytest

import autobot.data.derived.market_state_v1 as market_state_module
from autobot.data.collect.ws_public_writer import WsRawRotatingWriter
from autobot.data.derived.market_state_v1 import MarketStateBuildOptions, build_market_state_v1_datasets
from autobot.data.sources.trades.writer import write_raw_trade_partitions


UTC = timezone.utc


def test_build_market_state_v1_outputs_expected_features_and_labels(tmp_path: Path) -> None:
    _write_source_plane_fixture(tmp_path, btc_future_bid=100.25, eth_future_bid=200.15)

    summary = build_market_state_v1_datasets(
        MarketStateBuildOptions(
            start="2026-04-10",
            end="2026-04-10",
            markets=("KRW-BTC", "KRW-ETH"),
            raw_ws_root=tmp_path / "data" / "raw_ws" / "upbit" / "public",
            raw_trade_root=tmp_path / "data" / "raw_trade_v1",
            candles_root=tmp_path / "data" / "parquet" / "candles_api_v1",
            market_state_root=tmp_path / "data" / "derived" / "market_state_v1",
            tradeable_label_root=tmp_path / "data" / "derived" / "tradeable_label_v1",
            net_edge_label_root=tmp_path / "data" / "derived" / "net_edge_label_v1",
            closed_operating_dates_only=False,
        )
    )

    assert summary.built_pairs == 2
    market_state = _load_single_market_parquet(tmp_path / "data" / "derived" / "market_state_v1", "2026-04-10", "KRW-BTC")
    tradeable = _load_single_market_parquet(tmp_path / "data" / "derived" / "tradeable_label_v1", "2026-04-10", "KRW-BTC")
    net_edge = _load_single_market_parquet(tmp_path / "data" / "derived" / "net_edge_label_v1", "2026-04-10", "KRW-BTC")

    first = market_state.row(0, named=True)
    assert first["queue_imbalance_top1"] == pl.Series([2.0 - 1.0]).item() / 3.0
    assert round(float(first["trade_imbalance_5s"]), 6) == round((2.0 - 1.0) / 3.0, 6)
    assert first["ticker_available"] is True
    assert first["book_available"] is True
    assert first["trade_available"] is True
    assert 0.0 <= float(first["source_quality_score"]) <= 1.0

    label_first = tradeable.row(0, named=True)
    assert label_first["label_available_20m"] is True
    assert label_first["spread_quality_pass_20m"] is True
    assert label_first["liquidity_pass_20m"] is True
    assert label_first["tradeable_20m"] == 1

    edge_first = net_edge.row(0, named=True)
    assert edge_first["future_best_bid_20m"] is not None
    assert float(edge_first["net_edge_20m_bps"]) > 3.0


def test_build_market_state_v1_marks_untradeable_when_net_edge_is_too_small(tmp_path: Path) -> None:
    _write_source_plane_fixture(tmp_path, btc_future_bid=100.10, eth_future_bid=200.15)

    build_market_state_v1_datasets(
        MarketStateBuildOptions(
            start="2026-04-10",
            end="2026-04-10",
            markets=("KRW-BTC",),
            raw_ws_root=tmp_path / "data" / "raw_ws" / "upbit" / "public",
            raw_trade_root=tmp_path / "data" / "raw_trade_v1",
            candles_root=tmp_path / "data" / "parquet" / "candles_api_v1",
            market_state_root=tmp_path / "data" / "derived" / "market_state_v1",
            tradeable_label_root=tmp_path / "data" / "derived" / "tradeable_label_v1",
            net_edge_label_root=tmp_path / "data" / "derived" / "net_edge_label_v1",
            closed_operating_dates_only=False,
        )
    )

    tradeable = _load_single_market_parquet(tmp_path / "data" / "derived" / "tradeable_label_v1", "2026-04-10", "KRW-BTC")
    first = tradeable.row(0, named=True)
    assert first["tradeable_20m"] == 0


def test_build_market_state_v1_rerun_replaces_existing_pair_outputs(tmp_path: Path) -> None:
    _write_source_plane_fixture(tmp_path, btc_future_bid=100.25, eth_future_bid=200.15)
    options = MarketStateBuildOptions(
        start="2026-04-10",
        end="2026-04-10",
        markets=("KRW-BTC",),
        raw_ws_root=tmp_path / "data" / "raw_ws" / "upbit" / "public",
        raw_trade_root=tmp_path / "data" / "raw_trade_v1",
        candles_root=tmp_path / "data" / "parquet" / "candles_api_v1",
        market_state_root=tmp_path / "data" / "derived" / "market_state_v1",
        tradeable_label_root=tmp_path / "data" / "derived" / "tradeable_label_v1",
        net_edge_label_root=tmp_path / "data" / "derived" / "net_edge_label_v1",
        closed_operating_dates_only=False,
        skip_existing_complete=False,
    )
    first = build_market_state_v1_datasets(options)
    manifest_first = pl.read_parquet(first.market_state_manifest_file)
    assert manifest_first.height == 1

    _write_source_plane_fixture(tmp_path, btc_future_bid=100.60, eth_future_bid=200.15)
    second = build_market_state_v1_datasets(options)
    manifest_second = pl.read_parquet(second.market_state_manifest_file)
    assert manifest_second.height == 1
    part_dir = tmp_path / "data" / "derived" / "market_state_v1" / "date=2026-04-10" / "market=KRW-BTC"
    assert len(list(part_dir.glob("*.parquet"))) == 1
    net_edge = _load_single_market_parquet(tmp_path / "data" / "derived" / "net_edge_label_v1", "2026-04-10", "KRW-BTC")
    assert float(net_edge.row(0, named=True)["net_edge_20m_bps"]) > 20.0


def test_build_market_state_v1_skips_existing_complete_pairs_by_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_source_plane_fixture(tmp_path, btc_future_bid=100.25, eth_future_bid=200.15)
    options = MarketStateBuildOptions(
        start="2026-04-10",
        end="2026-04-10",
        markets=("KRW-BTC",),
        raw_ws_root=tmp_path / "data" / "raw_ws" / "upbit" / "public",
        raw_trade_root=tmp_path / "data" / "raw_trade_v1",
        candles_root=tmp_path / "data" / "parquet" / "candles_api_v1",
        market_state_root=tmp_path / "data" / "derived" / "market_state_v1",
        tradeable_label_root=tmp_path / "data" / "derived" / "tradeable_label_v1",
        net_edge_label_root=tmp_path / "data" / "derived" / "net_edge_label_v1",
        closed_operating_dates_only=False,
    )
    first = build_market_state_v1_datasets(options)
    assert first.built_pairs == 1
    call_counts = {"ticker": 0, "orderbook": 0, "trade": 0}
    original_ticker = market_state_module._load_date_ticker_frames
    original_orderbook = market_state_module._load_date_orderbook_frames
    original_trade = market_state_module._load_date_trade_frames

    def _wrap_ticker(*args, **kwargs):
        call_counts["ticker"] += 1
        return original_ticker(*args, **kwargs)

    def _wrap_orderbook(*args, **kwargs):
        call_counts["orderbook"] += 1
        return original_orderbook(*args, **kwargs)

    def _wrap_trade(*args, **kwargs):
        call_counts["trade"] += 1
        return original_trade(*args, **kwargs)

    monkeypatch.setattr(market_state_module, "_load_date_ticker_frames", _wrap_ticker)
    monkeypatch.setattr(market_state_module, "_load_date_orderbook_frames", _wrap_orderbook)
    monkeypatch.setattr(market_state_module, "_load_date_trade_frames", _wrap_trade)

    second = build_market_state_v1_datasets(options)

    assert second.built_pairs == 0
    assert second.reused_pairs == 1
    assert second.skipped_pairs == 0
    assert call_counts == {"ticker": 0, "orderbook": 0, "trade": 0}


def test_build_market_state_v1_scans_date_raw_once_per_helper(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_source_plane_fixture(tmp_path, btc_future_bid=100.25, eth_future_bid=200.15)

    call_counts = {"ticker": 0, "orderbook": 0, "trade": 0}
    original_ticker = market_state_module._load_date_ticker_frames
    original_orderbook = market_state_module._load_date_orderbook_frames
    original_trade = market_state_module._load_date_trade_frames

    def _wrap_ticker(*args, **kwargs):
        call_counts["ticker"] += 1
        return original_ticker(*args, **kwargs)

    def _wrap_orderbook(*args, **kwargs):
        call_counts["orderbook"] += 1
        return original_orderbook(*args, **kwargs)

    def _wrap_trade(*args, **kwargs):
        call_counts["trade"] += 1
        return original_trade(*args, **kwargs)

    monkeypatch.setattr(market_state_module, "_load_date_ticker_frames", _wrap_ticker)
    monkeypatch.setattr(market_state_module, "_load_date_orderbook_frames", _wrap_orderbook)
    monkeypatch.setattr(market_state_module, "_load_date_trade_frames", _wrap_trade)

    build_market_state_v1_datasets(
        MarketStateBuildOptions(
            start="2026-04-10",
            end="2026-04-10",
            markets=("KRW-BTC", "KRW-ETH"),
            raw_ws_root=tmp_path / "data" / "raw_ws" / "upbit" / "public",
            raw_trade_root=tmp_path / "data" / "raw_trade_v1",
            candles_root=tmp_path / "data" / "parquet" / "candles_api_v1",
            market_state_root=tmp_path / "data" / "derived" / "market_state_v1",
            tradeable_label_root=tmp_path / "data" / "derived" / "tradeable_label_v1",
            net_edge_label_root=tmp_path / "data" / "derived" / "net_edge_label_v1",
            closed_operating_dates_only=False,
        )
    )

    assert call_counts == {"ticker": 1, "orderbook": 1, "trade": 1}


def test_build_market_state_v1_uses_ticker_proxy_when_raw_ticker_is_missing(tmp_path: Path) -> None:
    _write_source_plane_fixture(tmp_path, btc_future_bid=100.25, eth_future_bid=200.15)
    _clear_dir(tmp_path / "data" / "raw_ws" / "upbit" / "public" / "ticker")

    build_market_state_v1_datasets(
        MarketStateBuildOptions(
            start="2026-04-10",
            end="2026-04-10",
            markets=("KRW-BTC",),
            raw_ws_root=tmp_path / "data" / "raw_ws" / "upbit" / "public",
            raw_trade_root=tmp_path / "data" / "raw_trade_v1",
            candles_root=tmp_path / "data" / "parquet" / "candles_api_v1",
            market_state_root=tmp_path / "data" / "derived" / "market_state_v1",
            tradeable_label_root=tmp_path / "data" / "derived" / "tradeable_label_v1",
            net_edge_label_root=tmp_path / "data" / "derived" / "net_edge_label_v1",
            closed_operating_dates_only=False,
        )
    )

    market_state = _load_single_market_parquet(tmp_path / "data" / "derived" / "market_state_v1", "2026-04-10", "KRW-BTC")
    tradeable = _load_single_market_parquet(tmp_path / "data" / "derived" / "tradeable_label_v1", "2026-04-10", "KRW-BTC")
    first = market_state.row(0, named=True)
    label_first = tradeable.row(0, named=True)
    assert first["ticker_source_kind"] == "candle_proxy"
    assert first["ticker_proxy_available"] is True
    assert first["ticker_available"] is True
    assert first["acc_trade_price_24h"] is not None
    assert label_first["label_available_20m"] is True


def _write_source_plane_fixture(root: Path, *, btc_future_bid: float, eth_future_bid: float) -> None:
    raw_ws_root = root / "data" / "raw_ws" / "upbit" / "public"
    raw_trade_root = root / "data" / "raw_trade_v1"
    candles_root = root / "data" / "parquet" / "candles_api_v1"
    _clear_dir(raw_ws_root)
    _clear_dir(raw_trade_root)
    _clear_dir(candles_root)

    _write_candles(candles_root, market="KRW-BTC", base_price=99.0)
    _write_candles(candles_root, market="KRW-ETH", base_price=199.0)

    writer = WsRawRotatingWriter(raw_root=raw_ws_root, run_id="ws-fixture", rotate_sec=3600)
    start_dt = datetime(2026, 4, 10, 0, 0, 5, tzinfo=UTC)
    total_steps = 520
    for market, entry_ask, future_bid in (
        ("KRW-BTC", 100.05, btc_future_bid),
        ("KRW-ETH", 200.05, eth_future_bid),
    ):
        for idx in range(0, total_steps):
            ts_dt = start_dt + timedelta(seconds=idx * 5)
            ts_ms = int(ts_dt.timestamp() * 1000)
            base_bid = entry_ask - 0.10
            ramp_steps = 240
            step = 0.0 if idx < 4 else (future_bid - base_bid) / float(max(ramp_steps, 1))
            progressed = min(max(idx - 4, 0), ramp_steps)
            best_bid = round(base_bid + progressed * step, 6)
            best_ask = round(best_bid + 0.10, 6)
            writer.write(
                channel="ticker",
                row={
                    "channel": "ticker",
                    "market": market,
                    "ts_ms": ts_ms,
                    "trade_price": round((best_bid + best_ask) / 2.0, 6),
                    "acc_trade_price_24h": 1_000_000_000.0 + (idx * 1000.0),
                    "market_state": "ACTIVE",
                    "market_warning": "NONE",
                    "source": "ws",
                    "collected_at_ms": ts_ms + 1,
                },
                event_ts_ms=ts_ms,
            )
            writer.write(
                channel="orderbook",
                row={
                    "channel": "orderbook",
                    "market": market,
                    "ts_ms": ts_ms,
                    "total_ask_size": 10.0,
                    "total_bid_size": 10.0,
                    "topk": 5,
                    "level": 0,
                    "source": "ws",
                    "collected_at_ms": ts_ms + 2,
                    "ask1_price": best_ask,
                    "ask1_size": 1.0,
                    "bid1_price": best_bid,
                    "bid1_size": 2.0,
                    "ask2_price": best_ask + 0.1,
                    "ask2_size": 1.5,
                    "bid2_price": best_bid - 0.1,
                    "bid2_size": 1.0,
                    "ask3_price": best_ask + 0.2,
                    "ask3_size": 1.0,
                    "bid3_price": best_bid - 0.2,
                    "bid3_size": 1.0,
                    "ask4_price": best_ask + 0.3,
                    "ask4_size": 1.0,
                    "bid4_price": best_bid - 0.3,
                    "bid4_size": 1.0,
                    "ask5_price": best_ask + 0.4,
                    "ask5_size": 1.0,
                    "bid5_price": best_bid - 0.4,
                    "bid5_size": 1.0,
                },
                event_ts_ms=ts_ms,
            )
    writer.close()

    trade_rows = []
    for market, entry_ask in (("KRW-BTC", 100.05), ("KRW-ETH", 200.05)):
        for idx in range(0, total_steps):
            ts_dt = start_dt + timedelta(seconds=idx * 5)
            ts_ms = int(ts_dt.timestamp() * 1000)
            trade_rows.extend(
                [
                    {
                        "market": market,
                        "event_ts_ms": ts_ms,
                        "price": entry_ask - 0.5,
                        "volume": 2.0,
                        "ask_bid": "BID",
                        "side": "buy",
                        "sequential_id": idx * 2 + 1,
                        "source": "ws",
                        "source_event_channel": "trade",
                        "recv_ts_ms": ts_ms + 1,
                        "days_ago": None,
                        "collected_at_ms": ts_ms + 2,
                    },
                    {
                        "market": market,
                        "event_ts_ms": ts_ms + 1_000,
                        "price": entry_ask - 0.4,
                        "volume": 1.0,
                        "ask_bid": "ASK",
                        "side": "sell",
                        "sequential_id": idx * 2 + 2,
                        "source": "ws",
                        "source_event_channel": "trade",
                        "recv_ts_ms": ts_ms + 1_001,
                        "days_ago": None,
                        "collected_at_ms": ts_ms + 1_002,
                    },
                ]
            )
    write_raw_trade_partitions(out_root=raw_trade_root, trades=trade_rows, run_id="raw-trade-fixture")


def _write_candles(root: Path, *, market: str, base_price: float) -> None:
    start_dt = datetime(2026, 4, 9, 0, 1, tzinfo=UTC)
    one_m_rows = []
    last_close = base_price
    for idx in range(0, 1_520):
        ts_dt = start_dt + timedelta(minutes=idx)
        ts_ms = int(ts_dt.timestamp() * 1000)
        close = last_close + 0.01
        one_m_rows.append(
            {
                "ts_ms": ts_ms,
                "open": last_close,
                "high": close + 0.02,
                "low": last_close - 0.02,
                "close": close,
                "volume_base": 10.0 + idx,
            }
        )
        last_close = close
    _write_tf_frame(root, "1m", market, pl.DataFrame(one_m_rows))
    _write_tf_frame(root, "5m", market, _aggregate_tf(one_m_rows, 5))
    _write_tf_frame(root, "15m", market, _aggregate_tf(one_m_rows, 15))
    _write_tf_frame(root, "60m", market, _aggregate_tf(one_m_rows, 60))


def _aggregate_tf(rows: list[dict[str, float]], minutes: int) -> pl.DataFrame:
    grouped: list[dict[str, float]] = []
    for start in range(0, len(rows), minutes):
        chunk = rows[start : start + minutes]
        if not chunk:
            continue
        grouped.append(
            {
                "ts_ms": chunk[-1]["ts_ms"],
                "open": chunk[0]["open"],
                "high": max(item["high"] for item in chunk),
                "low": min(item["low"] for item in chunk),
                "close": chunk[-1]["close"],
                "volume_base": sum(item["volume_base"] for item in chunk),
            }
        )
    return pl.DataFrame(grouped)


def _write_tf_frame(root: Path, tf: str, market: str, frame: pl.DataFrame) -> None:
    target = root / f"tf={tf}" / f"market={market}"
    target.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(target / "part.parquet")


def _load_single_market_parquet(root: Path, date_value: str, market: str) -> pl.DataFrame:
    files = sorted((root / f"date={date_value}" / f"market={market}").glob("*.parquet"))
    assert files
    return pl.read_parquet(files[0]).sort("bucket_end_ts_ms")


def _clear_dir(path: Path) -> None:
    if path.exists():
        import shutil

        shutil.rmtree(path)
