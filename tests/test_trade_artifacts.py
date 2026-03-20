import csv
from pathlib import Path

from autobot.common.trade_artifacts import write_trade_artifacts


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_write_trade_artifacts_writes_shared_fill_analytics(tmp_path: Path) -> None:
    run_root = tmp_path / "run"
    fill_records = [
        {
            "ts_ms": 2_000,
            "market": "KRW-BTC",
            "side": "ask",
            "price_mode": "CROSS_1T",
            "slippage_bps": 2.0,
            "notional_quote": 120.0,
            "fee_quote": 1.5,
            "order_id": "order-ask",
            "intent_id": "intent-ask",
        },
        {
            "ts_ms": 1_000,
            "market": "KRW-BTC",
            "side": "bid",
            "price_mode": "JOIN",
            "slippage_bps": 1.0,
            "notional_quote": 100.0,
            "fee_quote": 1.0,
            "order_id": "order-bid",
            "intent_id": "intent-bid",
        },
        {
            "ts_ms": 1_500,
            "market": "KRW-ETH",
            "side": "bid",
            "price_mode": "PASSIVE_MAKER",
            "slippage_bps": 0.5,
            "notional_quote": 80.0,
            "fee_quote": 0.8,
            "order_id": "order-eth",
            "intent_id": "intent-eth",
        },
    ]

    write_trade_artifacts(run_root=run_root, fill_records=fill_records)

    trades_rows = _read_csv(run_root / "trades.csv")
    per_market_rows = _read_csv(run_root / "per_market.csv")
    slippage_rows = _read_csv(run_root / "slippage_by_market.csv")
    price_mode_rows = _read_csv(run_root / "price_mode_by_market.csv")

    assert [row["order_id"] for row in trades_rows] == ["order-bid", "order-eth", "order-ask"]

    btc_row = next(row for row in per_market_rows if row["market"] == "KRW-BTC")
    assert btc_row["fills_total"] == "2"
    assert btc_row["entry_fills"] == "1"
    assert btc_row["exit_fills"] == "1"
    assert float(btc_row["fees_quote"]) == 2.5
    assert float(btc_row["net_flow_quote"]) == 17.5

    eth_row = next(row for row in per_market_rows if row["market"] == "KRW-ETH")
    assert eth_row["fills_total"] == "1"
    assert float(eth_row["net_flow_quote"]) == -80.8

    btc_slippage = next(row for row in slippage_rows if row["market"] == "KRW-BTC")
    assert btc_slippage["fills"] == "2"
    assert float(btc_slippage["mean_bps"]) == 1.5
    assert float(btc_slippage["cross_ratio"]) == 0.5

    btc_price_modes = next(row for row in price_mode_rows if row["market"] == "KRW-BTC")
    assert btc_price_modes["JOIN_count"] == "1"
    assert btc_price_modes["CROSS_1T_count"] == "1"
    assert btc_price_modes["PASSIVE_count"] == "0"
    assert btc_price_modes["OTHER_count"] == "0"

    eth_price_modes = next(row for row in price_mode_rows if row["market"] == "KRW-ETH")
    assert eth_price_modes["PASSIVE_count"] == "1"
