from __future__ import annotations

from pathlib import Path

from autobot.data.collect.fixed_collection_contract import (
    load_fixed_collection_contract,
    resolve_fixed_collection_markets,
)
from autobot.data.collect.plan_candles import CandlePlanOptions, generate_candle_topup_plan
from autobot.data.collect.plan_ticks import TicksPlanOptions, generate_ticks_collection_plan
from autobot.data.collect.plan_ws_public import WsPublicPlanOptions, generate_ws_public_collection_plan


def _write_base_yaml(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "universe:",
                "  quote_currency: KRW",
                "source_plane:",
                "  fixed_collection:",
                "    enabled: true",
                "    quote: KRW",
                "    markets:",
                "      - KRW-BTC",
                "      - KRW-ETH",
                "      - KRW-XRP",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_load_fixed_collection_contract_reads_markets(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_base_yaml(config_dir / "base.yaml")

    contract = load_fixed_collection_contract(config_dir=config_dir)

    assert contract.enabled is True
    assert contract.quote == "KRW"
    assert contract.markets == ("KRW-BTC", "KRW-ETH", "KRW-XRP")


def test_resolve_fixed_collection_markets_prefers_contract_when_no_explicit_markets(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_base_yaml(config_dir / "base.yaml")

    markets = resolve_fixed_collection_markets(
        config_dir=config_dir,
        quote="KRW",
        explicit_markets=None,
    )

    assert markets == ("KRW-BTC", "KRW-ETH", "KRW-XRP")


def test_candle_plan_uses_fixed_collection_contract(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    config_dir = tmp_path / "config"
    _write_base_yaml(config_dir / "base.yaml")

    plan = generate_candle_topup_plan(
        CandlePlanOptions(
            parquet_root=parquet_root,
            base_dataset="candles_v1",
            output_path=tmp_path / "candle_plan.json",
            tf_set=("1m",),
            quote="KRW",
            market_mode="top_n_by_recent_value_est",
            top_n=20,
            config_dir=config_dir,
        )
    )

    assert plan["selected_markets"] == ["KRW-BTC", "KRW-ETH", "KRW-XRP"]
    assert plan["market_selection"]["mode"] == "fixed_collection_contract"


def test_ticks_plan_uses_fixed_collection_contract(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    config_dir = tmp_path / "config"
    _write_base_yaml(config_dir / "base.yaml")

    plan = generate_ticks_collection_plan(
        TicksPlanOptions(
            parquet_root=parquet_root,
            base_dataset="candles_v1",
            output_path=tmp_path / "ticks_plan.json",
            quote="KRW",
            market_mode="top_n_by_recent_value_est",
            top_n=20,
            config_dir=config_dir,
        )
    )

    assert plan["selected_markets"] == ["KRW-BTC", "KRW-ETH", "KRW-XRP"]
    assert plan["market_selection"]["mode"] == "fixed_collection_contract"


def test_ws_public_plan_uses_fixed_collection_contract(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    config_dir = tmp_path / "config"
    _write_base_yaml(config_dir / "base.yaml")

    plan = generate_ws_public_collection_plan(
        WsPublicPlanOptions(
            parquet_root=parquet_root,
            base_dataset="candles_v1",
            output_path=tmp_path / "ws_public_plan.json",
            quote="KRW",
            market_mode="top_n_by_recent_value_est",
            top_n=20,
            config_dir=config_dir,
        )
    )

    assert plan["selected_markets"] == ["KRW-BTC", "KRW-ETH", "KRW-XRP"]
    assert plan["market_selection"]["mode"] == "fixed_collection_contract"
