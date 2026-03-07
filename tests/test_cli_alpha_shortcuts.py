from __future__ import annotations

import argparse

from autobot.cli import (
    _normalize_backtest_alpha_args,
    _normalize_paper_alpha_args,
    build_parser,
)


def test_build_parser_supports_paper_alpha_shortcut() -> None:
    parser = build_parser()
    args = parser.parse_args(["paper", "alpha", "--duration-sec", "900", "--model-ref", "champion_v3"])
    assert args.command == "paper"
    assert args.paper_command == "alpha"
    assert args.duration_sec == 900
    assert args.model_ref == "champion_v3"
    assert args.preset == "live_v3"


def test_normalize_paper_alpha_args_uses_live_v3_preset_defaults() -> None:
    args = argparse.Namespace(
        paper_command="alpha",
        preset="live_v3",
        duration_sec=600,
        quote=None,
        top_n=None,
        tf=None,
        model_ref=None,
        model_family=None,
        feature_set=None,
        top_pct=None,
        min_prob=None,
        min_cands_per_ts=None,
        max_positions_total=None,
        cooldown_bars=None,
        exit_mode=None,
        hold_bars=None,
        tp_pct=None,
        sl_pct=None,
        trailing_pct=None,
        execution_price_mode=None,
        execution_timeout_bars=None,
        execution_replace_max=None,
        print_every_sec=None,
        starting_krw=None,
        per_trade_krw=None,
        max_positions=None,
        paper_micro_provider=None,
        paper_micro_warmup_sec=None,
        paper_micro_warmup_min_trade_events_per_market=None,
        paper_feature_provider=None,
    )
    normalized = _normalize_paper_alpha_args(args)
    assert normalized.paper_command == "run"
    assert normalized.strategy == "model_alpha_v1"
    assert normalized.feature_set == "v3"
    assert normalized.paper_feature_provider == "live_v3"
    assert normalized.paper_micro_provider == "live_ws"
    assert normalized.micro_order_policy == "on"
    assert normalized.micro_order_policy_mode == "trade_only"
    assert normalized.micro_order_policy_on_missing == "static_fallback"


def test_normalize_paper_alpha_args_uses_live_v4_preset_defaults() -> None:
    args = argparse.Namespace(
        paper_command="alpha",
        preset="live_v4",
        duration_sec=600,
        quote=None,
        top_n=None,
        tf=None,
        model_ref=None,
        model_family=None,
        feature_set=None,
        top_pct=None,
        min_prob=None,
        min_cands_per_ts=None,
        max_positions_total=None,
        cooldown_bars=None,
        exit_mode=None,
        hold_bars=None,
        tp_pct=None,
        sl_pct=None,
        trailing_pct=None,
        execution_price_mode=None,
        execution_timeout_bars=None,
        execution_replace_max=None,
        print_every_sec=None,
        starting_krw=None,
        per_trade_krw=None,
        max_positions=None,
        paper_micro_provider=None,
        paper_micro_warmup_sec=None,
        paper_micro_warmup_min_trade_events_per_market=None,
        paper_feature_provider=None,
    )
    normalized = _normalize_paper_alpha_args(args)
    assert normalized.paper_command == "run"
    assert normalized.strategy == "model_alpha_v1"
    assert normalized.feature_set == "v4"
    assert normalized.model_ref == "champion_v4"
    assert normalized.model_family == "train_v4_crypto_cs"
    assert normalized.paper_feature_provider == "live_v4"
    assert normalized.paper_micro_provider == "live_ws"


def test_build_parser_supports_backtest_alpha_shortcut() -> None:
    parser = build_parser()
    args = parser.parse_args(["backtest", "alpha", "--start", "2026-03-01", "--end", "2026-03-05"])
    assert args.command == "backtest"
    assert args.backtest_command == "alpha"
    assert args.start == "2026-03-01"
    assert args.end == "2026-03-05"
    assert args.preset == "default"


def test_normalize_backtest_alpha_args_acceptance_disables_micro_policy() -> None:
    args = argparse.Namespace(
        backtest_command="alpha",
        preset="acceptance",
        dataset_name=None,
        parquet_root=None,
        tf=None,
        market=None,
        markets=None,
        quote=None,
        top_n=None,
        universe_mode=None,
        model_ref=None,
        model_family=None,
        feature_set=None,
        top_pct=None,
        min_prob=None,
        min_cands_per_ts=None,
        exit_mode=None,
        hold_bars=None,
        tp_pct=None,
        sl_pct=None,
        trailing_pct=None,
        cooldown_bars=None,
        max_positions_total=None,
        execution_price_mode=None,
        execution_timeout_bars=None,
        execution_replace_max=None,
        start=None,
        end=None,
        from_ts_ms=None,
        to_ts_ms=None,
        days=7,
        dense_grid=False,
        starting_krw=None,
        per_trade_krw=None,
        max_positions=None,
    )
    normalized = _normalize_backtest_alpha_args(args)
    assert normalized.backtest_command == "run"
    assert normalized.strategy == "model_alpha_v1"
    assert normalized.entry == "top_pct"
    assert normalized.feature_set == "v3"
    assert normalized.duration_days == 7
    assert normalized.micro_order_policy == "off"
