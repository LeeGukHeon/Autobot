"""Command line interface for Upbit AutoBot."""

from __future__ import annotations

import asyncio
import argparse
from dataclasses import asdict
import json
from pathlib import Path
import time
from typing import Any, Callable

import yaml

from . import __version__
from .backtest import BacktestRunSettings, run_backtest_sync
from .data import (
    DuckDBSettings,
    IngestOptions,
    build_candle_inventory,
    default_inventory_window,
    ingest_dataset,
    parse_utc_ts_ms,
    sniff_csv_files,
    validate_dataset,
)
from .data.collect import (
    CandleCollectOptions,
    CandlePlanOptions,
    TicksCollectOptions,
    TicksPlanOptions,
    WsPublicCollectOptions,
    WsPublicPlanOptions,
    collect_candles_from_plan,
    collect_ticks_from_plan,
    collect_ticks_stats,
    collect_ws_public_from_plan,
    collect_ws_public_stats,
    generate_candle_topup_plan,
    generate_ticks_collection_plan,
    generate_ws_public_collection_plan,
    validate_candles_api_dataset,
    validate_ticks_raw_dataset,
    validate_ws_public_raw_dataset,
)
from .data.micro import (
    MicroAggregateOptions,
    aggregate_micro_v1,
    micro_stats_v1,
    validate_micro_dataset_v1,
)
from .features import (
    FeatureBuildOptions,
    FeatureValidateOptions,
    build_features_dataset,
    features_stats,
    load_features_config,
    sample_features,
    validate_features_dataset,
)
from .live import (
    LiveDaemonSettings,
    LiveStateStore,
    apply_cancel_actions,
    reconcile_exchange_snapshot,
    run_live_sync_daemon,
    run_live_sync_daemon_with_executor_events,
    run_live_sync_daemon_with_private_ws,
)
from .paper import PaperRunSettings, run_live_paper_sync
from .models import (
    TrainRunOptions,
    evaluate_registered_model,
    list_registered_models,
    load_train_defaults,
    show_registered_model,
    train_and_register,
)
from .strategy import TopTradeValueScanner
from .upbit import (
    ConfigError,
    UpbitError,
    UpbitHttpClient,
    UpbitPrivateClient,
    UpbitPublicClient,
    load_upbit_settings,
    require_upbit_credentials,
)
from .upbit.ws import UpbitWebSocketPublicClient
from .upbit.ws import UpbitWebSocketPrivateClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="autobot",
        description="Upbit AutoBot CLI",
    )
    parser.add_argument(
        "--config-dir",
        default="config",
        help="Path to config directory.",
    )
    parser.add_argument(
        "--mode",
        choices=("backtest", "paper", "live"),
        default="backtest",
        help="Execution mode.",
    )

    subparsers = parser.add_subparsers(dest="command")
    data_parser = subparsers.add_parser("data", help="Data operations.")
    data_subparsers = data_parser.add_subparsers(dest="data_command", required=True)

    ingest_parser = data_subparsers.add_parser("ingest", help="Ingest raw CSV into parquet dataset.")
    ingest_parser.add_argument("--raw-dir")
    ingest_parser.add_argument("--out-dir")
    ingest_parser.add_argument("--dataset-name")
    ingest_parser.add_argument("--pattern")
    ingest_parser.add_argument("--workers", type=int)
    ingest_parser.add_argument("--mode", choices=("overwrite", "skip_unchanged"))
    ingest_parser.add_argument("--compression", choices=("zstd", "snappy", "none"))
    ingest_parser.add_argument("--quote", help="Quote filter, ex: KRW")
    ingest_parser.add_argument("--tf", help="Comma separated timeframe filter, ex: 1m,5m")
    ingest_parser.add_argument("--symbol", help="Comma separated symbol filter, ex: BTC,ETH")
    ingest_parser.add_argument("--limit-files", type=int)
    ingest_parser.add_argument("--dry-run", action="store_true")
    ingest_parser.add_argument("--engine", choices=("duckdb", "polars"))
    ingest_parser.add_argument("--duckdb-temp-directory")
    ingest_parser.add_argument("--duckdb-memory-limit")
    ingest_parser.add_argument("--duckdb-threads", type=int)

    sniff_parser = data_subparsers.add_parser("sniff", help="Sniff raw CSV headers and mapping.")
    sniff_parser.add_argument("--raw-dir")
    sniff_parser.add_argument("--pattern")
    sniff_parser.add_argument("--sample-files", type=int, default=10)
    sniff_parser.add_argument("--sample-rows", type=int, default=5)

    validate_parser = data_subparsers.add_parser("validate", help="Validate parquet dataset.")
    validate_parser.add_argument("--parquet-dir")
    validate_parser.add_argument("--tf", help="Comma separated timeframe filter")
    validate_parser.add_argument("--market", help="Comma separated market filter")

    inventory_parser = data_subparsers.add_parser("inventory", help="Show dataset coverage inventory.")
    inventory_parser.add_argument("--dataset", help="Dataset name, ex: candles_v1")
    inventory_parser.add_argument("--parquet-root", help="Parquet root directory, ex: data/parquet")
    inventory_parser.add_argument("--tf", help="Comma separated timeframe filter, ex: 1m,5m")
    inventory_parser.add_argument("--quote", help="Quote filter, ex: KRW")
    inventory_parser.add_argument("--start", help="Window start date/time (YYYY-MM-DD or ISO UTC)")
    inventory_parser.add_argument("--end", help="Window end date/time (YYYY-MM-DD or ISO UTC)")
    inventory_parser.add_argument("--lookback-months", type=int, default=24)
    inventory_parser.add_argument("--out", help="Optional output JSON path")

    collect_parser = subparsers.add_parser("collect", help="Data collection operations.")
    collect_subparsers = collect_parser.add_subparsers(dest="collect_command", required=True)

    collect_plan_parser = collect_subparsers.add_parser("plan-candles", help="Generate candle top-up plan.")
    collect_plan_parser.add_argument("--base-dataset", help="Base dataset name, ex: candles_v1")
    collect_plan_parser.add_argument("--parquet-root", help="Parquet root directory, ex: data/parquet")
    collect_plan_parser.add_argument(
        "--out",
        default="data/collect/_meta/candle_topup_plan.json",
        help="Plan output path",
    )
    collect_plan_parser.add_argument("--lookback-months", type=int, default=24)
    collect_plan_parser.add_argument("--tf", help="Comma separated timeframe filter, ex: 1m,5m,15m,60m,240m")
    collect_plan_parser.add_argument("--quote", default="KRW")
    collect_plan_parser.add_argument(
        "--market-mode",
        default="top_n_by_recent_value_est",
        choices=("fixed_list", "top_n_by_recent_value_est", "one_m_existing_only"),
    )
    collect_plan_parser.add_argument("--top-n", type=int, default=50)
    collect_plan_parser.add_argument("--markets", help="Comma separated fixed market list, ex: KRW-BTC,KRW-ETH")
    collect_plan_parser.add_argument("--max-backfill-days-1m", type=int, default=90)
    collect_plan_parser.add_argument("--end", help="Window end date (YYYY-MM-DD)")

    collect_candles_parser = collect_subparsers.add_parser("candles", help="Execute candle collection plan.")
    collect_candles_parser.add_argument(
        "--plan",
        default="data/collect/_meta/candle_topup_plan.json",
        help="Path to candle top-up plan JSON",
    )
    collect_candles_parser.add_argument("--out-dataset", default="candles_api_v1")
    collect_candles_parser.add_argument("--parquet-root", help="Parquet root directory, ex: data/parquet")
    collect_candles_parser.add_argument("--workers", type=int, default=1)
    collect_candles_parser.add_argument("--dry-run", default="true", help="true|false")
    collect_candles_parser.add_argument("--max-requests", type=int)
    collect_candles_parser.add_argument("--stop-on-first-fail", default="false", help="true|false")
    collect_candles_parser.add_argument("--rate-limit-strict", default="true", help="true|false")

    collect_plan_ticks_parser = collect_subparsers.add_parser("plan-ticks", help="Generate REST ticks collection plan.")
    collect_plan_ticks_parser.add_argument("--base-dataset", help="Base dataset name, ex: candles_v1")
    collect_plan_ticks_parser.add_argument("--parquet-root", help="Parquet root directory, ex: data/parquet")
    collect_plan_ticks_parser.add_argument(
        "--out",
        default="data/raw_ticks/upbit/_meta/ticks_plan.json",
        help="Plan output path",
    )
    collect_plan_ticks_parser.add_argument("--quote", default="KRW")
    collect_plan_ticks_parser.add_argument(
        "--market-mode",
        default="top_n_by_recent_value_est",
        choices=("fixed_list", "top_n_by_recent_value_est", "one_m_existing_only"),
    )
    collect_plan_ticks_parser.add_argument("--top-n", type=int, default=20)
    collect_plan_ticks_parser.add_argument("--markets", help="Comma separated fixed market list, ex: KRW-BTC,KRW-ETH")
    collect_plan_ticks_parser.add_argument("--days-ago", default="1,2,3,4,5,6,7", help="Comma separated 1..7")

    collect_plan_ws_parser = collect_subparsers.add_parser(
        "plan-ws-public",
        help="Generate public websocket collection plan (trade/orderbook).",
    )
    collect_plan_ws_parser.add_argument("--base-dataset", help="Base dataset name, ex: candles_v1")
    collect_plan_ws_parser.add_argument("--parquet-root", help="Parquet root directory, ex: data/parquet")
    collect_plan_ws_parser.add_argument(
        "--out",
        default="data/raw_ws/upbit/_meta/ws_public_plan.json",
        help="Plan output path",
    )
    collect_plan_ws_parser.add_argument("--quote", default="KRW")
    collect_plan_ws_parser.add_argument(
        "--market-mode",
        default="top_n_by_recent_value_est",
        choices=("fixed_list", "top_n_by_recent_value_est", "one_m_existing_only"),
    )
    collect_plan_ws_parser.add_argument("--top-n", type=int, default=20)
    collect_plan_ws_parser.add_argument("--markets", help="Comma separated fixed market list, ex: KRW-BTC,KRW-ETH")
    collect_plan_ws_parser.add_argument("--channels", default="trade,orderbook", help="Comma separated channels")
    collect_plan_ws_parser.add_argument(
        "--format",
        default="DEFAULT",
        choices=("DEFAULT", "SIMPLE", "JSON_LIST", "SIMPLE_LIST"),
    )
    collect_plan_ws_parser.add_argument("--orderbook-topk", type=int, default=5)
    collect_plan_ws_parser.add_argument("--orderbook-level", default="0")
    collect_plan_ws_parser.add_argument("--orderbook-min-write-interval-ms", type=int, default=200)

    collect_ticks_parser = collect_subparsers.add_parser("ticks", help="Collect raw REST ticks.")
    collect_ticks_parser.add_argument("--plan", default="data/raw_ticks/upbit/_meta/ticks_plan.json")
    collect_ticks_parser.add_argument("--base-dataset", help="Base dataset name, ex: candles_v1")
    collect_ticks_parser.add_argument("--parquet-root", help="Parquet root directory, ex: data/parquet")
    collect_ticks_parser.add_argument("--quote", default="KRW")
    collect_ticks_parser.add_argument(
        "--market-mode",
        default="top_n_by_recent_value_est",
        choices=("fixed_list", "top_n_by_recent_value_est", "one_m_existing_only"),
    )
    collect_ticks_parser.add_argument("--top-n", type=int, default=20)
    collect_ticks_parser.add_argument("--markets", help="Comma separated fixed market list, ex: KRW-BTC,KRW-ETH")
    collect_ticks_parser.add_argument("--days-ago", help="Comma separated 1..7")
    collect_ticks_parser.add_argument("--mode", default="backfill", choices=("backfill", "daily"))
    collect_ticks_parser.add_argument("--raw-root", default="data/raw_ticks/upbit/trades")
    collect_ticks_parser.add_argument("--meta-dir", default="data/raw_ticks/upbit/_meta")
    collect_ticks_parser.add_argument("--retention-days", type=int, default=30)
    collect_ticks_parser.add_argument("--workers", type=int, default=1)
    collect_ticks_parser.add_argument("--rate-limit-strict", default="true", help="true|false")
    collect_ticks_parser.add_argument("--max-pages-per-target", type=int, default=500)
    collect_ticks_parser.add_argument("--max-requests", type=int)
    collect_ticks_parser.add_argument("--dry-run", default="false", help="true|false")
    collect_ticks_subparsers = collect_ticks_parser.add_subparsers(dest="collect_ticks_command")

    collect_ticks_validate_parser = collect_ticks_subparsers.add_parser("validate", help="Validate collected ticks.")
    collect_ticks_validate_parser.add_argument("--date", help="Filter date partition YYYY-MM-DD")
    collect_ticks_validate_parser.add_argument("--raw-root", default="data/raw_ticks/upbit/trades")
    collect_ticks_validate_parser.add_argument("--meta-dir", default="data/raw_ticks/upbit/_meta")
    collect_ticks_validate_parser.add_argument("--dup-ratio-threshold", type=float, default=0.05)

    collect_ticks_stats_parser = collect_ticks_subparsers.add_parser("stats", help="Show collected ticks stats.")
    collect_ticks_stats_parser.add_argument("--date", help="Filter date partition YYYY-MM-DD")
    collect_ticks_stats_parser.add_argument("--raw-root", default="data/raw_ticks/upbit/trades")
    collect_ticks_stats_parser.add_argument("--meta-dir", default="data/raw_ticks/upbit/_meta")

    collect_ws_public_parser = collect_subparsers.add_parser(
        "ws-public",
        help="Collect raw Upbit public websocket trade/orderbook.",
    )
    collect_ws_public_subparsers = collect_ws_public_parser.add_subparsers(
        dest="collect_ws_public_command",
        required=True,
    )

    collect_ws_public_run_parser = collect_ws_public_subparsers.add_parser("run", help="Run public websocket collector.")
    collect_ws_public_run_parser.add_argument("--plan", default="data/raw_ws/upbit/_meta/ws_public_plan.json")
    collect_ws_public_run_parser.add_argument("--raw-root", default="data/raw_ws/upbit/quotation")
    collect_ws_public_run_parser.add_argument("--meta-dir", default="data/raw_ws/upbit/_meta")
    collect_ws_public_run_parser.add_argument("--duration-sec", type=int, default=120)
    collect_ws_public_run_parser.add_argument("--rotate-sec", type=int, default=300)
    collect_ws_public_run_parser.add_argument("--max-bytes", type=int, default=67_108_864)
    collect_ws_public_run_parser.add_argument("--retention-days", type=int, default=7)
    collect_ws_public_run_parser.add_argument("--rate-limit-strict", default="true", help="true|false")
    collect_ws_public_run_parser.add_argument("--reconnect-max-per-min", type=int, default=3)
    collect_ws_public_run_parser.add_argument("--orderbook-spread-bps-threshold", type=float, default=0.5)
    collect_ws_public_run_parser.add_argument("--orderbook-top1-size-change-threshold", type=float, default=0.2)

    collect_ws_public_validate_parser = collect_ws_public_subparsers.add_parser(
        "validate",
        help="Validate collected public websocket dataset.",
    )
    collect_ws_public_validate_parser.add_argument("--date", help="Filter date partition YYYY-MM-DD")
    collect_ws_public_validate_parser.add_argument("--raw-root", default="data/raw_ws/upbit/quotation")
    collect_ws_public_validate_parser.add_argument("--meta-dir", default="data/raw_ws/upbit/_meta")

    collect_ws_public_stats_parser = collect_ws_public_subparsers.add_parser(
        "stats",
        help="Show collected public websocket stats.",
    )
    collect_ws_public_stats_parser.add_argument("--date", help="Filter date partition YYYY-MM-DD")
    collect_ws_public_stats_parser.add_argument("--raw-root", default="data/raw_ws/upbit/quotation")
    collect_ws_public_stats_parser.add_argument("--meta-dir", default="data/raw_ws/upbit/_meta")

    micro_parser = subparsers.add_parser("micro", help="Micro aggregation operations.")
    micro_subparsers = micro_parser.add_subparsers(dest="micro_command", required=True)

    micro_aggregate_parser = micro_subparsers.add_parser(
        "aggregate",
        help="Aggregate raw ticks/ws into micro_v1 parquet (1m/5m).",
    )
    micro_aggregate_parser.add_argument("--tf", default="1m,5m", help="Comma separated timeframe set: 1m,5m")
    micro_aggregate_parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    micro_aggregate_parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    micro_aggregate_parser.add_argument("--quote", default="KRW")
    micro_aggregate_parser.add_argument("--top-n", type=int, default=20)
    micro_aggregate_parser.add_argument("--markets", help="Comma separated fixed markets, ex: KRW-BTC,KRW-ETH")
    micro_aggregate_parser.add_argument("--raw-ticks-root", default="data/raw_ticks/upbit/trades")
    micro_aggregate_parser.add_argument("--raw-ws-root", default="data/raw_ws/upbit/quotation")
    micro_aggregate_parser.add_argument("--out-root", default="data/parquet/micro_v1")
    micro_aggregate_parser.add_argument(
        "--base-candles",
        default="candles_v1",
        help="Base candle dataset name under data/parquet or absolute path.",
    )
    micro_aggregate_parser.add_argument("--mode", default="append", choices=("append", "overwrite"))
    micro_aggregate_parser.add_argument("--chunk-rows", type=int, default=200000)
    micro_aggregate_parser.add_argument("--topk", type=int, default=5)
    micro_aggregate_parser.add_argument("--alignment-mode", default="auto", choices=("auto", "start", "end"))
    micro_aggregate_parser.add_argument("--sample-market", default="KRW-BTC")

    micro_validate_parser = micro_subparsers.add_parser(
        "validate",
        help="Validate micro_v1 parquet dataset.",
    )
    micro_validate_parser.add_argument("--tf", default="1m,5m", help="Comma separated timeframe set: 1m,5m")
    micro_validate_parser.add_argument("--out-root", default="data/parquet/micro_v1")
    micro_validate_parser.add_argument(
        "--base-candles",
        default="candles_v1",
        help="Base candle dataset name under data/parquet or absolute path.",
    )
    micro_validate_parser.add_argument("--join-match-warn", type=float, default=0.98)
    micro_validate_parser.add_argument("--join-match-fail", type=float, default=0.90)
    micro_validate_parser.add_argument("--micro-available-warn", type=float, default=0.10)
    micro_validate_parser.add_argument("--volume-fail-ratio", type=float, default=0.001)
    micro_validate_parser.add_argument("--price-fail-ratio", type=float, default=0.001)

    micro_stats_parser = micro_subparsers.add_parser(
        "stats",
        help="Show micro_v1 dataset summary.",
    )
    micro_stats_parser.add_argument("--tf", default="1m,5m", help="Comma separated timeframe set: 1m,5m")
    micro_stats_parser.add_argument("--out-root", default="data/parquet/micro_v1")

    features_parser = subparsers.add_parser("features", help="Feature store operations.")
    features_subparsers = features_parser.add_subparsers(dest="features_command", required=True)

    features_build_parser = features_subparsers.add_parser("build", help="Build features_v1 dataset.")
    features_build_parser.add_argument("--tf", required=True, help="Timeframe, ex: 5m")
    features_build_parser.add_argument("--quote", help="Quote filter, ex: KRW")
    features_build_parser.add_argument("--top-n", type=int, help="Universe size")
    features_build_parser.add_argument("--start", help="Start date YYYY-MM-DD")
    features_build_parser.add_argument("--end", help="End date YYYY-MM-DD")
    features_build_parser.add_argument("--feature-set", default="v1", choices=("v1",))
    features_build_parser.add_argument("--label-set", default="v1", choices=("v1",))
    features_build_parser.add_argument("--workers", type=int, default=1)
    features_build_parser.add_argument(
        "--fail-on-warn",
        default="false",
        help="Return non-zero when warnings exist (true|false).",
    )

    features_validate_parser = features_subparsers.add_parser("validate", help="Validate built feature dataset.")
    features_validate_parser.add_argument("--tf", required=True, help="Timeframe, ex: 5m")
    features_validate_parser.add_argument("--quote", help="Quote filter, ex: KRW")
    features_validate_parser.add_argument("--top-n", type=int, help="Universe size")

    features_sample_parser = features_subparsers.add_parser("sample", help="Print feature rows for one market.")
    features_sample_parser.add_argument("--tf", required=True, help="Timeframe, ex: 5m")
    features_sample_parser.add_argument("--market", required=True, help="Market, ex: KRW-BTC")
    features_sample_parser.add_argument("--rows", type=int, default=10)

    features_stats_parser = features_subparsers.add_parser("stats", help="Show feature dataset summary.")
    features_stats_parser.add_argument("--tf", required=True, help="Timeframe, ex: 5m")
    features_stats_parser.add_argument("--quote", help="Quote filter, ex: KRW")
    features_stats_parser.add_argument("--top-n", type=int, help="Universe size")

    model_parser = subparsers.add_parser("model", help="Model training and registry operations.")
    model_subparsers = model_parser.add_subparsers(dest="model_command", required=True)

    model_train_parser = model_subparsers.add_parser("train", help="Train baseline+booster and register champion.")
    model_train_parser.add_argument("--tf", help="Timeframe, ex: 5m")
    model_train_parser.add_argument("--quote", help="Quote filter, ex: KRW")
    model_train_parser.add_argument("--top-n", type=int, help="Universe size")
    model_train_parser.add_argument("--start", help="Start date YYYY-MM-DD")
    model_train_parser.add_argument("--end", help="End date YYYY-MM-DD")
    model_train_parser.add_argument("--feature-set", default="v1", choices=("v1",))
    model_train_parser.add_argument("--label-set", default="v1", choices=("v1",))
    model_train_parser.add_argument("--task", default="cls", choices=("cls",))
    model_train_parser.add_argument("--model-family", help="Registry family, ex: train_v1")
    model_train_parser.add_argument("--run-baseline", default="true", help="Enable baseline track (true|false).")
    model_train_parser.add_argument("--run-booster", default="true", help="Enable booster track (true|false).")
    model_train_parser.add_argument("--booster-sweep-trials", type=int)
    model_train_parser.add_argument("--seed", type=int)
    model_train_parser.add_argument("--nthread", type=int)

    model_eval_parser = model_subparsers.add_parser("eval", help="Evaluate registered model on split.")
    model_eval_parser.add_argument("--model-ref", default="latest", help="latest|champion|run_id|run_dir")
    model_eval_parser.add_argument("--model-family", help="Registry family, ex: train_v1")
    model_eval_parser.add_argument("--split", default="test", choices=("train", "valid", "test"))
    model_eval_parser.add_argument("--report-csv", help="Optional CSV output path")

    model_list_parser = model_subparsers.add_parser("list", help="List registered model runs.")
    model_list_parser.add_argument("--model-family", help="Registry family, ex: train_v1")
    model_list_parser.add_argument("--limit", type=int, default=20)

    model_show_parser = model_subparsers.add_parser("show", help="Show model run details.")
    model_show_parser.add_argument("--model-ref", default="latest", help="latest|champion|run_id|run_dir")
    model_show_parser.add_argument("--model-family", help="Registry family, ex: train_v1")

    upbit_parser = subparsers.add_parser("upbit", help="Upbit REST smoke tests.")
    upbit_subparsers = upbit_parser.add_subparsers(dest="upbit_scope", required=True)

    upbit_public_parser = upbit_subparsers.add_parser("public", help="Public quotation API.")
    upbit_public_subparsers = upbit_public_parser.add_subparsers(dest="upbit_public_command", required=True)

    upbit_public_markets_parser = upbit_public_subparsers.add_parser("markets", help="List all markets.")
    upbit_public_markets_parser.add_argument("--is-details", action="store_true")

    upbit_public_ticker_parser = upbit_public_subparsers.add_parser("ticker", help="Fetch ticker by markets.")
    upbit_public_ticker_parser.add_argument("--markets", required=True, help="Comma separated markets.")

    upbit_public_candles_parser = upbit_public_subparsers.add_parser("candles", help="Fetch minute candles.")
    upbit_public_candles_parser.add_argument("--market", required=True, help="Market, ex: KRW-BTC")
    upbit_public_candles_parser.add_argument("--tf-min", type=int, default=1, help="Candle timeframe in minutes")
    upbit_public_candles_parser.add_argument("--count", type=int, default=10)
    upbit_public_candles_parser.add_argument("--to", help="End time, ex: 2026-03-03T00:00:00+09:00")

    upbit_private_parser = upbit_subparsers.add_parser("private", help="Private exchange API.")
    upbit_private_subparsers = upbit_private_parser.add_subparsers(dest="upbit_private_command", required=True)

    upbit_private_subparsers.add_parser("accounts", help="List account balances.")

    upbit_private_chance_parser = upbit_private_subparsers.add_parser("chance", help="Get order chance for market.")
    upbit_private_chance_parser.add_argument("--market", required=True, help="Market, ex: KRW-BTC")

    upbit_private_order_test_parser = upbit_private_subparsers.add_parser(
        "order-test",
        help="Validate order format only (does not create a real order).",
    )
    upbit_private_order_test_parser.add_argument("--market", required=True, help="Market, ex: KRW-BTC")
    upbit_private_order_test_parser.add_argument("--side", required=True, choices=("bid", "ask"))
    upbit_private_order_test_parser.add_argument("--ord-type", required=True, help="Order type, ex: limit")
    upbit_private_order_test_parser.add_argument("--price", help="Price")
    upbit_private_order_test_parser.add_argument("--volume", help="Volume")
    upbit_private_order_test_parser.add_argument("--time-in-force", choices=("ioc", "fok", "post_only"))
    upbit_private_order_test_parser.add_argument("--identifier")

    upbit_ws_parser = upbit_subparsers.add_parser("ws", help="Public websocket quotation API.")
    upbit_ws_subparsers = upbit_ws_parser.add_subparsers(dest="upbit_ws_command", required=True)

    upbit_ws_ticker_parser = upbit_ws_subparsers.add_parser("ticker", help="Stream ticker events.")
    upbit_ws_ticker_parser.add_argument("--markets", required=True, help="Comma separated markets, ex: KRW-BTC,KRW-ETH")
    upbit_ws_ticker_parser.add_argument("--duration-sec", type=float, default=30, help="Stream duration in seconds")

    upbit_ws_top20_parser = upbit_ws_subparsers.add_parser("top20", help="Print top N by acc_trade_price_24h.")
    upbit_ws_top20_parser.add_argument("--quote", default="KRW", help="Quote currency filter, ex: KRW")
    upbit_ws_top20_parser.add_argument("--n", type=int, default=20, help="Top N size")
    upbit_ws_top20_parser.add_argument("--print-every-sec", type=float, default=5, help="Print interval seconds")
    upbit_ws_top20_parser.add_argument("--duration-sec", type=float, default=120, help="Stream duration in seconds")
    upbit_ws_top20_parser.add_argument("--exclude-caution", action="store_true")
    upbit_ws_top20_parser.add_argument("--exclude-inactive", action="store_true")

    paper_parser = subparsers.add_parser("paper", help="Paper-trading operations.")
    paper_subparsers = paper_parser.add_subparsers(dest="paper_command", required=True)

    paper_run_parser = paper_subparsers.add_parser("run", help="Run live websocket paper trading.")
    paper_run_parser.add_argument("--duration-sec", type=int, default=600)
    paper_run_parser.add_argument("--quote", help="Quote currency, ex: KRW")
    paper_run_parser.add_argument("--top-n", type=int)
    paper_run_parser.add_argument("--print-every-sec", type=float)
    paper_run_parser.add_argument("--starting-krw", type=float)
    paper_run_parser.add_argument("--per-trade-krw", type=float)
    paper_run_parser.add_argument("--max-positions", type=int)

    backtest_parser = subparsers.add_parser("backtest", help="Backtest operations.")
    backtest_subparsers = backtest_parser.add_subparsers(dest="backtest_command", required=True)

    backtest_run_parser = backtest_subparsers.add_parser("run", help="Run parquet candle backtest.")
    backtest_run_parser.add_argument("--tf", help="Timeframe, ex: 1m,5m")
    backtest_run_parser.add_argument("--market", help="Single market, ex: KRW-BTC")
    backtest_run_parser.add_argument("--markets", help="Comma separated markets, ex: KRW-BTC,KRW-ETH")
    backtest_run_parser.add_argument("--quote", help="Quote filter for universe mode, ex: KRW")
    backtest_run_parser.add_argument("--top-n", type=int, help="Universe size for static_start/fixed_list")
    backtest_run_parser.add_argument("--universe-mode", choices=("static_start", "fixed_list"))
    backtest_run_parser.add_argument("--from-ts-ms", type=int)
    backtest_run_parser.add_argument("--to-ts-ms", type=int)
    backtest_run_parser.add_argument("--duration-days", type=int)
    backtest_run_parser.add_argument("--dense-grid", action="store_true")
    backtest_run_parser.add_argument("--starting-krw", type=float)
    backtest_run_parser.add_argument("--per-trade-krw", type=float)
    backtest_run_parser.add_argument("--max-positions", type=int)
    backtest_run_parser.add_argument("--min-order-krw", type=float)
    backtest_run_parser.add_argument("--order-timeout-bars", type=int)
    backtest_run_parser.add_argument("--reprice-max-attempts", type=int)

    live_parser = subparsers.add_parser("live", help="Live runtime state/reconciliation operations.")
    live_subparsers = live_parser.add_subparsers(dest="live_command", required=True)

    live_status_parser = live_subparsers.add_parser("status", help="Show exchange/local state summary.")
    live_status_parser.add_argument("--bot-id", help="Override live.bot_id")

    live_reconcile_parser = live_subparsers.add_parser(
        "reconcile",
        help="Reconcile local state with exchange snapshot.",
    )
    live_reconcile_parser.add_argument("--bot-id", help="Override live.bot_id")
    live_reconcile_parser.add_argument("--apply", action="store_true", help="Apply planned actions.")
    live_reconcile_parser.add_argument("--dry-run", action="store_true", help="Force dry-run (default behavior).")
    live_reconcile_parser.add_argument(
        "--allow-cancel-external",
        action="store_true",
        help="Allow external order cancel only when config also allows it.",
    )

    live_run_parser = live_subparsers.add_parser("run", help="Run polling-based live sync daemon.")
    live_run_parser.add_argument("--bot-id", help="Override live.bot_id")
    live_run_parser.add_argument("--duration-sec", type=int, default=0, help="Run duration; 0 means until interrupted.")
    live_run_parser.add_argument(
        "--allow-cancel-external",
        action="store_true",
        help="Allow external order cancel only when config also allows it.",
    )

    live_export_state_parser = live_subparsers.add_parser("export-state", help="Export local state DB as JSON.")
    live_export_state_parser.add_argument("--bot-id", help="Override live.bot_id")

    exec_parser = subparsers.add_parser("exec", help="Execution-engine (gRPC) operations.")
    exec_subparsers = exec_parser.add_subparsers(dest="exec_command", required=True)

    exec_subparsers.add_parser("ping", help="Ping executor health endpoint.")

    exec_submit_test_parser = exec_subparsers.add_parser(
        "submit-test",
        help="Submit a limit intent to executor (recommended with executor order-test mode).",
    )
    exec_submit_test_parser.add_argument("--market", required=True, help="Market, ex: KRW-BTC")
    exec_submit_test_parser.add_argument("--side", required=True, choices=("bid", "ask"))
    exec_submit_test_parser.add_argument("--price", required=True, type=float)
    exec_submit_test_parser.add_argument("--volume", required=True, type=float)
    exec_submit_test_parser.add_argument("--identifier", help="Optional idempotency identifier")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    config = _load_base_config(Path(args.config_dir))

    if args.command == "data":
        return _handle_data_command(args, config)
    if args.command == "collect":
        return _handle_collect_command(args, Path(args.config_dir), config)
    if args.command == "micro":
        return _handle_micro_command(args, Path(args.config_dir), config)
    if args.command == "features":
        return _handle_features_command(args, Path(args.config_dir), config)
    if args.command == "model":
        return _handle_model_command(args, Path(args.config_dir), config)
    if args.command == "paper":
        return _handle_paper_command(args, Path(args.config_dir), config)
    if args.command == "backtest":
        return _handle_backtest_command(args, Path(args.config_dir), config)
    if args.command == "live":
        return _handle_live_command(args, Path(args.config_dir), config)
    if args.command == "exec":
        return _handle_exec_command(args, config)
    if args.command == "upbit":
        return _handle_upbit_command(args, Path(args.config_dir))

    print(f"autobot bootstrap | mode={args.mode} | config_dir={args.config_dir}")
    return 0


def _handle_data_command(args: argparse.Namespace, config: dict[str, Any]) -> int:
    if args.data_command == "sniff":
        return _handle_data_sniff(args, config)
    if args.data_command == "ingest":
        return _handle_data_ingest(args, config)
    if args.data_command == "validate":
        return _handle_data_validate(args, config)
    if args.data_command == "inventory":
        return _handle_data_inventory(args, config)
    raise ValueError(f"Unsupported data command: {args.data_command}")


def _handle_data_sniff(args: argparse.Namespace, config: dict[str, Any]) -> int:
    defaults = _data_defaults(config)
    raw_dir = Path(args.raw_dir or defaults["raw_dir"])
    pattern = args.pattern or defaults["pattern"]

    result = sniff_csv_files(
        raw_dir=raw_dir,
        pattern=pattern,
        sample_files=args.sample_files,
        sample_rows=args.sample_rows,
    )

    print(f"[sniff] raw_dir={result['raw_dir']} pattern={result['pattern']} sampled={result['sampled_files']}")
    for entry in result["entries"]:
        print(f"- {entry['file']} | status={entry['status']}")
        if "parsed" in entry:
            parsed = entry["parsed"]
            print(
                f"  parsed: quote={parsed['quote']} symbol={parsed['symbol']} "
                f"tf={parsed['tf']} market={parsed['market']}"
            )
        if "mapping" in entry:
            mapping = entry["mapping"]
            print(f"  mapping: ts={mapping['ts_source']} ({mapping['ts_policy']})")
        if entry.get("error_message"):
            print(f"  error: {entry['error_message']}")

    if result["failed_files"]:
        print("[sniff] failed files:")
        for failed_file in result["failed_files"]:
            print(f"  - {failed_file}")
    return 0


def _handle_data_ingest(args: argparse.Namespace, config: dict[str, Any]) -> int:
    defaults = _data_defaults(config)
    ingest_defaults = defaults["ingest"]
    duckdb_defaults = ingest_defaults["duckdb"]

    quote_filter = (args.quote.strip().upper(),) if args.quote else None
    tf_filter = _parse_csv_list(args.tf, normalize=str.lower)
    symbol_filter = _parse_csv_list(args.symbol, normalize=str.upper)

    duckdb_settings = DuckDBSettings(
        temp_directory=args.duckdb_temp_directory or duckdb_defaults["temp_directory"],
        memory_limit=args.duckdb_memory_limit or duckdb_defaults["memory_limit"],
        threads=args.duckdb_threads if args.duckdb_threads is not None else duckdb_defaults["threads"],
        fail_if_temp_not_set=duckdb_defaults["fail_if_temp_not_set"],
    )

    options = IngestOptions(
        raw_dir=Path(args.raw_dir or defaults["raw_dir"]),
        out_dir=Path(args.out_dir or defaults["parquet_root"]),
        dataset_name=args.dataset_name or defaults["dataset_name"],
        pattern=args.pattern or defaults["pattern"],
        workers=args.workers if args.workers is not None else ingest_defaults["workers"],
        mode=args.mode or ingest_defaults["mode"],
        compression=args.compression or ingest_defaults["compression"],
        quote_filter=quote_filter,
        tf_filter=tf_filter,
        symbol_filter=symbol_filter,
        limit_files=args.limit_files,
        dry_run=bool(args.dry_run),
        allow_sort_on_non_monotonic=ingest_defaults["allow_sort_on_non_monotonic"],
        allow_dedupe_on_duplicate_ts=ingest_defaults["allow_dedupe_on_duplicate_ts"],
        quote_volume_policy=ingest_defaults["quote_volume_policy"],
        gap_severity=defaults["qa"]["gap_severity"],
        quote_est_severity=defaults["qa"]["quote_est_severity"],
        ohlc_violation_policy=defaults["qa"]["ohlc_violation_policy"],
        engine=args.engine or ingest_defaults["engine"],
        duckdb=duckdb_settings,
    )

    summary = ingest_dataset(options)
    print(
        "[ingest] "
        f"discovered={summary.discovered_files} selected={summary.selected_files} "
        f"processed={summary.processed_files} skipped={summary.skipped_files} "
        f"ok={summary.ok_files} warn={summary.warn_files} fail={summary.fail_files}"
    )
    print(f"[ingest] manifest={summary.manifest_file}")
    print(f"[ingest] report={summary.report_file}")

    if options.dry_run:
        print("[ingest][dry-run] files:")
        for detail in summary.details:
            line = f"  - {detail.get('file')} | status={detail.get('status')}"
            if detail.get("quote") and detail.get("symbol"):
                line += f" | quote={detail['quote']} symbol={detail['symbol']}"
            if detail.get("tf"):
                line += f" | tf={detail['tf']}"
            if detail.get("market"):
                line += f" | market={detail['market']}"
            if detail.get("reason"):
                line += f" | reason={detail['reason']}"
            print(line)

    if summary.failures:
        print("[ingest] failures:")
        for failure in summary.failures:
            file_path = failure.get("file") or failure.get("source_csv_relpath")
            print(f"  - {file_path}: {failure.get('error_message', 'unknown error')}")

    return 2 if summary.fail_files > 0 else 0


def _handle_data_validate(args: argparse.Namespace, config: dict[str, Any]) -> int:
    defaults = _data_defaults(config)
    parquet_root = Path(defaults["parquet_root"])
    dataset_name = defaults["dataset_name"]
    parquet_dir = Path(args.parquet_dir) if args.parquet_dir else parquet_root / dataset_name

    tf_filter = _parse_csv_list(args.tf, normalize=str.lower)
    market_filter = _parse_csv_list(args.market, normalize=str.upper)
    summary = validate_dataset(
        parquet_dir=parquet_dir,
        tf_filter=tf_filter,
        market_filter=market_filter,
        gap_severity=defaults["qa"]["gap_severity"],
        quote_est_severity=defaults["qa"]["quote_est_severity"],
        ohlc_violation_policy=defaults["qa"]["ohlc_violation_policy"],
    )

    print(
        f"[validate] checked={summary.checked_files} ok={summary.ok_files} "
        f"warn={summary.warn_files} fail={summary.fail_files}"
    )
    print(f"[validate] report={summary.report_file}")
    return 2 if summary.fail_files > 0 else 0


def _handle_data_inventory(args: argparse.Namespace, config: dict[str, Any]) -> int:
    defaults = _data_defaults(config)
    parquet_root = Path(args.parquet_root or defaults["parquet_root"])
    dataset_name = str(args.dataset or defaults["dataset_name"]).strip() or defaults["dataset_name"]
    dataset_root = parquet_root / dataset_name

    tf_filter = _parse_csv_list(args.tf, normalize=str.lower)
    quote = str(args.quote or "KRW").strip().upper() if args.quote is not None else "KRW"
    lookback_months = max(int(args.lookback_months), 1)

    start_ts_ms = parse_utc_ts_ms(args.start)
    end_ts_ms = parse_utc_ts_ms(args.end, end_of_day=True)
    if start_ts_ms is None or end_ts_ms is None:
        default_start, default_end = default_inventory_window(lookback_months=lookback_months, end_ts_ms=end_ts_ms)
        start_ts_ms = start_ts_ms if start_ts_ms is not None else default_start
        end_ts_ms = end_ts_ms if end_ts_ms is not None else default_end

    summary = build_candle_inventory(
        dataset_root,
        tf_filter=tf_filter,
        quote=quote,
        window_start_ts_ms=start_ts_ms,
        window_end_ts_ms=end_ts_ms,
    )
    print(
        "[inventory] "
        f"dataset={dataset_name} pairs={summary['total_pairs']} with_data={summary['with_data_pairs']} "
        f"avg_coverage_pct={summary['average_coverage_pct']:.4f}"
    )
    for tf, tf_item in sorted(summary.get("by_tf", {}).items()):
        print(
            f"  - tf={tf} pairs={tf_item['pairs']} with_data={tf_item['with_data_pairs']} "
            f"avg_coverage_pct={tf_item['average_coverage_pct']:.4f}"
        )

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        print(f"[inventory] out={out_path}")
    return 0


def _handle_collect_command(args: argparse.Namespace, config_dir: Path, base_config: dict[str, Any]) -> int:
    if args.collect_command == "plan-candles":
        return _handle_collect_plan_candles(args, base_config)
    if args.collect_command == "candles":
        return _handle_collect_candles(args, config_dir, base_config)
    if args.collect_command == "plan-ticks":
        return _handle_collect_plan_ticks(args, base_config)
    if args.collect_command == "plan-ws-public":
        return _handle_collect_plan_ws_public(args, base_config)
    if args.collect_command == "ticks":
        return _handle_collect_ticks(args, config_dir, base_config)
    if args.collect_command == "ws-public":
        return _handle_collect_ws_public(args, config_dir)
    raise ValueError(f"Unsupported collect command: {args.collect_command}")


def _handle_collect_plan_candles(args: argparse.Namespace, config: dict[str, Any]) -> int:
    defaults = _data_defaults(config)
    parquet_root = str(args.parquet_root or defaults["parquet_root"])
    base_dataset = str(args.base_dataset or defaults["dataset_name"]).strip() or defaults["dataset_name"]
    plan_options = CandlePlanOptions(
        parquet_root=Path(parquet_root),
        base_dataset=base_dataset,
        output_path=Path(args.out),
        lookback_months=max(int(args.lookback_months), 1),
        tf_set=_parse_csv_list(args.tf, normalize=str.lower) or ("1m", "5m", "15m", "60m", "240m"),
        quote=str(args.quote).strip().upper() or "KRW",
        market_mode=str(args.market_mode).strip().lower(),
        top_n=max(int(args.top_n), 1),
        fixed_markets=_parse_csv_list(args.markets, normalize=str.upper),
        max_backfill_days_1m=max(int(args.max_backfill_days_1m), 1),
        end_ts_ms=parse_utc_ts_ms(args.end, end_of_day=True),
    )

    plan = generate_candle_topup_plan(plan_options)
    print(
        "[collect][plan-candles] "
        f"selected_markets={plan['summary']['selected_markets']} "
        f"targets={plan['summary']['targets']} skipped_ranges={plan['summary']['skipped_ranges']}"
    )
    print(f"[collect][plan-candles] out={plan_options.output_path}")
    return 0


def _handle_collect_candles(args: argparse.Namespace, config_dir: Path, base_config: dict[str, Any]) -> int:
    defaults = _data_defaults(base_config)
    parquet_root = Path(args.parquet_root or defaults["parquet_root"])

    collect_options = CandleCollectOptions(
        plan_path=Path(args.plan),
        parquet_root=parquet_root,
        out_dataset=str(args.out_dataset).strip() or "candles_api_v1",
        dry_run=_parse_bool_arg(args.dry_run, default=True),
        workers=max(int(args.workers), 1),
        max_requests=args.max_requests,
        stop_on_first_fail=_parse_bool_arg(args.stop_on_first_fail, default=False),
        rate_limit_strict=_parse_bool_arg(args.rate_limit_strict, default=True),
        config_dir=config_dir,
    )

    collect_summary = collect_candles_from_plan(collect_options)
    print(
        "[collect][candles] "
        f"discovered={collect_summary.discovered_targets} selected={collect_summary.selected_targets} "
        f"processed={collect_summary.processed_targets} ok={collect_summary.ok_targets} "
        f"warn={collect_summary.warn_targets} fail={collect_summary.fail_targets} "
        f"calls={collect_summary.calls_made} throttled={collect_summary.throttled_count} "
        f"backoff={collect_summary.backoff_count}"
    )
    print(f"[collect][candles] collect_report={collect_summary.collect_report_file}")
    print(f"[collect][candles] build_report={collect_summary.build_report_file}")
    if collect_options.dry_run:
        return 0

    validate_summary = validate_candles_api_dataset(
        parquet_root=parquet_root,
        dataset_name=collect_options.out_dataset,
        plan_path=collect_options.plan_path,
        report_path=Path("data/collect/_meta/candle_validate_report.json"),
        gap_severity=defaults["qa"]["gap_severity"],
        quote_est_severity=defaults["qa"]["quote_est_severity"],
        ohlc_violation_policy=defaults["qa"]["ohlc_violation_policy"],
    )
    print(
        "[collect][validate] "
        f"checked={validate_summary.checked_files} ok={validate_summary.ok_files} "
        f"warn={validate_summary.warn_files} fail={validate_summary.fail_files} "
        f"schema_ok={validate_summary.schema_ok} ohlc_ok={validate_summary.ohlc_ok}"
    )
    print(f"[collect][validate] report={validate_summary.validate_report_file}")

    if collect_summary.fail_targets > 0 or validate_summary.fail_files > 0:
        return 2
    return 0


def _handle_collect_plan_ticks(args: argparse.Namespace, config: dict[str, Any]) -> int:
    defaults = _data_defaults(config)
    parquet_root = Path(args.parquet_root or defaults["parquet_root"])
    base_dataset = str(args.base_dataset or defaults["dataset_name"]).strip() or defaults["dataset_name"]

    plan_options = TicksPlanOptions(
        parquet_root=parquet_root,
        base_dataset=base_dataset,
        output_path=Path(args.out),
        quote=str(args.quote).strip().upper() or "KRW",
        market_mode=str(args.market_mode).strip().lower(),
        top_n=max(int(args.top_n), 1),
        fixed_markets=_parse_csv_list(args.markets, normalize=str.upper),
        days_ago=_parse_days_ago_csv(args.days_ago, default=(1, 2, 3, 4, 5, 6, 7)),
    )
    plan = generate_ticks_collection_plan(plan_options)
    print(
        "[collect][plan-ticks] "
        f"selected_markets={plan['summary']['selected_markets']} "
        f"targets={plan['summary']['targets']} "
        f"days_ago_count={plan['summary']['days_ago_count']}"
    )
    print(f"[collect][plan-ticks] out={plan_options.output_path}")
    return 0


def _handle_collect_plan_ws_public(args: argparse.Namespace, config: dict[str, Any]) -> int:
    defaults = _data_defaults(config)
    parquet_root = Path(args.parquet_root or defaults["parquet_root"])
    base_dataset = str(args.base_dataset or defaults["dataset_name"]).strip() or defaults["dataset_name"]

    plan_options = WsPublicPlanOptions(
        parquet_root=parquet_root,
        base_dataset=base_dataset,
        output_path=Path(args.out),
        quote=str(args.quote).strip().upper() or "KRW",
        market_mode=str(args.market_mode).strip().lower(),
        top_n=max(int(args.top_n), 1),
        fixed_markets=_parse_csv_list(args.markets, normalize=str.upper),
        channels=_parse_csv_list(args.channels, normalize=str.lower) or ("trade", "orderbook"),
        format=str(args.format).strip().upper() or "DEFAULT",
        orderbook_topk=max(int(args.orderbook_topk), 1),
        orderbook_level=_parse_orderbook_level_arg(args.orderbook_level),
        orderbook_min_write_interval_ms=max(int(args.orderbook_min_write_interval_ms), 1),
    )
    plan = generate_ws_public_collection_plan(plan_options)
    print(
        "[collect][plan-ws-public] "
        f"selected_markets={plan['summary']['selected_markets']} "
        f"codes_count={plan['summary']['codes_count']} "
        f"channels_count={plan['summary']['channels_count']}"
    )
    print(f"[collect][plan-ws-public] out={plan_options.output_path}")
    return 0


def _handle_collect_ticks(args: argparse.Namespace, config_dir: Path, base_config: dict[str, Any]) -> int:
    ticks_command = getattr(args, "collect_ticks_command", None)
    if ticks_command == "validate":
        summary = validate_ticks_raw_dataset(
            raw_root=Path(args.raw_root),
            report_path=Path(args.meta_dir) / "ticks_validate_report.json",
            date_filter=args.date,
            dup_ratio_warn_threshold=float(args.dup_ratio_threshold),
        )
        print(
            "[collect][ticks][validate] "
            f"checked={summary.checked_files} ok={summary.ok_files} "
            f"warn={summary.warn_files} fail={summary.fail_files} "
            f"schema_ok_ratio={summary.schema_ok_ratio:.6f} "
            f"dup_ratio_overall={summary.dup_ratio_overall:.6f}"
        )
        print(f"[collect][ticks][validate] report={summary.validate_report_file}")
        return 2 if summary.fail_files > 0 else 0

    if ticks_command == "stats":
        stats = collect_ticks_stats(
            raw_root=Path(args.raw_root),
            meta_dir=Path(args.meta_dir),
            date_filter=args.date,
        )
        _print_json(stats)
        return 0

    defaults = _data_defaults(base_config)
    parquet_root = Path(args.parquet_root or defaults["parquet_root"])
    base_dataset = str(args.base_dataset or defaults["dataset_name"]).strip() or defaults["dataset_name"]
    mode = str(args.mode).strip().lower()
    default_days_ago = (1,) if mode == "daily" else (1, 2, 3, 4, 5, 6, 7)
    days_ago = _parse_days_ago_csv(args.days_ago, default=default_days_ago)

    collect_options = TicksCollectOptions(
        plan_path=Path(args.plan),
        raw_root=Path(args.raw_root),
        meta_dir=Path(args.meta_dir),
        parquet_root=parquet_root,
        base_dataset=base_dataset,
        quote=str(args.quote).strip().upper() or "KRW",
        market_mode=str(args.market_mode).strip().lower(),
        top_n=max(int(args.top_n), 1),
        fixed_markets=_parse_csv_list(args.markets, normalize=str.upper),
        days_ago=days_ago,
        mode=mode,
        dry_run=_parse_bool_arg(args.dry_run, default=False),
        workers=max(int(args.workers), 1),
        rate_limit_strict=_parse_bool_arg(args.rate_limit_strict, default=True),
        max_pages_per_target=(max(int(args.max_pages_per_target), 1) if args.max_pages_per_target is not None else None),
        max_requests=args.max_requests,
        retention_days=max(int(args.retention_days), 1),
        config_dir=config_dir,
    )

    collect_summary = collect_ticks_from_plan(collect_options)
    print(
        "[collect][ticks] "
        f"discovered={collect_summary.discovered_targets} selected={collect_summary.selected_targets} "
        f"processed={collect_summary.processed_targets} ok={collect_summary.ok_targets} "
        f"warn={collect_summary.warn_targets} fail={collect_summary.fail_targets} "
        f"calls={collect_summary.calls_made} throttled={collect_summary.throttled_count} "
        f"backoff={collect_summary.backoff_count} rows={collect_summary.rows_collected_total}"
    )
    print(f"[collect][ticks] plan={collect_summary.plan_file}")
    print(f"[collect][ticks] manifest={collect_summary.manifest_file}")
    print(f"[collect][ticks] checkpoint={collect_summary.checkpoint_file}")
    print(f"[collect][ticks] collect_report={collect_summary.collect_report_file}")

    if collect_options.dry_run:
        return 0

    validate_summary = validate_ticks_raw_dataset(
        raw_root=collect_options.raw_root,
        report_path=collect_options.validate_report_path,
        date_filter=None,
        dup_ratio_warn_threshold=0.05,
    )
    print(
        "[collect][ticks][validate] "
        f"checked={validate_summary.checked_files} ok={validate_summary.ok_files} "
        f"warn={validate_summary.warn_files} fail={validate_summary.fail_files} "
        f"schema_ok_ratio={validate_summary.schema_ok_ratio:.6f} "
        f"dup_ratio_overall={validate_summary.dup_ratio_overall:.6f}"
    )
    print(f"[collect][ticks][validate] report={validate_summary.validate_report_file}")
    if collect_summary.fail_targets > 0 or validate_summary.fail_files > 0:
        return 2
    return 0


def _handle_collect_ws_public(args: argparse.Namespace, config_dir: Path) -> int:
    ws_command = getattr(args, "collect_ws_public_command", None)
    if ws_command == "validate":
        summary = validate_ws_public_raw_dataset(
            raw_root=Path(args.raw_root),
            meta_dir=Path(args.meta_dir),
            report_path=Path(args.meta_dir) / "ws_validate_report.json",
            date_filter=args.date,
        )
        print(
            "[collect][ws-public][validate] "
            f"checked={summary.checked_files} ok={summary.ok_files} "
            f"warn={summary.warn_files} fail={summary.fail_files} "
            f"parse_ok_ratio={summary.parse_ok_ratio:.6f}"
        )
        print(f"[collect][ws-public][validate] report={summary.validate_report_file}")
        return 2 if summary.fail_files > 0 else 0

    if ws_command == "stats":
        stats = collect_ws_public_stats(
            raw_root=Path(args.raw_root),
            meta_dir=Path(args.meta_dir),
            date_filter=args.date,
        )
        _print_json(stats)
        return 0

    collect_options = WsPublicCollectOptions(
        plan_path=Path(args.plan),
        raw_root=Path(args.raw_root),
        meta_dir=Path(args.meta_dir),
        duration_sec=max(int(args.duration_sec), 1),
        rotate_sec=max(int(args.rotate_sec), 1),
        max_bytes=max(int(args.max_bytes), 1024),
        retention_days=max(int(args.retention_days), 1),
        rate_limit_strict=_parse_bool_arg(args.rate_limit_strict, default=True),
        reconnect_max_per_min=max(int(args.reconnect_max_per_min), 1),
        orderbook_spread_bps_threshold=float(args.orderbook_spread_bps_threshold),
        orderbook_top1_size_change_threshold=float(args.orderbook_top1_size_change_threshold),
        config_dir=config_dir,
    )

    collect_summary = collect_ws_public_from_plan(collect_options)
    print(
        "[collect][ws-public] "
        f"run_id={collect_summary.run_id} duration={collect_summary.duration_sec}s "
        f"codes={collect_summary.codes_count} "
        f"recv_trade={collect_summary.received_trade} recv_orderbook={collect_summary.received_orderbook} "
        f"written_trade={collect_summary.written_trade} written_orderbook={collect_summary.written_orderbook} "
        f"dropped_orderbook_interval={collect_summary.dropped_orderbook_by_interval} "
        f"parse_drop={collect_summary.dropped_by_parse_error} "
        f"reconnect={collect_summary.reconnect_count}"
    )
    print(f"[collect][ws-public] collect_report={collect_summary.collect_report_file}")
    print(f"[collect][ws-public] manifest={collect_summary.manifest_file}")
    print(f"[collect][ws-public] checkpoint={collect_summary.checkpoint_file}")
    print(f"[collect][ws-public] runs_summary={collect_summary.runs_summary_file}")

    validate_summary = validate_ws_public_raw_dataset(
        raw_root=collect_options.raw_root,
        meta_dir=collect_options.meta_dir,
        report_path=collect_options.validate_report_path,
        date_filter=None,
    )
    print(
        "[collect][ws-public][validate] "
        f"checked={validate_summary.checked_files} ok={validate_summary.ok_files} "
        f"warn={validate_summary.warn_files} fail={validate_summary.fail_files} "
        f"parse_ok_ratio={validate_summary.parse_ok_ratio:.6f}"
    )
    print(f"[collect][ws-public][validate] report={validate_summary.validate_report_file}")
    if collect_summary.failures or validate_summary.fail_files > 0:
        return 2
    return 0


def _handle_micro_command(args: argparse.Namespace, config_dir: Path, base_config: dict[str, Any]) -> int:
    tf_set = _parse_csv_list(args.tf, normalize=str.lower) or ("1m", "5m")
    out_root = Path(args.out_root)

    if args.micro_command == "aggregate":
        defaults = _load_micro_defaults(config_dir=config_dir, base_config=base_config)
        base_candles_root = _resolve_base_candles_path(
            base_candles=args.base_candles,
            default_dataset=defaults["base_candles_dataset"],
            parquet_root=Path(defaults["parquet_root"]),
        )
        options = MicroAggregateOptions(
            tf_set=tuple(tf_set),
            start=str(args.start).strip(),
            end=str(args.end).strip(),
            quote=str(args.quote or defaults["quote"]).strip().upper(),
            top_n=max(int(args.top_n if args.top_n is not None else defaults["top_n"]), 1),
            fixed_markets=_parse_csv_list(args.markets, normalize=str.upper),
            raw_ticks_root=Path(args.raw_ticks_root or defaults["raw_ticks_root"]),
            raw_ws_root=Path(args.raw_ws_root or defaults["raw_ws_root"]),
            out_root=out_root,
            base_candles_root=base_candles_root,
            mode=str(args.mode).strip().lower(),
            chunk_rows=max(int(args.chunk_rows), 1),
            topk=max(int(args.topk), 1),
            alignment_mode=str(args.alignment_mode).strip().lower(),
            sample_market=str(args.sample_market).strip().upper(),
        )
        summary = aggregate_micro_v1(options)
        print(
            "[micro][aggregate] "
            f"run_id={summary.run_id} dates={len(summary.dates)} markets={len(summary.markets)} "
            f"rows_written={summary.rows_written_total} parse_ok_ratio={summary.parse_ok_ratio:.6f} "
            f"alignment_mode={summary.alignment_mode}"
        )
        print(f"[micro][aggregate] manifest={summary.manifest_file}")
        print(f"[micro][aggregate] report={summary.aggregate_report_file}")
        return 0

    if args.micro_command == "validate":
        defaults = _load_micro_defaults(config_dir=config_dir, base_config=base_config)
        base_candles_root = _resolve_base_candles_path(
            base_candles=args.base_candles,
            default_dataset=defaults["base_candles_dataset"],
            parquet_root=Path(defaults["parquet_root"]),
        )
        summary = validate_micro_dataset_v1(
            out_root=out_root,
            tf_set=tuple(tf_set),
            base_candles_root=base_candles_root,
            join_match_warn_threshold=float(args.join_match_warn),
            join_match_fail_threshold=float(args.join_match_fail),
            micro_available_warn_threshold=float(args.micro_available_warn),
            volume_fail_ratio_threshold=float(args.volume_fail_ratio),
            price_fail_ratio_threshold=float(args.price_fail_ratio),
        )
        print(
            "[micro][validate] "
            f"checked={summary.checked_files} ok={summary.ok_files} "
            f"warn={summary.warn_files} fail={summary.fail_files} "
            f"parse_ok_ratio={summary.parse_ok_ratio:.6f} "
            f"join_match_ratio={summary.join_match_ratio if summary.join_match_ratio is not None else 'NA'}"
        )
        print(f"[micro][validate] report={summary.validate_report_file}")
        return 2 if summary.fail_files > 0 else 0

    if args.micro_command == "stats":
        stats = micro_stats_v1(
            out_root=out_root,
            tf_set=tuple(tf_set),
        )
        _print_json(stats)
        return 0

    raise ValueError(f"Unsupported micro command: {args.micro_command}")


def _handle_features_command(args: argparse.Namespace, config_dir: Path, base_config: dict[str, Any]) -> int:
    features_config = load_features_config(config_dir, base_config=base_config)

    if args.features_command == "build":
        options = FeatureBuildOptions(
            tf=str(args.tf).strip().lower(),
            quote=(str(args.quote).strip().upper() if args.quote else None),
            top_n=args.top_n,
            start=args.start,
            end=args.end,
            feature_set=str(args.feature_set).strip().lower(),
            label_set=str(args.label_set).strip().lower(),
            workers=max(int(args.workers), 1),
            fail_on_warn=_parse_bool_arg(args.fail_on_warn, default=False),
        )
        summary = build_features_dataset(features_config, options)
        print(
            "[features][build] "
            f"discovered={summary.discovered_markets} selected={len(summary.selected_markets)} "
            f"processed={summary.processed_markets} ok={summary.ok_markets} "
            f"warn={summary.warn_markets} fail={summary.fail_markets}"
        )
        print(f"[features][build] rows_total={summary.rows_total} min_ts_ms={summary.min_ts_ms} max_ts_ms={summary.max_ts_ms}")
        print(f"[features][build] output={summary.output_path}")
        print(f"[features][build] manifest={summary.manifest_file}")
        print(f"[features][build] report={summary.build_report_file}")
        code = 2 if summary.fail_markets > 0 else 0
        if options.fail_on_warn and summary.warn_markets > 0:
            code = 2
        return code

    if args.features_command == "validate":
        options = FeatureValidateOptions(
            tf=str(args.tf).strip().lower(),
            quote=(str(args.quote).strip().upper() if args.quote else None),
            top_n=args.top_n,
        )
        summary = validate_features_dataset(features_config, options)
        print(
            "[features][validate] "
            f"checked={summary.checked_files} ok={summary.ok_files} "
            f"warn={summary.warn_files} fail={summary.fail_files}"
        )
        print(f"[features][validate] schema_ok={summary.schema_ok} null_ratio_overall={summary.null_ratio_overall:.6f}")
        print(f"[features][validate] leakage_smoke={summary.leakage_smoke}")
        print(f"[features][validate] report={summary.validate_report_file}")
        if summary.fail_files > 0 or summary.leakage_smoke != "PASS":
            return 2
        return 0

    if args.features_command == "sample":
        rows = sample_features(
            features_config,
            tf=str(args.tf).strip().lower(),
            market=str(args.market).strip().upper(),
            rows=max(int(args.rows), 0),
        )
        _print_json(rows)
        return 0

    if args.features_command == "stats":
        stats = features_stats(
            features_config,
            tf=str(args.tf).strip().lower(),
            quote=(str(args.quote).strip().upper() if args.quote else None),
            top_n=args.top_n,
        )
        _print_json(stats)
        return 0

    raise ValueError(f"Unsupported features command: {args.features_command}")


def _handle_model_command(args: argparse.Namespace, config_dir: Path, base_config: dict[str, Any]) -> int:
    try:
        defaults = load_train_defaults(config_dir, base_config=base_config)
        features_config = load_features_config(config_dir, base_config=base_config)
        registry_root = Path(str(defaults["registry_root"]))
        logs_root = Path(str(defaults["logs_root"]))
        model_family = str(getattr(args, "model_family", None) or defaults["model_family"]).strip()
        if not model_family:
            model_family = "train_v1"

        if args.model_command == "train":
            top_n = int(args.top_n if args.top_n is not None else defaults["top_n"])
            options = TrainRunOptions(
                dataset_root=features_config.output_dataset_root,
                registry_root=registry_root,
                logs_root=logs_root,
                model_family=model_family,
                tf=str(args.tf or defaults["tf"]).strip().lower(),
                quote=str(args.quote or defaults["quote"]).strip().upper(),
                top_n=max(top_n, 1),
                start=str(args.start or defaults["start"]).strip(),
                end=str(args.end or defaults["end"]).strip(),
                feature_set=str(args.feature_set).strip().lower(),
                label_set=str(args.label_set).strip().lower(),
                task=str(args.task or defaults["task"]).strip().lower(),
                run_baseline=_parse_bool_arg(args.run_baseline, default=bool(defaults["run_baseline"])),
                run_booster=_parse_bool_arg(args.run_booster, default=bool(defaults["run_booster"])),
                booster_sweep_trials=int(
                    args.booster_sweep_trials
                    if args.booster_sweep_trials is not None
                    else defaults["booster_sweep_trials"]
                ),
                seed=int(args.seed if args.seed is not None else defaults["seed"]),
                nthread=int(args.nthread if args.nthread is not None else defaults["nthread"]),
                batch_rows=max(int(defaults["batch_rows"]), 1),
                train_ratio=float(defaults["train_ratio"]),
                valid_ratio=float(defaults["valid_ratio"]),
                test_ratio=float(defaults["test_ratio"]),
                embargo_bars=max(int(defaults["embargo_bars"]), 0),
                baseline_alpha=float(defaults["baseline_alpha"]),
                baseline_epochs=max(int(defaults["baseline_epochs"]), 1),
                fee_bps_est=float(defaults["fee_bps_est"]),
                safety_bps=float(defaults["safety_bps"]),
                ev_scan_steps=max(int(defaults["ev_scan_steps"]), 10),
                ev_min_selected=max(int(defaults["ev_min_selected"]), 1),
                gate_min_pr_auc=float(defaults["gate_min_pr_auc"]),
                gate_min_precision_top5=float(defaults["gate_min_precision_top5"]),
                gate_max_two_market_bias=float(defaults["gate_max_two_market_bias"]),
            )
            summary = train_and_register(options)
            print(
                "[model][train] "
                f"run_id={summary.run_id} champion={summary.champion} "
                f"test_precision_top5={summary.leaderboard_row.get('test_precision_top5', 0.0):.6f} "
                f"test_pr_auc={summary.leaderboard_row.get('test_pr_auc', 0.0):.6f}"
            )
            print(f"[model][train] run_dir={summary.run_dir}")
            print(f"[model][train] train_report={summary.train_report_path}")
            return 0

        if args.model_command == "eval":
            result = evaluate_registered_model(
                registry_root=registry_root,
                model_ref=str(args.model_ref).strip(),
                model_family=(str(args.model_family).strip() if args.model_family else None),
                split=str(args.split).strip().lower(),
                report_csv=(Path(args.report_csv) if args.report_csv else None),
            )
            _print_json(result)
            return 0

        if args.model_command == "list":
            rows = list_registered_models(
                registry_root=registry_root,
                model_family=(str(args.model_family).strip() if args.model_family else None),
            )
            limit = max(int(args.limit), 1)
            _print_json(rows[:limit])
            return 0

        if args.model_command == "show":
            detail = show_registered_model(
                registry_root=registry_root,
                model_ref=str(args.model_ref).strip(),
                model_family=(str(args.model_family).strip() if args.model_family else None),
            )
            _print_json(detail)
            return 0

        raise ValueError(f"Unsupported model command: {args.model_command}")
    except (ValueError, FileNotFoundError, RuntimeError) as exc:
        print(f"[model][error] {exc}")
        return 2


def _handle_upbit_command(args: argparse.Namespace, config_dir: Path) -> int:
    try:
        settings = load_upbit_settings(config_dir)
        _ensure_upbit_runtime_available()

        if args.upbit_scope == "public":
            return _handle_upbit_public_command(args, settings)
        if args.upbit_scope == "private":
            return _handle_upbit_private_command(args, settings)
        if args.upbit_scope == "ws":
            return _handle_upbit_ws_command(args, settings)
        raise ValueError(f"Unsupported upbit scope: {args.upbit_scope}")
    except (ConfigError, UpbitError) as exc:
        print(f"[upbit][error] {exc}")
        return 2


def _handle_paper_command(args: argparse.Namespace, config_dir: Path, base_config: dict[str, Any]) -> int:
    try:
        if args.paper_command != "run":
            raise ValueError(f"Unsupported paper command: {args.paper_command}")

        settings = load_upbit_settings(config_dir)
        _ensure_upbit_runtime_available()
        risk_doc = _load_yaml_doc(config_dir / "risk.yaml")
        strategy_doc = _load_yaml_doc(config_dir / "strategy.yaml")
        defaults = _paper_defaults(base_config=base_config, risk_doc=risk_doc, strategy_doc=strategy_doc)

        run_settings = PaperRunSettings(
            duration_sec=max(int(args.duration_sec), 1),
            quote=str(args.quote or defaults["quote"]).strip().upper(),
            top_n=max(int(args.top_n if args.top_n is not None else defaults["top_n"]), 1),
            print_every_sec=max(
                float(args.print_every_sec if args.print_every_sec is not None else defaults["print_every_sec"]),
                1.0,
            ),
            universe_refresh_sec=max(float(defaults["universe_refresh_sec"]), 1.0),
            universe_hold_sec=max(float(defaults["universe_hold_sec"]), 0.0),
            momentum_window_sec=max(int(defaults["momentum_window_sec"]), 1),
            min_momentum_pct=float(defaults["min_momentum_pct"]),
            starting_krw=max(
                float(args.starting_krw if args.starting_krw is not None else defaults["starting_krw"]),
                0.0,
            ),
            per_trade_krw=max(
                float(args.per_trade_krw if args.per_trade_krw is not None else defaults["per_trade_krw"]),
                1.0,
            ),
            max_positions=max(int(args.max_positions if args.max_positions is not None else defaults["max_positions"]), 1),
            min_order_krw=max(float(defaults["min_order_krw"]), 0.0),
            order_timeout_sec=max(float(defaults["order_timeout_sec"]), 1.0),
            reprice_max_attempts=max(int(defaults["reprice_max_attempts"]), 0),
            cooldown_sec_after_fail=max(int(defaults["cooldown_sec_after_fail"]), 0),
            max_consecutive_failures=max(int(defaults["max_consecutive_failures"]), 1),
            out_root_dir=str(defaults["paper_out_dir"]),
        )

        summary = run_live_paper_sync(upbit_settings=settings, run_settings=run_settings)
        _print_json(asdict(summary))
        return 0
    except (ConfigError, UpbitError, ValueError) as exc:
        print(f"[paper][error] {exc}")
        return 2


def _handle_backtest_command(args: argparse.Namespace, config_dir: Path, base_config: dict[str, Any]) -> int:
    try:
        if args.backtest_command != "run":
            raise ValueError(f"Unsupported backtest command: {args.backtest_command}")

        risk_doc = _load_yaml_doc(config_dir / "risk.yaml")
        strategy_doc = _load_yaml_doc(config_dir / "strategy.yaml")
        backtest_doc = _load_yaml_doc(config_dir / "backtest.yaml")
        defaults = _backtest_defaults(
            base_config=base_config,
            risk_doc=risk_doc,
            strategy_doc=strategy_doc,
            backtest_doc=backtest_doc,
        )

        markets_cli = _parse_csv_list(getattr(args, "markets", None), normalize=str.upper) or ()
        run_settings = BacktestRunSettings(
            dataset_name=str(defaults["dataset_name"]),
            parquet_root=str(defaults["parquet_root"]),
            tf=str(args.tf or defaults["tf"]).strip().lower(),
            from_ts_ms=getattr(args, "from_ts_ms", None) if getattr(args, "from_ts_ms", None) is not None else defaults["from_ts_ms"],
            to_ts_ms=getattr(args, "to_ts_ms", None) if getattr(args, "to_ts_ms", None) is not None else defaults["to_ts_ms"],
            duration_days=(
                int(getattr(args, "duration_days", defaults["duration_days"]))
                if getattr(args, "duration_days", defaults["duration_days"]) is not None
                else None
            ),
            market=str(args.market).strip().upper() if getattr(args, "market", None) else None,
            markets=tuple(markets_cli),
            universe_mode=str(args.universe_mode or defaults["universe_mode"]).strip().lower(),
            quote=str(args.quote or defaults["quote"]).strip().upper(),
            top_n=max(int(args.top_n if args.top_n is not None else defaults["top_n"]), 1),
            dense_grid=bool(args.dense_grid) if bool(args.dense_grid) else bool(defaults["dense_grid"]),
            starting_krw=max(
                float(args.starting_krw if args.starting_krw is not None else defaults["starting_krw"]),
                0.0,
            ),
            per_trade_krw=max(
                float(args.per_trade_krw if args.per_trade_krw is not None else defaults["per_trade_krw"]),
                1.0,
            ),
            max_positions=max(int(args.max_positions if args.max_positions is not None else defaults["max_positions"]), 1),
            min_order_krw=max(
                float(args.min_order_krw if args.min_order_krw is not None else defaults["min_order_krw"]),
                0.0,
            ),
            order_timeout_bars=max(
                int(args.order_timeout_bars if args.order_timeout_bars is not None else defaults["order_timeout_bars"]),
                1,
            ),
            reprice_max_attempts=max(
                int(
                    args.reprice_max_attempts
                    if args.reprice_max_attempts is not None
                    else defaults["reprice_max_attempts"]
                ),
                0,
            ),
            reprice_tick_steps=max(int(defaults["reprice_tick_steps"]), 1),
            rules_ttl_sec=max(int(defaults["rules_ttl_sec"]), 1),
            momentum_window_sec=max(int(defaults["momentum_window_sec"]), 1),
            min_momentum_pct=float(defaults["min_momentum_pct"]),
            output_root_dir=str(defaults["backtest_out_dir"]),
            seed=int(defaults["seed"]),
        )

        upbit_settings: Any = None
        try:
            upbit_settings = load_upbit_settings(config_dir)
            _ensure_upbit_runtime_available()
        except ConfigError:
            upbit_settings = None

        summary = run_backtest_sync(run_settings=run_settings, upbit_settings=upbit_settings)
        _print_json(asdict(summary))
        return 0
    except (ConfigError, UpbitError, ValueError, RuntimeError) as exc:
        print(f"[backtest][error] {exc}")
        return 2


def _handle_live_command(args: argparse.Namespace, config_dir: Path, base_config: dict[str, Any]) -> int:
    try:
        defaults = _live_defaults(base_config)
        bot_id = str(args.bot_id or defaults["bot_id"]).strip()
        if not bot_id:
            raise ValueError("live.bot_id must not be blank")
        db_path = Path(str(defaults["state_db_path"]))
        command = str(args.live_command)
        apply_mode = bool(getattr(args, "apply", False)) and not bool(getattr(args, "dry_run", False))
        if bool(getattr(args, "apply", False)) and bool(getattr(args, "dry_run", False)):
            raise ValueError("cannot use --apply and --dry-run together")
        allow_cancel_external_cli = bool(getattr(args, "allow_cancel_external", False))

        with LiveStateStore(db_path) as store:
            if command == "export-state":
                _print_json(store.export_state())
                return 0

            store.bootstrap_bot_meta(bot_id=bot_id, version=__version__)
            needs_write_lock = bool(defaults["state_run_lock"]) and (
                command == "run" or (command == "reconcile" and apply_mode)
            )
            lock_acquired = False
            if needs_write_lock:
                lock_acquired = store.acquire_run_lock(bot_id=bot_id)
                if not lock_acquired:
                    raise ValueError(f"run lock is already held for bot_id={bot_id}")
            try:
                settings = load_upbit_settings(config_dir)
                _ensure_upbit_runtime_available()
                credentials = require_upbit_credentials(settings)
                with UpbitHttpClient(settings, credentials=credentials) as http_client:
                    client = UpbitPrivateClient(http_client)
                    accounts: Any = None
                    open_orders: Any = None

                    if command in {"status", "reconcile"}:
                        accounts = client.accounts()
                        open_orders = client.open_orders(states=("wait", "watch"))

                    if command == "status":
                        reconcile_report = reconcile_exchange_snapshot(
                            store=store,
                            bot_id=bot_id,
                            identifier_prefix=str(defaults["identifier_prefix"]),
                            accounts_payload=accounts,
                            open_orders_payload=open_orders,
                            unknown_open_orders_policy=str(defaults["unknown_open_orders_policy"]),
                            unknown_positions_policy=str(defaults["unknown_positions_policy"]),
                            allow_cancel_external_orders=bool(defaults["allow_cancel_external_orders"]),
                            default_risk_sl_pct=float(defaults["default_risk_sl_pct"]),
                            default_risk_tp_pct=float(defaults["default_risk_tp_pct"]),
                            default_risk_trailing_enabled=bool(defaults["default_risk_trailing_enabled"]),
                            quote_currency=str(defaults["quote_currency"]),
                            dry_run=True,
                        )
                        payload = {
                            "bot_id": bot_id,
                            "db_path": str(db_path),
                            "exchange": {
                                "accounts_count": len(accounts) if isinstance(accounts, list) else 0,
                                "open_orders_count": len(open_orders) if isinstance(open_orders, list) else 0,
                            },
                            "local": {
                                "positions_count": len(store.list_positions()),
                                "open_orders_count": len(store.list_orders(open_only=True)),
                            },
                            "reconcile_preview": reconcile_report,
                        }
                        _print_json(payload)
                        return 0

                    if command == "reconcile":
                        report = reconcile_exchange_snapshot(
                            store=store,
                            bot_id=bot_id,
                            identifier_prefix=str(defaults["identifier_prefix"]),
                            accounts_payload=accounts,
                            open_orders_payload=open_orders,
                            fetch_order_detail=lambda uuid, identifier: client.order(uuid=uuid, identifier=identifier),
                            unknown_open_orders_policy=str(defaults["unknown_open_orders_policy"]),
                            unknown_positions_policy=str(defaults["unknown_positions_policy"]),
                            allow_cancel_external_orders=bool(defaults["allow_cancel_external_orders"]),
                            default_risk_sl_pct=float(defaults["default_risk_sl_pct"]),
                            default_risk_tp_pct=float(defaults["default_risk_tp_pct"]),
                            default_risk_trailing_enabled=bool(defaults["default_risk_trailing_enabled"]),
                            quote_currency=str(defaults["quote_currency"]),
                            dry_run=not apply_mode,
                        )
                        cancel_summary = apply_cancel_actions(
                            report=report,
                            cancel_order=lambda uuid, identifier: client.cancel_order(uuid=uuid, identifier=identifier),
                            apply=apply_mode,
                            allow_cancel_external_cli=allow_cancel_external_cli,
                            allow_cancel_external_config=bool(defaults["allow_cancel_external_orders"]),
                        )
                        output = {
                            "apply": apply_mode,
                            "report": report,
                            "cancel_summary": cancel_summary,
                        }
                        if apply_mode:
                            store.set_checkpoint(name="last_reconcile", payload=output)
                        _print_json(output)
                        return 2 if bool(report.get("halted")) else 0

                    if command == "run":
                        if bool(defaults["sync_use_private_ws"]) and bool(defaults["sync_use_executor_ws"]):
                            raise ValueError("live.sync.use_private_ws and live.sync.use_executor_ws cannot both be true")

                        daemon_settings = LiveDaemonSettings(
                            bot_id=bot_id,
                            identifier_prefix=str(defaults["identifier_prefix"]),
                            unknown_open_orders_policy=str(defaults["unknown_open_orders_policy"]),
                            unknown_positions_policy=str(defaults["unknown_positions_policy"]),
                            allow_cancel_external_orders=bool(defaults["allow_cancel_external_orders"]),
                            poll_interval_sec=int(defaults["sync_poll_interval_sec"]),
                            quote_currency=str(defaults["quote_currency"]),
                            startup_reconcile=bool(defaults["startup_reconcile"]),
                            default_risk_sl_pct=float(defaults["default_risk_sl_pct"]),
                            default_risk_tp_pct=float(defaults["default_risk_tp_pct"]),
                            default_risk_trailing_enabled=bool(defaults["default_risk_trailing_enabled"]),
                            allow_cancel_external_cli=allow_cancel_external_cli,
                            use_private_ws=bool(defaults["sync_use_private_ws"]),
                            use_executor_ws=bool(defaults["sync_use_executor_ws"]),
                            duration_sec=(
                                int(getattr(args, "duration_sec", 0))
                                if int(getattr(args, "duration_sec", 0)) > 0
                                else None
                            ),
                        )
                        if daemon_settings.use_private_ws:
                            ws_client = UpbitWebSocketPrivateClient(settings.websocket, credentials)
                            daemon_summary = asyncio.run(
                                run_live_sync_daemon_with_private_ws(
                                    store=store,
                                    client=client,
                                    ws_client=ws_client,
                                    settings=daemon_settings,
                                )
                            )
                        elif daemon_settings.use_executor_ws:
                            from .execution import GrpcExecutionGateway

                            with GrpcExecutionGateway(
                                host=str(defaults["executor_host"]),
                                port=int(defaults["executor_port"]),
                                timeout_sec=float(defaults["executor_timeout_sec"]),
                                insecure=bool(defaults["executor_insecure"]),
                            ) as executor_gateway:
                                daemon_summary = run_live_sync_daemon_with_executor_events(
                                    store=store,
                                    client=client,
                                    executor_gateway=executor_gateway,
                                    settings=daemon_settings,
                                )
                        else:
                            daemon_summary = run_live_sync_daemon(
                                store=store,
                                client=client,
                                settings=daemon_settings,
                            )
                        store.set_checkpoint(name="daemon_last_run", payload=daemon_summary)
                        _print_json(daemon_summary)
                        return 2 if bool(daemon_summary.get("halted")) else 0

                raise ValueError(f"Unsupported live command: {command}")
            finally:
                if lock_acquired:
                    store.release_run_lock(bot_id=bot_id)
    except (ConfigError, UpbitError, ValueError) as exc:
        print(f"[live][error] {exc}")
        return 2


def _handle_exec_command(args: argparse.Namespace, base_config: dict[str, Any]) -> int:
    defaults = _live_defaults(base_config)
    command = str(args.exec_command)
    try:
        from .execution import GrpcExecutionGateway

        with GrpcExecutionGateway(
            host=str(defaults["executor_host"]),
            port=int(defaults["executor_port"]),
            timeout_sec=float(defaults["executor_timeout_sec"]),
            insecure=bool(defaults["executor_insecure"]),
        ) as gateway:
            if command == "ping":
                _print_json(gateway.ping())
                return 0

            if command == "submit-test":
                result = gateway.submit_test(
                    market=str(args.market),
                    side=str(args.side),
                    price=float(args.price),
                    volume=float(args.volume),
                    identifier=getattr(args, "identifier", None),
                )
                _print_json(asdict(result))
                return 0 if result.accepted else 2

            raise ValueError(f"Unsupported exec command: {command}")
    except (RuntimeError, ValueError) as exc:
        print(f"[exec][error] {exc}")
        return 2


def _handle_upbit_public_command(args: argparse.Namespace, settings: Any) -> int:
    with UpbitHttpClient(settings) as http_client:
        client = UpbitPublicClient(http_client)
        if args.upbit_public_command == "markets":
            result = client.markets(is_details=bool(args.is_details))
        elif args.upbit_public_command == "ticker":
            markets = [item.strip().upper() for item in args.markets.split(",") if item.strip()]
            result = client.ticker(markets=markets)
        elif args.upbit_public_command == "candles":
            result = client.candles_minutes(
                market=args.market,
                tf_min=int(args.tf_min),
                count=int(args.count),
                to=args.to,
            )
        else:
            raise ValueError(f"Unsupported upbit public command: {args.upbit_public_command}")
    _print_json(result)
    return 0


def _handle_upbit_private_command(args: argparse.Namespace, settings: Any) -> int:
    credentials = require_upbit_credentials(settings)
    with UpbitHttpClient(settings, credentials=credentials) as http_client:
        client = UpbitPrivateClient(http_client)
        if args.upbit_private_command == "accounts":
            result = client.accounts()
        elif args.upbit_private_command == "chance":
            result = client.chance(market=args.market)
        elif args.upbit_private_command == "order-test":
            result = client.order_test(
                market=args.market,
                side=args.side,
                ord_type=args.ord_type,
                price=args.price,
                volume=args.volume,
                time_in_force=args.time_in_force,
                identifier=args.identifier,
            )
        else:
            raise ValueError(f"Unsupported upbit private command: {args.upbit_private_command}")
    _print_json(result)
    return 0


def _handle_upbit_ws_command(args: argparse.Namespace, settings: Any) -> int:
    if args.upbit_ws_command == "ticker":
        markets = [item.strip().upper() for item in args.markets.split(",") if item.strip()]
        if not markets:
            raise ValueError("markets is required")
        return asyncio.run(
            _run_ws_ticker(
                settings=settings,
                markets=markets,
                duration_sec=max(float(args.duration_sec), 1.0),
            )
        )

    if args.upbit_ws_command == "top20":
        quote = str(args.quote).strip().upper()
        markets = _load_quote_markets(settings, quote=quote)
        if not markets:
            print(f"[upbit][ws][top20] no markets found for quote={quote}")
            return 2
        return asyncio.run(
            _run_ws_top20(
                settings=settings,
                markets=markets,
                quote=quote,
                n=max(int(args.n), 1),
                print_every_sec=max(float(args.print_every_sec), 1.0),
                duration_sec=max(float(args.duration_sec), 1.0),
                include_caution=not bool(args.exclude_caution),
                include_inactive=not bool(args.exclude_inactive),
            )
        )

    raise ValueError(f"Unsupported upbit ws command: {args.upbit_ws_command}")


async def _run_ws_ticker(
    *,
    settings: Any,
    markets: list[str],
    duration_sec: float,
) -> int:
    ws_client = UpbitWebSocketPublicClient(settings.websocket)
    received = 0
    async for event in ws_client.stream_ticker(markets, duration_sec=duration_sec):
        received += 1
        print(
            f"[ws][ticker] market={event.market} ts_ms={event.ts_ms} "
            f"trade_price={event.trade_price} acc_trade_price_24h={event.acc_trade_price_24h}"
        )
    print(f"[ws][ticker] done duration_sec={duration_sec} received={received}")
    return 0 if received > 0 else 2


async def _run_ws_top20(
    *,
    settings: Any,
    markets: list[str],
    quote: str,
    n: int,
    print_every_sec: float,
    duration_sec: float,
    include_caution: bool,
    include_inactive: bool,
) -> int:
    ws_client = UpbitWebSocketPublicClient(settings.websocket)
    scanner = TopTradeValueScanner()
    started_at = time.monotonic()
    next_print_at = started_at + print_every_sec

    async for event in ws_client.stream_ticker(markets, duration_sec=duration_sec):
        scanner.update(event)
        now = time.monotonic()
        if now >= next_print_at:
            _print_top_snapshot(
                scanner=scanner,
                n=n,
                quote=quote,
                include_caution=include_caution,
                include_inactive=include_inactive,
            )
            next_print_at = now + print_every_sec

    _print_top_snapshot(
        scanner=scanner,
        n=n,
        quote=quote,
        include_caution=include_caution,
        include_inactive=include_inactive,
    )
    return 0 if scanner.size() > 0 else 2


def _load_quote_markets(settings: Any, *, quote: str) -> list[str]:
    quote_prefix = f"{quote}-"
    with UpbitHttpClient(settings) as http_client:
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


def _print_top_snapshot(
    *,
    scanner: TopTradeValueScanner,
    n: int,
    quote: str,
    include_caution: bool,
    include_inactive: bool,
) -> None:
    top_items = scanner.top_n(
        n=n,
        quote=quote,
        include_caution=include_caution,
        include_inactive=include_inactive,
    )
    if not top_items:
        print(f"[ws][top{n}] warming-up quote={quote} seen={scanner.size()}")
        return

    now_ms = int(time.time() * 1000)
    print(f"[ws][top{n}] ts_ms={now_ms} quote={quote} seen={scanner.size()} emit={len(top_items)}")
    for idx, item in enumerate(top_items, start=1):
        print(
            f"  {idx:02d} {item.market} "
            f"acc_trade_price_24h={item.acc_trade_price_24h:.2f} "
            f"trade_price={item.trade_price:.8f}"
        )


def _print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2))


def _ensure_upbit_runtime_available() -> None:
    if UpbitHttpClient is None or UpbitPublicClient is None or UpbitPrivateClient is None:
        raise ConfigError("Upbit REST runtime dependency missing. Install requirements from python/requirements.txt.")


def _load_base_config(config_dir: Path) -> dict[str, Any]:
    return _load_yaml_doc(config_dir / "base.yaml")


def _load_yaml_doc(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return {}
    return raw


def _paper_defaults(
    *,
    base_config: dict[str, Any],
    risk_doc: dict[str, Any],
    strategy_doc: dict[str, Any],
) -> dict[str, Any]:
    universe_base = base_config.get("universe", {}) if isinstance(base_config.get("universe"), dict) else {}
    storage_base = base_config.get("storage", {}) if isinstance(base_config.get("storage"), dict) else {}

    strategy_root = strategy_doc.get("strategy") if isinstance(strategy_doc.get("strategy"), dict) else strategy_doc
    strategy_root = strategy_root if isinstance(strategy_root, dict) else {}
    strategy_universe = strategy_root.get("universe", {}) if isinstance(strategy_root.get("universe"), dict) else {}
    candidates_cfg = (
        strategy_root.get("candidates_v1", {}) if isinstance(strategy_root.get("candidates_v1"), dict) else {}
    )
    execution_policy = (
        strategy_doc.get("execution_policy", {}) if isinstance(strategy_doc.get("execution_policy"), dict) else {}
    )

    risk_root = risk_doc.get("risk") if isinstance(risk_doc.get("risk"), dict) else risk_doc
    risk_root = risk_root if isinstance(risk_root, dict) else {}
    position_cfg = risk_doc.get("position", {}) if isinstance(risk_doc.get("position"), dict) else {}
    limits_cfg = risk_doc.get("limits", {}) if isinstance(risk_doc.get("limits"), dict) else {}

    return {
        "quote": str(strategy_universe.get("quote", universe_base.get("quote_currency", "KRW"))).strip().upper(),
        "top_n": int(
            strategy_universe.get(
                "top_n",
                universe_base.get("top_n_by_acc_trade_price_24h", 20),
            )
        ),
        "print_every_sec": float(5.0),
        "paper_out_dir": str(storage_base.get("paper_dir", "data/paper")),
        "universe_refresh_sec": float(strategy_universe.get("refresh_sec", 60)),
        "universe_hold_sec": float(strategy_universe.get("hold_sec", 120)),
        "momentum_window_sec": int(candidates_cfg.get("momentum_window_sec", 60)),
        "min_momentum_pct": float(candidates_cfg.get("min_momentum_pct", 0.2)),
        "starting_krw": float(risk_root.get("starting_krw", position_cfg.get("initial_capital_krw", 50000))),
        "per_trade_krw": float(risk_root.get("per_trade_krw", position_cfg.get("max_krw_per_position", 10000))),
        "max_positions": int(risk_root.get("max_positions", position_cfg.get("max_positions", 2))),
        "min_order_krw": float(risk_root.get("min_order_krw", position_cfg.get("min_order_krw", 5000))),
        "order_timeout_sec": float(
            risk_root.get("order_timeout_sec", execution_policy.get("order_timeout_sec", 20))
        ),
        "reprice_max_attempts": int(
            risk_root.get("reprice_max_attempts", execution_policy.get("reprice_max_attempts", 2))
        ),
        "cooldown_sec_after_fail": int(risk_root.get("cooldown_sec_after_fail", 60)),
        "max_consecutive_failures": int(
            risk_root.get("max_consecutive_failures", limits_cfg.get("consecutive_order_failures", 3))
        ),
    }


def _backtest_defaults(
    *,
    base_config: dict[str, Any],
    risk_doc: dict[str, Any],
    strategy_doc: dict[str, Any],
    backtest_doc: dict[str, Any],
) -> dict[str, Any]:
    storage_base = base_config.get("storage", {}) if isinstance(base_config.get("storage"), dict) else {}
    data_defaults = _data_defaults(base_config)

    risk_root = risk_doc.get("risk") if isinstance(risk_doc.get("risk"), dict) else risk_doc
    risk_root = risk_root if isinstance(risk_root, dict) else {}
    position_cfg = risk_doc.get("position", {}) if isinstance(risk_doc.get("position"), dict) else {}

    strategy_root = strategy_doc.get("strategy") if isinstance(strategy_doc.get("strategy"), dict) else strategy_doc
    strategy_root = strategy_root if isinstance(strategy_root, dict) else {}
    strategy_universe = strategy_root.get("universe", {}) if isinstance(strategy_root.get("universe"), dict) else {}
    candidates_cfg = (
        strategy_root.get("candidates_v1", {}) if isinstance(strategy_root.get("candidates_v1"), dict) else {}
    )

    root = backtest_doc.get("backtest", backtest_doc) if isinstance(backtest_doc, dict) else {}
    root = root if isinstance(root, dict) else {}
    universe_cfg = root.get("universe", {}) if isinstance(root.get("universe"), dict) else {}
    data_cfg = root.get("data", {}) if isinstance(root.get("data"), dict) else {}
    execution_cfg = root.get("execution", {}) if isinstance(root.get("execution"), dict) else {}
    output_cfg = root.get("output", {}) if isinstance(root.get("output"), dict) else {}

    return {
        "dataset_name": str(root.get("dataset_name", data_defaults["dataset_name"])),
        "parquet_root": str(root.get("parquet_root", data_defaults["parquet_root"])),
        "tf": str(root.get("tf", "1m")).strip().lower(),
        "from_ts_ms": root.get("from_ts_ms"),
        "to_ts_ms": root.get("to_ts_ms"),
        "duration_days": root.get("duration_days"),
        "universe_mode": str(universe_cfg.get("mode", "static_start")).strip().lower(),
        "quote": str(universe_cfg.get("quote", strategy_universe.get("quote", "KRW"))).strip().upper(),
        "top_n": int(universe_cfg.get("top_n", strategy_universe.get("top_n", 20))),
        "dense_grid": bool(data_cfg.get("dense_grid", False)),
        "starting_krw": float(risk_root.get("starting_krw", position_cfg.get("initial_capital_krw", 50000))),
        "per_trade_krw": float(risk_root.get("per_trade_krw", position_cfg.get("max_krw_per_position", 10000))),
        "max_positions": int(risk_root.get("max_positions", position_cfg.get("max_positions", 2))),
        "min_order_krw": float(risk_root.get("min_order_krw", position_cfg.get("min_order_krw", 5000))),
        "order_timeout_bars": int(execution_cfg.get("order_timeout_bars", 5)),
        "reprice_max_attempts": int(execution_cfg.get("reprice_max_attempts", 1)),
        "reprice_tick_steps": int(execution_cfg.get("reprice_tick_steps", 1)),
        "rules_ttl_sec": int(execution_cfg.get("rules_ttl_sec", 86400)),
        "momentum_window_sec": int(candidates_cfg.get("momentum_window_sec", 60)),
        "min_momentum_pct": float(candidates_cfg.get("min_momentum_pct", 0.2)),
        "backtest_out_dir": str(output_cfg.get("root", storage_base.get("backtest_dir", "data/backtest"))),
        "seed": int(root.get("seed", 0)),
    }


def _live_defaults(base_config: dict[str, Any]) -> dict[str, Any]:
    live_cfg = base_config.get("live", {}) if isinstance(base_config.get("live"), dict) else {}
    state_cfg = live_cfg.get("state", {}) if isinstance(live_cfg.get("state"), dict) else {}
    startup_cfg = live_cfg.get("startup", {}) if isinstance(live_cfg.get("startup"), dict) else {}
    sync_cfg = live_cfg.get("sync", {}) if isinstance(live_cfg.get("sync"), dict) else {}
    executor_cfg = live_cfg.get("executor", {}) if isinstance(live_cfg.get("executor"), dict) else {}
    orders_cfg = live_cfg.get("orders", {}) if isinstance(live_cfg.get("orders"), dict) else {}
    default_risk_cfg = live_cfg.get("default_risk", {}) if isinstance(live_cfg.get("default_risk"), dict) else {}
    live_risk_cfg = live_cfg.get("risk", {}) if isinstance(live_cfg.get("risk"), dict) else {}
    universe_cfg = base_config.get("universe", {}) if isinstance(base_config.get("universe"), dict) else {}

    unknown_open_orders_policy = str(startup_cfg.get("unknown_open_orders_policy", "halt")).strip().lower()
    if unknown_open_orders_policy not in {"halt", "ignore", "cancel"}:
        unknown_open_orders_policy = "halt"

    unknown_positions_policy = str(startup_cfg.get("unknown_positions_policy", "halt")).strip().lower()
    if unknown_positions_policy not in {"halt", "import_as_unmanaged", "attach_default_risk"}:
        unknown_positions_policy = "halt"

    return {
        "enabled": bool(live_cfg.get("enabled", False)),
        "bot_id": str(live_cfg.get("bot_id", "autobot-001")).strip().lower(),
        "state_db_path": str(state_cfg.get("db_path", "data/state/live_state.db")),
        "state_run_lock": bool(state_cfg.get("run_lock", True)),
        "startup_reconcile": bool(startup_cfg.get("reconcile", True)),
        "unknown_open_orders_policy": unknown_open_orders_policy,
        "unknown_positions_policy": unknown_positions_policy,
        "allow_cancel_external_orders": bool(startup_cfg.get("allow_cancel_external_orders", False)),
        "sync_poll_interval_sec": max(int(sync_cfg.get("poll_interval_sec", 15)), 1),
        "sync_use_private_ws": bool(sync_cfg.get("use_private_ws", False)),
        "sync_use_executor_ws": bool(sync_cfg.get("use_executor_ws", False)),
        "executor_host": str(executor_cfg.get("host", "127.0.0.1")).strip(),
        "executor_port": max(int(executor_cfg.get("port", 50051)), 1),
        "executor_timeout_sec": max(float(executor_cfg.get("timeout_sec", 5.0)), 0.1),
        "executor_insecure": bool(executor_cfg.get("insecure", True)),
        "identifier_prefix": str(orders_cfg.get("identifier_prefix", "AUTOBOT")).strip().upper(),
        "default_risk_sl_pct": max(float(default_risk_cfg.get("sl_pct", 2.0)), 0.0),
        "default_risk_tp_pct": max(float(default_risk_cfg.get("tp_pct", 3.0)), 0.0),
        "default_risk_trailing_enabled": bool(default_risk_cfg.get("trailing_enabled", False)),
        "risk_enabled": bool(live_risk_cfg.get("enabled", False)),
        "risk_exit_aggress_bps": max(float(live_risk_cfg.get("exit_aggress_bps", 8.0)), 0.0),
        "risk_timeout_sec": max(int(live_risk_cfg.get("timeout_sec", 20)), 1),
        "risk_replace_max": max(int(live_risk_cfg.get("replace_max", 2)), 0),
        "risk_default_trail_pct": max(float(live_risk_cfg.get("default_trail_pct", 1.0)), 0.0),
        "quote_currency": str(universe_cfg.get("quote_currency", "KRW")).strip().upper(),
    }


def _load_micro_defaults(*, config_dir: Path, base_config: dict[str, Any]) -> dict[str, Any]:
    storage_cfg = base_config.get("storage", {}) if isinstance(base_config.get("storage"), dict) else {}
    data_cfg = base_config.get("data", {}) if isinstance(base_config.get("data"), dict) else {}
    parquet_root = str(data_cfg.get("parquet_root", storage_cfg.get("parquet_dir", "data/parquet")))

    micro_doc = _load_yaml_doc(config_dir / "micro.yaml")
    root = micro_doc.get("micro", {}) if isinstance(micro_doc.get("micro"), dict) else {}
    validation_cfg = root.get("validation", {}) if isinstance(root.get("validation"), dict) else {}

    return {
        "parquet_root": parquet_root,
        "quote": str(root.get("quote", "KRW")).strip().upper(),
        "top_n": int(root.get("top_n", 20)),
        "raw_ticks_root": str(root.get("raw_ticks_root", "data/raw_ticks/upbit/trades")),
        "raw_ws_root": str(root.get("raw_ws_root", "data/raw_ws/upbit/quotation")),
        "out_root": str(root.get("out_root", "data/parquet/micro_v1")),
        "base_candles_dataset": str(root.get("base_candles_dataset", "candles_v1")).strip(),
        "join_match_warn": float(validation_cfg.get("join_match_warn", 0.98)),
        "join_match_fail": float(validation_cfg.get("join_match_fail", 0.90)),
        "micro_available_warn": float(validation_cfg.get("micro_available_warn", 0.10)),
        "volume_fail_ratio": float(validation_cfg.get("volume_fail_ratio", 0.001)),
        "price_fail_ratio": float(validation_cfg.get("price_fail_ratio", 0.001)),
    }


def _resolve_base_candles_path(*, base_candles: str | None, default_dataset: str, parquet_root: Path) -> Path:
    value = str(base_candles or default_dataset).strip()
    candidate = Path(value)
    if candidate.exists():
        return candidate
    if candidate.is_absolute():
        return candidate
    return parquet_root / value


def _data_defaults(config: dict[str, Any]) -> dict[str, Any]:
    data_cfg = config.get("data", {}) if isinstance(config.get("data"), dict) else {}
    ingest_cfg = data_cfg.get("ingest", {}) if isinstance(data_cfg.get("ingest"), dict) else {}
    duckdb_cfg = ingest_cfg.get("duckdb", {}) if isinstance(ingest_cfg.get("duckdb"), dict) else {}
    qa_cfg = data_cfg.get("qa", {}) if isinstance(data_cfg.get("qa"), dict) else {}
    storage_cfg = config.get("storage", {}) if isinstance(config.get("storage"), dict) else {}

    return {
        "raw_dir": str(data_cfg.get("raw_dir", storage_cfg.get("raw_dir", "data/raw"))),
        "parquet_root": str(data_cfg.get("parquet_root", storage_cfg.get("parquet_dir", "data/parquet"))),
        "dataset_name": str(data_cfg.get("dataset_name", "candles_v1")),
        "pattern": str(data_cfg.get("file_pattern", "upbit_*_full.csv")),
        "ingest": {
            "engine": str(ingest_cfg.get("engine", "duckdb")),
            "mode": str(ingest_cfg.get("mode", data_cfg.get("mode", "skip_unchanged"))),
            "workers": int(ingest_cfg.get("workers", data_cfg.get("ingest_workers", 1))),
            "compression": str(ingest_cfg.get("compression", data_cfg.get("default_compression", "zstd"))),
            "allow_sort_on_non_monotonic": bool(
                ingest_cfg.get("allow_sort_on_non_monotonic", data_cfg.get("allow_sort_on_non_monotonic", True))
            ),
            "allow_dedupe_on_duplicate_ts": bool(
                ingest_cfg.get("allow_dedupe_on_duplicate_ts", data_cfg.get("allow_dedupe_on_duplicate_ts", True))
            ),
            "quote_volume_policy": str(
                ingest_cfg.get("quote_volume_policy", data_cfg.get("quote_volume_policy", "estimate_if_missing"))
            ),
            "duckdb": {
                "temp_directory": str(duckdb_cfg.get("temp_directory", "D:/MyApps/Autobot/data/cache/duckdb_tmp")),
                "memory_limit": str(duckdb_cfg.get("memory_limit", "6GB")),
                "threads": int(duckdb_cfg.get("threads", 2)),
                "fail_if_temp_not_set": bool(duckdb_cfg.get("fail_if_temp_not_set", True)),
            },
        },
        "qa": {
            "gap_severity": str(qa_cfg.get("gap_severity", "info")).strip().lower(),
            "quote_est_severity": str(qa_cfg.get("quote_est_severity", "info")).strip().lower(),
            "ohlc_violation_policy": str(qa_cfg.get("ohlc_violation_policy", "drop_row_and_warn")).strip().lower(),
        },
    }


def _parse_csv_list(value: str | None, normalize: Callable[[str], str]) -> tuple[str, ...] | None:
    if value is None:
        return None
    items = tuple(normalize(item.strip()) for item in value.split(",") if item.strip())
    return items or None


def _parse_days_ago_csv(value: str | None, *, default: tuple[int, ...]) -> tuple[int, ...]:
    if value is None:
        return tuple(default)
    values: list[int] = []
    seen: set[int] = set()
    for token in str(value).split(","):
        text = token.strip()
        if not text:
            continue
        day = int(text)
        if day < 1 or day > 7:
            raise ValueError("days_ago must be between 1 and 7")
        if day in seen:
            continue
        seen.add(day)
        values.append(day)
    if not values:
        return tuple(default)
    return tuple(sorted(values))


def _parse_orderbook_level_arg(value: Any) -> int | str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return text


def _parse_bool_arg(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {value}")


if __name__ == "__main__":
    raise SystemExit(main())
