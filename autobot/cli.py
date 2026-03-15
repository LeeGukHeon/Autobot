"""Command line interface for Upbit AutoBot."""

from __future__ import annotations

import asyncio
import argparse
from dataclasses import asdict
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from typing import Any, Callable

import yaml

from . import __version__
from . import cli_model_helpers as _cli_model_helpers
from . import cli_train_v4_helpers as _cli_train_v4_helpers
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
    WsPublicDaemonOptions,
    WsPublicPlanOptions,
    collect_candles_from_plan,
    collect_ws_public_daemon,
    collect_ticks_from_plan,
    collect_ticks_stats,
    collect_ws_public_from_plan,
    collect_ws_public_stats,
    generate_candle_topup_plan,
    generate_ticks_collection_plan,
    generate_ws_public_collection_plan,
    load_ws_public_status,
    purge_ws_public_retention,
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
    FeatureBuildV2Options,
    FeatureBuildV3Options,
    FeatureBuildV4Options,
    FeatureValidateOptions,
    FeatureValidateV2Options,
    FeatureValidateV3Options,
    FeatureValidateV4Options,
    build_features_dataset,
    build_features_dataset_v2,
    build_features_dataset_v3,
    build_features_dataset_v4,
    features_stats,
    features_stats_v2,
    features_stats_v3,
    features_stats_v4,
    load_features_config,
    load_features_v2_config,
    load_features_v3_config,
    load_features_v4_config,
    sample_features,
    validate_features_dataset,
    validate_features_dataset_v2,
    validate_features_dataset_v3,
    validate_features_dataset_v4,
)
from .features.feature_spec import parse_date_to_ts_ms
from .live.breakers import ACTION_FULL_KILL_SWITCH, ACTION_HALT_AND_CANCEL_BOT_ORDERS, ACTION_HALT_NEW_INTENTS, arm_breaker, breaker_status, clear_breaker
from .live import (
    DEFAULT_LIVE_TARGET_UNIT,
    DEFAULT_ROLLOUT_MODE,
    LiveDaemonSettings,
    LiveModelAlphaRuntimeSettings,
    LiveStateStore,
    apply_cancel_actions,
    build_live_runtime_sync_status,
    build_live_admissibility_report,
    build_rollout_contract,
    build_rollout_disarmed_contract,
    build_rollout_test_order_record,
    build_live_order_admissibility_snapshot,
    evaluate_live_limit_order,
    evaluate_live_rollout_gate,
    hash_arm_token,
    load_ws_public_runtime_contract,
    reconcile_exchange_snapshot,
    resolve_rollout_gate_inputs,
    resolve_live_runtime_model_contract,
    rollout_gate_to_payload,
    rollout_latest_artifact_path,
    write_rollout_latest,
    run_live_model_alpha_runtime,
    run_live_sync_daemon,
    run_live_sync_daemon_with_executor_events,
    run_live_sync_daemon_with_private_ws,
)
from .live.small_account import (
    build_small_account_runtime_report,
    derive_volume_from_target_notional,
    record_small_account_decision,
    sizing_envelope_to_payload,
)
from .paper import PaperRunSettings, run_live_paper_sync
from .models import (
    AblationOptions,
    MetricAuditOptions,
    ModelBtProxyOptions,
    TrainRunOptions,
    TrainV2MicroOptions,
    TrainV3MtfMicroOptions,
    audit_registered_model,
    compare_registered_models,
    evaluate_registered_model_window,
    list_registered_models,
    load_train_defaults,
    run_ablation,
    run_modelbt_proxy,
    show_registered_model,
    train_and_register,
    train_and_register_v2_micro,
    train_and_register_v3_mtf_micro,
    train_and_register_v4_crypto_cs,
)
from .models.registry import promote_run_to_champion
from .strategy import TopTradeValueScanner
from .strategy.model_alpha_v1 import (
    ModelAlphaExecutionSettings,
    ModelAlphaExitSettings,
    ModelAlphaOperationalSettings,
    ModelAlphaPositionSettings,
    ModelAlphaSelectionSettings,
    ModelAlphaSettings,
)
from .strategy.micro_gate_v1 import (
    MicroGateBookSettings,
    MicroGateSettings,
    MicroGateTradeSettings,
)
from .strategy.micro_order_policy import (
    MicroOrderPolicySafetySettings,
    MicroOrderPolicySettings,
    MicroOrderPolicyTieringSettings,
    MicroOrderPolicyTiersSettings,
    MicroOrderPolicyTierSettings,
)
from .strategy.micro_snapshot import LiveWsProviderSettings
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


DEFAULT_MODEL_ALPHA_RUNTIME_REF = "champion_v4"
DEFAULT_V3_CANDIDATE_REF = "latest_candidate_v3"
DEFAULT_V4_RUNTIME_REF = "champion_v4"
DEFAULT_V4_CANDIDATE_REF = "latest_candidate_v4"
DEFAULT_PAPER_ALPHA_PRESET = "live_v4"


def _paper_alpha_preset_overrides(preset: str) -> dict[str, Any]:
    return _cli_model_helpers.paper_alpha_preset_overrides(preset)


def _normalize_paper_alpha_args(args: argparse.Namespace) -> argparse.Namespace:
    return _cli_model_helpers.normalize_paper_alpha_args(args)


def _load_registry_pointer_payload(path: Path) -> dict[str, Any]:
    return _cli_model_helpers.load_registry_pointer_payload(path)


def _resolve_v4_runtime_model_ref_fallback(
    model_ref: str,
    model_family: str | None,
    registry_root: Path,
) -> tuple[str, str | None, str | None]:
    return _cli_model_helpers.resolve_v4_runtime_model_ref_fallback(model_ref, model_family, registry_root)


def _backtest_alpha_preset_overrides(preset: str) -> dict[str, Any]:
    return _cli_model_helpers.backtest_alpha_preset_overrides(preset)


def _normalize_backtest_alpha_args(args: argparse.Namespace) -> argparse.Namespace:
    return _cli_model_helpers.normalize_backtest_alpha_args(args)


def _selection_use_learned_recommendations(
    *,
    args: argparse.Namespace,
    model_alpha_selection_defaults: dict[str, Any],
) -> bool:
    override = getattr(args, "use_learned_selection_recommendations", None)
    if override is not None:
        return bool(override)
    if getattr(args, "top_pct", None) is not None or getattr(args, "min_cands_per_ts", None) is not None:
        return False
    return bool(model_alpha_selection_defaults.get("use_learned_recommendations", True))


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
    collect_ws_public_run_parser.add_argument("--raw-root", default="data/raw_ws/upbit/public")
    collect_ws_public_run_parser.add_argument("--meta-dir", default="data/raw_ws/upbit/_meta")
    collect_ws_public_run_parser.add_argument("--duration-sec", type=int, default=120)
    collect_ws_public_run_parser.add_argument("--rotate-sec", type=int, default=300)
    collect_ws_public_run_parser.add_argument("--max-bytes", type=int, default=67_108_864)
    collect_ws_public_run_parser.add_argument("--retention-days", type=int, default=7)
    collect_ws_public_run_parser.add_argument("--rate-limit-strict", default="true", help="true|false")
    collect_ws_public_run_parser.add_argument("--reconnect-max-per-min", type=int, default=3)
    collect_ws_public_run_parser.add_argument("--orderbook-spread-bps-threshold", type=float, default=0.5)
    collect_ws_public_run_parser.add_argument("--orderbook-top1-size-change-threshold", type=float, default=0.2)
    collect_ws_public_run_parser.add_argument(
        "--keepalive-mode",
        default="auto",
        choices=("message", "frame", "auto", "off"),
        help="keepalive mode",
    )
    collect_ws_public_run_parser.add_argument("--keepalive-interval-sec", type=int, default=60)
    collect_ws_public_run_parser.add_argument("--keepalive-stale-sec", type=int, default=120)

    collect_ws_public_daemon_parser = collect_ws_public_subparsers.add_parser(
        "daemon",
        help="Run ws-public daemon with periodic top-N refresh and health snapshots.",
    )
    collect_ws_public_daemon_parser.add_argument("--raw-root", default="data/raw_ws/upbit/public")
    collect_ws_public_daemon_parser.add_argument("--meta-dir", default="data/raw_ws/upbit/_meta")
    collect_ws_public_daemon_parser.add_argument("--quote", default="KRW")
    collect_ws_public_daemon_parser.add_argument("--top-n", type=int, default=50)
    collect_ws_public_daemon_parser.add_argument("--refresh-sec", type=int, default=900)
    collect_ws_public_daemon_parser.add_argument(
        "--duration-sec",
        type=int,
        default=0,
        help="0 means long-running (effectively unbounded).",
    )
    collect_ws_public_daemon_parser.add_argument("--retention-days", type=int, default=30)
    collect_ws_public_daemon_parser.add_argument("--downsample-hz", type=float, default=1.0)
    collect_ws_public_daemon_parser.add_argument("--max-markets", type=int, default=60)
    collect_ws_public_daemon_parser.add_argument(
        "--format",
        default="DEFAULT",
        choices=("DEFAULT", "SIMPLE", "JSON_LIST", "SIMPLE_LIST"),
    )
    collect_ws_public_daemon_parser.add_argument("--channels", default="trade,orderbook")
    collect_ws_public_daemon_parser.add_argument("--orderbook-topk", type=int, default=5)
    collect_ws_public_daemon_parser.add_argument("--orderbook-level", default="0")
    collect_ws_public_daemon_parser.add_argument("--rotate-sec", type=int, default=3600)
    collect_ws_public_daemon_parser.add_argument("--max-bytes", type=int, default=67_108_864)
    collect_ws_public_daemon_parser.add_argument("--rate-limit-strict", default="true", help="true|false")
    collect_ws_public_daemon_parser.add_argument("--reconnect-max-per-min", type=int, default=3)
    collect_ws_public_daemon_parser.add_argument("--max-subscribe-messages-per-min", type=int, default=100)
    collect_ws_public_daemon_parser.add_argument("--min-subscribe-interval-sec", type=int, default=60)
    collect_ws_public_daemon_parser.add_argument("--orderbook-spread-bps-threshold", type=float, default=0.5)
    collect_ws_public_daemon_parser.add_argument("--orderbook-top1-size-change-threshold", type=float, default=0.2)
    collect_ws_public_daemon_parser.add_argument(
        "--keepalive-mode",
        default="message",
        choices=("message", "frame", "auto", "off"),
    )
    collect_ws_public_daemon_parser.add_argument("--keepalive-interval-sec", type=int, default=55)
    collect_ws_public_daemon_parser.add_argument("--keepalive-stale-sec", type=int, default=120)
    collect_ws_public_daemon_parser.add_argument("--health-update-sec", type=int, default=5)

    collect_ws_public_validate_parser = collect_ws_public_subparsers.add_parser(
        "validate",
        help="Validate collected public websocket dataset.",
    )
    collect_ws_public_validate_parser.add_argument("--date", help="Filter date partition YYYY-MM-DD")
    collect_ws_public_validate_parser.add_argument("--raw-root", default="data/raw_ws/upbit/public")
    collect_ws_public_validate_parser.add_argument("--meta-dir", default="data/raw_ws/upbit/_meta")
    collect_ws_public_validate_parser.add_argument("--quarantine-corrupt", default="false", help="true|false")
    collect_ws_public_validate_parser.add_argument("--quarantine-dir", default="data/raw_ws/upbit/_quarantine")
    collect_ws_public_validate_parser.add_argument("--min-age-sec", type=int, default=300)

    collect_ws_public_stats_parser = collect_ws_public_subparsers.add_parser(
        "stats",
        help="Show collected public websocket stats.",
    )
    collect_ws_public_stats_parser.add_argument("--date", help="Filter date partition YYYY-MM-DD")
    collect_ws_public_stats_parser.add_argument("--raw-root", default="data/raw_ws/upbit/public")
    collect_ws_public_stats_parser.add_argument("--meta-dir", default="data/raw_ws/upbit/_meta")

    collect_ws_public_status_parser = collect_ws_public_subparsers.add_parser(
        "status",
        help="Show ws-public daemon health snapshot and latest run summary.",
    )
    collect_ws_public_status_parser.add_argument("--raw-root", default="data/raw_ws/upbit/public")
    collect_ws_public_status_parser.add_argument("--meta-dir", default="data/raw_ws/upbit/_meta")

    collect_ws_public_purge_parser = collect_ws_public_subparsers.add_parser(
        "purge",
        help="Purge old ws-public raw partitions by retention policy.",
    )
    collect_ws_public_purge_parser.add_argument("--raw-root", default="data/raw_ws/upbit/public")
    collect_ws_public_purge_parser.add_argument("--meta-dir", default="data/raw_ws/upbit/_meta")
    collect_ws_public_purge_parser.add_argument("--retention-days", type=int, required=True)

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
    micro_aggregate_parser.add_argument("--raw-ws-root", default="data/raw_ws/upbit/public")
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

    features_build_parser = features_subparsers.add_parser("build", help="Build features dataset (v1/v2/v3).")
    features_build_parser.add_argument("--tf", required=True, help="Timeframe, ex: 5m")
    features_build_parser.add_argument("--quote", help="Quote filter, ex: KRW")
    features_build_parser.add_argument("--top-n", type=int, help="Universe size")
    features_build_parser.add_argument("--start", help="Start date YYYY-MM-DD")
    features_build_parser.add_argument("--end", help="End date YYYY-MM-DD")
    features_build_parser.add_argument("--feature-set", default="v1", choices=("v1", "v2", "v3", "v4"))
    features_build_parser.add_argument("--label-set", default="v1", choices=("v1", "v2"))
    features_build_parser.add_argument("--workers", type=int, default=1)
    features_build_parser.add_argument("--base-candles", help="Base candles dataset/path for v2/v3, ex: auto|candles_api_v1")
    features_build_parser.add_argument("--micro-dataset", help="Micro dataset/path for v2/v3, ex: micro_v1")
    features_build_parser.add_argument("--require-micro", help="Require m_micro_available for v2 (true|false)")
    features_build_parser.add_argument("--min-trade-events", type=int, help="v2 micro filter threshold")
    features_build_parser.add_argument("--min-trade-coverage-ms", type=int, help="v2 micro filter threshold")
    features_build_parser.add_argument("--min-book-events", type=int, help="v2 micro filter threshold")
    features_build_parser.add_argument("--min-book-coverage-ms", type=int, help="v2 micro filter threshold")
    features_build_parser.add_argument(
        "--use-precomputed-features-v1",
        help="Use existing features_v1 for mode B in v2 (true|false)",
    )
    features_build_parser.add_argument("--dry-run", default="false", help="v2/v3 preflight-only run (true|false)")
    features_build_parser.add_argument(
        "--fail-on-warn",
        default="false",
        help="Return non-zero when warnings exist (true|false).",
    )

    features_validate_parser = features_subparsers.add_parser("validate", help="Validate built feature dataset.")
    features_validate_parser.add_argument("--tf", required=True, help="Timeframe, ex: 5m")
    features_validate_parser.add_argument("--quote", help="Quote filter, ex: KRW")
    features_validate_parser.add_argument("--top-n", type=int, help="Universe size")
    features_validate_parser.add_argument("--start", help="Optional start date YYYY-MM-DD (v3)")
    features_validate_parser.add_argument("--end", help="Optional end date YYYY-MM-DD (v3)")
    features_validate_parser.add_argument("--feature-set", default="v1", choices=("v1", "v2", "v3", "v4"))
    features_validate_parser.add_argument("--join-match-warn", type=float, help="v2 join match warn threshold")
    features_validate_parser.add_argument("--join-match-fail", type=float, help="v2 join match fail threshold")

    features_sample_parser = features_subparsers.add_parser("sample", help="Print feature rows for one market.")
    features_sample_parser.add_argument("--tf", required=True, help="Timeframe, ex: 5m")
    features_sample_parser.add_argument("--market", required=True, help="Market, ex: KRW-BTC")
    features_sample_parser.add_argument("--rows", type=int, default=10)

    features_stats_parser = features_subparsers.add_parser("stats", help="Show feature dataset summary.")
    features_stats_parser.add_argument("--tf", default="5m", help="Timeframe, ex: 5m")
    features_stats_parser.add_argument("--quote", help="Quote filter, ex: KRW")
    features_stats_parser.add_argument("--top-n", type=int, help="Universe size")
    features_stats_parser.add_argument("--feature-set", default="v1", choices=("v1", "v2", "v3", "v4"))

    model_parser = subparsers.add_parser("model", help="Model training and registry operations.")
    model_subparsers = model_parser.add_subparsers(dest="model_command", required=True)

    model_train_parser = model_subparsers.add_parser("train", help="Train baseline+booster and register champion.")
    model_train_parser.add_argument("--trainer", default="v1", choices=("v1", "v2_micro", "v3_mtf_micro", "v4_crypto_cs"))
    model_train_parser.add_argument("--tf", help="Timeframe, ex: 5m")
    model_train_parser.add_argument("--quote", help="Quote filter, ex: KRW")
    model_train_parser.add_argument("--top-n", type=int, help="Universe size")
    model_train_parser.add_argument("--start", help="Start date YYYY-MM-DD")
    model_train_parser.add_argument("--end", help="End date YYYY-MM-DD")
    model_train_parser.add_argument("--feature-set", default="v1", choices=("v1", "v2", "v3", "v4"))
    model_train_parser.add_argument("--label-set", default="v1", choices=("v1", "v2"))
    model_train_parser.add_argument("--task", default="cls", choices=("cls", "reg", "rank"))
    model_train_parser.add_argument("--model-family", help="Registry family, ex: train_v1")
    model_train_parser.add_argument("--run-baseline", default="true", help="Enable baseline track (true|false).")
    model_train_parser.add_argument("--run-booster", default="true", help="Enable booster track (true|false).")
    model_train_parser.add_argument("--booster-sweep-trials", type=int)
    model_train_parser.add_argument("--seed", type=int)
    model_train_parser.add_argument("--nthread", type=int)
    model_train_parser.add_argument(
        "--execution-acceptance-top-n",
        type=int,
        help="Override trainer-internal execution acceptance universe size.",
    )
    model_train_parser.add_argument(
        "--execution-acceptance-top-pct",
        type=float,
        help="Override trainer-internal execution acceptance top_pct for model_alpha_v1.",
    )
    model_train_parser.add_argument(
        "--execution-acceptance-min-prob",
        type=float,
        help="Override trainer-internal execution acceptance min_prob for model_alpha_v1.",
    )
    model_train_parser.add_argument(
        "--execution-acceptance-min-cands-per-ts",
        type=int,
        help="Override trainer-internal execution acceptance min_candidates_per_ts for model_alpha_v1.",
    )
    model_train_parser.add_argument(
        "--execution-acceptance-hold-bars",
        type=int,
        help="Override trainer-internal execution acceptance hold_bars for model_alpha_v1.",
    )
    model_train_parser.add_argument(
        "--cpcv-lite",
        action="store_true",
        help="Enable research-only CPCV-lite summary for trainer=v4_crypto_cs.",
    )
    model_train_parser.add_argument(
        "--cpcv-lite-group-count",
        type=int,
        help="Override CPCV-lite contiguous time-group count.",
    )
    model_train_parser.add_argument(
        "--cpcv-lite-test-groups",
        type=int,
        help="Override CPCV-lite held-out group count per fold.",
    )
    model_train_parser.add_argument(
        "--cpcv-lite-max-combinations",
        type=int,
        help="Override CPCV-lite maximum evaluated combinations.",
    )
    model_train_parser.add_argument(
        "--factor-block-selection-mode",
        choices=("off", "report_only", "use_latest", "guarded_auto"),
        help="v4 factor block selector mode: off|report_only|use_latest|guarded_auto.",
    )
    model_train_parser.add_argument("--run-scope", help=argparse.SUPPRESS)

    model_daily_v4_parser = model_subparsers.add_parser(
        "daily-v4",
        help="Run the same v4 daily acceptance pipeline manually without mutating scheduled daily state.",
    )
    model_daily_v4_parser.add_argument(
        "--mode",
        choices=("spawn_only", "combined", "promote_only"),
        default="spawn_only",
        help="Orchestration mode. Default spawn_only.",
    )
    model_daily_v4_parser.add_argument(
        "--lane",
        choices=("cls_scout", "rank_shadow"),
        default="cls_scout",
        help="Explicit lane wrapper to run. Default cls_scout.",
    )
    model_daily_v4_parser.add_argument("--batch-date", help="Batch date YYYY-MM-DD. Default: yesterday.")
    model_daily_v4_parser.add_argument(
        "--run-paper-soak",
        action="store_true",
        help="Run paper soak instead of the timer-default skip behavior.",
    )
    model_daily_v4_parser.add_argument(
        "--paper-soak-duration-sec",
        type=int,
        help="Override paper soak duration in seconds. Implies --run-paper-soak.",
    )
    model_daily_v4_parser.add_argument("--dry-run", action="store_true")

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

    model_promote_parser = model_subparsers.add_parser("promote", help="Promote a registered model run to champion.")
    model_promote_parser.add_argument(
        "--model-ref",
        default=DEFAULT_V3_CANDIDATE_REF,
        help="latest_candidate_v3|latest_candidate|latest|run_id|run_dir",
    )
    model_promote_parser.add_argument("--model-family", help="Registry family, ex: train_v3_mtf_micro")
    model_promote_parser.add_argument("--score-key", default="test_precision_top5", help="Leaderboard metric key to record")

    model_compare_parser = model_subparsers.add_parser("compare", help="Compare two registered models on same window.")
    model_compare_parser.add_argument("--a", required=True, help="Model ref A, ex: latest_v1")
    model_compare_parser.add_argument("--b", required=True, help="Model ref B, ex: latest_v2")
    model_compare_parser.add_argument("--a-family", help="Optional model family for A")
    model_compare_parser.add_argument("--b-family", help="Optional model family for B")
    model_compare_parser.add_argument("--split", default="test", choices=("train", "valid", "test"))
    model_compare_parser.add_argument("--start", help="Start date YYYY-MM-DD")
    model_compare_parser.add_argument("--end", help="End date YYYY-MM-DD")

    model_audit_parser = model_subparsers.add_parser("audit", help="Audit registered model metrics against sklearn.")
    model_audit_parser.add_argument("--model-ref", default="latest", help="latest|champion|run_id|run_dir")
    model_audit_parser.add_argument("--model-family", help="Registry family, ex: train_v2_micro")
    model_audit_parser.add_argument("--split", default="test", choices=("train", "valid", "test"))
    model_audit_parser.add_argument("--start", help="Start date YYYY-MM-DD")
    model_audit_parser.add_argument("--end", help="End date YYYY-MM-DD")
    model_audit_parser.add_argument("--tolerance-warn", type=float, default=1e-6)
    model_audit_parser.add_argument("--tolerance-fail", type=float, default=1e-3)

    model_ablate_parser = model_subparsers.add_parser("ablate", help="Run A0~A4 feature ablations on v2 dataset.")
    model_ablate_parser.add_argument("--feature-set", default="v2", choices=("v2",))
    model_ablate_parser.add_argument("--tf", help="Timeframe, ex: 5m")
    model_ablate_parser.add_argument("--quote", help="Quote filter, ex: KRW")
    model_ablate_parser.add_argument("--top-n", type=int, help="Universe size")
    model_ablate_parser.add_argument("--start", help="Start date YYYY-MM-DD")
    model_ablate_parser.add_argument("--end", help="End date YYYY-MM-DD")
    model_ablate_parser.add_argument("--label-set", default="v1", choices=("v1",))
    model_ablate_parser.add_argument("--ablations", default="A0,A1,A2,A3,A4", help="Comma list, ex: A0,A1,A2,A3,A4")
    model_ablate_parser.add_argument("--booster-sweep-trials", type=int, default=30)
    model_ablate_parser.add_argument("--seed", type=int)
    model_ablate_parser.add_argument("--nthread", type=int)

    modelbt_parser = subparsers.add_parser("modelbt", help="Fast model-signal backtest proxy.")
    modelbt_subparsers = modelbt_parser.add_subparsers(dest="modelbt_command", required=True)

    modelbt_run_parser = modelbt_subparsers.add_parser("run", help="Run model-signal backtest proxy.")
    modelbt_run_parser.add_argument("--model-ref", required=True, help="latest|champion|run_id|run_dir")
    modelbt_run_parser.add_argument("--model-family", help="Registry family, ex: train_v3_mtf_micro")
    modelbt_run_parser.add_argument("--tf", required=True, help="Timeframe, ex: 5m")
    modelbt_run_parser.add_argument("--quote", default="KRW", help="Quote filter, ex: KRW")
    modelbt_run_parser.add_argument("--top-n", type=int, default=50, help="Universe size")
    modelbt_run_parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    modelbt_run_parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    modelbt_run_parser.add_argument("--select", default="top_pct", choices=("top_pct",))
    modelbt_run_parser.add_argument("--top-pct", type=float, default=0.05)
    modelbt_run_parser.add_argument("--hold-bars", type=int, default=6)
    modelbt_run_parser.add_argument("--fee-bps", type=float, default=5.0)
    modelbt_run_parser.add_argument("--out-root", default="data/backtest")

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
    paper_run_parser.add_argument("--duration-sec", type=int, default=600, help="Run duration in seconds. Use 0 to run until stopped.")
    paper_run_parser.add_argument("--quote", help="Quote currency, ex: KRW")
    paper_run_parser.add_argument("--top-n", type=int)
    paper_run_parser.add_argument("--strategy", choices=("candidates_v1", "model_alpha_v1"))
    paper_run_parser.add_argument("--tf", help="Model timeframe when strategy=model_alpha_v1, ex: 5m")
    paper_run_parser.add_argument("--model-ref", help="Registry model ref, ex: champion_v3")
    paper_run_parser.add_argument("--model-family", help="Registry model family, ex: train_v3_mtf_micro")
    paper_run_parser.add_argument("--feature-set", choices=("v1", "v2", "v3", "v4"))
    paper_run_parser.add_argument("--top-pct", type=float)
    paper_run_parser.add_argument("--min-prob", type=float)
    paper_run_parser.add_argument("--min-cands-per-ts", type=int)
    paper_run_parser.add_argument("--max-positions-total", type=int)
    paper_run_parser.add_argument("--cooldown-bars", type=int)
    paper_run_parser.add_argument("--exit-mode", choices=("hold", "risk"))
    paper_run_parser.add_argument("--hold-bars", type=int)
    paper_run_parser.add_argument("--tp-pct", type=float)
    paper_run_parser.add_argument("--sl-pct", type=float)
    paper_run_parser.add_argument("--trailing-pct", type=float)
    paper_run_parser.add_argument("--execution-price-mode", choices=("PASSIVE_MAKER", "JOIN", "CROSS_1T"))
    paper_run_parser.add_argument("--execution-timeout-bars", type=int)
    paper_run_parser.add_argument("--execution-replace-max", type=int)
    paper_run_parser.add_argument("--print-every-sec", type=float)
    paper_run_parser.add_argument("--starting-krw", type=float)
    paper_run_parser.add_argument("--per-trade-krw", type=float)
    paper_run_parser.add_argument("--max-positions", type=int)
    paper_run_parser.add_argument("--micro-gate", choices=("on", "off"))
    paper_run_parser.add_argument("--micro-gate-mode", choices=("trade_only", "trade_and_book"))
    paper_run_parser.add_argument("--micro-gate-on-missing", choices=("warn_allow", "block", "allow"))
    paper_run_parser.add_argument("--micro-order-policy", choices=("on", "off"))
    paper_run_parser.add_argument("--micro-order-policy-mode", choices=("trade_only", "trade_and_book"))
    paper_run_parser.add_argument(
        "--micro-order-policy-on-missing",
        choices=("static_fallback", "conservative", "abort"),
    )
    paper_run_parser.add_argument(
        "--paper-micro-provider",
        choices=("offline_parquet", "live_ws", "auto"),
        help="Paper micro snapshot provider selection.",
    )
    paper_run_parser.add_argument(
        "--paper-micro-warmup-sec",
        type=int,
        help="Warmup seconds before order submit when LIVE_WS provider is active.",
    )
    paper_run_parser.add_argument(
        "--paper-micro-warmup-min-trade-events-per-market",
        type=int,
        help="Minimum trade events per market to satisfy LIVE_WS warmup.",
    )
    paper_run_parser.add_argument(
        "--paper-feature-provider",
        choices=("offline_parquet", "live_v3", "live_v4"),
        help="Paper feature provider selection for model_alpha_v1.",
    )
    paper_alpha_parser = paper_subparsers.add_parser(
        "alpha",
        help="Run model_alpha_v1 paper test with concise defaults.",
    )
    paper_alpha_parser.add_argument(
        "--preset",
        choices=("live_v3", "live_v4", "candidate_v4", "offline", "offline_v4", "default"),
        default=DEFAULT_PAPER_ALPHA_PRESET,
        help="Shortcut preset. default=config-driven, live_v3/live_v4 use live providers, offline variants use parquet providers. Current default rollout is live_v4.",
    )
    paper_alpha_parser.add_argument("--duration-sec", type=int, default=600, help="Run duration in seconds. Use 0 to run until stopped.")
    paper_alpha_parser.add_argument("--quote", help="Quote currency, ex: KRW")
    paper_alpha_parser.add_argument("--top-n", type=int)
    paper_alpha_parser.add_argument("--tf", help="Model timeframe, ex: 5m")
    paper_alpha_parser.add_argument("--model-ref", help="Registry model ref, ex: champion_v3")
    paper_alpha_parser.add_argument("--model-family", help="Registry model family, ex: train_v3_mtf_micro")
    paper_alpha_parser.add_argument("--feature-set", choices=("v1", "v2", "v3", "v4"))
    paper_alpha_parser.add_argument("--top-pct", type=float)
    paper_alpha_parser.add_argument("--min-prob", type=float)
    paper_alpha_parser.add_argument("--min-cands-per-ts", type=int)
    paper_alpha_parser.add_argument("--max-positions-total", type=int)
    paper_alpha_parser.add_argument("--cooldown-bars", type=int)
    paper_alpha_parser.add_argument("--exit-mode", choices=("hold", "risk"))
    paper_alpha_parser.add_argument("--hold-bars", type=int)
    paper_alpha_parser.add_argument("--tp-pct", type=float)
    paper_alpha_parser.add_argument("--sl-pct", type=float)
    paper_alpha_parser.add_argument("--trailing-pct", type=float)
    paper_alpha_parser.add_argument("--execution-price-mode", choices=("PASSIVE_MAKER", "JOIN", "CROSS_1T"))
    paper_alpha_parser.add_argument("--execution-timeout-bars", type=int)
    paper_alpha_parser.add_argument("--execution-replace-max", type=int)
    paper_alpha_parser.add_argument("--print-every-sec", type=float)
    paper_alpha_parser.add_argument("--starting-krw", type=float)
    paper_alpha_parser.add_argument("--per-trade-krw", type=float)
    paper_alpha_parser.add_argument("--max-positions", type=int)
    paper_alpha_parser.add_argument(
        "--paper-micro-provider",
        choices=("offline_parquet", "live_ws", "auto"),
        help="Optional provider override on top of preset.",
    )
    paper_alpha_parser.add_argument(
        "--paper-feature-provider",
        choices=("offline_parquet", "live_v3", "live_v4"),
        help="Optional provider override on top of preset.",
    )
    paper_alpha_parser.add_argument("--paper-micro-warmup-sec", type=int)
    paper_alpha_parser.add_argument("--paper-micro-warmup-min-trade-events-per-market", type=int)

    backtest_parser = subparsers.add_parser("backtest", help="Backtest operations.")
    backtest_subparsers = backtest_parser.add_subparsers(dest="backtest_command", required=True)

    backtest_run_parser = backtest_subparsers.add_parser("run", help="Run parquet candle backtest.")
    backtest_run_parser.add_argument("--dataset-name", help="Candle dataset name, ex: candles_api_v1")
    backtest_run_parser.add_argument("--parquet-root", help="Parquet root, ex: data/parquet")
    backtest_run_parser.add_argument("--tf", help="Timeframe, ex: 1m,5m")
    backtest_run_parser.add_argument("--market", help="Single market, ex: KRW-BTC")
    backtest_run_parser.add_argument("--markets", help="Comma separated markets, ex: KRW-BTC,KRW-ETH")
    backtest_run_parser.add_argument("--quote", help="Quote filter for universe mode, ex: KRW")
    backtest_run_parser.add_argument("--top-n", type=int, help="Universe size for static_start/fixed_list")
    backtest_run_parser.add_argument("--universe-mode", choices=("static_start", "fixed_list"))
    backtest_run_parser.add_argument("--strategy", choices=("candidates_v1", "model_alpha_v1"))
    backtest_run_parser.add_argument("--model-ref", help="Registry model ref, ex: champion_v3")
    backtest_run_parser.add_argument("--model-family", help="Registry model family, ex: train_v3_mtf_micro")
    backtest_run_parser.add_argument("--feature-set", choices=("v1", "v2", "v3", "v4"))
    backtest_run_parser.add_argument("--entry", choices=("top_pct",))
    backtest_run_parser.add_argument("--top-pct", type=float)
    backtest_run_parser.add_argument("--min-prob", type=float)
    backtest_run_parser.add_argument("--min-cands-per-ts", type=int)
    backtest_run_parser.add_argument("--exit-mode", choices=("hold", "risk"))
    backtest_run_parser.add_argument("--hold-bars", type=int)
    backtest_run_parser.add_argument("--tp-pct", type=float)
    backtest_run_parser.add_argument("--sl-pct", type=float)
    backtest_run_parser.add_argument("--trailing-pct", type=float)
    backtest_run_parser.add_argument("--cooldown-bars", type=int)
    backtest_run_parser.add_argument("--max-positions-total", type=int)
    backtest_run_parser.add_argument("--execution-price-mode", choices=("PASSIVE_MAKER", "JOIN", "CROSS_1T"))
    backtest_run_parser.add_argument("--execution-timeout-bars", type=int)
    backtest_run_parser.add_argument("--execution-replace-max", type=int)
    backtest_run_parser.add_argument("--start", help="Start date YYYY-MM-DD (UTC day start).")
    backtest_run_parser.add_argument("--end", help="End date YYYY-MM-DD (UTC day end).")
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
    backtest_run_parser.add_argument("--micro-gate", choices=("on", "off"))
    backtest_run_parser.add_argument("--micro-gate-mode", choices=("trade_only", "trade_and_book"))
    backtest_run_parser.add_argument("--micro-gate-on-missing", choices=("warn_allow", "block", "allow"))
    backtest_run_parser.add_argument("--micro-order-policy", choices=("on", "off"))
    backtest_run_parser.add_argument("--micro-order-policy-mode", choices=("trade_only", "trade_and_book"))
    backtest_run_parser.add_argument(
        "--micro-order-policy-on-missing",
        choices=("static_fallback", "conservative", "abort"),
    )
    backtest_alpha_parser = backtest_subparsers.add_parser(
        "alpha",
        help="Run model_alpha_v1 backtest with concise defaults.",
    )
    backtest_alpha_parser.add_argument(
        "--preset",
        choices=("default", "acceptance"),
        default="default",
        help="Shortcut preset. acceptance disables micro_order_policy for cleaner alpha validation.",
    )
    backtest_alpha_parser.add_argument("--dataset-name", help="Candle dataset name, ex: candles_api_v1")
    backtest_alpha_parser.add_argument("--parquet-root", help="Parquet root, ex: data/parquet")
    backtest_alpha_parser.add_argument("--tf", help="Timeframe, ex: 1m,5m")
    backtest_alpha_parser.add_argument("--market", help="Single market, ex: KRW-BTC")
    backtest_alpha_parser.add_argument("--markets", help="Comma separated markets, ex: KRW-BTC,KRW-ETH")
    backtest_alpha_parser.add_argument("--quote", help="Quote filter for universe mode, ex: KRW")
    backtest_alpha_parser.add_argument("--top-n", type=int, help="Universe size for static_start/fixed_list")
    backtest_alpha_parser.add_argument("--universe-mode", choices=("static_start", "fixed_list"))
    backtest_alpha_parser.add_argument("--model-ref", help="Registry model ref, ex: champion_v3")
    backtest_alpha_parser.add_argument("--model-family", help="Registry model family, ex: train_v3_mtf_micro")
    backtest_alpha_parser.add_argument("--feature-set", choices=("v1", "v2", "v3", "v4"))
    backtest_alpha_parser.add_argument("--top-pct", type=float)
    backtest_alpha_parser.add_argument("--min-prob", type=float)
    backtest_alpha_parser.add_argument("--min-cands-per-ts", type=int)
    backtest_alpha_parser.add_argument("--exit-mode", choices=("hold", "risk"))
    backtest_alpha_parser.add_argument("--hold-bars", type=int)
    backtest_alpha_parser.add_argument("--tp-pct", type=float)
    backtest_alpha_parser.add_argument("--sl-pct", type=float)
    backtest_alpha_parser.add_argument("--trailing-pct", type=float)
    backtest_alpha_parser.add_argument("--cooldown-bars", type=int)
    backtest_alpha_parser.add_argument("--max-positions-total", type=int)
    backtest_alpha_parser.add_argument("--execution-price-mode", choices=("PASSIVE_MAKER", "JOIN", "CROSS_1T"))
    backtest_alpha_parser.add_argument("--execution-timeout-bars", type=int)
    backtest_alpha_parser.add_argument("--execution-replace-max", type=int)
    backtest_alpha_parser.add_argument("--start", help="Start date YYYY-MM-DD (UTC day start).")
    backtest_alpha_parser.add_argument("--end", help="End date YYYY-MM-DD (UTC day end).")
    backtest_alpha_parser.add_argument("--from-ts-ms", type=int)
    backtest_alpha_parser.add_argument("--to-ts-ms", type=int)
    backtest_alpha_parser.add_argument("--days", type=int, help="Shortcut for --duration-days.")
    backtest_alpha_parser.add_argument("--dense-grid", action="store_true")
    backtest_alpha_parser.add_argument("--starting-krw", type=float)
    backtest_alpha_parser.add_argument("--per-trade-krw", type=float)
    backtest_alpha_parser.add_argument("--max-positions", type=int)

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
    live_run_parser.add_argument("--use-private-ws", action="store_true", help="Override config and use private WS sync.")
    live_run_parser.add_argument("--use-executor-ws", action="store_true", help="Override config and use executor WS sync.")
    live_run_parser.add_argument(
        "--rollout-mode",
        choices=("shadow", "canary", "live"),
        help="Live rollout mode override.",
    )
    live_run_parser.add_argument(
        "--rollout-target-unit",
        help="Target live systemd unit name used for promote-to-live restart contract.",
    )
    live_run_parser.add_argument(
        "--strategy-runtime",
        action="store_true",
        help="Run model_alpha live strategy runtime instead of sync-only daemon.",
    )

    live_admissibility_parser = live_subparsers.add_parser(
        "admissibility",
        help="Evaluate exact live-order admissibility using exchange snapshots.",
    )
    live_admissibility_parser.add_argument("--bot-id", help="Override live.bot_id")
    live_admissibility_parser.add_argument("--market", required=True, help="Market, ex: KRW-BTC")
    live_admissibility_parser.add_argument("--side", required=True, choices=("bid", "ask"))
    live_admissibility_parser.add_argument("--price", required=True, type=float)
    live_admissibility_parser.add_argument("--volume", type=float)
    live_admissibility_parser.add_argument("--target-notional-quote", type=float)
    live_admissibility_parser.add_argument("--expected-edge-bps", type=float)
    live_admissibility_parser.add_argument("--replace-risk-steps", type=int, default=0)
    live_admissibility_parser.add_argument(
        "--test-order",
        action="store_true",
        help="Also call Upbit order-test with adjusted fields when admissible.",
    )
    live_admissibility_parser.add_argument("--identifier", help="Optional identifier for order-test.")
    live_admissibility_parser.add_argument("--time-in-force", choices=("ioc", "fok", "post_only"))

    live_rollout_parser = live_subparsers.add_parser(
        "rollout",
        help="Inspect or mutate live rollout contract for shadow/canary/live promotion.",
    )
    live_rollout_subparsers = live_rollout_parser.add_subparsers(dest="live_rollout_command", required=True)
    live_rollout_status_parser = live_rollout_subparsers.add_parser("status", help="Show live rollout contract status.")
    live_rollout_status_parser.add_argument("--bot-id", help="Override live.bot_id")
    live_rollout_arm_parser = live_rollout_subparsers.add_parser("arm", help="Arm live rollout contract.")
    live_rollout_arm_parser.add_argument("--bot-id", help="Override live.bot_id")
    live_rollout_arm_parser.add_argument("--mode", choices=("canary", "live"), default="canary")
    live_rollout_arm_parser.add_argument("--target-unit", default=DEFAULT_LIVE_TARGET_UNIT)
    live_rollout_arm_parser.add_argument("--arm-token", required=True)
    live_rollout_arm_parser.add_argument("--note")
    live_rollout_arm_parser.add_argument("--canary-max-notional-quote", type=float)
    live_rollout_disarm_parser = live_rollout_subparsers.add_parser("disarm", help="Disarm live rollout contract.")
    live_rollout_disarm_parser.add_argument("--bot-id", help="Override live.bot_id")
    live_rollout_disarm_parser.add_argument("--arm-token", required=True)
    live_rollout_disarm_parser.add_argument("--note")
    live_rollout_test_order_parser = live_rollout_subparsers.add_parser(
        "test-order",
        help="Run and persist a live test-order validation for rollout gating.",
    )
    live_rollout_test_order_parser.add_argument("--bot-id", help="Override live.bot_id")
    live_rollout_test_order_parser.add_argument("--market", required=True, help="Market, ex: KRW-BTC")
    live_rollout_test_order_parser.add_argument("--side", required=True, choices=("bid", "ask"))
    live_rollout_test_order_parser.add_argument("--ord-type", default="limit")
    live_rollout_test_order_parser.add_argument("--price", required=True)
    live_rollout_test_order_parser.add_argument("--volume")
    live_rollout_test_order_parser.add_argument("--identifier")
    live_rollout_test_order_parser.add_argument("--time-in-force", choices=("ioc", "fok", "post_only"))

    live_kill_switch_parser = live_subparsers.add_parser(
        "kill-switch",
        help="Inspect or mutate persistent live breaker state.",
    )
    live_kill_switch_subparsers = live_kill_switch_parser.add_subparsers(
        dest="live_kill_switch_command",
        required=True,
    )
    live_kill_switch_status_parser = live_kill_switch_subparsers.add_parser("status", help="Show kill-switch status.")
    live_kill_switch_status_parser.add_argument("--bot-id", help="Override live.bot_id")
    live_kill_switch_arm_parser = live_kill_switch_subparsers.add_parser("arm", help="Arm live kill-switch.")
    live_kill_switch_arm_parser.add_argument("--bot-id", help="Override live.bot_id")
    live_kill_switch_arm_parser.add_argument(
        "--action",
        choices=("HALT_NEW_INTENTS", "HALT_AND_CANCEL_BOT_ORDERS", "FULL_KILL_SWITCH"),
        default="FULL_KILL_SWITCH",
    )
    live_kill_switch_arm_parser.add_argument("--reason-code", default="MANUAL_KILL_SWITCH")
    live_kill_switch_arm_parser.add_argument("--note")
    live_kill_switch_clear_parser = live_kill_switch_subparsers.add_parser("clear", help="Clear live kill-switch.")
    live_kill_switch_clear_parser.add_argument("--bot-id", help="Override live.bot_id")
    live_kill_switch_clear_parser.add_argument("--note")

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
    if args.command == "modelbt":
        return _handle_modelbt_command(args, Path(args.config_dir), config)
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
        return _handle_collect_ws_public(args, config_dir, base_config)
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


def _handle_collect_ws_public(args: argparse.Namespace, config_dir: Path, base_config: dict[str, Any]) -> int:
    ws_command = getattr(args, "collect_ws_public_command", None)
    if ws_command == "status":
        status = load_ws_public_status(
            meta_dir=Path(args.meta_dir),
            raw_root=Path(args.raw_root),
        )
        _print_json(status)
        return 0

    if ws_command == "purge":
        payload = purge_ws_public_retention(
            raw_root=Path(args.raw_root),
            meta_dir=Path(args.meta_dir),
            retention_days=max(int(args.retention_days), 1),
        )
        _print_json(payload)
        return 0

    if ws_command == "validate":
        summary = validate_ws_public_raw_dataset(
            raw_root=Path(args.raw_root),
            meta_dir=Path(args.meta_dir),
            report_path=Path(args.meta_dir) / "ws_validate_report.json",
            date_filter=args.date,
            quarantine_corrupt=_parse_bool_arg(getattr(args, "quarantine_corrupt", "false"), default=False),
            quarantine_dir=Path(getattr(args, "quarantine_dir", "data/raw_ws/upbit/_quarantine")),
            min_age_sec=max(int(getattr(args, "min_age_sec", 300)), 0),
        )
        print(
            "[collect][ws-public][validate] "
            f"checked={summary.checked_files} ok={summary.ok_files} "
            f"warn={summary.warn_files} fail={summary.fail_files} "
            f"parse_ok_ratio={summary.parse_ok_ratio:.6f}"
        )
        print(f"[collect][ws-public][validate] report={summary.validate_report_file}")
        if getattr(summary, "quarantined_files", 0) > 0 and getattr(summary, "quarantine_report_file", None) is not None:
            print(
                "[collect][ws-public][validate] "
                f"quarantined={summary.quarantined_files} report={summary.quarantine_report_file}"
            )
        return 2 if summary.fail_files > 0 else 0

    if ws_command == "stats":
        stats = collect_ws_public_stats(
            raw_root=Path(args.raw_root),
            meta_dir=Path(args.meta_dir),
            date_filter=args.date,
        )
        _print_json(stats)
        return 0

    if ws_command == "daemon":
        daemon_options = WsPublicDaemonOptions(
            raw_root=Path(args.raw_root),
            meta_dir=Path(args.meta_dir),
            quote=str(args.quote).strip().upper() or "KRW",
            top_n=max(int(args.top_n), 1),
            refresh_sec=max(int(args.refresh_sec), 30),
            duration_sec=(int(args.duration_sec) if int(args.duration_sec) > 0 else None),
            retention_days=max(int(args.retention_days), 1),
            downsample_hz=max(float(args.downsample_hz), 0.1),
            max_markets=max(int(args.max_markets), 1),
            format=str(args.format).strip().upper() or "DEFAULT",
            channels=_parse_csv_list(args.channels, normalize=str.lower) or ("trade", "orderbook"),
            orderbook_topk=max(int(args.orderbook_topk), 1),
            orderbook_level=_parse_orderbook_level_arg(args.orderbook_level),
            keepalive_mode=str(args.keepalive_mode).strip().lower(),
            keepalive_interval_sec=max(int(args.keepalive_interval_sec), 1),
            keepalive_stale_sec=max(int(args.keepalive_stale_sec), 30),
            rotate_sec=max(int(args.rotate_sec), 1),
            max_bytes=max(int(args.max_bytes), 1024),
            rate_limit_strict=_parse_bool_arg(args.rate_limit_strict, default=True),
            reconnect_max_per_min=max(int(args.reconnect_max_per_min), 1),
            max_subscribe_messages_per_min=max(int(args.max_subscribe_messages_per_min), 1),
            min_subscribe_interval_sec=max(int(args.min_subscribe_interval_sec), 1),
            orderbook_spread_bps_threshold=float(args.orderbook_spread_bps_threshold),
            orderbook_top1_size_change_threshold=float(args.orderbook_top1_size_change_threshold),
            health_update_sec=max(int(args.health_update_sec), 1),
            config_dir=config_dir,
        )
        summary = collect_ws_public_daemon(daemon_options)
        print(
            "[collect][ws-public][daemon] "
            f"run_id={summary.run_id} duration={summary.duration_sec}s quote={summary.quote} "
            f"top_n={summary.top_n} refresh={summary.refresh_sec}s subscribed={summary.subscribed_markets_count} "
            f"written_trade={summary.written_trade} written_orderbook={summary.written_orderbook} "
            f"reconnect={summary.reconnect_count} refresh_apply={summary.refresh_applied_count}"
        )
        print(f"[collect][ws-public][daemon] plan={summary.plan_file}")
        print(f"[collect][ws-public][daemon] collect_report={summary.collect_report_file}")
        print(f"[collect][ws-public][daemon] health={summary.health_snapshot_file}")
        print(f"[collect][ws-public][daemon] manifest={summary.manifest_file}")
        print(f"[collect][ws-public][daemon] checkpoint={summary.checkpoint_file}")
        print(f"[collect][ws-public][daemon] runs_summary={summary.runs_summary_file}")
        return 2 if summary.failures else 0

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
        keepalive_mode=str(args.keepalive_mode).strip().lower(),
        keepalive_interval_sec=max(int(args.keepalive_interval_sec), 1),
        keepalive_stale_sec=max(int(args.keepalive_stale_sec), 30),
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
    try:
        feature_set = str(getattr(args, "feature_set", "v1")).strip().lower()

        if args.features_command == "build":
            if feature_set == "v4":
                features_v4_config = load_features_v4_config(config_dir, base_config=base_config)
                options_v4 = FeatureBuildV4Options(
                    tf=str(args.tf).strip().lower(),
                    quote=(str(args.quote).strip().upper() if args.quote else None),
                    top_n=args.top_n,
                    start=args.start,
                    end=args.end,
                    feature_set=feature_set,
                    label_set=str(args.label_set).strip().lower(),
                    workers=max(int(args.workers), 1),
                    fail_on_warn=_parse_bool_arg(args.fail_on_warn, default=False),
                    dry_run=_parse_bool_arg(args.dry_run, default=False),
                    base_candles=(str(args.base_candles).strip() if args.base_candles else None),
                    micro_dataset=(str(args.micro_dataset).strip() if args.micro_dataset else None),
                )
                summary_v4 = build_features_dataset_v4(features_v4_config, options_v4)
                print(
                    "[features][build][v4] "
                    f"discovered={summary_v4.discovered_markets} selected={len(summary_v4.selected_markets)} "
                    f"processed={summary_v4.processed_markets} ok={summary_v4.ok_markets} "
                    f"warn={summary_v4.warn_markets} fail={summary_v4.fail_markets}"
                )
                print(
                    "[features][build][v4] "
                    f"rows_base_total={summary_v4.rows_base_total} "
                    f"rows_dropped_no_micro={summary_v4.rows_dropped_no_micro} "
                    f"rows_dropped_one_m_before_densify={summary_v4.rows_dropped_one_m_before_densify} "
                    f"rows_dropped_one_m={summary_v4.rows_dropped_one_m} "
                    f"rows_rescued_by_one_m_densify={summary_v4.rows_rescued_by_one_m_densify} "
                    f"rows_final={summary_v4.rows_final}"
                )
                print(
                    "[features][build][v4] "
                    f"one_m_synth_ratio_p50={summary_v4.one_m_synth_ratio_p50} "
                    f"one_m_synth_ratio_p90={summary_v4.one_m_synth_ratio_p90}"
                )
                print(
                    "[features][build][v4] "
                    f"effective_start={summary_v4.effective_start} "
                    f"effective_end={summary_v4.effective_end}"
                )
                print(f"[features][build][v4] output={summary_v4.output_path}")
                print(f"[features][build][v4] manifest={summary_v4.manifest_file}")
                print(f"[features][build][v4] report={summary_v4.build_report_file}")
                code = 2 if summary_v4.fail_markets > 0 else 0
                if options_v4.fail_on_warn and summary_v4.warn_markets > 0:
                    code = 2
                return code

            if feature_set == "v3":
                features_v3_config = load_features_v3_config(config_dir, base_config=base_config)
                options_v3 = FeatureBuildV3Options(
                    tf=str(args.tf).strip().lower(),
                    quote=(str(args.quote).strip().upper() if args.quote else None),
                    top_n=args.top_n,
                    start=args.start,
                    end=args.end,
                    feature_set=feature_set,
                    label_set=str(args.label_set).strip().lower(),
                    workers=max(int(args.workers), 1),
                    fail_on_warn=_parse_bool_arg(args.fail_on_warn, default=False),
                    dry_run=_parse_bool_arg(args.dry_run, default=False),
                    base_candles=(str(args.base_candles).strip() if args.base_candles else None),
                    micro_dataset=(str(args.micro_dataset).strip() if args.micro_dataset else None),
                )
                summary_v3 = build_features_dataset_v3(features_v3_config, options_v3)
                print(
                    "[features][build][v3] "
                    f"discovered={summary_v3.discovered_markets} selected={len(summary_v3.selected_markets)} "
                    f"processed={summary_v3.processed_markets} ok={summary_v3.ok_markets} "
                    f"warn={summary_v3.warn_markets} fail={summary_v3.fail_markets}"
                )
                print(
                    "[features][build][v3] "
                    f"rows_base_total={summary_v3.rows_base_total} "
                    f"rows_dropped_no_micro={summary_v3.rows_dropped_no_micro} "
                    f"rows_dropped_one_m_before_densify={summary_v3.rows_dropped_one_m_before_densify} "
                    f"rows_dropped_one_m={summary_v3.rows_dropped_one_m} "
                    f"rows_rescued_by_one_m_densify={summary_v3.rows_rescued_by_one_m_densify} "
                    f"rows_final={summary_v3.rows_final}"
                )
                print(
                    "[features][build][v3] "
                    f"one_m_synth_ratio_p50={summary_v3.one_m_synth_ratio_p50} "
                    f"one_m_synth_ratio_p90={summary_v3.one_m_synth_ratio_p90}"
                )
                print(
                    "[features][build][v3] "
                    f"effective_start={summary_v3.effective_start} "
                    f"effective_end={summary_v3.effective_end}"
                )
                print(f"[features][build][v3] output={summary_v3.output_path}")
                print(f"[features][build][v3] manifest={summary_v3.manifest_file}")
                print(f"[features][build][v3] report={summary_v3.build_report_file}")
                code = 2 if summary_v3.fail_markets > 0 else 0
                if options_v3.fail_on_warn and summary_v3.warn_markets > 0:
                    code = 2
                return code

            if feature_set == "v2":
                features_v2_config = load_features_v2_config(config_dir, base_config=base_config)
                options_v2 = FeatureBuildV2Options(
                    tf=str(args.tf).strip().lower(),
                    quote=(str(args.quote).strip().upper() if args.quote else None),
                    top_n=args.top_n,
                    start=args.start,
                    end=args.end,
                    feature_set=feature_set,
                    label_set=str(args.label_set).strip().lower(),
                    workers=max(int(args.workers), 1),
                    fail_on_warn=_parse_bool_arg(args.fail_on_warn, default=False),
                    dry_run=_parse_bool_arg(args.dry_run, default=False),
                    base_candles=(str(args.base_candles).strip() if args.base_candles else None),
                    micro_dataset=(str(args.micro_dataset).strip() if args.micro_dataset else None),
                    require_micro=(
                        _parse_bool_arg(args.require_micro, default=True) if args.require_micro is not None else None
                    ),
                    min_trade_events=args.min_trade_events,
                    min_trade_coverage_ms=args.min_trade_coverage_ms,
                    min_book_events=args.min_book_events,
                    min_book_coverage_ms=args.min_book_coverage_ms,
                    use_precomputed_features_v1=(
                        _parse_bool_arg(args.use_precomputed_features_v1, default=False)
                        if args.use_precomputed_features_v1 is not None
                        else None
                    ),
                )
                summary_v2 = build_features_dataset_v2(features_v2_config, options_v2)
                print(
                    "[features][build][v2] "
                    f"discovered={summary_v2.discovered_markets} selected={len(summary_v2.selected_markets)} "
                    f"processed={summary_v2.processed_markets} ok={summary_v2.ok_markets} "
                    f"warn={summary_v2.warn_markets} fail={summary_v2.fail_markets}"
                )
                print(
                    f"[features][build][v2] rows_total={summary_v2.rows_total} "
                    f"min_ts_ms={summary_v2.min_ts_ms} max_ts_ms={summary_v2.max_ts_ms}"
                )
                print(f"[features][build][v2] output={summary_v2.output_path}")
                print(f"[features][build][v2] manifest={summary_v2.manifest_file}")
                print(f"[features][build][v2] report={summary_v2.build_report_file}")
                code = 2 if summary_v2.fail_markets > 0 else 0
                if options_v2.fail_on_warn and summary_v2.warn_markets > 0:
                    code = 2
                return code

            features_config = load_features_config(config_dir, base_config=base_config)
            options = FeatureBuildOptions(
                tf=str(args.tf).strip().lower(),
                quote=(str(args.quote).strip().upper() if args.quote else None),
                top_n=args.top_n,
                start=args.start,
                end=args.end,
                feature_set=feature_set,
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
            if feature_set == "v4":
                features_v4_config = load_features_v4_config(config_dir, base_config=base_config)
                options_v4 = FeatureValidateV4Options(
                    tf=str(args.tf).strip().lower(),
                    quote=(str(args.quote).strip().upper() if args.quote else None),
                    top_n=args.top_n,
                    start=args.start,
                    end=args.end,
                )
                summary_v4 = validate_features_dataset_v4(features_v4_config, options_v4)
                print(
                    "[features][validate][v4] "
                    f"checked={summary_v4.checked_files} ok={summary_v4.ok_files} "
                    f"warn={summary_v4.warn_files} fail={summary_v4.fail_files}"
                )
                print(
                    "[features][validate][v4] "
                    f"schema_ok={summary_v4.schema_ok} "
                    f"null_ratio_overall={summary_v4.null_ratio_overall:.6f} "
                    f"leakage_smoke={summary_v4.leakage_smoke} "
                    f"staleness_fail_rows={summary_v4.staleness_fail_rows}"
                )
                print(f"[features][validate][v4] dropped_rows_no_micro={summary_v4.dropped_rows_no_micro}")
                print(f"[features][validate][v4] report={summary_v4.validate_report_file}")
                if summary_v4.fail_files > 0 or summary_v4.leakage_smoke != "PASS":
                    return 2
                return 0

            if feature_set == "v3":
                features_v3_config = load_features_v3_config(config_dir, base_config=base_config)
                options_v3 = FeatureValidateV3Options(
                    tf=str(args.tf).strip().lower(),
                    quote=(str(args.quote).strip().upper() if args.quote else None),
                    top_n=args.top_n,
                    start=args.start,
                    end=args.end,
                )
                summary_v3 = validate_features_dataset_v3(features_v3_config, options_v3)
                print(
                    "[features][validate][v3] "
                    f"checked={summary_v3.checked_files} ok={summary_v3.ok_files} "
                    f"warn={summary_v3.warn_files} fail={summary_v3.fail_files}"
                )
                print(
                    "[features][validate][v3] "
                    f"schema_ok={summary_v3.schema_ok} "
                    f"null_ratio_overall={summary_v3.null_ratio_overall:.6f} "
                    f"leakage_smoke={summary_v3.leakage_smoke} "
                    f"staleness_fail_rows={summary_v3.staleness_fail_rows}"
                )
                print(f"[features][validate][v3] dropped_rows_no_micro={summary_v3.dropped_rows_no_micro}")
                print(f"[features][validate][v3] report={summary_v3.validate_report_file}")
                if summary_v3.fail_files > 0 or summary_v3.leakage_smoke != "PASS":
                    return 2
                return 0

            if feature_set == "v2":
                features_v2_config = load_features_v2_config(config_dir, base_config=base_config)
                options_v2 = FeatureValidateV2Options(
                    tf=str(args.tf).strip().lower(),
                    quote=(str(args.quote).strip().upper() if args.quote else None),
                    top_n=args.top_n,
                    join_match_warn=args.join_match_warn,
                    join_match_fail=args.join_match_fail,
                )
                summary_v2 = validate_features_dataset_v2(features_v2_config, options_v2)
                print(
                    "[features][validate][v2] "
                    f"checked={summary_v2.checked_files} ok={summary_v2.ok_files} "
                    f"warn={summary_v2.warn_files} fail={summary_v2.fail_files}"
                )
                print(
                    f"[features][validate][v2] schema_ok={summary_v2.schema_ok} "
                    f"null_ratio_overall={summary_v2.null_ratio_overall:.6f}"
                )
                print(
                    "[features][validate][v2] "
                    f"join_match_ratio={summary_v2.join_match_ratio if summary_v2.join_match_ratio is not None else 'NA'} "
                    f"micro_available_ratio={summary_v2.micro_available_ratio:.6f}"
                )
                print(f"[features][validate][v2] report={summary_v2.validate_report_file}")
                return 2 if summary_v2.fail_files > 0 else 0

            features_config = load_features_config(config_dir, base_config=base_config)
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
            features_config = load_features_config(config_dir, base_config=base_config)
            rows = sample_features(
                features_config,
                tf=str(args.tf).strip().lower(),
                market=str(args.market).strip().upper(),
                rows=max(int(args.rows), 0),
            )
            _print_json(rows)
            return 0

        if args.features_command == "stats":
            if feature_set == "v4":
                features_v4_config = load_features_v4_config(config_dir, base_config=base_config)
                stats_v4 = features_stats_v4(
                    features_v4_config,
                    tf=str(args.tf).strip().lower(),
                    quote=(str(args.quote).strip().upper() if args.quote else None),
                    top_n=args.top_n,
                )
                _print_json(stats_v4)
                return 0

            if feature_set == "v3":
                features_v3_config = load_features_v3_config(config_dir, base_config=base_config)
                stats_v3 = features_stats_v3(
                    features_v3_config,
                    tf=str(args.tf).strip().lower(),
                    quote=(str(args.quote).strip().upper() if args.quote else None),
                    top_n=args.top_n,
                )
                _print_json(stats_v3)
                return 0

            if feature_set == "v2":
                features_v2_config = load_features_v2_config(config_dir, base_config=base_config)
                stats_v2 = features_stats_v2(
                    features_v2_config,
                    tf=str(args.tf).strip().lower(),
                    quote=(str(args.quote).strip().upper() if args.quote else None),
                    top_n=args.top_n,
                )
                _print_json(stats_v2)
                return 0

            features_config = load_features_config(config_dir, base_config=base_config)
            stats = features_stats(
                features_config,
                tf=str(args.tf).strip().lower(),
                quote=(str(args.quote).strip().upper() if args.quote else None),
                top_n=args.top_n,
            )
            _print_json(stats)
            return 0

        raise ValueError(f"Unsupported features command: {args.features_command}")
    except (ValueError, FileNotFoundError, RuntimeError) as exc:
        print(f"[features][error] {exc}")
        return 2


def _resolve_powershell_exe() -> str:
    for name in ("pwsh", "powershell.exe", "powershell"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    raise RuntimeError("PowerShell executable not found in PATH")


def _run_manual_v4_daily_pipeline(args: argparse.Namespace, config_dir: Path) -> int:
    mode = str(getattr(args, "mode", "spawn_only")).strip().lower() or "spawn_only"
    if mode != "spawn_only":
        raise ValueError("model daily-v4 currently supports only --mode spawn_only to avoid runtime mutation")
    project_root = config_dir.parent.resolve()
    lane = str(getattr(args, "lane", "cls_scout")).strip().lower() or "cls_scout"
    if lane == "rank_shadow":
        wrapper_script = project_root / "scripts" / "v4_rank_shadow_candidate_acceptance.ps1"
        out_dir = "logs/model_v4_acceptance_rank_shadow"
    else:
        wrapper_script = project_root / "scripts" / "v4_scout_candidate_acceptance.ps1"
        out_dir = "logs/model_v4_acceptance_manual"
    if not wrapper_script.exists():
        raise FileNotFoundError(f"missing wrapper script: {wrapper_script}")
    pwsh_exe = _resolve_powershell_exe()
    command = [
        pwsh_exe,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(wrapper_script),
        "-ProjectRoot",
        str(project_root),
        "-PythonExe",
        sys.executable,
        "-OutDir",
        out_dir,
        "-SkipPromote",
    ]
    if getattr(args, "batch_date", None):
        command.extend(["-BatchDate", str(args.batch_date).strip()])
    run_paper_soak = bool(getattr(args, "run_paper_soak", False))
    paper_soak_duration_sec = int(getattr(args, "paper_soak_duration_sec", None) or 0)
    if paper_soak_duration_sec > 0:
        run_paper_soak = True
        command.extend(["-PaperSoakDurationSec", str(paper_soak_duration_sec)])
    if not run_paper_soak:
        command.append("-SkipPaperSoak")
    if getattr(args, "dry_run", False):
        command.append("-DryRun")
    completed = subprocess.run(
        command,
        cwd=project_root,
        text=True,
    )
    exit_code = int(completed.returncode)
    if exit_code == 2:
        latest_report = project_root / out_dir / "latest.json"
        report = _load_registry_pointer_payload(latest_report)
        reasons = [str(item).strip() for item in report.get("reasons", []) if str(item).strip()]
        backtest_gate = report.get("gates", {}).get("backtest", {}) if isinstance(report.get("gates"), dict) else {}
        budget_reasons = [
            str(item).strip()
            for item in ((backtest_gate.get("budget_contract_reasons", []) if isinstance(backtest_gate, dict) else []) or [])
            if str(item).strip()
        ]
        if "SCOUT_ONLY_BUDGET_EVIDENCE" in reasons or "SCOUT_ONLY_BUDGET_EVIDENCE" in budget_reasons:
            return 0
    return exit_code


def _handle_model_command(args: argparse.Namespace, config_dir: Path, base_config: dict[str, Any]) -> int:
    try:
        defaults = load_train_defaults(config_dir, base_config=base_config)
        risk_doc = _load_yaml_doc(config_dir / "risk.yaml")
        strategy_doc = _load_yaml_doc(config_dir / "strategy.yaml")
        backtest_doc = _load_yaml_doc(config_dir / "backtest.yaml")
        backtest_defaults = _backtest_defaults(
            base_config=base_config,
            risk_doc=risk_doc,
            strategy_doc=strategy_doc,
            backtest_doc=backtest_doc,
        )
        features_config = load_features_config(config_dir, base_config=base_config)
        features_v2_config = load_features_v2_config(config_dir, base_config=base_config)
        features_v3_config = load_features_v3_config(config_dir, base_config=base_config)
        features_v4_config = load_features_v4_config(config_dir, base_config=base_config)
        registry_root = Path(str(defaults["registry_root"]))
        logs_root = Path(str(defaults["logs_root"]))

        if args.model_command == "daily-v4":
            return _run_manual_v4_daily_pipeline(args, config_dir)

        if args.model_command == "train":
            trainer = str(getattr(args, "trainer", "v1")).strip().lower() or "v1"
            top_n = int(args.top_n if args.top_n is not None else defaults["top_n"])
            if trainer == "v4_crypto_cs":
                options_v4 = _cli_train_v4_helpers.build_v4_train_options(
                    args=args,
                    defaults=defaults,
                    backtest_defaults=backtest_defaults,
                    features_v4_config=features_v4_config,
                    registry_root=registry_root,
                    logs_root=logs_root,
                    top_n=top_n,
                    resolve_backtest_dataset_name_for_model_features=_resolve_backtest_dataset_name_for_model_features,
                    clamp_prob_value=_clamp_prob_value,
                    optional_float_value=_optional_float_value,
                )
                summary_v4 = train_and_register_v4_crypto_cs(options_v4)
                print(
                    "[model][train][v4_crypto_cs] "
                    f"run_id={summary_v4.run_id} status={summary_v4.status} "
                    f"test_precision_top5={summary_v4.leaderboard_row.get('test_precision_top5', 0.0):.6f} "
                    f"test_pr_auc={summary_v4.leaderboard_row.get('test_pr_auc', 0.0):.6f}"
                )
                print(f"[model][train][v4_crypto_cs] run_dir={summary_v4.run_dir}")
                print(f"[model][train][v4_crypto_cs] train_report={summary_v4.train_report_path}")
                if summary_v4.walk_forward_report_path is not None:
                    print(f"[model][train][v4_crypto_cs] walk_forward={summary_v4.walk_forward_report_path}")
                if summary_v4.cpcv_lite_report_path is not None:
                    print(f"[model][train][v4_crypto_cs] cpcv_lite={summary_v4.cpcv_lite_report_path}")
                if summary_v4.factor_block_selection_path is not None:
                    print(f"[model][train][v4_crypto_cs] factor_blocks={summary_v4.factor_block_selection_path}")
                if summary_v4.factor_block_policy_path is not None:
                    print(f"[model][train][v4_crypto_cs] factor_block_policy={summary_v4.factor_block_policy_path}")
                if summary_v4.search_budget_decision_path is not None:
                    print(f"[model][train][v4_crypto_cs] search_budget={summary_v4.search_budget_decision_path}")
                if summary_v4.execution_acceptance_report_path is not None:
                    print(
                        f"[model][train][v4_crypto_cs] execution_acceptance={summary_v4.execution_acceptance_report_path}"
                    )
                print(f"[model][train][v4_crypto_cs] promotion={summary_v4.promotion_path}")
                return 0

            if trainer == "v3_mtf_micro":
                model_family = (
                    str(getattr(args, "model_family", None) or "train_v3_mtf_micro").strip() or "train_v3_mtf_micro"
                )
                options_v3 = TrainV3MtfMicroOptions(
                    dataset_root=features_v3_config.output_dataset_root,
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
                    fee_bps_est=float(defaults["fee_bps_est"]),
                    safety_bps=float(defaults["safety_bps"]),
                    ev_scan_steps=max(int(defaults["ev_scan_steps"]), 10),
                    ev_min_selected=max(int(defaults["ev_min_selected"]), 1),
                    min_rows_for_train=max(int(features_v3_config.build.min_rows_for_train), 1),
                )
                summary_v3 = train_and_register_v3_mtf_micro(options_v3)
                print(
                    "[model][train][v3_mtf_micro] "
                    f"run_id={summary_v3.run_id} status={summary_v3.status} "
                    f"test_precision_top5={summary_v3.leaderboard_row.get('test_precision_top5', 0.0):.6f} "
                    f"test_pr_auc={summary_v3.leaderboard_row.get('test_pr_auc', 0.0):.6f}"
                )
                print(f"[model][train][v3_mtf_micro] run_dir={summary_v3.run_dir}")
                print(f"[model][train][v3_mtf_micro] train_report={summary_v3.train_report_path}")
                print(f"[model][train][v3_mtf_micro] promotion={summary_v3.promotion_path}")
                return 0

            if trainer == "v2_micro":
                model_family = str(getattr(args, "model_family", None) or "train_v2_micro").strip() or "train_v2_micro"
                options_v2 = TrainV2MicroOptions(
                    dataset_root=features_v2_config.output_dataset_root,
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
                    fee_bps_est=float(defaults["fee_bps_est"]),
                    safety_bps=float(defaults["safety_bps"]),
                    ev_scan_steps=max(int(defaults["ev_scan_steps"]), 10),
                    ev_min_selected=max(int(defaults["ev_min_selected"]), 1),
                    min_rows_for_train=max(int(features_v2_config.build.min_rows_for_train), 1),
                    parquet_root=features_v2_config.parquet_root,
                    base_candles_dataset=str(features_v2_config.build.base_candles_dataset),
                )
                summary_v2 = train_and_register_v2_micro(options_v2)
                print(
                    "[model][train][v2_micro] "
                    f"run_id={summary_v2.run_id} status={summary_v2.status} "
                    f"test_precision_top5={summary_v2.leaderboard_row.get('test_precision_top5', 0.0):.6f} "
                    f"test_pr_auc={summary_v2.leaderboard_row.get('test_pr_auc', 0.0):.6f}"
                )
                print(f"[model][train][v2_micro] run_dir={summary_v2.run_dir}")
                print(f"[model][train][v2_micro] train_report={summary_v2.train_report_path}")
                print(f"[model][train][v2_micro] compare_to_v1={summary_v2.compare_report_path}")
                print(f"[model][train][v2_micro] promotion={summary_v2.promotion_path}")
                return 0

            model_family = str(getattr(args, "model_family", None) or defaults["model_family"]).strip()
            if not model_family:
                model_family = "train_v1"
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
            model_ref, model_family = _resolve_model_ref_alias(
                str(args.model_ref).strip(),
                str(args.model_family).strip() if args.model_family else None,
            )
            result = evaluate_registered_model_window(
                registry_root=registry_root,
                model_ref=model_ref,
                model_family=model_family,
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
            model_ref, model_family = _resolve_model_ref_alias(
                str(args.model_ref).strip(),
                str(args.model_family).strip() if args.model_family else None,
            )
            detail = show_registered_model(
                registry_root=registry_root,
                model_ref=model_ref,
                model_family=model_family,
            )
            _print_json(detail)
            return 0

        if args.model_command == "promote":
            model_ref, model_family = _resolve_model_ref_alias(
                str(args.model_ref).strip(),
                str(args.model_family).strip() if args.model_family else None,
            )
            result = promote_run_to_champion(
                registry_root=registry_root,
                model_ref=model_ref,
                model_family=model_family,
                score_key=str(args.score_key).strip() or "test_precision_top5",
            )
            print(
                "[model][promote] "
                f"run_id={result.get('run_id')} family={result.get('model_family')} "
                f"score_key={result.get('score_key')} score={float(result.get('score', 0.0)):.6f}"
            )
            print(f"[model][promote] champion={result.get('champion_path')}")
            print(f"[model][promote] promotion={result.get('promotion_path')}")
            _print_json(result)
            return 0

        if args.model_command == "compare":
            a_ref, a_family = _resolve_model_ref_alias(str(args.a).strip(), getattr(args, "a_family", None))
            b_ref, b_family = _resolve_model_ref_alias(str(args.b).strip(), getattr(args, "b_family", None))
            result = compare_registered_models(
                registry_root=registry_root,
                a_ref=a_ref,
                b_ref=b_ref,
                a_family=(str(a_family).strip() if a_family else None),
                b_family=(str(b_family).strip() if b_family else None),
                split=str(args.split).strip().lower(),
                start=(str(args.start).strip() if args.start else None),
                end=(str(args.end).strip() if args.end else None),
            )
            _print_json(result)
            return 0

        if args.model_command == "audit":
            model_ref, model_family = _resolve_model_ref_alias(
                str(args.model_ref).strip(),
                str(args.model_family).strip() if args.model_family else None,
            )
            audit = audit_registered_model(
                MetricAuditOptions(
                    registry_root=registry_root,
                    logs_root=logs_root,
                    model_ref=model_ref,
                    model_family=model_family,
                    split=str(args.split).strip().lower(),
                    start=(str(args.start).strip() if args.start else None),
                    end=(str(args.end).strip() if args.end else None),
                    tolerance_warn=float(args.tolerance_warn),
                    tolerance_fail=float(args.tolerance_fail),
                )
            )
            issue_count = len(audit.payload.get("issues", [])) if isinstance(audit.payload, dict) else 0
            print(
                "[model][audit] "
                f"status={audit.status} run_id={audit.run_id} split={audit.split} "
                f"rows={audit.rows_in_split} issues={issue_count}"
            )
            print(f"[model][audit] report={audit.output_path}")
            _print_json(audit.payload)
            return 0 if audit.status == "PASS" else 2

        if args.model_command == "ablate":
            tf = str(args.tf or defaults["tf"]).strip().lower()
            quote = str(args.quote or defaults["quote"]).strip().upper()
            top_n = max(int(args.top_n if args.top_n is not None else defaults["top_n"]), 1)
            start = str(args.start or defaults["start"]).strip()
            end = str(args.end or defaults["end"]).strip()
            ablation_ids = _parse_ablation_ids(getattr(args, "ablations", None))
            result = run_ablation(
                AblationOptions(
                    dataset_root=features_v2_config.output_dataset_root,
                    parquet_root=features_v2_config.parquet_root,
                    logs_root=logs_root,
                    tf=tf,
                    quote=quote,
                    top_n=top_n,
                    start=start,
                    end=end,
                    feature_set=str(args.feature_set).strip().lower(),
                    label_set=str(args.label_set).strip().lower(),
                    booster_sweep_trials=max(int(args.booster_sweep_trials), 1),
                    seed=int(args.seed if args.seed is not None else defaults["seed"]),
                    nthread=int(args.nthread if args.nthread is not None else defaults["nthread"]),
                    batch_rows=max(int(defaults["batch_rows"]), 1),
                    train_ratio=float(defaults["train_ratio"]),
                    valid_ratio=float(defaults["valid_ratio"]),
                    test_ratio=float(defaults["test_ratio"]),
                    embargo_bars=max(int(defaults["embargo_bars"]), 0),
                    fee_bps_est=float(defaults["fee_bps_est"]),
                    safety_bps=float(defaults["safety_bps"]),
                    ablations=ablation_ids,
                    base_candles_dataset=str(features_v2_config.build.base_candles_dataset),
                )
            )
            print(
                "[model][ablate] "
                f"rows={len(result.rows)} best_prec={result.summary.get('best_ablation_by_prec_top5')} "
                f"best_ev={result.summary.get('best_ablation_by_ev_top5')}"
            )
            print(f"[model][ablate] csv={result.results_csv_path}")
            print(f"[model][ablate] summary={result.summary_json_path}")
            _print_json(result.summary)
            return 0

        raise ValueError(f"Unsupported model command: {args.model_command}")
    except (ValueError, FileNotFoundError, RuntimeError) as exc:
        print(f"[model][error] {exc}")
        return 2


def _handle_modelbt_command(args: argparse.Namespace, config_dir: Path, base_config: dict[str, Any]) -> int:
    try:
        if args.modelbt_command != "run":
            raise ValueError(f"Unsupported modelbt command: {args.modelbt_command}")

        defaults = load_train_defaults(config_dir, base_config=base_config)
        features_v3_config = load_features_v3_config(config_dir, base_config=base_config)
        registry_root = Path(str(defaults["registry_root"]))
        model_ref, model_family = _resolve_model_ref_alias(
            str(args.model_ref).strip(),
            str(args.model_family).strip() if args.model_family else None,
        )
        result = run_modelbt_proxy(
            ModelBtProxyOptions(
                registry_root=registry_root,
                parquet_root=features_v3_config.parquet_root,
                base_candles_dataset=str(features_v3_config.build.base_candles_dataset),
                out_root=Path(str(args.out_root)),
                model_ref=model_ref,
                model_family=model_family,
                tf=str(args.tf).strip().lower(),
                quote=str(args.quote).strip().upper(),
                top_n=max(int(args.top_n), 1),
                start=str(args.start).strip(),
                end=str(args.end).strip(),
                select_mode=str(args.select).strip().lower(),
                top_pct=float(args.top_pct),
                hold_bars=max(int(args.hold_bars), 1),
                fee_bps=float(args.fee_bps),
            )
        )
        print(
            "[modelbt][run] "
            f"trades={result.summary.get('trades_count', 0)} "
            f"win_rate={result.summary.get('win_rate', 0.0):.6f} "
            f"avg_return_net={result.summary.get('avg_return_net', 0.0):.6f} "
            f"equity_end={result.summary.get('equity_end', 1.0):.6f}"
        )
        print(f"[modelbt][run] run_dir={result.run_dir}")
        print(f"[modelbt][run] equity={result.equity_csv}")
        print(f"[modelbt][run] summary={result.summary_json}")
        print(f"[modelbt][run] trades={result.trades_csv}")
        print(f"[modelbt][run] diagnostics={result.diagnostics_json}")
        return 0
    except (ValueError, FileNotFoundError, RuntimeError) as exc:
        print(f"[modelbt][error] {exc}")
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
        if args.paper_command == "alpha":
            shortcut_args = _normalize_paper_alpha_args(args)
            print(
                "[paper][alpha] "
                f"preset={getattr(shortcut_args, 'preset', DEFAULT_PAPER_ALPHA_PRESET)} "
                f"feature_provider={getattr(shortcut_args, 'paper_feature_provider', None) or 'config'} "
                f"micro_provider={getattr(shortcut_args, 'paper_micro_provider', None) or 'config'}"
            )
            return _handle_paper_command(shortcut_args, config_dir, base_config)

        if args.paper_command != "run":
            raise ValueError(f"Unsupported paper command: {args.paper_command}")

        settings = load_upbit_settings(config_dir)
        _ensure_upbit_runtime_available()
        risk_doc = _load_yaml_doc(config_dir / "risk.yaml")
        strategy_doc = _load_yaml_doc(config_dir / "strategy.yaml")
        defaults = _paper_defaults(base_config=base_config, risk_doc=risk_doc, strategy_doc=strategy_doc)
        strategy_mode = str(getattr(args, "strategy", None) or defaults.get("strategy", "candidates_v1")).strip().lower()
        model_alpha_defaults = defaults.get("model_alpha", {}) if isinstance(defaults.get("model_alpha"), dict) else {}
        model_alpha_selection_defaults = (
            model_alpha_defaults.get("selection", {}) if isinstance(model_alpha_defaults.get("selection"), dict) else {}
        )
        model_alpha_position_defaults = (
            model_alpha_defaults.get("position", {}) if isinstance(model_alpha_defaults.get("position"), dict) else {}
        )
        model_alpha_exit_defaults = (
            model_alpha_defaults.get("exit", {}) if isinstance(model_alpha_defaults.get("exit"), dict) else {}
        )
        model_alpha_execution_defaults = (
            model_alpha_defaults.get("execution", {}) if isinstance(model_alpha_defaults.get("execution"), dict) else {}
        )
        model_alpha_operational_defaults = (
            model_alpha_defaults.get("operational", {})
            if isinstance(model_alpha_defaults.get("operational"), dict)
            else {}
        )
        model_ref_value = str(
            getattr(args, "model_ref", None)
            or defaults.get("model_ref")
            or model_alpha_defaults.get("model_ref", DEFAULT_MODEL_ALPHA_RUNTIME_REF)
        ).strip()
        model_family_raw = getattr(args, "model_family", None)
        if model_family_raw is None:
            model_family_raw = defaults.get("model_family")
        if model_family_raw in {None, ""}:
            model_family_raw = model_alpha_defaults.get("model_family")
        model_family_value = str(model_family_raw).strip() if model_family_raw else None
        model_ref_value, model_family_value = _resolve_model_ref_alias(model_ref_value, model_family_value)
        registry_root_value = Path(str(defaults.get("model_registry_root", "models/registry")).strip() or "models/registry")
        if not registry_root_value.is_absolute():
            registry_root_value = (config_dir.parent / registry_root_value).resolve()
        model_ref_value, model_family_value, runtime_fallback_warning = _resolve_v4_runtime_model_ref_fallback(
            model_ref_value,
            model_family_value,
            registry_root_value,
        )
        if runtime_fallback_warning:
            print(runtime_fallback_warning)
        feature_set_value = str(
            getattr(args, "feature_set", None)
            or defaults.get("feature_set", model_alpha_defaults.get("feature_set", "v3"))
        ).strip().lower() or "v3"
        selection_use_learned_recommendations = _selection_use_learned_recommendations(
            args=args,
            model_alpha_selection_defaults=model_alpha_selection_defaults,
        )
        selection_top_pct = float(
            getattr(args, "top_pct", None)
            if getattr(args, "top_pct", None) is not None
            else model_alpha_selection_defaults.get("top_pct", 0.05)
        )
        selection_min_prob = _clamp_prob_value(
            _optional_float_value(
                getattr(args, "min_prob", None)
                if getattr(args, "min_prob", None) is not None
                else model_alpha_selection_defaults.get("min_prob")
            )
        )
        selection_min_cands = int(
            getattr(args, "min_cands_per_ts", None)
            if getattr(args, "min_cands_per_ts", None) is not None
            else model_alpha_selection_defaults.get("min_candidates_per_ts", 10)
        )
        selection_registry_threshold_key = str(
            model_alpha_selection_defaults.get("registry_threshold_key", "top_5pct")
        ).strip() or "top_5pct"
        position_max_total = int(
            getattr(args, "max_positions_total", None)
            if getattr(args, "max_positions_total", None) is not None
            else model_alpha_position_defaults.get("max_positions_total", defaults["max_positions"])
        )
        position_cooldown_bars = int(
            getattr(args, "cooldown_bars", None)
            if getattr(args, "cooldown_bars", None) is not None
            else model_alpha_position_defaults.get("cooldown_bars", 6)
        )
        position_entry_min_notional_buffer_bps = max(
            float(model_alpha_position_defaults.get("entry_min_notional_buffer_bps", 25.0)),
            0.0,
        )
        position_sizing_mode = str(model_alpha_position_defaults.get("sizing_mode", "prob_ramp")).strip().lower() or "prob_ramp"
        position_size_multiplier_min = max(
            float(model_alpha_position_defaults.get("size_multiplier_min", 0.5)),
            0.0,
        )
        position_size_multiplier_max = max(
            float(model_alpha_position_defaults.get("size_multiplier_max", 1.5)),
            position_size_multiplier_min,
        )
        exit_mode = str(
            getattr(args, "exit_mode", None) or model_alpha_exit_defaults.get("mode", "hold")
        ).strip().lower() or "hold"
        hold_bars_value = int(
            getattr(args, "hold_bars", None)
            if getattr(args, "hold_bars", None) is not None
            else model_alpha_exit_defaults.get("hold_bars", 6)
        )
        tp_pct_value = float(
            getattr(args, "tp_pct", None)
            if getattr(args, "tp_pct", None) is not None
            else model_alpha_exit_defaults.get("tp_pct", 0.02)
        )
        sl_pct_value = float(
            getattr(args, "sl_pct", None)
            if getattr(args, "sl_pct", None) is not None
            else model_alpha_exit_defaults.get("sl_pct", 0.01)
        )
        trailing_pct_value = float(
            getattr(args, "trailing_pct", None)
            if getattr(args, "trailing_pct", None) is not None
            else model_alpha_exit_defaults.get("trailing_pct", 0.0)
        )
        expected_exit_slippage_bps_value = _optional_float_value(
            model_alpha_exit_defaults.get("expected_exit_slippage_bps")
        )
        expected_exit_fee_bps_value = _optional_float_value(model_alpha_exit_defaults.get("expected_exit_fee_bps"))
        exit_use_learned_exit_mode = bool(
            getattr(args, "use_learned_exit_mode", model_alpha_exit_defaults.get("use_learned_exit_mode", True))
        )
        exit_use_learned_hold_bars = bool(
            getattr(args, "use_learned_hold_bars", model_alpha_exit_defaults.get("use_learned_hold_bars", True))
        )
        exit_use_learned_risk_recommendations = bool(
            getattr(
                args,
                "use_learned_risk_recommendations",
                model_alpha_exit_defaults.get("use_learned_risk_recommendations", True),
            )
        )
        exit_use_trade_level_action_policy = bool(
            getattr(
                args,
                "use_trade_level_action_policy",
                model_alpha_exit_defaults.get("use_trade_level_action_policy", True),
            )
        )
        exit_risk_scaling_mode = str(model_alpha_exit_defaults.get("risk_scaling_mode", "fixed")).strip().lower() or "fixed"
        exit_risk_vol_feature = str(model_alpha_exit_defaults.get("risk_vol_feature", "rv_12")).strip() or "rv_12"
        exit_tp_vol_multiplier = _optional_float_value(model_alpha_exit_defaults.get("tp_vol_multiplier"))
        exit_sl_vol_multiplier = _optional_float_value(model_alpha_exit_defaults.get("sl_vol_multiplier"))
        exit_trailing_vol_multiplier = _optional_float_value(model_alpha_exit_defaults.get("trailing_vol_multiplier"))
        exec_timeout_bars = int(
            getattr(args, "execution_timeout_bars", None)
            if getattr(args, "execution_timeout_bars", None) is not None
            else model_alpha_execution_defaults.get("timeout_bars", 2)
        )
        exec_replace_max = int(
            getattr(args, "execution_replace_max", None)
            if getattr(args, "execution_replace_max", None) is not None
            else model_alpha_execution_defaults.get("replace_max", 2)
        )
        exec_price_mode = str(
            getattr(args, "execution_price_mode", None)
            or model_alpha_execution_defaults.get("price_mode", "JOIN")
        ).strip().upper() or "JOIN"
        execution_use_learned_recommendations = bool(
            model_alpha_execution_defaults.get("use_learned_recommendations", True)
        )
        paper_tf_value = str(args.tf or defaults.get("tf", "5m")).strip().lower() or "5m"
        max_positions_value = max(
            int(args.max_positions if args.max_positions is not None else defaults["max_positions"]),
            1,
        )
        if strategy_mode == "model_alpha_v1":
            max_positions_value = max(position_max_total, 1)
            if getattr(args, "paper_feature_provider", None) is None:
                print(
                    "[paper][warn] --paper-feature-provider not set; "
                    f"using default='{defaults.get('paper_feature_provider', 'offline_parquet')}'."
                )
            if getattr(args, "paper_micro_provider", None) is None:
                print(
                    "[paper][warn] --paper-micro-provider not set; "
                    f"using default='{defaults.get('paper_micro_provider', 'offline_parquet')}'."
                )

        run_settings = PaperRunSettings(
            duration_sec=max(int(args.duration_sec), 0),
            quote=str(args.quote or defaults["quote"]).strip().upper(),
            top_n=max(int(args.top_n if args.top_n is not None else defaults["top_n"]), 1),
            tf=paper_tf_value,
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
            max_positions=max_positions_value,
            min_order_krw=max(float(defaults["min_order_krw"]), 0.0),
            order_timeout_sec=max(float(defaults["order_timeout_sec"]), 1.0),
            reprice_max_attempts=max(int(defaults["reprice_max_attempts"]), 0),
            cooldown_sec_after_fail=max(int(defaults["cooldown_sec_after_fail"]), 0),
            max_consecutive_failures=max(int(defaults["max_consecutive_failures"]), 1),
            out_root_dir=str(defaults["paper_out_dir"]),
            strategy=strategy_mode,
            model_ref=model_ref_value or None,
            model_family=model_family_value,
            feature_set=feature_set_value,
            model_registry_root=str(registry_root_value),
            model_feature_dataset_root=(
                str(defaults.get("model_feature_dataset_root")).strip()
                if defaults.get("model_feature_dataset_root") is not None
                else None
            ),
            model_alpha=ModelAlphaSettings(
                model_ref=str(model_ref_value or model_alpha_defaults.get("model_ref", DEFAULT_MODEL_ALPHA_RUNTIME_REF)).strip()
                or DEFAULT_MODEL_ALPHA_RUNTIME_REF,
                model_family=model_family_value,
                feature_set=str(model_alpha_defaults.get("feature_set", feature_set_value)).strip().lower() or "v3",
                selection=ModelAlphaSelectionSettings(
                    top_pct=max(min(selection_top_pct, 1.0), 0.0),
                    min_prob=selection_min_prob,
                    min_candidates_per_ts=max(selection_min_cands, 0),
                    registry_threshold_key=selection_registry_threshold_key,
                    use_learned_recommendations=selection_use_learned_recommendations,
                ),
                position=ModelAlphaPositionSettings(
                    max_positions_total=max(position_max_total, 1),
                    cooldown_bars=max(position_cooldown_bars, 0),
                    entry_min_notional_buffer_bps=position_entry_min_notional_buffer_bps,
                    sizing_mode=position_sizing_mode,
                    size_multiplier_min=position_size_multiplier_min,
                    size_multiplier_max=position_size_multiplier_max,
                ),
                exit=ModelAlphaExitSettings(
                    mode=exit_mode,
                    hold_bars=max(hold_bars_value, 0),
                    use_learned_exit_mode=exit_use_learned_exit_mode,
                    use_learned_hold_bars=exit_use_learned_hold_bars,
                    use_learned_risk_recommendations=exit_use_learned_risk_recommendations,
                    use_trade_level_action_policy=exit_use_trade_level_action_policy,
                    risk_scaling_mode=exit_risk_scaling_mode,
                    risk_vol_feature=exit_risk_vol_feature,
                    tp_vol_multiplier=exit_tp_vol_multiplier,
                    sl_vol_multiplier=exit_sl_vol_multiplier,
                    trailing_vol_multiplier=exit_trailing_vol_multiplier,
                    tp_pct=max(tp_pct_value, 0.0),
                    sl_pct=max(sl_pct_value, 0.0),
                    trailing_pct=max(trailing_pct_value, 0.0),
                    expected_exit_slippage_bps=(
                        max(expected_exit_slippage_bps_value, 0.0)
                        if expected_exit_slippage_bps_value is not None
                        else None
                    ),
                    expected_exit_fee_bps=(
                        max(expected_exit_fee_bps_value, 0.0)
                        if expected_exit_fee_bps_value is not None
                        else None
                    ),
                ),
                execution=ModelAlphaExecutionSettings(
                    price_mode=exec_price_mode,
                    timeout_bars=max(exec_timeout_bars, 1),
                    replace_max=max(exec_replace_max, 0),
                    use_learned_recommendations=execution_use_learned_recommendations,
                ),
                operational=ModelAlphaOperationalSettings(
                    enabled=bool(model_alpha_operational_defaults.get("enabled", True)),
                    risk_multiplier_min=max(
                        float(model_alpha_operational_defaults.get("risk_multiplier_min", 0.80)),
                        0.0,
                    ),
                    risk_multiplier_max=max(
                        float(model_alpha_operational_defaults.get("risk_multiplier_max", 1.20)),
                        max(float(model_alpha_operational_defaults.get("risk_multiplier_min", 0.80)), 0.0),
                    ),
                    max_positions_scale_min=max(
                        float(model_alpha_operational_defaults.get("max_positions_scale_min", 0.50)),
                        0.10,
                    ),
                    max_positions_scale_max=max(
                        float(model_alpha_operational_defaults.get("max_positions_scale_max", 1.50)),
                        max(float(model_alpha_operational_defaults.get("max_positions_scale_min", 0.50)), 0.10),
                    ),
                    session_overlap_boost=max(
                        float(model_alpha_operational_defaults.get("session_overlap_boost", 0.10)),
                        0.0,
                    ),
                    session_offpeak_penalty=max(
                        float(model_alpha_operational_defaults.get("session_offpeak_penalty", 0.05)),
                        0.0,
                    ),
                    micro_quality_block_threshold=max(
                        float(model_alpha_operational_defaults.get("micro_quality_block_threshold", 0.15)),
                        0.0,
                    ),
                    micro_quality_conservative_threshold=max(
                        float(model_alpha_operational_defaults.get("micro_quality_conservative_threshold", 0.35)),
                        0.0,
                    ),
                    micro_quality_aggressive_threshold=max(
                        float(model_alpha_operational_defaults.get("micro_quality_aggressive_threshold", 0.75)),
                        0.0,
                    ),
                    max_execution_spread_bps_for_join=max(
                        float(model_alpha_operational_defaults.get("max_execution_spread_bps_for_join", 20.0)),
                        0.0,
                    ),
                    max_execution_spread_bps_for_cross=max(
                        float(model_alpha_operational_defaults.get("max_execution_spread_bps_for_cross", 6.0)),
                        0.0,
                    ),
                    min_execution_depth_krw_for_cross=max(
                        float(model_alpha_operational_defaults.get("min_execution_depth_krw_for_cross", 1_500_000.0)),
                        0.0,
                    ),
                    snapshot_stale_ms=max(
                        int(model_alpha_operational_defaults.get("snapshot_stale_ms", 15_000)),
                        0,
                    ),
                    conservative_timeout_scale=max(
                        float(model_alpha_operational_defaults.get("conservative_timeout_scale", 1.25)),
                        0.10,
                    ),
                    aggressive_timeout_scale=max(
                        float(model_alpha_operational_defaults.get("aggressive_timeout_scale", 0.75)),
                        0.10,
                    ),
                    conservative_replace_interval_scale=max(
                        float(model_alpha_operational_defaults.get("conservative_replace_interval_scale", 1.50)),
                        0.10,
                    ),
                    aggressive_replace_interval_scale=max(
                        float(model_alpha_operational_defaults.get("aggressive_replace_interval_scale", 0.50)),
                        0.10,
                    ),
                    conservative_max_replaces_scale=max(
                        float(model_alpha_operational_defaults.get("conservative_max_replaces_scale", 0.50)),
                        0.0,
                    ),
                    aggressive_max_replaces_bonus=max(
                        int(model_alpha_operational_defaults.get("aggressive_max_replaces_bonus", 1)),
                        0,
                    ),
                    conservative_max_chase_bps_scale=max(
                        float(model_alpha_operational_defaults.get("conservative_max_chase_bps_scale", 0.75)),
                        0.0,
                    ),
                    aggressive_max_chase_bps_bonus=max(
                        int(model_alpha_operational_defaults.get("aggressive_max_chase_bps_bonus", 5)),
                        0,
                    ),
                    runtime_timeout_ms_floor=max(
                        int(model_alpha_operational_defaults.get("runtime_timeout_ms_floor", 5_000)),
                        1_000,
                    ),
                    runtime_replace_interval_ms_floor=max(
                        int(model_alpha_operational_defaults.get("runtime_replace_interval_ms_floor", 1_500)),
                        1,
                    ),
                ),
            ),
            micro_gate=_build_micro_gate_settings(
                defaults=defaults["micro_gate"],
                cli_enabled=getattr(args, "micro_gate", None),
                cli_mode=getattr(args, "micro_gate_mode", None),
                cli_on_missing=getattr(args, "micro_gate_on_missing", None),
            ),
            micro_order_policy=_build_micro_order_policy_settings(
                defaults=defaults["micro_order_policy"],
                cli_enabled=getattr(args, "micro_order_policy", None),
                cli_mode=getattr(args, "micro_order_policy_mode", None),
                cli_on_missing=getattr(args, "micro_order_policy_on_missing", None),
            ),
            paper_micro_provider=str(
                getattr(args, "paper_micro_provider", None) or defaults.get("paper_micro_provider", "offline_parquet")
            ).strip().lower(),
            paper_micro_warmup_sec=max(
                int(
                    getattr(args, "paper_micro_warmup_sec", None)
                    if getattr(args, "paper_micro_warmup_sec", None) is not None
                    else defaults.get("paper_micro_warmup_sec", 60)
                ),
                0,
            ),
            paper_micro_warmup_min_trade_events_per_market=max(
                int(
                    getattr(args, "paper_micro_warmup_min_trade_events_per_market", None)
                    if getattr(args, "paper_micro_warmup_min_trade_events_per_market", None) is not None
                    else defaults.get("paper_micro_warmup_min_trade_events_per_market", 1)
                ),
                1,
            ),
            paper_micro_auto_health_path=str(
                defaults.get("paper_micro_auto_health_path", "data/raw_ws/upbit/_meta/ws_public_health.json")
            ),
            paper_micro_auto_health_stale_sec=max(int(defaults.get("paper_micro_auto_health_stale_sec", 180)), 1),
            paper_feature_provider=str(
                getattr(args, "paper_feature_provider", None) or defaults.get("paper_feature_provider", "offline_parquet")
            ).strip().lower(),
            paper_live_parquet_root=str(defaults.get("paper_live_parquet_root", "data/parquet")).strip() or "data/parquet",
            paper_live_candles_dataset=str(defaults.get("paper_live_candles_dataset", "candles_api_v1")).strip()
            or "candles_api_v1",
            paper_live_bootstrap_1m_bars=max(int(defaults.get("paper_live_bootstrap_1m_bars", 2000)), 256),
            paper_live_micro_max_age_ms=max(int(defaults.get("paper_live_micro_max_age_ms", 300000)), 0),
        )

        summary = run_live_paper_sync(upbit_settings=settings, run_settings=run_settings)
        _print_json(asdict(summary))
        return 0
    except (ConfigError, UpbitError, ValueError, RuntimeError) as exc:
        print(f"[paper][error] {exc}")
        return 2


def _handle_backtest_command(args: argparse.Namespace, config_dir: Path, base_config: dict[str, Any]) -> int:
    try:
        if args.backtest_command == "alpha":
            shortcut_args = _normalize_backtest_alpha_args(args)
            print(
                "[backtest][alpha] "
                f"preset={getattr(shortcut_args, 'preset', 'default')} "
                f"micro_order_policy={getattr(shortcut_args, 'micro_order_policy', None) or 'config'}"
            )
            return _handle_backtest_command(shortcut_args, config_dir, base_config)

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
        strategy_mode = str(getattr(args, "strategy", None) or defaults.get("strategy", "candidates_v1")).strip().lower()
        model_alpha_defaults = defaults.get("model_alpha", {}) if isinstance(defaults.get("model_alpha"), dict) else {}
        model_alpha_selection_defaults = (
            model_alpha_defaults.get("selection", {}) if isinstance(model_alpha_defaults.get("selection"), dict) else {}
        )
        model_alpha_position_defaults = (
            model_alpha_defaults.get("position", {}) if isinstance(model_alpha_defaults.get("position"), dict) else {}
        )
        model_alpha_exit_defaults = (
            model_alpha_defaults.get("exit", {}) if isinstance(model_alpha_defaults.get("exit"), dict) else {}
        )
        model_alpha_execution_defaults = (
            model_alpha_defaults.get("execution", {}) if isinstance(model_alpha_defaults.get("execution"), dict) else {}
        )
        model_alpha_operational_defaults = (
            model_alpha_defaults.get("operational", {}) if isinstance(model_alpha_defaults.get("operational"), dict) else {}
        )
        model_ref_value = str(
            getattr(args, "model_ref", None)
            or defaults.get("model_ref")
            or model_alpha_defaults.get("model_ref", DEFAULT_MODEL_ALPHA_RUNTIME_REF)
        ).strip()
        model_family_raw = getattr(args, "model_family", None)
        if model_family_raw is None:
            model_family_raw = defaults.get("model_family")
        if model_family_raw in {None, ""}:
            model_family_raw = model_alpha_defaults.get("model_family")
        model_family_value = str(model_family_raw).strip() if model_family_raw else None
        model_ref_value, model_family_value = _resolve_model_ref_alias(model_ref_value, model_family_value)
        feature_set_value = str(
            getattr(args, "feature_set", None)
            or defaults.get("feature_set", model_alpha_defaults.get("feature_set", "v3"))
        ).strip().lower() or "v3"
        selection_use_learned_recommendations = _selection_use_learned_recommendations(
            args=args,
            model_alpha_selection_defaults=model_alpha_selection_defaults,
        )
        selection_top_pct = float(
            getattr(args, "top_pct", None)
            if getattr(args, "top_pct", None) is not None
            else model_alpha_selection_defaults.get("top_pct", 0.05)
        )
        selection_min_prob = _clamp_prob_value(
            _optional_float_value(
                getattr(args, "min_prob", None)
                if getattr(args, "min_prob", None) is not None
                else model_alpha_selection_defaults.get("min_prob")
            )
        )
        selection_min_cands = int(
            getattr(args, "min_cands_per_ts", None)
            if getattr(args, "min_cands_per_ts", None) is not None
            else model_alpha_selection_defaults.get("min_candidates_per_ts", 10)
        )
        selection_registry_threshold_key = str(
            model_alpha_selection_defaults.get("registry_threshold_key", "top_5pct")
        ).strip() or "top_5pct"
        position_max_total = int(
            getattr(args, "max_positions_total", None)
            if getattr(args, "max_positions_total", None) is not None
            else model_alpha_position_defaults.get("max_positions_total", defaults["max_positions"])
        )
        position_cooldown_bars = int(
            getattr(args, "cooldown_bars", None)
            if getattr(args, "cooldown_bars", None) is not None
            else model_alpha_position_defaults.get("cooldown_bars", 6)
        )
        position_entry_min_notional_buffer_bps = max(
            float(model_alpha_position_defaults.get("entry_min_notional_buffer_bps", 25.0)),
            0.0,
        )
        position_sizing_mode = str(model_alpha_position_defaults.get("sizing_mode", "prob_ramp")).strip().lower() or "prob_ramp"
        position_size_multiplier_min = max(
            float(model_alpha_position_defaults.get("size_multiplier_min", 0.5)),
            0.0,
        )
        position_size_multiplier_max = max(
            float(model_alpha_position_defaults.get("size_multiplier_max", 1.5)),
            position_size_multiplier_min,
        )
        exit_mode = str(
            getattr(args, "exit_mode", None) or model_alpha_exit_defaults.get("mode", "hold")
        ).strip().lower() or "hold"
        hold_bars_value = int(
            getattr(args, "hold_bars", None)
            if getattr(args, "hold_bars", None) is not None
            else model_alpha_exit_defaults.get("hold_bars", 6)
        )
        tp_pct_value = float(
            getattr(args, "tp_pct", None)
            if getattr(args, "tp_pct", None) is not None
            else model_alpha_exit_defaults.get("tp_pct", 0.02)
        )
        sl_pct_value = float(
            getattr(args, "sl_pct", None)
            if getattr(args, "sl_pct", None) is not None
            else model_alpha_exit_defaults.get("sl_pct", 0.01)
        )
        trailing_pct_value = float(
            getattr(args, "trailing_pct", None)
            if getattr(args, "trailing_pct", None) is not None
            else model_alpha_exit_defaults.get("trailing_pct", 0.0)
        )
        expected_exit_slippage_bps_value = _optional_float_value(
            model_alpha_exit_defaults.get("expected_exit_slippage_bps")
        )
        expected_exit_fee_bps_value = _optional_float_value(model_alpha_exit_defaults.get("expected_exit_fee_bps"))
        exit_use_learned_exit_mode = bool(
            getattr(args, "use_learned_exit_mode", model_alpha_exit_defaults.get("use_learned_exit_mode", True))
        )
        exit_use_learned_hold_bars = bool(
            getattr(args, "use_learned_hold_bars", model_alpha_exit_defaults.get("use_learned_hold_bars", True))
        )
        exit_use_learned_risk_recommendations = bool(
            getattr(
                args,
                "use_learned_risk_recommendations",
                model_alpha_exit_defaults.get("use_learned_risk_recommendations", True),
            )
        )
        exit_use_trade_level_action_policy = bool(
            getattr(
                args,
                "use_trade_level_action_policy",
                model_alpha_exit_defaults.get("use_trade_level_action_policy", True),
            )
        )
        exit_risk_scaling_mode = str(model_alpha_exit_defaults.get("risk_scaling_mode", "fixed")).strip().lower() or "fixed"
        exit_risk_vol_feature = str(model_alpha_exit_defaults.get("risk_vol_feature", "rv_12")).strip() or "rv_12"
        exit_tp_vol_multiplier = _optional_float_value(model_alpha_exit_defaults.get("tp_vol_multiplier"))
        exit_sl_vol_multiplier = _optional_float_value(model_alpha_exit_defaults.get("sl_vol_multiplier"))
        exit_trailing_vol_multiplier = _optional_float_value(model_alpha_exit_defaults.get("trailing_vol_multiplier"))
        exec_timeout_bars = int(
            getattr(args, "execution_timeout_bars", None)
            if getattr(args, "execution_timeout_bars", None) is not None
            else model_alpha_execution_defaults.get("timeout_bars", 2)
        )
        exec_replace_max = int(
            getattr(args, "execution_replace_max", None)
            if getattr(args, "execution_replace_max", None) is not None
            else model_alpha_execution_defaults.get("replace_max", 2)
        )
        exec_price_mode = str(
            getattr(args, "execution_price_mode", None)
            or model_alpha_execution_defaults.get("price_mode", "JOIN")
        ).strip().upper() or "JOIN"
        execution_use_learned_recommendations = bool(
            model_alpha_execution_defaults.get("use_learned_recommendations", True)
        )

        max_positions_value = max(
            int(args.max_positions if args.max_positions is not None else defaults["max_positions"]),
            1,
        )
        order_timeout_bars_value = max(
            int(args.order_timeout_bars if args.order_timeout_bars is not None else defaults["order_timeout_bars"]),
            1,
        )
        reprice_max_attempts_value = max(
            int(args.reprice_max_attempts if args.reprice_max_attempts is not None else defaults["reprice_max_attempts"]),
            0,
        )
        if strategy_mode == "model_alpha_v1":
            max_positions_value = max(position_max_total, 1)
            if getattr(args, "order_timeout_bars", None) is None:
                order_timeout_bars_value = max(exec_timeout_bars, 1)
            if getattr(args, "reprice_max_attempts", None) is None:
                reprice_max_attempts_value = max(exec_replace_max, 0)

        from_ts_ms_value = (
            getattr(args, "from_ts_ms", None)
            if getattr(args, "from_ts_ms", None) is not None
            else defaults["from_ts_ms"]
        )
        to_ts_ms_value = (
            getattr(args, "to_ts_ms", None)
            if getattr(args, "to_ts_ms", None) is not None
            else defaults["to_ts_ms"]
        )
        if from_ts_ms_value is None and getattr(args, "start", None):
            from_ts_ms_value = parse_date_to_ts_ms(str(getattr(args, "start")).strip())
        if to_ts_ms_value is None and getattr(args, "end", None):
            to_ts_ms_value = parse_date_to_ts_ms(str(getattr(args, "end")).strip(), end_of_day=True)

        dataset_name_value = str(getattr(args, "dataset_name", None) or defaults["dataset_name"]).strip()
        parquet_root_value = str(getattr(args, "parquet_root", None) or defaults["parquet_root"]).strip()
        if (
            strategy_mode == "model_alpha_v1"
            and feature_set_value in {"v3", "v4"}
            and getattr(args, "dataset_name", None) is None
        ):
            features_runtime_cfg = (
                load_features_v4_config(config_dir, base_config=base_config)
                if feature_set_value == "v4"
                else load_features_v3_config(config_dir, base_config=base_config)
            )
            dataset_name_value = _resolve_backtest_dataset_name_for_model_features(
                parquet_root=Path(parquet_root_value),
                base_candles_dataset=str(features_runtime_cfg.build.base_candles_dataset),
                fallback=dataset_name_value,
            )

        run_settings = BacktestRunSettings(
            dataset_name=dataset_name_value,
            parquet_root=parquet_root_value,
            tf=str(args.tf or defaults["tf"]).strip().lower(),
            from_ts_ms=from_ts_ms_value,
            to_ts_ms=to_ts_ms_value,
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
            max_positions=max_positions_value,
            min_order_krw=max(
                float(args.min_order_krw if args.min_order_krw is not None else defaults["min_order_krw"]),
                0.0,
            ),
            order_timeout_bars=order_timeout_bars_value,
            reprice_max_attempts=reprice_max_attempts_value,
            reprice_tick_steps=max(int(defaults["reprice_tick_steps"]), 1),
            rules_ttl_sec=max(int(defaults["rules_ttl_sec"]), 1),
            momentum_window_sec=max(int(defaults["momentum_window_sec"]), 1),
            min_momentum_pct=float(defaults["min_momentum_pct"]),
            strategy=strategy_mode,
            model_ref=model_ref_value or None,
            model_family=model_family_value,
            feature_set=feature_set_value,
            model_registry_root=str(defaults.get("model_registry_root", "models/registry")),
            model_feature_dataset_root=(
                str(defaults.get("model_feature_dataset_root")).strip()
                if defaults.get("model_feature_dataset_root") is not None
                else None
            ),
            model_alpha=ModelAlphaSettings(
                model_ref=str(model_ref_value or model_alpha_defaults.get("model_ref", DEFAULT_MODEL_ALPHA_RUNTIME_REF)).strip()
                or DEFAULT_MODEL_ALPHA_RUNTIME_REF,
                model_family=model_family_value,
                feature_set=str(model_alpha_defaults.get("feature_set", feature_set_value)).strip().lower() or "v3",
                selection=ModelAlphaSelectionSettings(
                    top_pct=max(min(selection_top_pct, 1.0), 0.0),
                    min_prob=selection_min_prob,
                    min_candidates_per_ts=max(selection_min_cands, 0),
                    registry_threshold_key=selection_registry_threshold_key,
                    use_learned_recommendations=selection_use_learned_recommendations,
                ),
                position=ModelAlphaPositionSettings(
                    max_positions_total=max(position_max_total, 1),
                    cooldown_bars=max(position_cooldown_bars, 0),
                    entry_min_notional_buffer_bps=position_entry_min_notional_buffer_bps,
                    sizing_mode=position_sizing_mode,
                    size_multiplier_min=position_size_multiplier_min,
                    size_multiplier_max=position_size_multiplier_max,
                ),
                exit=ModelAlphaExitSettings(
                    mode=exit_mode,
                    hold_bars=max(hold_bars_value, 0),
                    use_learned_exit_mode=exit_use_learned_exit_mode,
                    use_learned_hold_bars=exit_use_learned_hold_bars,
                    use_learned_risk_recommendations=exit_use_learned_risk_recommendations,
                    use_trade_level_action_policy=exit_use_trade_level_action_policy,
                    risk_scaling_mode=exit_risk_scaling_mode,
                    risk_vol_feature=exit_risk_vol_feature,
                    tp_vol_multiplier=exit_tp_vol_multiplier,
                    sl_vol_multiplier=exit_sl_vol_multiplier,
                    trailing_vol_multiplier=exit_trailing_vol_multiplier,
                    tp_pct=max(tp_pct_value, 0.0),
                    sl_pct=max(sl_pct_value, 0.0),
                    trailing_pct=max(trailing_pct_value, 0.0),
                    expected_exit_slippage_bps=(
                        max(expected_exit_slippage_bps_value, 0.0)
                        if expected_exit_slippage_bps_value is not None
                        else None
                    ),
                    expected_exit_fee_bps=(
                        max(expected_exit_fee_bps_value, 0.0)
                        if expected_exit_fee_bps_value is not None
                        else None
                    ),
                ),
                execution=ModelAlphaExecutionSettings(
                    price_mode=exec_price_mode,
                    timeout_bars=max(exec_timeout_bars, 1),
                    replace_max=max(exec_replace_max, 0),
                    use_learned_recommendations=execution_use_learned_recommendations,
                ),
                operational=ModelAlphaOperationalSettings(
                    enabled=bool(model_alpha_operational_defaults.get("enabled", True)),
                    risk_multiplier_min=max(
                        float(model_alpha_operational_defaults.get("risk_multiplier_min", 0.80)),
                        0.0,
                    ),
                    risk_multiplier_max=max(
                        float(model_alpha_operational_defaults.get("risk_multiplier_max", 1.20)),
                        max(float(model_alpha_operational_defaults.get("risk_multiplier_min", 0.80)), 0.0),
                    ),
                    max_positions_scale_min=max(
                        float(model_alpha_operational_defaults.get("max_positions_scale_min", 0.50)),
                        0.10,
                    ),
                    max_positions_scale_max=max(
                        float(model_alpha_operational_defaults.get("max_positions_scale_max", 1.50)),
                        max(float(model_alpha_operational_defaults.get("max_positions_scale_min", 0.50)), 0.10),
                    ),
                    session_overlap_boost=max(
                        float(model_alpha_operational_defaults.get("session_overlap_boost", 0.10)),
                        0.0,
                    ),
                    session_offpeak_penalty=max(
                        float(model_alpha_operational_defaults.get("session_offpeak_penalty", 0.05)),
                        0.0,
                    ),
                    micro_quality_block_threshold=max(
                        float(model_alpha_operational_defaults.get("micro_quality_block_threshold", 0.15)),
                        0.0,
                    ),
                    micro_quality_conservative_threshold=max(
                        float(model_alpha_operational_defaults.get("micro_quality_conservative_threshold", 0.35)),
                        0.0,
                    ),
                    micro_quality_aggressive_threshold=max(
                        float(model_alpha_operational_defaults.get("micro_quality_aggressive_threshold", 0.75)),
                        0.0,
                    ),
                    max_execution_spread_bps_for_join=max(
                        float(model_alpha_operational_defaults.get("max_execution_spread_bps_for_join", 20.0)),
                        0.0,
                    ),
                    max_execution_spread_bps_for_cross=max(
                        float(model_alpha_operational_defaults.get("max_execution_spread_bps_for_cross", 6.0)),
                        0.0,
                    ),
                    min_execution_depth_krw_for_cross=max(
                        float(model_alpha_operational_defaults.get("min_execution_depth_krw_for_cross", 1_500_000.0)),
                        0.0,
                    ),
                    snapshot_stale_ms=max(
                        int(model_alpha_operational_defaults.get("snapshot_stale_ms", 15_000)),
                        1,
                    ),
                    conservative_timeout_scale=max(
                        float(model_alpha_operational_defaults.get("conservative_timeout_scale", 1.25)),
                        0.10,
                    ),
                    aggressive_timeout_scale=max(
                        float(model_alpha_operational_defaults.get("aggressive_timeout_scale", 0.75)),
                        0.10,
                    ),
                    conservative_replace_interval_scale=max(
                        float(model_alpha_operational_defaults.get("conservative_replace_interval_scale", 1.50)),
                        0.10,
                    ),
                    aggressive_replace_interval_scale=max(
                        float(model_alpha_operational_defaults.get("aggressive_replace_interval_scale", 0.50)),
                        0.10,
                    ),
                    conservative_max_replaces_scale=max(
                        float(model_alpha_operational_defaults.get("conservative_max_replaces_scale", 0.50)),
                        0.0,
                    ),
                    aggressive_max_replaces_bonus=max(
                        int(model_alpha_operational_defaults.get("aggressive_max_replaces_bonus", 1)),
                        0,
                    ),
                    conservative_max_chase_bps_scale=max(
                        float(model_alpha_operational_defaults.get("conservative_max_chase_bps_scale", 0.75)),
                        0.0,
                    ),
                    aggressive_max_chase_bps_bonus=max(
                        int(model_alpha_operational_defaults.get("aggressive_max_chase_bps_bonus", 5)),
                        0,
                    ),
                    runtime_timeout_ms_floor=max(
                        int(model_alpha_operational_defaults.get("runtime_timeout_ms_floor", 5_000)),
                        1_000,
                    ),
                    runtime_replace_interval_ms_floor=max(
                        int(model_alpha_operational_defaults.get("runtime_replace_interval_ms_floor", 1_500)),
                        1,
                    ),
                ),
            ),
            output_root_dir=str(defaults["backtest_out_dir"]),
            seed=int(defaults["seed"]),
            micro_gate=_build_micro_gate_settings(
                defaults=defaults["micro_gate"],
                cli_enabled=getattr(args, "micro_gate", None),
                cli_mode=getattr(args, "micro_gate_mode", None),
                cli_on_missing=getattr(args, "micro_gate_on_missing", None),
            ),
            micro_order_policy=_build_micro_order_policy_settings(
                defaults=defaults["micro_order_policy"],
                cli_enabled=getattr(args, "micro_order_policy", None),
                cli_mode=getattr(args, "micro_order_policy_mode", None),
                cli_on_missing=getattr(args, "micro_order_policy_on_missing", None),
            ),
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
        project_root = config_dir.resolve().parent
        bot_id = str(getattr(args, "bot_id", None) or defaults["bot_id"]).strip()
        if not bot_id:
            raise ValueError("live.bot_id must not be blank")
        db_path = Path(str(defaults["state_db_path"]))
        command = str(args.live_command)
        if command == "admissibility":
            settings = load_upbit_settings(config_dir)
            credentials = require_upbit_credentials(settings)
            market = str(args.market).strip().upper()
            side = str(args.side).strip().lower()
            requested_volume = getattr(args, "volume", None)
            target_notional_quote = getattr(args, "target_notional_quote", None)
            if (requested_volume is None) == (target_notional_quote is None):
                raise ValueError("provide exactly one of --volume or --target-notional-quote")
            with UpbitHttpClient(settings, credentials=credentials) as private_http:
                private_client = UpbitPrivateClient(private_http)
                chance_payload = private_client.chance(market=market)
                accounts_payload = private_client.accounts()
                with UpbitHttpClient(settings) as public_http:
                    instruments_payload = UpbitPublicClient(public_http).orderbook_instruments([market])
            snapshot = build_live_order_admissibility_snapshot(
                market=market,
                side=side,
                chance_payload=chance_payload if isinstance(chance_payload, dict) else {},
                instruments_payload=instruments_payload,
                accounts_payload=accounts_payload,
            )
            sizing_payload: dict[str, Any] | None = None
            resolved_volume = requested_volume
            if target_notional_quote is not None:
                fee_rate = snapshot.bid_fee if side == "bid" else snapshot.ask_fee
                sizing = derive_volume_from_target_notional(
                    side=side,
                    price=float(args.price),
                    target_notional_quote=float(target_notional_quote),
                    fee_rate=fee_rate,
                )
                sizing_payload = sizing_envelope_to_payload(sizing)
                resolved_volume = float(sizing.admissible_volume)
            decision = evaluate_live_limit_order(
                snapshot=snapshot,
                price=float(args.price),
                volume=float(resolved_volume),
                expected_edge_bps=getattr(args, "expected_edge_bps", None),
                replace_risk_steps=max(int(getattr(args, "replace_risk_steps", 0)), 0),
            )
            now_ts = int(time.time() * 1000)
            persisted_test_order: dict[str, Any] | None = None
            with LiveStateStore(db_path) as store:
                record_small_account_decision(
                    store=store,
                    decision=decision,
                    source="cli_admissibility",
                    ts_ms=now_ts,
                    market=market,
                )
                build_small_account_runtime_report(
                    store=store,
                    canary_enabled=bool(defaults["small_account_canary_enabled"]),
                    max_positions=int(defaults["small_account_max_positions"]),
                    max_open_orders_per_market=int(defaults["small_account_max_open_orders_per_market"]),
                    local_positions=store.list_positions(),
                    exchange_bot_open_orders=store.list_orders(open_only=True),
                    ts_ms=now_ts,
                    persist=True,
                )
                test_order_payload: dict[str, Any] | None = None
                if bool(getattr(args, "test_order", False)):
                    test_order_payload = {
                        "requested": True,
                        "called": False,
                    }
                    if decision.admissible:
                        with UpbitHttpClient(settings, credentials=credentials) as private_http:
                            private_client = UpbitPrivateClient(private_http)
                            payload = private_client.order_test(
                                market=market,
                                side=side,
                                ord_type="limit",
                                price=f"{decision.adjusted_price:.16f}".rstrip("0").rstrip("."),
                                volume=f"{decision.adjusted_volume:.16f}".rstrip("0").rstrip("."),
                                time_in_force=getattr(args, "time_in_force", None),
                                identifier=getattr(args, "identifier", None),
                            )
                        test_order_payload = {
                            "requested": True,
                            "called": True,
                            "ok": True,
                            "payload": payload,
                        }
                        persisted_test_order = build_rollout_test_order_record(
                            market=market,
                            side=side,
                            ord_type="limit",
                            price=f"{decision.adjusted_price:.16f}".rstrip("0").rstrip("."),
                            volume=f"{decision.adjusted_volume:.16f}".rstrip("0").rstrip("."),
                            ok=True,
                            response_payload=payload if isinstance(payload, dict) else {},
                            ts_ms=now_ts,
                        )
                        store.set_live_test_order(payload=persisted_test_order, ts_ms=now_ts)
                        write_rollout_latest(
                            project_root=project_root,
                            event_kind="TEST_ORDER",
                            contract=store.live_rollout_contract(),
                            test_order=persisted_test_order,
                            status=store.live_rollout_status(),
                            ts_ms=now_ts,
                        )
            _print_json(
                build_live_admissibility_report(
                    snapshot=snapshot,
                    decision=decision,
                    test_order_payload=test_order_payload,
                    sizing_payload=sizing_payload,
                )
            )
            return 0 if decision.admissible else 2
        apply_mode = bool(getattr(args, "apply", False)) and not bool(getattr(args, "dry_run", False))
        if bool(getattr(args, "apply", False)) and bool(getattr(args, "dry_run", False)):
            raise ValueError("cannot use --apply and --dry-run together")
        allow_cancel_external_cli = bool(getattr(args, "allow_cancel_external", False))

        with LiveStateStore(db_path) as store:
            if command == "rollout":
                rollout_command = str(getattr(args, "live_rollout_command", "")).strip().lower()
                now_ts = int(time.time() * 1000)
                if rollout_command == "status":
                    effective_rollout_mode, effective_rollout_target_unit = resolve_rollout_gate_inputs(
                        default_mode=str(defaults["rollout_mode"]),
                        default_target_unit=str(defaults["rollout_target_unit"]),
                        contract=store.live_rollout_contract(),
                    )
                    gate = evaluate_live_rollout_gate(
                        mode=effective_rollout_mode,
                        target_unit=effective_rollout_target_unit,
                        contract=store.live_rollout_contract(),
                        test_order=store.live_test_order(),
                        breaker_active=bool(breaker_status(store).get("active", False)),
                        require_test_order=bool(defaults["rollout_require_test_order"]),
                        test_order_max_age_sec=int(defaults["rollout_test_order_max_age_sec"]),
                        small_account_single_slot_ready=bool(defaults["small_account_canary_enabled"])
                        and int(defaults["small_account_max_positions"]) == 1
                        and int(defaults["small_account_max_open_orders_per_market"]) == 1,
                        ts_ms=now_ts,
                    )
                    payload = {
                        "bot_id": bot_id,
                        "db_path": str(db_path),
                        "configured_mode": str(defaults["rollout_mode"]),
                        "configured_target_unit": str(defaults["rollout_target_unit"]),
                        "effective_mode": effective_rollout_mode,
                        "effective_target_unit": effective_rollout_target_unit,
                        "contract": store.live_rollout_contract(),
                        "test_order": store.live_test_order(),
                        "status": rollout_gate_to_payload(gate),
                        "artifact_path": str(
                            rollout_latest_artifact_path(project_root, target_unit=effective_rollout_target_unit)
                        ),
                    }
                    _print_json(payload)
                    return 0
                if rollout_command == "arm":
                    contract = build_rollout_contract(
                        mode=str(getattr(args, "mode", "canary")),
                        target_unit=str(getattr(args, "target_unit", DEFAULT_LIVE_TARGET_UNIT)),
                        arm_token=str(getattr(args, "arm_token", "")),
                        note=getattr(args, "note", None),
                        canary_max_notional_quote=getattr(args, "canary_max_notional_quote", None),
                        ts_ms=now_ts,
                    )
                    store.set_live_rollout_contract(payload=contract, ts_ms=now_ts)
                    gate = evaluate_live_rollout_gate(
                        mode=str(contract["mode"]),
                        target_unit=str(contract["target_unit"]),
                        contract=contract,
                        test_order=store.live_test_order(),
                        breaker_active=bool(breaker_status(store).get("active", False)),
                        require_test_order=bool(defaults["rollout_require_test_order"]),
                        test_order_max_age_sec=int(defaults["rollout_test_order_max_age_sec"]),
                        small_account_single_slot_ready=bool(defaults["small_account_canary_enabled"])
                        and int(defaults["small_account_max_positions"]) == 1
                        and int(defaults["small_account_max_open_orders_per_market"]) == 1,
                        ts_ms=now_ts,
                    )
                    status_payload = rollout_gate_to_payload(gate)
                    store.set_live_rollout_status(payload=status_payload, ts_ms=now_ts)
                    artifact_path = write_rollout_latest(
                        project_root=project_root,
                        event_kind="ARM",
                        contract=contract,
                        test_order=store.live_test_order(),
                        status=status_payload,
                        ts_ms=now_ts,
                    )
                    _print_json(
                        {
                            "bot_id": bot_id,
                            "db_path": str(db_path),
                            "contract": contract,
                            "status": status_payload,
                            "artifact_path": str(artifact_path),
                        }
                    )
                    return 0
                if rollout_command == "disarm":
                    existing_contract = store.live_rollout_contract() or {}
                    existing_hash = str(existing_contract.get("arm_token_sha256") or "").strip()
                    provided_hash = hash_arm_token(getattr(args, "arm_token", None))
                    if existing_hash and existing_hash != str(provided_hash or ""):
                        raise ValueError("arm token mismatch for live rollout disarm")
                    contract = build_rollout_disarmed_contract(
                        previous_contract=existing_contract,
                        note=getattr(args, "note", None),
                        ts_ms=now_ts,
                    )
                    store.set_live_rollout_contract(payload=contract, ts_ms=now_ts)
                    status_payload = {
                        "mode": str(contract.get("mode") or DEFAULT_ROLLOUT_MODE),
                        "target_unit": str(contract.get("target_unit") or DEFAULT_LIVE_TARGET_UNIT),
                        "armed": False,
                        "start_allowed": str(contract.get("mode") or DEFAULT_ROLLOUT_MODE) == "shadow",
                        "order_emission_allowed": False,
                        "reason_codes": [],
                    }
                    store.set_live_rollout_status(payload=status_payload, ts_ms=now_ts)
                    artifact_path = write_rollout_latest(
                        project_root=project_root,
                        event_kind="DISARM",
                        contract=contract,
                        test_order=store.live_test_order(),
                        status=status_payload,
                        ts_ms=now_ts,
                    )
                    _print_json(
                        {
                            "bot_id": bot_id,
                            "db_path": str(db_path),
                            "contract": contract,
                            "status": status_payload,
                            "artifact_path": str(artifact_path),
                        }
                    )
                    return 0
                if rollout_command == "test-order":
                    settings = load_upbit_settings(config_dir)
                    _ensure_upbit_runtime_available()
                    credentials = require_upbit_credentials(settings)
                    with UpbitHttpClient(settings, credentials=credentials) as private_http:
                        private_client = UpbitPrivateClient(private_http)
                        payload = private_client.order_test(
                            market=str(args.market).strip().upper(),
                            side=str(args.side).strip().lower(),
                            ord_type=str(args.ord_type).strip().lower(),
                            price=str(args.price).strip() if getattr(args, "price", None) is not None else None,
                            volume=str(args.volume).strip() if getattr(args, "volume", None) is not None else None,
                            time_in_force=getattr(args, "time_in_force", None),
                            identifier=getattr(args, "identifier", None),
                        )
                    test_order_record = build_rollout_test_order_record(
                        market=str(args.market),
                        side=str(args.side),
                        ord_type=str(args.ord_type),
                        price=str(args.price).strip() if getattr(args, "price", None) is not None else None,
                        volume=str(args.volume).strip() if getattr(args, "volume", None) is not None else None,
                        ok=True,
                        response_payload=payload if isinstance(payload, dict) else {},
                        ts_ms=now_ts,
                    )
                    store.set_live_test_order(payload=test_order_record, ts_ms=now_ts)
                    gate = evaluate_live_rollout_gate(
                        mode=str((store.live_rollout_contract() or {}).get("mode") or defaults["rollout_mode"]),
                        target_unit=str((store.live_rollout_contract() or {}).get("target_unit") or defaults["rollout_target_unit"]),
                        contract=store.live_rollout_contract(),
                        test_order=test_order_record,
                        breaker_active=bool(breaker_status(store).get("active", False)),
                        require_test_order=bool(defaults["rollout_require_test_order"]),
                        test_order_max_age_sec=int(defaults["rollout_test_order_max_age_sec"]),
                        small_account_single_slot_ready=bool(defaults["small_account_canary_enabled"])
                        and int(defaults["small_account_max_positions"]) == 1
                        and int(defaults["small_account_max_open_orders_per_market"]) == 1,
                        ts_ms=now_ts,
                    )
                    status_payload = rollout_gate_to_payload(gate)
                    store.set_live_rollout_status(payload=status_payload, ts_ms=now_ts)
                    artifact_path = write_rollout_latest(
                        project_root=project_root,
                        event_kind="TEST_ORDER",
                        contract=store.live_rollout_contract(),
                        test_order=test_order_record,
                        status=status_payload,
                        ts_ms=now_ts,
                    )
                    _print_json(
                        {
                            "bot_id": bot_id,
                            "db_path": str(db_path),
                            "test_order": test_order_record,
                            "status": status_payload,
                            "artifact_path": str(artifact_path),
                        }
                    )
                    return 0
                raise ValueError(f"Unsupported live rollout command: {rollout_command}")

            if command == "kill-switch":
                kill_switch_command = str(getattr(args, "live_kill_switch_command", "")).strip().lower()
                now_ts = int(time.time() * 1000)
                if kill_switch_command == "status":
                    _print_json({"bot_id": bot_id, "db_path": str(db_path), "breaker_status": breaker_status(store)})
                    return 0
                if kill_switch_command == "arm":
                    action_value = str(getattr(args, "action", ACTION_FULL_KILL_SWITCH)).strip().upper()
                    if action_value not in {
                        ACTION_HALT_NEW_INTENTS,
                        ACTION_HALT_AND_CANCEL_BOT_ORDERS,
                        ACTION_FULL_KILL_SWITCH,
                    }:
                        raise ValueError(f"unsupported kill-switch action: {action_value}")
                    payload = arm_breaker(
                        store,
                        reason_codes=[str(getattr(args, "reason_code", "MANUAL_KILL_SWITCH"))],
                        source="cli_kill_switch",
                        ts_ms=now_ts,
                        action=action_value,
                        details={"note": getattr(args, "note", None)},
                    )
                    _print_json({"bot_id": bot_id, "db_path": str(db_path), "breaker_status": payload})
                    return 0
                if kill_switch_command == "clear":
                    payload = clear_breaker(
                        store,
                        source="cli_kill_switch",
                        ts_ms=now_ts,
                        details={"note": getattr(args, "note", None)},
                    )
                    _print_json({"bot_id": bot_id, "db_path": str(db_path), "breaker_status": payload})
                    return 0
                raise ValueError(f"Unsupported live kill-switch command: {kill_switch_command}")

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
                        chance_cache: dict[str, Any] = {}

                        def _fetch_market_chance(market: str) -> Any:
                            market_value = str(market).strip().upper()
                            cached = chance_cache.get(market_value)
                            if cached is not None:
                                return cached
                            payload = client.chance(market=market_value)
                            chance_cache[market_value] = payload
                            return payload

                        pinned_contract = store.runtime_contract() or {}
                        persisted_ws_public_contract = store.ws_public_contract() or {}
                        persisted_rollout_contract = store.live_rollout_contract() or {}
                        persisted_rollout_test_order = store.live_test_order() or {}
                        current_runtime_contract: dict[str, Any] = {}
                        runtime_contract_error: str | None = None
                        try:
                            current_runtime_contract = resolve_live_runtime_model_contract(
                                registry_root=Path(str(defaults["model_registry_root"])),
                                model_ref=str(defaults["model_ref_source"]),
                                model_family=str(defaults["model_family"]),
                                ts_ms=int(time.time() * 1000),
                            )
                        except Exception as exc:
                            runtime_contract_error = str(exc)
                        ws_public_contract = load_ws_public_runtime_contract(
                            meta_dir=Path(str(defaults["ws_public_meta_dir"])),
                            raw_root=Path(str(defaults["ws_public_raw_root"])),
                            stale_threshold_sec=int(defaults["ws_public_stale_threshold_sec"]),
                            micro_aggregate_report_path=Path(str(defaults["micro_aggregate_report_path"])),
                            ts_ms=int(time.time() * 1000),
                        )
                        runtime_handoff = build_live_runtime_sync_status(
                            pinned_contract=pinned_contract,
                            current_contract=current_runtime_contract,
                            ws_public_contract=ws_public_contract,
                        )
                        rollout_mode, rollout_target_unit = resolve_rollout_gate_inputs(
                            default_mode=str(defaults["rollout_mode"]),
                            default_target_unit=str(defaults["rollout_target_unit"]),
                            contract=persisted_rollout_contract,
                        )
                        rollout_status = rollout_gate_to_payload(
                            evaluate_live_rollout_gate(
                                mode=rollout_mode,
                                target_unit=rollout_target_unit,
                                contract=persisted_rollout_contract,
                                test_order=persisted_rollout_test_order,
                                breaker_active=bool(breaker_status(store).get("active", False)),
                                require_test_order=bool(defaults["rollout_require_test_order"]),
                                test_order_max_age_sec=int(defaults["rollout_test_order_max_age_sec"]),
                                small_account_single_slot_ready=bool(defaults["small_account_canary_enabled"])
                                and int(defaults["small_account_max_positions"]) == 1
                                and int(defaults["small_account_max_open_orders_per_market"]) == 1,
                                ts_ms=int(time.time() * 1000),
                            )
                        )
                        reconcile_report = reconcile_exchange_snapshot(
                            store=store,
                            bot_id=bot_id,
                            identifier_prefix=str(defaults["identifier_prefix"]),
                            accounts_payload=accounts,
                            open_orders_payload=open_orders,
                            fetch_market_chance=_fetch_market_chance,
                            unknown_open_orders_policy=str(defaults["unknown_open_orders_policy"]),
                            unknown_positions_policy=str(defaults["unknown_positions_policy"]),
                            allow_cancel_external_orders=bool(defaults["allow_cancel_external_orders"]),
                            default_risk_sl_pct=float(defaults["default_risk_sl_pct"]),
                            default_risk_tp_pct=float(defaults["default_risk_tp_pct"]),
                            default_risk_trailing_enabled=bool(defaults["default_risk_trailing_enabled"]),
                            quote_currency=str(defaults["quote_currency"]),
                            dry_run=True,
                        )
                        small_account_report = build_small_account_runtime_report(
                            store=store,
                            canary_enabled=bool(defaults["small_account_canary_enabled"]),
                            max_positions=int(defaults["small_account_max_positions"]),
                            max_open_orders_per_market=int(defaults["small_account_max_open_orders_per_market"]),
                            local_positions=store.list_positions(),
                            exchange_bot_open_orders=list(reconcile_report.get("exchange_bot_open_orders", [])),
                            ts_ms=int(time.time() * 1000),
                            persist=False,
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
                            "small_account_report": small_account_report,
                            "runtime_handoff": runtime_handoff,
                            "live_runtime_model_run_id": runtime_handoff.get("live_runtime_model_run_id"),
                            "champion_pointer_run_id": runtime_handoff.get("champion_pointer_run_id"),
                            "ws_public_last_checkpoint_ts_ms": runtime_handoff.get("ws_public_last_checkpoint_ts_ms"),
                            "ws_public_staleness_sec": runtime_handoff.get("ws_public_staleness_sec"),
                            "model_pointer_divergence": runtime_handoff.get("model_pointer_divergence"),
                            "runtime_contract_error": runtime_contract_error,
                            "persisted_runtime_contract": pinned_contract,
                            "persisted_ws_public_contract": persisted_ws_public_contract,
                            "persisted_rollout_contract": persisted_rollout_contract,
                            "persisted_rollout_test_order": persisted_rollout_test_order,
                            "rollout": rollout_status,
                            "rollout_mode": rollout_status.get("mode"),
                            "rollout_target_unit": rollout_status.get("target_unit"),
                            "rollout_start_allowed": rollout_status.get("start_allowed"),
                            "rollout_order_emission_allowed": rollout_status.get("order_emission_allowed"),
                            "breaker_status": breaker_status(store),
                        }
                        _print_json(payload)
                        return 0

                    if command == "reconcile":
                        chance_cache: dict[str, Any] = {}

                        def _fetch_market_chance(market: str) -> Any:
                            market_value = str(market).strip().upper()
                            cached = chance_cache.get(market_value)
                            if cached is not None:
                                return cached
                            payload = client.chance(market=market_value)
                            chance_cache[market_value] = payload
                            return payload

                        report = reconcile_exchange_snapshot(
                            store=store,
                            bot_id=bot_id,
                            identifier_prefix=str(defaults["identifier_prefix"]),
                            accounts_payload=accounts,
                            open_orders_payload=open_orders,
                            fetch_order_detail=lambda uuid, identifier: client.order(uuid=uuid, identifier=identifier),
                            fetch_market_chance=_fetch_market_chance,
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
                            "breaker_status": breaker_status(store),
                        }
                        if apply_mode:
                            store.set_checkpoint(name="last_reconcile", payload=output)
                        _print_json(output)
                        return 2 if bool(report.get("halted")) else 0

                    if command == "run":
                        use_private_ws = bool(getattr(args, "use_private_ws", False)) or bool(defaults["sync_use_private_ws"])
                        use_executor_ws = bool(getattr(args, "use_executor_ws", False)) or bool(defaults["sync_use_executor_ws"])
                        if use_private_ws and use_executor_ws:
                            raise ValueError("live.sync.use_private_ws and live.sync.use_executor_ws cannot both be true")
                        strategy_runtime = bool(getattr(args, "strategy_runtime", False)) or bool(
                            defaults["strategy_runtime_enabled"]
                        )
                        if strategy_runtime and use_executor_ws:
                            raise ValueError("strategy-runtime currently supports poll sync or private ws only")

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
                            use_private_ws=use_private_ws,
                            use_executor_ws=use_executor_ws,
                            duration_sec=(
                                int(getattr(args, "duration_sec", 0))
                                if int(getattr(args, "duration_sec", 0)) > 0
                                else None
                            ),
                            breaker_cancel_reject_limit=int(defaults["breaker_cancel_reject_limit"]),
                            breaker_replace_reject_limit=int(defaults["breaker_replace_reject_limit"]),
                            breaker_rate_limit_error_limit=int(defaults["breaker_rate_limit_error_limit"]),
                            breaker_auth_error_limit=int(defaults["breaker_auth_error_limit"]),
                            breaker_nonce_error_limit=int(defaults["breaker_nonce_error_limit"]),
                            small_account_canary_enabled=bool(defaults["small_account_canary_enabled"]),
                            small_account_max_positions=int(defaults["small_account_max_positions"]),
                            small_account_max_open_orders_per_market=int(
                                defaults["small_account_max_open_orders_per_market"]
                            ),
                            registry_root=str(defaults["model_registry_root"]),
                            runtime_model_ref_source=str(defaults["model_ref_source"]),
                            runtime_model_family=str(defaults["model_family"]),
                            ws_public_raw_root=str(defaults["ws_public_raw_root"]),
                            ws_public_meta_dir=str(defaults["ws_public_meta_dir"]),
                            ws_public_stale_threshold_sec=int(defaults["ws_public_stale_threshold_sec"]),
                            micro_aggregate_report_path=str(defaults["micro_aggregate_report_path"]),
                            rollout_mode=str(getattr(args, "rollout_mode", None) or defaults["rollout_mode"]),
                            rollout_target_unit=str(getattr(args, "rollout_target_unit", None) or defaults["rollout_target_unit"]),
                            rollout_require_test_order=bool(defaults["rollout_require_test_order"]),
                            rollout_test_order_max_age_sec=int(defaults["rollout_test_order_max_age_sec"]),
                        )
                        if strategy_runtime:
                            strategy_defaults = _live_strategy_defaults(config_dir=config_dir, base_config=base_config)
                            runtime_settings = _build_live_model_alpha_runtime_settings(
                                daemon_settings=daemon_settings,
                                live_defaults=defaults,
                                strategy_defaults=strategy_defaults,
                            )
                            with UpbitHttpClient(settings) as public_http:
                                public_client = UpbitPublicClient(public_http)
                                public_ws_client = UpbitWebSocketPublicClient(settings.websocket)
                                private_ws_client = (
                                    UpbitWebSocketPrivateClient(settings.websocket, credentials)
                                    if use_private_ws
                                    else None
                                )
                                if daemon_settings.rollout_mode != "shadow" or bool(runtime_settings.risk_enabled):
                                    execution_backend = str(defaults["executor_backend"]).strip().lower()
                                    if execution_backend == "direct_rest":
                                        from .execution import DirectRestExecutionGateway

                                        executor_gateway_ctx = DirectRestExecutionGateway(client=client)
                                    else:
                                        from .execution import GrpcExecutionGateway

                                        executor_gateway_ctx = GrpcExecutionGateway(
                                            host=str(defaults["executor_host"]),
                                            port=int(defaults["executor_port"]),
                                            timeout_sec=float(defaults["executor_timeout_sec"]),
                                            insecure=bool(defaults["executor_insecure"]),
                                        )
                                    with executor_gateway_ctx as executor_gateway:
                                        daemon_summary = asyncio.run(
                                            run_live_model_alpha_runtime(
                                                store=store,
                                                client=client,
                                                public_client=public_client,
                                                public_ws_client=public_ws_client,
                                                settings=runtime_settings,
                                                executor_gateway=executor_gateway,
                                                private_ws_client=private_ws_client,
                                            )
                                        )
                                else:
                                    daemon_summary = asyncio.run(
                                        run_live_model_alpha_runtime(
                                            store=store,
                                            client=client,
                                            public_client=public_client,
                                            public_ws_client=public_ws_client,
                                            settings=runtime_settings,
                                            executor_gateway=None,
                                            private_ws_client=private_ws_client,
                                        )
                                    )
                        elif daemon_settings.use_private_ws:
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
                        write_rollout_latest(
                            project_root=project_root,
                            event_kind="RUN",
                            contract=store.live_rollout_contract(),
                            test_order=store.live_test_order(),
                            status=store.live_rollout_status() or daemon_summary.get("rollout"),
                            ts_ms=int(time.time() * 1000),
                        )
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


def _optional_float_value(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clamp_prob_value(value: float | None) -> float | None:
    if value is None:
        return None
    return max(min(float(value), 1.0), 0.0)


def _paper_defaults(
    *,
    base_config: dict[str, Any],
    risk_doc: dict[str, Any],
    strategy_doc: dict[str, Any],
) -> dict[str, Any]:
    universe_base = base_config.get("universe", {}) if isinstance(base_config.get("universe"), dict) else {}
    storage_base = base_config.get("storage", {}) if isinstance(base_config.get("storage"), dict) else {}
    data_defaults = _data_defaults(base_config)

    strategy_root = strategy_doc.get("strategy") if isinstance(strategy_doc.get("strategy"), dict) else strategy_doc
    strategy_root = strategy_root if isinstance(strategy_root, dict) else {}
    strategy_universe = strategy_root.get("universe", {}) if isinstance(strategy_root.get("universe"), dict) else {}
    candidates_cfg = (
        strategy_root.get("candidates_v1", {}) if isinstance(strategy_root.get("candidates_v1"), dict) else {}
    )
    micro_gate_cfg = strategy_root.get("micro_gate", {}) if isinstance(strategy_root.get("micro_gate"), dict) else {}
    micro_order_policy_cfg = (
        strategy_root.get("micro_order_policy", {})
        if isinstance(strategy_root.get("micro_order_policy"), dict)
        else {}
    )
    model_alpha_cfg = (
        strategy_root.get("model_alpha_v1", {}) if isinstance(strategy_root.get("model_alpha_v1"), dict) else {}
    )
    model_alpha_selection_cfg = (
        model_alpha_cfg.get("selection", {}) if isinstance(model_alpha_cfg.get("selection"), dict) else {}
    )
    model_alpha_position_cfg = (
        model_alpha_cfg.get("position", {}) if isinstance(model_alpha_cfg.get("position"), dict) else {}
    )
    model_alpha_exit_cfg = model_alpha_cfg.get("exit", {}) if isinstance(model_alpha_cfg.get("exit"), dict) else {}
    model_alpha_execution_cfg = (
        model_alpha_cfg.get("execution", {}) if isinstance(model_alpha_cfg.get("execution"), dict) else {}
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
        "micro_gate": _strategy_micro_gate_defaults(
            micro_gate_cfg=micro_gate_cfg,
            parquet_root=Path(data_defaults["parquet_root"]),
            default_tf="5m",
        ),
        "micro_order_policy": _strategy_micro_order_policy_defaults(micro_order_policy_cfg=micro_order_policy_cfg),
        "paper_micro_provider": "offline_parquet",
        "paper_micro_warmup_sec": 60,
        "paper_micro_warmup_min_trade_events_per_market": 1,
        "paper_micro_auto_health_path": "data/raw_ws/upbit/_meta/ws_public_health.json",
        "paper_micro_auto_health_stale_sec": 180,
        "paper_feature_provider": (
            str(strategy_root.get("paper_feature_provider", "offline_parquet")).strip().lower() or "offline_parquet"
        ),
        "paper_live_parquet_root": str(data_defaults["parquet_root"]),
        "paper_live_candles_dataset": str(strategy_root.get("paper_live_candles_dataset", "candles_api_v1")).strip()
        or "candles_api_v1",
        "paper_live_bootstrap_1m_bars": int(strategy_root.get("paper_live_bootstrap_1m_bars", 2000)),
        "paper_live_micro_max_age_ms": int(strategy_root.get("paper_live_micro_max_age_ms", 300000)),
        "strategy": str(strategy_root.get("paper_strategy_name", "candidates_v1")).strip().lower() or "candidates_v1",
        "tf": str(model_alpha_cfg.get("tf", "5m")).strip().lower() or "5m",
        "model_ref": str(model_alpha_cfg.get("model_ref", DEFAULT_MODEL_ALPHA_RUNTIME_REF)).strip()
        or DEFAULT_MODEL_ALPHA_RUNTIME_REF,
        "model_family": (
            str(model_alpha_cfg.get("model_family")).strip() if model_alpha_cfg.get("model_family") is not None else None
        ),
        "feature_set": str(model_alpha_cfg.get("feature_set", "v3")).strip().lower() or "v3",
        "model_registry_root": str(strategy_root.get("model_registry_root", "models/registry")).strip() or "models/registry",
        "model_feature_dataset_root": (
            str(strategy_root.get("model_feature_dataset_root")).strip()
            if strategy_root.get("model_feature_dataset_root") is not None
            else None
        ),
        "model_alpha": {
            "model_ref": str(model_alpha_cfg.get("model_ref", DEFAULT_MODEL_ALPHA_RUNTIME_REF)).strip()
            or DEFAULT_MODEL_ALPHA_RUNTIME_REF,
            "model_family": (
                str(model_alpha_cfg.get("model_family")).strip()
                if model_alpha_cfg.get("model_family") is not None
                else None
            ),
            "feature_set": str(model_alpha_cfg.get("feature_set", "v3")).strip().lower() or "v3",
            "selection": {
                "top_pct": float(model_alpha_selection_cfg.get("top_pct", 0.05)),
                "min_prob": _clamp_prob_value(_optional_float_value(model_alpha_selection_cfg.get("min_prob"))),
                "min_candidates_per_ts": int(model_alpha_selection_cfg.get("min_candidates_per_ts", 10)),
                "registry_threshold_key": (
                    str(model_alpha_selection_cfg.get("registry_threshold_key", "top_5pct")).strip() or "top_5pct"
                ),
                "use_learned_recommendations": bool(
                    model_alpha_selection_cfg.get("use_learned_recommendations", True)
                ),
            },
            "position": {
                "max_positions_total": int(model_alpha_position_cfg.get("max_positions_total", 3)),
                "cooldown_bars": int(model_alpha_position_cfg.get("cooldown_bars", 6)),
                "entry_min_notional_buffer_bps": max(
                    float(model_alpha_position_cfg.get("entry_min_notional_buffer_bps", 25.0)),
                    0.0,
                ),
                "sizing_mode": str(model_alpha_position_cfg.get("sizing_mode", "prob_ramp")).strip().lower()
                or "prob_ramp",
                "size_multiplier_min": max(float(model_alpha_position_cfg.get("size_multiplier_min", 0.5)), 0.0),
                "size_multiplier_max": max(
                    float(model_alpha_position_cfg.get("size_multiplier_max", 1.5)),
                    max(float(model_alpha_position_cfg.get("size_multiplier_min", 0.5)), 0.0),
                ),
            },
            "exit": {
                "mode": str(model_alpha_exit_cfg.get("mode", "hold")).strip().lower() or "hold",
                "hold_bars": int(model_alpha_exit_cfg.get("hold_bars", 6)),
                "tp_pct": float(model_alpha_exit_cfg.get("tp_pct", 0.02)),
                "sl_pct": float(model_alpha_exit_cfg.get("sl_pct", 0.01)),
                "trailing_pct": float(model_alpha_exit_cfg.get("trailing_pct", 0.0)),
                "expected_exit_slippage_bps": _optional_float_value(
                    model_alpha_exit_cfg.get("expected_exit_slippage_bps")
                ),
                "expected_exit_fee_bps": _optional_float_value(model_alpha_exit_cfg.get("expected_exit_fee_bps")),
            },
            "execution": {
                "price_mode": str(model_alpha_execution_cfg.get("price_mode", "JOIN")).strip().upper() or "JOIN",
                "timeout_bars": int(model_alpha_execution_cfg.get("timeout_bars", 2)),
                "replace_max": int(model_alpha_execution_cfg.get("replace_max", 2)),
            },
        },
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
    micro_gate_cfg = strategy_root.get("micro_gate", {}) if isinstance(strategy_root.get("micro_gate"), dict) else {}
    micro_order_policy_cfg = (
        strategy_root.get("micro_order_policy", {})
        if isinstance(strategy_root.get("micro_order_policy"), dict)
        else {}
    )
    model_alpha_cfg = (
        strategy_root.get("model_alpha_v1", {}) if isinstance(strategy_root.get("model_alpha_v1"), dict) else {}
    )
    model_alpha_selection_cfg = (
        model_alpha_cfg.get("selection", {}) if isinstance(model_alpha_cfg.get("selection"), dict) else {}
    )
    model_alpha_position_cfg = (
        model_alpha_cfg.get("position", {}) if isinstance(model_alpha_cfg.get("position"), dict) else {}
    )
    model_alpha_exit_cfg = model_alpha_cfg.get("exit", {}) if isinstance(model_alpha_cfg.get("exit"), dict) else {}
    model_alpha_execution_cfg = (
        model_alpha_cfg.get("execution", {}) if isinstance(model_alpha_cfg.get("execution"), dict) else {}
    )

    root = backtest_doc.get("backtest", backtest_doc) if isinstance(backtest_doc, dict) else {}
    root = root if isinstance(root, dict) else {}
    universe_cfg = root.get("universe", {}) if isinstance(root.get("universe"), dict) else {}
    data_cfg = root.get("data", {}) if isinstance(root.get("data"), dict) else {}
    execution_cfg = root.get("execution", {}) if isinstance(root.get("execution"), dict) else {}
    output_cfg = root.get("output", {}) if isinstance(root.get("output"), dict) else {}
    strategy_cfg = root.get("strategy", {}) if isinstance(root.get("strategy"), dict) else {}

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
        "strategy": str(root.get("strategy_name", strategy_cfg.get("name", "candidates_v1"))).strip().lower()
        or "candidates_v1",
        "model_ref": str(
            root.get(
                "model_ref",
                strategy_cfg.get("model_ref", model_alpha_cfg.get("model_ref", DEFAULT_MODEL_ALPHA_RUNTIME_REF)),
            )
        ).strip(),
        "model_family": str(
            root.get("model_family", strategy_cfg.get("model_family", model_alpha_cfg.get("model_family", "")))
        ).strip(),
        "feature_set": str(root.get("feature_set", strategy_cfg.get("feature_set", "v3"))).strip().lower() or "v3",
        "model_registry_root": str(
            root.get(
                "model_registry_root",
                strategy_cfg.get("model_registry_root", "models/registry"),
            )
        ).strip(),
        "model_feature_dataset_root": (
            str(root.get("model_feature_dataset_root")).strip()
            if root.get("model_feature_dataset_root") is not None
            else (
                str(strategy_cfg.get("model_feature_dataset_root")).strip()
                if strategy_cfg.get("model_feature_dataset_root") is not None
                else None
            )
        ),
        "model_alpha": {
            "model_ref": str(model_alpha_cfg.get("model_ref", DEFAULT_MODEL_ALPHA_RUNTIME_REF)).strip()
            or DEFAULT_MODEL_ALPHA_RUNTIME_REF,
            "model_family": (
                str(model_alpha_cfg.get("model_family")).strip()
                if model_alpha_cfg.get("model_family") is not None
                else None
            ),
            "feature_set": str(model_alpha_cfg.get("feature_set", "v3")).strip().lower() or "v3",
            "selection": {
                "top_pct": float(model_alpha_selection_cfg.get("top_pct", 0.05)),
                "min_prob": _clamp_prob_value(_optional_float_value(model_alpha_selection_cfg.get("min_prob"))),
                "min_candidates_per_ts": int(model_alpha_selection_cfg.get("min_candidates_per_ts", 10)),
                "registry_threshold_key": (
                    str(model_alpha_selection_cfg.get("registry_threshold_key", "top_5pct")).strip() or "top_5pct"
                ),
                "use_learned_recommendations": bool(
                    model_alpha_selection_cfg.get("use_learned_recommendations", True)
                ),
            },
            "position": {
                "max_positions_total": int(model_alpha_position_cfg.get("max_positions_total", 3)),
                "cooldown_bars": int(model_alpha_position_cfg.get("cooldown_bars", 6)),
                "entry_min_notional_buffer_bps": max(
                    float(model_alpha_position_cfg.get("entry_min_notional_buffer_bps", 25.0)),
                    0.0,
                ),
                "sizing_mode": str(model_alpha_position_cfg.get("sizing_mode", "prob_ramp")).strip().lower()
                or "prob_ramp",
                "size_multiplier_min": max(float(model_alpha_position_cfg.get("size_multiplier_min", 0.5)), 0.0),
                "size_multiplier_max": max(
                    float(model_alpha_position_cfg.get("size_multiplier_max", 1.5)),
                    max(float(model_alpha_position_cfg.get("size_multiplier_min", 0.5)), 0.0),
                ),
            },
            "exit": {
                "mode": str(model_alpha_exit_cfg.get("mode", "hold")).strip().lower() or "hold",
                "hold_bars": int(model_alpha_exit_cfg.get("hold_bars", 6)),
                "tp_pct": float(model_alpha_exit_cfg.get("tp_pct", 0.02)),
                "sl_pct": float(model_alpha_exit_cfg.get("sl_pct", 0.01)),
                "trailing_pct": float(model_alpha_exit_cfg.get("trailing_pct", 0.0)),
                "expected_exit_slippage_bps": _optional_float_value(
                    model_alpha_exit_cfg.get("expected_exit_slippage_bps")
                ),
                "expected_exit_fee_bps": _optional_float_value(model_alpha_exit_cfg.get("expected_exit_fee_bps")),
            },
            "execution": {
                "price_mode": str(model_alpha_execution_cfg.get("price_mode", "JOIN")).strip().upper() or "JOIN",
                "timeout_bars": int(model_alpha_execution_cfg.get("timeout_bars", 2)),
                "replace_max": int(model_alpha_execution_cfg.get("replace_max", 2)),
            },
        },
        "micro_gate": _strategy_micro_gate_defaults(
            micro_gate_cfg=micro_gate_cfg,
            parquet_root=Path(data_defaults["parquet_root"]),
            default_tf=str(root.get("tf", "1m")).strip().lower() or "1m",
        ),
        "micro_order_policy": _strategy_micro_order_policy_defaults(micro_order_policy_cfg=micro_order_policy_cfg),
    }


def _strategy_micro_gate_defaults(
    *,
    micro_gate_cfg: dict[str, Any],
    parquet_root: Path,
    default_tf: str,
) -> dict[str, Any]:
    trade_cfg = micro_gate_cfg.get("trade", {}) if isinstance(micro_gate_cfg.get("trade"), dict) else {}
    book_cfg = micro_gate_cfg.get("book", {}) if isinstance(micro_gate_cfg.get("book"), dict) else {}
    live_ws_cfg = micro_gate_cfg.get("live_ws", {}) if isinstance(micro_gate_cfg.get("live_ws"), dict) else {}
    reconnect_cfg = live_ws_cfg.get("reconnect", {}) if isinstance(live_ws_cfg.get("reconnect"), dict) else {}

    dataset_value = str(
        micro_gate_cfg.get(
            "dataset_name",
            micro_gate_cfg.get("dataset", "micro_v1"),
        )
    ).strip() or "micro_v1"
    dataset_path = Path(dataset_value)
    if not dataset_path.is_absolute():
        dataset_path = parquet_root / dataset_path

    return {
        "enabled": bool(micro_gate_cfg.get("enabled", False)),
        "mode": str(micro_gate_cfg.get("mode", "trade_only")).strip().lower() or "trade_only",
        "on_missing": str(micro_gate_cfg.get("on_missing", "warn_allow")).strip().lower() or "warn_allow",
        "stale_ms": max(int(micro_gate_cfg.get("stale_ms", 120000)), 0),
        "dataset_name": str(dataset_path),
        "tf": str(micro_gate_cfg.get("tf", default_tf)).strip().lower() or str(default_tf).strip().lower(),
        "cache_entries": max(int(micro_gate_cfg.get("cache_entries", 64)), 1),
        "trade": {
            "min_trade_events": max(int(trade_cfg.get("min_trade_events", 1)), 0),
            "min_trade_coverage_ms": max(int(trade_cfg.get("min_trade_coverage_ms", 0)), 0),
            "min_trade_notional_krw": max(float(trade_cfg.get("min_trade_notional_krw", 0.0)), 0.0),
        },
        "book": {
            "max_spread_bps": max(float(book_cfg.get("max_spread_bps", 0.0)), 0.0),
            "min_depth_top5_krw": max(float(book_cfg.get("min_depth_top5_krw", 0.0)), 0.0),
            "min_book_events": max(int(book_cfg.get("min_book_events", 0)), 0),
            "min_book_coverage_ms": max(int(book_cfg.get("min_book_coverage_ms", 0)), 0),
        },
        "live_ws": {
            "enabled": bool(live_ws_cfg.get("enabled", False)),
            "window_sec": max(int(live_ws_cfg.get("window_sec", 60)), 1),
            "orderbook_topk": max(int(live_ws_cfg.get("orderbook_topk", 5)), 1),
            "orderbook_level": live_ws_cfg.get("orderbook_level", 0),
            "subscribe_format": str(live_ws_cfg.get("subscribe_format", "DEFAULT")).strip().upper() or "DEFAULT",
            "max_markets": max(int(live_ws_cfg.get("max_markets", 30)), 1),
            "reconnect_max_per_min": max(int(reconnect_cfg.get("max_per_min", 3)), 1),
            "backoff_base_sec": max(float(reconnect_cfg.get("backoff_base_sec", 1.0)), 0.0),
            "backoff_max_sec": max(float(reconnect_cfg.get("backoff_max_sec", 32.0)), 0.0),
            "connect_rps": 5,
            "message_rps": 5,
            "message_rpm": 100,
            "max_subscribe_messages_per_min": 100,
        },
    }


def _strategy_micro_order_policy_defaults(*, micro_order_policy_cfg: dict[str, Any]) -> dict[str, Any]:
    tiering_cfg = (
        micro_order_policy_cfg.get("tiering", {})
        if isinstance(micro_order_policy_cfg.get("tiering"), dict)
        else {}
    )
    tiers_cfg = micro_order_policy_cfg.get("tiers", {}) if isinstance(micro_order_policy_cfg.get("tiers"), dict) else {}
    safety_cfg = (
        micro_order_policy_cfg.get("safety", {})
        if isinstance(micro_order_policy_cfg.get("safety"), dict)
        else {}
    )

    def _optional_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number

    def _optional_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _tier_values(name: str, *, timeout_ms: int, replace_interval_ms: int, max_replaces: int, price_mode: str, max_chase_bps: int) -> dict[str, Any]:
        tier = tiers_cfg.get(name, {}) if isinstance(tiers_cfg.get(name), dict) else {}
        return {
            "timeout_ms": max(int(tier.get("timeout_ms", timeout_ms)), 1),
            "replace_interval_ms": max(int(tier.get("replace_interval_ms", replace_interval_ms)), 1),
            "max_replaces": max(int(tier.get("max_replaces", max_replaces)), 0),
            "price_mode": str(tier.get("price_mode", price_mode)).strip().upper() or str(price_mode).strip().upper(),
            "max_chase_bps": max(int(tier.get("max_chase_bps", max_chase_bps)), 0),
            "post_only": bool(tier.get("post_only", False)),
        }

    return {
        "enabled": bool(micro_order_policy_cfg.get("enabled", False)),
        "mode": str(micro_order_policy_cfg.get("mode", "trade_only")).strip().lower() or "trade_only",
        "on_missing": str(micro_order_policy_cfg.get("on_missing", "static_fallback")).strip().lower()
        or "static_fallback",
        "tiering": {
            "w_notional": float(tiering_cfg.get("w_notional", 1.0)),
            "w_events": float(tiering_cfg.get("w_events", 0.5)),
            "t1": float(tiering_cfg.get("t1", 6.0)),
            "t2": float(tiering_cfg.get("t2", 9.0)),
        },
        "tiers": {
            "LOW": _tier_values(
                "LOW",
                timeout_ms=120_000,
                replace_interval_ms=60_000,
                max_replaces=1,
                price_mode="PASSIVE_MAKER",
                max_chase_bps=10,
            ),
            "MID": _tier_values(
                "MID",
                timeout_ms=45_000,
                replace_interval_ms=15_000,
                max_replaces=3,
                price_mode="JOIN",
                max_chase_bps=15,
            ),
            "HIGH": _tier_values(
                "HIGH",
                timeout_ms=15_000,
                replace_interval_ms=5_000,
                max_replaces=5,
                price_mode="CROSS_1T",
                max_chase_bps=20,
            ),
        },
        "safety": {
            "min_replace_interval_ms_global": max(int(safety_cfg.get("min_replace_interval_ms_global", 1500)), 1),
            "max_replaces_per_min_per_market": max(int(safety_cfg.get("max_replaces_per_min_per_market", 10)), 1),
            "forbid_post_only_with_cross": bool(safety_cfg.get("forbid_post_only_with_cross", True)),
        },
        "cross_tick_bps_max": max(float(micro_order_policy_cfg.get("cross_tick_bps_max", 10.0)), 0.0),
        "cross_escalate_after_timeouts": max(int(micro_order_policy_cfg.get("cross_escalate_after_timeouts", 2)), 0),
        "cross_min_prob": _optional_float(micro_order_policy_cfg.get("cross_min_prob")),
        "cross_micro_stale_ms": _optional_int(micro_order_policy_cfg.get("cross_micro_stale_ms")),
        "abort_if_tick_bps_gt": _optional_float(micro_order_policy_cfg.get("abort_if_tick_bps_gt")),
        "tick_size_resolver": str(micro_order_policy_cfg.get("tick_size_resolver", "auto")).strip().lower() or "auto",
    }


def _build_micro_gate_settings(
    *,
    defaults: dict[str, Any],
    cli_enabled: str | None,
    cli_mode: str | None,
    cli_on_missing: str | None,
) -> MicroGateSettings:
    trade_cfg = defaults.get("trade", {}) if isinstance(defaults.get("trade"), dict) else {}
    book_cfg = defaults.get("book", {}) if isinstance(defaults.get("book"), dict) else {}
    live_ws_cfg = defaults.get("live_ws", {}) if isinstance(defaults.get("live_ws"), dict) else {}

    enabled = bool(defaults.get("enabled", False))
    if cli_enabled is not None:
        enabled = str(cli_enabled).strip().lower() == "on"

    mode = str(cli_mode or defaults.get("mode", "trade_only")).strip().lower()
    on_missing = str(cli_on_missing or defaults.get("on_missing", "warn_allow")).strip().lower()
    tf_value = str(defaults.get("tf", "")).strip().lower() or None

    return MicroGateSettings(
        enabled=enabled,
        mode=mode,
        on_missing=on_missing,
        stale_ms=max(int(defaults.get("stale_ms", 120000)), 0),
        dataset_name=str(defaults.get("dataset_name", "micro_v1")),
        tf=tf_value,
        cache_entries=max(int(defaults.get("cache_entries", 64)), 1),
        trade=MicroGateTradeSettings(
            min_trade_events=max(int(trade_cfg.get("min_trade_events", 1)), 0),
            min_trade_coverage_ms=max(int(trade_cfg.get("min_trade_coverage_ms", 0)), 0),
            min_trade_notional_krw=max(float(trade_cfg.get("min_trade_notional_krw", 0.0)), 0.0),
        ),
        book=MicroGateBookSettings(
            max_spread_bps=max(float(book_cfg.get("max_spread_bps", 0.0)), 0.0),
            min_depth_top5_krw=max(float(book_cfg.get("min_depth_top5_krw", 0.0)), 0.0),
            min_book_events=max(int(book_cfg.get("min_book_events", 0)), 0),
            min_book_coverage_ms=max(int(book_cfg.get("min_book_coverage_ms", 0)), 0),
        ),
        live_ws=LiveWsProviderSettings(
            enabled=bool(live_ws_cfg.get("enabled", False)),
            window_sec=max(int(live_ws_cfg.get("window_sec", 60)), 1),
            orderbook_topk=max(int(live_ws_cfg.get("orderbook_topk", 5)), 1),
            orderbook_level=live_ws_cfg.get("orderbook_level", 0),
            subscribe_format=str(live_ws_cfg.get("subscribe_format", "DEFAULT")).strip().upper() or "DEFAULT",
            max_markets=max(int(live_ws_cfg.get("max_markets", 30)), 1),
            reconnect_max_per_min=max(int(live_ws_cfg.get("reconnect_max_per_min", 3)), 1),
            backoff_base_sec=max(float(live_ws_cfg.get("backoff_base_sec", 1.0)), 0.0),
            backoff_max_sec=max(float(live_ws_cfg.get("backoff_max_sec", 32.0)), 0.0),
            connect_rps=max(int(live_ws_cfg.get("connect_rps", 5)), 1),
            message_rps=max(int(live_ws_cfg.get("message_rps", 5)), 1),
            message_rpm=max(int(live_ws_cfg.get("message_rpm", 100)), 1),
            max_subscribe_messages_per_min=max(int(live_ws_cfg.get("max_subscribe_messages_per_min", 100)), 1),
        ),
    )


def _build_micro_order_policy_settings(
    *,
    defaults: dict[str, Any],
    cli_enabled: str | None,
    cli_mode: str | None,
    cli_on_missing: str | None,
) -> MicroOrderPolicySettings:
    enabled = bool(defaults.get("enabled", False))
    if cli_enabled is not None:
        enabled = str(cli_enabled).strip().lower() == "on"

    mode = str(cli_mode or defaults.get("mode", "trade_only")).strip().lower()
    on_missing = str(cli_on_missing or defaults.get("on_missing", "static_fallback")).strip().lower()

    tiering_cfg = defaults.get("tiering", {}) if isinstance(defaults.get("tiering"), dict) else {}
    tiers_cfg = defaults.get("tiers", {}) if isinstance(defaults.get("tiers"), dict) else {}
    safety_cfg = defaults.get("safety", {}) if isinstance(defaults.get("safety"), dict) else {}
    low_cfg = tiers_cfg.get("LOW", {}) if isinstance(tiers_cfg.get("LOW"), dict) else {}
    mid_cfg = tiers_cfg.get("MID", {}) if isinstance(tiers_cfg.get("MID"), dict) else {}
    high_cfg = tiers_cfg.get("HIGH", {}) if isinstance(tiers_cfg.get("HIGH"), dict) else {}

    def _optional_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _optional_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    return MicroOrderPolicySettings(
        enabled=enabled,
        mode=mode,
        on_missing=on_missing,
        tiering=MicroOrderPolicyTieringSettings(
            w_notional=float(tiering_cfg.get("w_notional", 1.0)),
            w_events=float(tiering_cfg.get("w_events", 0.5)),
            t1=float(tiering_cfg.get("t1", 6.0)),
            t2=float(tiering_cfg.get("t2", 9.0)),
        ),
        tiers=MicroOrderPolicyTiersSettings(
            low=MicroOrderPolicyTierSettings(
                timeout_ms=max(int(low_cfg.get("timeout_ms", 120_000)), 1),
                replace_interval_ms=max(int(low_cfg.get("replace_interval_ms", 60_000)), 1),
                max_replaces=max(int(low_cfg.get("max_replaces", 1)), 0),
                price_mode=str(low_cfg.get("price_mode", "PASSIVE_MAKER")),
                max_chase_bps=max(int(low_cfg.get("max_chase_bps", 10)), 0),
                post_only=bool(low_cfg.get("post_only", False)),
            ),
            mid=MicroOrderPolicyTierSettings(
                timeout_ms=max(int(mid_cfg.get("timeout_ms", 45_000)), 1),
                replace_interval_ms=max(int(mid_cfg.get("replace_interval_ms", 15_000)), 1),
                max_replaces=max(int(mid_cfg.get("max_replaces", 3)), 0),
                price_mode=str(mid_cfg.get("price_mode", "JOIN")),
                max_chase_bps=max(int(mid_cfg.get("max_chase_bps", 15)), 0),
                post_only=bool(mid_cfg.get("post_only", False)),
            ),
            high=MicroOrderPolicyTierSettings(
                timeout_ms=max(int(high_cfg.get("timeout_ms", 15_000)), 1),
                replace_interval_ms=max(int(high_cfg.get("replace_interval_ms", 5_000)), 1),
                max_replaces=max(int(high_cfg.get("max_replaces", 5)), 0),
                price_mode=str(high_cfg.get("price_mode", "CROSS_1T")),
                max_chase_bps=max(int(high_cfg.get("max_chase_bps", 20)), 0),
                post_only=bool(high_cfg.get("post_only", False)),
            ),
        ),
        safety=MicroOrderPolicySafetySettings(
            min_replace_interval_ms_global=max(int(safety_cfg.get("min_replace_interval_ms_global", 1500)), 1),
            max_replaces_per_min_per_market=max(int(safety_cfg.get("max_replaces_per_min_per_market", 10)), 1),
            forbid_post_only_with_cross=bool(safety_cfg.get("forbid_post_only_with_cross", True)),
        ),
        cross_tick_bps_max=max(float(defaults.get("cross_tick_bps_max", 10.0)), 0.0),
        cross_escalate_after_timeouts=max(int(defaults.get("cross_escalate_after_timeouts", 2)), 0),
        cross_min_prob=_optional_float(defaults.get("cross_min_prob")),
        cross_micro_stale_ms=_optional_int(defaults.get("cross_micro_stale_ms")),
        abort_if_tick_bps_gt=_optional_float(defaults.get("abort_if_tick_bps_gt")),
        tick_size_resolver=str(defaults.get("tick_size_resolver", "auto")).strip().lower() or "auto",
    )


def _live_defaults(base_config: dict[str, Any]) -> dict[str, Any]:
    live_cfg = base_config.get("live", {}) if isinstance(base_config.get("live"), dict) else {}
    state_cfg = live_cfg.get("state", {}) if isinstance(live_cfg.get("state"), dict) else {}
    startup_cfg = live_cfg.get("startup", {}) if isinstance(live_cfg.get("startup"), dict) else {}
    sync_cfg = live_cfg.get("sync", {}) if isinstance(live_cfg.get("sync"), dict) else {}
    breaker_cfg = live_cfg.get("breakers", {}) if isinstance(live_cfg.get("breakers"), dict) else {}
    executor_cfg = live_cfg.get("executor", {}) if isinstance(live_cfg.get("executor"), dict) else {}
    orders_cfg = live_cfg.get("orders", {}) if isinstance(live_cfg.get("orders"), dict) else {}
    default_risk_cfg = live_cfg.get("default_risk", {}) if isinstance(live_cfg.get("default_risk"), dict) else {}
    live_risk_cfg = live_cfg.get("risk", {}) if isinstance(live_cfg.get("risk"), dict) else {}
    live_strategy_cfg = live_cfg.get("strategy", {}) if isinstance(live_cfg.get("strategy"), dict) else {}
    small_account_cfg = (
        live_cfg.get("small_account", {}) if isinstance(live_cfg.get("small_account"), dict) else {}
    )
    model_cfg = live_cfg.get("model", {}) if isinstance(live_cfg.get("model"), dict) else {}
    rollout_cfg = live_cfg.get("rollout", {}) if isinstance(live_cfg.get("rollout"), dict) else {}
    data_plane_cfg = live_cfg.get("data_plane", {}) if isinstance(live_cfg.get("data_plane"), dict) else {}
    ws_public_cfg = (
        data_plane_cfg.get("ws_public", {}) if isinstance(data_plane_cfg.get("ws_public"), dict) else {}
    )
    micro_plane_cfg = (
        data_plane_cfg.get("micro", {}) if isinstance(data_plane_cfg.get("micro"), dict) else {}
    )
    universe_cfg = base_config.get("universe", {}) if isinstance(base_config.get("universe"), dict) else {}

    unknown_open_orders_policy = str(startup_cfg.get("unknown_open_orders_policy", "halt")).strip().lower()
    if unknown_open_orders_policy not in {"halt", "ignore", "cancel"}:
        unknown_open_orders_policy = "halt"

    unknown_positions_policy = str(startup_cfg.get("unknown_positions_policy", "halt")).strip().lower()
    if unknown_positions_policy not in {"halt", "import_as_unmanaged", "attach_default_risk", "attach_strategy_risk"}:
        unknown_positions_policy = "halt"

    env_bot_id = str(os.getenv("AUTOBOT_LIVE_BOT_ID", "")).strip().lower()
    env_state_db_path = str(os.getenv("AUTOBOT_LIVE_STATE_DB_PATH", "")).strip()
    env_model_ref_source = str(os.getenv("AUTOBOT_LIVE_MODEL_REF_SOURCE", "")).strip()
    env_model_family = str(os.getenv("AUTOBOT_LIVE_MODEL_FAMILY", "")).strip()
    env_model_registry_root = str(os.getenv("AUTOBOT_LIVE_MODEL_REGISTRY_ROOT", "")).strip()
    env_rollout_mode = str(os.getenv("AUTOBOT_LIVE_ROLLOUT_MODE", "")).strip().lower()
    env_rollout_target_unit = str(os.getenv("AUTOBOT_LIVE_TARGET_UNIT", "")).strip()
    env_sync_mode = str(os.getenv("AUTOBOT_LIVE_SYNC_MODE", "")).strip().lower()
    sync_use_private_ws = bool(sync_cfg.get("use_private_ws", False))
    sync_use_executor_ws = bool(sync_cfg.get("use_executor_ws", False))
    if env_sync_mode == "poll":
        sync_use_private_ws = False
        sync_use_executor_ws = False
    elif env_sync_mode == "private_ws":
        sync_use_private_ws = True
        sync_use_executor_ws = False
    elif env_sync_mode == "executor_ws":
        sync_use_private_ws = False
        sync_use_executor_ws = True

    return {
        "enabled": bool(live_cfg.get("enabled", False)),
        "bot_id": env_bot_id or str(live_cfg.get("bot_id", "autobot-001")).strip().lower(),
        "state_db_path": env_state_db_path or str(state_cfg.get("db_path", "data/state/live_state.db")),
        "state_run_lock": bool(state_cfg.get("run_lock", True)),
        "startup_reconcile": bool(startup_cfg.get("reconcile", True)),
        "unknown_open_orders_policy": unknown_open_orders_policy,
        "unknown_positions_policy": unknown_positions_policy,
        "allow_cancel_external_orders": bool(startup_cfg.get("allow_cancel_external_orders", False)),
        "sync_poll_interval_sec": max(int(sync_cfg.get("poll_interval_sec", 15)), 1),
        "sync_use_private_ws": sync_use_private_ws,
        "sync_use_executor_ws": sync_use_executor_ws,
        "breaker_cancel_reject_limit": max(int(breaker_cfg.get("cancel_reject_limit", 3)), 1),
        "breaker_replace_reject_limit": max(int(breaker_cfg.get("replace_reject_limit", 3)), 1),
        "breaker_rate_limit_error_limit": max(int(breaker_cfg.get("rate_limit_error_limit", 3)), 1),
        "breaker_auth_error_limit": max(int(breaker_cfg.get("auth_error_limit", 2)), 1),
        "breaker_nonce_error_limit": max(int(breaker_cfg.get("nonce_error_limit", 2)), 1),
        "small_account_canary_enabled": bool(small_account_cfg.get("single_slot_canary", False)),
        "small_account_max_positions": max(int(small_account_cfg.get("max_positions", 1)), 1),
        "small_account_max_open_orders_per_market": max(
            int(small_account_cfg.get("max_open_orders_per_market", 1)),
            1,
        ),
        "model_ref_source": env_model_ref_source or str(model_cfg.get("ref", "champion_v4")).strip() or "champion_v4",
        "model_family": env_model_family or str(model_cfg.get("family", "train_v4_crypto_cs")).strip() or "train_v4_crypto_cs",
        "model_registry_root": env_model_registry_root or str(model_cfg.get("registry_root", "models/registry")).strip() or "models/registry",
        "ws_public_raw_root": str(ws_public_cfg.get("raw_root", "data/raw_ws/upbit/public")).strip()
        or "data/raw_ws/upbit/public",
        "ws_public_meta_dir": str(ws_public_cfg.get("meta_dir", "data/raw_ws/upbit/_meta")).strip()
        or "data/raw_ws/upbit/_meta",
        "ws_public_stale_threshold_sec": max(int(ws_public_cfg.get("stale_threshold_sec", 180)), 1),
        "micro_aggregate_report_path": str(
            micro_plane_cfg.get("aggregate_report_path", "data/parquet/micro_v1/_meta/aggregate_report.json")
        ).strip()
        or "data/parquet/micro_v1/_meta/aggregate_report.json",
        "rollout_mode": env_rollout_mode or str(rollout_cfg.get("mode", DEFAULT_ROLLOUT_MODE)).strip().lower() or DEFAULT_ROLLOUT_MODE,
        "rollout_target_unit": env_rollout_target_unit
        or str(rollout_cfg.get("target_unit", DEFAULT_LIVE_TARGET_UNIT)).strip()
        or DEFAULT_LIVE_TARGET_UNIT,
        "rollout_require_test_order": bool(rollout_cfg.get("require_test_order", True)),
        "rollout_test_order_max_age_sec": max(int(rollout_cfg.get("test_order_max_age_sec", 86400)), 1),
        "executor_host": str(executor_cfg.get("host", "127.0.0.1")).strip(),
        "executor_port": max(int(executor_cfg.get("port", 50051)), 1),
        "executor_timeout_sec": max(float(executor_cfg.get("timeout_sec", 5.0)), 0.1),
        "executor_insecure": bool(executor_cfg.get("insecure", True)),
        "executor_backend": str(executor_cfg.get("backend", "grpc")).strip().lower() or "grpc",
        "identifier_prefix": str(orders_cfg.get("identifier_prefix", "AUTOBOT")).strip().upper(),
        "default_risk_sl_pct": max(float(default_risk_cfg.get("sl_pct", 2.0)), 0.0),
        "default_risk_tp_pct": max(float(default_risk_cfg.get("tp_pct", 3.0)), 0.0),
        "default_risk_trailing_enabled": bool(default_risk_cfg.get("trailing_enabled", False)),
        "risk_enabled": bool(live_risk_cfg.get("enabled", False)),
        "risk_exit_aggress_bps": max(float(live_risk_cfg.get("exit_aggress_bps", 8.0)), 0.0),
        "risk_timeout_sec": max(int(live_risk_cfg.get("timeout_sec", 20)), 1),
        "risk_replace_max": (
            min(max(int(live_risk_cfg.get("replace_max", 2)), 0), 1)
            if bool(small_account_cfg.get("single_slot_canary", False))
            else max(int(live_risk_cfg.get("replace_max", 2)), 0)
        ),
        "risk_default_trail_pct": max(float(live_risk_cfg.get("default_trail_pct", 1.0)), 0.0),
        "strategy_runtime_enabled": bool(live_strategy_cfg.get("enabled", False)),
        "strategy_decision_interval_sec": max(float(live_strategy_cfg.get("decision_interval_sec", 1.0)), 0.1),
        "quote_currency": str(universe_cfg.get("quote_currency", "KRW")).strip().upper(),
    }


def _live_strategy_defaults(*, config_dir: Path, base_config: dict[str, Any]) -> dict[str, Any]:
    risk_doc = _load_yaml_doc(config_dir / "risk.yaml")
    strategy_doc = _load_yaml_doc(config_dir / "strategy.yaml")
    defaults = _paper_defaults(base_config=base_config, risk_doc=risk_doc, strategy_doc=strategy_doc)
    return dict(defaults)


def _build_model_alpha_settings_from_defaults(defaults: dict[str, Any]) -> ModelAlphaSettings:
    model_alpha_defaults = (
        defaults.get("model_alpha", {}) if isinstance(defaults.get("model_alpha"), dict) else {}
    )
    selection_defaults = (
        model_alpha_defaults.get("selection", {})
        if isinstance(model_alpha_defaults.get("selection"), dict)
        else {}
    )
    position_defaults = (
        model_alpha_defaults.get("position", {})
        if isinstance(model_alpha_defaults.get("position"), dict)
        else {}
    )
    exit_defaults = (
        model_alpha_defaults.get("exit", {}) if isinstance(model_alpha_defaults.get("exit"), dict) else {}
    )
    execution_defaults = (
        model_alpha_defaults.get("execution", {})
        if isinstance(model_alpha_defaults.get("execution"), dict)
        else {}
    )
    operational_defaults = (
        model_alpha_defaults.get("operational", {})
        if isinstance(model_alpha_defaults.get("operational"), dict)
        else {}
    )
    return ModelAlphaSettings(
        model_ref=str(model_alpha_defaults.get("model_ref", DEFAULT_MODEL_ALPHA_RUNTIME_REF)).strip()
        or DEFAULT_MODEL_ALPHA_RUNTIME_REF,
        model_family=(
            str(model_alpha_defaults.get("model_family")).strip()
            if model_alpha_defaults.get("model_family") is not None
            else None
        ),
        feature_set=str(model_alpha_defaults.get("feature_set", "v4")).strip().lower() or "v4",
        selection=ModelAlphaSelectionSettings(
            top_pct=max(min(float(selection_defaults.get("top_pct", 0.5)), 1.0), 0.0),
            min_prob=_clamp_prob_value(_optional_float_value(selection_defaults.get("min_prob"))),
            min_candidates_per_ts=max(int(selection_defaults.get("min_candidates_per_ts", 1)), 0),
            registry_threshold_key=(
                str(selection_defaults.get("registry_threshold_key", "top_5pct")).strip() or "top_5pct"
            ),
            use_learned_recommendations=bool(selection_defaults.get("use_learned_recommendations", True)),
        ),
        position=ModelAlphaPositionSettings(
            max_positions_total=max(int(position_defaults.get("max_positions_total", 3)), 1),
            cooldown_bars=max(int(position_defaults.get("cooldown_bars", 6)), 0),
            entry_min_notional_buffer_bps=max(
                float(position_defaults.get("entry_min_notional_buffer_bps", 25.0)),
                0.0,
            ),
            sizing_mode=str(position_defaults.get("sizing_mode", "prob_ramp")).strip().lower() or "prob_ramp",
            size_multiplier_min=max(float(position_defaults.get("size_multiplier_min", 0.5)), 0.0),
            size_multiplier_max=max(
                float(position_defaults.get("size_multiplier_max", 1.5)),
                max(float(position_defaults.get("size_multiplier_min", 0.5)), 0.0),
            ),
        ),
        exit=ModelAlphaExitSettings(
            mode=str(exit_defaults.get("mode", "hold")).strip().lower() or "hold",
            hold_bars=max(int(exit_defaults.get("hold_bars", 6)), 0),
            use_learned_exit_mode=bool(exit_defaults.get("use_learned_exit_mode", True)),
            use_learned_hold_bars=bool(exit_defaults.get("use_learned_hold_bars", True)),
            use_learned_risk_recommendations=bool(exit_defaults.get("use_learned_risk_recommendations", True)),
            use_trade_level_action_policy=bool(exit_defaults.get("use_trade_level_action_policy", True)),
            risk_scaling_mode=str(exit_defaults.get("risk_scaling_mode", "fixed")).strip().lower() or "fixed",
            risk_vol_feature=str(exit_defaults.get("risk_vol_feature", "rv_12")).strip() or "rv_12",
            tp_vol_multiplier=_optional_float_value(exit_defaults.get("tp_vol_multiplier")),
            sl_vol_multiplier=_optional_float_value(exit_defaults.get("sl_vol_multiplier")),
            trailing_vol_multiplier=_optional_float_value(exit_defaults.get("trailing_vol_multiplier")),
            tp_pct=max(float(exit_defaults.get("tp_pct", 0.02)), 0.0),
            sl_pct=max(float(exit_defaults.get("sl_pct", 0.01)), 0.0),
            trailing_pct=max(float(exit_defaults.get("trailing_pct", 0.0)), 0.0),
            expected_exit_slippage_bps=_optional_float_value(exit_defaults.get("expected_exit_slippage_bps")),
            expected_exit_fee_bps=_optional_float_value(exit_defaults.get("expected_exit_fee_bps")),
        ),
        execution=ModelAlphaExecutionSettings(
            price_mode=str(execution_defaults.get("price_mode", "JOIN")).strip().upper() or "JOIN",
            timeout_bars=max(int(execution_defaults.get("timeout_bars", 2)), 1),
            replace_max=max(int(execution_defaults.get("replace_max", 2)), 0),
            use_learned_recommendations=bool(execution_defaults.get("use_learned_recommendations", True)),
        ),
        operational=ModelAlphaOperationalSettings(
            enabled=bool(operational_defaults.get("enabled", True)),
            risk_multiplier_min=max(float(operational_defaults.get("risk_multiplier_min", 0.80)), 0.0),
            risk_multiplier_max=max(
                float(operational_defaults.get("risk_multiplier_max", 1.20)),
                max(float(operational_defaults.get("risk_multiplier_min", 0.80)), 0.0),
            ),
            max_positions_scale_min=max(float(operational_defaults.get("max_positions_scale_min", 0.50)), 0.10),
            max_positions_scale_max=max(
                float(operational_defaults.get("max_positions_scale_max", 1.50)),
                max(float(operational_defaults.get("max_positions_scale_min", 0.50)), 0.10),
            ),
            session_overlap_boost=max(float(operational_defaults.get("session_overlap_boost", 0.10)), 0.0),
            session_offpeak_penalty=max(float(operational_defaults.get("session_offpeak_penalty", 0.05)), 0.0),
            micro_quality_block_threshold=max(
                float(operational_defaults.get("micro_quality_block_threshold", 0.15)),
                0.0,
            ),
            micro_quality_conservative_threshold=max(
                float(operational_defaults.get("micro_quality_conservative_threshold", 0.35)),
                0.0,
            ),
            micro_quality_aggressive_threshold=max(
                float(operational_defaults.get("micro_quality_aggressive_threshold", 0.75)),
                0.0,
            ),
            max_execution_spread_bps_for_join=max(
                float(operational_defaults.get("max_execution_spread_bps_for_join", 20.0)),
                0.0,
            ),
            use_calibration_artifact=bool(operational_defaults.get("use_calibration_artifact", True)),
            calibration_artifact_path=str(
                operational_defaults.get(
                    "calibration_artifact_path",
                    "logs/operational_overlay/auto/latest_calibration.json",
                )
            ).strip()
            or "logs/operational_overlay/auto/latest_calibration.json",
        ),
    )


def _build_live_model_alpha_runtime_settings(
    *,
    daemon_settings: LiveDaemonSettings,
    live_defaults: dict[str, Any],
    strategy_defaults: dict[str, Any],
) -> LiveModelAlphaRuntimeSettings:
    return LiveModelAlphaRuntimeSettings(
        daemon=daemon_settings,
        quote=str(strategy_defaults.get("quote", live_defaults.get("quote_currency", "KRW"))),
        top_n=int(strategy_defaults.get("top_n", 20)),
        tf=str(strategy_defaults.get("tf", "5m")),
        decision_interval_sec=float(live_defaults.get("strategy_decision_interval_sec", 1.0)),
        universe_refresh_sec=float(strategy_defaults.get("universe_refresh_sec", 60.0)),
        universe_hold_sec=float(strategy_defaults.get("universe_hold_sec", 120.0)),
        per_trade_krw=float(strategy_defaults.get("per_trade_krw", 10_000.0)),
        max_positions=max(int(strategy_defaults.get("max_positions", 2)), 1),
        min_order_krw=float(strategy_defaults.get("min_order_krw", 5_000.0)),
        max_consecutive_failures=max(int(strategy_defaults.get("max_consecutive_failures", 3)), 1),
        cooldown_sec_after_fail=max(int(strategy_defaults.get("cooldown_sec_after_fail", 60)), 0),
        model_alpha=_build_model_alpha_settings_from_defaults(strategy_defaults),
        micro_gate=_build_micro_gate_settings(
            defaults=strategy_defaults["micro_gate"],
            cli_enabled=None,
            cli_mode=None,
            cli_on_missing=None,
        ),
        micro_order_policy=_build_micro_order_policy_settings(
            defaults=strategy_defaults["micro_order_policy"],
            cli_enabled=None,
            cli_mode=None,
            cli_on_missing=None,
        ),
        paper_live_parquet_root=str(strategy_defaults.get("paper_live_parquet_root", "data/parquet")),
        paper_live_candles_dataset=str(strategy_defaults.get("paper_live_candles_dataset", "candles_api_v1")),
        paper_live_bootstrap_1m_bars=int(strategy_defaults.get("paper_live_bootstrap_1m_bars", 2000)),
        paper_live_micro_max_age_ms=int(strategy_defaults.get("paper_live_micro_max_age_ms", 300000)),
        risk_enabled=bool(live_defaults.get("risk_enabled", False)),
        risk_exit_aggress_bps=float(live_defaults.get("risk_exit_aggress_bps", 8.0)),
        risk_timeout_sec=int(live_defaults.get("risk_timeout_sec", 20)),
        risk_replace_max=int(live_defaults.get("risk_replace_max", 2)),
        risk_default_trail_pct=float(live_defaults.get("risk_default_trail_pct", 1.0)),
    )


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
        "raw_ws_root": str(root.get("raw_ws_root", "data/raw_ws/upbit/public")),
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


def _resolve_backtest_dataset_name_for_model_features(
    *,
    parquet_root: Path,
    base_candles_dataset: str,
    fallback: str,
) -> str:
    value = str(base_candles_dataset).strip() or "auto"
    if value.lower() == "auto":
        for name in ("candles_api_v1", "candles_v1"):
            if (parquet_root / name).exists():
                return name
        return str(fallback).strip() or "candles_v1"
    candidate = Path(value)
    if candidate.is_absolute():
        return str(fallback).strip() or "candles_v1"
    if (parquet_root / candidate).exists():
        return str(candidate).strip()
    return str(fallback).strip() or "candles_v1"


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


def _parse_ablation_ids(value: str | None) -> tuple[str, ...]:
    raw = str(value).strip() if value is not None else ""
    if not raw:
        return ("A0", "A1", "A2", "A3", "A4")
    seen: set[str] = set()
    ids: list[str] = []
    for token in raw.split(","):
        aid = str(token).strip().upper()
        if not aid:
            continue
        if aid not in {"A0", "A1", "A2", "A3", "A4"}:
            raise ValueError(f"unsupported ablation id: {aid}")
        if aid in seen:
            continue
        seen.add(aid)
        ids.append(aid)
    if not ids:
        raise ValueError("ablations must include at least one of A0,A1,A2,A3,A4")
    return tuple(ids)


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


def _resolve_model_ref_alias(model_ref: str, model_family: str | None = None) -> tuple[str, str | None]:
    return _cli_model_helpers.resolve_model_ref_alias(model_ref, model_family)


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
