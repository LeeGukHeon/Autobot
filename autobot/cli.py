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
from .data import DuckDBSettings, IngestOptions, ingest_dataset, sniff_csv_files, validate_dataset
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


if __name__ == "__main__":
    raise SystemExit(main())
