"""Command line interface for Upbit AutoBot."""

from __future__ import annotations

import asyncio
import argparse
import json
from pathlib import Path
import time
from typing import Any, Callable

import yaml

from .data import DuckDBSettings, IngestOptions, ingest_dataset, sniff_csv_files, validate_dataset
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

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    config = _load_base_config(Path(args.config_dir))

    if args.command == "data":
        return _handle_data_command(args, config)
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
    config_path = config_dir / "base.yaml"
    if not config_path.exists():
        return {}

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return {}
    return raw


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
        },
    }


def _parse_csv_list(value: str | None, normalize: Callable[[str], str]) -> tuple[str, ...] | None:
    if value is None:
        return None
    items = tuple(normalize(item.strip()) for item in value.split(",") if item.strip())
    return items or None


if __name__ == "__main__":
    raise SystemExit(main())
