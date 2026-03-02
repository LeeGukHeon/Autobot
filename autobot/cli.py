"""Command line interface for Upbit AutoBot."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Callable

import yaml

from .data import DuckDBSettings, IngestOptions, ingest_dataset, sniff_csv_files, validate_dataset


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

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    config = _load_base_config(Path(args.config_dir))

    if args.command == "data":
        return _handle_data_command(args, config)

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
    summary = validate_dataset(parquet_dir=parquet_dir, tf_filter=tf_filter, market_filter=market_filter)

    print(
        f"[validate] checked={summary.checked_files} ok={summary.ok_files} "
        f"warn={summary.warn_files} fail={summary.fail_files}"
    )
    print(f"[validate] report={summary.report_file}")
    return 2 if summary.fail_files > 0 else 0


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
    }


def _parse_csv_list(value: str | None, normalize: Callable[[str], str]) -> tuple[str, ...] | None:
    if value is None:
        return None
    items = tuple(normalize(item.strip()) for item in value.split(",") if item.strip())
    return items or None


if __name__ == "__main__":
    raise SystemExit(main())
