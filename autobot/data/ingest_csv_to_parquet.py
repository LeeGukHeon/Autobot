"""CSV to Parquet ingestion pipeline for Candle Data Contract v1."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from dataclasses import dataclass, field
import json
from pathlib import Path
import time
from typing import Any

import polars as pl

from .column_mapper import ColumnMappingError, detect_column_mapping
from .duckdb_utils import DEFAULT_DUCKDB_TEMP_DIRECTORY, DuckDBSettings, create_duckdb_connection
from .filename_parser import FilenameParseError, parse_upbit_filename
from .manifest import append_manifest_rows, build_manifest_index, load_manifest, manifest_path, should_skip_file
from .schema_contract import CandleSchemaV1, EXPECTED_INTERVAL_MS, expected_interval_ms


SUPPORTED_TFS = tuple(EXPECTED_INTERVAL_MS.keys())
VALID_QA_SEVERITIES = {"info", "warn", "fail"}
VALID_OHLC_VIOLATION_POLICIES = {"drop_row_and_warn", "fail"}


@dataclass(frozen=True)
class IngestOptions:
    raw_dir: Path = Path("data/raw")
    out_dir: Path = Path("data/parquet")
    dataset_name: str = "candles_v1"
    pattern: str = "upbit_*_full.csv"
    workers: int = 1
    mode: str = "skip_unchanged"
    compression: str = "zstd"
    quote_filter: tuple[str, ...] | None = None
    tf_filter: tuple[str, ...] | None = None
    symbol_filter: tuple[str, ...] | None = None
    limit_files: int | None = None
    dry_run: bool = False
    supported_tfs: tuple[str, ...] = SUPPORTED_TFS
    allow_sort_on_non_monotonic: bool = True
    allow_dedupe_on_duplicate_ts: bool = True
    quote_volume_policy: str = "estimate_if_missing"
    gap_severity: str = "info"
    quote_est_severity: str = "info"
    ohlc_violation_policy: str = "drop_row_and_warn"
    engine: str = "duckdb"
    duckdb: DuckDBSettings = field(default_factory=DuckDBSettings)

    @property
    def dataset_root(self) -> Path:
        return self.out_dir / self.dataset_name


@dataclass(frozen=True)
class _FileTask:
    csv_path: Path
    source_csv_relpath: str
    source_csv_size: int
    source_csv_mtime: int
    quote: str
    symbol: str
    market: str
    tf: str


@dataclass(frozen=True)
class IngestSummary:
    discovered_files: int
    selected_files: int
    processed_files: int
    skipped_files: int
    ok_files: int
    warn_files: int
    fail_files: int
    dataset_root: Path
    manifest_file: Path
    report_file: Path
    details: tuple[dict[str, Any], ...]
    failures: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class ValidationSummary:
    checked_files: int
    ok_files: int
    warn_files: int
    fail_files: int
    report_file: Path
    details: tuple[dict[str, Any], ...]


def sniff_csv_files(
    raw_dir: Path,
    pattern: str = "upbit_*_full.csv",
    sample_files: int = 10,
    sample_rows: int = 5,
    supported_tfs: tuple[str, ...] = SUPPORTED_TFS,
) -> dict[str, Any]:
    """Inspect sample files and report filename/column mapping decisions."""

    files = sorted(path for path in raw_dir.rglob(pattern) if path.is_file())
    sampled = files[: max(sample_files, 0)]

    entries: list[dict[str, Any]] = []
    failed: list[str] = []
    for path in sampled:
        relpath = _safe_relpath(path, raw_dir)
        header, sample = _read_csv_header_and_rows(path, sample_rows)
        entry: dict[str, Any] = {
            "file": relpath,
            "header": header,
            "sample_rows": sample,
            "status": "OK",
        }

        try:
            parsed = parse_upbit_filename(path, supported_tfs=supported_tfs)
            entry["parsed"] = parsed
        except FilenameParseError as exc:
            entry["status"] = "FAIL"
            entry["error_message"] = str(exc)
            failed.append(relpath)
            entries.append(entry)
            continue

        try:
            mapping = detect_column_mapping(header)
            entry["mapping"] = mapping.as_dict()
        except ColumnMappingError as exc:
            entry["status"] = "FAIL"
            entry["error_message"] = str(exc)
            failed.append(relpath)

        entries.append(entry)

    return {
        "raw_dir": str(raw_dir),
        "pattern": pattern,
        "sampled_files": len(sampled),
        "failed_files": failed,
        "entries": entries,
    }


def ingest_dataset(options: IngestOptions) -> IngestSummary:
    """Ingest CSV files into standardized partitioned Parquet dataset."""

    started_at = int(time.time())
    options = _normalize_options(options)
    if options.engine == "duckdb" and not options.dry_run:
        # Fail fast so no file processing starts without validated temp settings.
        con = create_duckdb_connection(options.duckdb)
        con.close()

    dataset_root = options.dataset_root
    dataset_root.mkdir(parents=True, exist_ok=True)
    manifest_file = manifest_path(dataset_root)

    existing_manifest = load_manifest(manifest_file) if options.mode == "skip_unchanged" else pl.DataFrame()
    skip_index = build_manifest_index(existing_manifest) if options.mode == "skip_unchanged" else {}

    tasks, discovery_failures = _discover_tasks(options)
    discovered_files = len(tasks) + len(discovery_failures)

    if options.limit_files is not None:
        tasks = tasks[: max(options.limit_files, 0)]

    processed_rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = [dict(item) for item in discovery_failures]
    details: list[dict[str, Any]] = []
    skipped_files = 0

    for discovery_failure in discovery_failures:
        processed_rows.append(
            _build_manifest_row_from_failure(
                source_csv_relpath=discovery_failure["source_csv_relpath"],
                source_csv_size=discovery_failure["source_csv_size"],
                source_csv_mtime=discovery_failure["source_csv_mtime"],
                error_message=discovery_failure["error_message"],
            )
        )

    runnable_tasks: list[_FileTask] = []
    for task in tasks:
        if options.mode == "skip_unchanged" and should_skip_file(
            manifest_index=skip_index,
            source_csv_relpath=task.source_csv_relpath,
            source_csv_size=task.source_csv_size,
            source_csv_mtime=task.source_csv_mtime,
        ):
            skipped_files += 1
            details.append(
                {
                    "file": task.source_csv_relpath,
                    "market": task.market,
                    "tf": task.tf,
                    "status": "SKIP",
                    "reason": "unchanged",
                }
            )
            continue
        runnable_tasks.append(task)

    if options.dry_run:
        for task in runnable_tasks:
            details.append(
                {
                    "file": task.source_csv_relpath,
                    "market": task.market,
                    "tf": task.tf,
                    "quote": task.quote,
                    "symbol": task.symbol,
                    "status": "DRY_RUN",
                }
            )
    else:
        if options.workers <= 1 or len(runnable_tasks) <= 1:
            for task in runnable_tasks:
                row, detail = _ingest_one_file(task, options)
                processed_rows.append(row)
                details.append(detail)
                if row["status"] == "FAIL":
                    failures.append(detail)
        else:
            with ThreadPoolExecutor(max_workers=options.workers) as executor:
                futures = {executor.submit(_ingest_one_file, task, options): task for task in runnable_tasks}
                for future in as_completed(futures):
                    row, detail = future.result()
                    processed_rows.append(row)
                    details.append(detail)
                    if row["status"] == "FAIL":
                        failures.append(detail)

    if not options.dry_run:
        append_manifest_rows(manifest_file, processed_rows)

    ok_files = sum(1 for row in processed_rows if row.get("status") == "OK")
    warn_files = sum(1 for row in processed_rows if row.get("status") == "WARN")
    fail_files = sum(1 for row in processed_rows if row.get("status") == "FAIL")
    processed_files = len(runnable_tasks) if not options.dry_run else 0

    report_file = dataset_root / "_meta" / "ingest_report.json"
    report_file.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "started_at": started_at,
        "finished_at": int(time.time()),
        "engine": options.engine,
        "mode": options.mode,
        "compression": options.compression,
        "duckdb_temp_directory": (options.duckdb.temp_directory or DEFAULT_DUCKDB_TEMP_DIRECTORY),
        "duckdb_memory_limit": options.duckdb.memory_limit,
        "duckdb_threads": options.duckdb.threads,
        "dry_run": options.dry_run,
        "discovered_files": discovered_files,
        "selected_files": len(tasks),
        "processed_files": processed_files,
        "skipped_files": skipped_files,
        "ok_files": ok_files,
        "warn_files": warn_files,
        "fail_files": fail_files,
        "failures": failures,
    }
    report_file.write_text(json.dumps(report, indent=2), encoding="utf-8")

    return IngestSummary(
        discovered_files=discovered_files,
        selected_files=len(tasks),
        processed_files=processed_files,
        skipped_files=skipped_files,
        ok_files=ok_files,
        warn_files=warn_files,
        fail_files=fail_files,
        dataset_root=dataset_root,
        manifest_file=manifest_file,
        report_file=report_file,
        details=tuple(details),
        failures=tuple(failures),
    )


def validate_dataset(
    parquet_dir: Path,
    tf_filter: tuple[str, ...] | None = None,
    market_filter: tuple[str, ...] | None = None,
    gap_severity: str = "info",
    quote_est_severity: str = "info",
    ohlc_violation_policy: str = "drop_row_and_warn",
) -> ValidationSummary:
    """Validate existing v1 partitioned parquet files and emit a JSON report."""

    gap_severity = _normalize_severity(gap_severity, field_name="gap_severity")
    quote_est_severity = _normalize_severity(quote_est_severity, field_name="quote_est_severity")
    ohlc_violation_policy = _normalize_ohlc_violation_policy(ohlc_violation_policy)

    tf_set = {item.strip().lower() for item in tf_filter} if tf_filter else None
    market_set = {item.strip().upper() for item in market_filter} if market_filter else None

    part_files = sorted(path for path in parquet_dir.glob("tf=*/market=*/part.parquet") if path.is_file())
    details: list[dict[str, Any]] = []
    ok_files = 0
    warn_files = 0
    fail_files = 0

    for part_path in part_files:
        tf = part_path.parent.parent.name.replace("tf=", "", 1).lower()
        market = part_path.parent.name.replace("market=", "", 1).upper()
        if tf_set and tf not in tf_set:
            continue
        if market_set and market not in market_set:
            continue

        try:
            frame = pl.read_parquet(part_path)
            stats = _validate_frame(
                frame,
                tf=tf,
                gap_severity=gap_severity,
                quote_est_severity=quote_est_severity,
                ohlc_violation_policy=ohlc_violation_policy,
            )
            status = stats["status"]
            detail = {
                "file": str(part_path),
                "tf": tf,
                "market": market,
                "rows": stats["rows"],
                "min_ts_ms": stats["min_ts_ms"],
                "max_ts_ms": stats["max_ts_ms"],
                "duplicates_dropped": 0,
                "non_monotonic_found": stats["non_monotonic_found"],
                "gaps_found": stats["gaps_found"],
                "invalid_rows_dropped": stats["invalid_rows_dropped"],
                "type_cast_failure_rows": stats["type_cast_failure_rows"],
                "volume_quote_est": stats["volume_quote_est"],
                "ohlc_violations": stats["ohlc_violations"],
                "status": status,
                "status_reasons": stats["status_reasons"],
                "error_message": stats["error_message"],
            }
        except Exception as exc:
            status = "FAIL"
            detail = {
                "file": str(part_path),
                "tf": tf,
                "market": market,
                "rows": 0,
                "min_ts_ms": None,
                "max_ts_ms": None,
                "duplicates_dropped": 0,
                "non_monotonic_found": False,
                "gaps_found": 0,
                "invalid_rows_dropped": 0,
                "type_cast_failure_rows": 0,
                "volume_quote_est": False,
                "ohlc_violations": 0,
                "status": "FAIL",
                "status_reasons": ["UNEXPECTED_VALIDATE_EXCEPTION"],
                "error_message": str(exc),
            }

        details.append(detail)
        if status == "OK":
            ok_files += 1
        elif status == "WARN":
            warn_files += 1
        else:
            fail_files += 1

    report_file = parquet_dir / "_meta" / "validate_report.json"
    report_file.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "checked_files": len(details),
        "ok_files": ok_files,
        "warn_files": warn_files,
        "fail_files": fail_files,
        "details": details,
    }
    report_file.write_text(json.dumps(report, indent=2), encoding="utf-8")

    return ValidationSummary(
        checked_files=len(details),
        ok_files=ok_files,
        warn_files=warn_files,
        fail_files=fail_files,
        report_file=report_file,
        details=tuple(details),
    )


def _discover_tasks(options: IngestOptions) -> tuple[list[_FileTask], list[dict[str, Any]]]:
    tasks: list[_FileTask] = []
    failures: list[dict[str, Any]] = []

    quote_filter = {item.upper() for item in options.quote_filter} if options.quote_filter else None
    tf_filter = {item.lower() for item in options.tf_filter} if options.tf_filter else None
    symbol_filter = {item.upper() for item in options.symbol_filter} if options.symbol_filter else None

    for csv_path in sorted(path for path in options.raw_dir.rglob(options.pattern) if path.is_file()):
        stat = csv_path.stat()
        relpath = _safe_relpath(csv_path, options.raw_dir)

        try:
            parsed = parse_upbit_filename(csv_path, supported_tfs=options.supported_tfs)
        except FilenameParseError as exc:
            failures.append(
                {
                    "source_csv_relpath": relpath,
                    "source_csv_size": int(stat.st_size),
                    "source_csv_mtime": int(stat.st_mtime),
                    "error_message": str(exc),
                    "status": "FAIL",
                }
            )
            continue

        if quote_filter and parsed["quote"] not in quote_filter:
            continue
        if tf_filter and parsed["tf"] not in tf_filter:
            continue
        if symbol_filter and parsed["symbol"] not in symbol_filter:
            continue

        tasks.append(
            _FileTask(
                csv_path=csv_path,
                source_csv_relpath=relpath,
                source_csv_size=int(stat.st_size),
                source_csv_mtime=int(stat.st_mtime),
                quote=parsed["quote"],
                symbol=parsed["symbol"],
                market=parsed["market"],
                tf=parsed["tf"],
            )
        )

    return tasks, failures


def _ingest_one_file(task: _FileTask, options: IngestOptions) -> tuple[dict[str, Any], dict[str, Any]]:
    ingested_at = int(time.time())
    mapping_info: dict[str, Any] = {}
    try:
        header, _ = _read_csv_header_and_rows(task.csv_path, sample_rows=0)
        mapping = detect_column_mapping(header)
        mapping_info = mapping.as_dict()

        frame = _read_csv(task.csv_path, options)
        canonical = _to_canonical_frame(frame, mapping, options.quote_volume_policy)
        canonical, stats = _validate_and_fix_frame(canonical, tf=task.tf, options=options)

        if stats["status"] == "FAIL":
            raise ValueError(stats["error_message"] or "validation failed")

        partition_path = options.dataset_root / f"tf={task.tf}" / f"market={task.market}" / "part.parquet"
        partition_path.parent.mkdir(parents=True, exist_ok=True)
        compression = None if options.compression == "none" else options.compression
        canonical.write_parquet(partition_path, compression=compression)

        row = {
            "quote": task.quote,
            "symbol": task.symbol,
            "market": task.market,
            "tf": task.tf,
            "source_csv_relpath": task.source_csv_relpath,
            "source_csv_size": task.source_csv_size,
            "source_csv_mtime": task.source_csv_mtime,
            "ingested_at": ingested_at,
            "rows": stats["rows"],
            "min_ts_ms": stats["min_ts_ms"],
            "max_ts_ms": stats["max_ts_ms"],
            "duplicates_dropped": stats["duplicates_dropped"],
            "non_monotonic_found": stats["non_monotonic_found"],
            "gaps_found": stats["gaps_found"],
            "invalid_rows_dropped": stats["invalid_rows_dropped"],
            "type_cast_failure_rows": stats["type_cast_failure_rows"],
            "volume_quote_est": stats["volume_quote_est"],
            "ohlc_violations": stats["ohlc_violations"],
            "status": stats["status"],
            "reasons_json": json.dumps(stats["status_reasons"], ensure_ascii=False),
            "error_message": None,
            "timestamp_source": mapping.ts_source,
            "timestamp_policy": mapping.ts_policy,
            "engine": options.engine,
        }
        detail = {
            "file": task.source_csv_relpath,
            "market": task.market,
            "tf": task.tf,
            "status": stats["status"],
            "rows": stats["rows"],
            "gaps_found": stats["gaps_found"],
            "duplicates_dropped": stats["duplicates_dropped"],
            "non_monotonic_found": stats["non_monotonic_found"],
            "invalid_rows_dropped": stats["invalid_rows_dropped"],
            "type_cast_failure_rows": stats["type_cast_failure_rows"],
            "volume_quote_est": stats["volume_quote_est"],
            "ohlc_violations": stats["ohlc_violations"],
            "status_reasons": stats["status_reasons"],
            "timestamp_policy": mapping.ts_policy,
            "output": str(partition_path),
        }
        return row, detail
    except Exception as exc:
        row = {
            "quote": task.quote,
            "symbol": task.symbol,
            "market": task.market,
            "tf": task.tf,
            "source_csv_relpath": task.source_csv_relpath,
            "source_csv_size": task.source_csv_size,
            "source_csv_mtime": task.source_csv_mtime,
            "ingested_at": ingested_at,
            "rows": 0,
            "min_ts_ms": None,
            "max_ts_ms": None,
            "duplicates_dropped": 0,
            "non_monotonic_found": False,
            "gaps_found": 0,
            "invalid_rows_dropped": 0,
            "type_cast_failure_rows": 0,
            "volume_quote_est": False,
            "ohlc_violations": 0,
            "status": "FAIL",
            "reasons_json": json.dumps(["INGEST_EXCEPTION"], ensure_ascii=False),
            "error_message": str(exc),
            "timestamp_source": str(mapping_info.get("ts_source", "")),
            "timestamp_policy": str(mapping_info.get("ts_policy", "")),
            "engine": options.engine,
        }
        detail = {
            "file": task.source_csv_relpath,
            "market": task.market,
            "tf": task.tf,
            "status": "FAIL",
            "status_reasons": ["INGEST_EXCEPTION"],
            "error_message": str(exc),
        }
        return row, detail


def _build_manifest_row_from_failure(
    source_csv_relpath: str,
    source_csv_size: int,
    source_csv_mtime: int,
    error_message: str,
) -> dict[str, Any]:
    return {
        "quote": "",
        "symbol": "",
        "market": "",
        "tf": "",
        "source_csv_relpath": source_csv_relpath,
        "source_csv_size": source_csv_size,
        "source_csv_mtime": source_csv_mtime,
        "ingested_at": int(time.time()),
        "rows": 0,
        "min_ts_ms": None,
        "max_ts_ms": None,
        "duplicates_dropped": 0,
        "non_monotonic_found": False,
        "gaps_found": 0,
        "invalid_rows_dropped": 0,
        "type_cast_failure_rows": 0,
        "volume_quote_est": False,
        "ohlc_violations": 0,
        "status": "FAIL",
        "reasons_json": json.dumps(["DISCOVERY_FAILURE"], ensure_ascii=False),
        "error_message": error_message,
        "timestamp_source": "",
        "timestamp_policy": "",
        "engine": "",
    }


def _normalize_options(options: IngestOptions) -> IngestOptions:
    mode = options.mode.strip().lower()
    if mode not in {"overwrite", "skip_unchanged"}:
        raise ValueError("mode must be one of: overwrite, skip_unchanged")

    compression = options.compression.strip().lower()
    if compression not in {"zstd", "snappy", "none"}:
        raise ValueError("compression must be one of: zstd, snappy, none")

    quote_volume_policy = options.quote_volume_policy.strip().lower()
    if quote_volume_policy not in {"estimate_if_missing", "null_if_missing"}:
        raise ValueError("quote_volume_policy must be estimate_if_missing or null_if_missing")

    gap_severity = _normalize_severity(options.gap_severity, field_name="gap_severity")
    quote_est_severity = _normalize_severity(options.quote_est_severity, field_name="quote_est_severity")
    ohlc_violation_policy = _normalize_ohlc_violation_policy(options.ohlc_violation_policy)

    engine = options.engine.strip().lower()
    if engine not in {"duckdb", "polars"}:
        raise ValueError("engine must be duckdb or polars")

    return IngestOptions(
        raw_dir=options.raw_dir,
        out_dir=options.out_dir,
        dataset_name=options.dataset_name,
        pattern=options.pattern,
        workers=max(1, int(options.workers)),
        mode=mode,
        compression=compression,
        quote_filter=_normalize_filter(options.quote_filter, str.upper),
        tf_filter=_normalize_filter(options.tf_filter, lambda item: item.lower()),
        symbol_filter=_normalize_filter(options.symbol_filter, str.upper),
        limit_files=options.limit_files,
        dry_run=bool(options.dry_run),
        supported_tfs=tuple(item.lower() for item in options.supported_tfs),
        allow_sort_on_non_monotonic=bool(options.allow_sort_on_non_monotonic),
        allow_dedupe_on_duplicate_ts=bool(options.allow_dedupe_on_duplicate_ts),
        quote_volume_policy=quote_volume_policy,
        gap_severity=gap_severity,
        quote_est_severity=quote_est_severity,
        ohlc_violation_policy=ohlc_violation_policy,
        engine=engine,
        duckdb=options.duckdb,
    )


def _normalize_filter(values: tuple[str, ...] | None, normalize: Any) -> tuple[str, ...] | None:
    if values is None:
        return None
    normalized = tuple(normalize(item.strip()) for item in values if item.strip())
    return normalized or None


def _read_csv(path: Path, options: IngestOptions) -> pl.DataFrame:
    if options.engine == "polars":
        return pl.read_csv(path)

    con = create_duckdb_connection(options.duckdb)
    try:
        query = "SELECT * FROM read_csv_auto(?, HEADER=TRUE, ALL_VARCHAR=TRUE)"
        arrow_table = con.execute(query, [str(path)]).arrow()
        return pl.from_arrow(arrow_table)
    finally:
        con.close()


def _to_canonical_frame(frame: pl.DataFrame, mapping: Any, quote_volume_policy: str) -> pl.DataFrame:
    ts_expr = _timestamp_expr(mapping.ts_source, mapping.ts_policy).alias("ts_ms")

    if mapping.volume_quote_col:
        volume_quote_expr = pl.col(mapping.volume_quote_col).cast(pl.Float64, strict=False)
        volume_quote_est_expr = pl.lit(False, dtype=pl.Boolean)
    elif quote_volume_policy == "estimate_if_missing":
        volume_quote_expr = (
            pl.col(mapping.close_col).cast(pl.Float64, strict=False)
            * pl.col(mapping.volume_base_col).cast(pl.Float64, strict=False)
        ).cast(pl.Float64)
        volume_quote_est_expr = pl.lit(True, dtype=pl.Boolean)
    else:
        volume_quote_expr = pl.lit(None, dtype=pl.Float64)
        volume_quote_est_expr = pl.lit(False, dtype=pl.Boolean)

    canonical = frame.select(
        ts_expr,
        pl.col(mapping.open_col).cast(pl.Float64, strict=False).alias("open"),
        pl.col(mapping.high_col).cast(pl.Float64, strict=False).alias("high"),
        pl.col(mapping.low_col).cast(pl.Float64, strict=False).alias("low"),
        pl.col(mapping.close_col).cast(pl.Float64, strict=False).alias("close"),
        pl.col(mapping.volume_base_col).cast(pl.Float64, strict=False).alias("volume_base"),
        volume_quote_expr.alias("volume_quote"),
        volume_quote_est_expr.alias("volume_quote_est"),
    )

    canonical = canonical.select(
        [pl.col(name).cast(dtype, strict=False).alias(name) for name, dtype in CandleSchemaV1.DTYPES.items()]
    )
    return canonical


def _timestamp_expr(source_col: str, policy: str) -> pl.Expr:
    if policy == "timestamp_ms":
        ts = pl.col(source_col).cast(pl.Int64, strict=False)
        return pl.when(ts.abs() < 10_000_000_000).then(ts * 1_000).otherwise(ts).cast(pl.Int64)

    if policy == "utc_string":
        return (
            pl.col(source_col)
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .str.to_datetime(strict=False, time_zone="UTC")
            .dt.epoch(time_unit="ms")
            .cast(pl.Int64)
        )

    if policy == "kst_string":
        return (
            pl.col(source_col)
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .str.to_datetime(strict=False, time_zone="Asia/Seoul")
            .dt.convert_time_zone("UTC")
            .dt.epoch(time_unit="ms")
            .cast(pl.Int64)
        )

    return (
        pl.col(source_col)
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.to_datetime(strict=False, time_zone="UTC")
        .dt.epoch(time_unit="ms")
        .cast(pl.Int64)
    )


def _validate_and_fix_frame(frame: pl.DataFrame, tf: str, options: IngestOptions) -> tuple[pl.DataFrame, dict[str, Any]]:
    working = frame
    stats = _validate_frame(
        working,
        tf=tf,
        gap_severity=options.gap_severity,
        quote_est_severity=options.quote_est_severity,
        ohlc_violation_policy=options.ohlc_violation_policy,
    )

    invalid_rows_dropped = 0
    if int(stats.get("ohlc_violations", 0)) > 0 and options.ohlc_violation_policy == "drop_row_and_warn":
        before = working.height
        working = working.filter(~_ohlc_violation_expr())
        invalid_rows_dropped = before - working.height
        stats = _validate_frame(
            working,
            tf=tf,
            gap_severity=options.gap_severity,
            quote_est_severity=options.quote_est_severity,
            ohlc_violation_policy=options.ohlc_violation_policy,
        )

    stats["invalid_rows_dropped"] = int(stats.get("invalid_rows_dropped", 0)) + invalid_rows_dropped
    if stats["status"] == "FAIL":
        return working, stats

    duplicates_dropped = 0
    sorted_applied = False
    working = working.with_row_index("__row_id")
    if stats["non_monotonic_found"] and options.allow_sort_on_non_monotonic:
        working = working.sort(["ts_ms", "__row_id"])
        sorted_applied = True

    if stats["duplicates_found"] > 0 and options.allow_dedupe_on_duplicate_ts:
        before = working.height
        working = working.unique(subset=["ts_ms"], keep="last", maintain_order=True)
        duplicates_dropped = before - working.height

    working = working.drop("__row_id")
    working = working.select(CandleSchemaV1.COLUMN_ORDER)
    stats["duplicates_dropped"] = duplicates_dropped
    stats["sorted_applied"] = sorted_applied
    stats["rows"] = working.height
    if working.height:
        stats["min_ts_ms"] = int(working.get_column("ts_ms").min())
        stats["max_ts_ms"] = int(working.get_column("ts_ms").max())
    else:
        stats["min_ts_ms"] = None
        stats["max_ts_ms"] = None

    stats["gaps_found"] = _count_gaps(working.get_column("ts_ms"), tf=tf)
    status, reasons = _compute_status(
        stats,
        gap_severity=options.gap_severity,
        quote_est_severity=options.quote_est_severity,
        ohlc_violation_policy=options.ohlc_violation_policy,
    )
    stats["status"] = status
    stats["status_reasons"] = reasons
    stats["error_message"] = _build_error_message(stats) if status == "FAIL" else None

    return working, stats


def _validate_frame(
    frame: pl.DataFrame,
    tf: str,
    *,
    gap_severity: str,
    quote_est_severity: str,
    ohlc_violation_policy: str,
) -> dict[str, Any]:
    stats = _empty_stats()
    stats["rows"] = frame.height
    missing_columns = [col for col in CandleSchemaV1.REQUIRED_COLUMNS if col not in frame.columns]
    if missing_columns:
        stats["missing_columns"] = tuple(missing_columns)
        stats["status"] = "FAIL"
        stats["status_reasons"] = ["MISSING_REQUIRED_COLUMNS"]
        stats["error_message"] = _build_error_message(stats)
        return stats

    ts_null_rows = int(frame.get_column("ts_ms").null_count())
    required_null_expr = (
        pl.col("open").is_null()
        | pl.col("high").is_null()
        | pl.col("low").is_null()
        | pl.col("close").is_null()
        | pl.col("volume_base").is_null()
    )
    required_null_rows = int(frame.filter(required_null_expr).height)

    stats["ts_null_rows"] = ts_null_rows
    stats["required_value_null_rows"] = required_null_rows
    stats["type_cast_failure_rows"] = ts_null_rows + required_null_rows

    ts_non_null = frame.get_column("ts_ms").drop_nulls()
    if ts_non_null.len() > 0:
        stats["min_ts_ms"] = int(ts_non_null.min())
        stats["max_ts_ms"] = int(ts_non_null.max())

    if ts_null_rows == 0:
        ts_series = frame.get_column("ts_ms")
        diff = ts_series.diff().drop_nulls()
        stats["non_monotonic_found"] = bool((diff <= 0).any()) if diff.len() > 0 else False
        stats["duplicates_found"] = frame.height - int(frame.select(pl.col("ts_ms").n_unique()).item())
        stats["gaps_found"] = _count_gaps(ts_series, tf=tf)

    if required_null_rows == 0:
        ohlc_violations = int(frame.filter(_ohlc_violation_expr()).height)
        stats["ohlc_violations"] = ohlc_violations

    if "volume_quote_est" in frame.columns:
        stats["volume_quote_est"] = bool(frame.get_column("volume_quote_est").fill_null(False).any())

    status, reasons = _compute_status(
        stats,
        gap_severity=gap_severity,
        quote_est_severity=quote_est_severity,
        ohlc_violation_policy=ohlc_violation_policy,
    )
    stats["status"] = status
    stats["status_reasons"] = reasons
    stats["error_message"] = _build_error_message(stats) if status == "FAIL" else None
    return stats


def _empty_stats() -> dict[str, Any]:
    return {
        "rows": 0,
        "min_ts_ms": None,
        "max_ts_ms": None,
        "duplicates_found": 0,
        "duplicates_dropped": 0,
        "non_monotonic_found": False,
        "gaps_found": 0,
        "invalid_rows_dropped": 0,
        "type_cast_failure_rows": 0,
        "volume_quote_est": False,
        "missing_columns": (),
        "ts_null_rows": 0,
        "required_value_null_rows": 0,
        "ohlc_violations": 0,
        "sorted_applied": False,
        "status": "OK",
        "status_reasons": [],
        "error_message": None,
    }


def _ohlc_violation_expr() -> pl.Expr:
    return (pl.col("high") < pl.max_horizontal("open", "close", "low")) | (
        pl.col("low") > pl.min_horizontal("open", "close", "high")
    )


def _count_gaps(ts_series: pl.Series, tf: str) -> int:
    if ts_series.len() <= 1:
        return 0
    if str(tf).strip().lower() == "1s":
        # Upbit second candles are sparse by construction when no trade occurs in a second.
        return 0
    expected = expected_interval_ms(tf)
    sorted_diff = ts_series.sort().diff().drop_nulls()
    if sorted_diff.len() == 0:
        return 0
    return int((sorted_diff > expected).sum())


def _compute_status(
    stats: dict[str, Any],
    *,
    gap_severity: str,
    quote_est_severity: str,
    ohlc_violation_policy: str,
) -> tuple[str, list[str]]:
    scored_reasons: list[tuple[int, str]] = []
    seen: set[str] = set()

    def add(reason: str, severity: str) -> None:
        if reason in seen:
            return
        seen.add(reason)
        scored_reasons.append((_severity_rank(severity), reason))

    if stats.get("missing_columns"):
        add("MISSING_REQUIRED_COLUMNS", "fail")
    if int(stats.get("ts_null_rows", 0)) > 0:
        add("TS_NULL_FOUND", "fail")
        add("TIMESTAMP_PARSE_FAILED", "fail")
    if int(stats.get("required_value_null_rows", 0)) > 0:
        add("REQUIRED_VALUE_NULL_FOUND", "fail")
    if int(stats.get("ohlc_violations", 0)) > 0:
        add("OHLC_VIOLATIONS", "warn" if ohlc_violation_policy == "drop_row_and_warn" else "fail")

    if bool(stats.get("non_monotonic_found", False)):
        if bool(stats.get("sorted_applied", False)):
            add("NON_MONOTONIC_SORTED", "warn")
        else:
            add("NON_MONOTONIC_FOUND", "warn")
    if int(stats.get("duplicates_dropped", 0)) > 0:
        add("DUPLICATES_DROPPED", "warn")
    elif int(stats.get("duplicates_found", 0)) > 0:
        add("DUPLICATES_FOUND", "warn")
    if int(stats.get("invalid_rows_dropped", 0)) > 0:
        add("INVALID_ROWS_DROPPED", "warn")
    if int(stats.get("type_cast_failure_rows", 0)) > 0:
        add("TYPE_CAST_FAILURE_ROWS", "warn")

    if int(stats.get("gaps_found", 0)) > 0:
        add("GAPS_FOUND", gap_severity)
    if bool(stats.get("volume_quote_est", False)):
        add("VOLUME_QUOTE_ESTIMATED", quote_est_severity)

    max_severity = max((item[0] for item in scored_reasons), default=0)
    if max_severity >= _severity_rank("fail"):
        status = "FAIL"
    elif max_severity >= _severity_rank("warn"):
        status = "WARN"
    else:
        status = "OK"

    reasons = [reason for _, reason in scored_reasons]
    return status, reasons


def _build_error_message(stats: dict[str, Any]) -> str:
    messages: list[str] = []
    missing_columns = tuple(stats.get("missing_columns", ()))
    if missing_columns:
        messages.append(f"Missing required columns: {', '.join(missing_columns)}")
    ts_null_rows = int(stats.get("ts_null_rows", 0))
    if ts_null_rows > 0:
        messages.append(f"Null/parse-failed ts_ms rows: {ts_null_rows}")
    required_value_null_rows = int(stats.get("required_value_null_rows", 0))
    if required_value_null_rows > 0:
        messages.append(f"Null rows in required OHLCV columns: {required_value_null_rows}")
    ohlc_violations = int(stats.get("ohlc_violations", 0))
    if ohlc_violations > 0:
        messages.append(f"OHLC consistency violations: {ohlc_violations}")
    return "; ".join(messages) or "validation failed"


def _normalize_severity(value: str, *, field_name: str) -> str:
    normalized = value.strip().lower()
    if normalized not in VALID_QA_SEVERITIES:
        allowed = ", ".join(sorted(VALID_QA_SEVERITIES))
        raise ValueError(f"{field_name} must be one of: {allowed}")
    return normalized


def _normalize_ohlc_violation_policy(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in VALID_OHLC_VIOLATION_POLICIES:
        allowed = ", ".join(sorted(VALID_OHLC_VIOLATION_POLICIES))
        raise ValueError(f"ohlc_violation_policy must be one of: {allowed}")
    return normalized


def _severity_rank(severity: str) -> int:
    if severity == "fail":
        return 2
    if severity == "warn":
        return 1
    return 0


def _read_csv_header_and_rows(path: Path, sample_rows: int) -> tuple[list[str], list[list[str]]]:
    with path.open("r", encoding="utf-8", newline="") as stream:
        reader = csv.reader(stream)
        header = next(reader)
        samples: list[list[str]] = []
        for _ in range(max(sample_rows, 0)):
            row = next(reader, None)
            if row is None:
                break
            samples.append(row)
    return header, samples


def _safe_relpath(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path.resolve())
