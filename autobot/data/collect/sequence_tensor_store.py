"""Sequence and LOB tensor contract builders on top of v5 data layers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import glob
import json
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl


SECOND_FEATURE_NAMES: tuple[str, ...] = ("close", "logret_1", "volume_base", "volume_quote")
ONE_MIN_FEATURE_NAMES: tuple[str, ...] = ("close", "logret_1", "volume_base", "volume_quote")
MICRO_FEATURE_NAMES: tuple[str, ...] = (
    "trade_events",
    "trade_imbalance",
    "spread_bps_mean",
    "depth_bid_top5_mean",
    "depth_ask_top5_mean",
    "imbalance_top5_mean",
    "microprice_bias_bps_mean",
)
LOB_PER_LEVEL_CHANNELS: tuple[str, ...] = (
    "relative_price_bps",
    "bid_size",
    "ask_size",
    "normalized_depth_share",
    "event_delta",
)
LOB_GLOBAL_CHANNELS: tuple[str, ...] = (
    "spread_bps",
    "total_depth",
    "trade_imbalance",
    "tick_size",
    "relative_tick_bps",
)

SUPPORT_LEVEL_STRICT_FULL = "strict_full"
SUPPORT_LEVEL_REDUCED_CONTEXT = "reduced_context"
SUPPORT_LEVEL_STRUCTURAL_INVALID = "structural_invalid"


@dataclass(frozen=True)
class SequenceTensorBuildOptions:
    parquet_root: Path = Path("data/parquet")
    out_dataset: str = "sequence_v1"
    second_dataset: str = "candles_second_v1"
    ws_candle_dataset: str = "ws_candle_v1"
    micro_dataset: str = "micro_v1"
    lob_dataset: str = "lob30_v1"
    markets: tuple[str, ...] | None = None
    date: str | None = None
    max_markets: int = 5
    max_anchors_per_market: int = 32
    filter_markets_by_label_source: bool = True
    skip_existing_ready: bool = True
    second_lookback_steps: int = 120
    minute_lookback_steps: int = 30
    micro_lookback_steps: int = 30
    lob_lookback_steps: int = 32

    @property
    def out_root(self) -> Path:
        return self.parquet_root / self.out_dataset

    @property
    def second_root(self) -> Path:
        return self.parquet_root / self.second_dataset

    @property
    def ws_candle_root(self) -> Path:
        return self.parquet_root / self.ws_candle_dataset

    @property
    def micro_root(self) -> Path:
        return self.parquet_root / self.micro_dataset

    @property
    def lob_root(self) -> Path:
        return self.parquet_root / self.lob_dataset

    @property
    def cache_root(self) -> Path:
        return self.out_root / "cache"

    @property
    def meta_root(self) -> Path:
        return self.out_root / "_meta"

    @property
    def manifest_path(self) -> Path:
        return self.meta_root / "manifest.parquet"

    @property
    def build_report_path(self) -> Path:
        return self.meta_root / "build_report.json"

    @property
    def validate_report_path(self) -> Path:
        return self.meta_root / "validate_report.json"

    @property
    def sequence_contract_path(self) -> Path:
        return self.meta_root / "sequence_tensor_contract.json"

    @property
    def lob_contract_path(self) -> Path:
        return self.meta_root / "lob_tensor_contract.json"


@dataclass(frozen=True)
class SequenceTensorBuildSummary:
    selected_markets: int
    discovered_anchors: int
    built_anchors: int
    ok_anchors: int
    warn_anchors: int
    fail_anchors: int
    manifest_file: Path
    build_report_file: Path
    validate_report_file: Path
    sequence_contract_file: Path
    lob_contract_file: Path
    details: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class SequenceTensorValidateSummary:
    checked_files: int
    ok_files: int
    warn_files: int
    fail_files: int
    validate_report_file: Path
    details: tuple[dict[str, Any], ...]


def resolve_sequence_support_level_from_row(row: dict[str, Any] | None) -> str:
    item = dict(row or {})
    explicit = str(item.get("support_level") or "").strip().lower()
    if explicit in {
        SUPPORT_LEVEL_STRICT_FULL,
        SUPPORT_LEVEL_REDUCED_CONTEXT,
        SUPPORT_LEVEL_STRUCTURAL_INVALID,
    }:
        return explicit
    status = str(item.get("status") or "").strip().upper()
    if status == "FAIL":
        return SUPPORT_LEVEL_STRUCTURAL_INVALID
    coverage_values = [
        float(item.get("second_coverage_ratio") or 0.0),
        float(item.get("minute_coverage_ratio") or 0.0),
        float(item.get("micro_coverage_ratio") or 0.0),
        float(item.get("lob_coverage_ratio") or 0.0),
    ]
    if all(value >= 0.999999 for value in coverage_values):
        return SUPPORT_LEVEL_STRICT_FULL
    return SUPPORT_LEVEL_REDUCED_CONTEXT


@dataclass(frozen=True)
class _MarketSourceFrames:
    second_frame: pl.DataFrame
    minute_frame: pl.DataFrame
    micro_frame: pl.DataFrame
    lob_frame: pl.DataFrame


def build_sequence_tensor_store(options: SequenceTensorBuildOptions) -> SequenceTensorBuildSummary:
    options.meta_root.mkdir(parents=True, exist_ok=True)
    options.cache_root.mkdir(parents=True, exist_ok=True)

    selected_markets = _resolve_markets(options)
    details: list[dict[str, Any]] = []
    manifest_rows: list[dict[str, Any]] = []
    existing_manifest_rows = _load_manifest_rows(options.manifest_path)
    existing_ready_rows: dict[tuple[str, int], dict[str, Any]] = {}
    existing_ready_by_market_date: dict[tuple[str, str], list[int]] = {}
    for row in existing_manifest_rows:
        market = str(row.get("market") or "").strip().upper()
        anchor_ts_ms = int(row.get("anchor_ts_ms") or 0)
        date_value = str(row.get("date") or "").strip()
        cache_file_text = str(row.get("cache_file") or "").strip()
        if not market or anchor_ts_ms <= 0 or not cache_file_text:
            continue
        if str(row.get("status") or "").strip().upper() == "FAIL":
            continue
        cache_file = Path(cache_file_text)
        if not cache_file.exists() or cache_file.is_dir():
            continue
        existing_ready_rows[(market, anchor_ts_ms)] = dict(row)
        if date_value:
            existing_ready_by_market_date.setdefault((market, date_value), []).append(anchor_ts_ms)
    discovered_anchors = 0
    built_anchors = 0
    ok_anchors = 0
    warn_anchors = 0
    fail_anchors = 0
    reused_anchors = 0

    for market in selected_markets:
        if _can_skip_market_source_load(
            options=options,
            market=market,
            existing_ready_by_market_date=existing_ready_by_market_date,
        ):
            ready_ts_values = sorted(
                existing_ready_by_market_date.get((str(market).strip().upper(), str(options.date).strip()), [])
            )
            reused_count = min(len(ready_ts_values), max(int(options.max_anchors_per_market), 0))
            discovered_anchors += reused_count
            reused_anchors += reused_count
            continue
        market_frames = _load_market_source_frames(options=options, market=market)
        anchor_rows = _load_anchor_rows_from_frames(frames=market_frames)
        if options.date:
            anchor_rows = [row for row in anchor_rows if _date_utc_from_ts_ms(int(row["anchor_ts_ms"])) == str(options.date)]
        if options.max_anchors_per_market > 0:
            anchor_rows = anchor_rows[-max(int(options.max_anchors_per_market), 0) :]
        discovered_anchors += len(anchor_rows)
        for anchor_row in anchor_rows:
            anchor_ts_ms = int(anchor_row["anchor_ts_ms"])
            anchor_key = (str(market).strip().upper(), anchor_ts_ms)
            if bool(options.skip_existing_ready) and anchor_key in existing_ready_rows:
                reused_anchors += 1
                continue
            try:
                detail, manifest_row = _build_anchor_tensor(
                    options=options,
                    market=market,
                    anchor_ts_ms=anchor_ts_ms,
                    frames=market_frames,
                )
                details.append(detail)
                manifest_rows.append(manifest_row)
                built_anchors += 1
                if detail["status"] == "OK":
                    ok_anchors += 1
                elif detail["status"] == "WARN":
                    warn_anchors += 1
                else:
                    fail_anchors += 1
            except Exception as exc:
                detail = {
                    "market": market,
                    "anchor_ts_ms": anchor_ts_ms,
                    "anchor_utc": _ts_ms_to_utc_text(anchor_ts_ms),
                    "status": "FAIL",
                    "reasons": ["BUILD_EXCEPTION"],
                    "error_message": str(exc),
                }
                details.append(detail)
                manifest_rows.append(
                    {
                        "market": market,
                        "date": _date_utc_from_ts_ms(anchor_ts_ms),
                        "anchor_ts_ms": anchor_ts_ms,
                        "anchor_utc": _ts_ms_to_utc_text(anchor_ts_ms),
                        "status": "FAIL",
                        "support_level": SUPPORT_LEVEL_STRUCTURAL_INVALID,
                        "reasons_json": json.dumps(["BUILD_EXCEPTION"], ensure_ascii=False),
                        "error_message": str(exc),
                        "cache_file": "",
                        "second_coverage_ratio": 0.0,
                        "minute_coverage_ratio": 0.0,
                        "micro_coverage_ratio": 0.0,
                        "lob_coverage_ratio": 0.0,
                        "built_at_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
                    }
                )
                fail_anchors += 1

    merged_manifest_rows = _merge_manifest_rows(existing_rows=existing_manifest_rows, new_rows=manifest_rows)
    _write_manifest(path=options.manifest_path, rows=merged_manifest_rows)
    _write_contract_files(options=options)
    total_support_level_counts = {
        SUPPORT_LEVEL_STRICT_FULL: 0,
        SUPPORT_LEVEL_REDUCED_CONTEXT: 0,
        SUPPORT_LEVEL_STRUCTURAL_INVALID: 0,
    }
    total_status_counts = {"OK": 0, "WARN": 0, "FAIL": 0}
    for row in merged_manifest_rows:
        total_support_level_counts[resolve_sequence_support_level_from_row(row)] += 1
        status_value = str(row.get("status") or "").strip().upper() or "WARN"
        if status_value not in total_status_counts:
            total_status_counts[status_value] = 0
        total_status_counts[status_value] += 1
    build_report = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dataset_name": options.out_dataset,
        "dataset_root": str(options.out_root),
        "selected_markets": len(selected_markets),
        "discovered_anchors": discovered_anchors,
        "built_anchors": built_anchors,
        "reused_anchors": reused_anchors,
        "ok_anchors": ok_anchors,
        "warn_anchors": warn_anchors,
        "fail_anchors": fail_anchors,
        "delta_support_level_counts": {
            SUPPORT_LEVEL_STRICT_FULL: sum(1 for item in details if item.get("support_level") == SUPPORT_LEVEL_STRICT_FULL),
            SUPPORT_LEVEL_REDUCED_CONTEXT: sum(1 for item in details if item.get("support_level") == SUPPORT_LEVEL_REDUCED_CONTEXT),
            SUPPORT_LEVEL_STRUCTURAL_INVALID: sum(1 for item in details if item.get("support_level") == SUPPORT_LEVEL_STRUCTURAL_INVALID),
        },
        "support_level_counts": total_support_level_counts,
        "manifest_status_counts": total_status_counts,
        "manifest_file": str(options.manifest_path),
        "manifest_rows_total": len(merged_manifest_rows),
        "sequence_contract_file": str(options.sequence_contract_path),
        "lob_contract_file": str(options.lob_contract_path),
        "details": details,
    }
    options.build_report_path.write_text(json.dumps(build_report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    validate_summary = _validate_sequence_tensor_store_incremental(
        options=options,
        changed_rows=manifest_rows,
        merged_rows=merged_manifest_rows,
    )
    return SequenceTensorBuildSummary(
        selected_markets=len(selected_markets),
        discovered_anchors=discovered_anchors,
        built_anchors=built_anchors,
        ok_anchors=ok_anchors,
        warn_anchors=warn_anchors,
        fail_anchors=fail_anchors,
        manifest_file=options.manifest_path,
        build_report_file=options.build_report_path,
        validate_report_file=validate_summary.validate_report_file,
        sequence_contract_file=options.sequence_contract_path,
        lob_contract_file=options.lob_contract_path,
        details=tuple(details),
    )


def _can_skip_market_source_load(
    *,
    options: SequenceTensorBuildOptions,
    market: str,
    existing_ready_by_market_date: dict[tuple[str, str], list[int]],
) -> bool:
    if not bool(options.skip_existing_ready):
        return False
    if not options.date:
        return False
    if int(options.max_anchors_per_market) <= 0:
        return False
    target_date = str(options.date).strip()
    if not target_date:
        return False
    if target_date >= datetime.now(timezone.utc).date().isoformat():
        return False
    ready_anchor_ts = existing_ready_by_market_date.get((str(market).strip().upper(), target_date), [])
    return len(ready_anchor_ts) >= max(int(options.max_anchors_per_market), 0)


def validate_sequence_tensor_store(*, options: SequenceTensorBuildOptions) -> SequenceTensorValidateSummary:
    manifest = _load_manifest_rows(options.manifest_path)
    details = _validate_manifest_rows(rows=manifest, options=options)
    return _write_validate_report(options=options, details=details, mode="full")


def _validate_sequence_tensor_store_incremental(
    *,
    options: SequenceTensorBuildOptions,
    changed_rows: list[dict[str, Any]],
    merged_rows: list[dict[str, Any]],
) -> SequenceTensorValidateSummary:
    changed_keys = {
        (
            str(row.get("market") or "").strip().upper(),
            int(row.get("anchor_ts_ms") or 0),
        )
        for row in changed_rows
    }
    previous_details = _load_previous_validate_details(path=options.validate_report_path)
    if not changed_keys:
        if previous_details is not None:
            return _write_validate_report(options=options, details=previous_details, mode="incremental_reuse")
        return validate_sequence_tensor_store(options=options)

    changed_only = [
        row
        for row in merged_rows
        if (
            str(row.get("market") or "").strip().upper(),
            int(row.get("anchor_ts_ms") or 0),
        ) in changed_keys
    ]
    changed_details = _validate_manifest_rows(rows=changed_only, options=options)
    if previous_details is None:
        return _write_validate_report(options=options, details=changed_details, mode="incremental_seed")

    changed_detail_map = {
        (
            str(item.get("market") or "").strip().upper(),
            int(item.get("anchor_ts_ms") or 0),
        ): dict(item)
        for item in changed_details
    }
    previous_detail_map = {
        (
            str(item.get("market") or "").strip().upper(),
            int(item.get("anchor_ts_ms") or 0),
        ): dict(item)
        for item in previous_details
    }
    merged_details: list[dict[str, Any]] = []
    for row in merged_rows:
        key = (
            str(row.get("market") or "").strip().upper(),
            int(row.get("anchor_ts_ms") or 0),
        )
        if key in changed_detail_map:
            merged_details.append(dict(changed_detail_map[key]))
            continue
        previous_detail = previous_detail_map.get(key)
        cache_file_text = str(row.get("cache_file") or "").strip()
        cache_file = Path(cache_file_text) if cache_file_text else None
        if cache_file is None or not cache_file.exists() or cache_file.is_dir():
            merged_details.append(
                {
                    "market": row.get("market"),
                    "anchor_ts_ms": row.get("anchor_ts_ms"),
                    "cache_file": cache_file_text,
                    "status": "FAIL",
                    "support_level": SUPPORT_LEVEL_STRUCTURAL_INVALID,
                    "reasons": ["CACHE_FILE_MISSING"],
                }
            )
            continue
        if previous_detail is None:
            single_detail = _validate_manifest_rows(rows=[row], options=options)
            if single_detail:
                merged_details.append(dict(single_detail[0]))
            continue
        merged_details.append(dict(previous_detail))
    return _write_validate_report(options=options, details=merged_details, mode="incremental")


def _validate_manifest_rows(*, rows: list[dict[str, Any]], options: SequenceTensorBuildOptions) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for row in rows:
        cache_file_text = str(row.get("cache_file") or "").strip()
        cache_file = Path(cache_file_text) if cache_file_text else None
        status = "OK"
        reasons: list[str] = []
        if cache_file is None or not cache_file.exists() or cache_file.is_dir():
            status = "FAIL"
            reasons.append("CACHE_FILE_MISSING")
        else:
            payload = np.load(cache_file)
            checks = [
                ("second_tensor", (options.second_lookback_steps, len(SECOND_FEATURE_NAMES))),
                ("minute_tensor", (options.minute_lookback_steps, len(ONE_MIN_FEATURE_NAMES))),
                ("micro_tensor", (options.micro_lookback_steps, len(MICRO_FEATURE_NAMES))),
                ("lob_tensor", (options.lob_lookback_steps, 30, len(LOB_PER_LEVEL_CHANNELS))),
                ("lob_global_tensor", (options.lob_lookback_steps, len(LOB_GLOBAL_CHANNELS))),
                ("second_mask", (options.second_lookback_steps,)),
                ("minute_mask", (options.minute_lookback_steps,)),
                ("micro_mask", (options.micro_lookback_steps,)),
                ("lob_mask", (options.lob_lookback_steps,)),
            ]
            for key, expected_shape in checks:
                array = payload[key]
                if tuple(array.shape) != tuple(expected_shape):
                    status = "FAIL"
                    reasons.append(f"BAD_SHAPE:{key}")
            if status == "OK":
                coverage_values = [
                    float(row.get("second_coverage_ratio", 0.0)),
                    float(row.get("minute_coverage_ratio", 0.0)),
                    float(row.get("micro_coverage_ratio", 0.0)),
                    float(row.get("lob_coverage_ratio", 0.0)),
                ]
                if any(value < 0.999999 for value in coverage_values):
                    status = "WARN"
                    reasons.append("PARTIAL_COVERAGE")
        details.append(
            {
                "market": row.get("market"),
                "anchor_ts_ms": row.get("anchor_ts_ms"),
                "cache_file": str(cache_file) if cache_file is not None else "",
                "status": status,
                "support_level": resolve_sequence_support_level_from_row(
                    {
                        **dict(row),
                        "status": status,
                    }
                ),
                "reasons": reasons,
            }
        )
    return details


def _write_validate_report(
    *,
    options: SequenceTensorBuildOptions,
    details: list[dict[str, Any]],
    mode: str,
) -> SequenceTensorValidateSummary:
    ordered_details = sorted(
        details,
        key=lambda item: (
            str(item.get("market") or "").strip().upper(),
            int(item.get("anchor_ts_ms") or 0),
        ),
    )
    ok_files = 0
    warn_files = 0
    fail_files = 0
    support_level_counts = {
        SUPPORT_LEVEL_STRICT_FULL: 0,
        SUPPORT_LEVEL_REDUCED_CONTEXT: 0,
        SUPPORT_LEVEL_STRUCTURAL_INVALID: 0,
    }
    for detail in ordered_details:
        status = str(detail.get("status") or "").strip().upper()
        if status == "OK":
            ok_files += 1
        elif status == "WARN":
            warn_files += 1
        else:
            fail_files += 1
        support_level_counts[resolve_sequence_support_level_from_row(detail)] += 1
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dataset_name": options.out_dataset,
        "dataset_root": str(options.out_root),
        "validation_mode": str(mode).strip().lower(),
        "checked_files": len(ordered_details),
        "ok_files": ok_files,
        "warn_files": warn_files,
        "fail_files": fail_files,
        "support_level_counts": support_level_counts,
        "details": ordered_details,
    }
    options.validate_report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return SequenceTensorValidateSummary(
        checked_files=len(ordered_details),
        ok_files=ok_files,
        warn_files=warn_files,
        fail_files=fail_files,
        validate_report_file=options.validate_report_path,
        details=tuple(ordered_details),
    )


def _load_previous_validate_details(*, path: Path) -> list[dict[str, Any]] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    details = payload.get("details")
    if not isinstance(details, list):
        return None
    return [dict(item) for item in details if isinstance(item, dict)]


def _resolve_markets(options: SequenceTensorBuildOptions) -> tuple[str, ...]:
    explicit = tuple(
        str(item).strip().upper()
        for item in (options.markets or ())
        if str(item).strip()
    )
    if explicit:
        return _filter_markets_for_label_source_coverage(options=options, markets=explicit)

    preferred = _load_preferred_markets_from_plans(options)
    if preferred:
        return _filter_markets_for_label_source_coverage(
            options=options,
            markets=preferred[: max(int(options.max_markets), 1)],
        )

    ws_markets = sorted(
        path.name.replace("market=", "", 1).upper()
        for path in (options.ws_candle_root / "tf=1m").glob("market=*")
        if path.is_dir()
    )
    if ws_markets:
        return _filter_markets_for_label_source_coverage(
            options=options,
            markets=tuple(ws_markets[: max(int(options.max_markets), 1)]),
        )

    lob_markets = sorted(
        path.name.replace("market=", "", 1).upper()
        for path in options.lob_root.glob("market=*")
        if path.is_dir()
    )
    return _filter_markets_for_label_source_coverage(
        options=options,
        markets=tuple(lob_markets[: max(int(options.max_markets), 1)]),
    )


def _load_preferred_markets_from_plans(options: SequenceTensorBuildOptions) -> tuple[str, ...]:
    meta_root = options.parquet_root.parent / "collect" / "_meta"
    ordered: list[str] = []
    seen: set[str] = set()
    for path in (meta_root / "ws_candle_plan.json", meta_root / "lob30_plan.json"):
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        candidates = payload.get("selected_markets") or payload.get("request_codes") or []
        if not isinstance(candidates, list):
            continue
        for item in candidates:
            market = str(item).strip().upper()
            if not market or market in seen:
                continue
            if not (
                (options.ws_candle_root / "tf=1m" / f"market={market}").exists()
                or (options.lob_root / f"market={market}").exists()
                or (options.second_root / "tf=1s" / f"market={market}").exists()
            ):
                continue
            seen.add(market)
            ordered.append(market)
    return tuple(ordered)


def _filter_markets_for_label_source_coverage(
    *,
    options: SequenceTensorBuildOptions,
    markets: tuple[str, ...],
) -> tuple[str, ...]:
    ordered = tuple(str(item).strip().upper() for item in markets if str(item).strip())
    if not ordered or not bool(options.filter_markets_by_label_source):
        return ordered
    date_value = str(options.date or "").strip()
    if not date_value:
        return ordered
    min_ts_ms = _date_start_ts_ms(date_value)
    if min_ts_ms is None:
        return ordered
    filtered: list[str] = []
    for market in ordered:
        if _market_has_label_source_on_or_after_ts(options=options, market=market, min_ts_ms=min_ts_ms):
            filtered.append(market)
    return tuple(filtered)


def _market_has_label_source_on_or_after_ts(
    *,
    options: SequenceTensorBuildOptions,
    market: str,
    min_ts_ms: int,
) -> bool:
    roots = (
        options.ws_candle_root / "tf=1m",
        options.ws_candle_root / "tf=1s",
        options.second_root / "tf=1s",
        options.parquet_root / "candles_api_v1" / "tf=1m",
        options.parquet_root / "candles_v1" / "tf=1m",
    )
    for root in roots:
        latest_ts_ms = _latest_market_ts_ms(dataset_root=root, market=market)
        if latest_ts_ms is not None and int(latest_ts_ms) >= int(min_ts_ms):
            return True
    return False


def _load_market_source_frames(*, options: SequenceTensorBuildOptions, market: str) -> _MarketSourceFrames:
    second_start_ts_ms, second_end_ts_ms, minute_start_ts_ms, minute_end_ts_ms = _resolve_source_time_window(options=options)
    second_frame = _read_parquet_rows(
        options.second_root / "tf=1s" / f"market={market}" / "*.parquet",
        start_ts_ms=second_start_ts_ms,
        end_ts_ms=second_end_ts_ms,
    )
    ws_one_s_frame = _read_parquet_rows(
        options.ws_candle_root / "tf=1s" / f"market={market}" / "*.parquet",
        start_ts_ms=second_start_ts_ms,
        end_ts_ms=second_end_ts_ms,
    )
    if second_frame.height > 0 and ws_one_s_frame.height > 0:
        second_frame = (
            pl.concat([second_frame, ws_one_s_frame], how="vertical")
            .with_row_index("__row_id")
            .sort(["ts_ms", "__row_id"])
            .unique(subset=["ts_ms"], keep="last")
            .sort("ts_ms")
            .drop("__row_id")
        )
    elif second_frame.height <= 0:
        second_frame = ws_one_s_frame
    return _MarketSourceFrames(
        second_frame=second_frame,
        minute_frame=_read_parquet_rows(
            options.ws_candle_root / "tf=1m" / f"market={market}" / "*.parquet",
            start_ts_ms=minute_start_ts_ms,
            end_ts_ms=minute_end_ts_ms,
        ),
        micro_frame=_read_parquet_rows(
            options.micro_root / "tf=1m" / f"market={market}" / "date=*" / "*.parquet",
            start_ts_ms=minute_start_ts_ms,
            end_ts_ms=minute_end_ts_ms,
        ),
        lob_frame=_read_parquet_rows(
            options.lob_root / f"market={market}" / "date=*" / "*.parquet",
            start_ts_ms=minute_start_ts_ms,
            end_ts_ms=second_end_ts_ms,
        ),
    )


def _load_anchor_rows_from_frames(*, frames: _MarketSourceFrames) -> list[dict[str, Any]]:
    ws_one_m = frames.minute_frame
    if ws_one_m.height > 0 and "ts_ms" in ws_one_m.columns:
        unique_ts = sorted(int(value) for value in ws_one_m.get_column("ts_ms").unique().to_list())
        return [{"anchor_ts_ms": value} for value in unique_ts]

    second_rows = frames.second_frame
    if second_rows.height > 0 and "ts_ms" in second_rows.columns:
        anchor_ts = sorted({int(value // 60_000) * 60_000 for value in second_rows.get_column("ts_ms").to_list()})
        return [{"anchor_ts_ms": value} for value in anchor_ts]

    micro_rows = frames.micro_frame
    if micro_rows.height > 0 and "ts_ms" in micro_rows.columns:
        unique_ts = sorted(int(value) for value in micro_rows.get_column("ts_ms").unique().to_list())
        return [{"anchor_ts_ms": value} for value in unique_ts]
    return []


def _build_anchor_tensor(
    *,
    options: SequenceTensorBuildOptions,
    market: str,
    anchor_ts_ms: int,
    frames: _MarketSourceFrames,
) -> tuple[dict[str, Any], dict[str, Any]]:
    second_frame = frames.second_frame
    ws_one_m_frame = frames.minute_frame
    micro_frame = frames.micro_frame
    lob_frame = frames.lob_frame
    context_end_ts_ms = _resolve_context_end_ts_ms(
        anchor_ts_ms=anchor_ts_ms,
        second_frame=second_frame,
        lob_frame=lob_frame,
    )

    second_tensor, second_mask, second_ratio = _build_second_tensor(
        frame=second_frame,
        anchor_ts_ms=context_end_ts_ms,
        lookback_steps=options.second_lookback_steps,
    )
    minute_tensor, minute_mask, minute_ratio = _build_minute_tensor(frame=ws_one_m_frame, anchor_ts_ms=anchor_ts_ms, lookback_steps=options.minute_lookback_steps)
    micro_tensor, micro_mask, micro_ratio = _build_micro_tensor(frame=micro_frame, anchor_ts_ms=anchor_ts_ms, lookback_steps=options.micro_lookback_steps)
    lob_tensor, lob_global_tensor, lob_mask, lob_ratio = _build_lob_tensor(
        frame=lob_frame,
        micro_frame=micro_frame,
        anchor_ts_ms=context_end_ts_ms,
        lookback_steps=options.lob_lookback_steps,
    )

    reasons: list[str] = []
    if second_ratio < 1.0:
        reasons.append("PARTIAL_SECOND_CONTEXT")
    if minute_ratio < 1.0:
        reasons.append("PARTIAL_MINUTE_CONTEXT")
    if micro_ratio < 1.0:
        reasons.append("PARTIAL_MICRO_CONTEXT")
    if lob_ratio < 1.0:
        reasons.append("PARTIAL_LOB_CONTEXT")
    status = "WARN" if reasons else "OK"
    support_level = resolve_sequence_support_level_from_row(
        {
            "status": status,
            "second_coverage_ratio": second_ratio,
            "minute_coverage_ratio": minute_ratio,
            "micro_coverage_ratio": micro_ratio,
            "lob_coverage_ratio": lob_ratio,
        }
    )

    date_value = _date_utc_from_ts_ms(anchor_ts_ms)
    cache_dir = options.cache_root / f"market={market}" / f"date={date_value}"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"anchor-{anchor_ts_ms}.npz"
    np.savez_compressed(
        cache_file,
        second_tensor=second_tensor,
        second_mask=second_mask,
        minute_tensor=minute_tensor,
        minute_mask=minute_mask,
        micro_tensor=micro_tensor,
        micro_mask=micro_mask,
        lob_tensor=lob_tensor,
        lob_global_tensor=lob_global_tensor,
        lob_mask=lob_mask,
    )

    detail = {
        "market": market,
        "date": date_value,
        "anchor_ts_ms": anchor_ts_ms,
        "anchor_utc": _ts_ms_to_utc_text(anchor_ts_ms),
        "context_end_ts_ms": context_end_ts_ms,
        "context_end_utc": _ts_ms_to_utc_text(context_end_ts_ms),
        "cache_file": str(cache_file),
        "status": status,
        "support_level": support_level,
        "reasons": reasons,
        "second_coverage_ratio": round(second_ratio, 6),
        "minute_coverage_ratio": round(minute_ratio, 6),
        "micro_coverage_ratio": round(micro_ratio, 6),
        "lob_coverage_ratio": round(lob_ratio, 6),
    }
    manifest_row = {
        "market": market,
        "date": date_value,
        "anchor_ts_ms": anchor_ts_ms,
        "anchor_utc": _ts_ms_to_utc_text(anchor_ts_ms),
        "status": status,
        "support_level": support_level,
        "reasons_json": json.dumps(reasons, ensure_ascii=False),
        "error_message": None,
        "cache_file": str(cache_file),
        "second_coverage_ratio": float(second_ratio),
        "minute_coverage_ratio": float(minute_ratio),
        "micro_coverage_ratio": float(micro_ratio),
        "lob_coverage_ratio": float(lob_ratio),
        "built_at_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
    }
    return detail, manifest_row


def _resolve_context_end_ts_ms(*, anchor_ts_ms: int, second_frame: pl.DataFrame, lob_frame: pl.DataFrame) -> int:
    window_end_ts_ms = int(anchor_ts_ms + 59_000)
    candidates: list[int] = [int(anchor_ts_ms)]
    for frame in (second_frame, lob_frame):
        if frame.height <= 0 or "ts_ms" not in frame.columns:
            continue
        selected = frame.filter((pl.col("ts_ms") >= int(anchor_ts_ms)) & (pl.col("ts_ms") <= int(window_end_ts_ms)))
        if selected.height <= 0:
            continue
        max_ts = selected.get_column("ts_ms").max()
        if max_ts is not None:
            candidates.append(int(max_ts))
    return max(candidates)


def _build_second_tensor(*, frame: pl.DataFrame, anchor_ts_ms: int, lookback_steps: int) -> tuple[np.ndarray, np.ndarray, float]:
    selected = _slice_rows(frame=frame, start_ts_ms=None, end_ts_ms=anchor_ts_ms)
    if selected.height > max(int(lookback_steps), 1):
        selected = selected.tail(max(int(lookback_steps), 1))
    rows = []
    prev_close: float | None = None
    for row in selected.iter_rows(named=True):
        close = _as_float(row.get("close"))
        volume_base = _as_float(row.get("volume_base"))
        volume_quote = _as_float(row.get("volume_quote"))
        logret = 0.0
        if prev_close is not None and close is not None and prev_close > 0:
            logret = float(np.log(max(close, 1e-12) / max(prev_close, 1e-12)))
        prev_close = close if close is not None else prev_close
        rows.append([close or 0.0, logret, volume_base or 0.0, volume_quote or 0.0])
    return _left_pad_2d(rows, lookback_steps, len(SECOND_FEATURE_NAMES))


def _build_minute_tensor(*, frame: pl.DataFrame, anchor_ts_ms: int, lookback_steps: int) -> tuple[np.ndarray, np.ndarray, float]:
    start_ts_ms = anchor_ts_ms - ((max(int(lookback_steps), 1) - 1) * 60_000)
    selected = _slice_rows(frame=frame, start_ts_ms=start_ts_ms, end_ts_ms=anchor_ts_ms)
    rows = []
    prev_close: float | None = None
    for row in selected.iter_rows(named=True):
        close = _as_float(row.get("close"))
        volume_base = _as_float(row.get("volume_base"))
        volume_quote = _as_float(row.get("volume_quote"))
        logret = 0.0
        if prev_close is not None and close is not None and prev_close > 0:
            logret = float(np.log(max(close, 1e-12) / max(prev_close, 1e-12)))
        prev_close = close if close is not None else prev_close
        rows.append([close or 0.0, logret, volume_base or 0.0, volume_quote or 0.0])
    return _left_pad_2d(rows, lookback_steps, len(ONE_MIN_FEATURE_NAMES))


def _build_micro_tensor(*, frame: pl.DataFrame, anchor_ts_ms: int, lookback_steps: int) -> tuple[np.ndarray, np.ndarray, float]:
    start_ts_ms = anchor_ts_ms - ((max(int(lookback_steps), 1) - 1) * 60_000)
    selected = _slice_rows(frame=frame, start_ts_ms=start_ts_ms, end_ts_ms=anchor_ts_ms)
    rows = []
    for row in selected.iter_rows(named=True):
        rows.append(
            [
                _as_float(row.get("trade_events")) or 0.0,
                _as_float(row.get("trade_imbalance")) or 0.0,
                _as_float(row.get("spread_bps_mean")) or 0.0,
                _as_float(row.get("depth_bid_top5_mean")) or 0.0,
                _as_float(row.get("depth_ask_top5_mean")) or 0.0,
                _as_float(row.get("imbalance_top5_mean")) or 0.0,
                _as_float(row.get("microprice_bias_bps_mean")) or 0.0,
            ]
        )
    return _left_pad_2d(rows, lookback_steps, len(MICRO_FEATURE_NAMES))


def _build_lob_tensor(
    *,
    frame: pl.DataFrame,
    micro_frame: pl.DataFrame,
    anchor_ts_ms: int,
    lookback_steps: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, float]:
    selected = _slice_rows(frame=frame, start_ts_ms=None, end_ts_ms=anchor_ts_ms)
    if selected.height > max(int(lookback_steps), 1):
        selected = selected.tail(max(int(lookback_steps), 1))
    micro_map = _build_latest_micro_map(micro_frame)
    per_level_rows: list[list[list[float]]] = []
    global_rows: list[list[float]] = []
    prev_depth_shares: list[float] | None = None
    for row in selected.iter_rows(named=True):
        bid1 = _as_float(row.get("bid1_price")) or 0.0
        ask1 = _as_float(row.get("ask1_price")) or 0.0
        mid = (bid1 + ask1) / 2.0 if bid1 > 0.0 and ask1 > 0.0 else max(bid1, ask1, 1.0)
        total_depth = float((_as_float(row.get("total_ask_size")) or 0.0) + (_as_float(row.get("total_bid_size")) or 0.0))
        current_depth_shares: list[float] = []
        per_level: list[list[float]] = []
        for idx in range(1, 31):
            ask_price = _as_float(row.get(f"ask{idx}_price")) or 0.0
            bid_price = _as_float(row.get(f"bid{idx}_price")) or 0.0
            ask_size = _as_float(row.get(f"ask{idx}_size")) or 0.0
            bid_size = _as_float(row.get(f"bid{idx}_size")) or 0.0
            relative_price_bps = (((ask_price - mid) + (mid - bid_price)) / 2.0) / max(mid, 1e-12) * 10_000.0
            depth_share = (ask_size + bid_size) / max(total_depth, 1e-12)
            current_depth_shares.append(depth_share)
            prev_share = prev_depth_shares[idx - 1] if prev_depth_shares is not None else depth_share
            event_delta = depth_share - prev_share
            per_level.append([relative_price_bps, bid_size, ask_size, depth_share, event_delta])
        prev_depth_shares = current_depth_shares
        tick_size = _infer_tick_size(price=mid, quote=str(row.get("market", "")).split("-", 1)[0] if row.get("market") else "KRW")
        trade_imbalance = _latest_micro_trade_imbalance(micro_map=micro_map, ts_ms=int(row.get("ts_ms") or 0))
        spread_bps = ((ask1 - bid1) / max(mid, 1e-12)) * 10_000.0 if ask1 > 0.0 and bid1 > 0.0 else 0.0
        relative_tick_bps = tick_size / max(mid, 1e-12) * 10_000.0 if mid > 0.0 else 0.0
        global_rows.append([spread_bps, total_depth, trade_imbalance, tick_size, relative_tick_bps])
        per_level_rows.append(per_level)

    return _left_pad_lob(per_level_rows, global_rows, lookback_steps)


def _build_latest_micro_map(frame: pl.DataFrame) -> dict[int, float]:
    if frame.height <= 0 or "ts_ms" not in frame.columns:
        return {}
    rows = sorted((dict(row) for row in frame.iter_rows(named=True)), key=lambda item: int(item.get("ts_ms") or 0))
    return {int(row.get("ts_ms") or 0): float(_as_float(row.get("trade_imbalance")) or 0.0) for row in rows}


def _latest_micro_trade_imbalance(*, micro_map: dict[int, float], ts_ms: int) -> float:
    if not micro_map:
        return 0.0
    minute_ts = (int(ts_ms) // 60_000) * 60_000
    candidates = [key for key in micro_map.keys() if key <= minute_ts]
    if not candidates:
        return 0.0
    latest_key = max(candidates)
    return float(micro_map.get(latest_key) or 0.0)


def _left_pad_2d(rows: list[list[float]], steps: int, feature_count: int) -> tuple[np.ndarray, np.ndarray, float]:
    step_count = max(int(steps), 1)
    arr = np.zeros((step_count, feature_count), dtype=np.float32)
    mask = np.zeros((step_count,), dtype=np.float32)
    trimmed = rows[-step_count:]
    offset = step_count - len(trimmed)
    for idx, row in enumerate(trimmed):
        arr[offset + idx, :] = np.asarray(row, dtype=np.float32)
        mask[offset + idx] = 1.0
    coverage = float(len(trimmed)) / float(step_count)
    return arr, mask, coverage


def _left_pad_lob(
    per_level_rows: list[list[list[float]]],
    global_rows: list[list[float]],
    steps: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, float]:
    step_count = max(int(steps), 1)
    lob_arr = np.zeros((step_count, 30, len(LOB_PER_LEVEL_CHANNELS)), dtype=np.float32)
    global_arr = np.zeros((step_count, len(LOB_GLOBAL_CHANNELS)), dtype=np.float32)
    mask = np.zeros((step_count,), dtype=np.float32)
    trimmed_levels = per_level_rows[-step_count:]
    trimmed_globals = global_rows[-step_count:]
    offset = step_count - len(trimmed_levels)
    for idx, row in enumerate(trimmed_levels):
        lob_arr[offset + idx, :, :] = np.asarray(row, dtype=np.float32)
        global_arr[offset + idx, :] = np.asarray(trimmed_globals[idx], dtype=np.float32)
        mask[offset + idx] = 1.0
    coverage = float(len(trimmed_levels)) / float(step_count)
    return lob_arr, global_arr, mask, coverage


def _write_contract_files(options: SequenceTensorBuildOptions) -> None:
    sequence_contract = {
        "version": 1,
        "policy": "sequence_tensor_contract_v1",
        "dataset_name": options.out_dataset,
        "dataset_root": str(options.out_root),
        "cache_format": "npz_v1",
        "time_order": "oldest_to_newest",
        "anchor_granularity": "1m",
        "source_priority": {
            "second": [options.second_dataset, f"{options.ws_candle_dataset}:1s_fallback"],
            "minute": [f"{options.ws_candle_dataset}:1m"],
            "micro": [f"{options.micro_dataset}:1m"],
            "lob": [options.lob_dataset],
        },
        "second_tensor": {
            "lookback_steps": int(options.second_lookback_steps),
            "interval_ms": 1_000,
            "feature_names": list(SECOND_FEATURE_NAMES),
        },
        "minute_tensor": {
            "lookback_steps": int(options.minute_lookback_steps),
            "interval_ms": 60_000,
            "feature_names": list(ONE_MIN_FEATURE_NAMES),
        },
        "micro_tensor": {
            "lookback_steps": int(options.micro_lookback_steps),
            "interval_ms": 60_000,
            "feature_names": list(MICRO_FEATURE_NAMES),
        },
        "mask_fields": ["second_mask", "minute_mask", "micro_mask", "lob_mask"],
    }
    lob_contract = {
        "version": 1,
        "policy": "lob_tensor_contract_v1",
        "source_dataset": options.lob_dataset,
        "shape": {
            "layout": "TLC",
            "lookback_steps": int(options.lob_lookback_steps),
            "levels": 30,
            "per_level_channels": len(LOB_PER_LEVEL_CHANNELS),
        },
        "per_level_channels": list(LOB_PER_LEVEL_CHANNELS),
        "global_channels": list(LOB_GLOBAL_CHANNELS),
        "global_shape": {
            "layout": "TG",
            "lookback_steps": int(options.lob_lookback_steps),
            "global_channels": len(LOB_GLOBAL_CHANNELS),
        },
        "event_delta_definition": "change in normalized_depth_share versus previous snapshot at the same paired level",
        "tick_size_rule": "krw_table_or_quote_floor",
    }
    options.sequence_contract_path.write_text(json.dumps(sequence_contract, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    options.lob_contract_path.write_text(json.dumps(lob_contract, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _write_manifest(*, path: Path, rows: list[dict[str, Any]]) -> None:
    schema = {
        "market": pl.Utf8,
        "date": pl.Utf8,
        "anchor_ts_ms": pl.Int64,
        "anchor_utc": pl.Utf8,
        "status": pl.Utf8,
        "support_level": pl.Utf8,
        "reasons_json": pl.Utf8,
        "error_message": pl.Utf8,
        "cache_file": pl.Utf8,
        "second_coverage_ratio": pl.Float64,
        "minute_coverage_ratio": pl.Float64,
        "micro_coverage_ratio": pl.Float64,
        "lob_coverage_ratio": pl.Float64,
        "built_at_ms": pl.Int64,
    }
    frame = pl.DataFrame(rows, schema=schema, orient="row") if rows else pl.DataFrame([], schema=schema, orient="row")
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(path, compression="zstd")


def _merge_manifest_rows(*, existing_rows: list[dict[str, Any]], new_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, int], dict[str, Any]] = {}
    for row in existing_rows:
        market = str(row.get("market") or "").strip().upper()
        anchor_ts_ms = int(row.get("anchor_ts_ms") or 0)
        cache_file_text = str(row.get("cache_file") or "").strip()
        status = str(row.get("status") or "").strip().upper()
        if not market or anchor_ts_ms <= 0:
            continue
        if status == "FAIL" or not cache_file_text:
            continue
        merged[(market, anchor_ts_ms)] = dict(row)
    for row in new_rows:
        market = str(row.get("market") or "").strip().upper()
        anchor_ts_ms = int(row.get("anchor_ts_ms") or 0)
        cache_file_text = str(row.get("cache_file") or "").strip()
        status = str(row.get("status") or "").strip().upper()
        if not market or anchor_ts_ms <= 0:
            continue
        if status == "FAIL" or not cache_file_text:
            merged.pop((market, anchor_ts_ms), None)
            continue
        payload = dict(row)
        payload["market"] = market
        payload["anchor_ts_ms"] = anchor_ts_ms
        merged[(market, anchor_ts_ms)] = payload
    return sorted(
        merged.values(),
        key=lambda item: (
            str(item.get("market") or "").strip().upper(),
            int(item.get("anchor_ts_ms") or 0),
        ),
    )


def _load_manifest_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    frame = pl.read_parquet(path)
    return [dict(row) for row in frame.iter_rows(named=True)]


def _latest_market_ts_ms(*, dataset_root: Path, market: str) -> int | None:
    market_root = dataset_root / f"market={market}"
    if not market_root.exists():
        return None
    files = sorted(path for path in market_root.glob("*.parquet") if path.is_file())
    if not files:
        for date_dir in sorted(market_root.glob("date=*")):
            if not date_dir.is_dir():
                continue
            files.extend(sorted(path for path in date_dir.glob("*.parquet") if path.is_file()))
    latest_ts_ms: int | None = None
    for path in files:
        try:
            frame = pl.scan_parquet(str(path)).select(pl.col("ts_ms").max().alias("ts_ms")).collect()
        except Exception:
            continue
        if frame.height <= 0 or "ts_ms" not in frame.columns:
            continue
        value = frame.item(0, "ts_ms")
        if value is None:
            continue
        latest_ts_ms = int(value) if latest_ts_ms is None else max(latest_ts_ms, int(value))
    return latest_ts_ms


def _resolve_source_time_window(
    *,
    options: SequenceTensorBuildOptions,
) -> tuple[int | None, int | None, int | None, int | None]:
    if not options.date:
        return None, None, None, None
    date_start_ts_ms = _date_start_ts_ms(str(options.date))
    if date_start_ts_ms is None:
        return None, None, None, None
    date_end_ts_ms = int(date_start_ts_ms) + 86_400_000 - 1
    second_lookback_ms = max(int(options.second_lookback_steps), 1) * 1_000
    minute_context_lookback_ms = max(
        int(options.minute_lookback_steps),
        int(options.micro_lookback_steps),
        int(options.lob_lookback_steps),
        1,
    ) * 60_000
    second_start_ts_ms = int(date_start_ts_ms) - max(second_lookback_ms, 60_000)
    second_end_ts_ms = int(date_end_ts_ms) + 60_000
    minute_start_ts_ms = int(date_start_ts_ms) - max(minute_context_lookback_ms, 60_000)
    minute_end_ts_ms = int(date_end_ts_ms) + 60_000
    return second_start_ts_ms, second_end_ts_ms, minute_start_ts_ms, minute_end_ts_ms


def _read_parquet_rows(
    glob_path: Path,
    *,
    start_ts_ms: int | None = None,
    end_ts_ms: int | None = None,
) -> pl.DataFrame:
    files = sorted(Path(path) for path in glob.glob(str(glob_path)))
    if not files:
        return pl.DataFrame()
    frames: list[pl.DataFrame] = []
    for path in files:
        lazy = pl.scan_parquet(str(path))
        if start_ts_ms is not None:
            lazy = lazy.filter(pl.col("ts_ms") >= int(start_ts_ms))
        if end_ts_ms is not None:
            lazy = lazy.filter(pl.col("ts_ms") <= int(end_ts_ms))
        try:
            frame = lazy.collect(engine="streaming")
        except TypeError:
            frame = lazy.collect(streaming=True)
        if frame.height > 0:
            frames.append(frame)
    if not frames:
        return pl.DataFrame()
    if len(frames) == 1:
        return frames[0]
    return pl.concat(frames, how="diagonal_relaxed")


def _slice_rows(frame: pl.DataFrame, *, start_ts_ms: int | None, end_ts_ms: int | None) -> pl.DataFrame:
    if frame.height <= 0 or "ts_ms" not in frame.columns:
        return pl.DataFrame()
    working = frame.sort("ts_ms")
    if start_ts_ms is not None:
        working = working.filter(pl.col("ts_ms") >= int(start_ts_ms))
    if end_ts_ms is not None:
        working = working.filter(pl.col("ts_ms") <= int(end_ts_ms))
    return working


def _date_utc_from_ts_ms(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
    return dt.date().isoformat()


def _date_start_ts_ms(value: str) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    except ValueError:
        return None


def _ts_ms_to_utc_text(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
    return dt.isoformat(timespec="seconds")


def _infer_tick_size(*, price: float, quote: str) -> float:
    quote_value = str(quote).strip().upper()
    if quote_value != "KRW":
        return 0.00000001
    px = max(float(price), 0.0)
    if px >= 2_000_000:
        return 1000.0
    if px >= 1_000_000:
        return 500.0
    if px >= 500_000:
        return 100.0
    if px >= 100_000:
        return 50.0
    if px >= 10_000:
        return 10.0
    if px >= 1_000:
        return 1.0
    if px >= 100:
        return 0.1
    if px >= 10:
        return 0.01
    if px >= 1:
        return 0.001
    return 0.0001


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
