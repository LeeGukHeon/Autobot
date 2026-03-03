"""Validation, alignment checks, and stats for micro_v1 dataset."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import polars as pl

from .store import aggregate_report_path, load_micro_manifest, manifest_path, validate_report_path, write_json_report


REQUIRED_COLUMNS: tuple[str, ...] = (
    "market",
    "tf",
    "ts_ms",
    "trade_source",
    "trade_events",
    "book_events",
    "trade_min_ts_ms",
    "trade_max_ts_ms",
    "book_min_ts_ms",
    "book_max_ts_ms",
    "trade_coverage_ms",
    "book_coverage_ms",
    "micro_trade_available",
    "micro_book_available",
    "micro_available",
    "trade_count",
    "buy_count",
    "sell_count",
    "trade_volume_total",
    "buy_volume",
    "sell_volume",
    "trade_imbalance",
    "vwap",
    "avg_trade_size",
    "max_trade_size",
    "last_trade_price",
    "mid_mean",
    "spread_bps_mean",
    "depth_bid_top5_mean",
    "depth_ask_top5_mean",
    "imbalance_top5_mean",
    "microprice_bias_bps_mean",
    "book_update_count",
)


@dataclass(frozen=True)
class MicroValidateSummary:
    checked_files: int
    ok_files: int
    warn_files: int
    fail_files: int
    parse_ok_ratio: float
    join_match_ratio: float | None
    validate_report_file: Path
    details: tuple[dict[str, Any], ...]


def validate_micro_dataset_v1(
    *,
    out_root: Path,
    tf_set: tuple[str, ...] = ("1m", "5m"),
    base_candles_root: Path = Path("data/parquet/candles_v1"),
    join_match_warn_threshold: float = 0.98,
    join_match_fail_threshold: float = 0.90,
    micro_available_warn_threshold: float = 0.10,
    volume_fail_ratio_threshold: float = 0.001,
    price_fail_ratio_threshold: float = 0.001,
) -> MicroValidateSummary:
    selected_tf = _normalize_tf_set(tf_set)
    files = _discover_micro_part_files(out_root=out_root, tf_set=selected_tf)

    details: list[dict[str, Any]] = []
    ok_files = 0
    warn_files = 0
    fail_files = 0

    market_stats: dict[str, dict[str, Any]] = {}
    join_frames: list[pl.DataFrame] = []

    for part_file in files:
        tf_value, market_value, date_value = _parse_partition_tags(part_file)
        frame = pl.read_parquet(part_file).sort("ts_ms")

        missing_columns = [name for name in REQUIRED_COLUMNS if name not in frame.columns]
        rows = int(frame.height)
        non_monotonic = _is_non_monotonic(frame.get_column("ts_ms")) if "ts_ms" in frame.columns else True

        volume_violation_ratio = _negative_ratio(frame, ["trade_volume_total", "buy_volume", "sell_volume"])
        price_violation_ratio = _nonpositive_ratio(frame, ["vwap", "last_trade_price", "mid_mean"])

        micro_available_ratio = (
            float(frame.get_column("micro_available").cast(pl.Int64).sum()) / float(rows) if rows > 0 else 0.0
        )
        source_ratio = _trade_source_ratio(frame)
        short_trade_coverage_ratio = _short_coverage_ratio(frame, flag_col="micro_trade_available", coverage_col="trade_coverage_ms")
        short_book_coverage_ratio = _short_coverage_ratio(frame, flag_col="micro_book_available", coverage_col="book_coverage_ms")

        reasons: list[str] = []
        status = "OK"
        if missing_columns:
            status = "FAIL"
            reasons.append("SCHEMA_MISMATCH")
        if non_monotonic:
            status = "FAIL"
            reasons.append("TS_NON_MONOTONIC")
        if volume_violation_ratio > float(volume_fail_ratio_threshold):
            status = "FAIL"
            reasons.append("NEGATIVE_VOLUME_RATIO_HIGH")
        if price_violation_ratio > float(price_fail_ratio_threshold):
            status = "FAIL"
            reasons.append("NONPOSITIVE_PRICE_RATIO_HIGH")
        if rows <= 0 and status != "FAIL":
            status = "WARN"
            reasons.append("NO_ROWS")
        if micro_available_ratio < float(micro_available_warn_threshold) and status == "OK":
            status = "WARN"
            reasons.append("LOW_MICRO_AVAILABLE_RATIO")
        if source_ratio.get("none", 0.0) >= 1.0 and status == "OK":
            status = "WARN"
            reasons.append("TRADE_SOURCE_ALL_NONE")
        if short_trade_coverage_ratio >= 0.9 and status == "OK":
            status = "WARN"
            reasons.append("TRADE_COVERAGE_TOO_SHORT")
        if short_book_coverage_ratio >= 0.9 and status == "OK":
            status = "WARN"
            reasons.append("BOOK_COVERAGE_TOO_SHORT")

        detail = {
            "file": str(part_file),
            "tf": tf_value,
            "market": market_value,
            "date": date_value,
            "rows": rows,
            "missing_columns": missing_columns,
            "non_monotonic": non_monotonic,
            "micro_available_ratio": round(micro_available_ratio, 8),
            "trade_source_ratio": {k: round(v, 8) for k, v in source_ratio.items()},
            "volume_violation_ratio": round(volume_violation_ratio, 8),
            "price_violation_ratio": round(price_violation_ratio, 8),
            "short_trade_coverage_ratio": round(short_trade_coverage_ratio, 8),
            "short_book_coverage_ratio": round(short_book_coverage_ratio, 8),
            "status": status,
            "reasons": reasons,
        }
        details.append(detail)

        if status == "OK":
            ok_files += 1
        elif status == "WARN":
            warn_files += 1
        else:
            fail_files += 1

        _accumulate_market_stats(market_stats, market_value, frame)
        if tf_value == "1m" and rows > 0:
            join_frames.append(frame.select(["market", "ts_ms", "trade_events"]))

    parse_ok_ratio = _aggregate_parse_ok_ratio(out_root)
    alignment_hint = _aggregate_alignment_hint(out_root)
    join_payload = _compute_join_match_ratio(
        frames=join_frames,
        base_candles_root=base_candles_root,
        tf="1m",
        alignment_hint=alignment_hint,
        interval_ms=60_000,
    )

    join_match_ratio = join_payload.get("join_match_ratio")
    join_compared = int(join_payload.get("compared_trade_bars") or 0)
    if join_compared > 0 and join_match_ratio is not None:
        if float(join_match_ratio) < float(join_match_fail_threshold):
            fail_files += 1
        elif float(join_match_ratio) < float(join_match_warn_threshold):
            warn_files += 1
    elif join_compared <= 0:
        warn_files += 1

    per_market = _per_market_payload(market_stats)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "out_root": str(out_root),
        "tf_set": list(selected_tf),
        "checked_files": len(details),
        "ok_files": ok_files,
        "warn_files": warn_files,
        "fail_files": fail_files,
        "parse_ok_ratio": round(parse_ok_ratio, 8),
        "join": join_payload,
        "per_market": per_market,
        "thresholds": {
            "join_match_warn": float(join_match_warn_threshold),
            "join_match_fail": float(join_match_fail_threshold),
            "micro_available_warn": float(micro_available_warn_threshold),
            "volume_fail_ratio": float(volume_fail_ratio_threshold),
            "price_fail_ratio": float(price_fail_ratio_threshold),
        },
        "details": details,
    }
    report_file = validate_report_path(out_root)
    write_json_report(report_file, report)

    return MicroValidateSummary(
        checked_files=len(details),
        ok_files=ok_files,
        warn_files=warn_files,
        fail_files=fail_files,
        parse_ok_ratio=parse_ok_ratio,
        join_match_ratio=(float(join_match_ratio) if join_match_ratio is not None else None),
        validate_report_file=report_file,
        details=tuple(details),
    )


def micro_stats_v1(
    *,
    out_root: Path,
    tf_set: tuple[str, ...] = ("1m", "5m"),
) -> dict[str, Any]:
    selected_tf = _normalize_tf_set(tf_set)
    manifest = load_micro_manifest(manifest_path(out_root))
    if manifest.height > 0:
        manifest = manifest.filter(pl.col("tf").is_in(list(selected_tf)))

    rows_total = int(manifest.get_column("rows").fill_null(0).sum()) if manifest.height > 0 else 0
    micro_available_rows = (
        int(manifest.get_column("micro_available_rows").fill_null(0).sum()) if manifest.height > 0 else 0
    )
    source_ws_rows = int(manifest.get_column("trade_source_ws_rows").fill_null(0).sum()) if manifest.height > 0 else 0
    source_rest_rows = int(manifest.get_column("trade_source_rest_rows").fill_null(0).sum()) if manifest.height > 0 else 0
    source_none_rows = int(manifest.get_column("trade_source_none_rows").fill_null(0).sum()) if manifest.height > 0 else 0

    by_tf: list[dict[str, Any]] = []
    if manifest.height > 0:
        grouped = (
            manifest.group_by("tf")
            .agg(
                pl.col("rows").sum().alias("rows"),
                pl.col("micro_available_rows").sum().alias("micro_available_rows"),
                pl.col("trade_source_ws_rows").sum().alias("ws_rows"),
                pl.col("trade_source_rest_rows").sum().alias("rest_rows"),
                pl.col("trade_source_none_rows").sum().alias("none_rows"),
                pl.len().alias("parts"),
            )
            .sort("tf")
        )
        by_tf = [dict(row) for row in grouped.iter_rows(named=True)]

    aggregate_report = _load_json(aggregate_report_path(out_root))
    validate_report = _load_json(validate_report_path(out_root))

    return {
        "out_root": str(out_root),
        "tf_set": list(selected_tf),
        "manifest_file": str(manifest_path(out_root)),
        "parts": int(manifest.height),
        "rows_total": rows_total,
        "micro_available_ratio": (
            float(micro_available_rows) / float(rows_total) if rows_total > 0 else 0.0
        ),
        "trade_source_ratio": {
            "ws": (float(source_ws_rows) / float(rows_total) if rows_total > 0 else 0.0),
            "rest": (float(source_rest_rows) / float(rows_total) if rows_total > 0 else 0.0),
            "none": (float(source_none_rows) / float(rows_total) if rows_total > 0 else 0.0),
        },
        "by_tf": by_tf,
        "aggregate_report": _report_excerpt(aggregate_report),
        "validate_report": _report_excerpt(validate_report),
    }


def detect_alignment_mode(
    *,
    micro_frame: pl.DataFrame,
    base_candles_root: Path,
    sample_market: str = "KRW-BTC",
    sample_date: str | None = None,
    interval_ms: int = 60_000,
) -> dict[str, Any]:
    if micro_frame.height <= 0:
        return {
            "mode": "start",
            "match_ratio_start": None,
            "match_ratio_end": None,
            "compared_start": 0,
            "compared_end": 0,
            "sample_market": None,
            "sample_date": sample_date,
            "note": "NO_MICRO_ROWS",
        }

    markets = [str(item) for item in micro_frame.get_column("market").unique().to_list()]
    market = sample_market if sample_market in markets else markets[0]
    filtered = micro_frame.filter((pl.col("market") == market) & (pl.col("trade_events") > 0))

    if sample_date:
        start_ts = int(datetime.fromisoformat(sample_date).replace(tzinfo=timezone.utc).timestamp() * 1000)
        end_ts = start_ts + 86_400_000 - 1
        filtered = filtered.filter((pl.col("ts_ms") >= start_ts) & (pl.col("ts_ms") <= end_ts))

    if filtered.height <= 0:
        return {
            "mode": "start",
            "match_ratio_start": None,
            "match_ratio_end": None,
            "compared_start": 0,
            "compared_end": 0,
            "sample_market": market,
            "sample_date": sample_date,
            "note": "NO_TRADE_BARS_FOR_SAMPLE",
        }

    result = _market_alignment(
        market=market,
        micro_ts=filtered.get_column("ts_ms").cast(pl.Int64).to_list(),
        base_candles_root=base_candles_root,
        tf="1m",
        interval_ms=interval_ms,
    )

    ratio_start = result.get("match_ratio_start")
    ratio_end = result.get("match_ratio_end")
    mode = "start"
    if ratio_start is None and ratio_end is None:
        mode = "start"
    elif ratio_start is None:
        mode = "end"
    elif ratio_end is None:
        mode = "start"
    elif float(ratio_end) > float(ratio_start):
        mode = "end"

    return {
        "mode": mode,
        "match_ratio_start": ratio_start,
        "match_ratio_end": ratio_end,
        "compared_start": int(result.get("compared_start") or 0),
        "compared_end": int(result.get("compared_end") or 0),
        "sample_market": market,
        "sample_date": sample_date,
        "note": result.get("note"),
    }


def _compute_join_match_ratio(
    *,
    frames: list[pl.DataFrame],
    base_candles_root: Path,
    tf: str,
    alignment_hint: str | None,
    interval_ms: int,
) -> dict[str, Any]:
    if not frames:
        return {
            "join_match_ratio": None,
            "join_match_ratio_start": None,
            "join_match_ratio_end": None,
            "compared_trade_bars": 0,
            "compared_trade_bars_start": 0,
            "compared_trade_bars_end": 0,
            "alignment_mode": alignment_hint,
            "note": "NO_1M_FRAME",
            "per_market": [],
        }

    merged = pl.concat(frames, how="vertical_relaxed")
    merged = merged.filter(pl.col("trade_events") > 0)
    if merged.height <= 0:
        return {
            "join_match_ratio": None,
            "join_match_ratio_start": None,
            "join_match_ratio_end": None,
            "compared_trade_bars": 0,
            "compared_trade_bars_start": 0,
            "compared_trade_bars_end": 0,
            "alignment_mode": alignment_hint,
            "note": "NO_TRADE_BARS",
            "per_market": [],
        }

    per_market: list[dict[str, Any]] = []
    total_compared_start = 0
    total_matched_start = 0
    total_compared_end = 0
    total_matched_end = 0

    markets = sorted({str(item) for item in merged.get_column("market").unique().to_list()})
    for market in markets:
        micro_ts = (
            merged.filter(pl.col("market") == market)
            .select(pl.col("ts_ms").cast(pl.Int64))
            .unique(maintain_order=True)
            .to_series()
            .to_list()
        )
        result = _market_alignment(
            market=market,
            micro_ts=micro_ts,
            base_candles_root=base_candles_root,
            tf=tf,
            interval_ms=interval_ms,
        )
        per_market.append(result)

        compared_start = int(result.get("compared_start") or 0)
        matched_start = int(result.get("matched_start") or 0)
        compared_end = int(result.get("compared_end") or 0)
        matched_end = int(result.get("matched_end") or 0)
        total_compared_start += compared_start
        total_matched_start += matched_start
        total_compared_end += compared_end
        total_matched_end += matched_end

    ratio_start = (
        float(total_matched_start) / float(total_compared_start) if total_compared_start > 0 else None
    )
    ratio_end = float(total_matched_end) / float(total_compared_end) if total_compared_end > 0 else None

    hint = str(alignment_hint or "").strip().lower()
    selected_mode = hint if hint in {"start", "end"} else "start"
    if hint not in {"start", "end"}:
        if ratio_start is None and ratio_end is not None:
            selected_mode = "end"
        elif ratio_start is not None and ratio_end is not None and float(ratio_end) > float(ratio_start):
            selected_mode = "end"

    if selected_mode == "end":
        selected_ratio = ratio_end
        compared_selected = total_compared_end
    else:
        selected_ratio = ratio_start
        compared_selected = total_compared_start

    note = None
    if compared_selected <= 0:
        note = "NO_OVERLAP_WITH_BASE_CANDLES"

    return {
        "join_match_ratio": selected_ratio,
        "join_match_ratio_start": ratio_start,
        "join_match_ratio_end": ratio_end,
        "compared_trade_bars": compared_selected,
        "compared_trade_bars_start": total_compared_start,
        "compared_trade_bars_end": total_compared_end,
        "alignment_mode": selected_mode,
        "note": note,
        "per_market": per_market,
    }


def _market_alignment(
    *,
    market: str,
    micro_ts: list[int],
    base_candles_root: Path,
    tf: str,
    interval_ms: int,
) -> dict[str, Any]:
    candle_ts = _load_candle_ts(base_candles_root=base_candles_root, tf=tf, market=market)
    if not candle_ts:
        return {
            "market": market,
            "match_ratio_start": None,
            "match_ratio_end": None,
            "compared_start": 0,
            "compared_end": 0,
            "matched_start": 0,
            "matched_end": 0,
            "note": "NO_CANDLE_DATA",
        }

    candle_set = set(candle_ts)
    candle_min = min(candle_set)
    candle_max = max(candle_set)

    overlap_start = [ts for ts in micro_ts if candle_min <= int(ts) <= candle_max]
    overlap_end = [ts for ts in micro_ts if candle_min <= int(ts) + int(interval_ms) <= candle_max]

    matched_start = sum(1 for ts in overlap_start if int(ts) in candle_set)
    matched_end = sum(1 for ts in overlap_end if int(ts) + int(interval_ms) in candle_set)

    ratio_start = (float(matched_start) / float(len(overlap_start))) if overlap_start else None
    ratio_end = (float(matched_end) / float(len(overlap_end))) if overlap_end else None

    note = None
    if not overlap_start and not overlap_end:
        note = "NO_OVERLAP"

    return {
        "market": market,
        "match_ratio_start": ratio_start,
        "match_ratio_end": ratio_end,
        "compared_start": len(overlap_start),
        "compared_end": len(overlap_end),
        "matched_start": matched_start,
        "matched_end": matched_end,
        "note": note,
    }


def _load_candle_ts(*, base_candles_root: Path, tf: str, market: str) -> list[int]:
    files = _candle_part_files(base_candles_root=base_candles_root, tf=tf, market=market)
    if not files:
        return []

    lazy = pl.scan_parquet([str(path) for path in files]).select(
        ((pl.col("ts_ms") // _interval_ms(tf)) * _interval_ms(tf)).cast(pl.Int64).alias("ts_bucket")
    )
    frame = _collect_lazy(lazy).unique(subset=["ts_bucket"]).sort("ts_bucket")
    return frame.get_column("ts_bucket").to_list()


def _candle_part_files(*, base_candles_root: Path, tf: str, market: str) -> list[Path]:
    market_dir = base_candles_root / f"tf={tf}" / f"market={market}"
    if not market_dir.exists():
        return []

    files = sorted(path for path in market_dir.glob("part-*.parquet") if path.is_file())
    legacy = market_dir / "part.parquet"
    if legacy.exists() and legacy.is_file():
        return [legacy]
    return files


def _discover_micro_part_files(*, out_root: Path, tf_set: tuple[str, ...]) -> list[Path]:
    files: list[Path] = []
    for tf in tf_set:
        pattern = f"tf={tf}/market=*/date=*/*.parquet"
        files.extend(path for path in out_root.glob(pattern) if path.is_file())
    return sorted(files)


def _parse_partition_tags(path: Path) -> tuple[str, str, str]:
    date_value = path.parent.name.replace("date=", "", 1)
    market_value = path.parent.parent.name.replace("market=", "", 1)
    tf_value = path.parent.parent.parent.name.replace("tf=", "", 1)
    return tf_value, market_value, date_value


def _negative_ratio(frame: pl.DataFrame, columns: list[str]) -> float:
    total_cells = 0
    neg_cells = 0
    for col in columns:
        if col not in frame.columns:
            continue
        series = frame.get_column(col)
        total_cells += series.len()
        neg_cells += int((series.fill_null(0.0) < 0.0).sum())
    if total_cells <= 0:
        return 0.0
    return float(neg_cells) / float(total_cells)


def _nonpositive_ratio(frame: pl.DataFrame, columns: list[str]) -> float:
    nonnull_cells = 0
    bad_cells = 0
    for col in columns:
        if col not in frame.columns:
            continue
        series = frame.get_column(col)
        non_null = series.drop_nulls()
        nonnull_cells += non_null.len()
        if non_null.len() > 0:
            bad_cells += int((non_null <= 0.0).sum())
    if nonnull_cells <= 0:
        return 0.0
    return float(bad_cells) / float(nonnull_cells)


def _trade_source_ratio(frame: pl.DataFrame) -> dict[str, float]:
    if frame.height <= 0 or "trade_source" not in frame.columns:
        return {"ws": 0.0, "rest": 0.0, "none": 0.0}
    grouped = frame.group_by("trade_source").len()
    ratios = {"ws": 0.0, "rest": 0.0, "none": 0.0}
    for row in grouped.iter_rows(named=True):
        key = str(row.get("trade_source") or "none").strip().lower()
        if key not in ratios:
            continue
        ratios[key] = float(int(row.get("len") or 0)) / float(frame.height)
    return ratios


def _short_coverage_ratio(frame: pl.DataFrame, *, flag_col: str, coverage_col: str) -> float:
    if frame.height <= 0 or flag_col not in frame.columns or coverage_col not in frame.columns:
        return 0.0
    subset = frame.filter(pl.col(flag_col) == True)  # noqa: E712
    if subset.height <= 0:
        return 0.0
    short = int((subset.get_column(coverage_col).fill_null(0) < 10_000).sum())
    return float(short) / float(subset.height)


def _accumulate_market_stats(bucket: dict[str, dict[str, Any]], market: str, frame: pl.DataFrame) -> None:
    item = bucket.setdefault(
        market,
        {
            "bar_count": 0,
            "micro_available_rows": 0,
            "trade_source_ws_rows": 0,
            "trade_source_rest_rows": 0,
            "trade_source_none_rows": 0,
            "trade_coverages": [],
            "book_coverages": [],
        },
    )
    item["bar_count"] += int(frame.height)
    item["micro_available_rows"] += int(frame.get_column("micro_available").cast(pl.Int64).sum())

    source_counts = frame.group_by("trade_source").len()
    for row in source_counts.iter_rows(named=True):
        key = str(row.get("trade_source") or "none").strip().lower()
        count = int(row.get("len") or 0)
        if key == "ws":
            item["trade_source_ws_rows"] += count
        elif key == "rest":
            item["trade_source_rest_rows"] += count
        else:
            item["trade_source_none_rows"] += count

    trade_cov = frame.filter(pl.col("micro_trade_available") == True).get_column("trade_coverage_ms").to_list()  # noqa: E712
    book_cov = frame.filter(pl.col("micro_book_available") == True).get_column("book_coverage_ms").to_list()  # noqa: E712
    item["trade_coverages"].extend(int(v) for v in trade_cov if v is not None)
    item["book_coverages"].extend(int(v) for v in book_cov if v is not None)


def _per_market_payload(market_stats: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for market in sorted(market_stats):
        item = market_stats[market]
        bar_count = int(item["bar_count"])
        ws_rows = int(item["trade_source_ws_rows"])
        rest_rows = int(item["trade_source_rest_rows"])
        none_rows = int(item["trade_source_none_rows"])
        total = max(bar_count, 1)

        rows.append(
            {
                "market": market,
                "bar_count": bar_count,
                "micro_available_ratio": round(float(item["micro_available_rows"]) / float(total), 8),
                "trade_source_ratio": {
                    "ws": round(float(ws_rows) / float(total), 8),
                    "rest": round(float(rest_rows) / float(total), 8),
                    "none": round(float(none_rows) / float(total), 8),
                },
                "trade_coverage_ms": {
                    "p50": _quantile(item["trade_coverages"], 0.5),
                    "p90": _quantile(item["trade_coverages"], 0.9),
                },
                "book_coverage_ms": {
                    "p50": _quantile(item["book_coverages"], 0.5),
                    "p90": _quantile(item["book_coverages"], 0.9),
                },
            }
        )
    return rows


def _aggregate_parse_ok_ratio(out_root: Path) -> float:
    report = _load_json(aggregate_report_path(out_root))
    if not isinstance(report, dict):
        return 1.0
    value = report.get("parse_ok_ratio")
    try:
        if value is None:
            return 1.0
        return float(value)
    except (TypeError, ValueError):
        return 1.0


def _aggregate_alignment_hint(out_root: Path) -> str | None:
    report = _load_json(aggregate_report_path(out_root))
    if not isinstance(report, dict):
        return None
    value = str(report.get("alignment_mode") or "").strip().lower()
    return value if value in {"start", "end"} else None


def _report_excerpt(report: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(report, dict):
        return None
    keys = (
        "run_id",
        "started_at",
        "finished_at",
        "rows_written_total",
        "parse_ok_ratio",
        "alignment_mode",
        "checked_files",
        "ok_files",
        "warn_files",
        "fail_files",
        "join",
    )
    return {key: report.get(key) for key in keys if key in report}


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _interval_ms(tf: str) -> int:
    tf_value = str(tf).strip().lower()
    if tf_value == "1m":
        return 60_000
    if tf_value == "5m":
        return 300_000
    raise ValueError(f"unsupported tf: {tf}")


def _normalize_tf_set(tf_set: tuple[str, ...]) -> tuple[str, ...]:
    values: list[str] = []
    seen: set[str] = set()
    for raw in tf_set:
        tf = str(raw).strip().lower()
        if tf not in {"1m", "5m"}:
            continue
        if tf in seen:
            continue
        seen.add(tf)
        values.append(tf)
    return tuple(values or ["1m", "5m"])


def _quantile(values: list[int], q: float) -> int | None:
    if not values:
        return None
    sorted_values = sorted(int(v) for v in values)
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = max(min(float(q), 1.0), 0.0) * float(len(sorted_values) - 1)
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    if lower == upper:
        return sorted_values[lower]
    weight = position - float(lower)
    interpolated = (1.0 - weight) * float(sorted_values[lower]) + weight * float(sorted_values[upper])
    return int(round(interpolated))


def _is_non_monotonic(ts_series: pl.Series) -> bool:
    if ts_series.len() <= 1:
        return False
    diffs = ts_series.diff().drop_nulls()
    if diffs.len() <= 0:
        return False
    return bool((diffs < 0).any())


def _collect_lazy(lazy_frame: pl.LazyFrame) -> pl.DataFrame:
    try:
        return lazy_frame.collect(engine="streaming")
    except TypeError:
        return lazy_frame.collect(streaming=True)
