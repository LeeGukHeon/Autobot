"""micro_v1 loader and join helpers for features_v2."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import polars as pl


MICRO_KEY_COLUMNS: tuple[str, ...] = ("market", "tf", "ts_ms")
MICRO_VALUE_COLUMNS: tuple[str, ...] = (
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

MICRO_BOOL_COLUMNS: set[str] = {"micro_trade_available", "micro_book_available", "micro_available"}
MICRO_INT_COLUMNS: set[str] = {
    "trade_events",
    "book_events",
    "trade_min_ts_ms",
    "trade_max_ts_ms",
    "book_min_ts_ms",
    "book_max_ts_ms",
    "trade_coverage_ms",
    "book_coverage_ms",
    "trade_count",
    "buy_count",
    "sell_count",
    "book_update_count",
}
MICRO_FLOAT_COLUMNS: set[str] = set(MICRO_VALUE_COLUMNS) - MICRO_BOOL_COLUMNS - MICRO_INT_COLUMNS - {"trade_source"}

PREFIX = "m_"
PREFIXED_MICRO_COLUMNS: tuple[str, ...] = tuple(f"{PREFIX}{name}" for name in MICRO_VALUE_COLUMNS)


@dataclass(frozen=True)
class MicroJoinStats:
    compared_rows: int
    matched_rows: int
    join_match_ratio: float | None
    micro_rows: int
    micro_available_ratio: float
    trade_coverage_p50_ms: int | None
    trade_coverage_p90_ms: int | None
    book_coverage_p50_ms: int | None
    book_coverage_p90_ms: int | None


def prefixed_micro_columns() -> list[str]:
    return list(PREFIXED_MICRO_COLUMNS)


def resolve_dataset_path(*, dataset: str | Path, parquet_root: Path) -> Path:
    if isinstance(dataset, Path):
        candidate = dataset
    else:
        candidate = Path(str(dataset).strip())
    if candidate.exists():
        return candidate
    if candidate.is_absolute():
        return candidate
    return parquet_root / candidate


def load_market_micro_frame(
    *,
    micro_root: Path,
    tf: str,
    market: str,
    from_ts_ms: int,
    to_ts_ms: int,
) -> pl.DataFrame:
    files = _micro_part_files(
        micro_root=micro_root,
        tf=tf,
        market=market,
        from_ts_ms=from_ts_ms,
        to_ts_ms=to_ts_ms,
    )
    if not files:
        return pl.DataFrame(schema=_micro_schema(), orient="row")

    frame = _load_micro_files_robust(
        files=files,
        from_ts_ms=from_ts_ms,
        to_ts_ms=to_ts_ms,
    )
    if frame.height <= 0:
        return pl.DataFrame(schema=_micro_schema(), orient="row")
    return (
        _ensure_micro_columns(frame)
        .with_columns(pl.col("ts_ms").cast(pl.Int64).alias("ts_ms"))
        .sort("ts_ms")
        .unique(subset=["ts_ms"], keep="last", maintain_order=True)
    )


def join_market_micro(
    *,
    base_frame: pl.DataFrame,
    micro_frame: pl.DataFrame,
) -> tuple[pl.DataFrame, MicroJoinStats]:
    if "ts_ms" not in base_frame.columns:
        raise ValueError("base_frame must include ts_ms")

    base = base_frame.sort("ts_ms")
    if base.height <= 0:
        return base, MicroJoinStats(
            compared_rows=0,
            matched_rows=0,
            join_match_ratio=None,
            micro_rows=int(micro_frame.height),
            micro_available_ratio=0.0,
            trade_coverage_p50_ms=None,
            trade_coverage_p90_ms=None,
            book_coverage_p50_ms=None,
            book_coverage_p90_ms=None,
        )

    micro = _ensure_micro_columns(micro_frame)
    prefixed = micro.select(
        [
            pl.col("ts_ms").cast(pl.Int64).alias("ts_ms"),
            *[pl.col(name).alias(f"{PREFIX}{name}") for name in MICRO_VALUE_COLUMNS],
        ]
    )
    joined = base.join(prefixed, on="ts_ms", how="left")

    compared_rows = int(joined.height)
    matched_rows = (
        int(joined.get_column("m_trade_source").is_not_null().sum())
        if "m_trade_source" in joined.columns
        else 0
    )
    join_match_ratio = (float(matched_rows) / float(compared_rows)) if compared_rows > 0 else None

    joined = _fill_prefixed_micro_defaults(joined)
    micro_available_ratio = (
        float(joined.get_column("m_micro_available").cast(pl.Int64).sum()) / float(compared_rows)
        if compared_rows > 0
        else 0.0
    )
    trade_p50, trade_p90 = _coverage_quantiles(joined, flag_col="m_micro_trade_available", coverage_col="m_trade_coverage_ms")
    book_p50, book_p90 = _coverage_quantiles(joined, flag_col="m_micro_book_available", coverage_col="m_book_coverage_ms")

    return joined, MicroJoinStats(
        compared_rows=compared_rows,
        matched_rows=matched_rows,
        join_match_ratio=join_match_ratio,
        micro_rows=int(micro.height),
        micro_available_ratio=micro_available_ratio,
        trade_coverage_p50_ms=trade_p50,
        trade_coverage_p90_ms=trade_p90,
        book_coverage_p50_ms=book_p50,
        book_coverage_p90_ms=book_p90,
    )


def _micro_part_files(
    *,
    micro_root: Path,
    tf: str,
    market: str,
    from_ts_ms: int,
    to_ts_ms: int,
) -> list[Path]:
    market_dir = micro_root / f"tf={str(tf).strip().lower()}" / f"market={str(market).strip().upper()}"
    if not market_dir.exists():
        return []

    files: list[Path] = []
    for day in _date_range_from_ts(from_ts_ms, to_ts_ms):
        date_dir = market_dir / f"date={day.isoformat()}"
        if not date_dir.exists():
            continue
        files.extend(path for path in sorted(date_dir.glob("*.parquet")) if path.is_file())
    return files


def _date_range_from_ts(start_ts_ms: int, end_ts_ms: int) -> list[date]:
    start_day = datetime.fromtimestamp(int(start_ts_ms) / 1000.0, tz=timezone.utc).date()
    end_day = datetime.fromtimestamp(int(end_ts_ms) / 1000.0, tz=timezone.utc).date()
    days: list[date] = []
    cursor = start_day
    while cursor <= end_day:
        days.append(cursor)
        cursor = cursor + timedelta(days=1)
    return days


def _fill_prefixed_micro_defaults(frame: pl.DataFrame) -> pl.DataFrame:
    exprs: list[pl.Expr] = []
    for raw_name in MICRO_VALUE_COLUMNS:
        name = f"{PREFIX}{raw_name}"
        if name not in frame.columns:
            if raw_name in MICRO_BOOL_COLUMNS:
                exprs.append(pl.lit(False, dtype=pl.Boolean).alias(name))
            elif raw_name in MICRO_INT_COLUMNS:
                exprs.append(pl.lit(0, dtype=pl.Int64).alias(name))
            elif raw_name == "trade_source":
                exprs.append(pl.lit("none", dtype=pl.Utf8).alias(name))
            else:
                exprs.append(pl.lit(None, dtype=pl.Float64).alias(name))
            continue

        if raw_name in MICRO_BOOL_COLUMNS:
            exprs.append(pl.col(name).fill_null(False).cast(pl.Boolean).alias(name))
        elif raw_name in MICRO_INT_COLUMNS:
            exprs.append(pl.col(name).fill_null(0).cast(pl.Int64).alias(name))
        elif raw_name == "trade_source":
            exprs.append(pl.col(name).fill_null("none").cast(pl.Utf8).alias(name))
        else:
            exprs.append(pl.col(name).cast(pl.Float64).alias(name))
    return frame.with_columns(exprs)


def _coverage_quantiles(frame: pl.DataFrame, *, flag_col: str, coverage_col: str) -> tuple[int | None, int | None]:
    if frame.height <= 0 or flag_col not in frame.columns or coverage_col not in frame.columns:
        return None, None
    subset = frame.filter(pl.col(flag_col) == True)  # noqa: E712
    if subset.height <= 0:
        return None, None
    values = [int(item) for item in subset.get_column(coverage_col).drop_nulls().to_list()]
    if not values:
        return None, None
    p50 = _quantile(values, 0.5)
    p90 = _quantile(values, 0.9)
    return p50, p90


def _quantile(values: list[int], q: float) -> int:
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


def _ensure_micro_columns(frame: pl.DataFrame) -> pl.DataFrame:
    working = frame
    for name in MICRO_KEY_COLUMNS:
        if name not in working.columns:
            if name == "ts_ms":
                working = working.with_columns(pl.lit(None, dtype=pl.Int64).alias(name))
            else:
                working = working.with_columns(pl.lit(None, dtype=pl.Utf8).alias(name))
    for name in MICRO_VALUE_COLUMNS:
        if name in working.columns:
            continue
        if name in MICRO_BOOL_COLUMNS:
            working = working.with_columns(pl.lit(False, dtype=pl.Boolean).alias(name))
        elif name in MICRO_INT_COLUMNS:
            working = working.with_columns(pl.lit(0, dtype=pl.Int64).alias(name))
        elif name == "trade_source":
            working = working.with_columns(pl.lit("none", dtype=pl.Utf8).alias(name))
        else:
            working = working.with_columns(pl.lit(None, dtype=pl.Float64).alias(name))
    return working.select(list(MICRO_KEY_COLUMNS) + list(MICRO_VALUE_COLUMNS))


def _micro_schema() -> dict[str, pl.DataType]:
    schema: dict[str, pl.DataType] = {
        "market": pl.Utf8,
        "tf": pl.Utf8,
        "ts_ms": pl.Int64,
    }
    for name in MICRO_VALUE_COLUMNS:
        if name in MICRO_BOOL_COLUMNS:
            schema[name] = pl.Boolean
        elif name in MICRO_INT_COLUMNS:
            schema[name] = pl.Int64
        elif name == "trade_source":
            schema[name] = pl.Utf8
        elif name in MICRO_FLOAT_COLUMNS:
            schema[name] = pl.Float64
        else:
            schema[name] = pl.Float64
    return schema


def _collect_lazy(lazy_frame: pl.LazyFrame) -> pl.DataFrame:
    try:
        return lazy_frame.collect(engine="streaming")
    except TypeError:
        return lazy_frame.collect(streaming=True)


def _load_micro_files_robust(
    *,
    files: list[Path],
    from_ts_ms: int,
    to_ts_ms: int,
) -> pl.DataFrame:
    try:
        lazy = pl.scan_parquet([str(path) for path in files]).filter(
            (pl.col("ts_ms") >= int(from_ts_ms)) & (pl.col("ts_ms") <= int(to_ts_ms))
        )
        wanted = [name for name in MICRO_KEY_COLUMNS + MICRO_VALUE_COLUMNS if name in lazy.collect_schema().names()]
        return _collect_lazy(lazy.select(wanted))
    except Exception:
        # Fallback for mixed daily schemas (e.g., all-null columns inferred as Null on some dates).
        parts: list[pl.DataFrame] = []
        for path in files:
            part = pl.read_parquet(str(path))
            if "ts_ms" not in part.columns:
                continue
            wanted = [name for name in MICRO_KEY_COLUMNS + MICRO_VALUE_COLUMNS if name in part.columns]
            if "ts_ms" not in wanted:
                continue
            sliced = (
                part.select(wanted)
                .filter((pl.col("ts_ms") >= int(from_ts_ms)) & (pl.col("ts_ms") <= int(to_ts_ms)))
            )
            if sliced.height > 0:
                parts.append(sliced)
        if not parts:
            return pl.DataFrame(schema=_micro_schema(), orient="row")
        return pl.concat(parts, how="vertical_relaxed")
