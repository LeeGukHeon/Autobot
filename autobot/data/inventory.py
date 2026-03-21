"""Inventory helpers for candle datasets."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import polars as pl

from .schema_contract import expected_interval_ms


DAY_MS = 86_400_000
DEFAULT_TFS: tuple[str, ...] = ("1m", "5m", "15m", "60m", "240m")


def default_inventory_window(*, lookback_months: int = 24, end_ts_ms: int | None = None) -> tuple[int, int]:
    """Return a default `[start_ts_ms, end_ts_ms]` window in UTC."""

    lookback_days = max(int(lookback_months), 1) * 30
    if end_ts_ms is None:
        now_utc = datetime.now(timezone.utc) - timedelta(days=1)
        now_utc = now_utc.replace(second=0, microsecond=0)
        end_ts_ms = int(now_utc.timestamp() * 1000)
    start_ts_ms = end_ts_ms - (lookback_days * DAY_MS)
    return int(start_ts_ms), int(end_ts_ms)


def parse_utc_ts_ms(value: str | None, *, end_of_day: bool = False) -> int | None:
    """Parse `YYYY-MM-DD` or ISO datetime into UTC epoch ms."""

    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 10:
        parsed = datetime.fromisoformat(text)
        if end_of_day:
            parsed = parsed.replace(hour=23, minute=59, second=59, microsecond=999000)
        parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1000)
    normalized = text.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return int(parsed.timestamp() * 1000)


def ts_ms_to_utc_text(value: int | None) -> str | None:
    if value is None:
        return None
    dt = datetime.fromtimestamp(int(value) / 1000.0, tz=timezone.utc)
    return dt.isoformat(timespec="seconds")


def build_candle_inventory(
    dataset_root: Path,
    *,
    tf_filter: tuple[str, ...] | None = None,
    quote: str | None = None,
    window_start_ts_ms: int | None = None,
    window_end_ts_ms: int | None = None,
) -> dict[str, Any]:
    """Build inventory snapshot from dataset manifest."""

    tf_set = {str(item).strip().lower() for item in (tf_filter or DEFAULT_TFS) if str(item).strip()}
    quote_prefix = f"{str(quote).strip().upper()}-" if quote else None
    manifest_file = dataset_root / "_meta" / "manifest.parquet"
    if not manifest_file.exists():
        return {
            "dataset_root": str(dataset_root),
            "manifest_file": str(manifest_file),
            "window": _window_payload(window_start_ts_ms, window_end_ts_ms),
            "total_pairs": 0,
            "with_data_pairs": 0,
            "average_coverage_pct": 0.0,
            "by_tf": {},
            "entries": [],
        }

    frame = pl.read_parquet(manifest_file)
    rows = _select_latest_rows(frame)

    entries: list[dict[str, Any]] = []
    by_tf: dict[str, dict[str, Any]] = {}
    for row in rows:
        market = str(row.get("market", "")).strip().upper()
        tf = str(row.get("tf", "")).strip().lower()
        if not market or not tf:
            continue
        if tf_set and tf not in tf_set:
            continue
        if quote_prefix and not market.startswith(quote_prefix):
            continue

        rows_count = _as_int(row.get("rows"), default=0)
        min_ts_ms = _as_int(row.get("min_ts_ms"))
        max_ts_ms = _as_int(row.get("max_ts_ms"))
        coverage_ratio = _coverage_ratio(
            min_ts_ms=min_ts_ms,
            max_ts_ms=max_ts_ms,
            start_ts_ms=window_start_ts_ms,
            end_ts_ms=window_end_ts_ms,
        )
        missing_ranges = _missing_ranges(
            tf=tf,
            min_ts_ms=min_ts_ms,
            max_ts_ms=max_ts_ms,
            start_ts_ms=window_start_ts_ms,
            end_ts_ms=window_end_ts_ms,
        )

        entry = {
            "market": market,
            "tf": tf,
            "rows": rows_count,
            "min_ts_ms": min_ts_ms,
            "max_ts_ms": max_ts_ms,
            "min_utc": ts_ms_to_utc_text(min_ts_ms),
            "max_utc": ts_ms_to_utc_text(max_ts_ms),
            "status": str(row.get("status") or "").upper() or None,
            "reasons_json": str(row.get("reasons_json") or "[]"),
            "non_monotonic_found": bool(row.get("non_monotonic_found") or False),
            "gaps_found": _as_int(row.get("gaps_found"), default=0),
            "coverage_ratio": coverage_ratio,
            "coverage_pct": round(coverage_ratio * 100.0, 6),
            "missing_ranges": missing_ranges,
        }
        entries.append(entry)

        tf_bucket = by_tf.setdefault(
            tf,
            {
                "pairs": 0,
                "with_data_pairs": 0,
                "coverage_sum": 0.0,
            },
        )
        tf_bucket["pairs"] += 1
        if rows_count > 0 and min_ts_ms is not None and max_ts_ms is not None:
            tf_bucket["with_data_pairs"] += 1
        tf_bucket["coverage_sum"] += coverage_ratio

    entries.sort(key=lambda item: (item["tf"], item["market"]))
    total_pairs = len(entries)
    with_data_pairs = sum(
        1
        for item in entries
        if int(item.get("rows", 0)) > 0 and item.get("min_ts_ms") is not None and item.get("max_ts_ms") is not None
    )
    avg_coverage = (
        sum(float(item.get("coverage_ratio", 0.0)) for item in entries) / float(total_pairs)
        if total_pairs > 0
        else 0.0
    )

    by_tf_summary: dict[str, Any] = {}
    for tf, bucket in by_tf.items():
        pairs = int(bucket["pairs"])
        by_tf_summary[tf] = {
            "pairs": pairs,
            "with_data_pairs": int(bucket["with_data_pairs"]),
            "average_coverage_pct": round((float(bucket["coverage_sum"]) / float(pairs) * 100.0) if pairs > 0 else 0.0, 6),
        }

    return {
        "dataset_root": str(dataset_root),
        "manifest_file": str(manifest_file),
        "window": _window_payload(window_start_ts_ms, window_end_ts_ms),
        "total_pairs": total_pairs,
        "with_data_pairs": with_data_pairs,
        "average_coverage_pct": round(avg_coverage * 100.0, 6),
        "by_tf": by_tf_summary,
        "entries": entries,
    }


def estimate_recent_value_by_market(
    dataset_root: Path,
    *,
    end_ts_ms: int,
    lookback_days: int = 30,
    quote: str | None = None,
    preferred_tfs: tuple[str, ...] = ("1m", "5m", "15m", "60m", "240m"),
) -> tuple[dict[str, float], str | None]:
    """Estimate recent quote-value by market from local parquet candles."""

    start_ts_ms = int(end_ts_ms - (max(int(lookback_days), 1) * DAY_MS))
    quote_prefix = f"{str(quote).strip().upper()}-" if quote else None

    for tf in preferred_tfs:
        tf_value = str(tf).strip().lower()
        tf_dir = dataset_root / f"tf={tf_value}"
        if not tf_dir.exists():
            continue

        glob_path = str((tf_dir / "market=*" / "*.parquet").resolve())
        lazy = (
            pl.scan_parquet(glob_path, hive_partitioning=True)
            .filter((pl.col("ts_ms") >= start_ts_ms) & (pl.col("ts_ms") <= end_ts_ms))
            .with_columns((pl.col("volume_base") * pl.col("close")).cast(pl.Float64).alias("__value_est"))
            .group_by("market")
            .agg(pl.col("__value_est").sum().alias("value_est"))
            .filter(pl.col("value_est").is_not_null())
        )
        if quote_prefix:
            lazy = lazy.filter(pl.col("market").str.starts_with(quote_prefix))

        try:
            frame = lazy.collect(engine="streaming")
        except TypeError:
            frame = lazy.collect(streaming=True)
        except Exception:
            continue

        if frame.height <= 0:
            continue
        estimates = {
            str(row["market"]).strip().upper(): float(row["value_est"])
            for row in frame.iter_rows(named=True)
            if row.get("market") is not None and row.get("value_est") is not None
        }
        if estimates:
            return estimates, tf_value

    return {}, None


def _select_latest_rows(frame: pl.DataFrame) -> list[dict[str, Any]]:
    if frame.height <= 0:
        return []
    rows = [dict(row) for row in frame.iter_rows(named=True)]
    rows.sort(key=_manifest_sort_key)

    latest_any: dict[tuple[str, str], dict[str, Any]] = {}
    latest_ok_or_warn: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        market = str(row.get("market", "")).strip().upper()
        tf = str(row.get("tf", "")).strip().lower()
        if not market or not tf:
            continue
        key = (market, tf)
        latest_any[key] = row

        status = str(row.get("status") or "").strip().upper()
        rows_count = _as_int(row.get("rows"), default=0)
        has_span = _as_int(row.get("min_ts_ms")) is not None and _as_int(row.get("max_ts_ms")) is not None
        if status in {"OK", "WARN"} and rows_count > 0 and has_span:
            latest_ok_or_warn[key] = row

    selected = []
    for key, row in latest_any.items():
        selected.append(latest_ok_or_warn.get(key, row))
    return selected


def _manifest_sort_key(row: dict[str, Any]) -> tuple[int, int]:
    primary = _as_int(row.get("ingested_at"))
    secondary = _as_int(row.get("collected_at"))
    return (
        int(primary) if primary is not None else 0,
        int(secondary) if secondary is not None else 0,
    )


def _window_payload(start_ts_ms: int | None, end_ts_ms: int | None) -> dict[str, Any]:
    return {
        "start_ts_ms": start_ts_ms,
        "end_ts_ms": end_ts_ms,
        "start_utc": ts_ms_to_utc_text(start_ts_ms),
        "end_utc": ts_ms_to_utc_text(end_ts_ms),
    }


def _coverage_ratio(
    *,
    min_ts_ms: int | None,
    max_ts_ms: int | None,
    start_ts_ms: int | None,
    end_ts_ms: int | None,
) -> float:
    if start_ts_ms is None or end_ts_ms is None:
        return 0.0
    window_ms = max(int(end_ts_ms) - int(start_ts_ms), 1)
    if min_ts_ms is None or max_ts_ms is None:
        return 0.0
    overlap_start = max(int(min_ts_ms), int(start_ts_ms))
    overlap_end = min(int(max_ts_ms), int(end_ts_ms))
    if overlap_end <= overlap_start:
        return 0.0
    return max(0.0, min(1.0, float(overlap_end - overlap_start) / float(window_ms)))


def _missing_ranges(
    *,
    tf: str,
    min_ts_ms: int | None,
    max_ts_ms: int | None,
    start_ts_ms: int | None,
    end_ts_ms: int | None,
) -> list[dict[str, Any]]:
    if start_ts_ms is None or end_ts_ms is None:
        return []
    if end_ts_ms <= start_ts_ms:
        return []

    interval_ms = expected_interval_ms(tf)
    if min_ts_ms is None or max_ts_ms is None:
        return [
            {
                "from_ts_ms": int(start_ts_ms),
                "to_ts_ms": int(end_ts_ms),
                "from_utc": ts_ms_to_utc_text(start_ts_ms),
                "to_utc": ts_ms_to_utc_text(end_ts_ms),
                "reason": "NO_LOCAL_DATA",
            }
        ]

    ranges: list[dict[str, Any]] = []
    if min_ts_ms > start_ts_ms:
        from_ts = int(start_ts_ms)
        to_ts = int(min_ts_ms - interval_ms)
        if to_ts >= from_ts:
            ranges.append(
                {
                    "from_ts_ms": from_ts,
                    "to_ts_ms": to_ts,
                    "from_utc": ts_ms_to_utc_text(from_ts),
                    "to_utc": ts_ms_to_utc_text(to_ts),
                    "reason": "MISSING_FRONT",
                }
            )

    if max_ts_ms < end_ts_ms:
        from_ts = int(max_ts_ms + interval_ms)
        to_ts = int(end_ts_ms)
        if to_ts >= from_ts:
            ranges.append(
                {
                    "from_ts_ms": from_ts,
                    "to_ts_ms": to_ts,
                    "from_utc": ts_ms_to_utc_text(from_ts),
                    "to_utc": ts_ms_to_utc_text(to_ts),
                    "reason": "MISSING_TAIL",
                }
            )
    return ranges


def _as_int(value: Any, *, default: int | None = None) -> int | None:
    if value is None:
        return default
    try:
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return default
            return int(float(text))
        return int(value)
    except (TypeError, ValueError):
        return default
