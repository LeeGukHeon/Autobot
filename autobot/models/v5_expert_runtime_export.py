"""Helpers for certification-window expert table exports."""

from __future__ import annotations

from datetime import date, datetime, time, timezone
import json
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl


_EXPORTS_DIRNAME = "_runtime_exports"
_EXPORT_FILENAME = "expert_prediction_table.parquet"
_METADATA_FILENAME = "metadata.json"
OPERATING_WINDOW_TIMEZONE = "Asia/Seoul"


def _zoneinfo(name: str = OPERATING_WINDOW_TIMEZONE) -> ZoneInfo:
    return ZoneInfo(str(name or OPERATING_WINDOW_TIMEZONE))


def parse_operating_date_to_ts_ms(
    value: str | None,
    *,
    end_of_day: bool = False,
    timezone_name: str = OPERATING_WINDOW_TIMEZONE,
) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 10 and "T" not in text:
        parsed_date = date.fromisoformat(text)
        local_dt = datetime.combine(
            parsed_date,
            time(23, 59, 59, 999000) if end_of_day else time(0, 0, 0, 0),
            tzinfo=_zoneinfo(timezone_name),
        )
        return int(local_dt.astimezone(timezone.utc).timestamp() * 1000)
    normalized = text.replace("Z", "+00:00")
    parsed_dt = datetime.fromisoformat(normalized)
    if parsed_dt.tzinfo is None:
        parsed_dt = parsed_dt.replace(tzinfo=_zoneinfo(timezone_name))
    else:
        parsed_dt = parsed_dt.astimezone(timezone.utc)
    return int(parsed_dt.astimezone(timezone.utc).timestamp() * 1000)


def operating_date_from_ts_ms(ts_ms: int | None, *, timezone_name: str = OPERATING_WINDOW_TIMEZONE) -> str:
    if ts_ms is None:
        return ""
    return (
        datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
        .astimezone(_zoneinfo(timezone_name))
        .date()
        .isoformat()
    )


def operating_date_range(start: str, end: str) -> list[str]:
    start_day = date.fromisoformat(str(start).strip())
    end_day = date.fromisoformat(str(end).strip())
    if end_day < start_day:
        raise ValueError("operating_date_range requires end >= start")
    days: list[str] = []
    cursor = start_day
    while cursor <= end_day:
        days.append(cursor.isoformat())
        cursor = cursor.fromordinal(cursor.toordinal() + 1)
    return days


def build_ts_date_coverage_payload(
    ts_values: list[int] | tuple[int, ...] | Any,
    *,
    timezone_name: str = OPERATING_WINDOW_TIMEZONE,
) -> dict[str, Any]:
    raw_values = [] if ts_values is None else list(ts_values)
    values = sorted({int(item) for item in raw_values})
    if not values:
        return {
            "window_timezone": timezone_name,
            "coverage_start_date": "",
            "coverage_end_date": "",
            "coverage_dates": [],
        }
    coverage_dates = sorted({operating_date_from_ts_ms(item, timezone_name=timezone_name) for item in values if item > 0})
    return {
        "window_timezone": timezone_name,
        "coverage_start_date": coverage_dates[0] if coverage_dates else "",
        "coverage_end_date": coverage_dates[-1] if coverage_dates else "",
        "coverage_dates": coverage_dates,
    }


def build_operating_window_mask(
    ts_values: list[int] | tuple[int, ...] | Any,
    *,
    start: str,
    end: str,
    timezone_name: str = OPERATING_WINDOW_TIMEZONE,
) -> list[bool]:
    expected_dates = set(operating_date_range(start, end))
    raw_values = [] if ts_values is None else list(ts_values)
    return [
        operating_date_from_ts_ms(int(item), timezone_name=timezone_name) in expected_dates
        for item in raw_values
    ]


def build_expert_export_window_id(start: str, end: str) -> str:
    start_text = str(start).strip()
    end_text = str(end).strip()
    if not start_text or not end_text:
        raise ValueError("expert export window requires non-empty start/end")
    return f"{start_text}__{end_text}"


def resolve_expert_runtime_export_paths(run_dir: Path, start: str, end: str) -> dict[str, Path | str]:
    window_id = build_expert_export_window_id(start, end)
    export_root = Path(run_dir) / _EXPORTS_DIRNAME / window_id
    return {
        "window_id": window_id,
        "export_root": export_root,
        "export_path": export_root / _EXPORT_FILENAME,
        "metadata_path": export_root / _METADATA_FILENAME,
    }


def load_existing_expert_runtime_export(run_dir: Path, start: str, end: str) -> dict[str, Any]:
    paths = resolve_expert_runtime_export_paths(run_dir, start, end)
    metadata_path = Path(str(paths["metadata_path"]))
    export_path = Path(str(paths["export_path"]))
    metadata: dict[str, Any] = {}
    if metadata_path.exists():
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except Exception:
            metadata = {}
    return {
        "paths": paths,
        "exists": export_path.exists() and metadata_path.exists(),
        "metadata": metadata,
    }


def write_expert_runtime_export_metadata(
    *,
    run_dir: Path,
    start: str,
    end: str,
    payload: dict[str, Any],
) -> Path:
    paths = resolve_expert_runtime_export_paths(run_dir, start, end)
    export_root = Path(str(paths["export_root"]))
    export_root.mkdir(parents=True, exist_ok=True)
    metadata_path = Path(str(paths["metadata_path"]))
    doc = dict(payload or {})
    doc.setdefault("window_id", str(paths["window_id"]))
    doc.setdefault("export_path", str(paths["export_path"]))
    metadata_path.write_text(
        json.dumps(doc, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return metadata_path


def load_anchor_export_keys(anchor_export_path: Path) -> pl.DataFrame:
    resolved_path = Path(anchor_export_path).resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"anchor export parquet missing: {resolved_path}")
    frame = pl.read_parquet(resolved_path, columns=["market", "ts_ms"])
    if frame.height <= 0:
        raise ValueError("anchor export parquet is empty")
    return (
        frame.with_columns(
            pl.col("market").cast(pl.Utf8).str.strip_chars().str.to_uppercase().alias("market"),
            pl.col("ts_ms").cast(pl.Int64).alias("ts_ms"),
        )
        .unique(subset=["market", "ts_ms"], keep="first")
        .sort(["market", "ts_ms"])
    )
