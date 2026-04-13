"""Compressed JSONL writer/reader for canonical raw trade parts."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import zstandard as zstd

from .raw_trade_v1 import RAW_TRADE_V1_COLUMNS


def write_raw_trade_partitions(
    *,
    out_root: Path,
    trades: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    run_id: str,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for raw in trades:
        normalized = _normalize_raw_trade_row(raw)
        if normalized is None:
            continue
        date_utc = _date_utc_from_ts_ms(int(normalized["event_ts_ms"]))
        key = (date_utc, str(normalized["market"]))
        grouped.setdefault(key, []).append(normalized)

    written_parts: list[dict[str, Any]] = []
    for (date_utc, market), rows in sorted(grouped.items()):
        rows.sort(key=lambda item: (int(item["event_ts_ms"]), int(item["sequential_id"])))
        part_dir = out_root / f"date={date_utc}" / f"market={market}"
        part_dir.mkdir(parents=True, exist_ok=True)
        part_file = _next_part_file(part_dir, run_id)
        tmp_file = part_file.with_name(part_file.name + ".tmp")
        payload = "".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n" for row in rows)

        compressor = zstd.ZstdCompressor(level=3)
        try:
            with tmp_file.open("wb") as fp:
                with compressor.stream_writer(fp) as writer:
                    writer.write(payload.encode("utf-8"))
            tmp_file.replace(part_file)
        finally:
            if tmp_file.exists():
                tmp_file.unlink()

        written_parts.append(
            {
                "date": date_utc,
                "market": market,
                "rows": len(rows),
                "min_ts_ms": int(rows[0]["event_ts_ms"]),
                "max_ts_ms": int(rows[-1]["event_ts_ms"]),
                "part_file": str(part_file),
            }
        )
    return written_parts


def read_raw_trade_part_file(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"raw trade part file not found: {path}")
    decompressor = zstd.ZstdDecompressor()
    with path.open("rb") as fp:
        with decompressor.stream_reader(fp) as reader:
            raw_bytes = reader.read()
    text = raw_bytes.decode("utf-8")

    rows: list[dict[str, Any]] = []
    for line_no, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            item = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON at {path}:{line_no}") from exc
        if not isinstance(item, dict):
            raise ValueError(f"JSON row must be object at {path}:{line_no}")
        rows.append(item)
    return rows


def _normalize_raw_trade_row(row: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(row, dict):
        return None
    normalized: dict[str, Any] = {}
    for column in RAW_TRADE_V1_COLUMNS:
        normalized[column] = row.get(column)
    market = _as_str(normalized.get("market"), upper=True)
    event_ts_ms = _as_int(normalized.get("event_ts_ms"))
    price = _as_float(normalized.get("price"))
    volume = _as_float(normalized.get("volume"))
    ask_bid = _as_str(normalized.get("ask_bid"), upper=True)
    side = _as_str(normalized.get("side"), upper=False)
    sequential_id = _as_int(normalized.get("sequential_id"))
    source = _as_str(normalized.get("source"), upper=False)
    source_event_channel = _as_str(normalized.get("source_event_channel"), upper=False)
    recv_ts_ms = _as_int(normalized.get("recv_ts_ms"))
    days_ago = _as_int(normalized.get("days_ago"))
    collected_at_ms = _as_int(normalized.get("collected_at_ms"))
    if market is None or event_ts_ms is None or sequential_id is None:
        return None
    if price is None or volume is None or price <= 0.0 or volume <= 0.0:
        return None
    if ask_bid not in {"ASK", "BID"}:
        return None
    if side not in {"buy", "sell"}:
        return None
    if source not in {"ws", "rest"}:
        return None
    if source_event_channel != "trade":
        return None

    return {
        "market": market,
        "event_ts_ms": int(event_ts_ms),
        "price": float(price),
        "volume": float(volume),
        "ask_bid": ask_bid,
        "side": side,
        "sequential_id": int(sequential_id),
        "source": source,
        "source_event_channel": "trade",
        "recv_ts_ms": recv_ts_ms,
        "days_ago": days_ago,
        "collected_at_ms": collected_at_ms,
    }


def _next_part_file(part_dir: Path, run_id: str) -> Path:
    base_name = f"part-{run_id}.jsonl.zst"
    base = part_dir / base_name
    if not base.exists():
        return base
    for idx in range(1, 10_000):
        candidate = part_dir / f"part-{run_id}-{idx:04d}.jsonl.zst"
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"Too many part files in {part_dir}")


def _date_utc_from_ts_ms(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
    return dt.date().isoformat()


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            return int(float(text))
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_str(value: Any, *, upper: bool) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text.upper() if upper else text.lower()
