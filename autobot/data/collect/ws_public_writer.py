"""Rotating JSONL.ZST writer for Upbit public websocket raw data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time
from typing import Any

import zstandard as zstd


VALID_CHANNELS: set[str] = {"trade", "orderbook"}


@dataclass
class _OpenPart:
    channel: str
    date_utc: str
    hour_utc: str
    seq: int
    part_file: Path
    tmp_file: Path
    fp: Any
    writer: Any
    opened_at_monotonic: float
    rows: int = 0
    min_ts_ms: int | None = None
    max_ts_ms: int | None = None
    bytes_uncompressed: int = 0


class WsRawRotatingWriter:
    def __init__(
        self,
        *,
        raw_root: Path,
        run_id: str,
        rotate_sec: int = 300,
        max_bytes: int = 64 * 1024 * 1024,
        compression_level: int = 3,
    ) -> None:
        self._raw_root = raw_root
        self._run_id = run_id
        self._rotate_sec = max(int(rotate_sec), 1)
        self._max_bytes = max(int(max_bytes), 1024)
        self._compression_level = int(compression_level)
        self._open_parts: dict[str, _OpenPart] = {}
        self._next_seq: dict[str, int] = {}
        self._closed_parts: list[dict[str, Any]] = []

    @property
    def closed_parts(self) -> list[dict[str, Any]]:
        return list(self._closed_parts)

    def write(self, *, channel: str, row: dict[str, Any], event_ts_ms: int) -> None:
        channel_value = str(channel).strip().lower()
        if channel_value not in VALID_CHANNELS:
            raise ValueError(f"unsupported channel: {channel}")
        ts_ms = int(event_ts_ms)
        date_utc, hour_utc = _date_hour_from_ts_ms(ts_ms)
        payload = (json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")

        part = self._open_parts.get(channel_value)
        now = time.monotonic()
        if part is None or self._should_rotate(
            part=part,
            now_monotonic=now,
            next_bytes=len(payload),
            date_utc=date_utc,
            hour_utc=hour_utc,
        ):
            if part is not None:
                self._close_part(part, status="OK", reasons=[])
            part = self._open_part(channel=channel_value, date_utc=date_utc, hour_utc=hour_utc, now_monotonic=now)
            self._open_parts[channel_value] = part

        part.writer.write(payload)
        part.rows += 1
        part.bytes_uncompressed += len(payload)
        if part.min_ts_ms is None or ts_ms < part.min_ts_ms:
            part.min_ts_ms = ts_ms
        if part.max_ts_ms is None or ts_ms > part.max_ts_ms:
            part.max_ts_ms = ts_ms

    def close(self) -> list[dict[str, Any]]:
        for channel, part in list(self._open_parts.items()):
            self._close_part(part, status="OK", reasons=[])
            self._open_parts.pop(channel, None)
        return list(self._closed_parts)

    def _open_part(self, *, channel: str, date_utc: str, hour_utc: str, now_monotonic: float) -> _OpenPart:
        seq = int(self._next_seq.get(channel, 0) + 1)
        self._next_seq[channel] = seq
        part_dir = self._raw_root / channel / f"date={date_utc}" / f"hour={hour_utc}"
        part_dir.mkdir(parents=True, exist_ok=True)
        part_file = part_dir / f"part-{self._run_id}-{seq:06d}.jsonl.zst"
        tmp_file = part_file.with_name(part_file.name + ".tmp")
        compressor = zstd.ZstdCompressor(level=self._compression_level)
        fp = tmp_file.open("wb")
        writer = compressor.stream_writer(fp)
        return _OpenPart(
            channel=channel,
            date_utc=date_utc,
            hour_utc=hour_utc,
            seq=seq,
            part_file=part_file,
            tmp_file=tmp_file,
            fp=fp,
            writer=writer,
            opened_at_monotonic=now_monotonic,
        )

    def _close_part(self, part: _OpenPart, *, status: str, reasons: list[str]) -> None:
        try:
            part.writer.flush(zstd.FLUSH_FRAME)
        finally:
            part.writer.close()
            try:
                part.fp.flush()
                os.fsync(part.fp.fileno())
            except Exception:
                pass
            part.fp.close()

        if part.rows <= 0:
            if part.tmp_file.exists():
                part.tmp_file.unlink()
            return

        part.tmp_file.replace(part.part_file)
        bytes_written = int(part.part_file.stat().st_size) if part.part_file.exists() else 0
        self._closed_parts.append(
            {
                "run_id": self._run_id,
                "channel": part.channel,
                "date": part.date_utc,
                "hour": part.hour_utc,
                "rows": int(part.rows),
                "min_ts_ms": part.min_ts_ms,
                "max_ts_ms": part.max_ts_ms,
                "bytes": bytes_written,
                "status": str(status).strip().upper() or "OK",
                "reasons_json": json.dumps(reasons, ensure_ascii=False),
                "part_file": str(part.part_file),
                "collected_at_ms": int(time.time() * 1000),
            }
        )

    def _should_rotate(
        self,
        *,
        part: _OpenPart,
        now_monotonic: float,
        next_bytes: int,
        date_utc: str,
        hour_utc: str,
    ) -> bool:
        if date_utc != part.date_utc or hour_utc != part.hour_utc:
            return True
        if now_monotonic - part.opened_at_monotonic >= float(self._rotate_sec):
            return True
        if part.bytes_uncompressed + int(next_bytes) > self._max_bytes:
            return True
        return False


def discover_ws_part_files(
    *,
    raw_root: Path,
    date_filter: str | None = None,
) -> list[Path]:
    files: list[Path] = []
    if date_filter:
        patterns = [
            f"trade/date={date_filter}/hour=*/*.jsonl.zst",
            f"orderbook/date={date_filter}/hour=*/*.jsonl.zst",
        ]
    else:
        patterns = [
            "trade/date=*/hour=*/*.jsonl.zst",
            "orderbook/date=*/hour=*/*.jsonl.zst",
        ]
    for pattern in patterns:
        files.extend(path for path in raw_root.glob(pattern) if path.is_file())
    return sorted(files)


def read_ws_part_file(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"ws part file not found: {path}")

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


def _date_hour_from_ts_ms(ts_ms: int) -> tuple[str, str]:
    dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H")
