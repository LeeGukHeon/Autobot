"""Append-only run artifact writer for paper sessions."""

from __future__ import annotations

import csv
from dataclasses import asdict, is_dataclass
import json
from pathlib import Path
from typing import Any

_JSONL_FLUSH_EVERY = 256
_CSV_FLUSH_EVERY = 64


def _to_jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return _to_jsonable(asdict(value))
    if isinstance(value, dict):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_to_jsonable(item) for item in value]
    return value


class JsonlEventStore:
    def __init__(
        self,
        run_dir: Path,
        *,
        write_events: bool = True,
        write_orders: bool = True,
        write_fills: bool = True,
        write_equity: bool = True,
    ) -> None:
        self.run_dir = Path(run_dir)
        self.run_dir.mkdir(parents=True, exist_ok=True)

        self._events_path = self.run_dir / "events.jsonl"
        self._orders_path = self.run_dir / "orders.jsonl"
        self._fills_path = self.run_dir / "fills.jsonl"
        self._equity_path = self.run_dir / "equity.csv"
        self._events_fp = self._events_path.open("a", encoding="utf-8") if write_events else None
        self._orders_fp = self._orders_path.open("a", encoding="utf-8") if write_orders else None
        self._fills_fp = self._fills_path.open("a", encoding="utf-8") if write_fills else None
        self._equity_fp = self._equity_path.open("a", encoding="utf-8", newline="") if write_equity else None

        self._equity_writer = (
            csv.DictWriter(
                self._equity_fp,
                fieldnames=[
                    "ts_ms",
                    "equity_quote",
                    "cash_free",
                    "cash_locked",
                    "realized_pnl_quote",
                    "unrealized_pnl_quote",
                    "open_positions",
                ],
            )
            if self._equity_fp is not None
            else None
        )
        if self._equity_fp is not None and self._equity_writer is not None and self._equity_fp.tell() == 0:
            self._equity_writer.writeheader()
            self._equity_fp.flush()
        self._pending_jsonl_writes = {
            "events": 0,
            "orders": 0,
            "fills": 0,
        }
        self._pending_equity_writes = 0

    def close(self) -> None:
        self.flush()
        if self._events_fp is not None:
            self._events_fp.close()
        if self._orders_fp is not None:
            self._orders_fp.close()
        if self._fills_fp is not None:
            self._fills_fp.close()
        if self._equity_fp is not None:
            self._equity_fp.close()

    def __enter__(self) -> JsonlEventStore:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def append_event(self, *, event_type: str, ts_ms: int, payload: dict[str, Any] | None = None) -> None:
        if self._events_fp is None:
            return
        record = {
            "ts_ms": int(ts_ms),
            "event_type": str(event_type),
            "payload": _to_jsonable(payload or {}),
        }
        self._write_jsonl(self._events_fp, record)
        self._bump_jsonl_counter("events")

    def append_order(self, order: Any) -> None:
        if self._orders_fp is None:
            return
        self._write_jsonl(self._orders_fp, _to_jsonable(order))
        self._bump_jsonl_counter("orders")

    def append_fill(self, fill: Any) -> None:
        if self._fills_fp is None:
            return
        self._write_jsonl(self._fills_fp, _to_jsonable(fill))
        self._bump_jsonl_counter("fills")

    def append_equity(self, snapshot: Any) -> None:
        if self._equity_writer is None or self._equity_fp is None:
            return
        value = _to_jsonable(snapshot)
        if not isinstance(value, dict):
            return

        positions = value.get("positions")
        open_positions = len(positions) if isinstance(positions, list) else 0
        row = {
            "ts_ms": int(value.get("ts_ms", 0)),
            "equity_quote": float(value.get("equity_quote", 0.0)),
            "cash_free": float(value.get("cash_free", 0.0)),
            "cash_locked": float(value.get("cash_locked", 0.0)),
            "realized_pnl_quote": float(value.get("realized_pnl_quote", 0.0)),
            "unrealized_pnl_quote": float(value.get("unrealized_pnl_quote", 0.0)),
            "open_positions": int(open_positions),
        }
        self._equity_writer.writerow(row)
        self._pending_equity_writes += 1
        if self._pending_equity_writes >= _CSV_FLUSH_EVERY:
            self._equity_fp.flush()
            self._pending_equity_writes = 0

    def flush(self) -> None:
        if self._events_fp is not None:
            self._events_fp.flush()
            self._pending_jsonl_writes["events"] = 0
        if self._orders_fp is not None:
            self._orders_fp.flush()
            self._pending_jsonl_writes["orders"] = 0
        if self._fills_fp is not None:
            self._fills_fp.flush()
            self._pending_jsonl_writes["fills"] = 0
        if self._equity_fp is not None:
            self._equity_fp.flush()
            self._pending_equity_writes = 0

    def _bump_jsonl_counter(self, key: str) -> None:
        self._pending_jsonl_writes[key] = int(self._pending_jsonl_writes.get(key, 0)) + 1
        if self._pending_jsonl_writes[key] >= _JSONL_FLUSH_EVERY:
            handle = {
                "events": self._events_fp,
                "orders": self._orders_fp,
                "fills": self._fills_fp,
            }.get(key)
            if handle is not None:
                handle.flush()
            self._pending_jsonl_writes[key] = 0

    @staticmethod
    def _write_jsonl(handle: Any, record: dict[str, Any]) -> None:
        handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
