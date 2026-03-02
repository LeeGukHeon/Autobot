"""Append-only run artifact writer for paper sessions."""

from __future__ import annotations

import csv
from dataclasses import asdict, is_dataclass
import json
from pathlib import Path
from typing import Any


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
    def __init__(self, run_dir: Path) -> None:
        self.run_dir = Path(run_dir)
        self.run_dir.mkdir(parents=True, exist_ok=True)

        self._events_path = self.run_dir / "events.jsonl"
        self._orders_path = self.run_dir / "orders.jsonl"
        self._fills_path = self.run_dir / "fills.jsonl"
        self._equity_path = self.run_dir / "equity.csv"

        self._events_fp = self._events_path.open("a", encoding="utf-8")
        self._orders_fp = self._orders_path.open("a", encoding="utf-8")
        self._fills_fp = self._fills_path.open("a", encoding="utf-8")
        self._equity_fp = self._equity_path.open("a", encoding="utf-8", newline="")

        self._equity_writer = csv.DictWriter(
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
        if self._equity_fp.tell() == 0:
            self._equity_writer.writeheader()
            self._equity_fp.flush()

    def close(self) -> None:
        self._events_fp.close()
        self._orders_fp.close()
        self._fills_fp.close()
        self._equity_fp.close()

    def __enter__(self) -> JsonlEventStore:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def append_event(self, *, event_type: str, ts_ms: int, payload: dict[str, Any] | None = None) -> None:
        record = {
            "ts_ms": int(ts_ms),
            "event_type": str(event_type),
            "payload": _to_jsonable(payload or {}),
        }
        self._write_jsonl(self._events_fp, record)

    def append_order(self, order: Any) -> None:
        self._write_jsonl(self._orders_fp, _to_jsonable(order))

    def append_fill(self, fill: Any) -> None:
        self._write_jsonl(self._fills_fp, _to_jsonable(fill))

    def append_equity(self, snapshot: Any) -> None:
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
        self._equity_fp.flush()

    @staticmethod
    def _write_jsonl(handle: Any, record: dict[str, Any]) -> None:
        handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        handle.flush()