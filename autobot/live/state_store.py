"""SQLite-backed live state store for reconciliation and restart recovery."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import sqlite3
import time
from typing import Any


OPEN_ORDER_STATES = frozenset({"wait", "watch", "open", "partial"})


@dataclass(frozen=True)
class PositionRecord:
    market: str
    base_currency: str
    base_amount: float
    avg_entry_price: float
    updated_ts: int
    tp_json: str = "{}"
    sl_json: str = "{}"
    trailing_json: str = "{}"
    managed: bool = True


@dataclass(frozen=True)
class OrderRecord:
    uuid: str
    identifier: str | None
    market: str
    side: str | None
    ord_type: str | None
    price: float | None
    volume_req: float | None
    volume_filled: float
    state: str
    created_ts: int
    updated_ts: int
    intent_id: str | None = None
    tp_sl_link: str | None = None


@dataclass(frozen=True)
class IntentRecord:
    intent_id: str
    ts_ms: int
    market: str
    side: str
    price: float | None
    volume: float | None
    reason_code: str | None
    meta_json: str = "{}"
    status: str = "NEW"


@dataclass(frozen=True)
class RiskPlanRecord:
    plan_id: str
    market: str
    side: str
    entry_price_str: str
    qty_str: str
    tp_enabled: bool = False
    tp_price_str: str | None = None
    tp_pct: float | None = None
    sl_enabled: bool = False
    sl_price_str: str | None = None
    sl_pct: float | None = None
    trailing_enabled: bool = False
    trail_pct: float | None = None
    high_watermark_price_str: str | None = None
    armed_ts_ms: int | None = None
    state: str = "ACTIVE"
    last_eval_ts_ms: int = 0
    last_action_ts_ms: int = 0
    current_exit_order_uuid: str | None = None
    current_exit_order_identifier: str | None = None
    replace_attempt: int = 0
    created_ts: int = 0
    updated_ts: int = 0


class LiveStateStore:
    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._db_path))
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    @property
    def db_path(self) -> Path:
        return self._db_path

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> LiveStateStore:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def bootstrap_bot_meta(self, *, bot_id: str, version: str, ts_ms: int | None = None) -> None:
        now_ts = int(ts_ms if ts_ms is not None else time.time() * 1000)
        created_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_ts / 1000.0))
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO bot_meta (bot_id, created_at, version, last_start_ts)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(bot_id) DO UPDATE SET
                    version=excluded.version,
                    last_start_ts=excluded.last_start_ts
                """,
                (bot_id, created_at, version, now_ts),
            )

    def acquire_run_lock(self, *, bot_id: str, ts_ms: int | None = None) -> bool:
        now_ts = int(ts_ms if ts_ms is not None else time.time() * 1000)
        try:
            with self._conn:
                self._conn.execute(
                    "INSERT INTO run_locks (bot_id, acquired_ts, owner_pid) VALUES (?, ?, ?)",
                    (bot_id, now_ts, os.getpid()),
                )
        except sqlite3.IntegrityError:
            return False
        return True

    def release_run_lock(self, *, bot_id: str) -> None:
        with self._conn:
            self._conn.execute("DELETE FROM run_locks WHERE bot_id = ?", (bot_id,))

    def upsert_position(self, record: PositionRecord) -> None:
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO positions (
                    market, base_currency, base_amount, avg_entry_price, updated_ts,
                    tp_json, sl_json, trailing_json, managed
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(market) DO UPDATE SET
                    base_currency=excluded.base_currency,
                    base_amount=excluded.base_amount,
                    avg_entry_price=excluded.avg_entry_price,
                    updated_ts=excluded.updated_ts,
                    tp_json=excluded.tp_json,
                    sl_json=excluded.sl_json,
                    trailing_json=excluded.trailing_json,
                    managed=excluded.managed
                """,
                (
                    record.market,
                    record.base_currency,
                    float(record.base_amount),
                    float(record.avg_entry_price),
                    int(record.updated_ts),
                    record.tp_json,
                    record.sl_json,
                    record.trailing_json,
                    1 if record.managed else 0,
                ),
            )

    def delete_position(self, *, market: str) -> None:
        with self._conn:
            self._conn.execute("DELETE FROM positions WHERE market = ?", (market,))

    def upsert_order(self, record: OrderRecord) -> None:
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO orders (
                    uuid, identifier, market, side, ord_type, price,
                    volume_req, volume_filled, state, created_ts, updated_ts, intent_id, tp_sl_link
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(uuid) DO UPDATE SET
                    identifier=excluded.identifier,
                    market=excluded.market,
                    side=excluded.side,
                    ord_type=excluded.ord_type,
                    price=excluded.price,
                    volume_req=excluded.volume_req,
                    volume_filled=excluded.volume_filled,
                    state=excluded.state,
                    created_ts=excluded.created_ts,
                    updated_ts=excluded.updated_ts,
                    intent_id=excluded.intent_id,
                    tp_sl_link=excluded.tp_sl_link
                """,
                (
                    record.uuid,
                    record.identifier,
                    record.market,
                    record.side,
                    record.ord_type,
                    record.price,
                    record.volume_req,
                    float(record.volume_filled),
                    record.state,
                    int(record.created_ts),
                    int(record.updated_ts),
                    record.intent_id,
                    record.tp_sl_link,
                ),
            )

    def mark_order_state(self, *, uuid: str, state: str, updated_ts: int | None = None) -> None:
        now_ts = int(updated_ts if updated_ts is not None else time.time() * 1000)
        with self._conn:
            self._conn.execute(
                "UPDATE orders SET state = ?, updated_ts = ? WHERE uuid = ?",
                (state, now_ts, uuid),
            )

    def upsert_intent(self, record: IntentRecord) -> None:
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO intents (
                    intent_id, ts_ms, market, side, price, volume, reason_code, meta_json, status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(intent_id) DO UPDATE SET
                    ts_ms=excluded.ts_ms,
                    market=excluded.market,
                    side=excluded.side,
                    price=excluded.price,
                    volume=excluded.volume,
                    reason_code=excluded.reason_code,
                    meta_json=excluded.meta_json,
                    status=excluded.status
                """,
                (
                    record.intent_id,
                    int(record.ts_ms),
                    record.market,
                    record.side,
                    record.price,
                    record.volume,
                    record.reason_code,
                    record.meta_json,
                    record.status,
                ),
            )

    def order_by_uuid(self, *, uuid: str) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT * FROM orders WHERE uuid = ?", (uuid,)).fetchone()
        if row is None:
            return None
        return _row_to_order(row)

    def position_by_market(self, *, market: str) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT * FROM positions WHERE market = ?", (market,)).fetchone()
        if row is None:
            return None
        return _row_to_position(row)

    def list_positions(self) -> list[dict[str, Any]]:
        rows = self._conn.execute("SELECT * FROM positions ORDER BY market").fetchall()
        return [_row_to_position(row) for row in rows]

    def list_orders(self, *, open_only: bool = False) -> list[dict[str, Any]]:
        if open_only:
            placeholders = ",".join("?" for _ in OPEN_ORDER_STATES)
            query = f"SELECT * FROM orders WHERE lower(state) IN ({placeholders}) ORDER BY updated_ts DESC"
            rows = self._conn.execute(query, tuple(sorted(OPEN_ORDER_STATES))).fetchall()
        else:
            rows = self._conn.execute("SELECT * FROM orders ORDER BY updated_ts DESC").fetchall()
        return [_row_to_order(row) for row in rows]

    def list_intents(self) -> list[dict[str, Any]]:
        rows = self._conn.execute("SELECT * FROM intents ORDER BY ts_ms DESC, intent_id").fetchall()
        return [_row_to_intent(row) for row in rows]

    def upsert_risk_plan(self, record: RiskPlanRecord) -> None:
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO risk_plans (
                    plan_id, market, side, entry_price_str, qty_str,
                    tp_enabled, tp_price_str, tp_pct,
                    sl_enabled, sl_price_str, sl_pct,
                    trailing_enabled, trail_pct, high_watermark_price_str, armed_ts_ms,
                    state, last_eval_ts_ms, last_action_ts_ms,
                    current_exit_order_uuid, current_exit_order_identifier, replace_attempt,
                    created_ts, updated_ts
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(plan_id) DO UPDATE SET
                    market=excluded.market,
                    side=excluded.side,
                    entry_price_str=excluded.entry_price_str,
                    qty_str=excluded.qty_str,
                    tp_enabled=excluded.tp_enabled,
                    tp_price_str=excluded.tp_price_str,
                    tp_pct=excluded.tp_pct,
                    sl_enabled=excluded.sl_enabled,
                    sl_price_str=excluded.sl_price_str,
                    sl_pct=excluded.sl_pct,
                    trailing_enabled=excluded.trailing_enabled,
                    trail_pct=excluded.trail_pct,
                    high_watermark_price_str=excluded.high_watermark_price_str,
                    armed_ts_ms=excluded.armed_ts_ms,
                    state=excluded.state,
                    last_eval_ts_ms=excluded.last_eval_ts_ms,
                    last_action_ts_ms=excluded.last_action_ts_ms,
                    current_exit_order_uuid=excluded.current_exit_order_uuid,
                    current_exit_order_identifier=excluded.current_exit_order_identifier,
                    replace_attempt=excluded.replace_attempt,
                    created_ts=excluded.created_ts,
                    updated_ts=excluded.updated_ts
                """,
                (
                    record.plan_id,
                    record.market,
                    record.side,
                    record.entry_price_str,
                    record.qty_str,
                    1 if record.tp_enabled else 0,
                    record.tp_price_str,
                    record.tp_pct,
                    1 if record.sl_enabled else 0,
                    record.sl_price_str,
                    record.sl_pct,
                    1 if record.trailing_enabled else 0,
                    record.trail_pct,
                    record.high_watermark_price_str,
                    record.armed_ts_ms,
                    record.state,
                    int(record.last_eval_ts_ms),
                    int(record.last_action_ts_ms),
                    record.current_exit_order_uuid,
                    record.current_exit_order_identifier,
                    int(record.replace_attempt),
                    int(record.created_ts),
                    int(record.updated_ts),
                ),
            )

    def risk_plan_by_id(self, *, plan_id: str) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT * FROM risk_plans WHERE plan_id = ?", (plan_id,)).fetchone()
        if row is None:
            return None
        return _row_to_risk_plan(row)

    def list_risk_plans(
        self,
        *,
        states: tuple[str, ...] | None = None,
        market: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if states:
            placeholders = ",".join("?" for _ in states)
            clauses.append(f"state IN ({placeholders})")
            params.extend(states)
        if market:
            clauses.append("market = ?")
            params.append(market)

        query = "SELECT * FROM risk_plans"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY market, plan_id"
        rows = self._conn.execute(query, tuple(params)).fetchall()
        return [_row_to_risk_plan(row) for row in rows]

    def set_checkpoint(self, *, name: str, payload: dict[str, Any], ts_ms: int | None = None) -> None:
        now_ts = int(ts_ms if ts_ms is not None else time.time() * 1000)
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO checkpoints (name, ts_ms, payload_json)
                VALUES (?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    ts_ms=excluded.ts_ms,
                    payload_json=excluded.payload_json
                """,
                (name, now_ts, json.dumps(payload, ensure_ascii=False, sort_keys=True)),
            )

    def export_state(self) -> dict[str, Any]:
        return {
            "db_path": str(self._db_path),
            "bot_meta": [_row_to_plain_dict(row) for row in self._conn.execute("SELECT * FROM bot_meta").fetchall()],
            "positions": self.list_positions(),
            "orders": self.list_orders(open_only=False),
            "intents": self.list_intents(),
            "risk_plans": self.list_risk_plans(),
            "checkpoints": [
                _row_to_checkpoint(row) for row in self._conn.execute("SELECT * FROM checkpoints ORDER BY name").fetchall()
            ],
            "run_locks": [_row_to_plain_dict(row) for row in self._conn.execute("SELECT * FROM run_locks").fetchall()],
        }

    def _init_schema(self) -> None:
        with self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS bot_meta (
                    bot_id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    version TEXT NOT NULL,
                    last_start_ts INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS positions (
                    market TEXT PRIMARY KEY,
                    base_currency TEXT NOT NULL,
                    base_amount REAL NOT NULL,
                    avg_entry_price REAL NOT NULL,
                    updated_ts INTEGER NOT NULL,
                    tp_json TEXT NOT NULL DEFAULT '{}',
                    sl_json TEXT NOT NULL DEFAULT '{}',
                    trailing_json TEXT NOT NULL DEFAULT '{}',
                    managed INTEGER NOT NULL DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS orders (
                    uuid TEXT PRIMARY KEY,
                    identifier TEXT UNIQUE,
                    market TEXT NOT NULL,
                    side TEXT,
                    ord_type TEXT,
                    price REAL,
                    volume_req REAL,
                    volume_filled REAL NOT NULL DEFAULT 0,
                    state TEXT NOT NULL,
                    created_ts INTEGER NOT NULL,
                    updated_ts INTEGER NOT NULL,
                    intent_id TEXT,
                    tp_sl_link TEXT
                );

                CREATE TABLE IF NOT EXISTS intents (
                    intent_id TEXT PRIMARY KEY,
                    ts_ms INTEGER NOT NULL,
                    market TEXT NOT NULL,
                    side TEXT NOT NULL,
                    price REAL,
                    volume REAL,
                    reason_code TEXT,
                    meta_json TEXT NOT NULL DEFAULT '{}',
                    status TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS risk_plans (
                    plan_id TEXT PRIMARY KEY,
                    market TEXT NOT NULL,
                    side TEXT NOT NULL,
                    entry_price_str TEXT NOT NULL,
                    qty_str TEXT NOT NULL,
                    tp_enabled INTEGER NOT NULL DEFAULT 0,
                    tp_price_str TEXT,
                    tp_pct REAL,
                    sl_enabled INTEGER NOT NULL DEFAULT 0,
                    sl_price_str TEXT,
                    sl_pct REAL,
                    trailing_enabled INTEGER NOT NULL DEFAULT 0,
                    trail_pct REAL,
                    high_watermark_price_str TEXT,
                    armed_ts_ms INTEGER,
                    state TEXT NOT NULL DEFAULT 'ACTIVE',
                    last_eval_ts_ms INTEGER NOT NULL DEFAULT 0,
                    last_action_ts_ms INTEGER NOT NULL DEFAULT 0,
                    current_exit_order_uuid TEXT,
                    current_exit_order_identifier TEXT,
                    replace_attempt INTEGER NOT NULL DEFAULT 0,
                    created_ts INTEGER NOT NULL,
                    updated_ts INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_risk_plans_market_state ON risk_plans (market, state);

                CREATE TABLE IF NOT EXISTS checkpoints (
                    name TEXT PRIMARY KEY,
                    ts_ms INTEGER NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS run_locks (
                    bot_id TEXT PRIMARY KEY,
                    acquired_ts INTEGER NOT NULL,
                    owner_pid INTEGER NOT NULL
                );
                """
            )


def _row_to_plain_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _row_to_position(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "market": row["market"],
        "base_currency": row["base_currency"],
        "base_amount": float(row["base_amount"]),
        "avg_entry_price": float(row["avg_entry_price"]),
        "updated_ts": int(row["updated_ts"]),
        "tp": _parse_json(row["tp_json"]),
        "sl": _parse_json(row["sl_json"]),
        "trailing": _parse_json(row["trailing_json"]),
        "managed": bool(row["managed"]),
    }


def _row_to_order(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "uuid": row["uuid"],
        "identifier": row["identifier"],
        "market": row["market"],
        "side": row["side"],
        "ord_type": row["ord_type"],
        "price": float(row["price"]) if row["price"] is not None else None,
        "volume_req": float(row["volume_req"]) if row["volume_req"] is not None else None,
        "volume_filled": float(row["volume_filled"]),
        "state": row["state"],
        "created_ts": int(row["created_ts"]),
        "updated_ts": int(row["updated_ts"]),
        "intent_id": row["intent_id"],
        "tp_sl_link": row["tp_sl_link"],
    }


def _row_to_intent(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "intent_id": row["intent_id"],
        "ts_ms": int(row["ts_ms"]),
        "market": row["market"],
        "side": row["side"],
        "price": float(row["price"]) if row["price"] is not None else None,
        "volume": float(row["volume"]) if row["volume"] is not None else None,
        "reason_code": row["reason_code"],
        "meta": _parse_json(row["meta_json"]),
        "status": row["status"],
    }


def _row_to_risk_plan(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "plan_id": row["plan_id"],
        "market": row["market"],
        "side": row["side"],
        "entry_price_str": row["entry_price_str"],
        "qty_str": row["qty_str"],
        "tp": {
            "enabled": bool(row["tp_enabled"]),
            "tp_price_str": row["tp_price_str"],
            "tp_pct": float(row["tp_pct"]) if row["tp_pct"] is not None else None,
        },
        "sl": {
            "enabled": bool(row["sl_enabled"]),
            "sl_price_str": row["sl_price_str"],
            "sl_pct": float(row["sl_pct"]) if row["sl_pct"] is not None else None,
        },
        "trailing": {
            "enabled": bool(row["trailing_enabled"]),
            "trail_pct": float(row["trail_pct"]) if row["trail_pct"] is not None else None,
            "high_watermark_price_str": row["high_watermark_price_str"],
            "armed_ts_ms": int(row["armed_ts_ms"]) if row["armed_ts_ms"] is not None else None,
        },
        "state": row["state"],
        "last_eval_ts_ms": int(row["last_eval_ts_ms"]),
        "last_action_ts_ms": int(row["last_action_ts_ms"]),
        "current_exit_order_uuid": row["current_exit_order_uuid"],
        "current_exit_order_identifier": row["current_exit_order_identifier"],
        "replace_attempt": int(row["replace_attempt"]),
        "created_ts": int(row["created_ts"]),
        "updated_ts": int(row["updated_ts"]),
    }


def _row_to_checkpoint(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "name": row["name"],
        "ts_ms": int(row["ts_ms"]),
        "payload": _parse_json(row["payload_json"]),
    }


def _parse_json(raw: object) -> dict[str, Any]:
    if raw is None:
        return {}
    try:
        value = json.loads(str(raw))
    except json.JSONDecodeError:
        return {}
    if isinstance(value, dict):
        return value
    return {}
