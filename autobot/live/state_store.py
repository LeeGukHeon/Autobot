"""SQLite-backed live state store for reconciliation and restart recovery."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import sqlite3
import time
from typing import Any

from .order_state import is_open_local_state, normalize_order_state, resolve_transition


def _pid_is_alive(pid: int) -> bool:
    pid_value = int(pid)
    if pid_value <= 0:
        return False
    try:
        os.kill(pid_value, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


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
    local_state: str | None = None
    raw_exchange_state: str | None = None
    last_event_name: str | None = None
    event_source: str | None = None
    replace_seq: int = 0
    root_order_uuid: str | None = None
    prev_order_uuid: str | None = None
    prev_order_identifier: str | None = None
    executed_funds: float | None = None
    paid_fee: float | None = None
    reserved_fee: float | None = None
    remaining_fee: float | None = None
    exchange_payload_json: str = "{}"


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
    timeout_ts_ms: int | None = None
    state: str = "ACTIVE"
    last_eval_ts_ms: int = 0
    last_action_ts_ms: int = 0
    current_exit_order_uuid: str | None = None
    current_exit_order_identifier: str | None = None
    replace_attempt: int = 0
    created_ts: int = 0
    updated_ts: int = 0
    plan_source: str | None = None
    source_intent_id: str | None = None


@dataclass(frozen=True)
class TradeJournalRecord:
    journal_id: str
    market: str
    status: str
    entry_intent_id: str | None = None
    entry_order_uuid: str | None = None
    exit_order_uuid: str | None = None
    plan_id: str | None = None
    entry_submitted_ts_ms: int | None = None
    entry_filled_ts_ms: int | None = None
    exit_ts_ms: int | None = None
    entry_price: float | None = None
    exit_price: float | None = None
    qty: float | None = None
    entry_notional_quote: float | None = None
    exit_notional_quote: float | None = None
    realized_pnl_quote: float | None = None
    realized_pnl_pct: float | None = None
    entry_reason_code: str | None = None
    close_reason_code: str | None = None
    close_mode: str | None = None
    model_prob: float | None = None
    selection_policy_mode: str | None = None
    trade_action: str | None = None
    expected_edge_bps: float | None = None
    expected_downside_bps: float | None = None
    expected_net_edge_bps: float | None = None
    notional_multiplier: float | None = None
    entry_meta_json: str = "{}"
    exit_meta_json: str = "{}"
    updated_ts: int = 0


@dataclass(frozen=True)
class OrderLineageRecord:
    ts_ms: int
    event_source: str
    intent_id: str | None
    prev_uuid: str | None
    prev_identifier: str | None
    new_uuid: str | None
    new_identifier: str | None
    replace_seq: int = 0


@dataclass(frozen=True)
class BreakerStateRecord:
    breaker_key: str
    active: bool
    action: str | None
    source: str
    reason_codes_json: str = "[]"
    details_json: str = "{}"
    updated_ts: int = 0
    armed_ts: int = 0


@dataclass(frozen=True)
class BreakerEventRecord:
    ts_ms: int
    breaker_key: str
    event_kind: str
    action: str | None
    source: str
    reason_codes_json: str = "[]"
    details_json: str = "{}"


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
            existing = self.run_lock(bot_id=bot_id)
            if existing is None:
                return False
            owner_pid = int(existing.get("owner_pid") or 0)
            if _pid_is_alive(owner_pid):
                return False
            with self._conn:
                self._conn.execute(
                    "DELETE FROM run_locks WHERE bot_id = ? AND owner_pid = ?",
                    (bot_id, owner_pid),
                )
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

    def run_lock(self, *, bot_id: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT bot_id, acquired_ts, owner_pid FROM run_locks WHERE bot_id = ?",
            (bot_id,),
        ).fetchone()
        return _row_to_plain_dict(row) if row is not None else None

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
        previous = self.order_by_uuid(uuid=record.uuid)
        identifier = str(record.identifier or "").strip()
        rebind_from_uuid: str | None = None
        if identifier:
            existing_identifier = self.order_by_identifier(identifier=identifier)
            if existing_identifier is not None and str(existing_identifier.get("uuid") or "") != record.uuid:
                if _can_rebind_pending_identifier(
                    existing_identifier=existing_identifier,
                    incoming_uuid=record.uuid,
                    market=record.market,
                    side=record.side,
                ):
                    rebind_from_uuid = str(existing_identifier.get("uuid") or "").strip() or None
                    if previous is None:
                        previous = existing_identifier
                else:
                    raise ValueError(
                        f"IDENTIFIER_COLLISION: identifier={identifier} existing_uuid={existing_identifier.get('uuid')} "
                        f"incoming_uuid={record.uuid}"
                    )
        previous_local_state = str(previous.get("local_state") or "").strip() if previous is not None else None
        normalized = normalize_order_state(
            exchange_state=record.raw_exchange_state if record.raw_exchange_state is not None else record.state,
            event_name=record.last_event_name,
            executed_volume=record.volume_filled,
        )
        requested_local_state = record.local_state or normalized.local_state
        resolved_local_state, transition_ok = resolve_transition(previous_local_state, requested_local_state)
        created_ts = _min_positive_int(
            int(record.created_ts),
            int(previous.get("created_ts") or 0) if previous is not None else 0,
        ) or int(record.created_ts)
        root_order_uuid = record.root_order_uuid or (previous.get("root_order_uuid") if previous is not None else None) or record.uuid
        if _is_pending_order_uuid(root_order_uuid) and not _is_pending_order_uuid(record.uuid):
            root_order_uuid = record.uuid
        intent_id = record.intent_id or (str(previous.get("intent_id") or "").strip() if previous is not None else None) or None
        tp_sl_link = record.tp_sl_link or (str(previous.get("tp_sl_link") or "").strip() if previous is not None else None) or None
        replace_seq = max(
            int(record.replace_seq),
            int(previous.get("replace_seq") or 0) if previous is not None else 0,
        )
        prev_order_uuid = record.prev_order_uuid or (str(previous.get("prev_order_uuid") or "").strip() if previous is not None else None) or None
        prev_order_identifier = (
            record.prev_order_identifier
            or (str(previous.get("prev_order_identifier") or "").strip() if previous is not None else None)
            or None
        )
        executed_funds = (
            record.executed_funds if record.executed_funds is not None else (previous.get("executed_funds") if previous is not None else None)
        )
        paid_fee = record.paid_fee if record.paid_fee is not None else (previous.get("paid_fee") if previous is not None else None)
        reserved_fee = (
            record.reserved_fee if record.reserved_fee is not None else (previous.get("reserved_fee") if previous is not None else None)
        )
        remaining_fee = (
            record.remaining_fee if record.remaining_fee is not None else (previous.get("remaining_fee") if previous is not None else None)
        )
        with self._conn:
            if rebind_from_uuid is not None:
                self._conn.execute("DELETE FROM orders WHERE uuid = ?", (rebind_from_uuid,))
                self._rebind_order_uuid_references(
                    previous_uuid=rebind_from_uuid,
                    new_uuid=str(record.uuid),
                )
            self._conn.execute(
                """
                INSERT INTO orders (
                    uuid, identifier, market, side, ord_type, price,
                    volume_req, volume_filled, state, created_ts, updated_ts, intent_id, tp_sl_link,
                    local_state, raw_exchange_state, last_event_name, event_source,
                    replace_seq, root_order_uuid, prev_order_uuid, prev_order_identifier,
                    executed_funds, paid_fee, reserved_fee, remaining_fee, exchange_payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    intent_id=COALESCE(excluded.intent_id, orders.intent_id),
                    tp_sl_link=COALESCE(excluded.tp_sl_link, orders.tp_sl_link),
                    local_state=excluded.local_state,
                    raw_exchange_state=excluded.raw_exchange_state,
                    last_event_name=excluded.last_event_name,
                    event_source=excluded.event_source,
                    replace_seq=excluded.replace_seq,
                    root_order_uuid=excluded.root_order_uuid,
                    prev_order_uuid=excluded.prev_order_uuid,
                    prev_order_identifier=excluded.prev_order_identifier,
                    executed_funds=COALESCE(excluded.executed_funds, orders.executed_funds),
                    paid_fee=COALESCE(excluded.paid_fee, orders.paid_fee),
                    reserved_fee=COALESCE(excluded.reserved_fee, orders.reserved_fee),
                    remaining_fee=COALESCE(excluded.remaining_fee, orders.remaining_fee),
                    exchange_payload_json=CASE
                        WHEN excluded.exchange_payload_json IS NOT NULL AND excluded.exchange_payload_json != '{}' THEN excluded.exchange_payload_json
                        ELSE orders.exchange_payload_json
                    END
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
                    created_ts,
                    int(record.updated_ts),
                    intent_id,
                    tp_sl_link,
                    resolved_local_state,
                    record.raw_exchange_state if record.raw_exchange_state is not None else normalized.exchange_state or record.state,
                    record.last_event_name or normalized.event_name,
                    record.event_source,
                    replace_seq,
                    root_order_uuid,
                    prev_order_uuid,
                    prev_order_identifier,
                    executed_funds,
                    paid_fee,
                    reserved_fee,
                    remaining_fee,
                    record.exchange_payload_json,
                ),
            )
            if not transition_ok:
                self._conn.execute(
                    """
                    INSERT INTO checkpoints (name, ts_ms, payload_json)
                    VALUES (?, ?, ?)
                    ON CONFLICT(name) DO UPDATE SET
                        ts_ms=excluded.ts_ms,
                        payload_json=excluded.payload_json
                    """,
                    (
                        f"illegal_order_transition:{record.uuid}",
                        int(record.updated_ts),
                        json.dumps(
                            {
                                "uuid": record.uuid,
                                "previous_local_state": previous_local_state,
                                "requested_local_state": requested_local_state,
                                "resolved_local_state": resolved_local_state,
                                "event_source": record.event_source,
                                "last_event_name": record.last_event_name or normalized.event_name,
                            },
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                    ),
                )

    def _rebind_order_uuid_references(self, *, previous_uuid: str, new_uuid: str) -> None:
        if not previous_uuid or not new_uuid or previous_uuid == new_uuid:
            return
        self._conn.execute(
            "UPDATE trade_journal SET entry_order_uuid = ? WHERE entry_order_uuid = ?",
            (new_uuid, previous_uuid),
        )
        self._conn.execute(
            "UPDATE trade_journal SET exit_order_uuid = ? WHERE exit_order_uuid = ?",
            (new_uuid, previous_uuid),
        )
        self._conn.execute(
            "UPDATE risk_plans SET current_exit_order_uuid = ? WHERE current_exit_order_uuid = ?",
            (new_uuid, previous_uuid),
        )
        self._conn.execute(
            "UPDATE orders SET root_order_uuid = ? WHERE root_order_uuid = ?",
            (new_uuid, previous_uuid),
        )
        self._conn.execute(
            "UPDATE orders SET prev_order_uuid = ? WHERE prev_order_uuid = ?",
            (new_uuid, previous_uuid),
        )
        self._conn.execute(
            "UPDATE order_lineage SET prev_uuid = ? WHERE prev_uuid = ?",
            (new_uuid, previous_uuid),
        )
        self._conn.execute(
            "UPDATE order_lineage SET new_uuid = ? WHERE new_uuid = ?",
            (new_uuid, previous_uuid),
        )

    def mark_order_state(self, *, uuid: str, state: str, updated_ts: int | None = None) -> None:
        now_ts = int(updated_ts if updated_ts is not None else time.time() * 1000)
        previous = self.order_by_uuid(uuid=uuid)
        previous_local_state = str(previous.get("local_state") or "").strip() if previous is not None else None
        normalized = normalize_order_state(exchange_state=state, event_name="STATE_MARK")
        resolved_local_state, _ = resolve_transition(previous_local_state, normalized.local_state)
        with self._conn:
            self._conn.execute(
                """
                UPDATE orders
                SET state = ?, raw_exchange_state = ?, local_state = ?, last_event_name = ?, event_source = ?, updated_ts = ?
                WHERE uuid = ?
                """,
                (state, state, resolved_local_state, normalized.event_name, "state_store", now_ts, uuid),
            )

    def order_by_identifier(self, *, identifier: str) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT * FROM orders WHERE identifier = ?", (identifier,)).fetchone()
        if row is None:
            return None
        return _row_to_order(row)

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
            rows = self._conn.execute("SELECT * FROM orders ORDER BY updated_ts DESC").fetchall()
            return [item for item in (_row_to_order(row) for row in rows) if is_open_local_state(item.get("local_state"))]
        else:
            rows = self._conn.execute("SELECT * FROM orders ORDER BY updated_ts DESC").fetchall()
        return [_row_to_order(row) for row in rows]

    def list_intents(self) -> list[dict[str, Any]]:
        rows = self._conn.execute("SELECT * FROM intents ORDER BY ts_ms DESC, intent_id").fetchall()
        return [_row_to_intent(row) for row in rows]

    def intent_by_id(self, *, intent_id: str) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT * FROM intents WHERE intent_id = ?", (intent_id,)).fetchone()
        if row is None:
            return None
        return _row_to_intent(row)

    def upsert_risk_plan(self, record: RiskPlanRecord) -> None:
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO risk_plans (
                    plan_id, market, side, entry_price_str, qty_str,
                    tp_enabled, tp_price_str, tp_pct,
                    sl_enabled, sl_price_str, sl_pct,
                    trailing_enabled, trail_pct, high_watermark_price_str, armed_ts_ms, timeout_ts_ms,
                    state, last_eval_ts_ms, last_action_ts_ms,
                    current_exit_order_uuid, current_exit_order_identifier, replace_attempt,
                    created_ts, updated_ts, plan_source, source_intent_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    timeout_ts_ms=excluded.timeout_ts_ms,
                    state=excluded.state,
                    last_eval_ts_ms=excluded.last_eval_ts_ms,
                    last_action_ts_ms=excluded.last_action_ts_ms,
                    current_exit_order_uuid=excluded.current_exit_order_uuid,
                    current_exit_order_identifier=excluded.current_exit_order_identifier,
                    replace_attempt=excluded.replace_attempt,
                    created_ts=excluded.created_ts,
                    updated_ts=excluded.updated_ts,
                    plan_source=excluded.plan_source,
                    source_intent_id=excluded.source_intent_id
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
                    record.timeout_ts_ms,
                    record.state,
                    int(record.last_eval_ts_ms),
                    int(record.last_action_ts_ms),
                    record.current_exit_order_uuid,
                    record.current_exit_order_identifier,
                    int(record.replace_attempt),
                    int(record.created_ts),
                    int(record.updated_ts),
                    record.plan_source,
                    record.source_intent_id,
                ),
            )

    def upsert_trade_journal(self, record: TradeJournalRecord) -> None:
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO trade_journal (
                    journal_id, market, status, entry_intent_id, entry_order_uuid, exit_order_uuid, plan_id,
                    entry_submitted_ts_ms, entry_filled_ts_ms, exit_ts_ms,
                    entry_price, exit_price, qty, entry_notional_quote, exit_notional_quote,
                    realized_pnl_quote, realized_pnl_pct,
                    entry_reason_code, close_reason_code, close_mode,
                    model_prob, selection_policy_mode, trade_action,
                    expected_edge_bps, expected_downside_bps, expected_net_edge_bps, notional_multiplier,
                    entry_meta_json, exit_meta_json, updated_ts
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(journal_id) DO UPDATE SET
                    market=excluded.market,
                    status=excluded.status,
                    entry_intent_id=excluded.entry_intent_id,
                    entry_order_uuid=excluded.entry_order_uuid,
                    exit_order_uuid=excluded.exit_order_uuid,
                    plan_id=excluded.plan_id,
                    entry_submitted_ts_ms=excluded.entry_submitted_ts_ms,
                    entry_filled_ts_ms=excluded.entry_filled_ts_ms,
                    exit_ts_ms=excluded.exit_ts_ms,
                    entry_price=excluded.entry_price,
                    exit_price=excluded.exit_price,
                    qty=excluded.qty,
                    entry_notional_quote=excluded.entry_notional_quote,
                    exit_notional_quote=excluded.exit_notional_quote,
                    realized_pnl_quote=excluded.realized_pnl_quote,
                    realized_pnl_pct=excluded.realized_pnl_pct,
                    entry_reason_code=excluded.entry_reason_code,
                    close_reason_code=excluded.close_reason_code,
                    close_mode=excluded.close_mode,
                    model_prob=excluded.model_prob,
                    selection_policy_mode=excluded.selection_policy_mode,
                    trade_action=excluded.trade_action,
                    expected_edge_bps=excluded.expected_edge_bps,
                    expected_downside_bps=excluded.expected_downside_bps,
                    expected_net_edge_bps=excluded.expected_net_edge_bps,
                    notional_multiplier=excluded.notional_multiplier,
                    entry_meta_json=excluded.entry_meta_json,
                    exit_meta_json=excluded.exit_meta_json,
                    updated_ts=excluded.updated_ts
                """,
                (
                    record.journal_id,
                    record.market,
                    record.status,
                    record.entry_intent_id,
                    record.entry_order_uuid,
                    record.exit_order_uuid,
                    record.plan_id,
                    record.entry_submitted_ts_ms,
                    record.entry_filled_ts_ms,
                    record.exit_ts_ms,
                    record.entry_price,
                    record.exit_price,
                    record.qty,
                    record.entry_notional_quote,
                    record.exit_notional_quote,
                    record.realized_pnl_quote,
                    record.realized_pnl_pct,
                    record.entry_reason_code,
                    record.close_reason_code,
                    record.close_mode,
                    record.model_prob,
                    record.selection_policy_mode,
                    record.trade_action,
                    record.expected_edge_bps,
                    record.expected_downside_bps,
                    record.expected_net_edge_bps,
                    record.notional_multiplier,
                    record.entry_meta_json,
                    record.exit_meta_json,
                    int(record.updated_ts),
                ),
            )

    def trade_journal_by_id(self, *, journal_id: str) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT * FROM trade_journal WHERE journal_id = ?", (journal_id,)).fetchone()
        if row is None:
            return None
        return _row_to_trade_journal(row)

    def trade_journal_by_entry_intent(self, *, entry_intent_id: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT * FROM trade_journal WHERE entry_intent_id = ? ORDER BY updated_ts DESC LIMIT 1",
            (entry_intent_id,),
        ).fetchone()
        if row is None:
            return None
        return _row_to_trade_journal(row)

    def trade_journal_by_plan_id(self, *, plan_id: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT * FROM trade_journal WHERE plan_id = ? ORDER BY updated_ts DESC LIMIT 1",
            (plan_id,),
        ).fetchone()
        if row is None:
            return None
        return _row_to_trade_journal(row)

    def trade_journal_by_entry_order_uuid(self, *, entry_order_uuid: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT * FROM trade_journal WHERE entry_order_uuid = ? ORDER BY updated_ts DESC LIMIT 1",
            (entry_order_uuid,),
        ).fetchone()
        if row is None:
            return None
        return _row_to_trade_journal(row)

    def trade_journal_by_exit_order_uuid(self, *, exit_order_uuid: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT * FROM trade_journal WHERE exit_order_uuid = ? ORDER BY updated_ts DESC LIMIT 1",
            (exit_order_uuid,),
        ).fetchone()
        if row is None:
            return None
        return _row_to_trade_journal(row)

    def list_trade_journal(
        self,
        *,
        statuses: tuple[str, ...] | None = None,
        market: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            clauses.append(f"status IN ({placeholders})")
            params.extend(statuses)
        if market:
            clauses.append("market = ?")
            params.append(market)

        query = "SELECT * FROM trade_journal"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY COALESCE(exit_ts_ms, entry_filled_ts_ms, entry_submitted_ts_ms, updated_ts) DESC, updated_ts DESC"
        if limit is not None and limit > 0:
            query += " LIMIT ?"
            params.append(int(limit))
        rows = self._conn.execute(query, tuple(params)).fetchall()
        return [_row_to_trade_journal(row) for row in rows]

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

    def get_checkpoint(self, *, name: str) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT * FROM checkpoints WHERE name = ?", (name,)).fetchone()
        if row is None:
            return None
        return _row_to_checkpoint(row)

    def set_runtime_contract(self, *, payload: dict[str, Any], ts_ms: int | None = None) -> None:
        self.set_checkpoint(name="live_runtime_contract", payload=payload, ts_ms=ts_ms)

    def runtime_contract(self) -> dict[str, Any] | None:
        checkpoint = self.get_checkpoint(name="live_runtime_contract")
        return dict(checkpoint.get("payload", {})) if checkpoint is not None else None

    def set_ws_public_contract(self, *, payload: dict[str, Any], ts_ms: int | None = None) -> None:
        self.set_checkpoint(name="ws_public_contract", payload=payload, ts_ms=ts_ms)

    def ws_public_contract(self) -> dict[str, Any] | None:
        checkpoint = self.get_checkpoint(name="ws_public_contract")
        return dict(checkpoint.get("payload", {})) if checkpoint is not None else None

    def set_live_runtime_health(self, *, payload: dict[str, Any], ts_ms: int | None = None) -> None:
        self.set_checkpoint(name="live_runtime_health", payload=payload, ts_ms=ts_ms)

    def live_runtime_health(self) -> dict[str, Any] | None:
        checkpoint = self.get_checkpoint(name="live_runtime_health")
        return dict(checkpoint.get("payload", {})) if checkpoint is not None else None

    def set_live_rollout_contract(self, *, payload: dict[str, Any], ts_ms: int | None = None) -> None:
        self.set_checkpoint(name="live_rollout_contract", payload=payload, ts_ms=ts_ms)

    def live_rollout_contract(self) -> dict[str, Any] | None:
        checkpoint = self.get_checkpoint(name="live_rollout_contract")
        return dict(checkpoint.get("payload", {})) if checkpoint is not None else None

    def set_live_test_order(self, *, payload: dict[str, Any], ts_ms: int | None = None) -> None:
        self.set_checkpoint(name="live_rollout_test_order", payload=payload, ts_ms=ts_ms)

    def live_test_order(self) -> dict[str, Any] | None:
        checkpoint = self.get_checkpoint(name="live_rollout_test_order")
        return dict(checkpoint.get("payload", {})) if checkpoint is not None else None

    def set_live_rollout_status(self, *, payload: dict[str, Any], ts_ms: int | None = None) -> None:
        self.set_checkpoint(name="live_rollout_status", payload=payload, ts_ms=ts_ms)

    def live_rollout_status(self) -> dict[str, Any] | None:
        checkpoint = self.get_checkpoint(name="live_rollout_status")
        return dict(checkpoint.get("payload", {})) if checkpoint is not None else None

    def upsert_breaker_state(self, record: BreakerStateRecord) -> None:
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO breaker_state (
                    breaker_key, active, action, source, reason_codes_json, details_json, updated_ts, armed_ts
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(breaker_key) DO UPDATE SET
                    active=excluded.active,
                    action=excluded.action,
                    source=excluded.source,
                    reason_codes_json=excluded.reason_codes_json,
                    details_json=excluded.details_json,
                    updated_ts=excluded.updated_ts,
                    armed_ts=excluded.armed_ts
                """,
                (
                    record.breaker_key,
                    1 if record.active else 0,
                    record.action,
                    record.source,
                    record.reason_codes_json,
                    record.details_json,
                    int(record.updated_ts),
                    int(record.armed_ts),
                ),
            )

    def breaker_state(self, *, breaker_key: str) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT * FROM breaker_state WHERE breaker_key = ?", (breaker_key,)).fetchone()
        if row is None:
            return None
        return _row_to_breaker_state(row)

    def append_breaker_event(self, record: BreakerEventRecord) -> None:
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO breaker_events (
                    ts_ms, breaker_key, event_kind, action, source, reason_codes_json, details_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(record.ts_ms),
                    record.breaker_key,
                    record.event_kind,
                    record.action,
                    record.source,
                    record.reason_codes_json,
                    record.details_json,
                ),
            )

    def list_breaker_events(self, *, breaker_key: str | None = None, limit: int | None = None) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if breaker_key:
            clauses.append("breaker_key = ?")
            params.append(breaker_key)
        query = "SELECT * FROM breaker_events"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY event_id DESC"
        if limit is not None and limit > 0:
            query += " LIMIT ?"
            params.append(int(limit))
        rows = self._conn.execute(query, tuple(params)).fetchall()
        return [_row_to_breaker_event(row) for row in rows]

    def append_order_lineage(self, record: OrderLineageRecord) -> None:
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO order_lineage (
                    ts_ms, event_source, intent_id, prev_uuid, prev_identifier,
                    new_uuid, new_identifier, replace_seq
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(record.ts_ms),
                    record.event_source,
                    record.intent_id,
                    record.prev_uuid,
                    record.prev_identifier,
                    record.new_uuid,
                    record.new_identifier,
                    int(record.replace_seq),
                ),
            )

    def list_order_lineage(
        self,
        *,
        intent_id: str | None = None,
        root_order_uuid: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if intent_id:
            clauses.append("intent_id = ?")
            params.append(intent_id)
        if root_order_uuid:
            clauses.append("(prev_uuid = ? OR new_uuid = ?)")
            params.extend([root_order_uuid, root_order_uuid])
        query = "SELECT * FROM order_lineage"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY edge_id"
        rows = self._conn.execute(query, tuple(params)).fetchall()
        return [_row_to_order_lineage(row) for row in rows]

    def export_state(self) -> dict[str, Any]:
        return {
            "db_path": str(self._db_path),
            "bot_meta": [_row_to_plain_dict(row) for row in self._conn.execute("SELECT * FROM bot_meta").fetchall()],
            "positions": self.list_positions(),
            "orders": self.list_orders(open_only=False),
            "intents": self.list_intents(),
            "risk_plans": self.list_risk_plans(),
            "trade_journal": self.list_trade_journal(),
            "checkpoints": [
                _row_to_checkpoint(row) for row in self._conn.execute("SELECT * FROM checkpoints ORDER BY name").fetchall()
            ],
            "breaker_state": [
                _row_to_breaker_state(row)
                for row in self._conn.execute("SELECT * FROM breaker_state ORDER BY breaker_key").fetchall()
            ],
            "breaker_events": [
                _row_to_breaker_event(row)
                for row in self._conn.execute("SELECT * FROM breaker_events ORDER BY event_id").fetchall()
            ],
            "order_lineage": [
                _row_to_order_lineage(row)
                for row in self._conn.execute("SELECT * FROM order_lineage ORDER BY edge_id").fetchall()
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
                    tp_sl_link TEXT,
                    local_state TEXT,
                    raw_exchange_state TEXT,
                    last_event_name TEXT,
                    event_source TEXT,
                    replace_seq INTEGER NOT NULL DEFAULT 0,
                    root_order_uuid TEXT,
                    prev_order_uuid TEXT,
                    prev_order_identifier TEXT,
                    executed_funds REAL,
                    paid_fee REAL,
                    reserved_fee REAL,
                    remaining_fee REAL,
                    exchange_payload_json TEXT NOT NULL DEFAULT '{}'
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
                    timeout_ts_ms INTEGER,
                    state TEXT NOT NULL DEFAULT 'ACTIVE',
                    last_eval_ts_ms INTEGER NOT NULL DEFAULT 0,
                    last_action_ts_ms INTEGER NOT NULL DEFAULT 0,
                    current_exit_order_uuid TEXT,
                    current_exit_order_identifier TEXT,
                    replace_attempt INTEGER NOT NULL DEFAULT 0,
                    created_ts INTEGER NOT NULL,
                    updated_ts INTEGER NOT NULL,
                    plan_source TEXT,
                    source_intent_id TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_risk_plans_market_state ON risk_plans (market, state);

                CREATE TABLE IF NOT EXISTS trade_journal (
                    journal_id TEXT PRIMARY KEY,
                    market TEXT NOT NULL,
                    status TEXT NOT NULL,
                    entry_intent_id TEXT,
                    entry_order_uuid TEXT,
                    exit_order_uuid TEXT,
                    plan_id TEXT,
                    entry_submitted_ts_ms INTEGER,
                    entry_filled_ts_ms INTEGER,
                    exit_ts_ms INTEGER,
                    entry_price REAL,
                    exit_price REAL,
                    qty REAL,
                    entry_notional_quote REAL,
                    exit_notional_quote REAL,
                    realized_pnl_quote REAL,
                    realized_pnl_pct REAL,
                    entry_reason_code TEXT,
                    close_reason_code TEXT,
                    close_mode TEXT,
                    model_prob REAL,
                    selection_policy_mode TEXT,
                    trade_action TEXT,
                    expected_edge_bps REAL,
                    expected_downside_bps REAL,
                    expected_net_edge_bps REAL,
                    notional_multiplier REAL,
                    entry_meta_json TEXT NOT NULL DEFAULT '{}',
                    exit_meta_json TEXT NOT NULL DEFAULT '{}',
                    updated_ts INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS checkpoints (
                    name TEXT PRIMARY KEY,
                    ts_ms INTEGER NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS breaker_state (
                    breaker_key TEXT PRIMARY KEY,
                    active INTEGER NOT NULL DEFAULT 0,
                    action TEXT,
                    source TEXT NOT NULL,
                    reason_codes_json TEXT NOT NULL DEFAULT '[]',
                    details_json TEXT NOT NULL DEFAULT '{}',
                    updated_ts INTEGER NOT NULL,
                    armed_ts INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS breaker_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_ms INTEGER NOT NULL,
                    breaker_key TEXT NOT NULL,
                    event_kind TEXT NOT NULL,
                    action TEXT,
                    source TEXT NOT NULL,
                    reason_codes_json TEXT NOT NULL DEFAULT '[]',
                    details_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS order_lineage (
                    edge_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_ms INTEGER NOT NULL,
                    event_source TEXT NOT NULL,
                    intent_id TEXT,
                    prev_uuid TEXT,
                    prev_identifier TEXT,
                    new_uuid TEXT,
                    new_identifier TEXT,
                    replace_seq INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS run_locks (
                    bot_id TEXT PRIMARY KEY,
                    acquired_ts INTEGER NOT NULL,
                    owner_pid INTEGER NOT NULL
                );
                """
            )
        self._ensure_column("orders", "local_state", "TEXT")
        self._ensure_column("orders", "raw_exchange_state", "TEXT")
        self._ensure_column("orders", "last_event_name", "TEXT")
        self._ensure_column("orders", "event_source", "TEXT")
        self._ensure_column("orders", "replace_seq", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("orders", "root_order_uuid", "TEXT")
        self._ensure_column("orders", "prev_order_uuid", "TEXT")
        self._ensure_column("orders", "prev_order_identifier", "TEXT")
        self._ensure_column("orders", "executed_funds", "REAL")
        self._ensure_column("orders", "paid_fee", "REAL")
        self._ensure_column("orders", "reserved_fee", "REAL")
        self._ensure_column("orders", "remaining_fee", "REAL")
        self._ensure_column("orders", "exchange_payload_json", "TEXT NOT NULL DEFAULT '{}'")
        self._ensure_column("risk_plans", "timeout_ts_ms", "INTEGER")
        self._ensure_column("risk_plans", "plan_source", "TEXT")
        self._ensure_column("risk_plans", "source_intent_id", "TEXT")
        with self._conn:
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_trade_journal_market_status ON trade_journal (market, status, updated_ts)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_trade_journal_entry_intent_id ON trade_journal (entry_intent_id)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_trade_journal_exit_order_uuid ON trade_journal (exit_order_uuid)"
            )
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_root_order_uuid ON orders (root_order_uuid)")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_local_state ON orders (local_state)")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_breaker_events_key_ts ON breaker_events (breaker_key, ts_ms)")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS order_lineage (
                    edge_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_ms INTEGER NOT NULL,
                    event_source TEXT NOT NULL,
                    intent_id TEXT,
                    prev_uuid TEXT,
                    prev_identifier TEXT,
                    new_uuid TEXT,
                    new_identifier TEXT,
                    replace_seq INTEGER NOT NULL DEFAULT 0
                )
                """
            )

    def _ensure_column(self, table_name: str, column_name: str, definition: str) -> None:
        rows = self._conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        existing = {str(row["name"]) for row in rows}
        if column_name in existing:
            return
        with self._conn:
            self._conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


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
        "local_state": row["local_state"],
        "raw_exchange_state": row["raw_exchange_state"],
        "last_event_name": row["last_event_name"],
        "event_source": row["event_source"],
        "replace_seq": int(row["replace_seq"]) if row["replace_seq"] is not None else 0,
        "root_order_uuid": row["root_order_uuid"],
        "prev_order_uuid": row["prev_order_uuid"],
        "prev_order_identifier": row["prev_order_identifier"],
        "executed_funds": float(row["executed_funds"]) if row["executed_funds"] is not None else None,
        "paid_fee": float(row["paid_fee"]) if row["paid_fee"] is not None else None,
        "reserved_fee": float(row["reserved_fee"]) if row["reserved_fee"] is not None else None,
        "remaining_fee": float(row["remaining_fee"]) if row["remaining_fee"] is not None else None,
        "exchange_payload": _parse_json(row["exchange_payload_json"]),
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
        "timeout_ts_ms": int(row["timeout_ts_ms"]) if row["timeout_ts_ms"] is not None else None,
        "state": row["state"],
        "last_eval_ts_ms": int(row["last_eval_ts_ms"]),
        "last_action_ts_ms": int(row["last_action_ts_ms"]),
        "current_exit_order_uuid": row["current_exit_order_uuid"],
        "current_exit_order_identifier": row["current_exit_order_identifier"],
        "replace_attempt": int(row["replace_attempt"]),
        "created_ts": int(row["created_ts"]),
        "updated_ts": int(row["updated_ts"]),
        "plan_source": row["plan_source"],
        "source_intent_id": row["source_intent_id"],
    }


def _row_to_trade_journal(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "journal_id": row["journal_id"],
        "market": row["market"],
        "status": row["status"],
        "entry_intent_id": row["entry_intent_id"],
        "entry_order_uuid": row["entry_order_uuid"],
        "exit_order_uuid": row["exit_order_uuid"],
        "plan_id": row["plan_id"],
        "entry_submitted_ts_ms": int(row["entry_submitted_ts_ms"]) if row["entry_submitted_ts_ms"] is not None else None,
        "entry_filled_ts_ms": int(row["entry_filled_ts_ms"]) if row["entry_filled_ts_ms"] is not None else None,
        "exit_ts_ms": int(row["exit_ts_ms"]) if row["exit_ts_ms"] is not None else None,
        "entry_price": float(row["entry_price"]) if row["entry_price"] is not None else None,
        "exit_price": float(row["exit_price"]) if row["exit_price"] is not None else None,
        "qty": float(row["qty"]) if row["qty"] is not None else None,
        "entry_notional_quote": float(row["entry_notional_quote"]) if row["entry_notional_quote"] is not None else None,
        "exit_notional_quote": float(row["exit_notional_quote"]) if row["exit_notional_quote"] is not None else None,
        "realized_pnl_quote": float(row["realized_pnl_quote"]) if row["realized_pnl_quote"] is not None else None,
        "realized_pnl_pct": float(row["realized_pnl_pct"]) if row["realized_pnl_pct"] is not None else None,
        "entry_reason_code": row["entry_reason_code"],
        "close_reason_code": row["close_reason_code"],
        "close_mode": row["close_mode"],
        "model_prob": float(row["model_prob"]) if row["model_prob"] is not None else None,
        "selection_policy_mode": row["selection_policy_mode"],
        "trade_action": row["trade_action"],
        "expected_edge_bps": float(row["expected_edge_bps"]) if row["expected_edge_bps"] is not None else None,
        "expected_downside_bps": float(row["expected_downside_bps"]) if row["expected_downside_bps"] is not None else None,
        "expected_net_edge_bps": float(row["expected_net_edge_bps"]) if row["expected_net_edge_bps"] is not None else None,
        "notional_multiplier": float(row["notional_multiplier"]) if row["notional_multiplier"] is not None else None,
        "entry_meta": _parse_json(row["entry_meta_json"]),
        "exit_meta": _parse_json(row["exit_meta_json"]),
        "updated_ts": int(row["updated_ts"]),
    }


def _row_to_checkpoint(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "name": row["name"],
        "ts_ms": int(row["ts_ms"]),
        "payload": _parse_json(row["payload_json"]),
    }


def _row_to_order_lineage(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "edge_id": int(row["edge_id"]),
        "ts_ms": int(row["ts_ms"]),
        "event_source": row["event_source"],
        "intent_id": row["intent_id"],
        "prev_uuid": row["prev_uuid"],
        "prev_identifier": row["prev_identifier"],
        "new_uuid": row["new_uuid"],
        "new_identifier": row["new_identifier"],
        "replace_seq": int(row["replace_seq"]),
    }


def _row_to_breaker_state(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "breaker_key": row["breaker_key"],
        "active": bool(row["active"]),
        "action": row["action"],
        "source": row["source"],
        "reason_codes": _parse_json_list(row["reason_codes_json"]),
        "details": _parse_json(row["details_json"]),
        "updated_ts": int(row["updated_ts"]),
        "armed_ts": int(row["armed_ts"]),
    }


def _row_to_breaker_event(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "event_id": int(row["event_id"]),
        "ts_ms": int(row["ts_ms"]),
        "breaker_key": row["breaker_key"],
        "event_kind": row["event_kind"],
        "action": row["action"],
        "source": row["source"],
        "reason_codes": _parse_json_list(row["reason_codes_json"]),
        "details": _parse_json(row["details_json"]),
    }


def _is_pending_order_uuid(value: object) -> bool:
    text = str(value or "").strip().lower()
    return text.startswith("pending-") or text.startswith("pending:")


def _can_rebind_pending_identifier(
    *,
    existing_identifier: dict[str, Any],
    incoming_uuid: str,
    market: str | None,
    side: str | None,
) -> bool:
    existing_uuid = str(existing_identifier.get("uuid") or "").strip()
    incoming_uuid_value = str(incoming_uuid or "").strip()
    if not _is_pending_order_uuid(existing_uuid) or _is_pending_order_uuid(incoming_uuid_value):
        return False
    existing_market = str(existing_identifier.get("market") or "").strip().upper()
    incoming_market = str(market or "").strip().upper()
    if existing_market and incoming_market and existing_market != incoming_market:
        return False
    existing_side = str(existing_identifier.get("side") or "").strip().lower()
    incoming_side = str(side or "").strip().lower()
    if existing_side and incoming_side and existing_side != incoming_side:
        return False
    return bool(incoming_uuid_value)


def _min_positive_int(*values: int) -> int | None:
    candidates = [int(value) for value in values if int(value) > 0]
    if not candidates:
        return None
    return min(candidates)


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


def _parse_json_list(raw: object) -> list[Any]:
    if raw is None:
        return []
    try:
        value = json.loads(str(raw))
    except json.JSONDecodeError:
        return []
    if isinstance(value, list):
        return value
    return []
