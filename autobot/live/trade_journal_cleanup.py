from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
from typing import Any


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {str(key): row[key] for key in row.keys()}


def _as_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _matches_close_geometry(imported: dict[str, Any], canonical: dict[str, Any]) -> bool:
    if str(imported.get("market") or "").strip().upper() != str(canonical.get("market") or "").strip().upper():
        return False
    imported_exit_ts = _as_int(imported.get("exit_ts_ms"))
    canonical_exit_ts = _as_int(canonical.get("exit_ts_ms"))
    if imported_exit_ts is None or canonical_exit_ts is None or abs(imported_exit_ts - canonical_exit_ts) > 120_000:
        return False
    imported_qty = _as_float(imported.get("qty"))
    canonical_qty = _as_float(canonical.get("qty"))
    if imported_qty is not None and canonical_qty is not None and abs(imported_qty - canonical_qty) > max(1e-8, abs(imported_qty) * 1e-6):
        return False
    imported_entry_price = _as_float(imported.get("entry_price"))
    canonical_entry_price = _as_float(canonical.get("entry_price"))
    if (
        imported_entry_price is not None
        and canonical_entry_price is not None
        and abs(imported_entry_price - canonical_entry_price) > max(1e-8, abs(imported_entry_price) * 1e-6)
    ):
        return False
    return True


def _load_duplicate_candidates(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    imported_rows = [
        _row_to_dict(row)
        for row in conn.execute(
            """
            SELECT *
            FROM trade_journal
            WHERE journal_id LIKE 'imported-%'
              AND status = 'CLOSED'
            ORDER BY updated_ts ASC, journal_id ASC
            """
        ).fetchall()
    ]
    canonical_rows = [
        _row_to_dict(row)
        for row in conn.execute(
            """
            SELECT *
            FROM trade_journal
            WHERE journal_id NOT LIKE 'imported-%'
              AND status = 'CLOSED'
            ORDER BY updated_ts ASC, journal_id ASC
            """
        ).fetchall()
    ]
    candidates: list[dict[str, Any]] = []
    for imported in imported_rows:
        exit_order_uuid = str(imported.get("exit_order_uuid") or "").strip()
        if exit_order_uuid:
            matched = [row for row in canonical_rows if str(row.get("exit_order_uuid") or "").strip() == exit_order_uuid]
            if matched:
                candidates.append(
                    {
                        "match_kind": "exit_order_uuid",
                        "delete_row": imported,
                        "keep_rows": matched,
                    }
                )
                continue
        matched = [row for row in canonical_rows if _matches_close_geometry(imported, row)]
        if matched:
            candidates.append(
                {
                    "match_kind": "recent_market_geometry",
                    "delete_row": imported,
                    "keep_rows": matched,
                }
            )
    return candidates


def find_imported_trade_journal_duplicates(db_path: Path) -> list[dict[str, Any]]:
    conn = sqlite3.connect(str(db_path))
    try:
        candidates = _load_duplicate_candidates(conn)
        return [
            {
                "match_kind": item["match_kind"],
                "delete_journal_id": str(item["delete_row"]["journal_id"]),
                "keep_journal_ids": [str(row["journal_id"]) for row in item["keep_rows"]],
            }
            for item in candidates
        ]
    finally:
        conn.close()


def cleanup_imported_trade_journal_duplicates(db_path: Path, *, apply_changes: bool) -> dict[str, Any]:
    conn = sqlite3.connect(str(db_path))
    try:
        candidates = _load_duplicate_candidates(conn)
        groups: list[dict[str, Any]] = []
        for item in candidates:
            delete_row = item["delete_row"]
            keep_rows = item["keep_rows"]
            groups.append(
                {
                    "match_kind": str(item["match_kind"]),
                    "market": delete_row.get("market"),
                    "exit_order_uuid": delete_row.get("exit_order_uuid"),
                    "delete_journal_ids": [str(delete_row["journal_id"])],
                    "keep_journal_ids": [str(row["journal_id"]) for row in keep_rows],
                    "deleted_realized_pnl_quote_total": float(_as_float(delete_row.get("realized_pnl_quote")) or 0.0),
                }
            )

        if apply_changes and candidates:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.executemany(
                    "DELETE FROM trade_journal WHERE journal_id = ?",
                    [(str(item["delete_row"]["journal_id"]),) for item in candidates],
                )
            except Exception:
                conn.rollback()
                raise
            conn.commit()

        return {
            "db_path": str(db_path),
            "apply_changes": bool(apply_changes),
            "duplicate_row_count": len(candidates),
            "deleted_realized_pnl_quote_total": float(
                sum(float(_as_float(item["delete_row"].get("realized_pnl_quote")) or 0.0) for item in candidates)
            ),
            "groups": groups,
        }
    finally:
        conn.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean imported duplicate trade journal close rows.")
    parser.add_argument("--db-path", required=True, help="Path to live_state.db")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Delete duplicate imported rows. Without this flag, only report candidates.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    report = cleanup_imported_trade_journal_duplicates(Path(args.db_path), apply_changes=bool(args.apply))
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
